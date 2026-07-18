"""Backtrader broker adapter for QUIK.

Design constraints for the 1.0 line:
* one QKBroker instance manages exactly one QUIK account;
* provider callbacks enqueue raw events only;
* Order and Position objects are mutated from the Cerebro thread in ``next``;
* sizes that are not valid exchange lots are rejected, never truncated.
"""
from __future__ import annotations

from collections import OrderedDict, defaultdict, deque
from datetime import date, datetime, timedelta
from decimal import Decimal
from queue import Empty, Queue
from typing import Iterable
from time import monotonic

from backtrader import BrokerBase, BuyOrder, Order, SellOrder, date2num, num2date
from backtrader.position import Position
from backtrader.utils.py3 import with_metaclass

from .logger_config import logger
from .QKStore import QKStore


class MetaQKBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        QKStore.BrokerCls = cls


class QKBroker(with_metaclass(MetaQKBroker, BrokerBase)):
    """Live QUIK broker for a single account."""

    params = (
        ('account_id', None),
        ('lots', False),
        ('slippage_steps', 10),
        ('client_code', None),
        ('client_code_for_orders', None),
        ('max_seen_trades', 100_000),
        ('account_snapshot_reuse_window', 0.25),
        ('market_order_poll_interval', 0.5),
    )

    _FINAL_STATUSES = {
        Order.Completed, Order.Canceled, Order.Expired,
        Order.Margin, Order.Rejected,
    }
    _TRANS_ERROR_STATUSES = {2, 4, 5, 10, 11, 12, 13, 14, 16}

    def __init__(self, **kwargs):
        super().__init__()
        self.store = QKStore(**kwargs)
        self.accounts = list(self.store.provider.accounts)
        self.account = dict(self._select_account(self.p.account_id))
        self.account_id = self.account['account_id']
        if self.p.client_code is not None:
            self.account['client_code'] = str(self.p.client_code)
        if not self.account.get('futures') and not self.account.get('client_code'):
            candidates = self.account.get('client_code_candidates', [])
            raise ValueError(
                'Для фондового счета не удалось однозначно определить '
                'client_code. Передайте client_code=... в getbroker(). '
                f'Кандидаты: {candidates}'
            )

        if int(self.p.slippage_steps) != self.p.slippage_steps or self.p.slippage_steps < 0:
            raise ValueError('slippage_steps должен быть неотрицательным целым')
        if int(self.p.max_seen_trades) <= 0:
            raise ValueError('max_seen_trades должен быть положительным')
        if float(self.p.account_snapshot_reuse_window) < 0:
            raise ValueError('account_snapshot_reuse_window не может быть отрицательным')
        if float(self.p.market_order_poll_interval) <= 0:
            raise ValueError('market_order_poll_interval должен быть положительным')

        self.notifs = deque()
        self._event_queue: Queue[tuple[str, dict]] = Queue()
        self._subscribed = False
        self._datas = []
        self._cash_snapshot_at = 0.0
        self._cash_snapshot_valid = False
        self._broker_stopped = False
        self._market_trade_poll_at = {}

        self.startingcash = self.cash = 0.0
        self.startingvalue = self.value = 0.0
        self.positions = defaultdict(Position)
        self.orders = OrderedDict()
        self.ocos: dict[int, int] = {}
        self.pcs = defaultdict(deque)
        self._seen_trades = OrderedDict()

    def _select_account(self, account_id):
        if account_id is None:
            if len(self.accounts) != 1:
                available = [account.get('account_id') for account in self.accounts]
                raise ValueError(
                    'При нескольких счетах account_id обязателен. '
                    f'Доступные account_id: {available}'
                )
            return self.accounts[0]
        account = next(
            (item for item in self.accounts if item.get('account_id') == account_id),
            None,
        )
        if account is None:
            raise ValueError(f'Счет account_id={account_id!r} не найден')
        return account

    # Backtrader lifecycle -------------------------------------------------

    def start(self):
        super().start()
        self._broker_stopped = False
        if not self._subscribed:
            self.store.provider.on_trans_reply.subscribe(self.on_trans_reply)
            self.store.provider.on_trade.subscribe(self.on_trade)
            self._subscribed = True
        self._datas = list(getattr(self.cerebro, 'datas', ()))
        self.get_all_active_positions()
        self.cash = self.getcash(refresh=True)
        self.value = self.getvalue()
        self.startingcash = self.cash
        self.startingvalue = self.value

    def stop(self):
        self.process_pending_events()
        if not self._broker_stopped:
            try:
                self.cash = self.getcash(refresh=True)
                self.value = self.getvalue(refresh=False)
            except Exception as exc:
                logger.warning('Не удалось обновить финальный снимок брокера: %s', exc)
                self.store.put_notification(
                    'BROKER_FINAL_SNAPSHOT_ERROR',
                    f'{type(exc).__name__}: {exc}',
                )
            self._broker_stopped = True
        if self._subscribed:
            self.store.provider.on_trans_reply.unsubscribe(self.on_trans_reply)
            self.store.provider.on_trade.unsubscribe(self.on_trade)
            self._subscribed = False
        super().stop()

    def next(self):
        self._poll_accepted_market_orders()
        self.process_pending_events()
        self.notifs.append(None)

    def get_notification(self):
        if not self.notifs:
            return None
        return self.notifs.popleft()

    # Thread boundary -----------------------------------------------------

    def on_trans_reply(self, data):
        """Provider callback; do not mutate Backtrader objects here."""
        self._event_queue.put(('trans_reply', data))

    def on_trade(self, data):
        """Provider callback; do not mutate Backtrader objects here."""
        self._event_queue.put(('trade', data))

    def process_pending_events(self):
        while True:
            try:
                event_type, payload = self._event_queue.get_nowait()
            except Empty:
                return
            try:
                if event_type == 'trans_reply':
                    self._handle_trans_reply(payload)
                elif event_type == 'trade':
                    self._handle_trade(payload)
            except Exception:
                logger.exception('Ошибка применения события QUIK: %s %r', event_type, payload)
                self.store.put_notification('BROKER_EVENT_ERROR', event_type, payload)

    def _poll_accepted_market_orders(self):
        """Recover executions when QUIK Junior omits an OnTrade callback.

        Only an accepted Market order with an exact exchange order number is
        queried. Native and recovered events share the normal trade dedupe.
        """
        now = monotonic()
        required = {
            'trade_num', 'trans_id', 'qty', 'price', 'class_code', 'sec_code',
        }
        for order in tuple(self.orders.values()):
            if not order.alive() or order.exectype != Order.Market:
                continue
            order_num = order.info.get('order_num')
            if not order_num:
                continue
            last_poll = self._market_trade_poll_at.get(order.ref, 0.0)
            if now - last_poll < float(self.p.market_order_poll_interval):
                continue
            self._market_trade_poll_at[order.ref] = now
            try:
                response = self.store.provider.get_trades_by_order_number(order_num)
                payload = response.get('data') or []
                rows = payload if isinstance(payload, list) else [payload]
                matched = False
                for trade in rows:
                    if not isinstance(trade, dict) or not required.issubset(trade):
                        continue
                    if int(trade.get('trans_id') or 0) != order.ref:
                        continue
                    matched = True
                    self.on_trade({'cmd': 'MarketTradePoll', 'data': trade})
                if not matched:
                    order_row = self.store.provider.get_order_by_number(
                        order_num
                    ).get('data')
                    if self._is_inactive_unfilled_order(order_row):
                        order.addinfo(
                            reconciled_cancel=True,
                            exchange_flags=order_row.get('flags'),
                            exchange_balance=order_row.get('balance'),
                        )
                        order.cancel()
                        self.notifs.append(order.clone())
                        self._terminal_order_actions(order)
            except Exception as exc:
                self.store.put_notification(
                    'MARKET_TRADE_POLL_ERROR',
                    order.ref,
                    f'{type(exc).__name__}: {exc}',
                )

    @staticmethod
    def _is_inactive_unfilled_order(item):
        if not isinstance(item, dict):
            return False
        try:
            return not (int(item.get('flags') or 0) & 0b1) and float(
                item.get('balance') or 0.0
            ) > 0
        except (TypeError, ValueError):
            return False

    # Account, cash and value --------------------------------------------

    def _validate_account_scope(self, account_id):
        if account_id is not None and account_id != self.account_id:
            raise ValueError(
                f'Этот QKBroker обслуживает account_id={self.account_id}, '
                f'а запрошен account_id={account_id}'
            )

    def _class_allowed(self, class_code):
        if class_code not in self.account.get('class_codes', ()):
            return False
        is_futures_class = class_code == self.store.provider.futures_cls_code
        return is_futures_class if self.account.get('futures') else not is_futures_class

    def getcash(self, account_id=None, refresh=True):
        """Return the selected account cash component.

        By default this is a synchronous refresh. ``getvalue()`` may reuse
        the immediately preceding result for a short configurable window,
        preventing Backtrader from issuing the same QUIK request twice.
        """
        self._validate_account_scope(account_id)
        if self._broker_stopped:
            return self.cash
        interval = float(self.p.account_snapshot_reuse_window)
        if (
            not refresh and self._cash_snapshot_valid
            and monotonic() - self._cash_snapshot_at <= interval
        ):
            return self.cash
        account = self.account
        if account.get('futures'):
            response = self.store.provider.get_futures_limit(
                account['firm_id'], account['trade_account_id'], 0,
                self.store.provider.currency,
            )
            limit = response.get('data') or {}
            cash = sum(float(limit.get(field) or 0.0) for field in (
                'cbplimit', 'varmargin', 'accruedint'
            ))
        else:
            limits = self.store.provider.get_money_limits().get('data') or []
            matching = [
                item for item in limits
                if item.get('client_code') == account.get('client_code')
                and item.get('firmid') == account.get('firm_id')
                and item.get('currcode') == self.store.provider.currency
            ]
            if not matching:
                cash = 0.0
            else:
                latest = max(matching, key=lambda item: int(item.get('limit_kind') or 0))
                cash = float(latest.get('currentbal') or 0.0)
        self.cash = cash
        self._cash_snapshot_at = monotonic()
        self._cash_snapshot_valid = True
        return cash

    def getvalue(self, datas=None, account_id=None, refresh=False):
        """Return equity, or position value when ``datas`` is supplied.

        For a futures account the account-level value comes from the QUIK
        futures limit (variation margin included); futures nominal is not added.
        """
        self._validate_account_scope(account_id)
        if self._broker_stopped and datas is None:
            return self.value
        if datas is not None:
            selected = list(datas)
            value = self._positions_value(selected)
            return value

        cash = self.getcash(refresh=refresh)
        if self.account.get('futures'):
            self.value = cash
        else:
            self.value = cash + self._positions_value(None)
        return self.value

    def _positions_value(self, datas: Iterable | None):
        selected = None if datas is None else list(datas)
        data_by_name = {data._name: data for data in self._datas}
        if selected is not None:
            data_by_name.update({data._name: data for data in selected})
        names = None if selected is None else {data._name for data in selected}
        total = 0.0
        for dataname, position in list(self.positions.items()):
            if not position.size or (names is not None and dataname not in names):
                continue
            class_code, sec_code = self.store.provider.dataname_to_class_sec_codes(dataname)
            if not self._class_allowed(class_code):  # defensive
                continue
            data = data_by_name.get(dataname)
            last_price = self._last_price(class_code, sec_code, data=data)
            if data is not None:
                # Backtrader's default short-cash model treats a short position
                # as a negative market value. Preserve the sign instead of
                # applying abs(), otherwise account equity is overstated.
                raw = self.getcommissioninfo(data).getvaluesize(
                    position.size, last_price
                )
            else:
                raw = position.size * last_price
            if names is not None and len(names) == 1:
                return raw
            total += raw
        return total

    def _last_price(self, class_code, sec_code, data=None):
        if data is not None:
            try:
                value = float(data.close[0])
                if value == value:  # not NaN
                    return value
            except (AttributeError, IndexError, TypeError, ValueError):
                pass
        response = self.store.provider.get_param_ex(class_code, sec_code, 'LAST')
        payload = response.get('data') or {}
        raw_value = payload.get('param_value')
        if raw_value in (None, ''):
            raise RuntimeError(f'QUIK не вернул LAST для {class_code}.{sec_code}')
        raw = float(raw_value)
        if class_code == self.store.provider.futures_cls_code:
            return raw
        return self.store.provider.quik_price_to_price(class_code, sec_code, raw)

    def getposition(self, data):
        return self.positions[data._name]

    def get_all_active_positions(self):
        """Load initial positions for this broker's account only."""
        self.positions.clear()
        account = self.account
        if account.get('futures'):
            holdings = self.store.provider.get_futures_holdings().get('data') or []
            for holding in holdings:
                if not self._holding_matches_account(holding):
                    continue
                size = int(holding.get('totalnet') or 0)
                if not size:
                    continue
                class_code = self.store.provider.futures_cls_code
                sec_code = holding['sec_code']
                if self.p.lots:
                    size = self.store.provider.lots_to_size(class_code, sec_code, size)
                price = float(holding.get('avrposnprice') or 0.0)
                dataname = self.store.provider.class_sec_codes_to_dataname(class_code, sec_code)
                self.positions[dataname] = Position(size, price)
            return

        limits = self.store.provider.get_all_depo_limits().get('data') or []
        account_limits = []
        for item in limits:
            if self._depo_limit_matches_account(item):
                account_limits.append(item)
        latest_by_sec = {}
        for item in account_limits:
            key = item.get('sec_code')
            if key not in latest_by_sec or int(item.get('limit_kind') or 0) > int(latest_by_sec[key].get('limit_kind') or 0):
                latest_by_sec[key] = item
        for item in latest_by_sec.values():
            size = int(item.get('currentbal') or 0)
            if not size:
                continue
            try:
                class_code, sec_code = self._resolve_account_security(item['sec_code'])
            except ValueError as exc:
                logger.error('%s; позиция пропущена', exc)
                continue
            if self.p.lots:
                size = self.store.provider.lots_to_size(class_code, sec_code, size)
            price = self.store.provider.quik_price_to_price(
                class_code, sec_code, float(item.get('wa_position_price') or 0.0)
            )
            dataname = self.store.provider.class_sec_codes_to_dataname(class_code, sec_code)
            self.positions[dataname] = Position(size, price)

    def _holding_matches_account(self, holding):
        for field in ('trdaccid', 'trade_account_id', 'account'):
            if field in holding and holding[field] not in (None, '', self.account['trade_account_id']):
                return False
        if 'firmid' in holding and holding['firmid'] not in (None, '', self.account['firm_id']):
            return False
        return True

    def _depo_limit_matches_account(self, item):
        if item.get('client_code') not in (None, '', self.account.get('client_code')):
            return False
        if item.get('firmid') not in (None, '', self.account.get('firm_id')):
            return False
        scoped = False
        for field in ('trdaccid', 'trade_account_id', 'account'):
            value = item.get(field)
            if value not in (None, ''):
                scoped = True
                if value != self.account.get('trade_account_id'):
                    return False
        if not scoped:
            same_owner_accounts = [
                account for account in self.accounts
                if account.get('client_code') == self.account.get('client_code')
                and account.get('firm_id') == self.account.get('firm_id')
                and not account.get('futures')
            ]
            if len(same_owner_accounts) > 1:
                raise RuntimeError(
                    'QUIK не указал торговый счет в depo_limit, а у клиента '
                    'несколько счетов одной фирмы. Безопасно разделить позиции '
                    'невозможно.'
                )
        return True

    def _resolve_account_security(self, dataname_or_sec_code):
        value = str(dataname_or_sec_code)
        if '.' in value:
            class_code, sec_code = self.store.provider.dataname_to_class_sec_codes(value)
            if not self._class_allowed(class_code):
                raise ValueError(
                    f'Инструмент {value} не относится к account_id={self.account_id}'
                )
            return class_code, sec_code
        matches = [
            class_code for class_code in self.account.get('class_codes', ())
            if self._class_allowed(class_code)
            and value in getattr(self.store.provider, 'classes', {}).get(class_code, ())
        ]
        if len(matches) != 1:
            raise ValueError(
                f'Нельзя однозначно определить класс для {value!r} на '
                f'account_id={self.account_id}; найдено: {matches}'
            )
        return matches[0], value

    # Order creation ------------------------------------------------------

    def buy(self, owner, data, size, price=None, plimit=None, exectype=None,
            valid=None, tradeid=0, oco=None, trailamount=None,
            trailpercent=None, parent=None, transmit=True, **kwargs):
        order = self.create_order(
            owner, data, size, price, plimit, exectype, valid, tradeid,
            oco, trailamount, trailpercent, parent, transmit, True, **kwargs
        )
        return order

    def sell(self, owner, data, size, price=None, plimit=None, exectype=None,
             valid=None, tradeid=0, oco=None, trailamount=None,
             trailpercent=None, parent=None, transmit=True, **kwargs):
        order = self.create_order(
            owner, data, size, price, plimit, exectype, valid, tradeid,
            oco, trailamount, trailpercent, parent, transmit, False, **kwargs
        )
        return order

    def cancel(self, order):
        return self.cancel_order(order)

    def create_order(self, owner, data, size, price=None, plimit=None,
                     exectype=None, valid=None, tradeid=0, oco=None,
                     trailamount=None, trailpercent=None, parent=None,
                     transmit=True, is_buy=True, **kwargs):
        order_cls = BuyOrder if is_buy else SellOrder
        order = order_cls(
            owner=owner, data=data, size=size, price=price,
            pricelimit=plimit, exectype=exectype, valid=valid,
            tradeid=tradeid, trailamount=trailamount,
            trailpercent=trailpercent, parent=parent, transmit=transmit,
        )
        self.orders[order.ref] = order
        order.addcomminfo(self.getcommissioninfo(data))
        order.addinfo(original_valid=valid, **kwargs)

        class_code, sec_code = data.class_code, data.sec_code
        if order.exectype in (
            Order.Close, Order.StopTrail, Order.StopTrailLimit, Order.Historical
        ):
            return self._reject(order, f'Тип заявки {order.exectype} не реализован')

        if order.exectype in (Order.Market, Order.Limit):
            original_valid = order.info.get('original_valid')
            if original_valid not in (None, 0, Order.DAY):
                return self._reject(
                    order,
                    'QUIK не поддерживает датированную valid для обычной '
                    'рыночной/лимитной заявки; используйте DAY/None',
                )

        requested_account_id = order.info.get('account_id', self.account_id)
        if requested_account_id != self.account_id or not self._class_allowed(class_code):
            return self._reject(order, 'Инструмент/счет не соответствует QKBroker')
        order.addinfo(account=self.account)

        symbol_info = self.store.provider.get_symbol_info(class_code, sec_code)
        if not symbol_info:
            return self._reject(order, f'Инструмент {class_code}.{sec_code} не найден')
        order.addinfo(min_price_step=float(symbol_info['min_price_step']))

        if oco is not None:
            if oco.status in self._FINAL_STATUSES:
                return self._reject(
                    order,
                    f'OCO-заявка {oco.ref} уже завершена '
                    f'({oco.getstatusname()})',
                )
            self.ocos[order.ref] = oco.ref
            self.ocos[oco.ref] = order.ref

        if not transmit or parent is not None:
            parent_ref = parent.ref if parent is not None else order.ref
            if parent is not None and parent_ref not in self.pcs:
                return self._reject(order, 'Родительская заявка не зарегистрирована')
            self.pcs[parent_ref].append(order)

        if transmit:
            if parent is None:
                self.place_order(order)
            else:
                self.place_order(parent)
                # Contract: return this child, not the parent.
        return order

    def _reject(self, order, reason):
        order.addinfo(rejection_reason=reason)
        changed = order.status not in self._FINAL_STATUSES
        if changed:
            order.reject(self)
            self.notifs.append(order.clone())
        logger.error('Заявка %s отклонена: %s', order.ref, reason)
        self._terminal_order_actions(order)
        return order

    def place_order(self, order):
        if order.status != Order.Created:
            return order
        class_code, sec_code = order.data.class_code, order.data.sec_code
        try:
            quantity = self._order_quantity(order)
            transaction = self._build_transaction(order, quantity)
            order.submit(self)
            self.notifs.append(order.clone())
            response = self.store.provider.send_transaction(transaction)
            if response.get('cmd') == 'lua_transaction_error':
                return self._reject(order, str(response.get('lua_error') or response))
            return order
        except (ValueError, TypeError, KeyError) as exc:
            return self._reject(order, str(exc))
        except Exception as exc:
            logger.exception('Ошибка отправки заявки %s', order.ref)
            return self._reject(order, f'Ошибка связи/провайдера: {exc}')

    def _order_quantity(self, order):
        raw_size = abs(Decimal(str(order.size)))
        if raw_size != raw_size.to_integral_value() or raw_size <= 0:
            raise ValueError(f'Размер заявки должен быть положительным целым: {order.size}')
        size = int(raw_size)
        if order.data.derivative:
            return size
        return self.store.provider.size_to_lots(
            order.data.class_code, order.data.sec_code, size
        )

    def _build_transaction(self, order, quantity):
        class_code, sec_code = order.data.class_code, order.data.sec_code
        account = order.info['account']
        transaction = {
            'TRANS_ID': str(order.ref),
            'CLIENT_CODE': self.p.client_code_for_orders or account.get('client_code', ''),
            'ACCOUNT': account['trade_account_id'],
            'CLASSCODE': class_code,
            'SECCODE': sec_code,
            'OPERATION': 'B' if order.isbuy() else 'S',
            'QUANTITY': str(quantity),
            'ACTION': 'NEW_ORDER' if order.exectype in (Order.Market, Order.Limit) else 'NEW_STOP_ORDER',
        }
        step = order.info['min_price_step']
        slippage = step * self.p.slippage_steps
        if order.exectype == Order.Market:
            transaction['TYPE'] = 'M'
            if order.data.derivative:
                last = float(self.store.provider.get_param_ex(
                    class_code, sec_code, 'LAST'
                )['data']['param_value'])
                target = last + slippage if order.isbuy() else last - slippage
                market_price = self.store.provider.price_to_valid_price(
                    class_code, sec_code, target,
                    rounding='ceil' if order.isbuy() else 'floor',
                )
            else:
                market_price = 0
            transaction['PRICE'] = str(market_price)
        elif order.exectype == Order.Limit:
            transaction['TYPE'] = 'L'
            transaction['PRICE'] = str(self._to_quik_order_price(order, order.price))
        elif order.exectype == Order.Stop:
            stop = self._to_quik_order_price(order, order.price)
            transaction['STOPPRICE'] = str(stop)
            if order.data.derivative:
                target = float(stop) + slippage if order.isbuy() else float(stop) - slippage
                market = self.store.provider.price_to_valid_price(
                    class_code, sec_code, target,
                    rounding='ceil' if order.isbuy() else 'floor',
                )
            else:
                target = float(order.price) + slippage if order.isbuy() else float(order.price) - slippage
                valid = self.store.provider.price_to_valid_price(
                    class_code, sec_code, target,
                    rounding='ceil' if order.isbuy() else 'floor',
                )
                market = self.store.provider.price_to_quik_price(
                    class_code, sec_code, valid
                )
            transaction['PRICE'] = str(market)
        elif order.exectype == Order.StopLimit:
            transaction['STOPPRICE'] = str(self._to_quik_order_price(order, order.price))
            transaction['PRICE'] = str(self._to_quik_order_price(order, order.pricelimit))
        else:
            raise ValueError(f'Неподдерживаемый тип заявки: {order.exectype}')

        if order.exectype in (Order.Stop, Order.StopLimit):
            transaction['EXPIRY_DATE'] = self._format_expiry(order)
        return transaction

    def _to_quik_order_price(self, order, price):
        class_code, sec_code = order.data.class_code, order.data.sec_code
        if order.data.derivative:
            return self.store.provider.price_to_valid_price(
                class_code, sec_code, price, rounding='nearest'
            )
        return self.store.provider.price_to_quik_price(class_code, sec_code, price)

    def _format_expiry(self, order):
        original = order.info.get('original_valid')
        if original is None:
            return 'GTC'
        if original == Order.DAY or original == 0:
            return 'TODAY'
        if isinstance(original, datetime):
            return original.strftime('%Y%m%d')
        if isinstance(original, date):
            return original.strftime('%Y%m%d')
        if isinstance(original, timedelta):
            return num2date(order.valid).strftime('%Y%m%d')
        if isinstance(order.valid, (int, float)) and order.valid:
            return num2date(order.valid).strftime('%Y%m%d')
        return 'GTC'

    # Cancellation / OCO / bracket ---------------------------------------

    def cancel_order(self, order):
        if order is None or not order.alive():
            return order
        if order.info.get('cancel_requested') or order.info.get('cancel_inflight'):
            return order
        if order.status == Order.Created:
            order.cancel()
            self.notifs.append(order.clone())
            self._terminal_order_actions(order)
            return order
        order_num = order.info.get('order_num')
        if not order_num:
            order.addinfo(cancel_requested=True)
            return order
        return self._send_cancel(order)

    def _send_cancel(self, order):
        order_num = order.info.get('order_num')
        if not order_num or not order.alive():
            return order
        stop_order = False
        if order.exectype in (Order.Stop, Order.StopLimit):
            try:
                stop_order = isinstance(
                    self.store.provider.get_order_by_number(order_num).get('data'),
                    int,
                )
            except Exception:
                logger.exception('Не удалось определить вид заявки %s при отмене', order.ref)
        transaction = {
            'TRANS_ID': str(order.ref),
            'CLASSCODE': order.data.class_code,
            'SECCODE': order.data.sec_code,
        }
        if stop_order:
            transaction.update(ACTION='KILL_STOP_ORDER', STOP_ORDER_KEY=str(order_num))
        else:
            transaction.update(ACTION='KILL_ORDER', ORDER_KEY=str(order_num))
        order.addinfo(cancel_inflight=True, cancel_requested=False)
        try:
            response = self.store.provider.send_transaction(transaction)
            if response.get('cmd') == 'lua_transaction_error':
                raise RuntimeError(str(response.get('lua_error') or response))
        except Exception as exc:
            order.addinfo(cancel_inflight=False, cancel_error=str(exc))
            logger.exception('Ошибка отправки отмены заявки %s', order.ref)
            self.store.put_notification('ORDER_CANCEL_SEND_ERROR', order.ref, str(exc))
        return order

    def _terminal_order_actions(self, order):
        if order.status not in self._FINAL_STATUSES:
            return
        paired_ref = self.ocos.pop(order.ref, None)
        if paired_ref is not None:
            self.ocos.pop(paired_ref, None)
            paired = self.orders.get(paired_ref)
            if paired is not None and paired.alive():
                self.cancel_order(paired)

        if order.parent is None and order.ref in self.pcs:
            children = list(self.pcs[order.ref])
            if order.status == Order.Completed:
                for child in children:
                    if child.parent is not None and child.status == Order.Created:
                        self.place_order(child)
            else:
                for child in children:
                    if child.parent is not None and child.status == Order.Created:
                        child.cancel()
                        self.notifs.append(child.clone())
        elif order.parent is not None:
            siblings = self.pcs.get(order.parent.ref, ())
            for sibling in siblings:
                if sibling.parent is not None and sibling.ref != order.ref and sibling.alive():
                    self.cancel_order(sibling)

    # Transaction and execution events ----------------------------------

    def _handle_trans_reply(self, data):
        reply = data.get('data') or {}
        trans_id = int(reply.get('trans_id') or 0)
        if not trans_id or trans_id not in self.orders:
            return
        order = self.orders[trans_id]
        order_num = int(reply.get('order_num') or 0)
        if order_num:
            order.addinfo(order_num=order_num)
        status = int(reply.get('status') or 0)

        if order.status in self._FINAL_STATUSES:
            return
        changed = False
        if order.info.get('cancel_inflight'):
            if status == 3:
                order.cancel()
                order.addinfo(cancel_inflight=False)
                changed = True
            elif status in self._TRANS_ERROR_STATUSES:
                # A failed cancel must not reject the still-live exchange order.
                order.addinfo(
                    cancel_inflight=False,
                    cancel_error=reply.get('result_msg'),
                )
                logger.error(
                    'Не удалось отменить заявку %s: %s',
                    order.ref,
                    reply.get('result_msg'),
                )
                return
            elif status in (0, 1, 15):
                return
            else:
                logger.warning(
                    'Неизвестный статус ответа на отмену=%s для заявки %s: %r',
                    status, order.ref, reply,
                )
                return
        elif status in (3, 15):
            if order.status in (Order.Created, Order.Submitted):
                order.accept(self)
                changed = True
        elif status == 6:
            order.margin()
            changed = True
        elif status in self._TRANS_ERROR_STATUSES:
            order.reject(self)
            changed = True
        elif status in (0, 1):
            return
        else:
            logger.warning('Неизвестный статус OnTransReply=%s для заявки %s: %r', status, order.ref, reply)
            return

        if changed:
            self.notifs.append(order.clone())
        if order.status == Order.Accepted and order.info.get('cancel_requested'):
            self._send_cancel(order)
        if order.status in self._FINAL_STATUSES:
            self._terminal_order_actions(order)

    def _handle_trade(self, data):
        self._cash_snapshot_valid = False
        trade = data.get('data') or {}
        trans_id = int(trade.get('trans_id') or 0)
        if not trans_id or trans_id not in self.orders:
            return
        if not self._trade_matches_account(trade):
            return
        order = self.orders[trans_id]
        if order.status in self._FINAL_STATUSES and order.status != Order.Completed:
            logger.warning('Сделка пришла для финальной заявки %s (%s)', order.ref, order.getstatusname())
            return

        trade_num = int(trade['trade_num'])
        order_num = int(trade.get('order_num') or 0)
        seen_key = (self.account_id, trade_num, order_num, str(trade.get('datetime') or trade.get('date') or ''))
        if seen_key in self._seen_trades:
            return
        self._seen_trades[seen_key] = None
        while len(self._seen_trades) > self.p.max_seen_trades:
            self._seen_trades.popitem(last=False)

        if order_num:
            order.addinfo(order_num=order_num)
        class_code = trade['class_code']
        sec_code = trade['sec_code']
        size = int(trade['qty'])
        if not order.data.derivative:
            size = self.store.provider.lots_to_size(class_code, sec_code, size)
        if int(trade.get('flags') or 0) & 0b100:
            size *= -1
        if (order.isbuy() and size < 0) or (order.issell() and size > 0):
            logger.error(
                'Направление сделки QUIK (%s) не совпадает с заявкой %s; '
                'событие не применено', size, order.ref
            )
            self.store.put_notification(
                'TRADE_DIRECTION_MISMATCH', order.ref, dict(trade)
            )
            return
        remsize = order.executed.remsize
        if abs(size) > abs(remsize):
            logger.error('Исполнение %s превышает остаток %s заявки %s; обрезаем до остатка', size, remsize, order.ref)
            order.addinfo(reconciliation_required=True)
            self.store.put_notification(
                'TRADE_OVERFILL_MISMATCH', order.ref, size, remsize, dict(trade)
            )
            size = int(remsize)
        if not size:
            return

        raw_price = float(trade['price'])
        price = raw_price if class_code == self.store.provider.futures_cls_code else self.store.provider.quik_price_to_price(class_code, sec_code, raw_price)
        dt = self._trade_datetime_num(trade, order)

        position = self.getposition(order.data)
        old_price = position.price
        psize, pprice, opened, closed = position.update(size, price)
        comminfo = order.comminfo or self.getcommissioninfo(order.data)
        closedvalue = comminfo.getoperationcost(closed, old_price) if closed else 0.0
        openedvalue = comminfo.getoperationcost(opened, price) if opened else 0.0
        total_commission = self._trade_commission(trade, comminfo, size, price)
        total_abs = abs(closed) + abs(opened)
        closedcomm = total_commission * abs(closed) / total_abs if closed and total_abs else 0.0
        openedcomm = total_commission * abs(opened) / total_abs if opened and total_abs else 0.0
        pnl = comminfo.profitandloss(-closed, old_price, price) if closed else 0.0

        order.execute(
            dt, size, price,
            closed, closedvalue, closedcomm,
            opened, openedvalue, openedcomm,
            comminfo.margin, pnl,
            psize, pprice,
        )
        self.notifs.append(order.clone())
        if order.status == Order.Completed:
            self._terminal_order_actions(order)

    def _trade_matches_account(self, trade):
        if self.account.get('futures'):
            trade_account = self.account.get('trade_account_id')
            for field in ('account', 'trdaccid'):
                value = trade.get(field)
                if value not in (None, '', trade_account):
                    return False
            if trade.get('firmid') not in (
                None, '', self.account.get('firm_id')
            ):
                return False
            # QUIK Junior can put the futures trade account into client_code
            # while getTradeAccounts reports an empty client code.
            return trade.get('client_code') in (
                None, '', self.account.get('client_code'), trade_account,
            )
        fields = {
            'account': self.account.get('trade_account_id'),
            'trdaccid': self.account.get('trade_account_id'),
            'client_code': self.account.get('client_code'),
            'firmid': self.account.get('firm_id'),
        }
        for field, expected in fields.items():
            value = trade.get(field)
            if value not in (None, '', expected):
                return False
        return True

    def _trade_commission(self, trade, comminfo, size, price):
        for key in ('broker_commission', 'commission', 'broker_comission'):
            value = trade.get(key)
            if value not in (None, ''):
                try:
                    return abs(float(value))
                except (TypeError, ValueError):
                    logger.warning('Некорректная комиссия в OnTrade: %r', value)
        return abs(float(comminfo.getcommission(size, price)))

    def _trade_datetime_num(self, trade, order):
        value = trade.get('datetime')
        if isinstance(value, dict):
            try:
                dt = datetime(
                    int(value['year']), int(value['month']), int(value['day']),
                    int(value.get('hour', 0)), int(value.get('min', 0)),
                    int(value.get('sec', 0)),
                )
                return date2num(dt)
            except (KeyError, TypeError, ValueError):
                logger.warning('Не удалось разобрать datetime сделки: %r', value)
        try:
            current = order.data.datetime[0]
            if current:
                return current
        except (AttributeError, IndexError, TypeError):
            pass
        now = datetime.now(self.store.provider.tz_msk).replace(tzinfo=None)
        return date2num(now)

    # Validation and instrument helpers ----------------------------------

    def check_data_names(self, data_name):
        if not isinstance(data_name, str):
            raise TypeError(f'Имя источника данных должно быть строкой: {data_name!r}')
        try:
            class_code, sec_code = data_name.split('.', 1)
        except ValueError as exc:
            raise ValueError(f'Ожидается имя <CLASS.SEC>, получено {data_name!r}') from exc
        classes = getattr(self.store.provider, 'classes', {})
        if class_code not in classes or sec_code not in classes[class_code]:
            raise ValueError(f'Market data {data_name} не найдены в QUIK')
        symbol_info = self.store.provider.get_symbol_info(class_code, sec_code)
        if not symbol_info:
            raise ValueError(f'QUIK не вернул спецификацию {data_name}')
        if not self._class_allowed(class_code):
            raise ValueError(
                f'Данные {data_name} доступны, но торговля ими недоступна '
                f'на account_id={self.account_id}'
            )
        logger.info('Инструмент %s проверен для account_id=%s', data_name, self.account_id)
        return symbol_info

    def get_price_step(self, cls, sec):
        return float(self.store.provider.get_param_ex(cls, sec, 'SEC_PRICE_STEP')['data']['param_value'])

    def get_cost_of_price_step(self, cls, sec):
        return float(self.store.provider.get_param_ex(cls, sec, 'STEPPRICE')['data']['param_value'])

    def get_buyer_go(self, cls, sec):
        return float(self.store.provider.get_param_ex(cls, sec, 'BUYDEPO')['data']['param_value'])

    def get_bayer_go(self, cls, sec):
        """Deprecated typo-compatible alias."""
        return self.get_buyer_go(cls, sec)

    def get_seller_go(self, cls, sec):
        return float(self.store.provider.get_param_ex(cls, sec, 'SELLDEPO')['data']['param_value'])
