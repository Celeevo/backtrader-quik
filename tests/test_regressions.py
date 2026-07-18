from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import ast
import logging
from threading import Lock

import backtrader as bt
import backtrader_quik as btq_package
import pandas as pd
import pytest

from backtrader_quik import (
    Event, QKStore, QuikPy, quik_connector_path,
    set_console_logging, set_file_logging,
)
from backtrader.position import Position


class FakeProvider:
    futures_cls_code = 'SPBFUT'
    currency = 'SUR'
    tz_msk = datetime.now().astimezone().tzinfo

    def __init__(self, accounts=None, auto_fill=False):
        self.on_trans_reply = Event()
        self.on_trade = Event()
        self.on_connected = Event()
        self.on_disconnected = Event()
        self.on_new_candle = Event()
        self.auto_fill = auto_fill
        self._trade_num = 1000
        self.sent = []
        self.subscribe_calls = 0
        self.unsubscribe_calls = 0
        self.closed = False
        self.accounts = accounts or [
            dict(account_id=0, client_code='C0', firm_id='F0',
                 trade_account_id='A0', class_codes=['TQBR'], futures=False),
        ]
        self.classes = {'TQBR': {'AAA'}, 'TQTF': {'BBB'}, 'SPBFUT': {'RI'}}

    def close_connection_and_thread(self):
        self.closed = True

    def get_money_limits(self):
        return {'data': [
            {'client_code': 'C0', 'firmid': 'F0', 'currcode': 'SUR',
             'limit_kind': 2, 'currentbal': '100'},
            {'client_code': 'C1', 'firmid': 'F1', 'currcode': 'SUR',
             'limit_kind': 2, 'currentbal': '200'},
        ]}

    def get_futures_limit(self, *_):
        return {'data': {'cbplimit': 10000, 'varmargin': 50, 'accruedint': 5}}

    def get_all_depo_limits(self):
        return {'data': []}

    def get_futures_holdings(self):
        return {'data': []}

    @staticmethod
    def dataname_to_class_sec_codes(dataname):
        if '.' not in dataname:
            return ('TQBR', dataname)
        return dataname.split('.', 1)

    @staticmethod
    def class_sec_codes_to_dataname(class_code, sec_code):
        return f'{class_code}.{sec_code}'

    @staticmethod
    def quik_price_to_price(_class_code, _sec_code, price):
        return float(price)

    @staticmethod
    def price_to_quik_price(_class_code, _sec_code, price):
        return float(price)

    @staticmethod
    def price_to_valid_price(_class_code, _sec_code, price, rounding='nearest'):
        return float(price)

    @staticmethod
    def lots_to_size(_class_code, _sec_code, lots):
        return int(lots) * 10

    @staticmethod
    def size_to_lots(_class_code, _sec_code, size):
        size = int(size)
        if size <= 0 or size % 10:
            raise ValueError('size must be a positive multiple of 10')
        return size // 10

    @staticmethod
    def get_symbol_info(_class_code, _sec_code):
        return {'lot_size': 10, 'min_price_step': 1, 'scale': 0}

    @staticmethod
    def get_param_ex(class_code, sec_code, param):
        prices = {('TQBR', 'AAA'): 10.0, ('TQTF', 'BBB'): 20.0,
                  ('SPBFUT', 'RI'): 100000.0}
        if param == 'LAST':
            value = prices[(class_code, sec_code)]
        else:
            value = 1
        return {'data': {'param_value': str(value)}}

    def is_subscribed(self, *_):
        return {'data': self.subscribe_calls > self.unsubscribe_calls}

    def subscribe_to_candles(self, *_):
        self.subscribe_calls += 1
        return {'data': True}

    def unsubscribe_from_candles(self, *_):
        self.unsubscribe_calls += 1
        return {'data': True}

    def get_order_by_number(self, _):
        return {'data': {}}

    def send_transaction(self, transaction):
        self.sent.append(dict(transaction))
        if self.auto_fill and transaction['ACTION'] == 'NEW_ORDER':
            trans_id = int(transaction['TRANS_ID'])
            order_num = trans_id + 10000
            self.on_trans_reply.trigger({'data': {
                'trans_id': trans_id, 'order_num': order_num,
                'status': 15, 'result_msg': 'accepted',
            }})
            self._trade_num += 1
            is_sell = transaction['OPERATION'] == 'S'
            self.on_trade.trigger({'data': {
                'trade_num': self._trade_num,
                'order_num': order_num,
                'trans_id': trans_id,
                'class_code': transaction['CLASSCODE'],
                'sec_code': transaction['SECCODE'],
                'qty': int(transaction['QUANTITY']),
                'flags': 4 if is_sell else 0,
                'price': 110 if is_sell else 100,
                'account': transaction['ACCOUNT'],
                'client_code': transaction['CLIENT_CODE'],
                'firmid': 'F0',
                'datetime': {'year': 2026, 'month': 7, 'day': 12,
                             'hour': 10, 'min': 0, 'sec': self._trade_num % 60},
            }})
        return {'cmd': 'lua_transaction_reply', 'data': transaction}


def reset_store():
    QKStore.reset(close=False)


def test_packaged_quik_connector_is_available():
    connector = quik_connector_path()
    assert connector.joinpath('lua', 'QuikSharp.lua').is_file()
    assert connector.joinpath('socket', 'core.dll').is_file()


def test_account_scope_cash_and_equity_zero_safe():
    accounts = [
        dict(account_id=0, client_code='C0', firm_id='F0', trade_account_id='A0', class_codes=['TQBR'], futures=False),
        dict(account_id=1, client_code='C1', firm_id='F1', trade_account_id='A1', class_codes=['TQTF'], futures=False),
    ]
    provider = FakeProvider(accounts)
    reset_store()
    broker = QKStore(provider=provider).getbroker(account_id=1)
    broker.positions['TQTF.BBB'] = Position(1, 19)
    assert broker.getcash() == 200
    assert broker.getvalue() == 220

    provider.get_money_limits = lambda: {'data': [
        {'client_code': 'C1', 'firmid': 'F1', 'currcode': 'SUR',
         'limit_kind': 2, 'currentbal': '0'}
    ]}
    broker.positions.clear()
    assert broker.getcash() == 0
    assert broker.getvalue() == 0
    with pytest.raises(ValueError):
        broker.getcash(account_id=0)


def test_short_position_value_keeps_negative_sign():
    reset_store()
    broker = QKStore(provider=FakeProvider()).getbroker(account_id=0)
    broker.positions['TQBR.AAA'] = Position(-10, 12)
    # Cash returned by the fake provider is 100. A short liability at LAST=10
    # must reduce equity by 100, not add another positive 100.
    assert broker.getvalue() == pytest.approx(0.0)


def test_ambiguous_spot_client_code_requires_override():
    accounts = [dict(
        account_id=0, client_code='', client_code_candidates=['C0', 'C1'],
        firm_id='F0', trade_account_id='A0', class_codes=['TQBR'],
        futures=False,
    )]
    reset_store()
    store = QKStore(provider=FakeProvider(accounts))
    with pytest.raises(ValueError, match='client_code'):
        store.getbroker(account_id=0)

    reset_store()
    store = QKStore(provider=FakeProvider(accounts))
    broker = store.getbroker(account_id=0, client_code='C1')
    assert broker.account['client_code'] == 'C1'


def test_store_returns_same_broker_and_rejects_second_config():
    reset_store()
    store = QKStore(provider=FakeProvider())
    first = store.getbroker(account_id=0)
    assert first is store.getbroker(account_id=0)
    with pytest.raises(RuntimeError):
        store.getbroker(account_id=1)


def test_bar_fanout_and_subscription_refcount():
    reset_store()
    provider = FakeProvider()
    store = QKStore(provider=provider)
    guid = ('TQBR', 'AAA', 1)
    q1 = store.register_bar_consumer(guid, 'one', provider_subscription=True)
    q2 = store.register_bar_consumer(guid, 'two', provider_subscription=True)
    assert provider.subscribe_calls == 1
    bar = {'datetime': datetime(2026, 7, 12, 10), 'open': 1, 'high': 2,
           'low': 1, 'close': 2, 'volume': 1}
    assert store.publish_bar(guid, bar) == 2
    assert q1.get_nowait() == bar
    assert q2.get_nowait() == bar
    store.unregister_bar_consumer(guid, 'one')
    assert provider.unsubscribe_calls == 0
    store.unregister_bar_consumer(guid, 'two')
    assert provider.unsubscribe_calls == 1


def test_size_to_lots_is_strict():
    qp = QuikPy.__new__(QuikPy)
    qp.get_symbol_info = lambda *_: {'lot_size': 10}
    assert qp.size_to_lots('TQBR', 'AAA', 20) == 2
    with pytest.raises(ValueError):
        qp.size_to_lots('TQBR', 'AAA', 5)
    with pytest.raises(ValueError):
        qp.size_to_lots('TQBR', 'AAA', 0)


class ExplodingSocket:
    def sendall(self, _):
        raise ConnectionError('synthetic send failure')


def test_candle_subscription_registry_uses_data_boolean():
    qp = QuikPy.__new__(QuikPy)
    qp.subscriptions = []
    qp.process_request = lambda request: {'data': True}
    state = {'subscribed': False}
    qp.is_subscribed = lambda *args, **kwargs: {'data': state['subscribed']}

    qp.subscribe_to_candles('TQBR', 'AAA', 1)
    assert qp.subscriptions == []
    state['subscribed'] = True
    qp.subscribe_to_candles('TQBR', 'AAA', 1)
    assert len(qp.subscriptions) == 1
    qp.subscribe_to_candles('TQBR', 'AAA', 1)
    assert len(qp.subscriptions) == 1
    state['subscribed'] = False
    qp.unsubscribe_from_candles('TQBR', 'AAA', 1)
    assert qp.subscriptions == []


def test_process_request_releases_lock_after_error():
    qp = QuikPy.__new__(QuikPy)
    qp.lock = Lock()
    qp.socket_requests = ExplodingSocket()
    qp.healthy = True
    qp.last_error = None
    qp._close_socket = lambda _: None
    with pytest.raises(ConnectionError):
        qp.process_request({'cmd': 'ping'})
    assert not qp.lock.locked()


def test_event_is_ordered_and_isolates_exceptions():
    event = Event()
    called = []

    def bad():
        called.append('bad')
        raise RuntimeError('boom')

    def good():
        called.append('good')

    event.subscribe(bad)
    event.subscribe(good)
    event.trigger()
    assert called == ['bad', 'good']


def make_data():
    df = pd.DataFrame({
        'open': [100, 101, 102, 103],
        'high': [101, 102, 103, 104],
        'low': [99, 100, 101, 102],
        'close': [100, 101, 102, 103],
        'volume': [1000, 1000, 1000, 1000],
    }, index=pd.to_datetime(['2026-07-12 10:00', '2026-07-12 10:01',
                             '2026-07-12 10:02', '2026-07-12 10:03']))
    data = bt.feeds.PandasData(dataname=df)
    data.class_code = 'TQBR'
    data.sec_code = 'AAA'
    data.derivative = False
    return data


def run_strategy(strategy_cls, provider=None, commission=None):
    provider = provider or FakeProvider()
    reset_store()
    store = QKStore(provider=provider)
    broker = store.getbroker(account_id=0)
    cerebro = bt.Cerebro(stdstats=False, quicknotify=True)
    cerebro.setbroker(broker)
    if commission is not None:
        broker.setcommission(commission=commission)
    cerebro.adddata(make_data(), name='TQBR.AAA')
    cerebro.addstrategy(strategy_cls)
    return cerebro.run()[0], broker, provider


def test_tradeid_and_invalid_lot_rejection():
    class Strategy(bt.Strategy):
        def __init__(self):
            self.orders = []

        def next(self):
            if not self.orders:
                self.orders.append(self.buy(size=10, tradeid=77))
                self.orders.append(self.buy(size=5, tradeid=88))

    strategy, _, provider = run_strategy(Strategy)
    valid, invalid = strategy.orders
    assert valid.tradeid == 77
    assert invalid.tradeid == 88
    assert invalid.status == invalid.Rejected
    assert 'multiple of 10' in invalid.info.rejection_reason
    assert len([t for t in provider.sent if t['ACTION'] == 'NEW_ORDER']) == 1


def test_new_oco_order_is_rejected_if_peer_already_terminal():
    class Strategy(bt.Strategy):
        def __init__(self):
            self.orders = []

        def next(self):
            if not self.orders:
                invalid = self.buy(size=5)
                linked = self.sell(size=10, oco=invalid)
                self.orders = [invalid, linked]

    strategy, _, provider = run_strategy(Strategy)
    invalid, linked = strategy.orders
    assert invalid.status == invalid.Rejected
    assert linked.status == linked.Rejected
    assert not [t for t in provider.sent if t['ACTION'] == 'NEW_ORDER']


def test_bracket_returns_three_distinct_orders():
    class Strategy(bt.Strategy):
        def __init__(self):
            self.bracket = None

        def next(self):
            if self.bracket is None:
                self.bracket = self.buy_bracket(
                    size=10, price=100, stopprice=90, limitprice=110
                )

    strategy, _, _ = run_strategy(Strategy)
    assert len(strategy.bracket) == 3
    assert len({order.ref for order in strategy.bracket}) == 3
    assert strategy.bracket[1].parent is strategy.bracket[0]
    assert strategy.bracket[2].parent is strategy.bracket[0]


def test_full_fill_value_commission_and_pnlcomm():
    class Strategy(bt.Strategy):
        def __init__(self):
            self.pending = None
            self.closed_trade = None
            self.completed_orders = []

        def notify_order(self, order):
            if order.status == order.Completed:
                self.completed_orders.append(order)
                self.pending = None

        def notify_trade(self, trade):
            if trade.isclosed:
                self.closed_trade = trade

        def next(self):
            if self.pending:
                return
            if not self.position and not self.completed_orders:
                self.pending = self.buy(size=10, tradeid=9)
            elif self.position and len(self.completed_orders) == 1:
                self.pending = self.sell(size=10, tradeid=9)

    provider = FakeProvider(auto_fill=True)
    strategy, broker, _ = run_strategy(Strategy, provider, commission=0.001)
    assert len(strategy.completed_orders) == 2
    buy, sell = strategy.completed_orders
    assert buy.executed.value == pytest.approx(1000.0)
    assert buy.executed.comm == pytest.approx(1.0)
    assert sell.executed.value == pytest.approx(1000.0)  # closed value uses entry cost
    assert strategy.closed_trade is not None
    assert strategy.closed_trade.pnl == pytest.approx(100.0)
    assert strategy.closed_trade.pnlcomm == pytest.approx(97.9)
    assert broker.getposition(strategy.data).size == 0


def test_failed_cancel_clears_inflight_without_rejecting_live_order():
    class Strategy(bt.Strategy):
        def __init__(self):
            self.order = None

        def next(self):
            if self.order is None:
                self.order = self.buy(size=10)

    strategy, broker, _ = run_strategy(Strategy)
    order = strategy.order
    broker.on_trans_reply({'data': {
        'trans_id': order.ref, 'order_num': 12345, 'status': 15,
        'result_msg': 'accepted',
    }})
    broker.process_pending_events()
    broker.cancel(order)
    assert order.info.cancel_inflight
    broker.on_trans_reply({'data': {
        'trans_id': order.ref, 'order_num': 12345, 'status': 5,
        'result_msg': 'cancel rejected',
    }})
    broker.process_pending_events()
    assert order.status == order.Accepted
    assert not order.info.cancel_inflight
    assert order.info.cancel_error == 'cancel rejected'


def test_cancel_before_exchange_order_number_is_deferred():
    class Strategy(bt.Strategy):
        def __init__(self):
            self.order = None

        def next(self):
            if self.order is None:
                self.order = self.buy(size=10)
                self.cancel(self.order)

    strategy, broker, provider = run_strategy(Strategy)
    assert strategy.order.info.cancel_requested
    broker.on_trans_reply({'data': {
        'trans_id': strategy.order.ref, 'order_num': 12345,
        'status': 15, 'result_msg': 'accepted',
    }})
    broker.process_pending_events()
    kills = [item for item in provider.sent if item['ACTION'] == 'KILL_ORDER']
    assert len(kills) == 1
    assert kills[0]['ORDER_KEY'] == '12345'



def test_cash_snapshot_is_reused_only_by_getvalue():
    class CountingProvider(FakeProvider):
        def __init__(self):
            super().__init__()
            self.money_calls = 0

        def get_money_limits(self):
            self.money_calls += 1
            return super().get_money_limits()

    provider = CountingProvider()
    reset_store()
    broker = QKStore(provider=provider).getbroker(
        account_id=0, account_snapshot_reuse_window=10.0
    )
    assert broker.getcash() == 100
    assert provider.money_calls == 1
    assert broker.getvalue() == 100
    assert provider.money_calls == 1  # getvalue reused the immediately prior cash
    assert broker.getcash() == 100
    assert provider.money_calls == 2  # explicit getcash remains a refresh


def test_position_value_prefers_current_data_close_over_sync_last():
    class CountingProvider(FakeProvider):
        def __init__(self):
            super().__init__()
            self.last_calls = 0

        def get_param_ex(self, class_code, sec_code, param):
            if param == 'LAST':
                self.last_calls += 1
            return super().get_param_ex(class_code, sec_code, param)

    class CloseLine:
        def __getitem__(self, index):
            assert index == 0
            return 12.5

    class DummyData:
        _name = 'TQBR.AAA'
        close = CloseLine()

    provider = CountingProvider()
    reset_store()
    broker = QKStore(provider=provider).getbroker(account_id=0)
    data = DummyData()
    broker._datas = [data]
    broker.positions[data._name] = Position(10, 10)
    assert broker.getvalue() == pytest.approx(225.0)
    assert provider.last_calls == 0


def test_external_candle_subscription_is_not_removed_by_store():
    class ExternalSubscriptionProvider(FakeProvider):
        def is_subscribed(self, *_):
            return {'data': True}

    provider = ExternalSubscriptionProvider()
    reset_store()
    store = QKStore(provider=provider)
    guid = ('TQBR', 'AAA', 1)
    store.register_bar_consumer(guid, 'external-user', provider_subscription=True)
    store.unregister_bar_consumer(guid, 'external-user')
    assert provider.subscribe_calls == 0
    assert provider.unsubscribe_calls == 0


def test_dated_valid_is_rejected_for_regular_order():
    class Strategy(bt.Strategy):
        def __init__(self):
            self.order = None

        def next(self):
            if self.order is None:
                self.order = self.buy(
                    size=10,
                    exectype=bt.Order.Limit,
                    price=100,
                    valid=datetime(2026, 7, 13),
                )

    strategy, _, provider = run_strategy(Strategy)
    assert strategy.order.status == strategy.order.Rejected
    assert 'датированную valid' in strategy.order.info.rejection_reason
    assert not provider.sent


def test_logger_compatibility_api_is_lazy_and_owned(tmp_path):
    set_console_logging(False)
    set_file_logging(False)
    log_path = set_file_logging(True, logs_dir=tmp_path, level='INFO')
    assert log_path == (tmp_path / 'app.log').resolve()
    assert log_path.exists()
    # Disabling our handler must not remove arbitrary handlers owned by users.
    package_logger = logging.getLogger('backtrader_quik')
    user_handler = logging.StreamHandler()
    package_logger.addHandler(user_handler)
    try:
        set_file_logging(False, logs_dir=tmp_path)
        assert user_handler in package_logger.handlers
    finally:
        package_logger.removeHandler(user_handler)
    assert set_console_logging(True, level='WARNING') is not None
    assert set_console_logging(False) is None


def test_history_reader_skips_malformed_rows(tmp_path):
    provider = FakeProvider()
    reset_store()
    store = QKStore(provider=provider)
    data = store.getdata(
        dataname='TQBR.AAA', timeframe=bt.TimeFrame.Minutes,
        compression=1, datapath=tmp_path, save_bars=False,
    )
    path = Path(data.file_name)
    path.write_text(
        'datetime\topen\thigh\tlow\tclose\tvolume\n'
        'broken\trow\n'
        '01.01.2020 10:00\t10\t11\t9\t10.5\t100\n',
        encoding='utf-8',
    )
    data.get_bars_from_file()
    assert len(data.history_bars) == 1
    assert data.history_bars[0]['close'] == 10.5


def test_sources_parse_with_python_310_grammar():
    package_dir = Path(btq_package.__file__).resolve().parent
    source_paths = sorted(package_dir.glob('*.py'))
    assert source_paths, package_dir
    for source_path in source_paths:
        source = source_path.read_text(encoding='utf-8')
        ast.parse(source, filename=str(source_path), feature_version=(3, 10))
    qkdata = (package_dir / 'QKData.py').read_text(encoding='utf-8')
    assert 'datetime, timedelta, time, UTC' not in qkdata


def test_broker_parameter_validation():
    reset_store()
    store = QKStore(provider=FakeProvider())
    with pytest.raises(ValueError, match='slippage_steps'):
        store.getbroker(account_id=0, slippage_steps=-1)


def test_stock_stop_uses_positive_protective_price():
    class Strategy(bt.Strategy):
        def __init__(self):
            self.created = False

        def next(self):
            if not self.created:
                self.created = True
                self.buy(size=10, exectype=bt.Order.Stop, price=105)

    _, _, provider = run_strategy(Strategy)
    transaction = next(item for item in provider.sent if item['ACTION'] == 'NEW_STOP_ORDER')
    assert float(transaction['STOPPRICE']) == pytest.approx(105.0)
    assert float(transaction['PRICE']) > 0
    assert float(transaction['PRICE']) >= float(transaction['STOPPRICE'])


def test_futures_trade_accepts_trade_account_in_client_code():
    accounts = [dict(
        account_id=0, client_code='', firm_id='F0',
        trade_account_id='A0', class_codes=['SPBFUT'], futures=True,
    )]
    reset_store()
    broker = QKStore(provider=FakeProvider(accounts)).getbroker(account_id=0)
    assert broker._trade_matches_account({
        'account': 'A0', 'firmid': 'F0', 'client_code': 'A0',
    })
    assert not broker._trade_matches_account({
        'account': 'A0', 'firmid': 'OTHER', 'client_code': 'A0',
    })


def test_market_order_poll_recovers_missing_ontrade_callback():
    class PollingProvider(FakeProvider):
        def send_transaction(self, transaction):
            self.sent.append(dict(transaction))
            trans_id = int(transaction['TRANS_ID'])
            self.order_num = trans_id + 5000
            self.on_trans_reply.trigger({'data': {
                'trans_id': trans_id,
                'order_num': self.order_num,
                'status': 15,
                'result_msg': 'accepted',
            }})
            return {'cmd': 'lua_transaction_reply', 'data': transaction}

        def get_trades_by_order_number(self, order_num):
            assert int(order_num) == self.order_num
            transaction = self.sent[-1]
            return {'data': [{
                'trade_num': 9001,
                'order_num': self.order_num,
                'trans_id': int(transaction['TRANS_ID']),
                'class_code': transaction['CLASSCODE'],
                'sec_code': transaction['SECCODE'],
                'qty': int(transaction['QUANTITY']),
                'flags': 0,
                'price': 100,
                'account': transaction['ACCOUNT'],
                'client_code': transaction['CLIENT_CODE'],
                'firmid': 'F0',
            }]}

    class Strategy(bt.Strategy):
        def __init__(self):
            self.order = None

        def next(self):
            if self.order is None:
                self.order = self.buy(size=10)

    strategy, _, _ = run_strategy(Strategy, provider=PollingProvider())
    assert strategy.order.status == bt.Order.Completed
    assert strategy.order.executed.size == 10


def test_stopped_broker_serves_cached_cash_and_value():
    provider = FakeProvider()
    reset_store()
    broker = QKStore(provider=provider).getbroker(account_id=0)
    broker.stop()
    cached_cash, cached_value = broker.cash, broker.value
    provider.get_money_limits = lambda: (_ for _ in ()).throw(
        RuntimeError('provider is closed')
    )
    assert broker.getcash() == cached_cash
    assert broker.getvalue() == cached_value


def test_store_stop_removes_only_owned_subscriptions():
    provider = FakeProvider()
    reset_store()
    store = QKStore(provider=provider)
    guid = ('TQBR', 'AAA', 1)
    store.register_bar_consumer(guid, 'owned', provider_subscription=True)
    assert provider.subscribe_calls == 1
    store.stop()
    assert provider.unsubscribe_calls == 1
    assert provider.closed


def test_save_bars_false_does_not_create_runtime_directory(tmp_path):
    provider = FakeProvider()
    reset_store()
    store = QKStore(provider=provider)
    target = tmp_path / 'not-created'
    data = store.getdata(
        dataname='TQBR.AAA', timeframe=bt.TimeFrame.Minutes,
        compression=1, datapath=target, save_bars=False,
    )
    assert Path(data.file_name).parent == target
    assert not target.exists()


def test_qkdata_stop_is_idempotent(tmp_path):
    provider = FakeProvider()
    reset_store()
    store = QKStore(provider=provider)
    data = store.getdata(
        dataname='TQBR.AAA', timeframe=bt.TimeFrame.Minutes,
        compression=1, datapath=tmp_path, save_bars=False,
        live_bars=False,
    )
    data.stop()
    data.stop()
    assert data._stopped


def test_qkdata_live_queue_advertises_data_and_has_nonzero_qcheck(tmp_path):
    from queue import Queue

    provider = FakeProvider()
    reset_store()
    store = QKStore(provider=provider)
    data = store.getdata(
        dataname='TQBR.AAA', timeframe=bt.TimeFrame.Minutes,
        compression=1, datapath=tmp_path, save_bars=False,
        live_bars=True,
    )
    data.bar_queue = Queue()
    assert data.p.qcheck == pytest.approx(0.25)
    assert not data.haslivedata()
    data.bar_queue.put({'dummy': True})
    assert data.haslivedata()


def test_futures_fill_uses_margin_value_and_fixed_commission():
    class Strategy(bt.Strategy):
        def __init__(self):
            self.pending = None
            self.completed = []
            self.closed_trade = None

        def notify_order(self, order):
            if order.status == order.Completed:
                self.completed.append(order)
                self.pending = None

        def notify_trade(self, trade):
            if trade.isclosed:
                self.closed_trade = trade

        def next(self):
            if self.pending:
                return
            if not self.position and not self.completed:
                self.pending = self.buy(size=1, tradeid=3)
            elif self.position and len(self.completed) == 1:
                self.pending = self.sell(size=1, tradeid=3)

    accounts = [dict(
        account_id=0, client_code='C0', firm_id='F0',
        trade_account_id='A0', class_codes=['SPBFUT'], futures=True,
    )]
    provider = FakeProvider(accounts=accounts, auto_fill=True)
    reset_store()
    store = QKStore(provider=provider)
    broker = store.getbroker(account_id=0)
    broker.setcommission(
        commission=2.0,
        margin=1000.0,
        mult=1.0,
        commtype=bt.CommInfoBase.COMM_FIXED,
        stocklike=False,
    )

    df = pd.DataFrame({
        'open': [100, 101, 102, 103],
        'high': [101, 102, 103, 104],
        'low': [99, 100, 101, 102],
        'close': [100, 101, 102, 103],
        'volume': [10, 10, 10, 10],
    }, index=pd.to_datetime([
        '2026-07-12 10:00', '2026-07-12 10:01',
        '2026-07-12 10:02', '2026-07-12 10:03',
    ]))
    data = bt.feeds.PandasData(dataname=df)
    data.class_code = 'SPBFUT'
    data.sec_code = 'RI'
    data.derivative = True

    cerebro = bt.Cerebro(stdstats=False, quicknotify=True)
    cerebro.setbroker(broker)
    cerebro.adddata(data, name='SPBFUT.RI')
    cerebro.addstrategy(Strategy)
    strategy = cerebro.run()[0]

    assert len(strategy.completed) == 2
    buy, sell = strategy.completed
    assert buy.executed.value == pytest.approx(1000.0)
    assert sell.executed.value == pytest.approx(1000.0)
    assert buy.executed.comm == pytest.approx(2.0)
    assert sell.executed.comm == pytest.approx(2.0)
    assert strategy.closed_trade.pnl == pytest.approx(10.0)
    assert strategy.closed_trade.pnlcomm == pytest.approx(6.0)


def test_live_subscription_is_registered_before_history_request(tmp_path):
    events = []

    class OrderedProvider(FakeProvider):
        def subscribe_to_candles(self, *args):
            events.append('subscribe')
            return super().subscribe_to_candles(*args)

    provider = OrderedProvider()
    reset_store()
    store = QKStore(provider=provider)
    data = store.getdata(
        dataname='TQBR.AAA', timeframe=bt.TimeFrame.Minutes,
        compression=1, datapath=tmp_path, save_bars=False,
        live_bars=True,
    )
    data.get_bars_from_file = lambda: events.append('file')
    data.get_bars_from_history = lambda: events.append('history')
    data.start()
    try:
        assert events.index('subscribe') < events.index('history')
    finally:
        data.stop()


def test_data_start_rolls_back_subscription_on_history_error(tmp_path):
    provider = FakeProvider()
    reset_store()
    store = QKStore(provider=provider)
    data = store.getdata(
        dataname='TQBR.AAA', timeframe=bt.TimeFrame.Minutes,
        compression=1, datapath=tmp_path, save_bars=False,
        live_bars=True,
    )
    data.get_bars_from_file = lambda: None

    def fail_history():
        raise RuntimeError('synthetic history failure')

    data.get_bars_from_history = fail_history
    with pytest.raises(RuntimeError, match='synthetic history failure'):
        data.start()
    assert provider.subscribe_calls == 1
    assert provider.unsubscribe_calls == 1
    assert data.guid is None
    assert data.bar_queue is None
