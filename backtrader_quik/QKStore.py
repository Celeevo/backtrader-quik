"""Backtrader store for QUIK.

The store owns the provider connection and fans out live bars to each data-feed
consumer. Provider callbacks never mutate Backtrader objects directly.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from queue import Empty, Queue
from threading import RLock
from typing import Any, Hashable

from backtrader.metabase import MetaParams
from backtrader.utils.py3 import with_metaclass

from .QuikPy import QuikPy
from .logger_config import logger


class MetaSingleton(MetaParams):
    """Singleton with explicit reset and provider consistency checks."""

    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        provider = kwargs.get('provider')
        if cls._singleton is None:
            cls._singleton = super().__call__(*args, **kwargs)
        elif provider is not None and provider is not cls._singleton.provider:
            raise RuntimeError(
                'QKStore уже создан с другим provider. Используйте '
                'QKStore.reset() после полной остановки предыдущего подключения.'
            )
        return cls._singleton


class QKStore(with_metaclass(MetaSingleton, object)):
    """QUIK store shared by one Cerebro run."""

    BrokerCls = None
    DataCls = None

    def __init__(self, provider=None):
        super().__init__()
        self.provider = provider if provider is not None else QuikPy()
        self._notifs: Queue[tuple[Any, tuple, dict]] = Queue()
        self._bar_lock = RLock()
        self._bar_consumers: dict[Hashable, Queue] = {}
        self._guid_consumers: dict[Hashable, set[Hashable]] = defaultdict(set)
        self._provider_subscription_guids: set[Hashable] = set()
        self._started = False
        self._stopped = False
        self._broker = None
        self._broker_config = None

    @classmethod
    def reset(cls, close: bool = True) -> None:
        """Reset singleton; intended for tests or a fully stopped application."""
        instance = cls._singleton
        if instance is not None and close:
            try:
                instance.stop()
            except Exception:
                logger.exception('Ошибка при остановке QKStore во время reset()')
        cls._singleton = None

    def getdata(self, *args, **kwargs):
        if self.DataCls is None:
            raise RuntimeError('QKData не зарегистрирован')
        return self.DataCls(*args, **kwargs)

    def getbroker(self, *args, **kwargs):
        """Return the single broker instance owned by this store."""
        if self.BrokerCls is None:
            raise RuntimeError('QKBroker не зарегистрирован')
        config = (args, tuple(sorted(kwargs.items(), key=lambda item: item[0])))
        if self._broker is None:
            self._broker = self.BrokerCls(*args, **kwargs)
            self._broker_config = config
        elif config != self._broker_config:
            raise RuntimeError(
                'Для одного QKStore допускается один QKBroker с неизменной '
                'конфигурацией. Создайте отдельный процесс для другого счета.'
            )
        return self._broker

    def start(self):
        if self._started:
            return
        if self._stopped:
            raise RuntimeError(
                'Остановленный QKStore нельзя запускать повторно: provider уже '
                'закрыт. Создайте новое подключение через QKStore.reset().'
            )
        self.provider.on_connected.subscribe(self.on_connection_event)
        self.provider.on_disconnected.subscribe(self.on_connection_event)
        self.provider.on_new_candle.subscribe(self.on_new_candle)
        self._started = True
        self._stopped = False

    def put_notification(self, msg, *args, **kwargs):
        self._notifs.put((msg, args, kwargs))

    def get_notifications(self):
        notifications = []
        while True:
            try:
                notifications.append(self._notifs.get_nowait())
            except Empty:
                return notifications

    def stop(self):
        if self._stopped:
            return
        if self._started:
            self.provider.on_connected.unsubscribe(self.on_connection_event)
            self.provider.on_disconnected.unsubscribe(self.on_connection_event)
            self.provider.on_new_candle.unsubscribe(self.on_new_candle)
        self._started = False
        self._stopped = True
        with self._bar_lock:
            owned_subscriptions = tuple(self._provider_subscription_guids)
            self._bar_consumers.clear()
            self._guid_consumers.clear()
            self._provider_subscription_guids.clear()
        for guid in owned_subscriptions:
            try:
                class_code, sec_code, interval = guid
                self.provider.unsubscribe_from_candles(
                    class_code, sec_code, interval
                )
            except Exception:
                logger.exception(
                    'Ошибка снятия принадлежащей QKStore подписки %r', guid
                )
        self.provider.close_connection_and_thread()

    def on_connection_event(self, data):
        """Provider callback: enqueue only; Cerebro receives it from the store."""
        logger.info('%s', data)
        self.put_notification(data)

    def register_bar_consumer(
        self,
        guid: Hashable,
        consumer_id: Hashable,
        *,
        provider_subscription: bool,
    ) -> Queue:
        """Register a dedicated queue and reference-count a provider subscription."""
        with self._bar_lock:
            if consumer_id in self._bar_consumers:
                raise RuntimeError(f'Повторная регистрация consumer_id={consumer_id!r}')
            queue: Queue = Queue()
            self._bar_consumers[consumer_id] = queue
            first = not self._guid_consumers[guid]
            self._guid_consumers[guid].add(consumer_id)
            if provider_subscription and first:
                class_code, sec_code, interval = guid
                try:
                    subscribed_before = bool(self.provider.is_subscribed(
                        class_code, sec_code, interval
                    ).get('data', False))
                    if not subscribed_before:
                        self.provider.subscribe_to_candles(
                            class_code, sec_code, interval
                        )
                        subscribed_after = bool(self.provider.is_subscribed(
                            class_code, sec_code, interval
                        ).get('data', False))
                        if not subscribed_after:
                            raise RuntimeError(
                                f'QUIK отклонил подписку '
                                f'{class_code}.{sec_code}/{interval}'
                            )
                        # Unsubscribe only subscriptions created by this store.
                        self._provider_subscription_guids.add(guid)
                except Exception:
                    self._guid_consumers[guid].discard(consumer_id)
                    if not self._guid_consumers[guid]:
                        self._guid_consumers.pop(guid, None)
                    self._bar_consumers.pop(consumer_id, None)
                    raise
            return queue

    def unregister_bar_consumer(self, guid: Hashable, consumer_id: Hashable) -> None:
        with self._bar_lock:
            self._bar_consumers.pop(consumer_id, None)
            consumers = self._guid_consumers.get(guid)
            if not consumers:
                return
            consumers.discard(consumer_id)
            if consumers:
                return
            self._guid_consumers.pop(guid, None)
            if guid in self._provider_subscription_guids:
                class_code, sec_code, interval = guid
                try:
                    self.provider.unsubscribe_from_candles(
                        class_code, sec_code, interval
                    )
                finally:
                    self._provider_subscription_guids.discard(guid)

    def publish_bar(self, guid: Hashable, bar: dict) -> int:
        """Fan out one bar copy to every feed subscribed to ``guid``."""
        with self._bar_lock:
            consumer_ids = tuple(self._guid_consumers.get(guid, ()))
            queues = [self._bar_consumers.get(cid) for cid in consumer_ids]
        delivered = 0
        for queue in queues:
            if queue is not None:
                queue.put(dict(bar))
                delivered += 1
        return delivered

    def on_new_candle(self, data):
        """Provider callback: convert and fan out without touching QKData."""
        try:
            raw = data['data']
            guid = (raw['class'], raw['sec'], raw['interval'])
            bar = {
                'datetime': self.get_bar_open_date_time(raw),
                'open': raw['open'],
                'high': raw['high'],
                'low': raw['low'],
                'close': raw['close'],
                'volume': int(raw['volume']),
            }
            self.publish_bar(guid, bar)
        except Exception:
            logger.exception('Ошибка обработки callback NewCandle: %r', data)
            self.put_notification('NEW_CANDLE_ERROR', data)

    @staticmethod
    def get_bar_open_date_time(bar):
        dt_json = bar['datetime']
        return datetime(
            int(dt_json['year']), int(dt_json['month']), int(dt_json['day']),
            int(dt_json['hour']), int(dt_json['min'])
        )
