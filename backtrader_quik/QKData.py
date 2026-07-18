from datetime import datetime, timedelta, time, timezone
from uuid import uuid4  # Номера расписаний должны быть уникальными во времени и пространстве
from threading import Thread, Event  # Управляемый поток расписания и событие остановки
from collections import deque
from queue import Empty
from pathlib import Path
import os.path
import csv

from backtrader.feed import AbstractDataBase
from backtrader.utils.py3 import with_metaclass
from backtrader import TimeFrame, date2num

from .QKStore import QKStore
from .logger_config import logger  # Будем вести лог


class MetaQKData(AbstractDataBase.__class__):
    # noinspection PyMethodParameters
    def __init__(cls, name, bases, dct):
        super(MetaQKData, cls).__init__(name, bases, dct)  # Инициализируем класс данных
        QKStore.DataCls = cls  # Регистрируем класс данных в хранилище QUIK


class QKData(with_metaclass(MetaQKData, AbstractDataBase)):
    """Данные QUIK"""
    params = (
        ('four_price_doji', False),  # False - отфильтровывать четырехценовые дожи, True - принимать
        ('schedule', None),  # Расписание работы биржи. Если не задано, то берем из подписки
        ('live_bars', False),  # False - только история, True - история и новые бары
        ('datapath', None),  # Каталог истории; по умолчанию ~/.backtraderquik/data/QUIK
        ('save_bars', True),  # Сохранять полученную историю и live-бары в локальный TSV-файл
        ('qcheck', 0.25),  # Общий budget ожидания Cerebro для live-источников
        ('live_queue_timeout', 0.25),  # Верхняя граница ожидания одной очереди
    )
    datapath = None  # Сохранено для совместимости; фактический путь задаётся параметром datapath
    delimiter = '\t'  # Разделитель значений в файле истории. По умолчанию табуляция
    dt_format = '%d.%m.%Y %H:%M'  # Формат представления даты и времени в файле истории. По умолчанию русский формат
    delta = 3  # Корректировка в секундах при проверке времени окончания бара

    def islive(self):
        """Если подаем новые бары, Cerebro отключает preload и runonce."""
        return self.p.live_bars

    def haslivedata(self):
        """Advisory check used by Cerebro to avoid unnecessary waits."""
        return self.bar_queue is not None and not self.bar_queue.empty()

    def __init__(self, **kwargs):
        self.store = QKStore(**kwargs)  # Хранилище QUIK
        self.class_code, self.sec_code = self.store.provider.dataname_to_class_sec_codes(self.p.dataname)  # По тикеру получаем код режима торгов и тикер
        self.derivative = self.class_code == self.store.provider.futures_cls_code  # Для деривативов не используем конвертацию цен и кол-ва
        self.quik_timeframe = self.bt_timeframe_to_quik_timeframe(self.p.timeframe, self.p.compression)  # Конвертируем временной интервал из BackTrader в QUIK
        self.tf = self.bt_timeframe_to_tf(self.p.timeframe, self.p.compression)  # Конвертируем временной интервал из BackTrader для имени файла истории и расписания
        self.file = f'{self.class_code}.{self.sec_code}_{self.tf}'  # Имя файла истории
        data_dir = Path(self.p.datapath).expanduser() if self.p.datapath else (Path.home() / '.backtraderquik' / 'data' / 'QUIK')
        # Создаём runtime-каталог только при фактической записи истории.
        # Чтение отсутствующего файла и save_bars=False не должны менять ФС.
        if self.p.save_bars:
            data_dir.mkdir(parents=True, exist_ok=True)
        self.file_name = str(data_dir / f'{self.file}.txt')
        self.history_bars = deque()  # O(1) извлечение исторических баров
        self.guid = None  # Идентификатор подписки/расписания на историю цен
        self.consumer_id = str(uuid4())
        self.bar_queue = None
        self.schedule_thread = None
        self.exit_event = Event()  # Определяем событие выхода из потока
        self.dt_last_open = datetime.min  # Дата и время открытия последнего полученного бара
        self.last_bar_received = False  # Получен последний бар
        self.live_mode = False  # Режим получения баров. False = История, True = Новые бары
        self._stopped = False

    def setenvironment(self, env):
        """Добавление хранилища QUIK в cerebro"""
        super(QKData, self).setenvironment(env)
        env.addstore(self.store)  # Добавление хранилища QUIK в cerebro

    def start(self):
        if self._stopped:
            raise RuntimeError('Остановленный QKData нельзя запускать повторно')
        super(QKData, self).start()
        self.put_notification(self.DELAYED)
        self.exit_event.clear()

        # Register the live source before requesting history. Otherwise a bar
        # may close between the history response and the later subscription.
        # Bars received while history is loading remain in this feed's queue
        # and are deduplicated by ``dt_last_open`` during normal loading.
        try:
            if self.p.live_bars:
                if self.p.schedule:
                    self.guid = str(uuid4())
                    self.bar_queue = self.store.register_bar_consumer(
                        self.guid, self.consumer_id,
                        provider_subscription=False,
                    )
                else:
                    self.guid = (
                        self.class_code, self.sec_code, self.quik_timeframe,
                    )
                    logger.debug('Регистрация подписки на новые бары %s', self.guid)
                    self.bar_queue = self.store.register_bar_consumer(
                        self.guid, self.consumer_id,
                        provider_subscription=True,
                    )

            self.get_bars_from_file()
            self.get_bars_from_history()
            if self.history_bars:
                self.put_notification(self.CONNECTED)

            if self.p.live_bars and self.p.schedule:
                self.schedule_thread = Thread(
                    target=self.stream_bars,
                    name=(
                        f'QKDataSchedule-{self.class_code}.'
                        f'{self.sec_code}-{self.tf}'
                    ),
                    daemon=True,
                )
                self.schedule_thread.start()
        except Exception:
            self.exit_event.set()
            if self.guid is not None:
                try:
                    self.store.unregister_bar_consumer(
                        self.guid, self.consumer_id,
                    )
                except Exception:
                    logger.exception(
                        'Ошибка rollback подписки QKData %s', self.guid,
                    )
            self.guid = None
            self.bar_queue = None
            raise

    def _load(self):
        """Загрузка бара из истории или нового бара"""
        if len(self.history_bars) > 0:  # Если есть исторические данные
            bar = self.history_bars.popleft()  # O(1)
        elif not self.p.live_bars:  # Если получаем только историю (self.history_bars) и исторических данных нет / все исторические данные получены
            self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения исторических бар
            logger.debug('Бары из файла/истории отправлены в ТС. Новые бары получать не нужно. Выход')
            return False  # Больше сюда заходить не будем
        else:
            if self.bar_queue is None:
                raise RuntimeError('Live-очередь QKData не зарегистрирована')
            timeout = min(
                float(self.p.live_queue_timeout),
                max(0.0, float(getattr(self, '_qcheck', self.p.qcheck))),
            )
            try:
                if timeout > 0:
                    bar = self.bar_queue.get(timeout=timeout)
                else:
                    bar = self.bar_queue.get_nowait()
            except Empty:
                return None
            self.last_bar_received = self.bar_queue.empty()
            if self.last_bar_received:
                logger.debug('Получение последнего возможного на данный момент бара')
            if not self.is_bar_valid(bar):  # Если бар не соответствует всем условиям выборки
                return None  # то пропускаем бар, будем заходить еще
            logger.debug(f'Сохранение нового бара с {bar["datetime"].strftime(self.dt_format)} в файл')
            if self.p.save_bars:
                self.save_bar_to_file(bar)  # Сохраняем бар в конец файла
            if self.last_bar_received and not self.live_mode:  # Если получили последний бар и еще не находимся в режиме получения новых бар (LIVE)
                self.put_notification(self.LIVE)  # Отправляем уведомление о получении новых бар
                self.live_mode = True  # Переходим в режим получения новых бар (LIVE)
            elif self.live_mode and not self.last_bar_received:  # Если находимся в режиме получения новых бар (LIVE)
                self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) бар
                self.live_mode = False  # Переходим в режим получения истории
        # Все проверки пройдены. Записываем полученный исторический/новый бар
        self.lines.datetime[0] = date2num(bar['datetime'])  # Переводим в формат хранения даты/времени в BackTrader
        self.lines.open[0] = bar['open'] if self.derivative else self.store.provider.quik_price_to_price(self.class_code, self.sec_code, bar['open'])  # Для деривативов
        self.lines.high[0] = bar['high'] if self.derivative else self.store.provider.quik_price_to_price(self.class_code, self.sec_code, bar['high'])  # цена без изменения
        self.lines.low[0] = bar['low'] if self.derivative else self.store.provider.quik_price_to_price(self.class_code, self.sec_code, bar['low'])  # Для остальных
        self.lines.close[0] = bar['close'] if self.derivative else self.store.provider.quik_price_to_price(self.class_code, self.sec_code, bar['close'])  # цена в рублях за штуку
        self.lines.volume[0] = int(bar['volume']) if self.derivative else self.store.provider.lots_to_size(self.class_code, self.sec_code, int(bar['volume']))  # Для деривативов кол-во лотов. Для остальных кол-во штук
        self.lines.openinterest[0] = 0  # Открытый интерес в QUIK не учитывается
        return True  # Будем заходить сюда еще

    def stop(self):
        if self._stopped:
            return
        self._stopped = True
        super(QKData, self).stop()
        if self.p.live_bars and self.guid is not None:
            self.exit_event.set()
            try:
                self.store.unregister_bar_consumer(self.guid, self.consumer_id)
            except Exception:
                logger.exception('Ошибка снятия подписки QKData %s', self.guid)
            finally:
                if self.schedule_thread is not None:
                    self.schedule_thread.join(timeout=5.0)
                    if self.schedule_thread.is_alive():
                        logger.error('Поток расписания QKData не завершился за 5 секунд')
                self.put_notification(self.DISCONNECTED)

    # Получение/сохранение бар

    def get_bars_from_file(self) -> None:
        """Получение бар из файла"""
        if not os.path.isfile(self.file_name):  # Если файл не существует
            return  # то выходим, дальше не продолжаем
        logger.debug(f'Получение бар из файла {self.file_name}')
        with open(self.file_name, encoding='utf-8') as file:  # Открываем файл на последовательное чтение
            reader = csv.reader(file, delimiter=self.delimiter)  # Данные в строке разделены табуляцией
            next(reader, None)  # Пропускаем первую строку с заголовками
            for line_no, csv_row in enumerate(reader, start=2):
                try:
                    if len(csv_row) < 6:
                        raise ValueError(f'ожидалось 6 колонок, получено {len(csv_row)}')
                    bar = dict(
                        datetime=datetime.strptime(csv_row[0], self.dt_format),
                        open=float(csv_row[1]), high=float(csv_row[2]),
                        low=float(csv_row[3]), close=float(csv_row[4]),
                        volume=int(float(csv_row[5])),
                    )
                except (ValueError, TypeError, IndexError) as exc:
                    logger.error(
                        'Некорректная строка %s файла %s пропущена: %r (%s)',
                        line_no, self.file_name, csv_row, exc,
                    )
                    continue
                if self.is_bar_valid(bar):  # Если исторический бар соответствует всем условиям выборки
                    self.history_bars.append(bar)  # то добавляем бар
        if len(self.history_bars) > 0:  # Если были получены бары из файла
            logger.debug(f'Получено бар из файла: {len(self.history_bars)} с {self.history_bars[0]["datetime"].strftime(self.dt_format)} по {self.history_bars[-1]["datetime"].strftime(self.dt_format)}')
        else:  # Бары из файла не получены
            logger.debug('Из файла новых бар не получено')

    def get_bars_from_history(self) -> None:
        """Получение бар из истории"""
        file_history_bars_len = len(self.history_bars)  # Кол-во полученных бар из файла для лога
        logger.debug(f'Получение всех бар из истории')
        response = self.store.provider.get_candles_from_data_source(
            self.class_code, self.sec_code, self.quik_timeframe
        )
        history_bars = response.get('data') or []  # Получаем все бары из QUIK
        for history_bar in history_bars:  # Пробегаемся по всем полученным барам
            bar = dict(datetime=self.store.get_bar_open_date_time(history_bar),  # Собираем дату и время открытия бара
                       open=history_bar['open'], high=history_bar['high'], low=history_bar['low'], close=history_bar['close'],  # Цены QUIK
                       volume=int(history_bar['volume']))  # Объем в лотах. Бар из истории
            if self.is_bar_valid(bar):  # Если исторический бар соответствует всем условиям выборки
                self.history_bars.append(bar)  # то добавляем бар
                if self.p.save_bars:
                    self.save_bar_to_file(bar)  # и сохраняем бар в конец файла
        if len(self.history_bars) - file_history_bars_len > 0:  # Если получены бары из истории
            logger.debug(f'Получено бар из истории: {len(self.history_bars) - file_history_bars_len} с {self.history_bars[file_history_bars_len]["datetime"].strftime(self.dt_format)} по {self.history_bars[-1]["datetime"].strftime(self.dt_format)}')
        else:  # Бары из истории не получены
            logger.debug('Из истории новых бар не получено')

    def is_bar_valid(self, bar) -> bool:
        """Проверка бара на соответствие условиям выборки"""
        dt_open = bar['datetime']  # Дата и время открытия бара МСК
        if dt_open <= self.dt_last_open:  # Если пришел бар из прошлого (дата открытия меньше последней даты открытия)
            # logger.debug(f'Дата/время открытия бара {dt_open} <= последней даты/времени открытия {self.dt_last_open}')  # Для отладки, т.к. идет замедление при обработке старых бар на возобновлении подписки
            return False  # то бар не соответствует условиям выборки
        if self.p.fromdate and dt_open < self.p.fromdate or self.p.todate and dt_open > self.p.todate:  # Если задан диапазон, а бар за его границами
            logger.debug(f'Дата/время открытия бара {dt_open} за границами диапазона {self.p.fromdate} - {self.p.todate}')
            self.dt_last_open = dt_open  # Запоминаем дату/время открытия пришедшего бара для будущих сравнений
            return False  # то бар не соответствует условиям выборки
        if self.p.sessionstart != time.min and dt_open.time() < self.p.sessionstart:  # Если задано время начала сессии и открытие бара до этого времени
            logger.debug(f'Дата/время открытия бара {dt_open} до начала торговой сессии {self.p.sessionstart}')
            self.dt_last_open = dt_open  # Запоминаем дату/время открытия пришедшего бара для будущих сравнений
            return False  # то бар не соответствует условиям выборки
        dt_close = self.get_bar_close_date_time(dt_open)  # Дата и время закрытия бара
        if self.p.sessionend != time(23, 59, 59, 999990) and dt_close.time() > self.p.sessionend:  # Если задано время окончания сессии и закрытие бара после этого времени
            logger.debug(f'Дата/время открытия бара {dt_open} после окончания торговой сессии {self.p.sessionend}')
            self.dt_last_open = dt_open  # Запоминаем дату/время открытия пришедшего бара для будущих сравнений
            return False  # то бар не соответствует условиям выборки
        if not self.p.four_price_doji and bar['high'] == bar['low']:  # Если не пропускаем дожи 4-х цен, но такой бар пришел
            logger.debug(f'Бар {dt_open} - дожи 4-х цен')
            self.dt_last_open = dt_open  # Запоминаем дату/время открытия пришедшего бара для будущих сравнений
            return False  # то бар не соответствует условиям выборки
        dt_market_now = self.get_quik_date_time_now()  # Текущая дата и время из QUIK
        dt_market_now_corrected = dt_market_now + timedelta(seconds=self.delta)  # Текущая дата и время из QUIK с корректировкой
        if dt_close > dt_market_now_corrected and dt_market_now_corrected.time() < self.p.sessionend:  # Если время закрытия бара еще не наступило на бирже, и сессия еще не закончилась
            logger.debug(f'Дата/время {dt_close:{self.dt_format}} закрытия бара на {dt_open:{self.dt_format}} еще не наступило. Текущее время {dt_market_now:%d.%m.%Y %H:%M:%S}')
            return False  # то бар не соответствует условиям выборки
        self.dt_last_open = dt_open  # Запоминаем дату/время открытия пришедшего бара для будущих сравнений
        return True  # В остальных случаях бар соответствуем условиям выборки

    def stream_bars(self) -> None:
        """Поток получения новых бар по расписанию биржи"""
        logger.debug('Запуск получения новых бар по расписанию')
        while True:
            market_datetime_now = self.p.schedule.utc_to_msk_datetime(datetime.now(timezone.utc))  # Текущее время на бирже
            trade_bar_request_datetime = self.p.schedule.trade_bar_request_datetime(market_datetime_now, self.tf)  # Дата и время запроса бара на бирже
            sleep_time_secs = max(0.0, (trade_bar_request_datetime - market_datetime_now).total_seconds())  # Время ожидания в секундах
            logger.debug(f'Получение последнего бара по расписанию в {trade_bar_request_datetime.strftime(self.dt_format)}. Ожидание {sleep_time_secs} с')
            exit_event_set = self.exit_event.wait(sleep_time_secs)  # Ждем нового бара или события выхода из потока
            if exit_event_set:  # Если произошло событие выхода из потока
                logger.warning('Отмена получения новых бар по расписанию')
                return  # Выходим из потока, дальше не продолжаем
            try:
                response = self.store.provider.get_candles_from_data_source(
                    self.class_code, self.sec_code, self.quik_timeframe, count=1
                )
                bars = response.get('data') or []
                if not bars:
                    logger.warning('QUIK не вернул бар по расписанию для %s.%s', self.class_code, self.sec_code)
                    continue
                stream_bar = bars[0]
                bar = dict(
                    datetime=self.store.get_bar_open_date_time(stream_bar),
                    open=stream_bar['open'], high=stream_bar['high'],
                    low=stream_bar['low'], close=stream_bar['close'],
                    volume=int(stream_bar['volume'])
                )
                logger.debug('Получен бар по расписанию')
                self.store.publish_bar(self.guid, bar)
            except Exception:
                logger.exception('Ошибка потока расписания QKData')
                self.put_notification(self.DISCONNECTED)
                if self.exit_event.wait(1.0):
                    return

    def save_bar_to_file(self, bar) -> None:
        """Сохранение бара в конец файла"""
        if not os.path.isfile(self.file_name):  # Существует ли файл
            logger.warning(f'Файл {self.file_name} не найден и будет создан')
            with open(self.file_name, 'w', newline='', encoding='utf-8') as file:  # Создаем файл
                writer = csv.writer(file, delimiter=self.delimiter)  # Данные в строке разделены табуляцией
                writer.writerow(bar.keys())  # Записываем заголовок в файл
        with open(self.file_name, 'a', newline='', encoding='utf-8') as file:  # Открываем файл на добавление в конец. Ставим newline, чтобы в Windows не создавались пустые строки в файле
            writer = csv.writer(file, delimiter=self.delimiter)  # Данные в строке разделены табуляцией
            csv_row = bar.copy()  # Копируем бар для того, чтобы изменить формат даты
            csv_row['datetime'] = csv_row['datetime'].strftime(self.dt_format)  # Приводим дату к формату файла
            writer.writerow(csv_row.values())  # Записываем бар в конец файла
            logger.debug(f'В файл {self.file_name} записан бар на {csv_row["datetime"]}')

    # Функции

    @staticmethod
    def bt_timeframe_to_quik_timeframe(timeframe, compression=1) -> int:
        """Перевод временнОго интервала из BackTrader в QUIK

        :param TimeFrame timeframe: Временной интервал
        :param int compression: Размер временнОго интервала
        :return: Временной интервал QUIK
        """
        if timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            return compression  # Кол-во минут
        elif timeframe == TimeFrame.Days:  # Дневной временной интервал (по умолчанию)
            return 1440  # В минутах
        elif timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            return 10080  # В минутах
        elif timeframe == TimeFrame.Months:  # Месячный временной интервал
            return 23200  # В минутах
        raise NotImplementedError  # С остальными временнЫми интервалами не работаем

    @staticmethod
    def bt_timeframe_to_tf(timeframe, compression=1) -> str:
        """Перевод временнОго интервала из BackTrader для имени файла истории и расписания https://ru.wikipedia.org/wiki/Таймфрейм

        :param TimeFrame timeframe: Временной интервал
        :param int compression: Размер временнОго интервала
        :return: Временной интервал для имени файла истории и расписания
        """
        if timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            return f'M{compression}'
        # Часовой график f'H{compression}' заменяем минутным. Пример: H1 = M60
        elif timeframe == TimeFrame.Days:  # Дневной временной интервал
            return 'D1'
        elif timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            return 'W1'
        elif timeframe == TimeFrame.Months:  # Месячный временной интервал
            return 'MN1'
        raise NotImplementedError  # С остальными временнЫми интервалами не работаем

    def get_bar_close_date_time(self, dt_open, period=1):
        """Дата и время закрытия бара"""
        if self.p.timeframe == TimeFrame.Days:  # Дневной временной интервал (по умолчанию)
            return dt_open + timedelta(days=period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            return dt_open + timedelta(weeks=period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Months:  # Месячный временной интервал
            year = dt_open.year + (dt_open.month + period - 1) // 12  # Год
            month = (dt_open.month + period - 1) % 12 + 1  # Месяц
            return datetime(year, month, 1)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Years:  # Годовой временной интервал
            return dt_open.replace(year=dt_open.year + period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            return dt_open + timedelta(minutes=self.p.compression * period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Seconds:  # Секундный временной интервал
            return dt_open + timedelta(seconds=self.p.compression * period)  # Время закрытия бара
        raise NotImplementedError  # С остальными временнЫми интервалами не работаем

    def get_quik_date_time_now(self):
        """Текущая дата и время
        - Если получили последний бар истории, то запрашием текущие дату и время из QUIK
        - Если находимся в режиме получения истории, то переводим текущие дату и время с компьютера в МСК
        """
        if not self.live_mode:  # Если не находимся в режиме получения новых баров
            return datetime.now(self.store.provider.tz_msk).replace(tzinfo=None)  # То время МСК получаем из локального времени
        try:  # Проверяем, можно ли привести полученные строки в дату и время
            d = self.store.provider.get_info_param('TRADEDATE')['data']  # Дата на сервере в виде строки dd.mm.yyyy. Может прийти неверная дата
            t = self.store.provider.get_info_param('SERVERTIME')['data']  # Время на сервере в виде строки hh:mi:ss
            return datetime.strptime(f'{d} {t}', '%d.%m.%Y %H:%M:%S')  # Переводим строки в дату и время и возвращаем ее
        except (ValueError, KeyError, TypeError, ConnectionError, OSError):  # Некорректный/недоступный ответ QUIK
            return datetime.now(self.store.provider.tz_msk).replace(tzinfo=None)  # То время МСК получаем из локального времени
