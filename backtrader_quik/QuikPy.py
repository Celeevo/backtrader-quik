from __future__ import annotations

import logging  # Будем вести лог
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # ВременнАя зона
from datetime import timezone, timedelta
from typing import Any  # Любой тип
from socket import (socket, AF_INET, SOCK_STREAM, SHUT_RDWR, timeout as SocketTimeout)
from threading import Thread, Event as ThreadingEvent, Lock, RLock, current_thread
from json import dumps, loads
from json.decoder import JSONDecodeError
from time import sleep
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP



try:
    _MSK_TIMEZONE = ZoneInfo('Europe/Moscow')
except ZoneInfoNotFoundError:  # Windows without the optional tzdata package
    _MSK_TIMEZONE = timezone(timedelta(hours=3), name='MSK')


class QuikPy:
    """Работа с QUIK из Python через LUA скрипты QUIK# https://github.com/finsight/QUIKSharp/tree/master/src/QuikSharp/lua
     На основе Документации по языку LUA в QUIK из https://arqatech.com/ru/support/files/
     Маркировка функций по пунктам документа: Документация по языку LUA в QUIK и примеры - Интерпретатор языка Lua - Версия 11.2
     """
    buffer_size = 1048576  # Размер буфера приема в байтах (1 МБайт)
    max_message_size = buffer_size * 64
    tz_msk = _MSK_TIMEZONE  # Время UTC будем приводить к московскому времени
    currency = 'SUR'  # Суммы будем получать в рублях
    limit_kind = 1  # Основной режим торгов T1
    futures_firm_id = 'SPBFUT'  # Код фирмы для срочного рынка. Если ваш брокер поставил другую фирму для срочного рынка, то измените ее
    futures_cls_code = 'SPBFUT'  # Код класса (режима торгов) срочного рынка. Одинаков в боевом QUIK и в QUIK Junior
    logger = logging.getLogger('QuikPy')  # Будем вести лог

    def __init__(self, host='127.0.0.1', requests_port=34130, callbacks_port=34131, socket_timeout=15.0, callback_retry_delay=1.0, candle_poll_interval=1.0):
        """Инициализация

        :param str host: IP адрес или название хоста
        :param int requests_port: Порт для отправки запросов и получения ответов
        :param int callbacks_port: Порт для функций обратного вызова
        """
        # 2.2 Функции обратного вызова
        self.on_firm = Event()  # 2.2.1 Новая фирма
        self.on_all_trade = Event()  # 2.2.2 Новая обезличенная сделка
        self.on_trade = Event()  # 2.2.3 Новая сделка / Изменение существующей сделки
        self.on_order = Event()  # 2.2.4 Новая заявка / Изменение существующей заявки
        self.on_account_balance = Event()  # 2.2.5 Изменение текущей позиции по счету
        self.on_futures_limit_change = Event()  # 2.2.6 Изменение ограничений по срочному рынку
        self.on_futures_limit_delete = Event()  # 2.2.7 Удаление ограничений по срочному рынку
        self.on_futures_client_holding = Event()  # 2.2.8 Изменение позиции по срочному рынку
        self.on_money_limit = Event()  # 2.2.9 Изменение денежной позиции
        self.on_money_limit_delete = Event()  # 2.2.10 Удаление денежной позиции
        self.on_depo_limit = Event()  # 2.2.11 Изменение позиций по инструментам
        self.on_depo_limit_delete = Event()  # 2.2.12 Удаление позиции по инструментам
        self.on_account_position = Event()  # 2.2.13 Изменение денежных средств
        # on_neg_deal - 2.2.14 Новая внебиржевая заявка / Изменение существующей внебиржевой заявки
        # on_neg_trade - 2.2.15 Новая внебиржевая сделка / Изменение существующей внебиржевой сделки
        self.on_stop_order = Event()  # 2.2.16 Новая стоп заявка / Изменение существующей стоп заявки
        self.on_trans_reply = Event()  # 2.2.17 Ответ на транзакцию пользователя
        self.on_param = Event()  # 2.2.18 Изменение текущих параметров
        self.on_quote = Event()  # 2.2.19 Изменение стакана котировок
        self.on_disconnected = Event()  # 2.2.20 Отключение терминала от сервера QUIK
        self.on_connected = Event()  # 2.2.21 Соединение терминала с сервером QUIK
        # on_clean_up - 2.2.22 Смена сервера QUIK / Пользователя / Сессии
        self.on_close = Event()  # 2.2.23 Закрытие терминала QUIK
        self.on_stop = Event()  # 2.2.24 Остановка LUA скрипта в терминале QUIK / закрытие терминала QUIK
        self.on_init = Event()  # 2.2.25 Запуск LUA скрипта в терминале QUIK
        # on_main - 2.2.26 Функция, реализующая основной поток выполнения в скрипте

        # Функции обратного вызова QUIK#
        self.on_new_candle = Event()  # Новая свечка
        self.on_error = Event()  # Сообщение об ошибке

        self.host = host
        self.requests_port = requests_port
        self.callbacks_port = callbacks_port
        self.socket_timeout = float(socket_timeout)
        self.callback_retry_delay = float(callback_retry_delay)
        self.candle_poll_interval = max(0.25, float(candle_poll_interval))
        self.lock = Lock()
        self._lifecycle_lock = RLock()
        self._subscriptions_lock = RLock()
        self._candle_poll_lock = RLock()
        self.callback_exit_event = ThreadingEvent()
        self.socket_requests = None
        self.callback_socket = None
        self.callback_thread = None
        self.candle_poll_thread = None
        self._candle_poll_current = {}
        self._candle_emitted = {}
        self._closed = False
        self.healthy = False
        self.last_error = None

        self.accounts = []
        self.classes, self.securities = {}, {}
        self.subscriptions = []
        self.symbols = {}

        # QUIKSharp Lua accepts the request connection and then waits for the
        # callback connection before it starts processing requests.  Start the
        # callback client first; otherwise the first synchronous request below
        # deadlocks with Lua until socket_timeout expires.
        self.callback_thread = Thread(
            target=self.callback_handler,
            name='QuikPyCallbackThread',
            daemon=True,
        )
        self.callback_thread.start()

        self._connect_request_socket()
        money_limits = self.get_money_limits().get('data') or []
        unique_classes = set()
        for account_id, account in enumerate(self.get_trade_accounts().get('data') or []):
            firm_id = account['firmid']
            client_candidates = sorted({
                str(limit.get('client_code') or '')
                for limit in money_limits
                if limit.get('firmid') == firm_id and limit.get('client_code')
            })
            explicit_client = (
                account.get('client_code')
                or account.get('clientcode')
                or account.get('client')
            )
            if explicit_client:
                client_code = str(explicit_client)
            elif len(client_candidates) == 1:
                client_code = client_candidates[0]
            else:
                # Never choose an arbitrary client code when a firm has several.
                # QKBroker will require an explicit override for a spot account.
                client_code = ''
            class_codes = [
                code for code in str(account.get('class_codes', '')).strip('|').split('|')
                if code
            ]
            unique_classes.update(class_codes)
            self.accounts.append({
                'account_id': account_id,
                'client_code': client_code,
                'client_code_candidates': client_candidates,
                'firm_id': firm_id,
                'trade_account_id': account['trdaccid'],
                'class_codes': class_codes,
                'futures': self.futures_cls_code in class_codes,
            })

        for class_code in sorted(unique_classes):
            response = self.get_class_securities(class_code)
            securities = [
                sec for sec in str(response.get('data') or '').rstrip(',').split(',')
                if sec
            ]
            self.classes[class_code] = set(securities)
        for class_code, sec_list in self.classes.items():
            for sec_code in sec_list:
                self.securities.setdefault(sec_code, set()).add(class_code)

        self.healthy = True
        # QUIK 11.4 can accept SetUpdateCallback but never invoke it. Poll the
        # same subscribed DataSource as a deterministic fallback. Native and
        # polled candles pass through one deduplicating emitter.
        self.candle_poll_thread = Thread(
            target=self._candle_poll_handler,
            name='QuikPyCandlePollThread',
            daemon=True,
        )
        self.candle_poll_thread.start()

    def __enter__(self):
        """Вход в класс, например, с with"""
        return self

    # Фукнции отладки QUIK#

    def ping(self, trans_id=0):
        """Проверка соединения. Отправка строки 'ping'. Получение строки 'pong'

        :param int trans_id: Код транзакции
        :return: Строка 'pong'
        """
        return self.process_request({'data': 'Ping', 'id': trans_id, 'cmd': 'ping', 't': ''})

    def echo(self, message, trans_id=0):
        """Отправка и получение одного и того же сообщения (эхо)

        :param str message: Сообщение
        :param int trans_id: Код транзакции
        :return: Это же сообщение
        """
        return self.process_request({'data': message, 'id': trans_id, 'cmd': 'echo', 't': ''})

    def divide_string_by_zero(self, trans_id=0):
        """Тест обработки ошибок. Выполняется деление строки на 0 с выдачей ошибки

        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'divide_string_by_zero', 't': ''})

    def is_quik(self, trans_id=0):
        """Скрипт запущен в QUIK

        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'is_quik', 't': ''})

    # 2.1 Сервисные функции

    def is_connected(self, trans_id=0):  # 2.1.1 Функция предназначена для определения состояния подключения клиентского места к серверу
        """Состояние подключения терминала к серверу QUIK

        :param int trans_id: Код транзакции
        :return: 1 - подключено / 0 - не подключено
        """
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'isConnected', 't': ''})

    def get_script_path(self, trans_id=0):  # 2.1.2 Функция возвращает путь, по которому находится запускаемый скрипт, без завершающего обратного слеша (\). Например, C:\QuikFront\Scripts
        """Путь скрипта

        :param int trans_id: Код транзакции
        :return: Путь скрипта без завершающего обратного слэша
        """
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getScriptPath', 't': ''})

    def get_info_param(self, params, trans_id=0):  # 2.1.3 Функция возвращает значения параметров информационного окна (пункт меню Система / О программе / Информационное окно…)
        """Значения параметров информационного окна

        :param str params: Параметр. Список возможных параметров на стр. 8
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': params, 'id': trans_id, 'cmd': 'getInfoParam', 't': ''})

    # message - 2.1.4. Сообщение в терминале QUIK. Реализовано в виде 3-х отдельных функций message_info/message_warning/message_error в QUIK# ниже

    def sleep(self, time, trans_id=0):  # 2.1.5 Функция приостанавливает выполнение скрипта
        """Приостановка скрипта. Время в миллисекундах

        :param int time: Время в миллисекундах
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': time, 'id': trans_id, 'cmd': 'sleep', 't': ''})

    def get_working_folder(self, trans_id=0):  # 2.1.6 Функция возвращает путь, по которому находится файл info.exe, исполняющий данный скрипт, без завершающего обратного слеша (\). Например, c:\QuikFront
        """Путь к info.exe, исполняющего скрипт

        :param int trans_id: Код транзакции
        :return: Путь к info.exe, исполняющего скрипта, без завершающего обратного слэша
        """
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getWorkingFolder', 't': ''})

    def print_dbg_str(self, message, trans_id=0):  # 2.1.7 Функция для вывода отладочной информации
        """Вывод отладочной информации. Можно посмотреть с помощью DebugView

        :param str message: Отладочная информация
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': message, 'id': trans_id, 'cmd': 'PrintDbgStr', 't': ''})

    # sysdate - 2.1.8. Системные дата и время
    # isDarkTheme - 2.1.9. Тема оформления. true - тёмная, false - светлая

    # Сервисные функции QUIK#

    def message_info(self, message, trans_id=0):  # В QUIK LUA message icon_type=1
        """Отправка информационного сообщения в терминал QUIK

        :param str message: Информационное сообщение
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': message, 'id': trans_id, 'cmd': 'message', 't': ''})

    def message_warning(self, message, trans_id=0):  # В QUIK LUA message icon_type=2
        """Отправка сообщения с предупреждением в терминал QUIK

        :param str message: Сообщение с предупреждением
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': message, 'id': trans_id, 'cmd': 'warning_message', 't': ''})

    def message_error(self, message, trans_id=0):  # В QUIK LUA message icon_type=3
        """Отправка сообщения об ошибке в терминал QUIK

        :param str message: Сообщение об ошибке
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': message, 'id': trans_id, 'cmd': 'error_message', 't': ''})

    # 3.1. Функции для обращения к строкам произвольных таблиц

    # getItem - 3.1.1. Строка таблицы
    # getOrderByNumber - 3.1.2. Заявка
    # getNumberOf - 3.1.3. Кол-во записей в таблице
    # SearchItems - 3.1.4. Быстрый поиск по таблице заданной функцией поиска

    def get_trade_accounts(self, trans_id=0):  # QUIK#
        """Торговые счета, у которых указаны поддерживаемые классы инструментов

        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getTradeAccounts', 't': ''})

    def get_trade_account(self, class_code, trans_id=0):  # QUIK#
        """Торговый счет для режима торгов

        :param str class_code: Код режима торгов
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': class_code, 'id': trans_id, 'cmd': 'getTradeAccount', 't': ''})

    def get_all_orders(self, trans_id=0):  # QUIK#
        """Все заявки

        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'', 'id': trans_id, 'cmd': 'get_orders', 't': ''})

    def get_orders(self, class_code, sec_code, trans_id=0):  # QUIK#
        """Заявки по тикеру

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'get_orders', 't': ''})

    def get_order_by_number(self, order_id, trans_id=0):  # QUIK#
        """Заявка по номеру

        :param str order_id: Номер заявки
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': order_id, 'id': trans_id, 'cmd': 'getOrder_by_Number', 't': ''})

    def get_order_by_id(self, class_code, sec_code, order_trans_id, trans_id=0):  # QUIK#
        """Заявка по тикеру и коду транзакции заявки

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param str order_trans_id: Код транзакции заявки
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{sec_code}|{order_trans_id}', 'id': trans_id, 'cmd': 'getOrder_by_ID', 't': ''})

    def get_order_by_class_number(self, class_code, order_id, trans_id=0):  # QUIK#
        """Заявка по режиму торгов и номеру

        :param str class_code: Код режима торгов
        :param str order_id: Номер заявки
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{order_id}', 'id': trans_id, 'cmd': 'getOrder_by_Number', 't': ''})

    def get_money_limits(self, trans_id=0):  # QUIK#
        """Все позиции по деньгам

        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getMoneyLimits', 't': ''})

    def get_client_code(self, trans_id=0):  # QUIK#
        """Основной (первый) код клиента

        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getClientCode', 't': ''})

    def get_client_codes(self, trans_id=0):  # QUIK#
        """Все коды клиента

        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getClientCodes', 't': ''})

    def get_all_depo_limits(self, trans_id=0):  # QUIK#
        """Лимиты по всем инструментам

        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'get_depo_limits', 't': ''})

    def get_depo_limits(self, sec_code, trans_id=0):  # QUIK#
        """Лимиты по инструменту

        :param str sec_code: Тикер
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': sec_code, 'id': trans_id, 'cmd': 'get_depo_limits', 't': ''})

    def get_all_trades(self, trans_id=0):  # QUIK#
        """Все сделки

        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'', 'id': trans_id, 'cmd': 'get_trades', 't': ''})

    def get_trades(self, class_code, sec_code, trans_id=0):  # QUIK#
        """Сделки по инструменту

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'get_trades', 't': ''})

    def get_trades_by_order_number(self, order_num, trans_id=0):  # QUIK#
        """Сделки по номеру заявки

        :param str order_num: Номер заявки
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': order_num, 'id': trans_id, 'cmd': 'get_Trades_by_OrderNumber', 't': ''})

    def get_all_stop_orders(self, trans_id=0):  # QUIK#
        """Все стоп заявки

        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'get_stop_orders', 't': ''})

    def get_stop_orders(self, class_code, sec_code, trans_id=0):  # QUIK#
        """Стоп заявки по инструменту

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'get_stop_orders', 't': ''})

    def get_all_trade(self, trans_id=0):  # QUIK#
        """Все обезличенные сделки

        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'', 'id': trans_id, 'cmd': 'get_all_trades', 't': ''})

    def get_trade(self, class_code, sec_code, trans_id=0):  # QUIK#
        """Обезличенные сделки по инструменту

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'get_all_trades', 't': ''})

    # 3.2 Функции для обращения к спискам доступных параметров

    def get_classes_list(self, trans_id=0):  # 3.2.1 Функция предназначена для получения списка режимов торгов, переданных с сервера в ходе сеанса связи
        """Все режимы торгов

        :param int trans_id: Код транзакции
        :return: Все режимы торгов разделенные запятыми. В конце также запятая
        """
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getClassesList', 't': ''})

    def get_class_info(self, class_code, trans_id=0):  # 3.2.2 Функция предназначена для получения информации о режиме торгов
        """Информация о режиме торгов

        :param str class_code: Код режима торгов
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': class_code, 'id': trans_id, 'cmd': 'getClassInfo', 't': ''})

    def get_class_securities(self, class_code, trans_id=0):  # 3.2.3 Функция предназначена для получения списка кодов инструментов для списка режимов торгов, заданного списком кодов
        """Тикеры режима торгов

        :param str class_code: Код режима торгов
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': class_code, 'id': trans_id, 'cmd': 'getClassSecurities', 't': ''})

    def get_option_board(self, class_code, sec_code, trans_id=0):  # QUIK#
        """Доска опционов

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'getOptionBoard', 't': ''})

    # 3.3 Функции для получения информации по денежным средствам

    def get_money(self, client_code, firm_id, tag, curr_code, trans_id=0):  # 3.3.1 Функция предназначена для получения информации по денежным позициям
        """Денежные позиции

        :param str client_code: Код клиента
        :param str firm_id: Код фирмы
        :param str tag: Идентификатор денежного лимита
        :param str curr_code: Код валюты. SUR для рублей
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{client_code}|{firm_id}|{tag}|{curr_code}', 'id': trans_id, 'cmd': 'getMoney', 't': ''})

    def get_money_ex(self, firm_id, client_code, tag, curr_code, limit_kind, trans_id=0):  # 3.3.2 Функция предназначена для получения информации по денежным позициям указанного типа
        """Денежные позиции указанного типа

        :param str firm_id: Код фирмы
        :param str client_code: Код клиента
        :param str tag: Идентификатор денежного лимита
        :param str curr_code: Код валюты. SUR для рублей
        :param int limit_kind: Срок расчетов
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{firm_id}|{client_code}|{tag}|{curr_code}|{limit_kind}', 'id': trans_id, 'cmd': 'getMoneyEx', 't': ''})

    # 3.4 Функции для получения позиций по инструментам

    def get_depo(self, client_code, firm_id, sec_code, account, trans_id=0):  # 3.4.1 Функция предназначена для получения позиций по инструментам
        """Позиции по инструментам

        :param str client_code: Код клиента
        :param str firm_id: Код фирмы
        :param str sec_code: Тикер
        :param str account: Счет
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{client_code}|{firm_id}|{sec_code}|{account}', 'id': trans_id, 'cmd': 'getDepo', 't': ''})

    def get_depo_ex(self, firm_id, client_code, sec_code, account, limit_kind, trans_id=0):  # 3.4.2 Функция предназначена для получения позиций по инструментам указанного типа
        """Позиции по инструментам указанного типа

        :param str firm_id: Код фирмы
        :param str client_code: Код клиента
        :param str sec_code: Тикер
        :param str account: Счет
        :param int limit_kind: Срок расчетов
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{firm_id}|{client_code}|{sec_code}|{account}|{limit_kind}', 'id': trans_id, 'cmd': 'getDepoEx', 't': ''})

    # 3.5 Функция для получения информации по фьючерсным лимитам

    def get_futures_limit(self, firm_id, account_id, limit_type, curr_code, trans_id=0):  # 3.5.1 Функция предназначена для получения информации по фьючерсным лимитам
        """Фьючерсные лимиты

        :param str firm_id: Код фирмы
        :param str account_id: Счет
        :param int limit_type: Срок расчетов (limit_kind)
        :param str curr_code: Код валюты. SUR для рублей
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{firm_id}|{account_id}|{limit_type}|{curr_code}', 'id': trans_id, 'cmd': 'getFuturesLimit', 't': ''})

    def get_futures_client_limits(self, trans_id=0):  # QUIK#
        """Все фьючерсные лимиты

        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getFuturesClientLimits', 't': ''})

    # 3.6 Функция для получения информации по фьючерсным позициям

    def get_futures_holding(self, firm_id, account_id, sec_code, position_type, trans_id=0):  # 3.6.1 Функция предназначена для получения информации по фьючерсным позициям
        """Фьючерсные позиции

        :param str firm_id: Код фирмы
        :param str account_id: Счет
        :param str sec_code: Тикер
        :param str position_type: Тип лимита. Возможные значения: 0 – не определён; 1 – основной счет; 2 – клиентские и дополнительные счета; 4 – все счета торг. членов
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{firm_id}|{account_id}|{sec_code}|{position_type}', 'id': trans_id, 'cmd': 'getFuturesHolding', 't': ''})

    def get_futures_holdings(self, trans_id=0):  # QUIK#
        """Все фьючерсные позиции

        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getFuturesClientHoldings', 't': ''})

    # 3.7 Функция для получения информации по инструменту

    def get_security_info(self, class_code, sec_code, trans_id=0):  # 3.7.1 Функция предназначена для получения информации по инструменту
        """Информация по инструменту

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'getSecurityInfo', 't': ''})

    def get_security_info_bulk(self, class_sec_codes, trans_id=0):  # QUIK#
        """Информация по инструментам

        :param set[str] class_sec_codes: Список кодов режимов торгов и тикеров. Например: {'TQBR|SBER', 'SPBFUT|CNYRUBF'}
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': class_sec_codes, 'id': trans_id, 'cmd': 'getSecurityInfoBulk', 't': ''})

    def get_security_class(self, classes_list, sec_code, trans_id=0):  # QUIK#
        """Режим торгов по коду инструмента из заданных режимов торгов

        :param str classes_list: Режимы торгов через запятую, по которым будет поиск
        :param str sec_code: Тикер
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{classes_list}|{sec_code}', 'id': trans_id, 'cmd': 'getSecurityClass', 't': ''})

    # 3.8 Функция для получения даты торговой сессии

    # getTradeDate - 3.8.1. Дата текущей торговой сессии

    # 3.9 Функция для получения стакана по указанному классу и инструменту

    def get_quote_level2(self, class_code, sec_code, trans_id=0):  # 3.9.1 Функция предназначена для получения стакана по указанному режиму торгов и инструменту
        """Стакан по классу и инструменту

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'GetQuoteLevel2', 't': ''})

    # 3.10 Функции для работы с графиками

    # getLinesCount - 3.10.1. Кол-во линий в графике

    def get_num_candles(self, tag, trans_id=0):  # 3.10.2 Функция предназначена для получения информации о количестве свечек по выбранному идентификатору
        """Кол-во свечей по идентификатору

        :param str tag: Строковый идентификатор графика или индикатора
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': tag, 'id': trans_id, 'cmd': 'get_num_candles', 't': ''})

    # getCandlesByIndex - 3.10.3. Информация о свечках (реализовано в get_candles)
    # CreateDataSource - 3.10.4. Создание источника данных c функциями: (реализовано в get_candles_from_data_source)
    # - SetUpdateCallback - Привязка функции обратного вызова на изменение свечи
    # - O, H, L, C, V, T - Функции получения цен, объемов и времени
    # - Size - Функция кол-ва свечек в источнике данных
    # - Close - Функция закрытия источника данных. Терминал прекращает получать данные с сервера
    # - SetEmptyCallback - Функция сброса функции обратного вызова на изменение свечи

    def get_candles(self, tag, line, first_candle, count, trans_id=0):  # QUIK#
        """Свечи по идентификатору графика

        :param str tag: Строковый идентификатор графика или индикатора
        :param int line: Номер линии графика или индикатора
        :param int first_candle: Номер первой свечи
        :param int count: Кол-во свечей. 0 - все
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{tag}|{line}|{first_candle}|{count}', 'id': trans_id, 'cmd': 'get_candles', 't': ''})

    def get_candles_from_data_source(self, class_code, sec_code, interval, param='-', count=0):  # QUIK#
        """Свечи

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int interval: Кол-во в минутах: 0 (тик), 1, 2, 3, 4, 5, 6, 10, 15, 20, 30, 60 (1 час), 120 (2 часа), 240 (4 часа), 1440 (день), 10080 (неделя), 23200 (месяц)
        :param str param: Если параметр не задан, то заказываются данные на основании Таблицы обезличенных сделок, если задан – данные по этому параметру
        :param int count: Кол-во свечей. 0 - все
        """
        return self.process_request({'data': f'{class_code}|{sec_code}|{interval}|{param}|{count}', 'id': '1', 'cmd': 'get_candles_from_data_source', 't': ''})

    def _get_subscriptions_lock(self):
        lock = getattr(self, '_subscriptions_lock', None)
        if lock is None:
            lock = self._subscriptions_lock = RLock()
        return lock

    def _remember_subscription(self, subscription: dict) -> None:
        with self._get_subscriptions_lock():
            if subscription not in self.subscriptions:
                self.subscriptions.append(subscription)

    def _forget_subscription(self, subscription: dict) -> None:
        with self._get_subscriptions_lock():
            try:
                self.subscriptions.remove(subscription)
            except ValueError:
                pass

    def _subscriptions_snapshot(self) -> tuple[dict, ...]:
        with self._get_subscriptions_lock():
            return tuple(dict(item) for item in self.subscriptions)

    @staticmethod
    def _candle_time_signature(candle: dict | None):
        if not isinstance(candle, dict):
            return None
        value = candle.get('datetime') or {}
        if not isinstance(value, dict):
            return None
        fields = ('year', 'month', 'day', 'hour', 'min', 'sec', 'ms')
        try:
            return tuple(int(value.get(field) or 0) for field in fields)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _closed_candles_after(cls, rows: list[dict], previous_current):
        """Return the new current signature and candles closed since baseline."""
        valid = [row for row in rows if cls._candle_time_signature(row) is not None]
        if not valid:
            return previous_current, []
        current = cls._candle_time_signature(valid[-1])
        if previous_current is None or current == previous_current:
            return current, []
        previous_index = next(
            (index for index, row in enumerate(valid)
             if cls._candle_time_signature(row) == previous_current),
            None,
        )
        if previous_index is None:
            # A long gap exceeded our bounded history window. Publish only
            # the most recent definitely closed candle, never the live bar.
            return current, valid[-2:-1]
        return current, valid[previous_index:-1]

    @staticmethod
    def _candle_subscription_key(class_code, sec_code, interval, param='-'):
        return str(class_code), str(sec_code), int(interval), str(param)

    def _get_candle_poll_lock(self):
        """Return lazily initialized polling state for lightweight clients/tests."""
        lock = getattr(self, '_candle_poll_lock', None)
        if lock is None:
            lock = RLock()
            self._candle_poll_lock = lock
        if not hasattr(self, '_candle_poll_current'):
            self._candle_poll_current = {}
        if not hasattr(self, '_candle_emitted'):
            self._candle_emitted = {}
        return lock

    def _reset_candle_poll_state(self, class_code, sec_code, interval, param='-'):
        key = self._candle_subscription_key(class_code, sec_code, interval, param)
        with self._get_candle_poll_lock():
            self._candle_poll_current.pop(key, None)
            self._candle_emitted.pop(key, None)

    def _emit_new_candle(self, candle: dict, *, source: str, envelope=None) -> bool:
        try:
            key = self._candle_subscription_key(
                candle['class'], candle['sec'], candle['interval'], '-'
            )
        except (KeyError, TypeError, ValueError):
            self.logger.warning('Invalid %s candle: %r', source, candle)
            return False
        signature = self._candle_time_signature(candle)
        if signature is None:
            self.logger.warning('Candle without valid time from %s: %r', source, candle)
            return False
        with self._get_candle_poll_lock():
            if self._candle_emitted.get(key) == signature:
                return False
            self._candle_emitted[key] = signature
        payload = dict(envelope or {})
        payload.update({'cmd': 'NewCandle', 'data': dict(candle), 'source': source})
        self.on_new_candle.trigger(payload)
        return True

    def _poll_candle_subscription(self, subscription: dict) -> None:
        class_code = subscription['class_code']
        sec_code = subscription['sec_code']
        interval = subscription['interval']
        param = subscription.get('param', '-')
        key = self._candle_subscription_key(class_code, sec_code, interval, param)
        response = self.get_candles_from_data_source(
            class_code, sec_code, interval, param=param, count=10
        )
        rows = response.get('data') or []
        if not isinstance(rows, list):
            return
        with self._get_candle_poll_lock():
            previous = self._candle_poll_current.get(key)
        current, closed = self._closed_candles_after(rows, previous)
        with self._get_candle_poll_lock():
            self._candle_poll_current[key] = current
        for candle in closed:
            self._emit_new_candle(candle, source='poll')

    def _candle_poll_handler(self):
        while not self.callback_exit_event.wait(self.candle_poll_interval):
            subscriptions = [
                item for item in self._subscriptions_snapshot()
                if item.get('subscription') == 'candles'
            ]
            for subscription in subscriptions:
                if self.callback_exit_event.is_set():
                    return
                try:
                    self._poll_candle_subscription(subscription)
                except Exception as exc:
                    if self.callback_exit_event.is_set():
                        return
                    self.last_error = exc
                    self.logger.debug(
                        'Candle fallback poll failed for %r',
                        subscription,
                        exc_info=True,
                    )

    def subscribe_to_candles(self, class_code, sec_code, interval, param='-', trans_id=0):  # QUIK#
        """Подписка на свечи

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int interval: Кол-во в минутах: 0 (тик), 1, 2, 3, 4, 5, 6, 10, 15, 20, 30, 60 (1 час), 120 (2 часа), 240 (4 часа), 1440 (день), 10080 (неделя), 23200 (месяц)
        :param str param: Если параметр не задан, то заказываются данные на основании Таблицы обезличенных сделок, если задан – данные по этому параметру
        :param int trans_id: Код транзакции
        """
        result = self.process_request({'data': f'{class_code}|{sec_code}|{interval}|{param}', 'id': trans_id, 'cmd': 'subscribe_to_candles', 't': ''})
        subscription = {'subscription': 'candles', 'class_code': class_code, 'sec_code': sec_code, 'interval': interval, 'param': param}  # Подписка
        if self.is_subscribed(class_code, sec_code, interval, param).get('data', False):
            self._reset_candle_poll_state(class_code, sec_code, interval, param)
            self._remember_subscription(subscription)
        return result

    def unsubscribe_from_candles(self, class_code, sec_code, interval, param='-', trans_id=0):  # QUIK#
        """Отмена подписки на свечи

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int interval: Кол-во в минутах: 0 (тик), 1, 2, 3, 4, 5, 6, 10, 15, 20, 30, 60 (1 час), 120 (2 часа), 240 (4 часа), 1440 (день), 10080 (неделя), 23200 (месяц)
        :param str param: Если параметр не задан, то заказываются данные на основании Таблицы обезличенных сделок, если задан – данные по этому параметру
        :param int trans_id: Код транзакции
        """
        result = self.process_request({'data': f'{class_code}|{sec_code}|{interval}|{param}', 'id': trans_id, 'cmd': 'unsubscribe_from_candles', 't': ''})
        subscription = {'subscription': 'candles', 'class_code': class_code, 'sec_code': sec_code, 'interval': interval, 'param': param}  # Подписка
        if not self.is_subscribed(class_code, sec_code, interval, param).get('data', False):
            self._forget_subscription(subscription)
            self._reset_candle_poll_state(class_code, sec_code, interval, param)
        return result

    def is_subscribed(self, class_code, sec_code, interval, param='-', trans_id=0):  # QUIK#
        """Есть ли подписка на свечи

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int interval: Кол-во в минутах: 0 (тик), 1, 2, 3, 4, 5, 6, 10, 15, 20, 30, 60 (1 час), 120 (2 часа), 240 (4 часа), 1440 (день), 10080 (неделя), 23200 (месяц)
        :param str param: Если параметр не задан, то заказываются данные на основании Таблицы обезличенных сделок, если задан – данные по этому параметру
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{sec_code}|{interval}|{param}', 'id': trans_id, 'cmd': 'is_subscribed', 't': ''})

    # 3.11 Функции для работы с заявками

    def send_transaction(self, transaction, trans_id=0):  # 3.11.1 Функция предназначена для отправки транзакций в торговую систему
        """Отправка транзакции в торговую систему

        :param dict transaction: Транзакция в виде словаря. Формат и правила формирования описаны в Руководстве пользователя QUIK https://arqatech.com/ru/support/files/ Файл 6. Совместная работа с другими приложениями. Пункт 6.9.2
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': transaction, 'id': trans_id, 'cmd': 'sendTransaction', 't': ''})

    # CalcBuySell - 3.11.2. Максимальное кол-во лотов в заявке

    # 3.12 Функции для получения значений таблицы "Текущие торги"

    def get_param_ex(self, class_code, sec_code, param_name, trans_id=0):  # 3.12.1 Функция предназначена для получения значений всех параметров биржевой информации из таблицы Текущие торги. С помощью этой функции можно получить любое из значений Таблицы текущих торгов для заданных кодов класса и инструмента
        """Таблица текущих торгов

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param str param_name: Параметр
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{sec_code}|{param_name}', 'id': trans_id, 'cmd': 'getParamEx', 't': ''})

    def get_param_ex2(self, class_code, sec_code, param_name, trans_id=0):  # 3.12.2 Функция предназначена для получения значений всех параметров биржевой информации из Таблицы текущих торгов с возможностью в дальнейшем отказаться от получения определенных параметров, заказанных с помощью функции ParamRequest
        """Таблица текущих торгов по инструменту с возможностью отказа от получения

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param str param_name: Параметр
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{sec_code}|{param_name}', 'id': trans_id, 'cmd': 'getParamEx2', 't': ''})

    def get_param_ex2_bulk(self, class_sec_codes_params, trans_id=0):  # QUIK#
        """Таблица текущих торгов по инструментам с возможностью отказа от получения

        :param set[str] class_sec_codes_params: Список кодов режимов торгов, тикеров, параметров. Например: {'TQBR|SBER|SEC_SCALE', 'SPBFUT|CNYRUBF|SEC_PRICE_STEP'}
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': class_sec_codes_params, 'id': trans_id, 'cmd': 'getParamEx2Bulk', 't': ''})

    # 3.13 Функции для получения параметров таблицы "Клиентский портфель"

    def get_portfolio_info(self, firm_id, client_code, trans_id=0):  # 3.13.1 Функция предназначена для получения значений параметров таблицы Клиентский портфель, соответствующих идентификатору участника торгов firmid, коду клиента client_code и сроку расчетов limit_kind со значением 0
        """Клиентский портфель

        :param str firm_id: Код фирмы
        :param str client_code: Код клиента
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{firm_id}|{client_code}', 'id': trans_id, 'cmd': 'getPortfolioInfo', 't': ''})

    def get_portfolio_info_ex(self, firm_id, client_code, limit_kind, trans_id=0):  # 3.13.2 Функция предназначена для получения значений параметров таблицы Клиентский портфель, соответствующих идентификатору участника торгов firmid, коду клиента client_code и сроку расчетов limit_kind со значением, заданным пользователем.
        """Клиентский портфель по сроку расчетов

        :param str firm_id: Код фирмы
        :param str client_code: Код клиента
        :param int limit_kind: Срок расчетов
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{firm_id}|{client_code}|{limit_kind}', 'id': trans_id, 'cmd': 'getPortfolioInfoEx', 't': ''})

    # 3.14 Функции для получения параметров таблицы "Купить/Продать"

    # getBuySellInfo - 3.14.1. Параметры таблицы купить/продать
    # getBuySellInfoEx - 3.14.2. Параметры таблицы купить/продать с дополнительными полями вывода

    # 3.15 Функции для работы с таблицами Рабочего места QUIK

    # AddColumn - 3.15.1. Добавление колонки в таблицу
    # AllocTable - 3.15.2. Структура, описывающая таблицу
    # Clear - 3.15.3. Удаление содержимого таблицы
    # CreateWindow - 3.15.4. Создание окна таблицы
    # DeleteRow - 3.15.5. Удаление строки из таблицы
    # DestroyTable - 3.15.6. Закрытие окна таблицы
    # InsertRow - 3.15.7. Добавление строки в таблицу
    # IsWindowClosed - 3.15.8. Закрыто ли окно с таблицей
    # GetCell - 3.15.9. Данные ячейки таблицы
    # GetTableSize - 3.15.10. Кол-во строк и столбцов таблицы
    # GetWindowCaption - 3.15.11. Заголовок окна таблицы
    # GetWindowRect - 3.15.12. Координаты верхнего левого и правого нижнего углов таблицы
    # SetCell - 3.15.13. Установка значения ячейки таблицы
    # SetWindowCaption - 3.15.14. Установка заголовка окна таблицы
    # SetWindowPos - 3.15.15. Установка верхнего левого угла, и размеры таблицы
    # SetTableNotificationCallback - 3.15.16. Установка функции обратного вызова для обработки событий в таблице
    # RGB - 3.15.17. Преобразование каждого цвета в одно число для функци SetColor
    # SetColor - 3.15.18. Установка цвета ячейки, столбца или строки таблицы
    # Highlight - 3.15.19. Подсветка диапазона ячеек цветом фона и цветом текста на заданное время с плавным затуханием
    # SetSelectedRow - 3.15.20. Выделение строки таблицы

    # 3.16 Функции для работы с метками

    def add_label(self, price, cur_date, cur_time, qty, path, chart_tag, alignment, background, trans_id=0):  # 3.16.1 Добавляет метку с заданными параметрами
        """Добавление метки на график"""
        return self.process_request({'data': f'{price}|{cur_date}|{cur_time}|{qty}|{path}|{chart_tag}|{alignment}|{background}', 'id': trans_id, 'cmd': 'AddLabel', 't': ''})

    def del_label(self, chart_tag, label_id, trans_id=0):  # 3.16.2 Удаляет метку с заданными параметрами
        """Удаление метки с графика"""
        return self.process_request({'data': f'{chart_tag}|{label_id}', 'id': trans_id, 'cmd': 'DelLabel', 't': ''})

    def del_all_labels(self, chart_tag, trans_id=0):  # 3.16.3 Команда удаляет все метки на диаграмме с указанным графиком
        """Удаление всех меток с графика"""
        return self.process_request({'data': chart_tag, 'id': trans_id, 'cmd': 'DelAllLabels', 't': ''})

    def get_label_params(self, chart_tag, label_id, trans_id=0):  # 3.16.4 Команда позволяет получить параметры метки
        """Получение параметров метки"""
        return self.process_request({'data': f'{chart_tag}|{label_id}', 'id': trans_id, 'cmd': 'GetLabelParams', 't': ''})

    # SetLabelParams - 3.16.5. Установка параметров метки

    # 3.17 Функции для заказа стакана котировок

    def subscribe_level2_quotes(self, class_code, sec_code, trans_id=0):  # 3.17.1 Функция заказывает на сервер получение стакана по указанному классу и инструменту
        """Подписка на стакан

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int trans_id: Код транзакции
        """
        result = self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'Subscribe_Level_II_Quotes', 't': ''})
        subscription = {'subscription': 'quotes', 'class_code': class_code, 'sec_code': sec_code}  # Подписка
        if self.is_subscribed_level2_quotes(class_code, sec_code).get('data', False):
            self._remember_subscription(subscription)
        return result

    def unsubscribe_level2_quotes(self, class_code, sec_code, trans_id=0):  # 3.17.2 Функция отменяет заказ на получение с сервера стакана по указанному классу и инструменту
        """Отмена подписки на стакан

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int trans_id: Код транзакции
        """
        result = self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'Unsubscribe_Level_II_Quotes', 't': ''})
        subscription = {'subscription': 'quotes', 'class_code': class_code, 'sec_code': sec_code}  # Подписка
        if not self.is_subscribed_level2_quotes(class_code, sec_code).get('data', False):
            self._forget_subscription(subscription)
        return result

    def is_subscribed_level2_quotes(self, class_code, sec_code, trans_id=0):  # 3.17.3 Функция позволяет узнать, заказан ли с сервера стакан по указанному классу и инструменту
        """Есть ли подписка на стакан

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'IsSubscribed_Level_II_Quotes', 't': ''})

    # 3.18 Функции для заказа параметров Таблицы текущих торгов

    def param_request(self, class_code, sec_code, param_name, trans_id=0):  # 3.18.1 Функция заказывает получение параметров Таблицы текущих торгов
        """Заказ получения таблицы текущих торгов по инструменту

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param str param_name: Параметр
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{sec_code}|{param_name}', 'id': trans_id, 'cmd': 'paramRequest', 't': ''})

    def cancel_param_request(self, class_code, sec_code, param_name, trans_id=0):  # 3.18.2 Функция отменяет заказ на получение параметров Таблицы текущих торгов
        """Отмена заказа получения таблицы текущих торгов по инструменту

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param str param_name: Параметр
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{class_code}|{sec_code}|{param_name}', 'id': trans_id, 'cmd': 'cancelParamRequest', 't': ''})

    def param_request_bulk(self, class_sec_codes_params, trans_id=0):  # QUIK#
        """Заказ получения таблицы текущих торгов по инструментам

        :param set[str] class_sec_codes_params: Список кодов режимов торгов, тикеров, параметров. Например: {'TQBR|SBER|SEC_SCALE', 'SPBFUT|CNYRUBF|SEC_PRICE_STEP'}
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': class_sec_codes_params, 'id': trans_id, 'cmd': 'paramRequestBulk', 't': ''})

    def cancel_param_request_bulk(self, class_sec_codes_params, trans_id=0):  # QUIK#
        """Отмена заказа получения таблицы текущих торгов по инструментам

        :param set[str] class_sec_codes_params: Список кодов режимов торгов, тикеров, параметров. Например: {'TQBR|SBER|SEC_SCALE', 'SPBFUT|CNYRUBF|SEC_PRICE_STEP'}
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': class_sec_codes_params, 'id': trans_id, 'cmd': 'cancelParamRequestBulk', 't': ''})

    # 3.19 Функции для получения информации по единой денежной позиции

    def get_trd_acc_by_client_code(self, firm_id, client_code, trans_id=0):  # 3.19.1 Функция возвращает торговый счет срочного рынка, соответствующий коду клиента фондового рынка с единой денежной позицией
        """Торговый счет срочного рынка по коду клиента фондового рынка

        :param str firm_id: Код фирмы
        :param str client_code: Код клиента
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{firm_id}|{client_code}', 'id': trans_id, 'cmd': 'getTrdAccByClientCode', 't': ''})

    def get_client_code_by_trd_acc(self, firm_id, trade_account_id, trans_id=0):  # 3.19.2 Функция возвращает код клиента фондового рынка с единой денежной позицией, соответствующий торговому счету срочного рынка
        """Код клиента фондового рынка с единой денежной позицией по торговому счету срочного рынка

        :param str firm_id: Код фирмы
        :param str trade_account_id: Счет
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{firm_id}|{trade_account_id}', 'id': trans_id, 'cmd': 'getClientCodeByTrdAcc', 't': ''})

    def is_ucp_client(self, firm_id, client, trans_id=0):  # 3.19.3 Функция предназначена для получения признака, указывающего имеет ли клиент единую денежную позицию
        """Имеет ли клиент единую денежную позицию

        :param str firm_id: Код фирмы
        :param str client: Код клиента фондового рынка или торговый счет срочного рынка
        :param int trans_id: Код транзакции
        """
        return self.process_request({'data': f'{firm_id}|{client}', 'id': trans_id, 'cmd': 'IsUcpClient', 't': ''})

    # Запросы

    def _connect_request_socket(self) -> None:
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError('QuikPy уже закрыт')
            if self.socket_requests is not None:
                return
            sock = socket(AF_INET, SOCK_STREAM)
            sock.settimeout(self.socket_timeout)
            sock.connect((self.host, self.requests_port))
            self.socket_requests = sock

    @staticmethod
    def _close_socket(sock) -> None:
        if sock is None:
            return
        try:
            sock.shutdown(SHUT_RDWR)
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass

    def process_request(self, request):
        """Send one JSON request and receive one JSON response safely.

        Requests are serialized by a lock. Network failures always release the
        lock and are surfaced to the caller; non-idempotent transactions are
        never retried automatically.
        """
        raw_data = (dumps(request, ensure_ascii=False, separators=(',', ':')) + '\r\n').encode('cp1251')
        with self.lock:
            if self.socket_requests is None:
                self._connect_request_socket()
            fragments = bytearray()
            try:
                self.socket_requests.sendall(raw_data)
                while True:
                    fragment = self.socket_requests.recv(self.buffer_size)
                    if fragment == b'':
                        raise ConnectionError('QUIK закрыл request-соединение')
                    fragments.extend(fragment)
                    if len(fragments) > self.max_message_size:
                        raise ValueError('Ответ QUIK превышает допустимый размер')
                    try:
                        result = loads(fragments.decode('cp1251'))
                    except JSONDecodeError:
                        continue
                    self.healthy = True
                    return result
            except Exception as exc:
                self.healthy = False
                self.last_error = exc
                self._close_socket(self.socket_requests)
                self.socket_requests = None
                raise

    # Подписки (функции обратного вызова)

    def callback_handler(self):
        """Receive callbacks with reconnect, timeout and exception isolation."""
        while not self.callback_exit_event.is_set():
            try:
                pending = ''
                callbacks = socket(AF_INET, SOCK_STREAM)
                callbacks.settimeout(1.0)
                callbacks.connect((self.host, self.callbacks_port))
                self.callback_socket = callbacks
                self.healthy = True
                while not self.callback_exit_event.is_set():
                    try:
                        fragment = callbacks.recv(self.buffer_size)
                    except SocketTimeout:
                        continue
                    if fragment == b'':
                        raise ConnectionError('QUIK закрыл callback-соединение')
                    pending += fragment.decode('cp1251')
                    if len(pending.encode('cp1251', errors='ignore')) > self.max_message_size:
                        raise ValueError('Callback QUIK превышает допустимый размер')
                    while '\n' in pending:
                        line, pending = pending.split('\n', 1)
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            data = loads(line)
                            self._dispatch_callback(data)
                        except JSONDecodeError:
                            self.logger.exception('Некорректный JSON callback: %r', line)
                        except Exception as exc:
                            self.last_error = exc
                            self.logger.exception('Ошибка обработки callback QUIK')
                            self.on_error.trigger({
                                'cmd': 'python_callback_error',
                                'data': {'error': repr(exc)},
                            })
            except Exception as exc:
                if self.callback_exit_event.is_set():
                    break
                self.healthy = False
                self.last_error = exc
                self.logger.exception('Callback-соединение потеряно; повтор через %.1f с', self.callback_retry_delay)
                self.callback_exit_event.wait(self.callback_retry_delay)
            finally:
                self._close_socket(self.callback_socket)
                self.callback_socket = None
        self.healthy = False

    def _dispatch_callback(self, data):
        cmd = data.get('cmd')
        if cmd == 'NewCandle':
            self._emit_new_candle(data.get('data') or {}, source='native', envelope=data)
            return
        events = {
            'OnFirm': self.on_firm,
            'OnAllTrade': self.on_all_trade,
            'OnTrade': self.on_trade,
            'OnOrder': self.on_order,
            'OnAccountBalance': self.on_account_balance,
            'OnFuturesLimitChange': self.on_futures_limit_change,
            'OnFuturesLimitDelete': self.on_futures_limit_delete,
            'OnFuturesClientHolding': self.on_futures_client_holding,
            'OnMoneyLimit': self.on_money_limit,
            'OnMoneyLimitDelete': self.on_money_limit_delete,
            'OnDepoLimit': self.on_depo_limit,
            'OnDepoLimitDelete': self.on_depo_limit_delete,
            'OnAccountPosition': self.on_account_position,
            'OnStopOrder': self.on_stop_order,
            'OnTransReply': self.on_trans_reply,
            'OnParam': self.on_param,
            'OnQuote': self.on_quote,
            'OnDisconnected': self.on_disconnected,
            'OnClose': self.on_close,
            'OnStop': self.on_stop,
            'OnInit': self.on_init,
            'lua_error': self.on_error,
        }
        if cmd == 'OnConnected':
            self._restore_subscriptions()
            self.on_connected.trigger(data)
            return
        event = events.get(cmd)
        if event is None:
            self.logger.warning('Неизвестный callback QUIK: %r', data)
            return
        event.trigger(data)

    def _restore_subscriptions(self):
        """Restore subscriptions after reconnect; errors are logged per item."""
        for subscription in self._subscriptions_snapshot():
            try:
                class_code = subscription['class_code']
                sec_code = subscription['sec_code']
                if subscription['subscription'] == 'quotes':
                    if not self.is_subscribed_level2_quotes(class_code, sec_code).get('data'):
                        self.subscribe_level2_quotes(class_code, sec_code)
                elif subscription['subscription'] == 'candles':
                    interval = subscription['interval']
                    param = subscription['param']
                    if not self.is_subscribed(class_code, sec_code, interval, param).get('data'):
                        self.subscribe_to_candles(class_code, sec_code, interval, param)
            except Exception:
                self.logger.exception('Не удалось восстановить подписку %r', subscription)

    # Выход и закрытие

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection_and_thread()

    def __del__(self):
        try:
            self.close_connection_and_thread()
        except Exception:
            pass

    def close_connection_and_thread(self):
        """Idempotently close both sockets and join the callback thread."""
        lock = getattr(self, '_lifecycle_lock', None)
        if lock is None:
            return
        with lock:
            if getattr(self, '_closed', False):
                return
            self._closed = True
            exit_event = getattr(self, 'callback_exit_event', None)
            if exit_event is not None:
                exit_event.set()
            self._close_socket(getattr(self, 'callback_socket', None))
            self.callback_socket = None
            self._close_socket(getattr(self, 'socket_requests', None))
            self.socket_requests = None
        threads = (
            ('Callback', getattr(self, 'callback_thread', None)),
            ('Candle poll', getattr(self, 'candle_poll_thread', None)),
        )
        for label, thread in threads:
            if thread is not None and thread.is_alive() and thread is not current_thread():
                thread.join(timeout=5.0)
                if thread.is_alive():
                    self.logger.error('%s thread did not stop within 5 seconds', label)
        self.healthy = False

    # Функции конвертации

    def dataname_to_class_sec_codes(self, dataname) -> tuple[str, str] | None:
        """Код режима торгов и тикер из названия тикера

        :param str dataname: Название тикера
        :return: Код режима торгов и тикер
        """
        symbol_parts = dataname.split('.')  # По разделителю пытаемся разбить тикер на части
        if len(symbol_parts) >= 2:  # Если тикер задан в формате <Код режима торгов>.<Код тикера>
            class_code = symbol_parts[0]  # Код режима торгов
            sec_code = '.'.join(symbol_parts[1:])  # Код тикера
        else:  # Если тикер задан без кода режима торгов
            sec_code = dataname  # Код тикера
            class_codes = self.get_classes_list()['data']  # Все режимы торгов через запятую
            class_code = self.get_security_class(class_codes, sec_code)['data']  # Код режима торгов из всех режимов по тикеру
        return class_code, sec_code

    @staticmethod
    def class_sec_codes_to_dataname(class_code, sec_code):
        """Название тикера из кода режима торгов и кода тикера

        :param str class_code: Код режима торгов
        :param str sec_code: Код тикера
        :return: Название тикера
        """
        return f'{class_code}.{sec_code}'

    def get_symbol_info(self, class_code, sec_code, reload=False):
        """Спецификация тикера

        :param str class_code: Код режима торгов
        :param str sec_code: Код тикера
        :param bool reload: Получить информацию из QUIK
        :return: Значение из кэша/QUIK или None, если тикер не найден
        """
        if reload or (class_code, sec_code) not in self.symbols:  # Если нужно получить информацию из QUIK или нет информации о тикере в справочнике
            symbol_info = self.get_security_info(class_code, sec_code)  # Получаем информацию о тикере из QUIK
            if 'data' not in symbol_info:  # Если ответ не пришел (возникла ошибка). Например, для опциона
                self.logger.error(f'Информация о {self.class_sec_codes_to_dataname(class_code, sec_code)} не найдена')
                return None  # то возвращаем пустое значение
            self.symbols[(class_code, sec_code)] = symbol_info['data']  # Заносим информацию о тикере в справочник
        return self.symbols[(class_code, sec_code)]  # Возвращаем значение из справочника

    @staticmethod
    def timeframe_to_quik_timeframe(tf) -> tuple[int, bool]:
        """Перевод временнОго интервала во временной интервал QUIK

        :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        :return: Временной интервал QUIK, внутридневной интервал
        """
        if 'MN' in tf:  # Месячный временной интервал
            return 23200, False
        if tf[0:1] == 'W':  # Недельный временной интервал
            return 10080, False
        if tf[0:1] == 'D':  # Дневной временной интервал
            return 1440, False
        if tf[0:1] == 'M':  # Минутный временной интервал
            minutes = int(tf[1:])  # Кол-во минут
            if minutes in (1, 2, 3, 4, 5, 6, 10, 15, 20, 30, 60, 120, 240):  # Разрешенные временнЫе интервалы в QUIK
                return minutes, True
        raise NotImplementedError  # С остальными временнЫми интервалами не работаем, в т.ч. и с тиками (интервал = 0)

    @staticmethod
    def quik_timeframe_to_timeframe(tf) -> tuple[str, bool]:
        """Перевод временнОго интервала QUIK во временной интервал

        :param int tf: Временной интервал QUIK
        :return: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм, внутридневной интервал
        """
        if tf == 23200:  # Месячный временной интервал
            return 'MN1', False
        if tf == 10080:  # Недельный временной интервал
            return 'W1', False
        if tf == 1440:  # Дневной временной интервал
            return 'D1', False
        if tf in (1, 2, 3, 4, 5, 6, 10, 15, 20, 30, 60, 120, 240):  # Минутный временной интервал
            return f'M{tf}', True
        raise NotImplementedError  # С остальными временнЫми интервалами не работаем , в т.ч. и с тиками (интервал = 0)

    def price_to_valid_price(self, class_code, sec_code, quik_price, rounding='nearest') -> int | float:
        """Return a price aligned to the instrument tick using Decimal.

        ``rounding`` may be ``nearest`` (default), ``floor`` or ``ceil``.
        """
        si = self.get_symbol_info(class_code, sec_code)
        if not si:
            return quik_price
        step = Decimal(str(si['min_price_step']))
        if step <= 0:
            raise ValueError(f'Некорректный шаг цены {step} для {class_code}.{sec_code}')
        value = Decimal(str(quik_price)) / step
        modes = {
            'nearest': ROUND_HALF_UP,
            'floor': ROUND_FLOOR,
            'ceil': ROUND_CEILING,
        }
        try:
            mode = modes[rounding]
        except KeyError as exc:
            raise ValueError(f'Неизвестный режим округления цены: {rounding}') from exc
        valid = value.quantize(Decimal('1'), rounding=mode) * step
        scale = int(si.get('scale', 0))
        if scale > 0:
            return round(float(valid), scale)
        return int(valid)

    def price_to_quik_price(self, class_code, sec_code, price) -> int | float:
        """Перевод цены в рублях за штуку в цену QUIK

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param float price: Цена в рублях за штуку
        :return: Цена в QUIK
        """
        si = self.get_symbol_info(class_code, sec_code)  # Спецификация тикера
        if not si:  # Если тикер не найден
            return price  # то цена не изменяется
        min_price_step = si['min_price_step']  # Шаг цены
        quik_price = price  # Изначально считаем, что цена не изменится
        if class_code in ('TQOB', 'TQCB', 'TQRD', 'TQIR'):  # Для облигаций (Т+ Гособлигации, Т+ Облигации, Т+ Облигации Д, Т+ Облигации ПИР)
            quik_price = price * 100 / si['face_value']  # Пункты цены для котировок облигаций представляют собой проценты номинала облигации
        elif class_code == 'SPBFUT':  # Для рынка фьючерсов
            lot_size = si['lot_size']  # Лот
            step_price = float(self.get_param_ex(class_code, sec_code, 'STEPPRICE')['data']['param_value'])  # Стоимость шага цены
            if lot_size > 1 and step_price:  # Если есть лот и стоимость шага цены
                lot_price = price * lot_size  # Цена в рублях за лот
                quik_price = lot_price * min_price_step / step_price  # Цена в рублях за штуку
        return self.price_to_valid_price(class_code, sec_code, quik_price)  # Возращаем цену, которую примет QUIK в заявке

    def quik_price_to_price(self, class_code, sec_code, quik_price) -> float:
        """Перевод цены QUIK в цену в рублях за штуку

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param float quik_price: Цена в QUIK
        :return: Цена в рублях за штуку
        """
        si = self.get_symbol_info(class_code, sec_code)  # Спецификация тикера
        if not si:  # Если тикер не найден
            return quik_price  # то цена не изменяется
        if class_code in ('TQOB', 'TQCB', 'TQRD', 'TQIR'):  # Для облигаций (Т+ Гособлигации, Т+ Облигации, Т+ Облигации Д, Т+ Облигации ПИР)
            return quik_price / 100 * si['face_value']  # Пункты цены для котировок облигаций представляют собой проценты номинала облигации
        elif class_code == 'SPBFUT':  # Для рынка фьючерсов
            lot_size = si['lot_size']  # Лот
            step_price = float(self.get_param_ex(class_code, sec_code, 'STEPPRICE')['data']['param_value'])  # Стоимость шага цены
            if lot_size > 1 and step_price:  # Если есть лот и стоимость шага цены
                min_price_step = si['min_price_step']  # Шаг цены
                lot_price = quik_price // min_price_step * step_price  # Цена за лот
                return lot_price / lot_size  # Цена за штуку
        return quik_price  # В остальных случаях цена не изменяется

    def lots_to_size(self, class_code, sec_code, lots) -> int:
        """Перевод лотов в штуки

        :param str class_code: Код режима торгов
        :param str sec_code: Тикер
        :param int lots: Кол-во лотов
        :return: Кол-во штук
        """
        si = self.get_symbol_info(class_code, sec_code)  # Спецификация тикера
        if si:  # Если тикер найден
            lot_size = si['lot_size']  # Кол-во штук в лоте
            if lot_size:  # Если задано кол-во штук в лоте
                return int(lots * lot_size)  # то возвращаем кол-во в штуках
        return lots  # В остальных случаях возвращаем кол-во в лотах

    def size_to_lots(self, class_code, sec_code, size) -> int:
        """Convert units to lots without silent truncation."""
        numeric = Decimal(str(size))
        if numeric != numeric.to_integral_value():
            raise ValueError(f'Размер должен быть целым: {size}')
        integer_size = int(numeric)
        if integer_size <= 0:
            raise ValueError(f'Размер должен быть положительным: {size}')
        si = self.get_symbol_info(class_code, sec_code)
        lot_size = int(si['lot_size']) if si else 1
        if lot_size <= 0:
            raise ValueError(f'Некорректный размер лота {lot_size} для {class_code}.{sec_code}')
        if integer_size % lot_size:
            raise ValueError(
                f'Размер {integer_size} не кратен лоту {lot_size} для '
                f'{class_code}.{sec_code}'
            )
        lots = integer_size // lot_size
        if lots < 1:
            raise ValueError('Количество лотов должно быть не меньше 1')
        return lots



class Event:
    """Ordered, thread-safe event with callback exception isolation."""

    def __init__(self):
        self._callbacks: list[Any] = []
        self._lock = RLock()

    def subscribe(self, callback) -> None:
        with self._lock:
            if callback not in self._callbacks:
                self._callbacks.append(callback)

    def unsubscribe(self, callback) -> None:
        with self._lock:
            try:
                self._callbacks.remove(callback)
            except ValueError:
                pass

    def trigger(self, *args, **kwargs) -> None:
        with self._lock:
            callbacks = tuple(self._callbacks)
        for callback in callbacks:
            try:
                callback(*args, **kwargs)
            except Exception:
                logging.getLogger('QuikPy').exception(
                    'Исключение в подписчике события %r', callback
                )
