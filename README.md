# backtrader-quik

`backtrader-quik` — устанавливаемая Python-библиотека для получения данных и
торговли из стратегий [Backtrader](https://github.com/mementum/backtrader)
через терминал QUIK. QuikPy и Lua-коннектор входят в репозиторий: отдельное
копирование Python-модулей QuikPy больше не требуется.

Текущая версия: **1.0.0a5**. Статус: **alpha**.

Проект является функциональной заменой удалённой библиотеки Игоря Чечета 
BackTraderQuik и переименован в `backtrader_quik`, чтобы не смешиваться с ней.

> Библиотека прошла локальные regression-тесты и полный тестовый цикл заявок
> на QUIK Junior 11.4.1.3 для `QJSIM.GAZP` и `SPBFUT.RIU6`. Это не заменяет
> проверку у конкретного брокера и не является допуском к торговле реальными
> деньгами.

## Возможности

- свечная история и live-бары через `QKData`;
- несколько DataFeed и несколько тайм-фреймов в одном `Cerebro`;
- фондовые и срочные счета с явным выбором `account_id`;
- Market, Limit, Stop, StopLimit, отмена, OCO и bracket orders;
- позиции, денежные лимиты, стоимость портфеля и комиссии;
- callbacks QUIK с очередью между сетевым потоком и потоком Backtrader;
- fan-out одной подписки на несколько DataFeed;
- reconnect request/callback sockets;
- polling fallback для свечей и Market-сделок при пропущенном callback;
- корректное завершение и повторный запуск процесса;
- встроенный Lua-коннектор для QUIK/QUIK Junior.

## Что изменилось

В 1.0.0a5 проект переименован в `backtrader-quik` (импорт `backtrader_quik`),
чтобы исключить путаницу с удалённой библиотекой BackTraderQuik Игоря Чечета;
функциональных изменений кода нет.

В 1.0.0a4:

- callback-сокет запускается до первоначальных запросов к QUIK;
- Lua-коннектор переподключается и корректно очищает DataSource;
- добавлен резервный опрос свечей, если `SetUpdateCallback` не работает;
- Market-сделка восстанавливается по точному `order_num`, если QUIK Junior не
  прислал `OnTrade`; дубликаты по-прежнему отсекаются;
- неисполненная неактивная Market-заявка согласуется как `Canceled`;
- срочная сделка принимается, когда QUIK Junior записал торговый счёт в
  `client_code`;
- фондовая Stop-заявка получает положительную защитную цену вместо `PRICE=0`;
- финальные cash/value кэшируются до закрытия QuikPy;
- добавлены регрессионные проверки этих сценариев.

Подробности: [CHANGELOG.md](https://github.com/Celeevo/backtrader-quik/blob/main/CHANGELOG.md), [VALIDATION.md](https://github.com/Celeevo/backtrader-quik/blob/main/docs/VALIDATION.md) и
[результаты QUIK Junior](https://github.com/Celeevo/backtrader-quik/blob/main/docs/QUIK_JUNIOR_TEST_RESULTS_2026-07-17.md).

## Требования

- Windows 10/11;
- Python 3.10;
- Backtrader 1.9.78.123 (установится автоматически);
- QUIK или QUIK Junior с разрешённым запуском Lua-скриптов;
- свободные локальные порты 34130 и 34131.

## Установка

Из PyPI (alpha-версия требует явного флага `--pre`):

```powershell
python -m pip install --pre backtrader-quik
```

Из локального клона:

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
```

Из собранного wheel:

```powershell
python -m pip install .\dist\backtrader_quik-1.0.0a5-py3-none-any.whl
```

Скопируйте содержимое `QUIK/lua` и `QUIK/socket` в каталог, из которого QUIK
может запускать `QuikSharp.lua`, затем запустите Lua-скрипт в терминале.
Пошаговая инструкция находится в [docs/INSTALL_QUIK.md](https://github.com/Celeevo/backtrader-quik/blob/main/docs/INSTALL_QUIK.md).

## Быстрый старт

Сначала получите список счетов без отправки заявок:

```powershell
python .\Examples\list_accounts.py
```

Минимальная схема подключения:

```python
import backtrader as bt
from backtrader_quik import QKStore

store = QKStore()
broker = store.getbroker(account_id=0)

cerebro = bt.Cerebro(stdstats=False, quicknotify=True)
cerebro.setbroker(broker)

fast = store.getdata(
    dataname="QJSIM.GAZP",
    timeframe=bt.TimeFrame.Minutes,
    compression=1,
    live_bars=True,
)
cerebro.adddata(fast, name="GAZP-1m")
cerebro.resampledata(fast, timeframe=bt.TimeFrame.Minutes,
                     compression=5, name="GAZP-5m")

cerebro.addstrategy(MyStrategy)
cerebro.run(preload=False, runonce=False)
```

Один `QKBroker` обслуживает один счёт. Для другого счёта или независимого
робота используйте отдельный процесс и отдельный комплект портов.

## Размер заявки и цены

- Для фондового инструмента `size` задаётся в бумагах и должен быть кратен
  размеру лота. Библиотека не округляет неверный размер молча.
- Для фьючерса `size` задаётся в контрактах.
- Market-заявка на срочном рынке QUIK реализуется с защитной ценой. Параметр
  `slippage_steps` задаёт ширину в шагах цены; значение по умолчанию — 10.
  В тестовом QUIK Junior для `RIU6` использовалось 100 шагов из-за особенностей
  симулятора. Подбирать это значение нужно под инструмент и риск.

```python
broker = store.getbroker(account_id=2, slippage_steps=100)
```

## Безопасность эксплуатации

1. Сначала запускайте только данные и сверяйте статусы `LIVE/DELAYED`.
2. Затем тестируйте единичные заявки в QUIK Junior.
3. Перед каждым запуском сверяйте счёт, класс, код контракта, срок экспирации,
   позиции и активные заявки.
4. Не запускайте два процесса на одном комплекте портов.
5. После аварии сверяйте фактическое состояние в QUIK; локальный объект Order
   не является источником истины для уже принятой биржей заявки.

## Разработка и проверка

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q
python -m build
```

CI проверяет Windows/Linux и Python 3.10. Реальная интеграционная проверка
QUIK выполняется вручную: GitHub-hosted runner не имеет терминала и счёта.

Правила участия: [CONTRIBUTING.md](https://github.com/Celeevo/backtrader-quik/blob/main/CONTRIBUTING.md). Сообщение об уязвимости:
[SECURITY.md](https://github.com/Celeevo/backtrader-quik/blob/main/SECURITY.md).

## Происхождение и лицензирование

Проект развивает идеи и код исторического BackTraderQuik Игоря Чечета
(репозиторий удалён автором) и [QuikPy](https://github.com/cia76/QuikPy), а также практический опыт
[BacktraderQuikJunior](https://github.com/Celeevo/BacktraderQuikJunior).
Сохранены указания на Игоря Чечета и проект «Финансовая Лаборатория».

Унаследованный код Игоря Чечета распространяется на его условиях: бесплатно, с
обязательной атрибуцией автору и проекту «Финансовая Лаборатория», и не
переводится под MIT/Apache/BSD. Новый вклад этого репозитория (новый код, тесты,
документация) распространяется под лицензией MIT. Точные условия и происхождение
файлов: [LICENSE.md](https://github.com/Celeevo/backtrader-quik/blob/main/LICENSE.md) и [THIRD_PARTY_NOTICES.md](https://github.com/Celeevo/backtrader-quik/blob/main/THIRD_PARTY_NOTICES.md).

## Отказ от гарантий

Программное обеспечение предоставляется для разработки и тестирования. Автор и
участники не гарантируют исполнение заявок, доступность соединения, совместимость
с конкретной версией QUIK или пригодность для торговли реальными средствами.
