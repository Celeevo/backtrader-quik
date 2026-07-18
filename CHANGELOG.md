# Changelog

## 1.0.0a5 — 2026-07-18

- проект переименован в `backtrader-quik` (импорт `backtrader_quik`), чтобы
  исключить путаницу с удалённой библиотекой BackTraderQuik Игоря Чечета;
- из репозитория убраны дубликат каталога `QUIK/` в корне, артефакты `dist/`
  и внутренние документы аудита; функциональных изменений кода нет.

## 1.0.0a4 — 2026-07-17

### QUIK Junior и транспорт

- callback-сокет поднимается до первоначальных синхронных запросов;
- Lua-коннектор получил reconnect и безопасную очистку DataSource;
- добавлен polling fallback закрытых свечей при неработающем callback;
- принятая Market-заявка восстанавливает пропущенный `OnTrade` по точному
  `order_num`, используя общую дедупликацию сделок;
- неактивная неисполненная Market-заявка согласуется как `Canceled`;
- учтён формат QUIK Junior, где торговый счёт срочной сделки попадает в
  `client_code`;
- фондовая Stop-заявка получает положительную защитную `PRICE`;
- финальный cash/value сохраняется до закрытия QuikPy.

### Проверка и поставка

- полный набор заявок пройден на `QJSIM.GAZP` и `SPBFUT.RIU6`;
- regression-набор расширен до 35 тестов;
- добавлены инструкции установки, миграции и тестирования;
- добавлены GitHub issue/PR templates, contributing и security policy;
- подготовлена русская статья о возможностях и происхождении проекта.

## 1.0.0a3 — 2026-07-12

Статус: итоговая alpha после сверки версии разработчика `1.0.0a1`, аудированной `1.0.0a2` и приложенных исходных snapshots.

### Совместимость и packaging

- версия пакета и public API обновлены до `1.0.0a3`;
- поддержка Python 3.10;
- `datetime.UTC` заменён на `timezone.utc`;
- для Windows добавлена условная зависимость `tzdata`;
- CI расширен на Windows/Linux и Python 3.10;
- добавлены `AUDIT_STATUS.md`, проверяемый provenance и безопасный Codex prompt;
- удалена неподтверждённая blanket MIT relicensing декларация.

### Торговая корректность

- сохранена модель «один QKBroker — один account_id»;
- cash/equity/positions строго ограничены выбранным счётом;
- неоднозначный `client_code` требует явного значения;
- фондовые и срочные классы не смешиваются;
- `tradeid` сохраняется;
- нецелые, нулевые и некратные лоту размеры отклоняются;
- execution bits содержат value, commission, margin и PnL;
- исправлены bracket/OCO/cancel и отложенная отмена;
- неподдерживаемая датированная `valid` обычной заявки отклоняется;
- overfill вызывает reconciliation warning вместо молчаливого игнорирования;
- начальные cash/value формируются после загрузки позиций.

### Производительность и snapshot consistency

- `getvalue()` повторно использует cash snapshot непосредственно предшествующего `getcash()`;
- явный `getcash()` остаётся синхронным refresh;
- торговое событие инвалидирует snapshot;
- для позиции используется текущий `data.close[0]`, fallback — синхронный `LAST`.

### Сеть и потоки

- provider callbacks складывают события в очереди;
- `Order`/`Position` меняются только в потоке Cerebro;
- callback errors изолируются;
- request lock освобождается через context/finally semantics;
- socket timeout, reconnect и управляемое завершение;
- буферы входящих сообщений ограничены;
- реестр subscriptions защищён блокировкой.

### DataFeed

- отдельная очередь на каждый feed;
- fan-out одинаковых подписок и reference counting;
- внешняя provider subscription не считается собственностью Store;
- `stop()` снимает только собственные подписки;
- `save_bars=False` не создаёт каталог;
- history TSV читается в UTF-8;
- повреждённые строки пропускаются;
- `QKData.stop()` идемпотентен;
- live-подписка и очередь создаются до history bootstrap, исключая окно потери закрывшегося бара;
- при ошибке bootstrap выполняется rollback созданной подписки;
- ожидание очереди учитывает `Cerebro._qcheck`, предотвращая суммирование задержек нескольких feed и busy polling.

### Логирование

- импорт не создаёт файлов;
- сохранены `configure_console_logging()` и совместимые `set_console_logging()` / `set_file_logging(logs_dir=...)`;
- библиотека удаляет только свои handlers.

### Проверки

- regression/mock suite расширен сценариями snapshot reuse, external subscription ownership, Python 3.10 grammar, logging ownership, malformed history, valid и parameter validation;
- полный перечень и результаты — в `VALIDATION.md`;
- отдельно тестируются subscription-before-history и rollback после ошибки history bootstrap.

## 1.0.0a2 — 2026-07-12

Первая полная аудированная alpha: закрыты основные блокеры `1.0.0rc1` по счёту, execution accounting, лотам, bracket/OCO/cancel, потокам, reconnect и DataFeed fan-out.

## 1.0.0a1 — версия разработчика

Частично исправлены account scope, lot validation, tradeid, logging и package metadata. Сетевые, callback, execution, fan-out и bracket/OCO blockers оставались открыты.
