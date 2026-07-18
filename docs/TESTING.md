# Стратегия тестирования

## Автоматические тесты

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q
python -m compileall -q backtrader_quik Examples tests
python -m build
```

Регрессионный набор проверяет account scope, cash/value, long/short positions,
размер лота, Market/Limit/Stop/StopLimit, cancel, bracket/OCO, комиссии,
callback queue, reconnect, подписки, fan-out, историю, live-очередь и сценарии
QUIK Junior, добавленные в 1.0.0a4.

## Ручной интеграционный gate

Для каждого релиза на QUIK Junior:

1. список счетов и инструментов без заявок;
2. параметры рынка и свечи;
3. два тайм-фрейма из одного feed;
4. Market buy/sell;
5. исполняемый и отменяемый Limit;
6. Stop и StopLimit, включая отмену;
7. bracket и OCO;
8. восстановление после отсутствующего callback;
9. остановка процесса и повторное подключение;
10. финальная сверка позиций и активных заявок.

Матрица от 17.07.2026 приведена в
[QUIK_JUNIOR_TEST_RESULTS_2026-07-17.md](QUIK_JUNIOR_TEST_RESULTS_2026-07-17.md).

## Что CI не доказывает

GitHub Actions не проверяет конкретный терминал, брокерский шлюз, расписание
торгов, права счёта, ликвидность и фактическое исполнение. Успешный CI означает
целостность Python-пакета, а не готовность робота к реальным деньгам.

