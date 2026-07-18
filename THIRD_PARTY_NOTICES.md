# Third-party notices and provenance

Дата фиксации: 2026-07-12.

Этот файл фиксирует происхождение компонентов проверенного snapshot. Он не заменяет официальные лицензии.

## Legacy BackTraderQuik

- Исторический форк: <https://github.com/Celeevo/BackTraderQuik>
- Автор исходного проекта: Игорь Чечет, проект «Финансовая Лаборатория».
- Commit из комментария приложенного ZIP: `8bc56e54d1a89ee97a22de7e52fcb032dfc4f496`.
- Требуемая архивная ветка: `refs/heads/legacy-fork-2025-05`.
- SHA-256 приложенного ZIP: `4bc3f7599292a30a3fcf44c5b181f96139368ddf71a06449a57490f37b4eb2e5`.
- README snapshot: бесплатное распространение, обязательна ссылка на автора и проект.

## QuikPy

- Upstream: <https://github.com/cia76/QuikPy>
- Автор: Игорь Чечет, проект «Финансовая Лаборатория».
- Commit из комментария приложенного ZIP: `0425e613a1395078b1140c58bee61c604a60b677`.
- SHA-256 приложенного ZIP: `6e3273521b5eecb72fc5b25298635c9bd8ee74389d0a799aceecc0c1b810ae51`.
- Включены и изменены `backtrader_quik/QuikPy.py`, Lua-коннектор и LuaSocket binary.
- README snapshot: бесплатное распространение, обязательна ссылка на автора и проект.

Версия разработчика `1.0.0a1` указывала commit `085c3a665c8fa789392c023cadfa0a739ec0636b`, но он не совпадает с commit metadata приложенного QuikPy ZIP и поэтому не используется как provenance этой сборки.

## BacktraderQuikJunior

- Репозиторий: <https://github.com/Celeevo/BacktraderQuikJunior>
- Использованы и переработаны решения для QUIK Junior и broker helpers.
- Для окончательного публичного релиза необходимо закрепить точный commit использованного snapshot.

## Backtrader

- Upstream: <https://github.com/mementum/backtrader>
- Python dependency: `1.9.78.123`.
- Commit из комментария приложенного ZIP: `b853d7c90b6721476eb5a5ea3135224e33db1f14`.
- SHA-256 приложенного ZIP: `9cd17ced6e16109b469dee1dced7585b9e82f4bcbb2d73ba1dcc6945db5f300b`.
- Backtrader не вендорен в данный репозиторий и распространяется по собственной лицензии.

## Хеши неизменённых файлов коннектора

```text
core.dll       4e8ca8ff62a12e2ce792b36f18d762695db5ac2b2288f13eca73dc639ca0dbea
QuikSharp.lua  cecc9e8c7d08151f36cb075e24d3be74fb8a1004a968a45ea03a54d5323bf99e
Quik_2.lua     2544cc6a681da14a14c9f939646bee36b580f41c1a7e5ac091c53f7154b35d77
config.json    9e5df4246bb19a06507a52900345e977314ff0d9184154802e3596fcac19a0cc
dkjson.lua     1f56a6971ffce3021ece3afdc06163f10bee91264d0d29cc88bbbeb43cffd2d2
qscallbacks.lua fecc148e527860840dad73f110aa1f52806c407aca8011de5b43b69b51824508
qsfunctions.lua f98b2547fe5d7141281dee3b36f7ea69406c01b65ca0d5141b98e44dedbb5cf4
qsutils.lua    2994ca4ecf0b8d111b1c4d85e07013f38ff5949acc9b64288c69500756cf3544
socket.lua     95cd324ffcc020b1ac446df32f478a4e37c7f5dc1abaf4fa6d3604763853ad01
```

Перед распространением `core.dll` проверьте требования безопасности и условия происхождения binary в вашей организации.
