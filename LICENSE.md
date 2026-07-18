# Лицензирование и условия распространения

Этот проект состоит из двух частей с разными правообладателями. Условия ниже —
итоговые и обязательные к исполнению при распространении.

## 1. Унаследованный код (BackTraderQuik и QuikPy)

Правообладатель: Игорь Чечет, проект «Финансовая Лаборатория».

Автор разрешил бесплатное использование и распространение при обязательном
сохранении авторства и ссылки на автора и проект. Это условие действует для
всего унаследованного кода в репозитории (в том числе `backtrader_quik/QuikPy.py`,
Lua-коннектор и связанные файлы). При любом распространении этой сборки
обязательно сохранять авторство Игоря Чечета и notices из
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

Унаследованный код не лицензирован под MIT/Apache/BSD и не переводится под них
этим репозиторием.

## 2. Новый вклад проекта (лицензия MIT)

Новый самостоятельный код, тесты и документация, добавленные в этом репозитории
поверх унаследованного кода, распространяются под лицензией MIT:

```
MIT License

Copyright (c) 2026 backtrader-quik contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

Лицензия MIT распространяется только на новый вклад и не отменяет условие
атрибуции для унаследованного кода из раздела 1.

## 3. Backtrader

Backtrader не вендорен в этот репозиторий и устанавливается как внешняя
зависимость. Его лицензия и обязательства применяются отдельно.

## 4. Отказ от гарантий

Программное обеспечение предназначено для разработки и тестирования торговых
систем. Оно предоставляется без гарантии корректности исполнения заявок,
доступности соединения, соответствия конкретной версии QUIK или пригодности для
торговли реальными средствами. Пользователь обязан выполнить собственную
проверку и несёт торговые и операционные риски.
