# Notebooks

## Назначение каталога

Каталог содержит notebook-артефакты, в которых размещена основная логика лабораторной работы.
Именно notebook-файлы являются источником исполняемой ETL-программы и сопровождаются внешними тестами.

## Файлы

### [spark_lab.ipynb](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/notebooks/spark_lab.ipynb)

Основной notebook со Spark SQL реализацией.

Содержит:

- bootstrap системного Spark и Java;
- создание `SparkSession`;
- инкрементальную логику загрузки и ведения технических таблиц;
- side-effect free функции верхнего уровня;
- примеры использования после каждой функции;
- финальный пример запуска через переменные среды.

Структурные теги ячеек:

- `bootstrap` — подготовка runtime и служебных констант;
- `function_defs` — определения функций;
- `examples` — примеры сразу после функций;
- `final_run` — демонстрационный финальный запуск.

### [polars_lab.ipynb](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/notebooks/polars_lab.ipynb)

Учебный notebook с эквивалентной логикой на Polars.

Используется для:

- локального запуска без Spark;
- иллюстрации контракта инкрементальной загрузки;
- parity-проверок против Spark notebook.

По структуре повторяет Spark notebook и поддерживает те же переменные среды финального запуска.

### [typical_questions_history_attrs.ipynb](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/notebooks/typical_questions_history_attrs.ipynb)

Исторический или вспомогательный notebook с предметными вопросами по атрибутам.

Статус:

- не является источником production-контракта текущего warehouse-контура;
- не участвует в текущем наборе автоматических тестов;
- может использоваться как справочный или exploratory-материал.

## Соглашения по notebook

- все поясняющие markdown-блоки, комментарии и docstring оформляются по-русски;
- имена функций и технических сущностей остаются англоязычными и формальными;
- после каждой функции должен идти отдельный пример;
- финальный запуск должен выполняться без ручного редактирования кода, только через переменные среды.

## Переменные среды

Оба основных notebook поддерживают:

- `DBSCORING_SOURCES_ROOT`
- `DBSCORING_WAREHOUSE_ROOT`
- `DBSCORING_DEBUG_ROOT`
- `DBSCORING_RUN_MODE`
- `DBSCORING_CLEAN_DEBUG`
- `DBSCORING_SKIP_FINAL_RUN`

## Связь с тестами

Тесты в [tests](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests) читают notebook как структурированный источник:

- извлекают только tagged code-cells;
- проверяют наличие docstring;
- проверяют, что после каждой `function_defs`-ячейки следует `examples`-ячейка;
- исполняют notebook целиком в smoke-сценариях.
