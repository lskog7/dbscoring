# Tests

## Назначение каталога

Каталог содержит автоматические проверки notebook-first архитектуры проекта.
Тесты не копируют бизнес-логику в отдельный production-модуль, а работают поверх notebook-файлов как над
источником исполняемого кода.

## Основные принципы

- notebook остаётся source of truth;
- тесты извлекают код из `bootstrap` и `function_defs`-ячеек;
- smoke-сценарии выполняют notebook целиком;
- parity-сценарий сравнивает Spark и Polars по канонизированному бизнес-результату.

## Файлы

### [__init__.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/__init__.py)

Маркер тестового пакета.

### [notebook_loader.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/notebook_loader.py)

Служебный модуль для:

- чтения notebook JSON;
- итерации по code-cells;
- загрузки namespace из ячеек по тегам;
- полного исполнения notebook в smoke-проверках.

### [fixture_builders.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/fixture_builders.py)

Генератор тестовых parquet fixtures.

Содержит:

- построение синтетических строк источников;
- создание исходных partition directories;
- сценарий добавления новой monthly-партиции;
- сценарий мутации уже загруженной партиции;
- чтение partitioned-output таблиц с восстановлением hive partition columns из пути.

### [test_notebook_structure.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_notebook_structure.py)

Структурные проверки notebook:

- наличие examples после каждой function cell;
- наличие docstring у всех функций;
- наличие ровно одной `final_run`-ячейки.

### [test_spark_notebook_unit.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_spark_notebook_unit.py)

Unit-тесты Spark notebook-функций без полного интеграционного прогона.

### [test_spark_integration.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_spark_integration.py)

Полный Spark integration-test набор:

- первичная загрузка;
- повторный idempotent run;
- инкрементальная monthly-партиция;
- fail-fast на изменённой старой партиции;
- debug rebuild.

### [test_polars_notebook.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_polars_notebook.py)

Аналогичный набор для Polars notebook.

### [test_notebook_smoke.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_notebook_smoke.py)

Smoke-проверки полного исполнения notebook как пользовательского артефакта.

### [test_parity.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_parity.py)

Parity-проверка Spark и Polars.

Нормализует:

- временные значения;
- строковое представление чисел вроде `1` и `1.0`;
- partition columns, восстановленные из путей parquet-файлов.

## Маркеры pytest

- `spark` — тест требует локальный Spark runtime;
- `smoke` — полное выполнение notebook;
- `parity` — междвижковое сравнение Spark и Polars.

## Базовая команда запуска

```bash
env UV_CACHE_DIR=/private/tmp/uv-cache uv run --python 3.12 pytest tests -q
```
