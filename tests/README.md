# Tests

## Назначение каталога

Каталог содержит автоматические проверки notebook-first архитектуры проекта.
Тесты не копируют бизнес-логику в отдельный production-модуль, а работают поверх notebook-файлов как над
источником исполняемого кода.

## Основные принципы

- notebook остаётся source of truth;
- тесты извлекают код из `bootstrap` и `function_defs`-ячеек;
- smoke-сценарии выполняют notebook целиком;
- Spark-сценарии проверяют контракт warehouse без альтернативного движка.

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

- построение bounded-копии реальных `data/sources` с лимитом строк на каждую физическую партицию;
- использование `data/test_sources`, если этот малый реальный срез уже материализован в репозитории;
- построение синтетических строк источников;
- создание исходных partition directories;
- сценарий добавления новой monthly-партиции;
- сценарий мутации уже загруженной партиции;
- чтение partitioned-output таблиц с восстановлением hive partition columns из пути.

### [test_notebook_structure.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_notebook_structure.py)

Структурные проверки notebook:

- отсутствие `examples`-ячеек;
- наличие docstring у всех функций;
- наличие ровно одной `final_run`-ячейки.

### [test_spark_notebook_unit.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_spark_notebook_unit.py)

Unit-тесты Spark notebook-функций без полного интеграционного прогона.

### [test_warehouse.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_warehouse.py)

Прямые тесты helper-функций warehouse-слоя:

- маппинг Spark-типов;
- построение схем;
- инициализация parquet-таблиц;
- загрузка малых справочников.

### [test_cli.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_cli.py)

Проверки CLI-контракта:

- разбор аргументов;
- JSON-вывод;
- корректное завершение Spark runtime.

### [test_spark_integration.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_spark_integration.py)

Полный Spark integration-test набор:

- bounded-прогон на реальных parquet-источниках без чтения полного объёма;
- первичная загрузка;
- повторный idempotent run;
- инкрементальная monthly-партиция;
- fail-fast на изменённой старой партиции;
- debug rebuild.

### [test_notebook_smoke.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_notebook_smoke.py)

Smoke-проверки полного исполнения:

- Spark src-контура;
- реального `spark_lab.ipynb` как пользовательского артефакта.

## Маркеры pytest

- `spark` — тест требует локальный Spark runtime;
- `smoke` — полное выполнение notebook;

## Базовая команда запуска

```bash
env UV_CACHE_DIR=/private/tmp/uv-cache uv run --python 3.12 pytest tests -q
```
