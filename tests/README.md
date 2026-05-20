# Tests

## Назначение каталога

Каталог содержит автоматические проверки notebook-first архитектуры проекта.
Тесты не копируют бизнес-логику в отдельный production-модуль, а работают поверх notebook-файлов как над
источником исполняемого кода.

## Основные принципы

- текущие lab3 notebook и script-файлы проверяются как единый набор артефактов;
- тесты не требуют локальных parquet-источников;
- Spark-сценарии проверяются статически: схемы, функции, синхронность script/notebook-кода.

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

Оставлен для совместимости со старыми проверками и примерами fixtures; текущий набор тестов его не использует.

### [test_notebook_structure.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_notebook_structure.py)

Структурные проверки notebook:

- компилируемость code cells;
- отсутствие импортов `dbscoring`;
- наличие одного финального линейного запуска лабораторной.

### [test_spark_notebook_unit.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_spark_notebook_unit.py)

Unit-тесты Spark helper-кода без полного интеграционного прогона.

### [test_warehouse.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_warehouse.py)

Прямые тесты helper-функций warehouse-слоя:

- наличие трех источников;
- наличие пяти таблиц из `schema.png`;
- отсутствие старой технической таблицы `tech_source_partitions`.

### [test_spark_integration.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_spark_integration.py)

Проверяет синхронность script и notebook-версий.

### [test_notebook_smoke.py](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/test_notebook_smoke.py)

Smoke-проверки полного исполнения:

- валидность notebook JSON;
- наличие всех таблиц схемы в notebook-тексте.

## Маркеры pytest

- `spark` — тест требует локальный Spark runtime;
- `smoke` — полное выполнение notebook;

## Базовая команда запуска

```bash
env UV_CACHE_DIR=/private/tmp/uv-cache uv run --python 3.12 pytest tests -q
```
