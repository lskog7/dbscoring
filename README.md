# dbscoring

## Назначение

Репозиторий содержит лабораторную реализацию инкрементального витринного хранилища для клиентских атрибутов.
Основной контур построен на `Spark SQL` для локального PySpark из `uv`-окружения.

Контур поддерживает единый контракт хранилища, одни и те же исходные партиции и одинаковые
правила принятия решений `new / skip / fail`.

## Актуальный контракт решения

### Источники

- `client_cards_daily` — daily-источник, партиция `row_actual_to='YYYY-MM-DD'`;
- `credit_cards_info` — monthly-источник, партиция `report_dt='YYYY-MM-DD'`;
- `deb_cards_info` — monthly-источник, партиция `report_dt='YYYY-MM-DD'`.

### Бизнес-ключ

- `client_id STRING` — единый бизнес-ключ клиента;
- глобальный surrogate key не используется, поскольку он мешает инкрементальной обработке очень больших наборов данных.

### Таблицы хранилища

- `dim_sources` — справочник источников;
- `dim_attributes` — справочник атрибутов;
- `load_log` — append-only журнал загрузок;
- `client_monthly_attrs_scd1` — monthly EAV-витрина, партиция `report_dt`;
- `client_daily_attrs_scd2` — daily EAV-витрина, партиция `row_actual_to`.

## Логика запуска

- создаёт таблицы только при необходимости;
- повторно уже обработанную партицию пропускает.

## Карта модулей

| Путь | Назначение |
| --- | --- |
| [notebooks](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/notebooks) | Основные notebook-артефакты со Spark-реализацией |
| [tests](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests) | Unit, integration и smoke-тесты |
| [schemas](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/schemas) | Версии физической схемы и визуальные артефакты |
| [data](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/data) | Локальные данные, источники и выгрузки хранилища |
| [models](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/models) | Артефакты модели scoring-контура |
| [pyproject.toml](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/pyproject.toml) | Минимальная конфигурация проекта и test-runtime |
| [REPORT_REQUIREMENTS.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/REPORT_REQUIREMENTS.md) | Требования к будущему итоговому отчёту |

## Notebook-ориентированная архитектура

Исходником production-логики являются notebook-файлы, а не отдельный Python-пакет.
Это означает следующее:

- функции определяются непосредственно в notebook;
- промежуточные демонстрации не дублируют production-код отдельными example-ячейками;
- финальный демонстрационный запуск помечен тегом `final_run` и работает от реальных parquet-источников;
- тесты загружают и исполняют notebook-программу через `nbformat`/`json`, не дублируя бизнес-логику.

Такой подход соответствует формату лабораторной работы и одновременно оставляет решение тестируемым.

## Как запускать

### Подготовка окружения

Проект использует `uv` и Python `3.12`.

Примеры команд:

```bash
uv sync
env UV_CACHE_DIR=/private/tmp/uv-cache uv run --python 3.12 pytest tests -q
```

### Lab3 notebooks

Файлы:

- [notebooks/lab3_learning_version.ipynb](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/notebooks/lab3_learning_version.ipynb)
- [notebooks/lab3_teacher_version.ipynb](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/notebooks/lab3_teacher_version.ipynb)

Требования:

- установленный `pyspark` из зависимостей проекта;
- доступная Java, совместимая с PySpark;
- доступ к parquet-источникам на локальной файловой системе.

## Что проверяется тестами

- корректность разрешения путей запуска;
- соответствие физических схем `schema.png`;
- корректность wide-to-EAV трансформаций на уровне контракта;
- синхронность script/notebook-версий;
- компилируемость Python-кода и notebook-структуры.

## Связанные документы

- [notebooks/README.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/notebooks/README.md)
- [tests/README.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/README.md)
- [schemas/README.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/schemas/README.md)
- [data/README.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/data/README.md)
- [models/README.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/models/README.md)
