# dbscoring

## Назначение

Репозиторий содержит лабораторную реализацию инкрементального витринного хранилища для клиентских атрибутов.
Основной контур построен в двух эквивалентных вариантах:

- `Spark SQL`-реализация для системного локального Spark;
- `Polars`-реализация для локального учебного запуска и проверки логики без кластера.

Обе реализации поддерживают один и тот же контракт хранилища, одни и те же исходные партиции и одинаковые
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
- `tech_source_partitions` — техническое состояние уже обработанных партиций;
- `client_monthly_attrs_scd1` — monthly EAV-витрина, партиция `report_dt`;
- `client_daily_attrs_scd2` — daily EAV-витрина, партиция `row_actual_to`.

## Режимы запуска

- `production`:
  - ничего не удаляет;
  - создаёт таблицы только при необходимости;
  - повторно уже обработанную и неизменённую партицию пропускает;
  - изменённую ранее загруженную партицию считает ошибкой.
- `debug`:
  - работает в отдельном debug-root;
  - по флагу может удалить только явно разрешённый debug-root;
  - после очистки выполняет rebuild в этом debug-root.

## Карта модулей

| Путь | Назначение |
| --- | --- |
| [notebooks](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/notebooks) | Основные notebook-артефакты со Spark и Polars реализациями |
| [tests](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests) | Unit, integration, smoke и parity-тесты |
| [schemas](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/schemas) | Версии физической схемы и визуальные артефакты |
| [data](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/data) | Локальные данные, источники и выгрузки хранилища |
| [models](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/models) | Артефакты модели scoring-контура |
| [pyproject.toml](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/pyproject.toml) | Минимальная конфигурация проекта и test-runtime |
| [REPORT_REQUIREMENTS.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/REPORT_REQUIREMENTS.md) | Требования к будущему итоговому отчёту |

## Notebook-ориентированная архитектура

Исходником production-логики являются notebook-файлы, а не отдельный Python-пакет.
Это означает следующее:

- функции определяются непосредственно в notebook;
- после каждой `function_defs`-ячейки следует `examples`-ячейка;
- финальный демонстрационный запуск помечен тегом `final_run`;
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

### Spark notebook

Файл: [notebooks/spark_lab.ipynb](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/notebooks/spark_lab.ipynb)

Требования:

- установленный локальный Spark;
- доступный `JAVA_HOME`, совместимый с локальным Spark;
- доступ к parquet-источникам на локальной файловой системе.

### Polars notebook

Файл: [notebooks/polars_lab.ipynb](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/notebooks/polars_lab.ipynb)

Используется как:

- локальная учебная реализация;
- быстрый способ воспроизведения контракта без Spark;
- эталон для parity-проверок.

## Переменные среды для финального прогона notebook

- `DBSCORING_SOURCES_ROOT` — корневой каталог источников;
- `DBSCORING_WAREHOUSE_ROOT` — production-root path-based warehouse;
- `DBSCORING_DEBUG_ROOT` — debug-root warehouse;
- `DBSCORING_RUN_MODE` — `production` или `debug`;
- `DBSCORING_CLEAN_DEBUG` — `1`/`0` для очистки debug-root перед запуском;
- `DBSCORING_SKIP_FINAL_RUN` — `1`, если финальную ячейку надо пропустить.

## Что проверяется тестами

- корректность валидации путей и режима запуска;
- корректность распознавания source partitions;
- стабильность manifest fingerprint;
- корректность wide-to-EAV трансформаций;
- идемпотентность `new / skip / fail`;
- полное исполнение Spark и Polars notebook;
- parity Spark и Polars по бизнес-результату.

## Связанные документы

- [notebooks/README.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/notebooks/README.md)
- [tests/README.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/README.md)
- [schemas/README.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/schemas/README.md)
- [data/README.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/data/README.md)
- [models/README.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/models/README.md)
