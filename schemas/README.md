# Schemas

Каталог содержит историю физических схем и визуальные артефакты модели данных.

## Файлы

- `schema_v1.drawio` — исходная согласованная версия схемы.
- `schema_v2.drawio` — схема с переходом на `client_id STRING` и согласованием ключей загрузки и партиционирования.
- `schema_v2.png` — экспорт `schema_v2.drawio` для быстрого просмотра без draw.io.
- `schema_v3.drawio` — актуальная схема текущей инкрементальной реализации с технической таблицей `tech_source_partitions` и обновлённым warehouse-контрактом.
- `schema_v3.png` — экспорт `schema_v3.drawio`.
- `schema_v4.drawio` — схема учебной Spark SQL-реализации, согласованная с PDF-заданием: поле загрузки называется `loading_id`, а `client_id` хранится как `STRING` из реальных parquet-файлов.
- `schema_v4.png` — экспорт `schema_v4.drawio`.
- `schema_v4_1.drawio` — схема учебной Spark SQL-реализации версии 4.1.
- `schema_v4_1.png` — экспорт `schema_v4_1.drawio`.

## Актуальная версия

Для текущего состояния репозитория следует считать основной именно `schema_v4_1.drawio`, поскольку она соответствует:

- простому Spark SQL notebook `notebooks/lab3_simple_solution.ipynb`;
- PDF-заданию `lab_3_task_1.pdf`;
- текущему описанию таблиц `dim_sources`, `dim_attributes`, `load_log`,
  `client_monthly_attrs_scd1` и `client_daily_attrs_scd2`.
