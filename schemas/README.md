# Schemas

Каталог содержит историю физических схем и визуальные артефакты модели данных.

## Файлы

- `schema_v1.drawio` — исходная согласованная версия схемы.
- `schema_v2.drawio` — схема с переходом на `client_id STRING` и согласованием ключей загрузки и партиционирования.
- `schema_v2.png` — экспорт `schema_v2.drawio` для быстрого просмотра без draw.io.
- `schema_v3.drawio` — актуальная схема текущей инкрементальной реализации с технической таблицей `tech_source_partitions` и обновлённым warehouse-контрактом.
- `schema_v3.png` — экспорт `schema_v3.drawio`.

## Актуальная версия

Для текущего состояния репозитория следует считать основной именно `schema_v3.drawio`, поскольку она соответствует:

- Spark notebook;
- Polars notebook;
- автоматическим тестам;
- текущему описанию таблиц `load_log`, `tech_source_partitions`,
  `client_monthly_attrs_scd1` и `client_daily_attrs_scd2`.
