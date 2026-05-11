# TODO

## Core Delivery
- [x] Перевести проект на Python `3.12`.
- [x] Обновить `pyproject.toml`, `.python-version`, `uv.lock`.
- [x] Создать production package layout в `src/dbscoring/`.
- [x] Вынести контракты источников, атрибутов, target tables и load log в python-модули.
- [x] Реализовать Polars ETL package API для warehouse build/validate/report.
- [x] Реализовать SCD1 monthly pipeline.
- [x] Реализовать SCD2 daily pipeline.
- [x] Реализовать строгий `load_log` и `should_update`.
- [x] Создать `notebooks/polars_lab.ipynb` как документированную рабочую лабораторную.
- [x] Создать `notebooks/spark_lab.ipynb` как Colab-only зеркальную Spark-лабораторную.
- [x] Добавить Colab Spark validation cells и manifest comparison.
- [x] Реализовать fixture generator на основе реальных parquet sample schemas.
- [x] Написать ETL unit/integration/contract tests.
- [x] Написать notebook structure tests.
- [x] Реализовать `Typer + Rich` CLI/TUI.
- [x] Добавить тесты CLI пользовательских сценариев.
- [x] Реализовать ML label-provider interface.
- [x] Реализовать deterministic synthetic labels только для тестов/демо.
- [x] Реализовать source-agnostic preprocessing для pandas/polars.
- [x] Реализовать CatBoost train/infer/save/load.
- [x] Реализовать Optuna tuning.
- [x] Написать ML tests.
- [x] Обновить `README.md` с CLI и notebook examples.
- [x] Обновить `AGENTS.md` под новые правила проекта.
- [x] Прогнать `uv run ruff check .`.
- [x] Прогнать `uv run ty check .`.
- [x] Прогнать `uv run pytest`.
- [x] Проверить warehouse CLI на реальных данных.
- [x] Проверить ML CLI на реальных warehouse features.

## Optional
- [ ] Запустить `notebooks/spark_lab.ipynb` в Colab на полном датасете.
- [ ] Подключить реальный business label dataset вместо synthetic demo labels.
- [ ] Добавить Spark-capable CI runner, если появится Java/Spark runtime.
