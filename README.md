# dbscoring

Production-grade реализация лабораторной работы 3 по кредитному скорингу:
физическая модель данных, локальный warehouse на `polars`, зеркальный Spark notebook
для Colab, CLI/TUI, тесты и ML-пайплайн на CatBoost/Optuna.

## Что реализовано
- `polars` ETL на реальных parquet-данных из `data/sources`.
- Физическая модель: `dim_sources`, `dim_attributes`,
  `client_monthly_attrs_scd1`, `client_daily_attrs_scd2`, `load_log`.
- SCD1 для monthly источников и SCD2 для daily источника.
- Idempotent update process через `should_update` и строгий `load_log`.
- Spark notebook для Colab с зеркальной логикой и manifest validation.
- Python package `dbscoring` с чистой структурой, типами и тестами.
- CLI/TUI на `Typer + Rich`.
- ML-модуль: feature frame, label-provider contract, synthetic demo labels,
  CatBoost train/infer/save/load, Optuna tuning.

## Структура
```text
src/dbscoring/
  contracts.py        # source, attribute, target-table contracts
  etl.py              # Polars warehouse build/validate/report
  cli.py              # Typer + Rich CLI/TUI
  paths.py            # path configuration
  testing.py          # deterministic fixtures from real schemas
  ml/
    labels.py         # label provider validation + demo labels
    pipeline.py       # CatBoost/Optuna train, tune, predict
notebooks/
  polars_lab.ipynb    # local runnable Polars lab
  spark_lab.ipynb     # Colab-only Spark mirror
tests/
  contract/
  integration/
  unit/
```

## Setup
Python runtime pinned to `3.12`.

```bash
uv venv --python 3.12
uv sync
```

Run quality gates:

```bash
uv run ruff check .
uv run ty check .
uv run pytest
```

## CLI examples
Show source registry:

```bash
uv run dbscoring status
```

Build and validate the warehouse on real data:

```bash
uv run dbscoring warehouse build \
  --data-root data/sources \
  --warehouse-root data/warehouse

uv run dbscoring warehouse validate \
  --data-root data/sources \
  --warehouse-root data/warehouse

uv run dbscoring warehouse report \
  --warehouse-root data/warehouse
```

Expected full-data row counts after first successful build:

```text
dim_sources: 3
dim_attributes: 24
client_monthly_attrs_scd1: 11,441,743
client_daily_attrs_scd2: 761,764
load_log: 6
```

ML demo path on real generated warehouse features:

```bash
uv run dbscoring ml make-features \
  --warehouse-root data/warehouse \
  --output data/ml/features.parquet

uv run dbscoring ml make-labels \
  --warehouse-root data/warehouse \
  --output data/ml/labels.parquet

uv run dbscoring ml train \
  --warehouse-root data/warehouse \
  --labels data/ml/labels.parquet \
  --model-out models/catboost.cbm \
  --iterations 20

uv run dbscoring ml tune \
  --warehouse-root data/warehouse \
  --labels data/ml/labels.parquet \
  --trials 10

uv run dbscoring ml predict \
  --model models/catboost.cbm \
  --input-path data/ml/features.parquet \
  --output data/ml/predictions.parquet
```

Synthetic labels are deterministic and intended only for tests/demo. Production
training must provide a real label dataset with `client_id` and binary `target`.

## Notebook examples
- Open `notebooks/polars_lab.ipynb` for the full local lab walkthrough.
- Open `notebooks/spark_lab.ipynb` in Colab for Spark. Spark is intentionally
  not a local dependency because this workspace has no Java/Spark runtime.

The Polars notebook demonstrates:
- `build_warehouse(paths, reset=True)`
- `validate_warehouse(paths)`
- load-log reporting
- `build_feature_frame(paths)`
- `make_synthetic_labels(features)`
- `train_catboost(features, labels)`
- `predict(model, features)`

The Spark notebook includes:
- Colab setup cell
- `SparkSession` creation
- source contracts
- mirrored vertical normalization
- `build_warehouse_spark()`
- manifest comparison against Polars row counts

## Testing strategy
Tests use deterministic small parquet fixtures created from the real source
schemas. They cover:
- contracts and table keys;
- SCD1/SCD2 normalization;
- idempotent reruns and `skipped` load-log behavior;
- failure logging on bad source schemas;
- CLI user scenarios;
- notebook structure and Spark Colab validation cells;
- ML label validation, preprocessing, CatBoost, Optuna and model persistence.

Spark execution is validated in Colab. Local tests assert Spark notebook
structure and parity hooks without installing PySpark.
