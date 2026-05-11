# dbscoring agent instructions

## Project purpose
- Production-grade implementation of a university data engineering lab for credit scoring.
- The project builds a physical warehouse model from source parquet datasets and provides local Polars ETL, Colab Spark parity, CLI/TUI, tests and a CatBoost ML scoring module.

## Runtime and tooling
- Python version is pinned to `3.12`.
- Use `uv` for all dependency and command execution.
- Required quality gates:
  - `uv run ruff check .`
  - `uv run ty check .`
  - `uv run pytest`
- Do not disable, delete or bypass tests.
- Do not add local Spark/PySpark dependencies unless the project explicitly changes direction. Spark is Colab-only because this workspace has no Java/Spark runtime.

## Main commands
- `uv run dbscoring status`
- `uv run dbscoring warehouse build --data-root data/sources --warehouse-root data/warehouse`
- `uv run dbscoring warehouse validate --data-root data/sources --warehouse-root data/warehouse`
- `uv run dbscoring warehouse report --warehouse-root data/warehouse`
- `uv run dbscoring ml make-features --warehouse-root data/warehouse --output data/ml/features.parquet`
- `uv run dbscoring ml make-labels --warehouse-root data/warehouse --output data/ml/labels.parquet`
- `uv run dbscoring ml train --warehouse-root data/warehouse --labels data/ml/labels.parquet --model-out models/catboost.cbm`
- `uv run dbscoring ml predict --model models/catboost.cbm --input-path data/ml/features.parquet --output data/ml/predictions.parquet`

## Data contracts
- Raw sources live under `data/sources`.
- Monthly SCD1 sources:
  - `credit_cards_info`
  - `deb_cards_info`
- Daily SCD2 source:
  - `client_cards_daily`
- Canonical `client_id` type is `STRING`.
- There are 24 business attributes:
  - 4 daily attributes from `client_cards_daily`
  - 11 monthly attributes from `credit_cards_info`
  - 9 monthly attributes from `deb_cards_info`

## Code structure
- `src/dbscoring/contracts.py` stores source, attribute and table contracts.
- `src/dbscoring/etl.py` stores the local Polars warehouse implementation.
- `src/dbscoring/ml/` stores label validation, preprocessing, CatBoost, Optuna and inference.
- `src/dbscoring/cli.py` stores the Typer + Rich CLI/TUI.
- `notebooks/polars_lab.ipynb` is the local documented lab notebook.
- `notebooks/spark_lab.ipynb` is the Colab-only Spark mirror notebook.

## Testing rules
- Tests must use deterministic fixtures derived from real source schemas.
- Keep tests strict: assert row counts, keys, schemas, statuses and failure behavior.
- ML tests must cover pandas and polars inputs, synthetic label reproducibility, model save/load and inference.
- Spark local execution is not expected. Local tests must check Spark notebook structure and Colab validation hooks.

## Artifact rules
- Do not commit generated large artifacts from:
  - `data/warehouse/`
  - `data/ml/`
  - `models/`
  - `reports/`
- Keep notebooks and package code synchronized. Business logic should live in the package; notebooks should demonstrate and document it.
