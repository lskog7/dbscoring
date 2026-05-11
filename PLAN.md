# dbscoring delivery plan

## Summary
- Python `3.12` + `uv` project is the canonical environment.
- Local runtime is `polars`; Spark is Colab-only.
- Business logic lives in `src/dbscoring`; notebooks document and demonstrate the same logic.
- ML uses a production label-provider contract. Synthetic labels are deterministic demo/test labels only.

## Completed Scope
- [x] Physical data model for all required warehouse tables.
- [x] Polars warehouse build on real `data/sources` parquet data.
- [x] Monthly SCD1 normalization for `credit_cards_info` and `deb_cards_info`.
- [x] Daily SCD2 normalization for `client_cards_daily`.
- [x] Strict `load_log`, `should_update`, idempotent rerun behavior.
- [x] Spark Colab notebook with mirrored contracts and manifest validation.
- [x] Python package with clean modules and CLI/TUI.
- [x] CatBoost/Optuna ML module with source-agnostic pandas/polars inputs.
- [x] Deterministic test fixtures derived from real source schemas.
- [x] Contract, integration, CLI, notebook and ML tests.
- [x] README and AGENTS updated for production workflow.

## Public Interfaces
- CLI:
  - `dbscoring status`
  - `dbscoring warehouse build`
  - `dbscoring warehouse validate`
  - `dbscoring warehouse report`
  - `dbscoring ml make-features`
  - `dbscoring ml make-labels`
  - `dbscoring ml train`
  - `dbscoring ml tune`
  - `dbscoring ml predict`
- Notebooks:
  - `notebooks/polars_lab.ipynb`
  - `notebooks/spark_lab.ipynb`
- Package:
  - `dbscoring.etl`
  - `dbscoring.contracts`
  - `dbscoring.ml`
  - `dbscoring.cli`

## Verified Results
- Full Polars warehouse build on real data:
  - `dim_sources`: `3`
  - `dim_attributes`: `24`
  - `client_monthly_attrs_scd1`: `11,441,743`
  - `client_daily_attrs_scd2`: `761,764`
  - `load_log`: `6`
- Real-data ML CLI smoke:
  - features: `306,911`
  - labels: `306,911`
  - predictions: `306,911`

## Quality Gates
- [x] `uv run ruff check .`
- [x] `uv run ty check .`
- [x] `uv run pytest`
- [x] Real-data CLI warehouse build.
- [x] Real-data CLI warehouse validation.
- [x] Real-data CLI ML feature, label, train and predict smoke run.

## Optional Future Work
- Run `notebooks/spark_lab.ipynb` in Colab on the full dataset and paste the Spark manifest into a report.
- Integrate a real external business default label dataset for production scoring.
- Add CI with a separate Spark-capable runner if Java/Spark becomes available.
