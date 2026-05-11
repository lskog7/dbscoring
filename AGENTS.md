# dbscoring

## Project overview
- Repository for a university data engineering lab ("Лабораторная работа 3") on credit scoring with Apache Spark and distributed data processing.
- Main goal: build a physical data model and update processes for client scoring attributes using three source datasets and SCD strategies.

## Task source
- Primary assignment: `docs/lab_3_task_1.pdf`
- Required outcome: physical schema for sources, client attributes, and load tracking for monthly and daily updates.

## Data sources
- Raw source datasets are in `data/sources`.
- `data/sources/deb_cards_info` and `data/sources/credit_cards_info`
  - Partition: `report_dt` (string)
  - Observed load partitions: `2023-02-28`, `2023-03-31`
  - SCD type from task: `SCD1` (monthly refresh)
- `data/sources/client_cards_daily`
  - Partition: `row_actual_to` (string)
  - Observed partitions: `2023-04-03` (closed), `9999-12-31` (active/current)
  - SCD type from task: `SCD2` (daily refresh)

## Intended warehouse schema (`schemas/schema_v2.drawio`)
- `dim_sources`
  - Source metadata table (source ID, source name/description, update frequency, technical timestamps/validity dates, etc.).
- `dim_attributes`
  - Attribute metadata table linked to sources (attribute ID/name/description/data type, source ID, update frequency, validity and load timestamps).
- `client_monthly_attrs_scd1`
  - Normalized monthly client attributes with PK on `client_id` + `attribute_id` + `report_month`, plus source + load metadata and value.
- `client_daily_attrs_scd2`
  - Normalized daily client attributes with SCD2 columns: `valid_from`, `valid_to`, and `row_hash_val` for change tracking.
- `load_log`
  - Load audit table for ETL runs (`load_status`, row counts, timing, target table, partition key, etc.).
- FK links encoded in the diagram:
  - `dim_sources.source_id` -> source refs in attributes and both client fact tables.
  - `dim_attributes.attribute_id` -> attribute refs in both client fact tables.
  - `load_log.load_id` -> row loading references in both fact tables.

## Repository structure
- `schemas/` — data model and schema artifacts.
- `docs/` — lab statements and documentation.
- `data/` — input data partitions (typically ignored in git; check `.gitignore`).
- `src/` — project code for pipelines / scoring logic.

## Working conventions (initial)
- Keep data layout and processing behavior aligned with the lab statement (monthly SCD1 and daily SCD2).
- Track source, partition, and loading metadata during all pipeline updates.
- Treat `_SUCCESS` and partition folders under `data/sources` as artifact markers from Spark/Hive output.

## UV workflow rules
- Python runtime is pinned to 3.13 (`pyproject.toml` uses `requires-python = ">=3.13.9,<3.14"`).
- Environment and tooling rules:
  - Always use `uv` for Python dependency and command execution where possible.
  - Install dependencies with `uv add` (including `--dev` for tooling packages).
  - Runtime env created with `uv venv --python 3.13` and dependencies installed with `uv add`.
  - Prefer the following command pattern:
    - `uv run ruff check .` for lint checks.
    - `uv run ty check .` for static type checks.
    - `uv run pytest` for tests.
  - Use scripts in `pyproject.toml`:
    - `uv run lint`
    - `uv run typecheck`
    - `uv run test`

## Installation
- Preferred install path: `uv` (all operations below)
  - Install uv:
    - `curl -LsSf https://astral.sh/uv/install.sh | sh`
    - fallback: `python3 -m pip install uv`
    - verify with `uv --version`
  - Bootstrap project:
    - `uv venv --python 3.13`
    - `uv sync`
  - Recommended workflow after clone:
    - `uv run lint`
    - `uv run typecheck`
    - `uv run test`
- Fallback `pip` path (if uv is unavailable):
  - `python3.13 -m venv .venv`
  - `source .venv/bin/activate`
  - `pip install numpy pandas polars pytest ruff ty`
  - optional package install: `pip install -e .`
  - run: `pytest`, `ruff check .`, `ty check .`

## Testing setup
- `tests/` folder is created for pytest tests and should be used for all project test modules.
