"""Polars ETL implementation for the credit-scoring warehouse."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from dbscoring.contracts import (
    ATTRIBUTE_REGISTRY,
    LOAD_STATUSES,
    SOURCE_REGISTRY,
    LoadStatus,
    SourceContract,
    get_attribute_contracts,
    get_source_contract,
    get_table_contract,
)
from dbscoring.paths import ProjectPaths

TARGET_SCHEMAS = {
    "dim_sources": {
        "source_id": pl.Int32,
        "source_name": pl.Utf8,
        "source_description": pl.Utf8,
        "update_frequency": pl.Utf8,
        "row_create_dtime": pl.Datetime,
        "row_update_dtime": pl.Datetime,
        "valid_from": pl.Datetime,
        "valid_to": pl.Datetime,
        "is_current": pl.Boolean,
    },
    "dim_attributes": {
        "attribute_id": pl.Int32,
        "attribute_name": pl.Utf8,
        "attribute_description": pl.Utf8,
        "data_type": pl.Utf8,
        "source_id": pl.Int32,
        "update_frequency": pl.Utf8,
        "row_create_dtime": pl.Datetime,
        "row_update_dtime": pl.Datetime,
    },
    "client_monthly_attrs_scd1": {
        "client_id": pl.Utf8,
        "attribute_id": pl.Int32,
        "report_dt": pl.Utf8,
        "attribute_value": pl.Utf8,
        "source_id": pl.Int32,
        "row_update_dtime": pl.Datetime,
        "row_loading_id": pl.Int64,
        "row_hash_val": pl.Utf8,
    },
    "client_daily_attrs_scd2": {
        "client_id": pl.Utf8,
        "attribute_id": pl.Int32,
        "attribute_value": pl.Utf8,
        "row_actual_from": pl.Utf8,
        "row_actual_to": pl.Utf8,
        "source_id": pl.Int32,
        "row_update_dtime": pl.Datetime,
        "row_loading_id": pl.Int64,
        "row_hash_val": pl.Utf8,
    },
    "load_log": {
        "load_id": pl.Int64,
        "source_id": pl.Int32,
        "source_report_dt": pl.Utf8,
        "load_start_dtime": pl.Datetime,
        "load_end_dtime": pl.Datetime,
        "target_table": pl.Utf8,
        "load_status": pl.Utf8,
        "rows_loaded": pl.Int64,
        "error_message": pl.Utf8,
    },
}


@dataclass(frozen=True, slots=True)
class LoadLogEntry:
    """One auditable load-log row."""

    load_id: int
    source_id: int
    source_report_dt: str
    load_start_dtime: datetime
    load_end_dtime: datetime
    target_table: str
    load_status: LoadStatus
    rows_loaded: int
    error_message: str | None


@dataclass(frozen=True, slots=True)
class WarehouseSummary:
    """Compact row-count summary for CLI, tests and notebooks."""

    dim_sources: int
    dim_attributes: int
    client_monthly_attrs_scd1: int
    client_daily_attrs_scd2: int
    load_log: int

    def as_dict(self) -> dict[str, int]:
        """Return summary as a plain dictionary."""

        return {
            "dim_sources": self.dim_sources,
            "dim_attributes": self.dim_attributes,
            "client_monthly_attrs_scd1": self.client_monthly_attrs_scd1,
            "client_daily_attrs_scd2": self.client_daily_attrs_scd2,
            "load_log": self.load_log,
        }


def utc_now() -> datetime:
    """Return a stable UTC timestamp without microseconds."""

    return datetime.now(UTC).replace(microsecond=0, tzinfo=None)


def empty_table(table_name: str) -> pl.DataFrame:
    """Create an empty target table with the canonical schema."""

    return pl.DataFrame(schema=TARGET_SCHEMAS[table_name])


def reset_warehouse(paths: ProjectPaths) -> None:
    """Remove and recreate the warehouse directory."""

    if paths.warehouse_root.exists():
        shutil.rmtree(paths.warehouse_root)
    paths.warehouse_root.mkdir(parents=True, exist_ok=True)


def read_table(paths: ProjectPaths, table_name: str) -> pl.DataFrame:
    """Read a warehouse table or return an empty canonical table."""

    table_path = paths.table_path(table_name)
    if table_path.exists():
        return pl.read_parquet(table_path)
    return empty_table(table_name)


def write_table(paths: ProjectPaths, table_name: str, frame: pl.DataFrame) -> None:
    """Write one warehouse table as a single parquet file."""

    paths.warehouse_root.mkdir(parents=True, exist_ok=True)
    table_path = paths.table_path(table_name)
    if table_path.exists():
        table_path.unlink()
    frame.select(list(TARGET_SCHEMAS[table_name])).write_parquet(table_path)


def list_source_partitions(paths: ProjectPaths, source_name: str) -> list[Path]:
    """List partition folders for a source in deterministic order."""

    source_dir = paths.data_root / source_name
    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory does not exist: {source_dir}")
    return sorted(
        [
            path
            for path in source_dir.iterdir()
            if path.is_dir() and not path.name.startswith(".")
        ],
        key=lambda path: path.name,
    )


def parse_partition_value(partition_path: Path) -> str:
    """Extract the Hive-style partition value from a folder name."""

    if "=" not in partition_path.name:
        raise ValueError(f"Invalid partition folder: {partition_path.name}")
    return partition_path.name.split("=", 1)[1].strip("'")


def read_source_partition(partition_path: Path) -> pl.DataFrame:
    """Read parquet files from a single source partition."""

    files = sorted(partition_path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {partition_path}")
    return pl.read_parquet(files)


def build_dim_sources() -> pl.DataFrame:
    """Build `dim_sources` from the frozen source registry."""

    timestamp = utc_now()
    return pl.DataFrame(
        [
            {
                "source_id": source.source_id,
                "source_name": source.source_name,
                "source_description": source.source_description,
                "update_frequency": source.update_frequency,
                "row_create_dtime": timestamp,
                "row_update_dtime": timestamp,
                "valid_from": timestamp,
                "valid_to": datetime(9999, 12, 31),
                "is_current": True,
            }
            for source in SOURCE_REGISTRY
        ],
        schema=TARGET_SCHEMAS["dim_sources"],
    )


def build_dim_attributes() -> pl.DataFrame:
    """Build `dim_attributes` from the frozen attribute registry."""

    timestamp = utc_now()
    return pl.DataFrame(
        [
            {
                "attribute_id": attribute.attribute_id,
                "attribute_name": attribute.attribute_name,
                "attribute_description": (
                    f"Business attribute from "
                    f"{attribute.source_name}.{attribute.source_column}"
                ),
                "data_type": attribute.data_type,
                "source_id": attribute.source_id,
                "update_frequency": attribute.update_frequency,
                "row_create_dtime": timestamp,
                "row_update_dtime": timestamp,
            }
            for attribute in ATTRIBUTE_REGISTRY
        ],
        schema=TARGET_SCHEMAS["dim_attributes"],
    )


def validate_source_schema(source: SourceContract, frame: pl.DataFrame) -> None:
    """Ensure a raw source frame contains all required fields."""

    required = (
        ("id",)
        + source.business_columns
        + source.technical_columns
        + (
            (source.partition_column,)
            if source.partition_column not in source.scd_columns
            else ()
        )
        + source.scd_columns
    )
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"{source.source_name} is missing columns: {missing}")


def should_update(
    *,
    source: SourceContract,
    partition_value: str,
    source_max_row_update_dtime: datetime,
    load_log: pl.DataFrame,
) -> bool:
    """Return whether a partition needs to be processed."""

    if load_log.is_empty():
        return True
    successful = load_log.filter(
        (pl.col("source_id") == source.source_id)
        & (pl.col("target_table") == source.target_table)
        & (pl.col("load_status") == "success")
    )
    if successful.is_empty():
        return True
    same_partition = successful.filter(pl.col("source_report_dt") == partition_value)
    if same_partition.is_empty():
        return True
    latest_load_time = same_partition.select(pl.col("load_end_dtime").max()).item()
    return bool(source_max_row_update_dtime > latest_load_time)


def next_load_id(load_log: pl.DataFrame) -> int:
    """Return the next monotonically increasing load identifier."""

    if load_log.is_empty():
        return 1
    return int(load_log.select(pl.col("load_id").max()).item()) + 1


def append_load_log(load_log: pl.DataFrame, entry: LoadLogEntry) -> pl.DataFrame:
    """Append one row to the canonical load-log table."""

    if entry.load_status not in LOAD_STATUSES:
        raise ValueError(f"Invalid load status: {entry.load_status}")
    row = pl.DataFrame(
        [
            {
                "load_id": entry.load_id,
                "source_id": entry.source_id,
                "source_report_dt": entry.source_report_dt,
                "load_start_dtime": entry.load_start_dtime,
                "load_end_dtime": entry.load_end_dtime,
                "target_table": entry.target_table,
                "load_status": entry.load_status,
                "rows_loaded": entry.rows_loaded,
                "error_message": entry.error_message,
            }
        ],
        schema=TARGET_SCHEMAS["load_log"],
    )
    if load_log.is_empty():
        return row
    return pl.concat([load_log, row], how="vertical_relaxed")


def upsert_by_keys(
    existing: pl.DataFrame, incoming: pl.DataFrame, keys: tuple[str, ...]
) -> pl.DataFrame:
    """Replace existing rows by key with incoming rows."""

    if existing.is_empty():
        return incoming
    preserved = existing.join(incoming.select(keys).unique(), on=list(keys), how="anti")
    return pl.concat([preserved, incoming], how="vertical_relaxed")


def verticalize_monthly(source: SourceContract, frame: pl.DataFrame) -> pl.DataFrame:
    """Normalize monthly source attributes into SCD1 vertical format."""

    parts = [
        frame.select(
            pl.col("id").cast(pl.Utf8).alias("client_id"),
            pl.lit(attribute.attribute_id).cast(pl.Int32).alias("attribute_id"),
            pl.col("report_dt").cast(pl.Utf8).alias("report_dt"),
            pl.col(attribute.source_column).cast(pl.Utf8).alias("attribute_value"),
            pl.lit(source.source_id).cast(pl.Int32).alias("source_id"),
            pl.col("row_update_dtime"),
            pl.col("loading_id").cast(pl.Int64).alias("row_loading_id"),
            pl.col("row_hash_val").cast(pl.Utf8),
        )
        for attribute in get_attribute_contracts(source.source_name)
    ]
    return pl.concat(parts, how="vertical_relaxed").select(
        list(TARGET_SCHEMAS["client_monthly_attrs_scd1"])
    )


def verticalize_daily(source: SourceContract, frame: pl.DataFrame) -> pl.DataFrame:
    """Normalize daily source attributes into SCD2 vertical format."""

    parts = [
        frame.select(
            pl.col("id").cast(pl.Utf8).alias("client_id"),
            pl.lit(attribute.attribute_id).cast(pl.Int32).alias("attribute_id"),
            pl.col(attribute.source_column).cast(pl.Utf8).alias("attribute_value"),
            pl.col("row_actual_from").cast(pl.Utf8),
            pl.col("row_actual_to").cast(pl.Utf8),
            pl.lit(source.source_id).cast(pl.Int32).alias("source_id"),
            pl.col("row_update_dtime"),
            pl.col("loading_id").cast(pl.Int64).alias("row_loading_id"),
            pl.col("row_hash_val").cast(pl.Utf8),
        )
        for attribute in get_attribute_contracts(source.source_name)
    ]
    return pl.concat(parts, how="vertical_relaxed").select(
        list(TARGET_SCHEMAS["client_daily_attrs_scd2"])
    )


def process_source(paths: ProjectPaths, source_name: str) -> pl.DataFrame:
    """Process all available partitions for one configured source."""

    source = get_source_contract(source_name)
    target = read_table(paths, source.target_table)
    load_log = read_table(paths, "load_log")
    keys = (
        ("client_id", "attribute_id", "row_actual_from")
        if source.update_frequency == "daily"
        else ("client_id", "attribute_id", "report_dt")
    )

    for partition_path in list_source_partitions(paths, source_name):
        partition_value = parse_partition_value(partition_path)
        start_time = utc_now()
        try:
            source_frame = read_source_partition(partition_path)
            validate_source_schema(source, source_frame)
            source_max_update = source_frame.select(
                pl.col("row_update_dtime").max()
            ).item()
            if not should_update(
                source=source,
                partition_value=partition_value,
                source_max_row_update_dtime=source_max_update,
                load_log=load_log,
            ):
                load_log = append_load_log(
                    load_log,
                    LoadLogEntry(
                        next_load_id(load_log),
                        source.source_id,
                        partition_value,
                        start_time,
                        utc_now(),
                        source.target_table,
                        "skipped",
                        0,
                        None,
                    ),
                )
                write_table(paths, "load_log", load_log)
                continue
            incoming = (
                verticalize_daily(source, source_frame)
                if source.update_frequency == "daily"
                else verticalize_monthly(source, source_frame)
            )
            target = upsert_by_keys(target, incoming, keys).sort(list(keys))
            write_table(paths, source.target_table, target)
            load_log = append_load_log(
                load_log,
                LoadLogEntry(
                    next_load_id(load_log),
                    source.source_id,
                    partition_value,
                    start_time,
                    utc_now(),
                    source.target_table,
                    "success",
                    incoming.height,
                    None,
                ),
            )
            write_table(paths, "load_log", load_log)
        except Exception as exc:
            load_log = append_load_log(
                load_log,
                LoadLogEntry(
                    next_load_id(load_log),
                    source.source_id,
                    partition_value,
                    start_time,
                    utc_now(),
                    source.target_table,
                    "failed",
                    0,
                    str(exc),
                ),
            )
            write_table(paths, "load_log", load_log)
            raise
    return read_table(paths, source.target_table)


def build_warehouse(paths: ProjectPaths, *, reset: bool = True) -> WarehouseSummary:
    """Build or incrementally refresh the complete Polars warehouse."""

    if reset:
        reset_warehouse(paths)
    write_table(paths, "dim_sources", build_dim_sources())
    write_table(paths, "dim_attributes", build_dim_attributes())
    if reset or not paths.table_path("load_log").exists():
        write_table(paths, "load_log", empty_table("load_log"))
    process_source(paths, "credit_cards_info")
    process_source(paths, "deb_cards_info")
    process_source(paths, "client_cards_daily")
    return summarize_warehouse(paths)


def summarize_warehouse(paths: ProjectPaths) -> WarehouseSummary:
    """Return row counts for all warehouse tables."""

    return WarehouseSummary(
        dim_sources=read_table(paths, "dim_sources").height,
        dim_attributes=read_table(paths, "dim_attributes").height,
        client_monthly_attrs_scd1=read_table(paths, "client_monthly_attrs_scd1").height,
        client_daily_attrs_scd2=read_table(paths, "client_daily_attrs_scd2").height,
        load_log=read_table(paths, "load_log").height,
    )


def validate_unique_key(frame: pl.DataFrame, table_name: str) -> None:
    """Validate that a table has no duplicated primary keys."""

    keys = get_table_contract(table_name).primary_key
    duplicated = frame.group_by(list(keys)).len().filter(pl.col("len") > 1)
    if not duplicated.is_empty():
        raise ValueError(f"{table_name} contains duplicated keys: {duplicated.head()}")


def validate_warehouse(paths: ProjectPaths) -> WarehouseSummary:
    """Validate schemas, primary keys and core row-count invariants."""

    for table_name, schema in TARGET_SCHEMAS.items():
        frame = read_table(paths, table_name)
        missing = sorted(set(schema) - set(frame.columns))
        if missing:
            raise ValueError(f"{table_name} is missing columns: {missing}")
        validate_unique_key(frame, table_name)

    dim_sources = read_table(paths, "dim_sources")
    dim_attributes = read_table(paths, "dim_attributes")
    load_log = read_table(paths, "load_log")
    if dim_sources.height != len(SOURCE_REGISTRY):
        raise ValueError("dim_sources row count does not match source registry")
    if dim_attributes.height != len(ATTRIBUTE_REGISTRY):
        raise ValueError("dim_attributes row count does not match attribute registry")
    bad_statuses = set(load_log.get_column("load_status").unique()) - set(LOAD_STATUSES)
    if bad_statuses:
        raise ValueError(f"load_log contains invalid statuses: {sorted(bad_statuses)}")
    return summarize_warehouse(paths)


def build_feature_frame(paths: ProjectPaths) -> pl.DataFrame:
    """Build a wide feature frame from current warehouse attributes."""

    monthly = read_table(paths, "client_monthly_attrs_scd1")
    daily = read_table(paths, "client_daily_attrs_scd2")
    if monthly.is_empty() and daily.is_empty():
        raise ValueError("Warehouse contains no client attributes")
    latest_report_dt = monthly.select(pl.col("report_dt").max()).item()
    monthly_wide = (
        monthly.filter(pl.col("report_dt") == latest_report_dt)
        .with_columns(
            pl.format("attr_{}", pl.col("attribute_id")).alias("feature_name")
        )
        .pivot(
            index="client_id",
            on="feature_name",
            values="attribute_value",
            aggregate_function="first",
        )
    )
    daily_wide = (
        daily.filter(pl.col("row_actual_to") == "9999-12-31")
        .with_columns(
            pl.format("attr_{}", pl.col("attribute_id")).alias("feature_name")
        )
        .pivot(
            index="client_id",
            on="feature_name",
            values="attribute_value",
            aggregate_function="first",
        )
    )
    return monthly_wide.join(daily_wide, on="client_id", how="full", coalesce=True)
