from __future__ import annotations

import os

import polars as pl
import pytest

from tests.fixture_builders import build_sample_sources, read_partitioned_table
from tests.notebook_loader import POLARS_NOTEBOOK_PATH, SPARK_NOTEBOOK_PATH, load_namespace


def _canonical_records(
    frame: pl.DataFrame,
    sort_columns: list[str],
    selected_columns: list[str] | None = None,
) -> list[dict[str, str]]:
    if frame.is_empty():
        return []
    columns = selected_columns or frame.columns
    expressions = []
    for column in columns:
        dtype = frame.schema[column]
        base_type = dtype.base_type() if hasattr(dtype, "base_type") else dtype
        if base_type == pl.Datetime:
            expr = pl.col(column).dt.strftime("%Y-%m-%d %H:%M:%S")
        elif base_type == pl.Date:
            expr = pl.col(column).dt.strftime("%Y-%m-%d")
        else:
            expr = pl.col(column).cast(pl.String)
        expressions.append(
            expr.fill_null("<NULL>").str.replace(r"^(-?\d+)\.0+$", "${1}").alias(column)
        )
    normalized = frame.select(expressions).sort(sort_columns)
    return normalized.to_dicts()


@pytest.fixture(scope="module")
def spark_ns():
    return load_namespace(SPARK_NOTEBOOK_PATH)


@pytest.fixture(scope="module")
def polars_ns():
    return load_namespace(POLARS_NOTEBOOK_PATH)


@pytest.fixture()
def spark_session(spark_ns, tmp_path):
    original_cwd = os.getcwd()
    os.chdir(tmp_path)
    spark_ns["bootstrap_system_spark_python"]()
    spark = spark_ns["create_spark_session"](
        app_name="dbscoring-parity-tests",
        warehouse_dir=tmp_path / "spark_warehouse",
        shuffle_partitions=2,
    )
    try:
        yield spark
    finally:
        spark.stop()
        os.chdir(original_cwd)


@pytest.mark.spark
@pytest.mark.parity
def test_spark_and_polars_outputs_match_on_same_fixture(spark_ns, polars_ns, spark_session, tmp_path):
    sources_root = tmp_path / "sources"
    build_sample_sources(sources_root)

    spark_warehouse = tmp_path / "spark_warehouse_output"
    polars_warehouse = tmp_path / "polars_warehouse_output"

    spark_summary = spark_ns["run_warehouse_update"](
        spark_session,
        sources_root=sources_root,
        warehouse_root=spark_warehouse,
        run_mode="production",
        debug_root=tmp_path / "spark_debug_output",
    )
    polars_summary = polars_ns["run_warehouse_update"](
        sources_root=sources_root,
        warehouse_root=polars_warehouse,
        run_mode="production",
        debug_root=tmp_path / "polars_debug_output",
    )

    assert spark_summary["loaded_partitions"] == polars_summary["loaded_partitions"] == 3

    spark_dim_sources = pl.read_parquet(spark_warehouse / "dim_sources" / "part-*.parquet")
    polars_dim_sources = pl.read_parquet(polars_warehouse / "dim_sources" / "data.parquet")
    assert _canonical_records(
        spark_dim_sources,
        ["source_id"],
        ["source_id", "source_name", "source_description", "update_frequency"],
    ) == _canonical_records(
        polars_dim_sources,
        ["source_id"],
        ["source_id", "source_name", "source_description", "update_frequency"],
    )

    spark_dim_attributes = pl.read_parquet(spark_warehouse / "dim_attributes" / "part-*.parquet")
    polars_dim_attributes = pl.read_parquet(polars_warehouse / "dim_attributes" / "data.parquet")
    assert _canonical_records(
        spark_dim_attributes,
        ["attribute_id"],
        [
            "attribute_id",
            "attribute_name",
            "attribute_description",
            "data_type",
            "source_id",
            "update_frequency",
        ],
    ) == _canonical_records(
        polars_dim_attributes,
        ["attribute_id"],
        [
            "attribute_id",
            "attribute_name",
            "attribute_description",
            "data_type",
            "source_id",
            "update_frequency",
        ],
    )

    spark_load_log = pl.read_parquet(spark_warehouse / "load_log" / "part-*.parquet")
    polars_load_log = pl.read_parquet(polars_warehouse / "load_log" / "data.parquet")
    assert _canonical_records(
        spark_load_log,
        ["load_id"],
        [
            "load_id",
            "source_id",
            "source_name",
            "source_partition_key",
            "source_partition_value",
            "target_table",
            "load_status",
            "rows_loaded",
            "error_message",
        ],
    ) == _canonical_records(
        polars_load_log,
        ["load_id"],
        [
            "load_id",
            "source_id",
            "source_name",
            "source_partition_key",
            "source_partition_value",
            "target_table",
            "load_status",
            "rows_loaded",
            "error_message",
        ],
    )

    spark_partition_state = pl.read_parquet(spark_warehouse / "tech_source_partitions" / "part-*.parquet")
    polars_partition_state = pl.read_parquet(polars_warehouse / "tech_source_partitions" / "data.parquet")
    assert _canonical_records(
        spark_partition_state,
        ["source_name", "partition_key", "partition_value"],
        [
            "source_id",
            "source_name",
            "target_table",
            "partition_key",
            "partition_value",
            "partition_path",
            "manifest_fingerprint",
            "last_processed_status",
            "first_load_id",
            "last_load_id",
        ],
    ) == _canonical_records(
        polars_partition_state,
        ["source_name", "partition_key", "partition_value"],
        [
            "source_id",
            "source_name",
            "target_table",
            "partition_key",
            "partition_value",
            "partition_path",
            "manifest_fingerprint",
            "last_processed_status",
            "first_load_id",
            "last_load_id",
        ],
    )

    spark_monthly = read_partitioned_table(spark_warehouse / "client_monthly_attrs_scd1")
    polars_monthly = read_partitioned_table(polars_warehouse / "client_monthly_attrs_scd1")
    assert _canonical_records(spark_monthly, ["client_id", "attribute_id", "report_dt"]) == _canonical_records(
        polars_monthly, ["client_id", "attribute_id", "report_dt"]
    )

    spark_daily = read_partitioned_table(spark_warehouse / "client_daily_attrs_scd2")
    polars_daily = read_partitioned_table(polars_warehouse / "client_daily_attrs_scd2")
    assert _canonical_records(spark_daily, ["client_id", "attribute_id", "row_actual_to"]) == _canonical_records(
        polars_daily, ["client_id", "attribute_id", "row_actual_to"]
    )
