"""Прямые тесты warehouse-хелперов из автономного notebook."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from tests.notebook_loader import SPARK_NOTEBOOK_PATH, load_namespace


NOTEBOOK_NAMESPACE = load_namespace(SPARK_NOTEBOOK_PATH)
SOURCES = NOTEBOOK_NAMESPACE["SOURCES"]
build_spark_schema = NOTEBOOK_NAMESPACE["build_spark_schema"]
initialize_warehouse_tables = NOTEBOOK_NAMESPACE["initialize_warehouse_tables"]
read_warehouse_table = NOTEBOOK_NAMESPACE["read_warehouse_table"]
table_has_parquet_files = NOTEBOOK_NAMESPACE["table_has_parquet_files"]
upsert_reference_dimensions = NOTEBOOK_NAMESPACE["upsert_reference_dimensions"]


def test_build_spark_schema_matches_table_contract():
    schema = build_spark_schema("load_log")

    assert schema.fieldNames() == [
        "load_id",
        "source_id",
        "source_report_dt",
        "load_start_dtime",
        "load_end_dtime",
        "target_table",
        "load_status",
        "row_loading_id",
        "error_message",
    ]


def test_initialize_warehouse_tables_creates_all_datasets(spark_session, tmp_path):
    warehouse_root = tmp_path / "warehouse"

    locations = initialize_warehouse_tables(spark_session, warehouse_root)

    assert set(locations) >= {
        "dim_sources",
        "dim_attributes",
        "load_log",
        "tech_source_partitions",
        "client_monthly_attrs_scd1",
        "client_daily_attrs_scd2",
    }
    for location in locations.values():
        assert Path(location).exists()

    assert table_has_parquet_files(locations["dim_sources"]) is True
    assert table_has_parquet_files(locations["load_log"]) is True
    assert read_warehouse_table(spark_session, "client_monthly_attrs_scd1", locations).count() == 0
    assert read_warehouse_table(spark_session, "client_daily_attrs_scd2", locations).count() == 0


def test_read_warehouse_table_returns_empty_dataframe_with_schema(spark_session, tmp_path):
    locations = initialize_warehouse_tables(spark_session, tmp_path / "warehouse")

    frame = read_warehouse_table(spark_session, "load_log", locations)

    assert frame.count() == 0
    assert frame.schema == build_spark_schema("load_log")


def test_upsert_reference_dimensions_rebuilds_small_tables(spark_session, tmp_path):
    locations = initialize_warehouse_tables(spark_session, tmp_path / "warehouse")
    load_timestamp = dt.datetime(2024, 5, 1, 12, 0, 0)

    summary = upsert_reference_dimensions(spark_session, load_timestamp, locations)

    assert summary == {"dim_sources_rows": 3, "dim_attributes_rows": 24}

    dim_sources = read_warehouse_table(spark_session, "dim_sources", locations)
    dim_attributes = read_warehouse_table(spark_session, "dim_attributes", locations)
    assert dim_sources.count() == len(SOURCES)
    assert dim_attributes.count() == sum(len(source_meta["columns"]) for source_meta in SOURCES.values())
