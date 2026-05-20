"""Интеграционные сценарии автономного Spark notebook."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from tests.fixture_builders import (
    SOURCE_REGISTRY,
    TEST_SOURCE_PARTITION_ROW_LIMIT,
    add_second_monthly_partition,
    build_limited_real_sources,
    mutate_existing_partition,
)
from tests.notebook_loader import SPARK_NOTEBOOK_PATH, load_namespace


NOTEBOOK_NAMESPACE = load_namespace(SPARK_NOTEBOOK_PATH)
read_warehouse_table = NOTEBOOK_NAMESPACE["read_warehouse_table"]
run_warehouse_update = NOTEBOOK_NAMESPACE["run_warehouse_update"]


def count_warehouse_rows(spark_session, summary, table_name: str) -> int:
    return read_warehouse_table(
        spark_session,
        table_name,
        summary["table_locations"],
    ).count()


def expected_eav_rows(source_counts: dict[tuple[str, str], int], target_table: str) -> int:
    return sum(
        row_count * len(SOURCE_REGISTRY[source_name]["columns"])
        for (source_name, _partition_name), row_count in source_counts.items()
        if SOURCE_REGISTRY[source_name]["target_table"] == target_table
    )


@pytest.mark.spark
def test_spark_run_warehouse_update_loads_and_skips(spark_session, tmp_path):
    sources_root = tmp_path / "sources"
    source_counts = build_limited_real_sources(spark_session, sources_root)
    expected_partitions = len(source_counts)

    first_summary = run_warehouse_update(
        spark_session,
        sources_root=sources_root,
        warehouse_root=tmp_path / "warehouse",
    )
    second_summary = run_warehouse_update(
        spark_session,
        sources_root=sources_root,
        warehouse_root=tmp_path / "warehouse",
    )

    assert first_summary["loaded_partitions"] == expected_partitions
    assert second_summary["skipped_partitions"] == expected_partitions
    assert count_warehouse_rows(spark_session, second_summary, "client_monthly_attrs_scd1") == expected_eav_rows(
        source_counts, "client_monthly_attrs_scd1"
    )
    assert count_warehouse_rows(spark_session, second_summary, "client_daily_attrs_scd2") == expected_eav_rows(
        source_counts, "client_daily_attrs_scd2"
    )
    assert count_warehouse_rows(spark_session, second_summary, "load_log") == expected_partitions * 2

    load_log = read_warehouse_table(spark_session, "load_log", second_summary["table_locations"])
    assert load_log.filter(F.col("load_status") == "loaded").count() == expected_partitions
    assert load_log.filter(F.col("load_status") == "skipped").count() == expected_partitions

    partition_state = read_warehouse_table(spark_session, "tech_source_partitions", second_summary["table_locations"])
    assert partition_state.count() == expected_partitions
    assert partition_state.filter(F.col("last_processed_status") == "skipped").count() == expected_partitions


@pytest.mark.spark
def test_limited_real_sources_keep_each_partition_under_test_limit(spark_session, tmp_path):
    source_counts = build_limited_real_sources(spark_session, tmp_path / "sources")

    assert source_counts
    assert max(source_counts.values()) <= TEST_SOURCE_PARTITION_ROW_LIMIT
    assert any(row_count > 1 for row_count in source_counts.values())


@pytest.mark.spark
def test_spark_incremental_monthly_partition(spark_session, tmp_path):
    sources_root = tmp_path / "sources"
    source_counts = build_limited_real_sources(spark_session, sources_root)
    expected_partitions = len(source_counts)
    warehouse_root = tmp_path / "warehouse"

    run_warehouse_update(
        spark_session,
        sources_root=sources_root,
        warehouse_root=warehouse_root,
    )

    add_second_monthly_partition(spark_session, sources_root)
    summary = run_warehouse_update(
        spark_session,
        sources_root=sources_root,
        warehouse_root=warehouse_root,
    )

    assert summary["loaded_partitions"] == 1
    assert summary["skipped_partitions"] == expected_partitions
    assert count_warehouse_rows(spark_session, summary, "client_monthly_attrs_scd1") == (
        expected_eav_rows(source_counts, "client_monthly_attrs_scd1") + len(SOURCE_REGISTRY["credit_cards_info"]["columns"])
    )

    load_log = read_warehouse_table(spark_session, "load_log", summary["table_locations"])
    april_credit_load = load_log.filter(
        (F.col("source_id") == 2)
        & (F.col("source_report_dt") == "2024-04-30")
        & (F.col("load_status") == "loaded")
    )
    assert april_credit_load.count() == 1


@pytest.mark.spark
def test_spark_mutated_loaded_partition_is_failed(spark_session, tmp_path):
    sources_root = tmp_path / "sources"
    source_counts = build_limited_real_sources(spark_session, sources_root)
    expected_partitions = len(source_counts)
    warehouse_root = tmp_path / "warehouse"

    run_warehouse_update(
        spark_session,
        sources_root=sources_root,
        warehouse_root=warehouse_root,
    )
    mutated_partition_value = mutate_existing_partition(spark_session, sources_root)

    summary = run_warehouse_update(
        spark_session,
        sources_root=sources_root,
        warehouse_root=warehouse_root,
    )

    assert summary["loaded_partitions"] == 0
    assert summary["skipped_partitions"] == expected_partitions - 1
    assert summary["failed_partitions"] == 1

    load_log = read_warehouse_table(spark_session, "load_log", summary["table_locations"])
    failed_rows = load_log.filter(F.col("load_status") == "failed").collect()
    assert len(failed_rows) == 1
    assert failed_rows[0]["error_message"] == "Source partition manifest changed after successful load."

    partition_state = read_warehouse_table(spark_session, "tech_source_partitions", summary["table_locations"])
    failed_state = partition_state.filter(
        (F.col("source_id") == 2) & (F.col("partition_value") == mutated_partition_value)
    ).collect()[0]
    assert failed_state["last_processed_status"] == "failed"


@pytest.mark.spark
def test_spark_warehouse_update_loads_to_explicit_path(spark_session, tmp_path):
    sources_root = tmp_path / "sources"
    source_counts = build_limited_real_sources(spark_session, sources_root)
    warehouse_root = tmp_path / "warehouse"

    summary = run_warehouse_update(
        spark_session,
        sources_root=sources_root,
        warehouse_root=warehouse_root,
    )

    assert summary["warehouse_root"] == str(warehouse_root.resolve())
    assert count_warehouse_rows(spark_session, summary, "client_daily_attrs_scd2") == expected_eav_rows(
        source_counts, "client_daily_attrs_scd2"
    )

    dim_sources = read_warehouse_table(spark_session, "dim_sources", summary["table_locations"])
    dim_attributes = read_warehouse_table(spark_session, "dim_attributes", summary["table_locations"])
    assert dim_sources.count() == 3
    assert dim_attributes.count() == 24
