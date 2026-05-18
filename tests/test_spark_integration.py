"""Полные интеграционные сценарии для Spark notebook и инкрементального warehouse-контура."""

from __future__ import annotations

import os

import pytest

from tests.fixture_builders import add_second_monthly_partition, build_sample_sources, mutate_existing_partition
from tests.notebook_loader import SPARK_NOTEBOOK_PATH, load_namespace


@pytest.fixture(scope="module")
def spark_ns():
    return load_namespace(SPARK_NOTEBOOK_PATH)


@pytest.fixture()
def spark_session(spark_ns, tmp_path):
    original_cwd = os.getcwd()
    os.chdir(tmp_path)
    spark_ns["bootstrap_system_spark_python"]()
    spark = spark_ns["create_spark_session"](
        app_name="dbscoring-integration-tests",
        warehouse_dir=tmp_path / "spark_warehouse",
        shuffle_partitions=2,
    )
    try:
        yield spark
    finally:
        spark.stop()
        os.chdir(original_cwd)


@pytest.mark.spark
def test_spark_run_warehouse_update_loads_and_skips(spark_ns, spark_session, tmp_path):
    sources_root = tmp_path / "sources"
    build_sample_sources(sources_root)

    first_summary = spark_ns["run_warehouse_update"](
        spark_session,
        sources_root=sources_root,
        warehouse_root=tmp_path / "warehouse_prod",
        run_mode="production",
        debug_root=tmp_path / "warehouse_debug",
    )
    second_summary = spark_ns["run_warehouse_update"](
        spark_session,
        sources_root=sources_root,
        warehouse_root=tmp_path / "warehouse_prod",
        run_mode="production",
        debug_root=tmp_path / "warehouse_debug",
    )

    assert first_summary["loaded_partitions"] == 3
    assert second_summary["skipped_partitions"] == 3
    assert spark_session.sql("SELECT COUNT(*) AS cnt FROM client_monthly_attrs_scd1").collect()[0]["cnt"] == 20
    assert spark_session.sql("SELECT COUNT(*) AS cnt FROM client_daily_attrs_scd2").collect()[0]["cnt"] == 4
    assert spark_session.sql("SELECT COUNT(*) AS cnt FROM load_log").collect()[0]["cnt"] == 6


@pytest.mark.spark
def test_spark_incremental_monthly_partition(spark_ns, spark_session, tmp_path):
    sources_root = tmp_path / "sources"
    build_sample_sources(sources_root)
    warehouse_root = tmp_path / "warehouse_prod"

    spark_ns["run_warehouse_update"](
        spark_session,
        sources_root=sources_root,
        warehouse_root=warehouse_root,
        run_mode="production",
        debug_root=tmp_path / "warehouse_debug",
    )

    add_second_monthly_partition(sources_root)
    summary = spark_ns["run_warehouse_update"](
        spark_session,
        sources_root=sources_root,
        warehouse_root=warehouse_root,
        run_mode="production",
        debug_root=tmp_path / "warehouse_debug",
    )

    assert summary["loaded_partitions"] == 1
    assert summary["skipped_partitions"] == 3
    assert spark_session.sql("SELECT COUNT(*) AS cnt FROM client_monthly_attrs_scd1").collect()[0]["cnt"] == 31


@pytest.mark.spark
def test_spark_changed_partition_fails(spark_ns, spark_session, tmp_path):
    sources_root = tmp_path / "sources"
    build_sample_sources(sources_root)
    warehouse_root = tmp_path / "warehouse_prod"

    spark_ns["run_warehouse_update"](
        spark_session,
        sources_root=sources_root,
        warehouse_root=warehouse_root,
        run_mode="production",
        debug_root=tmp_path / "warehouse_debug",
    )
    mutate_existing_partition(sources_root)

    with pytest.raises(RuntimeError):
        spark_ns["run_warehouse_update"](
            spark_session,
            sources_root=sources_root,
            warehouse_root=warehouse_root,
            run_mode="production",
            debug_root=tmp_path / "warehouse_debug",
        )


@pytest.mark.spark
def test_spark_debug_mode_rebuilds(spark_ns, spark_session, tmp_path):
    sources_root = tmp_path / "sources"
    build_sample_sources(sources_root)

    summary = spark_ns["run_warehouse_update"](
        spark_session,
        sources_root=sources_root,
        warehouse_root=tmp_path / "warehouse_prod",
        run_mode="debug",
        debug_root=tmp_path / "warehouse_debug",
        cleanup_existing_debug_root=True,
    )

    assert summary["run_mode"] == "debug"
    assert spark_session.sql("SELECT COUNT(*) AS cnt FROM client_daily_attrs_scd2").collect()[0]["cnt"] == 4
