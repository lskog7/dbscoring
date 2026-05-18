"""Интеграционные сценарии для Polars notebook, повторяющие основной warehouse-контракт."""

from __future__ import annotations

import polars as pl
import pytest

from tests.fixture_builders import (
    SOURCE_REGISTRY,
    add_second_monthly_partition,
    build_sample_sources,
    mutate_existing_partition,
    read_partitioned_table,
)
from tests.notebook_loader import POLARS_NOTEBOOK_PATH, load_namespace


@pytest.fixture(scope="module")
def polars_ns():
    return load_namespace(POLARS_NOTEBOOK_PATH)


def test_polars_run_warehouse_update_loads_and_skips(polars_ns, tmp_path):
    sources_root = tmp_path / "sources"
    build_sample_sources(sources_root)

    first_summary = polars_ns["run_warehouse_update"](
        sources_root=sources_root,
        warehouse_root=tmp_path / "warehouse_prod",
        run_mode="production",
        debug_root=tmp_path / "warehouse_debug",
    )
    second_summary = polars_ns["run_warehouse_update"](
        sources_root=sources_root,
        warehouse_root=tmp_path / "warehouse_prod",
        run_mode="production",
        debug_root=tmp_path / "warehouse_debug",
    )

    assert first_summary["loaded_partitions"] == 3
    assert second_summary["skipped_partitions"] == 3

    monthly_frame = read_partitioned_table(tmp_path / "warehouse_prod" / "client_monthly_attrs_scd1")
    daily_frame = read_partitioned_table(tmp_path / "warehouse_prod" / "client_daily_attrs_scd2")
    assert monthly_frame.height == 20
    assert daily_frame.height == 4


def test_polars_incremental_monthly_partition(polars_ns, tmp_path):
    sources_root = tmp_path / "sources"
    build_sample_sources(sources_root)
    warehouse_root = tmp_path / "warehouse_prod"

    polars_ns["run_warehouse_update"](
        sources_root=sources_root,
        warehouse_root=warehouse_root,
        run_mode="production",
        debug_root=tmp_path / "warehouse_debug",
    )

    add_second_monthly_partition(sources_root)
    summary = polars_ns["run_warehouse_update"](
        sources_root=sources_root,
        warehouse_root=warehouse_root,
        run_mode="production",
        debug_root=tmp_path / "warehouse_debug",
    )

    monthly_frame = read_partitioned_table(warehouse_root / "client_monthly_attrs_scd1")
    assert summary["loaded_partitions"] == 1
    assert summary["skipped_partitions"] == 3
    assert monthly_frame.height == 31


def test_polars_changed_partition_fails(polars_ns, tmp_path):
    sources_root = tmp_path / "sources"
    build_sample_sources(sources_root)
    warehouse_root = tmp_path / "warehouse_prod"

    polars_ns["run_warehouse_update"](
        sources_root=sources_root,
        warehouse_root=warehouse_root,
        run_mode="production",
        debug_root=tmp_path / "warehouse_debug",
    )
    mutate_existing_partition(sources_root)

    with pytest.raises(RuntimeError):
        polars_ns["run_warehouse_update"](
            sources_root=sources_root,
            warehouse_root=warehouse_root,
            run_mode="production",
            debug_root=tmp_path / "warehouse_debug",
        )


def test_polars_debug_mode_rebuilds(polars_ns, tmp_path):
    sources_root = tmp_path / "sources"
    build_sample_sources(sources_root)

    summary = polars_ns["run_warehouse_update"](
        sources_root=sources_root,
        warehouse_root=tmp_path / "warehouse_prod",
        run_mode="debug",
        debug_root=tmp_path / "warehouse_debug",
        cleanup_existing_debug_root=True,
    )

    assert summary["run_mode"] == "debug"
    assert (tmp_path / "warehouse_debug").exists()
    assert read_partitioned_table(tmp_path / "warehouse_debug" / "client_daily_attrs_scd2").height == 4


def test_polars_small_tables_are_materialized(polars_ns, tmp_path):
    warehouse_root = tmp_path / "warehouse_debug"
    polars_ns["initialize_warehouse_layout"](warehouse_root)
    summary = polars_ns["upsert_reference_dimensions"](warehouse_root, polars_ns["dt"].datetime(2024, 1, 31, 12, 0, 0))

    dim_sources = pl.read_parquet(warehouse_root / "dim_sources" / "data.parquet")
    dim_attributes = pl.read_parquet(warehouse_root / "dim_attributes" / "data.parquet")

    assert summary == {"dim_sources_rows": 3, "dim_attributes_rows": 24}
    assert dim_sources.height == 3
    assert dim_attributes.height == 24
