from __future__ import annotations

import polars as pl
import pytest

from dbscoring.etl import (
    build_feature_frame,
    build_warehouse,
    read_source_partition,
    read_table,
    should_update,
    validate_warehouse,
)
from dbscoring.paths import ProjectPaths


def test_build_warehouse_on_generated_real_schema_fixtures(
    fixture_paths: ProjectPaths,
) -> None:
    summary = build_warehouse(fixture_paths, reset=True)

    assert summary.as_dict() == {
        "dim_sources": 3,
        "dim_attributes": 24,
        "client_monthly_attrs_scd1": 160,
        "client_daily_attrs_scd2": 32,
        "load_log": 6,
    }
    assert validate_warehouse(fixture_paths).as_dict() == summary.as_dict()


def test_repeated_run_is_idempotent_and_logged_as_skipped(
    fixture_paths: ProjectPaths,
) -> None:
    build_warehouse(fixture_paths, reset=True)
    before_monthly = read_table(fixture_paths, "client_monthly_attrs_scd1").height
    before_daily = read_table(fixture_paths, "client_daily_attrs_scd2").height

    summary = build_warehouse(fixture_paths, reset=False)
    load_log = read_table(fixture_paths, "load_log")

    assert summary.client_monthly_attrs_scd1 == before_monthly
    assert summary.client_daily_attrs_scd2 == before_daily
    assert summary.load_log == 12
    assert load_log.filter(pl.col("load_status") == "skipped").height == 6


def test_primary_keys_are_unique_after_full_load(fixture_paths: ProjectPaths) -> None:
    build_warehouse(fixture_paths, reset=True)

    monthly = read_table(fixture_paths, "client_monthly_attrs_scd1")
    daily = read_table(fixture_paths, "client_daily_attrs_scd2")

    assert (
        monthly.group_by(["client_id", "attribute_id", "report_dt"]).len().height
        == monthly.height
    )
    assert (
        daily.group_by(["client_id", "attribute_id", "row_actual_from"]).len().height
        == daily.height
    )


def test_feature_frame_uses_latest_monthly_and_current_daily(
    fixture_paths: ProjectPaths,
) -> None:
    build_warehouse(fixture_paths, reset=True)

    features = build_feature_frame(fixture_paths)

    assert features.height == 4
    assert "client_id" in features.columns
    assert "attr_1" in features.columns
    assert "attr_24" in features.columns


def test_missing_source_column_fails_and_writes_failed_load(
    fixture_paths: ProjectPaths,
) -> None:
    bad_partition = (
        fixture_paths.data_root / "credit_cards_info" / "report_dt='2023-03-31'"
    )
    frame = read_source_partition(bad_partition).drop("client_income_amt")
    frame.write_parquet(bad_partition / "part-00000.parquet")

    with pytest.raises(ValueError, match="missing columns"):
        build_warehouse(fixture_paths, reset=True)

    load_log = read_table(fixture_paths, "load_log")
    assert load_log.filter(pl.col("load_status") == "failed").height == 1


def test_should_update_rejects_already_loaded_partition(
    fixture_paths: ProjectPaths,
) -> None:
    build_warehouse(fixture_paths, reset=True)
    load_log = read_table(fixture_paths, "load_log")
    source = next(
        source
        for source in __import__("dbscoring.contracts").contracts.SOURCE_REGISTRY
        if source.source_name == "credit_cards_info"
    )

    assert not should_update(
        source=source,
        partition_value="2023-03-31",
        source_max_row_update_dtime=load_log.select(
            pl.col("load_end_dtime").max()
        ).item(),
        load_log=load_log,
    )
