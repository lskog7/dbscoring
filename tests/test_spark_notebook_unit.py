from __future__ import annotations

from pathlib import Path

import pytest

from tests.fixture_builders import SOURCE_REGISTRY
from tests.notebook_loader import SPARK_NOTEBOOK_PATH, load_namespace


@pytest.fixture(scope="module")
def spark_ns():
    return load_namespace(SPARK_NOTEBOOK_PATH)


def test_validate_run_mode_normalizes_value(spark_ns):
    assert spark_ns["validate_run_mode"]("DEBUG") == "debug"


def test_resolve_runtime_paths_prefers_explicit_paths(spark_ns, tmp_path):
    sources_root = tmp_path / "sources"
    for source_name, source_meta in SOURCE_REGISTRY.items():
        (sources_root / source_name / f"{source_meta['partition_key']}='2024-01-01'").mkdir(parents=True, exist_ok=True)

    resolved = spark_ns["resolve_runtime_paths"](
        sources_root=sources_root,
        warehouse_root=tmp_path / "warehouse_prod",
        run_mode="debug",
        debug_root=tmp_path / "warehouse_debug",
    )

    assert resolved["sources_root"] == str(sources_root.resolve())
    assert resolved["active_warehouse_root"] == str((tmp_path / "warehouse_debug").resolve())
    assert resolved["run_mode"] == "debug"


def test_parse_partition_directory_name_extracts_key_and_value(spark_ns):
    assert spark_ns["parse_partition_directory_name"]("report_dt='2024-03-31'") == ("report_dt", "2024-03-31")


def test_discover_source_partitions_returns_all_sources(spark_ns, tmp_path):
    sources_root = tmp_path / "sources"
    for source_name, source_meta in SOURCE_REGISTRY.items():
        (sources_root / source_name / f"{source_meta['partition_key']}='2024-01-01'").mkdir(parents=True, exist_ok=True)

    discovered = spark_ns["discover_source_partitions"](sources_root)

    assert len(discovered) == len(SOURCE_REGISTRY)
    assert {item["source_name"] for item in discovered} == set(SOURCE_REGISTRY)


def test_build_manifest_fingerprint_is_stable_for_same_files(spark_ns, tmp_path):
    partition_dir = tmp_path / "report_dt='2024-03-31'"
    partition_dir.mkdir(parents=True, exist_ok=True)
    (partition_dir / "part-00000.parquet").write_bytes(b"abc")
    (partition_dir / "part-00001.parquet").write_bytes(b"xyz")

    first = spark_ns["build_manifest_fingerprint"](partition_dir)
    second = spark_ns["build_manifest_fingerprint"](partition_dir)

    assert first == second


def test_infer_attribute_data_type_handles_expected_suffixes(spark_ns):
    assert spark_ns["infer_attribute_data_type"]("client_income_amt") == "DECIMAL"
    assert spark_ns["infer_attribute_data_type"]("row_actual_from") == "DATE"
    assert spark_ns["infer_attribute_data_type"]("card_active_1m_nflag") == "INT"


def test_determine_partition_action_covers_new_skip_fail(spark_ns):
    assert spark_ns["determine_partition_action"](None, "abc") == "new"
    assert spark_ns["determine_partition_action"]({"manifest_fingerprint": "abc"}, "abc") == "skip"
    assert spark_ns["determine_partition_action"]({"manifest_fingerprint": "abc"}, "xyz") == "fail"


def test_build_stage_sql_contains_expected_projection_for_monthly_and_daily(spark_ns):
    monthly_sql = spark_ns["build_stage_sql"]("raw_credit", "credit_cards_info", "2024-03-31")
    daily_sql = spark_ns["build_stage_sql"]("raw_daily", "client_cards_daily", "2024-03-31")

    assert "LATERAL VIEW STACK" in monthly_sql
    assert "AS report_dt" in monthly_sql
    assert "row_actual_from" in daily_sql
    assert "AS row_actual_to" in daily_sql


def test_clear_debug_artifacts_removes_only_approved_debug_root(spark_ns, tmp_path):
    debug_root = tmp_path / "warehouse_debug"
    debug_root.mkdir(parents=True, exist_ok=True)
    (debug_root / "payload.txt").write_text("payload", encoding="utf-8")

    removed_path = spark_ns["clear_debug_artifacts"](debug_root, debug_root)

    assert removed_path == str(debug_root.resolve())
    assert not debug_root.exists()


def test_clear_debug_artifacts_rejects_non_debug_paths(spark_ns, tmp_path):
    non_debug_root = tmp_path / "warehouse_prod"
    non_debug_root.mkdir(parents=True, exist_ok=True)

    with pytest.raises(ValueError):
        spark_ns["clear_debug_artifacts"](non_debug_root, non_debug_root)
