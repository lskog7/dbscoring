"""Unit-тесты автономных функций из Spark notebook."""

from __future__ import annotations

import pytest

from tests.fixture_builders import SOURCE_REGISTRY
from tests.notebook_loader import SPARK_NOTEBOOK_PATH, load_namespace


NOTEBOOK_NAMESPACE = load_namespace(SPARK_NOTEBOOK_PATH)
build_manifest_fingerprint = NOTEBOOK_NAMESPACE["build_manifest_fingerprint"]
determine_partition_action = NOTEBOOK_NAMESPACE["determine_partition_action"]
discover_source_partitions = NOTEBOOK_NAMESPACE["discover_source_partitions"]
parse_partition_directory_name = NOTEBOOK_NAMESPACE["parse_partition_directory_name"]


def test_parse_partition_directory_name_extracts_key_and_value():
    assert parse_partition_directory_name("report_dt='2024-03-31'") == ("report_dt", "2024-03-31")


def test_parse_partition_directory_name_rejects_bad_format():
    with pytest.raises(ValueError):
        parse_partition_directory_name("report_dt=2024-03-31")


def test_discover_source_partitions_returns_all_sources(tmp_path):
    sources_root = tmp_path / "sources"
    for source_name, source_meta in SOURCE_REGISTRY.items():
        (sources_root / source_name / f"{source_meta['partition_key']}='2024-01-01'").mkdir(parents=True, exist_ok=True)

    discovered = discover_source_partitions(sources_root)

    assert len(discovered) == len(SOURCE_REGISTRY)
    assert {item["source_name"] for item in discovered} == set(SOURCE_REGISTRY)


def test_discover_source_partitions_rejects_missing_source_dir(tmp_path):
    sources_root = tmp_path / "sources"
    (sources_root / "client_cards_daily" / "row_actual_to='2024-01-01'").mkdir(parents=True, exist_ok=True)

    with pytest.raises(FileNotFoundError, match="Отсутствует директория источника"):
        discover_source_partitions(sources_root)


def test_discover_source_partitions_rejects_wrong_partition_key(tmp_path):
    sources_root = tmp_path / "sources"
    for source_name in SOURCE_REGISTRY:
        partition_key = "wrong_key" if source_name == "credit_cards_info" else SOURCE_REGISTRY[source_name]["partition_key"]
        (sources_root / source_name / f"{partition_key}='2024-01-01'").mkdir(parents=True, exist_ok=True)

    with pytest.raises(ValueError, match="ожидался ключ"):
        discover_source_partitions(sources_root)


def test_build_manifest_fingerprint_changes_when_partition_files_change(tmp_path):
    partition_dir = tmp_path / "report_dt='2024-03-31'"
    partition_dir.mkdir(parents=True)
    first_file = partition_dir / "part-00000.parquet"
    first_file.write_bytes(b"abc")

    first = build_manifest_fingerprint(partition_dir)
    second = build_manifest_fingerprint(partition_dir)
    (partition_dir / "part-00001.parquet").write_bytes(b"xyz")
    third = build_manifest_fingerprint(partition_dir)

    assert first == second
    assert third != first


def test_determine_partition_action_covers_new_skip_fail():
    assert determine_partition_action(None, "abc") == "new"
    assert determine_partition_action({"manifest_fingerprint": "abc"}, "abc") == "skip"
    assert determine_partition_action({"manifest_fingerprint": "abc"}, "def") == "fail"
