"""Checks for source registry and warehouse table names in lab3 scripts."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.notebook_loader import TARGET_SCRIPT_PATHS


EXPECTED_TABLES = {
    "dim_sources",
    "dim_attributes",
    "load_log",
    "client_monthly_attrs_scd1",
    "client_daily_attrs_scd2",
}


@pytest.mark.parametrize("script_path", TARGET_SCRIPT_PATHS)
def test_source_config_uses_three_expected_sources(script_path: Path):
    source = script_path.read_text(encoding="utf-8")

    assert '"deb_cards_info": {' in source
    assert '"credit_cards_info": {' in source
    assert '"client_cards_daily": {' in source
    assert '"target_table": "client_monthly_attrs_scd1"' in source
    assert '"target_table": "client_daily_attrs_scd2"' in source


@pytest.mark.parametrize("script_path", TARGET_SCRIPT_PATHS)
def test_table_schema_registry_contains_only_schema_tables(script_path: Path):
    source = script_path.read_text(encoding="utf-8")

    for table_name in EXPECTED_TABLES:
        assert f'"{table_name}"' in source

    assert '"tech_source_partitions"' not in source
