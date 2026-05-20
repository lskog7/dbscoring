"""Static checks for the physical model contract shown in schema.png."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.notebook_loader import TARGET_NOTEBOOK_PATHS
from tests.notebook_loader import TARGET_SCRIPT_PATHS
from tests.notebook_loader import notebook_code


EXPECTED_SCHEMAS = {
    "DIM_SOURCES_SCHEMA": [
        "source_id",
        "source_name",
        "source_description",
        "update_frequency",
        "row_create_dtime",
        "valid_to",
        "valid_from",
        "row_update_dtime",
    ],
    "DIM_ATTRIBUTES_SCHEMA": [
        "attribute_id",
        "attribute_name",
        "attribute_description",
        "data_type",
        "source_id",
        "update_frequency",
        "row_create_dtime",
        "row_update_dtime",
    ],
    "LOAD_LOG_SCHEMA": [
        "load_id",
        "source_id",
        "source_report_dt",
        "load_start_dtime",
        "load_end_dtime",
        "target_table",
        "load_status",
        "loading_id",
        "error_message",
    ],
    "CLIENT_MONTHLY_SCHEMA": [
        "client_id",
        "attribute_id",
        "report_dt",
        "attribute_value",
        "source_id",
        "row_update_dtime",
        "loading_id",
        "row_hash_val",
    ],
    "CLIENT_DAILY_SCHEMA": [
        "client_id",
        "attribute_id",
        "attribute_value",
        "row_actual_from",
        "row_actual_to",
        "source_id",
        "row_update_dtime",
        "loading_id",
        "row_hash_val",
    ],
}


def source_for(path: Path) -> str:
    if path.suffix == ".ipynb":
        return notebook_code(path)
    return path.read_text(encoding="utf-8")


def schema_fields(source: str, schema_name: str) -> list[str]:
    match = re.search(rf"{schema_name}\s*=\s*StructType\(\[(.*?)\]\)", source, flags=re.S)
    assert match, f"{schema_name} not found"
    return re.findall(r'StructField\("([^"]+)"', match.group(1))


@pytest.mark.parametrize("path", [*TARGET_SCRIPT_PATHS, *TARGET_NOTEBOOK_PATHS])
def test_table_schemas_match_schema_png(path: Path):
    source = source_for(path)

    for schema_name, expected_fields in EXPECTED_SCHEMAS.items():
        assert schema_fields(source, schema_name) == expected_fields


@pytest.mark.parametrize("path", [*TARGET_SCRIPT_PATHS, *TARGET_NOTEBOOK_PATHS])
def test_removed_fields_do_not_appear_in_warehouse_contract(path: Path):
    source = source_for(path)

    assert "row_loading_id" not in source
    assert "is_current" not in source
