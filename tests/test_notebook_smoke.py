"""Smoke checks for notebook JSON and visible lab sections."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.notebook_loader import TARGET_NOTEBOOK_PATHS
from tests.notebook_loader import read_notebook


@pytest.mark.parametrize("notebook_path", TARGET_NOTEBOOK_PATHS)
def test_notebook_json_is_valid_and_nonempty(notebook_path: Path):
    notebook = read_notebook(notebook_path)

    assert notebook["cells"]
    assert notebook["nbformat"] >= 4


@pytest.mark.parametrize("notebook_path", TARGET_NOTEBOOK_PATHS)
def test_notebook_mentions_all_schema_tables(notebook_path: Path):
    raw_text = notebook_path.read_text(encoding="utf-8")

    for table_name in [
        "dim_sources",
        "dim_attributes",
        "load_log",
        "client_monthly_attrs_scd1",
        "client_daily_attrs_scd2",
    ]:
        assert table_name in raw_text
