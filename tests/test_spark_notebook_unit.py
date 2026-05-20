"""Unit-level static checks for Spark helper code in current lab3 artifacts."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.notebook_loader import TARGET_SCRIPT_PATHS


EXPECTED_FUNCTIONS = {
    "init_spark",
    "find_data_dir",
    "normalize_source_columns",
    "read_source_partition",
    "build_dim_sources",
    "build_dim_attributes",
    "verticalize_monthly",
    "verticalize_daily",
    "merge_scd1",
    "merge_scd2",
    "create_warehouse",
    "initial_load_warehouse",
    "update_warehouse",
    "run_simple_checks",
}


@pytest.mark.parametrize("script_path", TARGET_SCRIPT_PATHS)
def test_scripts_compile(script_path: Path):
    source = script_path.read_text(encoding="utf-8")

    compile(source, str(script_path), "exec")


@pytest.mark.parametrize("script_path", TARGET_SCRIPT_PATHS)
def test_expected_helper_functions_exist(script_path: Path):
    tree = ast.parse(script_path.read_text(encoding="utf-8"))
    function_names = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}

    assert EXPECTED_FUNCTIONS <= function_names
