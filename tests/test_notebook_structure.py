"""Structural checks for the current lab3 notebooks."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.notebook_loader import TARGET_NOTEBOOK_PATHS
from tests.notebook_loader import iter_code_cells
from tests.notebook_loader import read_notebook


@pytest.mark.parametrize("notebook_path", TARGET_NOTEBOOK_PATHS)
def test_notebook_code_cells_compile(notebook_path: Path):
    notebook = read_notebook(notebook_path)

    for index, cell in iter_code_cells(notebook):
        source = "".join(cell["source"])
        ast.parse(source, filename=f"{notebook_path.name}:cell_{index}")


@pytest.mark.parametrize("notebook_path", TARGET_NOTEBOOK_PATHS)
def test_notebook_has_no_dbscoring_imports(notebook_path: Path):
    notebook = read_notebook(notebook_path)

    for index, cell in iter_code_cells(notebook):
        tree = ast.parse("".join(cell["source"]), filename=f"{notebook_path.name}:cell_{index}")
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name != "dbscoring" and not alias.name.startswith("dbscoring.")
            if isinstance(node, ast.ImportFrom):
                assert node.module != "dbscoring" and not (node.module or "").startswith("dbscoring.")


@pytest.mark.parametrize("notebook_path", TARGET_NOTEBOOK_PATHS)
def test_notebook_has_single_final_run_cell(notebook_path: Path):
    notebook = read_notebook(notebook_path)
    final_run_cells = [
        index
        for index, cell in iter_code_cells(notebook)
        if "create_warehouse()" in "".join(cell["source"])
        and "initial_load_warehouse()" in "".join(cell["source"])
        and "update_warehouse()" in "".join(cell["source"])
        and "run_simple_checks()" in "".join(cell["source"])
    ]

    assert len(final_run_cells) == 1
