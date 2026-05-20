"""Структурные проверки notebook: автономность, docstring и наличие финального запуска."""

from __future__ import annotations

import ast

import pytest

from tests.notebook_loader import SPARK_NOTEBOOK_PATH, read_notebook


@pytest.mark.parametrize("notebook_path", [SPARK_NOTEBOOK_PATH])
def test_notebook_has_no_dbscoring_imports(notebook_path):
    notebook = read_notebook(notebook_path)

    for index, cell in enumerate(notebook["cells"]):
        if cell["cell_type"] != "code":
            continue

        tree = ast.parse("".join(cell["source"]), filename=f"{notebook_path.name}:cell_{index}")
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name != "dbscoring" and not alias.name.startswith("dbscoring."), (
                        f"{notebook_path.name}: cell {index} не должна импортировать {alias.name}"
                    )
            if isinstance(node, ast.ImportFrom):
                assert node.module != "dbscoring" and not (node.module or "").startswith("dbscoring."), (
                    f"{notebook_path.name}: cell {index} не должна импортировать из {node.module}"
                )


@pytest.mark.parametrize("notebook_path", [SPARK_NOTEBOOK_PATH])
def test_notebook_has_no_examples_cells(notebook_path):
    notebook = read_notebook(notebook_path)
    example_cells = [
        index
        for index, cell in enumerate(notebook["cells"])
        if cell["cell_type"] == "code" and "examples" in cell.get("metadata", {}).get("tags", [])
    ]
    assert not example_cells, f"{notebook_path.name}: examples-ячейки больше не допускаются"


@pytest.mark.parametrize("notebook_path", [SPARK_NOTEBOOK_PATH])
def test_every_function_has_a_docstring(notebook_path):
    notebook = read_notebook(notebook_path)

    for index, cell in enumerate(notebook["cells"]):
        if cell["cell_type"] != "code":
            continue
        if "function_defs" not in cell.get("metadata", {}).get("tags", []):
            continue

        tree = ast.parse("".join(cell["source"]), filename=f"{notebook_path.name}:cell_{index}")
        functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)]
        assert functions, f"{notebook_path.name}: в function_defs-ячейке {index} не найдены функции"
        for function in functions:
            assert ast.get_docstring(function), (
                f"{notebook_path.name}: функция {function.name} должна содержать подробный docstring"
            )


@pytest.mark.parametrize("notebook_path", [SPARK_NOTEBOOK_PATH])
def test_notebook_has_single_final_run_cell(notebook_path):
    notebook = read_notebook(notebook_path)
    final_run_cells = [
        index
        for index, cell in enumerate(notebook["cells"])
        if cell["cell_type"] == "code" and "final_run" in cell.get("metadata", {}).get("tags", [])
    ]
    assert len(final_run_cells) == 1, f"{notebook_path.name}: ожидается ровно одна final_run-ячейка"
