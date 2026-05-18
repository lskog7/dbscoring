from __future__ import annotations

import ast

import pytest

from tests.notebook_loader import POLARS_NOTEBOOK_PATH, SPARK_NOTEBOOK_PATH, read_notebook


@pytest.mark.parametrize("notebook_path", [SPARK_NOTEBOOK_PATH, POLARS_NOTEBOOK_PATH])
def test_every_function_cell_is_followed_by_example_cell(notebook_path):
    notebook = read_notebook(notebook_path)
    cells = notebook["cells"]

    for index, cell in enumerate(cells):
        if cell["cell_type"] != "code":
            continue
        if "function_defs" not in cell.get("metadata", {}).get("tags", []):
            continue

        assert index + 1 < len(cells), f"{notebook_path.name}: после function_defs-ячейки должен быть пример"
        next_cell = cells[index + 1]
        assert next_cell["cell_type"] == "code", f"{notebook_path.name}: пример должен быть code-ячейкой"
        assert "examples" in next_cell.get("metadata", {}).get("tags", []), (
            f"{notebook_path.name}: сразу после function_defs-ячейки должна идти examples-ячейка"
        )


@pytest.mark.parametrize("notebook_path", [SPARK_NOTEBOOK_PATH, POLARS_NOTEBOOK_PATH])
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


@pytest.mark.parametrize("notebook_path", [SPARK_NOTEBOOK_PATH, POLARS_NOTEBOOK_PATH])
def test_notebook_has_single_final_run_cell(notebook_path):
    notebook = read_notebook(notebook_path)
    final_run_cells = [
        index
        for index, cell in enumerate(notebook["cells"])
        if cell["cell_type"] == "code" and "final_run" in cell.get("metadata", {}).get("tags", [])
    ]
    assert len(final_run_cells) == 1, f"{notebook_path.name}: ожидается ровно одна final_run-ячейка"
