"""Утилиты для чтения, частичного импорта и полного исполнения notebook как источника кода."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterable


ROOT_DIR = Path(__file__).resolve().parents[1]
NOTEBOOK_DIR = ROOT_DIR / "notebooks"
SPARK_NOTEBOOK_PATH = NOTEBOOK_DIR / "spark_lab.ipynb"
POLARS_NOTEBOOK_PATH = NOTEBOOK_DIR / "polars_lab.ipynb"


def read_notebook(notebook_path: Path) -> dict:
    return json.loads(notebook_path.read_text(encoding="utf-8"))


def iter_code_cells(notebook: dict) -> Iterable[tuple[int, dict]]:
    for index, cell in enumerate(notebook["cells"]):
        if cell["cell_type"] == "code":
            yield index, cell


def load_namespace(notebook_path: Path, allowed_tags: tuple[str, ...] = ("bootstrap", "function_defs")) -> dict:
    notebook = read_notebook(notebook_path)
    namespace: dict = {"__name__": f"loaded_{notebook_path.stem}"}
    allowed_tags_set = set(allowed_tags)

    for index, cell in iter_code_cells(notebook):
        cell_tags = set(cell.get("metadata", {}).get("tags", []))
        if cell_tags & allowed_tags_set:
            source = "".join(cell["source"])
            exec(compile(source, f"{notebook_path.name}:cell_{index}", "exec"), namespace)

    return namespace


def execute_notebook(notebook_path: Path, extra_env: dict[str, str] | None = None) -> dict:
    notebook = read_notebook(notebook_path)
    namespace: dict = {"__name__": f"executed_{notebook_path.stem}"}
    original_env = os.environ.copy()

    try:
        if extra_env:
            os.environ.update({key: str(value) for key, value in extra_env.items()})

        for index, cell in iter_code_cells(notebook):
            source = "".join(cell["source"])
            exec(compile(source, f"{notebook_path.name}:cell_{index}", "exec"), namespace)
    finally:
        os.environ.clear()
        os.environ.update(original_env)

    return namespace
