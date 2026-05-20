"""Small helpers for validating the current lab3 notebook artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable


ROOT_DIR = Path(__file__).resolve().parents[1]
NOTEBOOK_DIR = ROOT_DIR / "notebooks"
SCRIPTS_DIR = ROOT_DIR / "scripts"

TARGET_NOTEBOOK_PATHS = (
    NOTEBOOK_DIR / "lab3_learning_version.ipynb",
    NOTEBOOK_DIR / "lab3_teacher_version.ipynb",
)
TARGET_SCRIPT_PATHS = (
    SCRIPTS_DIR / "lab3_learning_version.py",
    SCRIPTS_DIR / "lab3_teacher_version.py",
)


def read_notebook(notebook_path: Path) -> dict:
    """Read a notebook JSON file as a Python dictionary."""
    return json.loads(notebook_path.read_text(encoding="utf-8"))


def iter_code_cells(notebook: dict) -> Iterable[tuple[int, dict]]:
    """Yield code cells with their notebook indexes."""
    for index, cell in enumerate(notebook["cells"]):
        if cell["cell_type"] == "code":
            yield index, cell


def notebook_code(notebook_path: Path) -> str:
    """Return all code cells concatenated in notebook order."""
    notebook = read_notebook(notebook_path)
    return "\n\n".join("".join(cell["source"]) for _, cell in iter_code_cells(notebook))
