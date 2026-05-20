"""Consistency checks between script and notebook versions."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.notebook_loader import NOTEBOOK_DIR
from tests.notebook_loader import SCRIPTS_DIR
from tests.notebook_loader import notebook_code


SCRIPT_NOTEBOOK_PAIRS = (
    (SCRIPTS_DIR / "lab3_learning_version.py", NOTEBOOK_DIR / "lab3_learning_version.ipynb"),
    (SCRIPTS_DIR / "lab3_teacher_version.py", NOTEBOOK_DIR / "lab3_teacher_version.ipynb"),
)


def normalized(source: str) -> str:
    lines = []
    for line in source.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        lines.append(line.rstrip())
    return "\n".join(lines)


@pytest.mark.parametrize(("script_path", "notebook_path"), SCRIPT_NOTEBOOK_PAIRS)
def test_script_and_notebook_versions_share_code_contract(script_path: Path, notebook_path: Path):
    script_source = normalized(script_path.read_text(encoding="utf-8"))
    notebook_source = normalized(notebook_code(notebook_path))

    assert script_source == notebook_source
