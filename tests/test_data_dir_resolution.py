"""Checks for source data directory discovery in lab3 scripts."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.notebook_loader import TARGET_SCRIPT_PATHS


def _load_path_resolution_namespace(script_path: Path) -> dict:
    source = script_path.read_text(encoding="utf-8")
    module = ast.parse(source, filename=str(script_path))
    selected_source = [
        "from pathlib import Path",
        "import zipfile",
    ]

    for node in module.body:
        if isinstance(node, ast.Assign) and any(
            getattr(target, "id", None) == "SOURCE_NAMES" for target in node.targets
        ):
            selected_source.append(ast.get_source_segment(source, node))
        if isinstance(node, ast.FunctionDef) and node.name in {
            "_contains_source_dirs",
            "_candidate_base_dirs",
            "find_data_dir",
        }:
            selected_source.append(ast.get_source_segment(source, node))

    namespace: dict = {}
    exec("\n\n".join(selected_source), namespace)
    return namespace


@pytest.mark.parametrize("script_path", TARGET_SCRIPT_PATHS)
def test_find_data_dir_resolves_sources_from_nested_workdir(script_path: Path, tmp_path: Path, monkeypatch):
    sources_root = tmp_path / "source" / "sources"
    for source_name in ["client_cards_daily", "credit_cards_info", "deb_cards_info"]:
        (sources_root / source_name).mkdir(parents=True)

    nested_workdir = tmp_path / "notebooks"
    nested_workdir.mkdir()
    monkeypatch.chdir(nested_workdir)

    namespace = _load_path_resolution_namespace(script_path)

    assert namespace["find_data_dir"]() == sources_root.resolve()
