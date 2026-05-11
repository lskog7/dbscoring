from __future__ import annotations

from pathlib import Path

import pytest

from dbscoring.paths import ProjectPaths
from dbscoring.testing import create_source_fixtures


@pytest.fixture()
def fixture_paths(tmp_path: Path) -> ProjectPaths:
    data_root = tmp_path / "sources"
    warehouse_root = tmp_path / "warehouse"
    create_source_fixtures(data_root)
    return ProjectPaths(data_root=data_root, warehouse_root=warehouse_root).resolve()
