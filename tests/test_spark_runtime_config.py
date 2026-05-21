"""Static checks for local Spark runtime settings used by notebooks and scripts."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.notebook_loader import TARGET_NOTEBOOK_PATHS
from tests.notebook_loader import TARGET_SCRIPT_PATHS
from tests.notebook_loader import notebook_code


def source_for(path: Path) -> str:
    if path.suffix == ".ipynb":
        return notebook_code(path)
    return path.read_text(encoding="utf-8")


@pytest.mark.parametrize("path", [*TARGET_SCRIPT_PATHS, *TARGET_NOTEBOOK_PATHS])
def test_spark_uses_notebook_safe_local_runtime_settings(path: Path):
    source = source_for(path)

    assert "SparkSession.getActiveSession()" in source
    assert ".master(\"local[2]\")" in source
    assert ".config(\"spark.driver.memory\", \"4g\")" in source
    assert ".config(\"spark.executor.memory\", \"4g\")" in source
    assert ".config(\"spark.sql.shuffle.partitions\", \"8\")" in source
    assert ".config(\"spark.default.parallelism\", \"8\")" in source
    assert ".master(\"local[*]\")" not in source
