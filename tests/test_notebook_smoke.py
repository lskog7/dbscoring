"""Smoke-тесты полного исполнения Spark и Polars notebook как пользовательских артефактов."""

from __future__ import annotations

import pytest

from tests.fixture_builders import build_sample_sources
from tests.notebook_loader import (
    POLARS_NOTEBOOK_PATH,
    SPARK_NOTEBOOK_PATH,
    execute_notebook,
)


@pytest.mark.spark
@pytest.mark.smoke
def test_spark_notebook_executes_end_to_end(tmp_path):
    sources_root = tmp_path / "sources"
    build_sample_sources(sources_root)

    namespace = execute_notebook(
        SPARK_NOTEBOOK_PATH,
        extra_env={
            "DBSCORING_SOURCES_ROOT": sources_root,
            "DBSCORING_WAREHOUSE_ROOT": tmp_path / "warehouse_prod",
            "DBSCORING_DEBUG_ROOT": tmp_path / "warehouse_debug",
            "DBSCORING_RUN_MODE": "debug",
            "DBSCORING_CLEAN_DEBUG": "1",
        },
    )

    assert "final_summary" in namespace
    assert namespace["final_summary"]["loaded_partitions"] == 3
    namespace["example_spark"].stop()


@pytest.mark.smoke
def test_polars_notebook_executes_end_to_end(tmp_path):
    sources_root = tmp_path / "sources"
    build_sample_sources(sources_root)

    namespace = execute_notebook(
        POLARS_NOTEBOOK_PATH,
        extra_env={
            "DBSCORING_SOURCES_ROOT": sources_root,
            "DBSCORING_WAREHOUSE_ROOT": tmp_path / "warehouse_prod",
            "DBSCORING_DEBUG_ROOT": tmp_path / "warehouse_debug",
            "DBSCORING_RUN_MODE": "debug",
            "DBSCORING_CLEAN_DEBUG": "1",
        },
    )

    assert "final_summary" in namespace
    assert namespace["final_summary"]["loaded_partitions"] == 3
