"""Smoke-тесты полного исполнения Spark-кода как пользовательского артефакта."""

from __future__ import annotations

import pytest

from tests.fixture_builders import build_limited_real_sources
from tests.notebook_loader import SPARK_NOTEBOOK_PATH, execute_notebook, load_namespace


NOTEBOOK_NAMESPACE = load_namespace(SPARK_NOTEBOOK_PATH)
create_spark_session = NOTEBOOK_NAMESPACE["create_spark_session"]
run_warehouse_update = NOTEBOOK_NAMESPACE["run_warehouse_update"]


@pytest.mark.spark
@pytest.mark.smoke
def test_notebook_runtime_executes_end_to_end(tmp_path):
    sources_root = tmp_path / "sources"
    spark = create_spark_session(
        app_name="spark-lab-runtime-smoke",
        warehouse_dir=tmp_path / "spark_warehouse",
        shuffle_partitions=2,
    )

    try:
        source_counts = build_limited_real_sources(spark, sources_root)
        summary = run_warehouse_update(
            spark,
            sources_root=sources_root,
            warehouse_root=tmp_path / "warehouse",
        )
    finally:
        spark.stop()

    assert summary["loaded_partitions"] == len(source_counts)


@pytest.mark.spark
@pytest.mark.smoke
def test_spark_notebook_executes_end_to_end(spark_session, tmp_path, monkeypatch):
    sources_root = tmp_path / "data" / "sources"
    source_counts = build_limited_real_sources(spark_session, sources_root)

    monkeypatch.setenv("DBSCORING_SKIP_FINAL_RUN", "0")
    monkeypatch.setenv("DBSCORING_SOURCES_ROOT", str(sources_root))
    monkeypatch.setenv("DBSCORING_WAREHOUSE_ROOT", str(tmp_path / "warehouse"))

    namespace = execute_notebook(
        SPARK_NOTEBOOK_PATH,
        cwd=tmp_path,
    )

    assert "final_summary" in namespace
    assert namespace["final_summary"]["loaded_partitions"] == len(source_counts)
