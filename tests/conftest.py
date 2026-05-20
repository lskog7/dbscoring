"""Shared pytest fixtures for Spark-based tests."""

from __future__ import annotations

import os

import pytest

from tests.notebook_loader import SPARK_NOTEBOOK_PATH, load_namespace


NOTEBOOK_NAMESPACE = load_namespace(SPARK_NOTEBOOK_PATH)
create_spark_session = NOTEBOOK_NAMESPACE["create_spark_session"]


@pytest.fixture()
def spark_session(tmp_path):
    original_cwd = os.getcwd()
    os.chdir(tmp_path)
    spark = create_spark_session(
        app_name="dbscoring-tests",
        warehouse_dir=tmp_path / "spark_warehouse",
        shuffle_partitions=2,
    )
    try:
        yield spark
    finally:
        spark.stop()
        os.chdir(original_cwd)
