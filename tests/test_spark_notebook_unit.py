"""Unit-level static checks for Spark helper code in current lab3 artifacts."""

from __future__ import annotations

import ast
import datetime as dt
from pathlib import Path

import pytest

from tests.notebook_loader import ROOT_DIR
from tests.notebook_loader import TARGET_SCRIPT_PATHS


EXPECTED_FUNCTIONS = {
    "init_spark",
    "find_data_dir",
    "normalize_source_columns",
    "read_source_partition",
    "build_dim_sources",
    "build_dim_attributes",
    "verticalize_monthly",
    "verticalize_daily",
    "merge_scd1",
    "merge_scd2",
    "create_warehouse",
    "initial_load_warehouse",
    "update_warehouse",
    "run_simple_checks",
}


@pytest.mark.parametrize("script_path", TARGET_SCRIPT_PATHS)
def test_scripts_compile(script_path: Path):
    source = script_path.read_text(encoding="utf-8")

    compile(source, str(script_path), "exec")


@pytest.mark.parametrize("script_path", TARGET_SCRIPT_PATHS)
def test_expected_helper_functions_exist(script_path: Path):
    tree = ast.parse(script_path.read_text(encoding="utf-8"))
    function_names = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}

    assert EXPECTED_FUNCTIONS <= function_names


def _load_merge_scd2(script_path: Path):
    pytest.importorskip("pyspark")
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    source = script_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(script_path))
    function_source = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "merge_scd2":
            function_source = ast.get_source_segment(source, node)
            break

    assert function_source is not None
    namespace = {"F": F, "Window": Window}
    exec(function_source, namespace)
    return namespace["merge_scd2"]


def _load_merge_scd1(script_path: Path):
    pytest.importorskip("pyspark")
    from pyspark.sql import functions as F

    source = script_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(script_path))
    function_source = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "merge_scd1":
            function_source = ast.get_source_segment(source, node)
            break

    assert function_source is not None
    namespace = {"F": F}
    exec(function_source, namespace)
    return namespace["merge_scd1"]


def _load_scd_namespace(script_path: Path):
    pytest.importorskip("pyspark")
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    source = script_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(script_path))
    namespace = {"F": F, "Window": Window}
    wanted_assignments = {"SOURCE_CONFIG", "COLUMN_ALIASES"}
    wanted_functions = {
        "normalize_source_columns",
        "verticalize_monthly",
        "verticalize_daily",
        "merge_scd1",
        "merge_scd2",
    }
    for node in tree.body:
        if isinstance(node, ast.Assign):
            assigned_names = {target.id for target in node.targets if isinstance(target, ast.Name)}
            if assigned_names & wanted_assignments:
                exec(ast.get_source_segment(source, node), namespace)
        if isinstance(node, ast.FunctionDef) and node.name in wanted_functions:
            exec(ast.get_source_segment(source, node), namespace)
    return namespace


def _dim_attributes_for_source(spark, source_config: dict, source_name: str):
    from pyspark.sql.types import IntegerType
    from pyspark.sql.types import StringType
    from pyspark.sql.types import StructField
    from pyspark.sql.types import StructType

    cfg = source_config[source_name]
    rows = [
        (index, column_name, int(cfg["source_id"]))
        for index, column_name in enumerate(
            [column for column in cfg["business_columns"] if column != "client_id"],
            start=1,
        )
    ]
    schema = StructType(
        [
            StructField("attribute_id", IntegerType(), False),
            StructField("attribute_name", StringType(), False),
            StructField("source_id", IntegerType(), False),
        ]
    )
    return spark.createDataFrame(rows, schema)


def _monthly_schema():
    from pyspark.sql.types import IntegerType
    from pyspark.sql.types import LongType
    from pyspark.sql.types import StringType
    from pyspark.sql.types import StructField
    from pyspark.sql.types import StructType
    from pyspark.sql.types import TimestampType

    return StructType(
        [
            StructField("client_id", StringType(), False),
            StructField("attribute_id", IntegerType(), False),
            StructField("report_dt", StringType(), False),
            StructField("attribute_value", StringType(), True),
            StructField("source_id", IntegerType(), False),
            StructField("row_update_dtime", TimestampType(), True),
            StructField("loading_id", LongType(), True),
            StructField("row_hash_val", StringType(), True),
        ]
    )


def _spark_session(app_name: str):
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[1]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


REAL_SOURCES_ROOT = ROOT_DIR / "source" / "sources"


@pytest.mark.spark
@pytest.mark.parametrize("script_path", TARGET_SCRIPT_PATHS)
def test_merge_scd1_replaces_old_keys_and_inserts_new_keys(script_path: Path):
    merge_scd1 = _load_merge_scd1(script_path)
    spark = _spark_session("test_merge_scd1")
    schema = _monthly_schema()
    columns = [field.name for field in schema.fields]

    try:
        old_df = spark.createDataFrame(
            [
                ("C1", 101, "2024-03-31", "old", 1, dt.datetime(2024, 3, 31, 9), 10, "old-hash"),
                ("C1", 102, "2024-03-31", "keep", 1, dt.datetime(2024, 3, 31, 9), 10, "keep-hash"),
            ],
            schema,
        )
        new_df = spark.createDataFrame(
            [
                ("C1", 101, "2024-04-30", "new", 1, dt.datetime(2024, 4, 30, 10), 20, "new-hash"),
                ("C1", 103, "2024-04-30", "insert", 1, dt.datetime(2024, 4, 30, 10), 20, "insert-hash"),
            ],
            schema,
        )

        result = merge_scd1(old_df, new_df)
        rows = [
            tuple(row[column] for column in ["attribute_id", "report_dt", "attribute_value", "loading_id", "row_hash_val"])
            for row in result.orderBy("attribute_id").collect()
        ]

        assert result.columns == columns
        assert rows == [
            (101, "2024-04-30", "new", 20, "new-hash"),
            (102, "2024-03-31", "keep", 10, "keep-hash"),
            (103, "2024-04-30", "insert", 20, "insert-hash"),
        ]
    finally:
        spark.stop()


@pytest.mark.spark
@pytest.mark.parametrize("script_path", TARGET_SCRIPT_PATHS)
def test_merge_scd1_rejects_duplicate_keys_inside_inputs(script_path: Path):
    merge_scd1 = _load_merge_scd1(script_path)
    spark = _spark_session("test_merge_scd1_duplicates")
    schema = _monthly_schema()

    try:
        old_df = spark.createDataFrame(
            [
                ("C1", 101, "2024-03-31", "old", 1, dt.datetime(2024, 3, 31, 9), 10, "old-hash"),
            ],
            schema,
        )
        duplicated_new_df = spark.createDataFrame(
            [
                ("C1", 101, "2024-03-31", "new-1", 1, dt.datetime(2024, 4, 1, 10), 20, "new-hash-1"),
                ("C1", 101, "2024-03-31", "new-2", 1, dt.datetime(2024, 4, 1, 11), 21, "new-hash-2"),
            ],
            schema,
        )

        with pytest.raises(ValueError, match="new_df contains duplicate SCD1 business keys"):
            merge_scd1(old_df, duplicated_new_df)
    finally:
        spark.stop()


@pytest.mark.spark
@pytest.mark.parametrize("script_path", TARGET_SCRIPT_PATHS)
def test_merge_scd2_preserves_history_and_closes_overlapping_versions(script_path: Path):
    merge_scd2 = _load_merge_scd2(script_path)
    from pyspark.sql.types import IntegerType
    from pyspark.sql.types import LongType
    from pyspark.sql.types import StringType
    from pyspark.sql.types import StructField
    from pyspark.sql.types import StructType
    from pyspark.sql.types import TimestampType

    spark = _spark_session("test_merge_scd2")

    schema = StructType(
        [
            StructField("client_id", StringType(), False),
            StructField("attribute_id", IntegerType(), False),
            StructField("attribute_value", StringType(), True),
            StructField("row_actual_from", StringType(), False),
            StructField("row_actual_to", StringType(), False),
            StructField("source_id", IntegerType(), False),
            StructField("row_update_dtime", TimestampType(), True),
            StructField("loading_id", LongType(), True),
            StructField("row_hash_val", StringType(), True),
        ]
    )
    columns = [field.name for field in schema.fields]
    try:
        old_df = spark.createDataFrame(
            [
                ("C1", 10, "old", "2024-01-01", "9999-12-31", 3, dt.datetime(2024, 1, 1, 9), 1, "old-hash"),
                ("C1", 20, "same", "2024-01-01", "9999-12-31", 3, dt.datetime(2024, 1, 1, 9), 1, "same-hash"),
                ("C1", 30, "history", "2023-12-01", "2024-01-01", 3, dt.datetime(2023, 12, 1, 9), 1, "history-hash"),
            ],
            schema,
        )
        new_df = spark.createDataFrame(
            [
                ("C1", 10, "new", "2024-02-01", "9999-12-31", 3, dt.datetime(2024, 2, 1, 10), 2, "new-hash"),
                ("C1", 20, "same", "2024-02-01", "9999-12-31", 3, dt.datetime(2024, 2, 1, 10), 2, "same-new-load-hash"),
                ("C2", 10, "insert", "2024-02-01", "9999-12-31", 3, dt.datetime(2024, 2, 1, 10), 2, "insert-hash"),
            ],
            schema,
        )

        result = merge_scd2(old_df, new_df)
        rows = [
            tuple(row[column] for column in ["client_id", "attribute_id", "attribute_value", "row_actual_from", "row_actual_to", "loading_id"])
            for row in result.orderBy("client_id", "attribute_id", "row_actual_from").collect()
        ]

        assert result.columns == columns
        assert rows == [
            ("C1", 10, "old", "2024-01-01", "2024-02-01", 1),
            ("C1", 10, "new", "2024-02-01", "9999-12-31", 2),
            ("C1", 20, "same", "2024-01-01", "9999-12-31", 1),
            ("C1", 30, "history", "2023-12-01", "2024-01-01", 1),
            ("C2", 10, "insert", "2024-02-01", "9999-12-31", 2),
        ]
    finally:
        spark.stop()


@pytest.mark.spark
@pytest.mark.parametrize("script_path", TARGET_SCRIPT_PATHS)
def test_real_debit_monthly_scd1_replaces_previous_real_partition(script_path: Path):
    from pyspark.sql import functions as F

    namespace = _load_scd_namespace(script_path)
    spark = _spark_session("test_real_debit_monthly_scd1")
    source_name = "deb_cards_info"
    common_client_ids = [
        "0011d814599888d498f9d207ecda24e39900d55c1a5bbf0c4946fd24e1828b7a",
        "0012bfa34f4199a64f5bd3c6da1f01890854714bbc410089ae47bae82f586ef7",
        "00226ce0da643acd1947a7dd8f1888931c9c8ebaa6c9b333b80881017683ce4c",
    ]

    try:
        dim_attributes_df = _dim_attributes_for_source(spark, namespace["SOURCE_CONFIG"], source_name)
        feb_df = (
            spark.read.parquet(str(REAL_SOURCES_ROOT / source_name / "report_dt='2023-02-28'"))
            .filter(F.col("id").isin(common_client_ids))
        )
        mar_df = (
            spark.read.parquet(str(REAL_SOURCES_ROOT / source_name / "report_dt='2023-03-31'"))
            .filter(F.col("id").isin(common_client_ids))
        )

        old_rows_df = namespace["verticalize_monthly"](
            namespace["normalize_source_columns"](feb_df),
            source_name,
            dim_attributes_df,
        )
        new_rows_df = namespace["verticalize_monthly"](
            namespace["normalize_source_columns"](mar_df),
            source_name,
            dim_attributes_df,
        )

        result_df = namespace["merge_scd1"](old_rows_df, new_rows_df)
        result_keys_df = result_df.select("client_id", "attribute_id", "report_dt", "attribute_value", "row_hash_val")
        expected_keys_df = new_rows_df.select("client_id", "attribute_id", "report_dt", "attribute_value", "row_hash_val")

        assert old_rows_df.count() == 27
        assert new_rows_df.count() == 27
        assert result_df.count() == 27
        assert result_df.filter(F.col("report_dt") == "2023-02-28").count() == 0
        assert result_df.filter(F.col("report_dt") == "2023-03-31").count() == 27
        assert result_keys_df.exceptAll(expected_keys_df).count() == 0
        assert expected_keys_df.exceptAll(result_keys_df).count() == 0
    finally:
        spark.stop()


@pytest.mark.spark
@pytest.mark.parametrize("script_path", TARGET_SCRIPT_PATHS)
def test_real_daily_scd2_closes_changed_current_versions_and_skips_unchanged_real_values(script_path: Path):
    from pyspark.sql import functions as F

    namespace = _load_scd_namespace(script_path)
    spark = _spark_session("test_real_daily_scd2")
    source_name = "client_cards_daily"
    client_id = "00612cbd2c801e3b6ae8c2027a3efca979f8720b2399a2260a687e3aaa19939e"

    try:
        dim_attributes_df = _dim_attributes_for_source(spark, namespace["SOURCE_CONFIG"], source_name)
        old_real_df = (
            spark.read.parquet(str(REAL_SOURCES_ROOT / source_name / "row_actual_to='2023-04-03'"))
            .filter(F.col("id") == client_id)
            .withColumn("row_actual_to", F.lit("9999-12-31"))
        )
        new_real_df = (
            spark.read.parquet(str(REAL_SOURCES_ROOT / source_name / "row_actual_to='9999-12-31'"))
            .filter(F.col("id") == client_id)
        )

        old_rows_df = namespace["verticalize_daily"](
            namespace["normalize_source_columns"](old_real_df),
            source_name,
            dim_attributes_df,
        )
        new_rows_df = namespace["verticalize_daily"](
            namespace["normalize_source_columns"](new_real_df),
            source_name,
            dim_attributes_df,
        )
        result_df = namespace["merge_scd2"](old_rows_df, new_rows_df)
        attribute_ids = {
            row["attribute_name"]: row["attribute_id"]
            for row in dim_attributes_df.collect()
        }

        cc_stoplist_rows = [
            (
                row["attribute_value"],
                row["row_actual_from"],
                row["row_actual_to"],
            )
            for row in (
                result_df
                .filter(F.col("attribute_id") == attribute_ids["cc_stoplist"])
                .orderBy("row_actual_from")
                .collect()
            )
        ]
        srv_mb_rows = [
            (
                row["attribute_value"],
                row["row_actual_from"],
                row["row_actual_to"],
            )
            for row in (
                result_df
                .filter(F.col("attribute_id") == attribute_ids["srv_mb_nflag"])
                .orderBy("row_actual_from")
                .collect()
            )
        ]

        assert old_rows_df.count() == 4
        assert new_rows_df.count() == 4
        assert result_df.count() == 5
        assert cc_stoplist_rows == [
            ("0", "2023-04-03", "2023-04-04"),
            ("1", "2023-04-04", "9999-12-31"),
        ]
        assert srv_mb_rows == [(None, "2023-04-03", "9999-12-31")]
    finally:
        spark.stop()
