import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path
from datetime import datetime

SOURCE_NAMES = [
    "client_cards_daily",
    "credit_cards_info",
    "deb_cards_info",
]


def init_spark(app_name="lab3_credit_scoring_warehouse"):
    """Создает локальную Spark-сессию для Google Colab или Jupyter Notebook."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark"])
        from pyspark.sql import SparkSession

    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        try:
            active_session.stop()
        except Exception:
            pass

    spark_session = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("ERROR")
    return spark_session


def _contains_source_dirs(path):
    path = Path(path)
    return path.exists() and all((path / name).is_dir() for name in SOURCE_NAMES)


def _candidate_base_dirs():
    """Возвращает рабочую директорию, ее родителей и стандартные внешние корни."""
    base_dirs = []
    for root in [Path.cwd(), *Path.cwd().parents, Path("/content"), Path("/mnt/data")]:
        if root.exists() and root not in base_dirs:
            base_dirs.append(root)
    return base_dirs


def find_data_dir():
    """Находит директорию, внутри которой лежат три исходника с parquet-партициями."""
    candidates = [Path("/data")]
    for base_dir in _candidate_base_dirs():
        candidates.extend(
            [
                base_dir / "data",
                base_dir / "source" / "sources",
                base_dir / "source",
            ]
        )

    for candidate in candidates:
        if _contains_source_dirs(candidate):
            return candidate.resolve()

    zip_candidates = [
        Path("source.zip"),
        Path("/content/source.zip"),
        Path("/mnt/data/source.zip"),
    ]
    for zip_path in zip_candidates:
        if zip_path.exists():
            extract_to = zip_path.parent
            print(f"Директории с parquet не найдены. Распаковываю {zip_path} в {extract_to}")
            with zipfile.ZipFile(zip_path, "r") as archive:
                archive.extractall(extract_to)
            break

    for root in _candidate_base_dirs():
        for path in root.rglob("*"):
            if path.is_dir() and _contains_source_dirs(path):
                return path.resolve()

    raise FileNotFoundError(
        "Не удалось найти DATA_DIR. Ожидалась директория с подпапками: "
        + ", ".join(SOURCE_NAMES)
    )


spark = init_spark()

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
)

DATA_DIR = find_data_dir()
WAREHOUSE_DIR = "warehouse"

print(f"DATA_DIR = {DATA_DIR}")
print(f"WAREHOUSE_DIR = {Path(WAREHOUSE_DIR).resolve()}")


SOURCE_CONFIG = {
    "deb_cards_info": {
        "source_id": 1,
        "source_name": "deb_cards_info",
        "source_description": "Данные о транзакционной активности клиента по дебетовым картам за отчетный месяц.",
        "update_frequency": "1 месяц",
        "storage_type": "SCD1",
        "partition_col": "report_dt",
        "initial_partition": "2023-02-28",
        "update_partition": "2023-03-31",
        "target_table": "client_monthly_attrs_scd1",
        "business_columns": [
            "client_id",
            "onl_bank_active_1m_nflag",
            "auto_pay_active_qty",
            "cl_income_1m_amt",
            "dep_acc_1st_open_dt",
            "wdr_cash_6m_amt",
            "cash_op_6m_amt",
            "cash_3m_qty",
            "lst_balance_amt",
            "card_active_1m_nflag",
        ],
    },
    "credit_cards_info": {
        "source_id": 2,
        "source_name": "credit_cards_info",
        "source_description": "Данные по кредитным картам клиента за отчетный месяц.",
        "update_frequency": "1 месяц",
        "storage_type": "SCD1",
        "partition_col": "report_dt",
        "initial_partition": "2023-02-28",
        "update_partition": "2023-03-31",
        "target_table": "client_monthly_attrs_scd1",
        "business_columns": [
            "client_id",
            "client_income_amt",
            "oi_total_amt",
            "act_pl_os_rub_amt",
            "payroll_client_nflag",
            "inf_payroll_rub_amt",
            "legal_entity_amt",
            "otf_loan_rub_amt",
            "otf_fee_rub_amt",
            "inf_transfer_rub_amt",
            "cc_ever_nflag",
        ],
    },
    "client_cards_daily": {
        "source_id": 3,
        "source_name": "client_cards_daily",
        "source_description": "Данные по клиенту, обновляемые раз в день.",
        "update_frequency": "1 день",
        "storage_type": "SCD2",
        "partition_col": "row_actual_to",
        "initial_partition": "2023-04-03",
        "update_partition": "9999-12-31",
        "target_table": "client_daily_attrs_scd2",
        "business_columns": [
            "client_id",
            "srv_mb_nflag",
            "cc_stoplist",
            "lne_tot_debt_int_ovrd_rub_amt",
            "lne_tot_debt_ovrd_rub_amt",
        ],
    },
}

# Технические поля не попадают в dim_attributes как бизнес-атрибуты.
TECHNICAL_COLUMNS = {
    "row_update_dtime",
    "loading_id",
    "row_hash_val",
    "report_dt",
    "row_actual_from",
    "row_actual_to",
}

# В реальных parquet из архива есть несколько расхождений с формулировкой задания:
# id вместо client_id и опечатка nfalg вместо nflag.
COLUMN_ALIASES = {
    "id": "client_id",
    "onl_bank_active_1m_nfalg": "onl_bank_active_1m_nflag",
}

ATTRIBUTE_DESCRIPTIONS = {
    "onl_bank_active_1m_nflag": "Флаг активности клиента в онлайн-банкинге за 1 месяц.",
    "auto_pay_active_qty": "Количество активных шаблонов Автоплатеж за месяц.",
    "cl_income_1m_amt": "Сумма дохода по клиенту за отчетный месяц по дебетовым картам.",
    "dep_acc_1st_open_dt": "Дата открытия первого депозитного договора клиента.",
    "wdr_cash_6m_amt": "Сумма снятия наличных в рублях за последние 6 месяцев.",
    "cash_op_6m_amt": "Объем операций в рублях за последние 6 месяцев.",
    "cash_3m_qty": "Количество операций за последние 3 месяца.",
    "lst_balance_amt": "Баланс последней действующей дебетовой карты.",
    "card_active_1m_nflag": "Флаг активности по дебетовым картам за 1 месяц.",
    "client_income_amt": "Доход клиента по данным от рисков.",
    "oi_total_amt": "Сумма общего дохода по клиенту за отчетный месяц по всем кредитам.",
    "act_pl_os_rub_amt": "Совокупная задолженность по кредитам наличными.",
    "payroll_client_nflag": "Флаг зарплатного клиента.",
    "inf_payroll_rub_amt": "Сумма зарплатных начислений по всем счетам клиента за месяц в рублях.",
    "legal_entity_amt": "Сумма поступлений от юридических лиц.",
    "otf_loan_rub_amt": "Общая сумма списания в погашение кредита по всем картам клиента за месяц в рублях.",
    "otf_fee_rub_amt": "Общая сумма списания комиссий по всем картам клиента за месяц в рублях.",
    "inf_transfer_rub_amt": "Общая сумма поступлений переводом с карты или счета за месяц в рублях.",
    "cc_ever_nflag": "Флаг наличия кредитной карты когда-либо.",
    "srv_mb_nflag": "Флаг, что у клиента подключен мобильный банк.",
    "cc_stoplist": "Флаг, что клиенту нельзя предлагать кредит.",
    "lne_tot_debt_int_ovrd_rub_amt": "Сумма просрочки по процентам по кредитам.",
    "lne_tot_debt_ovrd_rub_amt": "Сумма просрочки по основному долгу по кредитам.",
}

MONTHLY_SOURCES = ["deb_cards_info", "credit_cards_info"]
DAILY_SOURCE = "client_cards_daily"


DIM_SOURCES_SCHEMA = StructType([
    StructField("source_id", IntegerType(), False),
    StructField("source_name", StringType(), False),
    StructField("source_description", StringType(), True),
    StructField("update_frequency", StringType(), True),
    StructField("row_create_dtime", TimestampType(), True),
    StructField("valid_to", StringType(), True),
    StructField("valid_from", StringType(), True),
    StructField("row_update_dtime", TimestampType(), True),
])

DIM_ATTRIBUTES_SCHEMA = StructType([
    StructField("attribute_id", IntegerType(), False),
    StructField("attribute_name", StringType(), False),
    StructField("attribute_description", StringType(), True),
    StructField("data_type", StringType(), True),
    StructField("source_id", IntegerType(), False),
    StructField("update_frequency", StringType(), True),
    StructField("row_create_dtime", TimestampType(), True),
    StructField("row_update_dtime", TimestampType(), True),
])

LOAD_LOG_SCHEMA = StructType([
    StructField("load_id", LongType(), False),
    StructField("source_id", IntegerType(), False),
    StructField("source_report_dt", StringType(), False),
    StructField("load_start_dtime", TimestampType(), True),
    StructField("load_end_dtime", TimestampType(), True),
    StructField("target_table", StringType(), False),
    StructField("load_status", StringType(), False),
    StructField("loading_id", LongType(), True),
    StructField("error_message", StringType(), True),
])

CLIENT_MONTHLY_SCHEMA = StructType([
    StructField("client_id", StringType(), False),
    StructField("attribute_id", IntegerType(), False),
    StructField("report_dt", StringType(), False),
    StructField("attribute_value", StringType(), True),
    StructField("source_id", IntegerType(), False),
    StructField("row_update_dtime", TimestampType(), True),
    StructField("loading_id", LongType(), True),
    StructField("row_hash_val", StringType(), True),
])

CLIENT_DAILY_SCHEMA = StructType([
    StructField("client_id", StringType(), False),
    StructField("attribute_id", IntegerType(), False),
    StructField("attribute_value", StringType(), True),
    StructField("row_actual_from", StringType(), False),
    StructField("row_actual_to", StringType(), False),
    StructField("source_id", IntegerType(), False),
    StructField("row_update_dtime", TimestampType(), True),
    StructField("loading_id", LongType(), True),
    StructField("row_hash_val", StringType(), True),
])

TABLE_SCHEMAS = {
    "dim_sources": DIM_SOURCES_SCHEMA,
    "dim_attributes": DIM_ATTRIBUTES_SCHEMA,
    "load_log": LOAD_LOG_SCHEMA,
    "client_monthly_attrs_scd1": CLIENT_MONTHLY_SCHEMA,
    "client_daily_attrs_scd2": CLIENT_DAILY_SCHEMA,
}

TABLE_NAMES = list(TABLE_SCHEMAS.keys())


def table_path(table_name):
    return Path(WAREHOUSE_DIR) / table_name


def read_table_if_exists(table_name):
    """Читает таблицу хранилища или возвращает пустой DataFrame с нужной схемой."""
    path = table_path(table_name)
    if path.exists():
        return spark.read.parquet(str(path))
    return spark.createDataFrame([], TABLE_SCHEMAS[table_name])


def write_table(df, table_name):
    """Перезаписывает одну parquet-таблицу через временную директорию."""
    target = table_path(table_name)
    temp = Path(WAREHOUSE_DIR) / f"__tmp_{table_name}"
    Path(WAREHOUSE_DIR).mkdir(parents=True, exist_ok=True)

    if temp.exists():
        shutil.rmtree(temp)

    df.write.mode("overwrite").parquet(str(temp))

    if target.exists():
        shutil.rmtree(target)
    shutil.move(str(temp), str(target))


def normalize_source_columns(df):
    """Приводит реальные имена полей parquet к именам из задания."""
    for old_name, new_name in COLUMN_ALIASES.items():
        if old_name in df.columns and new_name not in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    if "client_id" in df.columns:
        df = df.withColumn("client_id", F.col("client_id").cast("string"))
    if "loading_id" in df.columns:
        df = df.withColumn("loading_id", F.col("loading_id").cast("long"))
    if "report_dt" in df.columns:
        df = df.withColumn("report_dt", F.col("report_dt").cast("string"))
    if "row_actual_from" in df.columns:
        df = df.withColumn("row_actual_from", F.col("row_actual_from").cast("string"))
    if "row_actual_to" in df.columns:
        df = df.withColumn("row_actual_to", F.col("row_actual_to").cast("string"))

    return df


def read_source_partition(source_name, partition_col, partition_value):
    """Читает parquet из директории нужной партиции целиком."""
    source_root = Path(DATA_DIR) / source_name
    candidates = [
        source_root / f"{partition_col}='{partition_value}'",
        source_root / f"{partition_col}={partition_value}",
    ]

    partition_path = None
    for candidate in candidates:
        if candidate.exists():
            partition_path = candidate
            break

    if partition_path is None:
        raise FileNotFoundError(
            f"Не найдена партиция {source_name}/{partition_col}={partition_value}. "
            f"Проверенные варианты: {candidates}"
        )

    df = spark.read.parquet(str(partition_path))
    df = normalize_source_columns(df)

    if partition_col not in df.columns:
        df = df.withColumn(partition_col, F.lit(partition_value))
    else:
        df = df.withColumn(partition_col, F.col(partition_col).cast("string"))

    return df


def build_dim_sources():
    now = datetime.now()
    rows = []
    for source_name in ["deb_cards_info", "credit_cards_info", "client_cards_daily"]:
        cfg = SOURCE_CONFIG[source_name]
        rows.append((
            cfg["source_id"],
            cfg["source_name"],
            cfg["source_description"],
            cfg["update_frequency"],
            now,
            "9999-12-31",
            "1900-01-01",
            now,
        ))
    return spark.createDataFrame(rows, DIM_SOURCES_SCHEMA)


def build_dim_attributes():
    now = datetime.now()
    rows = []
    attribute_id = 1

    for source_name in ["deb_cards_info", "credit_cards_info", "client_cards_daily"]:
        cfg = SOURCE_CONFIG[source_name]
        sample_df = read_source_partition(
            source_name,
            cfg["partition_col"],
            cfg["initial_partition"],
        )
        schema_by_name = {field.name: field.dataType.simpleString() for field in sample_df.schema.fields}

        for column_name in cfg["business_columns"]:
            if column_name == "client_id" or column_name in TECHNICAL_COLUMNS:
                continue

            rows.append((
                attribute_id,
                column_name,
                ATTRIBUTE_DESCRIPTIONS.get(column_name, f"Бизнес-атрибут {column_name}"),
                schema_by_name.get(column_name, "string"),
                cfg["source_id"],
                cfg["update_frequency"],
                now,
                now,
            ))
            attribute_id += 1

    return spark.createDataFrame(rows, DIM_ATTRIBUTES_SCHEMA)


def already_loaded(load_log_df, source_id, source_report_dt, target_table):
    return (
        load_log_df
        .filter(
            (F.col("source_id") == int(source_id))
            & (F.col("source_report_dt") == str(source_report_dt))
            & (F.col("target_table") == target_table)
            & (F.col("load_status") == "SUCCESS")
        )
        .limit(1)
        .count()
        > 0
    )


def _next_load_id(load_log_df):
    max_load_id = load_log_df.agg(F.max("load_id")).collect()[0][0]
    return 1 if max_load_id is None else int(max_load_id) + 1


def _source_loading_id(source_df):
    if "loading_id" not in source_df.columns:
        return None
    value = source_df.agg(F.max("loading_id")).collect()[0][0]
    return None if value is None else int(value)


def add_load_log_record(
    load_log_df,
    source_id,
    source_report_dt,
    target_table,
    load_status="SUCCESS",
    loading_id=None,
    error_message=None,
    load_start_dtime=None,
):
    if load_start_dtime is None:
        load_start_dtime = datetime.now()
    load_end_dtime = datetime.now()

    new_row = [(
        _next_load_id(load_log_df),
        int(source_id),
        str(source_report_dt),
        load_start_dtime,
        load_end_dtime,
        target_table,
        load_status,
        loading_id,
        error_message,
    )]
    new_df = spark.createDataFrame(new_row, LOAD_LOG_SCHEMA)
    return load_log_df.unionByName(new_df)


def verticalize_monthly(source_df, source_name, dim_attributes_df):
    cfg = SOURCE_CONFIG[source_name]
    source_id = cfg["source_id"]
    attribute_columns = [c for c in cfg["business_columns"] if c != "client_id"]
    missing_columns = [c for c in attribute_columns if c not in source_df.columns]
    if missing_columns:
        raise ValueError(f"В источнике {source_name} нет колонок: {missing_columns}")

    stack_args = ", ".join([f"'{c}', CAST(`{c}` AS STRING)" for c in attribute_columns])
    stack_expr = f"stack({len(attribute_columns)}, {stack_args}) AS (attribute_name, attribute_value)"

    vertical_df = (
        source_df
        .select(
            F.col("client_id").cast("string"),
            F.col("report_dt").cast("string"),
            F.expr(stack_expr),
            F.lit(source_id).cast("int").alias("source_id"),
            F.col("row_update_dtime"),
            F.col("loading_id").cast("long"),
            F.col("row_hash_val"),
        )
        .join(
            dim_attributes_df.select("attribute_id", "attribute_name", "source_id"),
            on=["attribute_name", "source_id"],
            how="left",
        )
        .select(
            "client_id",
            F.col("attribute_id").cast("int"),
            "report_dt",
            "attribute_value",
            "source_id",
            "row_update_dtime",
            "loading_id",
            "row_hash_val",
        )
    )
    return vertical_df


def verticalize_daily(source_df, source_name, dim_attributes_df):
    cfg = SOURCE_CONFIG[source_name]
    source_id = cfg["source_id"]
    attribute_columns = [c for c in cfg["business_columns"] if c != "client_id"]
    missing_columns = [c for c in attribute_columns if c not in source_df.columns]
    if missing_columns:
        raise ValueError(f"В источнике {source_name} нет колонок: {missing_columns}")

    stack_args = ", ".join([f"'{c}', CAST(`{c}` AS STRING)" for c in attribute_columns])
    stack_expr = f"stack({len(attribute_columns)}, {stack_args}) AS (attribute_name, attribute_value)"

    vertical_df = (
        source_df
        .select(
            F.col("client_id").cast("string"),
            F.expr(stack_expr),
            F.col("row_actual_from").cast("string"),
            F.col("row_actual_to").cast("string"),
            F.lit(source_id).cast("int").alias("source_id"),
            F.col("row_update_dtime"),
            F.col("loading_id").cast("long"),
            F.col("row_hash_val"),
        )
        .join(
            dim_attributes_df.select("attribute_id", "attribute_name", "source_id"),
            on=["attribute_name", "source_id"],
            how="left",
        )
        .select(
            "client_id",
            F.col("attribute_id").cast("int"),
            "attribute_value",
            "row_actual_from",
            "row_actual_to",
            "source_id",
            "row_update_dtime",
            "loading_id",
            "row_hash_val",
        )
    )
    return vertical_df


def merge_scd1(old_df, new_df, business_key_columns=None):
    """
    SCD1 merge:
    - если ключ есть в old_df и new_df, берем строку из new_df;
    - если ключ есть только в old_df, оставляем old_df;
    - если ключ есть только в new_df, добавляем new_df.
    """
    if business_key_columns is None:
        business_key_columns = ["client_id", "attribute_id"]

    def assert_unique_keys(df, df_name):
        duplicate_keys = df.groupBy(*business_key_columns).count().filter(F.col("count") > 1)
        examples = duplicate_keys.limit(5).collect()
        if examples:
            formatted_examples = [
                {column: row[column] for column in business_key_columns} | {"count": row["count"]}
                for row in examples
            ]
            raise ValueError(
                f"{df_name} contains duplicate SCD1 business keys "
                f"{business_key_columns}: {formatted_examples}"
            )

    missing_old_columns = set(new_df.columns) - set(old_df.columns)
    missing_new_columns = set(old_df.columns) - set(new_df.columns)
    if missing_old_columns or missing_new_columns:
        raise ValueError(
            "old_df and new_df must have the same columns. "
            f"Only in new_df: {sorted(missing_old_columns)}. "
            f"Only in old_df: {sorted(missing_new_columns)}."
        )

    assert_unique_keys(old_df, "old_df")
    assert_unique_keys(new_df, "new_df")

    updated_keys_df = new_df.select(*business_key_columns).distinct()
    unchanged_old_df = old_df.join(updated_keys_df, on=business_key_columns, how="left_anti")

    return unchanged_old_df.unionByName(new_df.select(old_df.columns)).select(old_df.columns)


def merge_scd2(
    old_df,
    new_df,
    business_key_columns=None,
    valid_from_col="row_actual_from",
    valid_to_col="row_actual_to",
    current_valid_to_value="9999-12-31",
    compare_columns=None,
):
    """
    SCD2 merge с полуоткрытым интервалом [row_actual_from, row_actual_to):
    - если бизнес-ключа нет в old_df, вставляем новую версию;
    - если текущая версия есть и атрибут изменился, закрываем старую версию и вставляем новую;
    - если текущая версия есть и атрибут не изменился, оставляем old_df без изменений;
    - исторические строки old_df не меняем.
    """
    if business_key_columns is None:
        business_key_columns = ["client_id", "attribute_id"]
    business_key_columns = list(business_key_columns)

    if compare_columns is None:
        compare_columns = ["attribute_value"]
    else:
        compare_columns = list(compare_columns)

    old_columns = old_df.columns
    old_types = {field.name: field.dataType for field in old_df.schema.fields}

    required_old_columns = set(business_key_columns + [valid_from_col, valid_to_col])
    missing_old_columns = required_old_columns - set(old_df.columns)
    if missing_old_columns:
        raise ValueError(f"old_df does not contain required columns: {sorted(missing_old_columns)}")

    required_new_columns = set(business_key_columns + [valid_from_col] + compare_columns)
    missing_new_columns = required_new_columns - set(new_df.columns)
    if missing_new_columns:
        raise ValueError(f"new_df does not contain required columns: {sorted(missing_new_columns)}")

    def assert_unique_keys(df, key_columns, df_name):
        duplicate_keys = df.groupBy(*key_columns).count().filter(F.col("count") > 1).limit(5).collect()
        if duplicate_keys:
            formatted_examples = [
                {column: row[column] for column in key_columns} | {"count": row["count"]}
                for row in duplicate_keys
            ]
            raise ValueError(f"{df_name} contains duplicate keys {list(key_columns)}: {formatted_examples}")

    assert_unique_keys(old_df, business_key_columns + [valid_from_col], "old_df")
    assert_unique_keys(new_df, business_key_columns, "new_df")

    new_prepared_df = new_df
    if valid_to_col not in new_prepared_df.columns:
        new_prepared_df = new_prepared_df.withColumn(
            valid_to_col,
            F.lit(current_valid_to_value).cast(old_types[valid_to_col]),
        )

    missing_columns_after_prepare = set(old_columns) - set(new_prepared_df.columns)
    if missing_columns_after_prepare:
        raise ValueError(
            "new_df cannot be aligned to old_df, missing columns: "
            f"{sorted(missing_columns_after_prepare)}"
        )

    new_prepared_df = new_prepared_df.select(
        *[F.col(column).cast(old_types[column]).alias(column) for column in old_columns]
    )

    def make_hash(columns):
        if not columns:
            return F.lit("__no_compare_columns__")
        return F.sha2(F.to_json(F.struct(*[F.col(column).alias(column) for column in columns])), 256)

    current_condition = F.col(valid_to_col).isNull() | (F.col(valid_to_col) == current_valid_to_value)
    old_current_df = old_df.filter(current_condition)
    old_history_df = old_df.filter(~current_condition)

    assert_unique_keys(old_current_df, business_key_columns, "old_df current records")

    old_current_with_hash_df = (
        old_current_df
        .withColumn("__old_hash", make_hash(compare_columns))
        .withColumn("__old_exists", F.lit(True))
    )
    new_with_hash_df = (
        new_prepared_df
        .withColumn("__new_hash", make_hash(compare_columns))
        .withColumn("__new_exists", F.lit(True))
    )

    joined_df = old_current_with_hash_df.join(new_with_hash_df, on=business_key_columns, how="full_outer")

    changed_keys_df = (
        joined_df
        .filter(
            F.col("__old_exists").isNotNull()
            & F.col("__new_exists").isNotNull()
            & (F.col("__old_hash") != F.col("__new_hash"))
        )
        .select(*business_key_columns)
        .distinct()
    )

    new_only_keys_df = (
        joined_df
        .filter(F.col("__old_exists").isNull() & F.col("__new_exists").isNotNull())
        .select(*business_key_columns)
        .distinct()
    )
    insert_keys_df = changed_keys_df.unionByName(new_only_keys_df).distinct()

    changed_new_from_df = (
        new_prepared_df
        .join(changed_keys_df, on=business_key_columns, how="inner")
        .select(*business_key_columns, F.col(valid_from_col).alias("__new_valid_from"))
    )

    expired_old_current_df = (
        old_current_df
        .join(changed_new_from_df, on=business_key_columns, how="inner")
        .withColumn(valid_to_col, F.col("__new_valid_from").cast(old_types[valid_to_col]))
        .drop("__new_valid_from")
    )

    unchanged_old_current_df = old_current_df.join(changed_keys_df, on=business_key_columns, how="left_anti")
    new_versions_df = new_prepared_df.join(insert_keys_df, on=business_key_columns, how="inner")

    result_df = (
        old_history_df.select(*old_columns)
        .unionByName(unchanged_old_current_df.select(*old_columns))
        .unionByName(expired_old_current_df.select(*old_columns))
        .unionByName(new_versions_df.select(*old_columns))
    )

    return result_df.select(*old_columns)

def check_duplicates(df, key_columns, table_name):
    duplicates = df.groupBy(*key_columns).count().filter(F.col("count") > 1)
    duplicate_count = duplicates.count()
    print(f"Дубли в {table_name} по ключу {key_columns}: {duplicate_count}")
    duplicates.show(truncate=False)
    return duplicates


def show_table_info(df, table_name, n=10):
    print(f"\nТаблица: {table_name}")
    print(f"Количество строк: {df.count()}")
    df.show(n, truncate=False)


def create_warehouse():
    """Создает пять parquet-таблиц хранилища в директории warehouse/."""
    Path(WAREHOUSE_DIR).mkdir(parents=True, exist_ok=True)

    dim_sources_df = build_dim_sources()
    dim_attributes_df = build_dim_attributes()
    load_log_df = spark.createDataFrame([], LOAD_LOG_SCHEMA)
    monthly_df = spark.createDataFrame([], CLIENT_MONTHLY_SCHEMA)
    daily_df = spark.createDataFrame([], CLIENT_DAILY_SCHEMA)

    write_table(dim_sources_df, "dim_sources")
    write_table(dim_attributes_df, "dim_attributes")
    write_table(load_log_df, "load_log")
    write_table(monthly_df, "client_monthly_attrs_scd1")
    write_table(daily_df, "client_daily_attrs_scd2")

    show_table_info(dim_sources_df, "dim_sources")
    show_table_info(dim_attributes_df, "dim_attributes")
    print("Пустое хранилище создано.")


def initial_load_warehouse():
    """Выполняет первую загрузку начальных партиций в хранилище."""
    print("\n=== Первая загрузка хранилища ===")

    dim_attributes_df = read_table_if_exists("dim_attributes")
    load_log_df = read_table_if_exists("load_log")
    monthly_df = read_table_if_exists("client_monthly_attrs_scd1")
    daily_df = read_table_if_exists("client_daily_attrs_scd2")

    for source_name in MONTHLY_SOURCES:
        cfg = SOURCE_CONFIG[source_name]
        source_report_dt = cfg["initial_partition"]
        target_table = cfg["target_table"]

        if already_loaded(load_log_df, cfg["source_id"], source_report_dt, target_table):
            print(f"{source_name} за {source_report_dt} уже загружен. Пропускаю.")
            continue

        load_start = datetime.now()
        source_df = read_source_partition(source_name, cfg["partition_col"], source_report_dt)

        print(f"\nИсточник {source_name}, партиция {cfg['partition_col']}={source_report_dt}")
        source_df.printSchema()
        source_df.show(5, truncate=False)

        new_rows_df = verticalize_monthly(source_df, source_name, dim_attributes_df)
        print(f"Вертикализованные строки для {source_name}")
        new_rows_df.show(10, truncate=False)

        monthly_df = merge_scd1(monthly_df, new_rows_df)
        load_log_df = add_load_log_record(
            load_log_df,
            source_id=cfg["source_id"],
            source_report_dt=source_report_dt,
            target_table=target_table,
            loading_id=_source_loading_id(source_df),
            load_start_dtime=load_start,
        )

    cfg = SOURCE_CONFIG[DAILY_SOURCE]
    source_report_dt = cfg["initial_partition"]
    target_table = cfg["target_table"]

    if already_loaded(load_log_df, cfg["source_id"], source_report_dt, target_table):
        print(f"{DAILY_SOURCE} за {source_report_dt} уже загружен. Пропускаю.")
    else:
        load_start = datetime.now()
        source_df = read_source_partition(DAILY_SOURCE, cfg["partition_col"], source_report_dt)

        print(f"\nИсточник {DAILY_SOURCE}, партиция {cfg['partition_col']}={source_report_dt}")
        source_df.printSchema()
        source_df.show(5, truncate=False)

        new_rows_df = verticalize_daily(source_df, DAILY_SOURCE, dim_attributes_df)
        print(f"Вертикализованные строки для {DAILY_SOURCE}")
        new_rows_df.show(10, truncate=False)

        daily_df = merge_scd2(daily_df, new_rows_df)
        load_log_df = add_load_log_record(
            load_log_df,
            source_id=cfg["source_id"],
            source_report_dt=source_report_dt,
            target_table=target_table,
            loading_id=_source_loading_id(source_df),
            load_start_dtime=load_start,
        )

    write_table(monthly_df, "client_monthly_attrs_scd1")
    write_table(daily_df, "client_daily_attrs_scd2")
    write_table(load_log_df, "load_log")

    monthly_df = read_table_if_exists("client_monthly_attrs_scd1")
    daily_df = read_table_if_exists("client_daily_attrs_scd2")
    load_log_df = read_table_if_exists("load_log")

    show_table_info(monthly_df, "client_monthly_attrs_scd1")
    show_table_info(daily_df, "client_daily_attrs_scd2")
    show_table_info(load_log_df, "load_log")


def update_warehouse():
    """Выполняет вторую загрузку и не загружает уже обработанные партиции повторно."""
    print("\n=== Вторая загрузка хранилища ===")

    dim_attributes_df = read_table_if_exists("dim_attributes")
    load_log_df = read_table_if_exists("load_log")
    monthly_df = read_table_if_exists("client_monthly_attrs_scd1")
    daily_df = read_table_if_exists("client_daily_attrs_scd2")

    for source_name in MONTHLY_SOURCES:
        cfg = SOURCE_CONFIG[source_name]
        source_report_dt = cfg["update_partition"]
        target_table = cfg["target_table"]

        if already_loaded(load_log_df, cfg["source_id"], source_report_dt, target_table):
            print(f"{source_name} за {source_report_dt} уже загружен. Пропускаю.")
            continue

        load_start = datetime.now()
        source_df = read_source_partition(source_name, cfg["partition_col"], source_report_dt)

        print(f"\nИсточник {source_name}, партиция {cfg['partition_col']}={source_report_dt}")
        source_df.printSchema()
        source_df.show(5, truncate=False)

        new_rows_df = verticalize_monthly(source_df, source_name, dim_attributes_df)
        print(f"Вертикализованные строки для {source_name}")
        new_rows_df.show(10, truncate=False)

        monthly_df = merge_scd1(monthly_df, new_rows_df)
        load_log_df = add_load_log_record(
            load_log_df,
            source_id=cfg["source_id"],
            source_report_dt=source_report_dt,
            target_table=target_table,
            loading_id=_source_loading_id(source_df),
            load_start_dtime=load_start,
        )

    cfg = SOURCE_CONFIG[DAILY_SOURCE]
    source_report_dt = cfg["update_partition"]
    target_table = cfg["target_table"]

    if already_loaded(load_log_df, cfg["source_id"], source_report_dt, target_table):
        print(f"{DAILY_SOURCE} за {source_report_dt} уже загружен. Пропускаю.")
    else:
        load_start = datetime.now()
        source_df = read_source_partition(DAILY_SOURCE, cfg["partition_col"], source_report_dt)

        print(f"\nИсточник {DAILY_SOURCE}, партиция {cfg['partition_col']}={source_report_dt}")
        source_df.printSchema()
        source_df.show(5, truncate=False)

        new_rows_df = verticalize_daily(source_df, DAILY_SOURCE, dim_attributes_df)
        print(f"Вертикализованные строки для {DAILY_SOURCE}")
        new_rows_df.show(10, truncate=False)

        daily_df = merge_scd2(daily_df, new_rows_df)
        load_log_df = add_load_log_record(
            load_log_df,
            source_id=cfg["source_id"],
            source_report_dt=source_report_dt,
            target_table=target_table,
            loading_id=_source_loading_id(source_df),
            load_start_dtime=load_start,
        )

    write_table(monthly_df, "client_monthly_attrs_scd1")
    write_table(daily_df, "client_daily_attrs_scd2")
    write_table(load_log_df, "load_log")

    monthly_df = read_table_if_exists("client_monthly_attrs_scd1")
    daily_df = read_table_if_exists("client_daily_attrs_scd2")
    load_log_df = read_table_if_exists("load_log")

    show_table_info(monthly_df, "client_monthly_attrs_scd1")
    show_table_info(daily_df, "client_daily_attrs_scd2")
    show_table_info(load_log_df, "load_log")


def run_simple_checks():
    """Показывает простые проверки результата лабораторной."""
    print("\n=== Проверки результата ===")

    dim_sources_df = read_table_if_exists("dim_sources")
    dim_attributes_df = read_table_if_exists("dim_attributes")
    load_log_df = read_table_if_exists("load_log")
    monthly_df = read_table_if_exists("client_monthly_attrs_scd1")
    daily_df = read_table_if_exists("client_daily_attrs_scd2")

    for table_name, df in [
        ("dim_sources", dim_sources_df),
        ("dim_attributes", dim_attributes_df),
        ("load_log", load_log_df),
        ("client_monthly_attrs_scd1", monthly_df),
        ("client_daily_attrs_scd2", daily_df),
    ]:
        show_table_info(df, table_name)

    print("\nПроверка, что все используемые source_id есть в dim_sources")
    source_ids_from_facts = (
        dim_attributes_df.select("source_id")
        .unionByName(load_log_df.select("source_id"))
        .unionByName(monthly_df.select("source_id"))
        .unionByName(daily_df.select("source_id"))
        .distinct()
    )
    missing_source_ids = source_ids_from_facts.join(
        dim_sources_df.select("source_id").distinct(),
        on="source_id",
        how="left_anti",
    )
    missing_source_ids.show(truncate=False)

    print("\nПроверка null в attribute_id после вертикализации")
    monthly_df.filter(F.col("attribute_id").isNull()).show(truncate=False)
    daily_df.filter(F.col("attribute_id").isNull()).show(truncate=False)

    check_duplicates(
        monthly_df,
        ["client_id", "attribute_id", "report_dt"],
        "client_monthly_attrs_scd1",
    )
    check_duplicates(
        daily_df,
        ["client_id", "attribute_id", "row_actual_from"],
        "client_daily_attrs_scd2",
    )

    print("\nЖурнал загрузок")
    load_log_df.orderBy("load_id").show(truncate=False)


# Полный линейный запуск лабораторной работы.
create_warehouse()
initial_load_warehouse()
update_warehouse()
run_simple_checks()
