# %% [markdown]
# # Секция 1. Назначение работы
#
# В этой лабораторной работе мы строим учебное хранилище данных для задачи кредитного скоринга. Важно: здесь не строится модель машинного обучения и не выполняется подготовка признаков для ML. Мы делаем только первые три пункта задания: описываем физическую модель, создаем хранилище и показываем, как Spark может обновлять распределенные parquet-данные.
#
# Решение специально написано линейно: без классов, без пакетной архитектуры, без командной строки и без оркестраторов. Такой код проще объяснить на защите: каждая функция делает один понятный шаг.

# %% [markdown]
# # Секция 2. Инициализация Spark и путей
#
# Spark – это движок для обработки больших данных. В ноутбуке мы запускаем локальную Spark-сессию, то есть Spark будет работать на одной машине, но код будет похож на код для распределенной обработки.
#
# `DATA_DIR` – папка с исходными parquet-источниками. Функция `find_data_dir()` ищет ее в нескольких типичных местах. Если рядом есть `source.zip`, функция распакует архив и найдет внутри директорию с тремя источниками.
#
# `WAREHOUSE_DIR` – отдельная папка, куда будут записаны таблицы учебного хранилища. Старые исходные файлы при этом не изменяются.

# %%
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


# %% [markdown]
# # Секция 3. Описание исходных источников
#
# В задании есть три источника.
#
# `deb_cards_info` и `credit_cards_info` обновляются раз в месяц и хранятся как SCD1. В SCD1 нам важно текущее значение атрибута за ключ, а при повторной загрузке дубль не должен появиться.
#
# `client_cards_daily` обновляется раз в день и хранится как SCD2. В SCD2 история хранится через период действия строки: `row_actual_from` и `row_actual_to`. В этой учебной работе мы не усложняем SCD2, потому что эти поля уже есть в источнике. Мы просто переносим их в вертикальную таблицу.
#
# Также здесь задан список бизнес-атрибутов. Технические поля вроде `row_update_dtime`, `loading_id`, `row_hash_val`, `report_dt`, `row_actual_from`, `row_actual_to` не попадают в `dim_attributes` как бизнес-атрибуты.

# %%
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


# %% [markdown]
# # Секция 4. Создание схем 5 таблиц хранилища
#
# Физическая модель состоит из пяти таблиц.
#
# `dim_sources` – справочник источников. Поля `valid_from` и `valid_to` позволяют хранить период действия описания источника.
#
# `dim_attributes` – справочник бизнес-атрибутов. Здесь каждому атрибуту назначается простой последовательный `attribute_id`.
#
# `load_log` – журнал загрузок. Он нужен, чтобы понимать, какие партиции уже успешно загружались.
#
# `client_monthly_attrs_scd1` – вертикальная таблица месячных атрибутов. Ключ: `client_id + attribute_id + report_dt`.
#
# `client_daily_attrs_scd2` – вертикальная таблица дневных атрибутов. Ключ: `client_id + attribute_id + row_actual_from`.
#
# `client_id` остается отдельным ключевым полем и не превращается в `attribute_id`.

# %%
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


# %% [markdown]
# # Секция 5. Вспомогательные функции
#
# Здесь собраны простые функции верхнего уровня. Они нужны, чтобы основной сценарий читался как последовательность учебных шагов: создать хранилище, выполнить первую загрузку, выполнить обновление и проверить результат.
#
# Что такое вертикализация: исходные таблицы имеют широкий вид, где много бизнес-колонок лежат в одной строке клиента. Например: `client_id, income, balance`. После вертикализации каждая бизнес-колонка становится отдельной строкой: `client_id, attribute_id, attribute_value`. Это удобно, когда список атрибутов нужно хранить единообразно.
#
# Почему `attribute_value` хранится как строка: в одной вертикальной колонке оказываются значения разных типов – числа, даты, флаги. Поэтому для учебной модели проще привести их к `string`.
#
# Зачем нужен `load_log`: без журнала загрузок мы могли бы повторно загрузить ту же самую партицию и получить дубли. Журнал фиксирует успешные загрузки по связке `source_id + source_report_dt + target_table`.

# %%
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
            F.col("client_id").cast("string").alias("client_id"),
            F.col("report_dt").cast("string").alias("report_dt"),
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
            F.col("attribute_id").cast("int").alias("attribute_id"),
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
            F.col("client_id").cast("string").alias("client_id"),
            F.expr(stack_expr),
            F.col("row_actual_from").cast("string").alias("row_actual_from"),
            F.col("row_actual_to").cast("string").alias("row_actual_to"),
            F.lit(source_id).cast("int").alias("source_id"),
            F.col("row_update_dtime"),
            F.col("loading_id").cast("long").alias("loading_id"),
            F.col("row_hash_val"),
        )
        .join(
            dim_attributes_df.select("attribute_id", "attribute_name", "source_id"),
            on=["attribute_name", "source_id"],
            how="left",
        )
        .select(
            "client_id",
            F.col("attribute_id").cast("int").alias("attribute_id"),
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


def merge_scd1(old_df, new_df):
    key_columns = ["client_id", "attribute_id", "report_dt"]
    window = Window.partitionBy(*key_columns).orderBy(
        F.col("_is_new").desc(),
        F.col("row_update_dtime").desc_nulls_last(),
        F.col("loading_id").desc_nulls_last(),
    )

    merged = (
        old_df.withColumn("_is_new", F.lit(0))
        .unionByName(new_df.withColumn("_is_new", F.lit(1)))
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_is_new")
    )
    return merged.select(old_df.columns)


def merge_scd2(old_df, new_df):
    key_columns = ["client_id", "attribute_id", "row_actual_from"]
    window = Window.partitionBy(*key_columns).orderBy(
        F.col("_is_new").desc(),
        F.col("row_update_dtime").desc_nulls_last(),
        F.col("loading_id").desc_nulls_last(),
    )

    merged = (
        old_df.withColumn("_is_new", F.lit(0))
        .unionByName(new_df.withColumn("_is_new", F.lit(1)))
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_is_new")
    )
    return merged.select(old_df.columns)


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


# %% [markdown]
# # Секция 6. create_warehouse()
#
# Функция создает пустое хранилище. Это первый шаг лабораторной: мы задаем физическую модель и записываем ее в parquet-таблицы.
#
# В `dim_sources` сразу попадают три источника. В `dim_attributes` попадают только бизнес-атрибуты, кроме `client_id`, потому что `client_id` – это ключ клиента, а не измеряемый атрибут.

# %%
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


# %% [markdown]
# # Секция 7. initial_load_warehouse()
#
# Первая загрузка моделирует начальное наполнение хранилища. Несмотря на то что parquet-файлы уже лежат статично в архиве, по смыслу лабораторной мы считаем, что это первая порция данных, пришедшая из источников.
#
# Для месячных источников читаются партиции `report_dt='2023-02-28'`. Для дневного источника читается партиция `row_actual_to='2023-04-03'`. После чтения данные вертикализируются и записываются в целевые таблицы.

# %%
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


# %% [markdown]
# # Секция 8. update_warehouse()
#
# Вторая загрузка моделирует обновление хранилища. Теперь читаются мартовские месячные партиции и актуальная дневная партиция `row_actual_to='9999-12-31'`.
#
# Перед загрузкой каждой партиции функция смотрит в `load_log`. Если такая партиция уже была успешно загружена в эту целевую таблицу, повторная загрузка пропускается. Это простая защита от дублей.

# %%
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


# %% [markdown]
# # Секция 9. Проверки результата
#
# В конце нужны короткие проверки, которые удобно показать преподавателю. Мы смотрим количество строк, показываем содержимое таблиц, проверяем справочник источников, проверяем `attribute_id` после вертикализации и ищем дубли по ключам SCD1 и SCD2.

# %%
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


# %% [markdown]
# ## Запуск всех шагов
#
# Ниже главный сценарий лабораторной. Он специально короткий: все детали спрятаны в простые функции, а порядок действий хорошо читается сверху вниз.

# %%
# Полный линейный запуск лабораторной работы.
create_warehouse()
initial_load_warehouse()
update_warehouse()
run_simple_checks()


# %% [markdown]
# # Секция 10. Краткое объяснение результата
#
# После запуска в папке `warehouse/` появляются пять parquet-таблиц. Это и есть учебное хранилище данных.
#
# `dim_sources` отвечает на вопрос, откуда пришли данные. `dim_attributes` отвечает на вопрос, какие бизнес-атрибуты клиента мы храним. `load_log` отвечает на вопрос, какие партиции уже загружены. `client_monthly_attrs_scd1` хранит месячные атрибуты в вертикальном виде. `client_daily_attrs_scd2` хранит дневные атрибуты в вертикальном виде и сохраняет период действия строк.
#
# Основное допущение: сами исходные parquet-файлы статичны, но в лабораторной мы моделируем процесс обновления так, как будто сначала пришли начальные партиции, а затем пришли новые партиции.
