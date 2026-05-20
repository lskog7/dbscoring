"""Построение синтетических parquet fixtures и чтение partitioned-output таблиц для тестов."""

from __future__ import annotations

import datetime as dt
import shutil
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

from tests.notebook_loader import ROOT_DIR, SPARK_NOTEBOOK_PATH, load_namespace

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


NOTEBOOK_NAMESPACE = load_namespace(SPARK_NOTEBOOK_PATH)
SOURCE_REGISTRY = NOTEBOOK_NAMESPACE["SOURCES"]
REAL_SOURCES_ROOT = ROOT_DIR / "data" / "sources"
BOUNDED_REAL_SOURCES_ROOT = ROOT_DIR / "data" / "test_sources"
TEST_SOURCE_PARTITION_ROW_LIMIT = 20



def _build_source_row(source_name: str, record_id: int, loading_id: int, row_hash_val: str, **overrides: Any) -> dict[str, Any]:
    source_meta = SOURCE_REGISTRY[source_name]
    row = {column_name: None for column_name in source_meta["columns"]}
    row.update(
        {
            "id": record_id,
            "row_update_dtime": overrides.pop("row_update_dtime", dt.datetime(2024, 3, 31, 9, 0, 0)),
            "loading_id": loading_id,
            "row_hash_val": row_hash_val,
        }
    )
    if source_meta["update_frequency"] == "daily":
        row["row_actual_from"] = overrides.pop("row_actual_from", "2024-03-01")
    row.update(overrides)
    return row


def _write_rows_to_parquet(
    spark: SparkSession,
    output_dir: Path,
    rows: list[dict[str, Any]],
    mode: str,
) -> None:
    spark.createDataFrame(rows).coalesce(1).write.mode(mode).parquet(str(output_dir))


def build_limited_real_sources(
    spark: SparkSession,
    sources_root: Path,
    real_sources_root: Path | None = None,
    row_limit: int = TEST_SOURCE_PARTITION_ROW_LIMIT,
) -> dict[tuple[str, str], int]:
    """Build a small parquet source tree from real project data for fast Spark tests."""
    real_sources_root = real_sources_root or (
        BOUNDED_REAL_SOURCES_ROOT if BOUNDED_REAL_SOURCES_ROOT.exists() else REAL_SOURCES_ROOT
    )
    if row_limit < 1 or row_limit > 1000:
        raise ValueError("row_limit must be between 1 and 1000")
    if not real_sources_root.exists():
        raise FileNotFoundError(f"Real source data directory does not exist: {real_sources_root}")

    if sources_root.exists():
        shutil.rmtree(sources_root)
    sources_root.mkdir(parents=True, exist_ok=True)

    row_counts: dict[tuple[str, str], int] = {}
    for source_name, source_meta in SOURCE_REGISTRY.items():
        real_source_dir = real_sources_root / source_name
        if not real_source_dir.exists():
            raise FileNotFoundError(f"Real source directory does not exist: {real_source_dir}")

        target_source_dir = sources_root / source_name
        target_source_dir.mkdir(parents=True, exist_ok=True)
        partition_dirs = sorted(path for path in real_source_dir.iterdir() if path.is_dir() and not path.name.startswith("."))
        if not partition_dirs:
            raise FileNotFoundError(f"Real source directory has no partitions: {real_source_dir}")

        for partition_dir in partition_dirs:
            target_partition_dir = target_source_dir / partition_dir.name
            limited_df = spark.read.parquet(str(partition_dir)).limit(row_limit)
            row_count = limited_df.count()
            if row_count > row_limit:
                raise AssertionError(f"{partition_dir} produced {row_count} rows, limit is {row_limit}")
            limited_df.coalesce(1).write.mode("overwrite").parquet(str(target_partition_dir))
            row_counts[(source_name, partition_dir.name)] = row_count

    return row_counts


def write_partition(
    spark: SparkSession,
    sources_root: Path,
    source_name: str,
    partition_value: str,
    rows: list[dict[str, Any]],
) -> Path:
    partition_key = SOURCE_REGISTRY[source_name]["partition_key"]
    partition_dir = sources_root / source_name / f"{partition_key}='{partition_value}'"
    partition_dir.mkdir(parents=True, exist_ok=True)
    _write_rows_to_parquet(spark, partition_dir, rows, mode="overwrite")
    return partition_dir


def build_sample_sources(spark: SparkSession, sources_root: Path) -> None:
    for source_name in SOURCE_REGISTRY:
        (sources_root / source_name).mkdir(parents=True, exist_ok=True)

    write_partition(
        spark,
        sources_root,
        "credit_cards_info",
        "2024-03-31",
        [
            _build_source_row(
                "credit_cards_info",
                record_id=100,
                loading_id=201,
                row_hash_val="credit-20240331-C100",
                client_income_amt=120000.0,
                oi_total_amt=190000.0,
                act_pl_os_rub_amt=80000.0,
                payroll_client_nflag=1,
                inf_payroll_rub_amt=62000.0,
                legal_entity_amt=24000.0,
                inc_avg_risk_rub_amt=900.0,
                otf_loan_rub_amt=4000.0,
                otf_fee_rub_amt=150.0,
                inf_transfer_rub_amt=1000.0,
                cc_ever_nflag=1,
            )
        ],
    )
    write_partition(
        spark,
        sources_root,
        "deb_cards_info",
        "2024-03-31",
        [
            _build_source_row(
                "deb_cards_info",
                record_id=100,
                loading_id=202,
                row_hash_val="debit-20240331-C100",
                onl_bank_active_1m_nfalg=1,
                auto_pay_active_qty=2,
                cl_income_1m_amt=70000.0,
                dep_acc_1st_open_dt="2021-06-01",
                wdr_cash_6m_amt=12000.0,
                cash_op_6m_amt=14000.0,
                cash_3m_qty=4,
                lst_balance_amt=30000.0,
                card_active_1m_nflag=1,
                row_update_dtime=dt.datetime(2024, 3, 31, 9, 5, 0),
            )
        ],
    )
    write_partition(
        spark,
        sources_root,
        "client_cards_daily",
        "2024-03-31",
        [
            _build_source_row(
                "client_cards_daily",
                record_id=100,
                loading_id=203,
                row_hash_val="daily-20240331-C100",
                srv_mb_nflag=1,
                cc_stoplist=0,
                lne_tot_debt_int_ovrd_rub_amt=0.0,
                lne_tot_debt_ovrd_rub_amt=100.0,
                row_actual_from="2024-03-01",
                row_update_dtime=dt.datetime(2024, 3, 31, 9, 10, 0),
            )
        ],
    )


def add_second_monthly_partition(spark: SparkSession, sources_root: Path) -> None:
    write_partition(
        spark,
        sources_root,
        "credit_cards_info",
        "2024-04-30",
        [
            _build_source_row(
                "credit_cards_info",
                record_id=100,
                loading_id=301,
                row_hash_val="credit-20240430-C100",
                client_income_amt=123000.0,
                oi_total_amt=191000.0,
                act_pl_os_rub_amt=81000.0,
                payroll_client_nflag=1,
                inf_payroll_rub_amt=62100.0,
                legal_entity_amt=24100.0,
                inc_avg_risk_rub_amt=901.0,
                otf_loan_rub_amt=4010.0,
                otf_fee_rub_amt=151.0,
                inf_transfer_rub_amt=1001.0,
                cc_ever_nflag=1,
                row_update_dtime=dt.datetime(2024, 4, 30, 9, 0, 0),
            )
        ],
    )


def mutate_existing_partition(spark: SparkSession, sources_root: Path) -> str:
    credit_source_dir = sources_root / "credit_cards_info"
    partition_dirs = sorted(path for path in credit_source_dir.iterdir() if path.is_dir() and not path.name.startswith("."))
    if not partition_dirs:
        raise FileNotFoundError(f"No credit_cards_info partitions found under {credit_source_dir}")

    partition_dir = partition_dirs[-1]
    partition_value = partition_dir.name.split("'", maxsplit=2)[1]
    _write_rows_to_parquet(
        spark,
        partition_dir,
        [
            _build_source_row(
                "credit_cards_info",
                record_id=100,
                loading_id=999,
                row_hash_val="credit-20240331-C100-mutated",
                client_income_amt=888888.0,
                oi_total_amt=190000.0,
                act_pl_os_rub_amt=80000.0,
                payroll_client_nflag=1,
                inf_payroll_rub_amt=62000.0,
                legal_entity_amt=24000.0,
                inc_avg_risk_rub_amt=900.0,
                otf_loan_rub_amt=4000.0,
                otf_fee_rub_amt=150.0,
                inf_transfer_rub_amt=1000.0,
                cc_ever_nflag=1,
            )
        ],
        mode="append",
    )
    return partition_value
