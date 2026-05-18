"""Построение синтетических parquet fixtures и чтение partitioned-output таблиц для тестов."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import polars as pl

from tests.notebook_loader import SPARK_NOTEBOOK_PATH, load_namespace


SPARK_NOTEBOOK_NAMESPACE = load_namespace(SPARK_NOTEBOOK_PATH)
SOURCE_REGISTRY = SPARK_NOTEBOOK_NAMESPACE["SOURCE_REGISTRY"]


def _build_source_row(source_name: str, record_id: str, loading_id: int, row_hash_val: str, **overrides: Any) -> dict[str, Any]:
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


def write_partition(sources_root: Path, source_name: str, partition_value: str, rows: list[dict[str, Any]]) -> Path:
    partition_key = SOURCE_REGISTRY[source_name]["partition_key"]
    partition_dir = sources_root / source_name / f"{partition_key}='{partition_value}'"
    partition_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(partition_dir / "part-00000.parquet")
    return partition_dir


def build_sample_sources(sources_root: Path) -> None:
    for source_name in SOURCE_REGISTRY:
        (sources_root / source_name).mkdir(parents=True, exist_ok=True)

    write_partition(
        sources_root,
        "credit_cards_info",
        "2024-03-31",
        [
            _build_source_row(
                "credit_cards_info",
                record_id="C100",
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
        sources_root,
        "deb_cards_info",
        "2024-03-31",
        [
            _build_source_row(
                "deb_cards_info",
                record_id="C100",
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
        sources_root,
        "client_cards_daily",
        "2024-03-31",
        [
            _build_source_row(
                "client_cards_daily",
                record_id="C100",
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


def add_second_monthly_partition(sources_root: Path) -> None:
    write_partition(
        sources_root,
        "credit_cards_info",
        "2024-04-30",
        [
            _build_source_row(
                "credit_cards_info",
                record_id="C100",
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


def mutate_existing_partition(sources_root: Path) -> None:
    partition_dir = sources_root / "credit_cards_info" / "report_dt='2024-03-31'"
    pl.DataFrame(
        [
            _build_source_row(
                "credit_cards_info",
                record_id="C100",
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
        ]
    ).write_parquet(partition_dir / "part-00001.parquet")


def read_partitioned_table(table_root: Path) -> pl.DataFrame:
    parquet_files = sorted(table_root.glob("*/*.parquet"))
    if not parquet_files:
        return pl.DataFrame()
    partitioned_frames: list[pl.DataFrame] = []
    for parquet_file in parquet_files:
        partition_name = parquet_file.parent.name
        partition_key, partition_value = partition_name.split("=", 1)
        partition_value = partition_value.strip("'")
        partitioned_frames.append(
            pl.read_parquet(parquet_file).with_columns(pl.lit(partition_value).alias(partition_key))
        )
    return pl.concat(partitioned_frames, how="diagonal_relaxed")
