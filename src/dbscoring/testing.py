"""Deterministic fixtures based on the real source schemas."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import polars as pl


def _write_partition(
    root: Path, source: str, partition: str, frame: pl.DataFrame
) -> None:
    path = root / source / partition
    path.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path / "part-00000.parquet")


def create_source_fixtures(root: Path) -> None:
    """Create compact source parquet fixtures with the real source columns."""

    clients = ["1001", "1002", "1003", "1004"]
    for report_dt, load_id, multiplier in [
        ("2023-02-28", 10, 1),
        ("2023-03-31", 20, 2),
    ]:
        credit = pl.DataFrame(
            {
                "id": clients,
                "client_income_amt": [
                    Decimal(1000 * multiplier + i * 100) for i in range(4)
                ],
                "oi_total_amt": [Decimal(100 * multiplier + i * 10) for i in range(4)],
                "act_pl_os_rub_amt": [Decimal(50 * multiplier + i) for i in range(4)],
                "payroll_client_nflag": [1, 0, 1, 0],
                "inf_payroll_rub_amt": [Decimal(20 * multiplier + i) for i in range(4)],
                "legal_entity_amt": [Decimal(i) for i in range(4)],
                "inc_avg_risk_rub_amt": [
                    Decimal(30 * multiplier + i) for i in range(4)
                ],
                "otf_loan_rub_amt": [Decimal(40 * multiplier + i) for i in range(4)],
                "otf_fee_rub_amt": [Decimal(5 * multiplier + i) for i in range(4)],
                "inf_transfer_rub_amt": [Decimal(7 * multiplier + i) for i in range(4)],
                "cc_ever_nflag": [0, 1, 0, 1],
                "row_update_dtime": [datetime(2023, 4, multiplier, 12, 0, 0)] * 4,
                "loading_id": [load_id] * 4,
                "row_hash_val": [f"credit-{report_dt}-{client}" for client in clients],
                "report_dt": [report_dt] * 4,
            }
        )
        debit = pl.DataFrame(
            {
                "id": clients,
                "onl_bank_active_1m_nfalg": [1, 1, 0, 0],
                "auto_pay_active_qty": [0, 1, 2, 3],
                "cl_income_1m_amt": [Decimal(500 * multiplier + i) for i in range(4)],
                "dep_acc_1st_open_dt": [datetime(2020, 1, 1 + i) for i in range(4)],
                "wdr_cash_6m_amt": [Decimal(11 * multiplier + i) for i in range(4)],
                "cash_op_6m_amt": [Decimal(12 * multiplier + i) for i in range(4)],
                "cash_3m_qty": [1, 2, 3, 4],
                "lst_balance_amt": [Decimal(100 * multiplier + i) for i in range(4)],
                "card_active_1m_nflag": [1, 0, 1, 0],
                "row_update_dtime": [datetime(2023, 4, multiplier, 13, 0, 0)] * 4,
                "loading_id": [load_id + 1] * 4,
                "row_hash_val": [f"debit-{report_dt}-{client}" for client in clients],
                "report_dt": [report_dt] * 4,
            }
        )
        _write_partition(root, "credit_cards_info", f"report_dt='{report_dt}'", credit)
        _write_partition(root, "deb_cards_info", f"report_dt='{report_dt}'", debit)

    for row_actual_to, load_id, offset in [
        ("2023-04-03", 30, 0),
        ("9999-12-31", 40, 1),
    ]:
        daily = pl.DataFrame(
            {
                "id": clients,
                "srv_mb_nflag": [1, 0, 1, 0],
                "cc_stoplist": [0, 0, 1, 0],
                "lne_tot_debt_int_ovrd_rub_amt": [
                    Decimal(3 + offset + i) for i in range(4)
                ],
                "lne_tot_debt_ovrd_rub_amt": [
                    Decimal(10 + offset + i) for i in range(4)
                ],
                "row_update_dtime": [datetime(2023, 4, 3 + offset, 14, 0, 0)] * 4,
                "loading_id": [load_id] * 4,
                "row_hash_val": [
                    f"daily-{row_actual_to}-{client}" for client in clients
                ],
                "row_actual_from": ["2023-04-01" if offset == 0 else "2023-04-04"] * 4,
                "row_actual_to": [row_actual_to] * 4,
            }
        )
        _write_partition(
            root, "client_cards_daily", f"row_actual_to='{row_actual_to}'", daily
        )
