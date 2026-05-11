"""Canonical contracts for sources, attributes, target tables and load logs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

UpdateFrequency = Literal["daily", "monthly"]
TargetTableName = Literal[
    "dim_sources",
    "dim_attributes",
    "client_monthly_attrs_scd1",
    "client_daily_attrs_scd2",
    "load_log",
]
LoadStatus = Literal["started", "success", "failed", "skipped"]

CANONICAL_CLIENT_ID_TYPE = "STRING"
LOAD_STATUSES: tuple[LoadStatus, ...] = ("started", "success", "failed", "skipped")


@dataclass(frozen=True, slots=True)
class SourceContract:
    """Description of one source dataset and its warehouse target."""

    source_id: int
    source_name: str
    source_description: str
    update_frequency: UpdateFrequency
    business_columns: tuple[str, ...]
    technical_columns: tuple[str, ...]
    partition_column: str
    scd_columns: tuple[str, ...]
    target_table: TargetTableName


@dataclass(frozen=True, slots=True)
class AttributeContract:
    """Description of one normalized business attribute."""

    attribute_id: int
    attribute_name: str
    source_name: str
    source_id: int
    source_column: str
    update_frequency: UpdateFrequency
    target_table: TargetTableName
    data_type: str


@dataclass(frozen=True, slots=True)
class TableContract:
    """Warehouse table contract used by tests, notebooks and validation."""

    table_name: TargetTableName
    primary_key: tuple[str, ...]
    columns: tuple[str, ...]


SOURCE_REGISTRY: tuple[SourceContract, ...] = (
    SourceContract(
        source_id=1,
        source_name="client_cards_daily",
        source_description="Daily client card source with SCD2 attributes.",
        update_frequency="daily",
        business_columns=(
            "srv_mb_nflag",
            "cc_stoplist",
            "lne_tot_debt_int_ovrd_rub_amt",
            "lne_tot_debt_ovrd_rub_amt",
        ),
        technical_columns=("row_update_dtime", "loading_id", "row_hash_val"),
        partition_column="row_actual_to",
        scd_columns=("row_actual_from", "row_actual_to"),
        target_table="client_daily_attrs_scd2",
    ),
    SourceContract(
        source_id=2,
        source_name="credit_cards_info",
        source_description="Monthly credit card source with SCD1 attributes.",
        update_frequency="monthly",
        business_columns=(
            "client_income_amt",
            "oi_total_amt",
            "act_pl_os_rub_amt",
            "payroll_client_nflag",
            "inf_payroll_rub_amt",
            "legal_entity_amt",
            "inc_avg_risk_rub_amt",
            "otf_loan_rub_amt",
            "otf_fee_rub_amt",
            "inf_transfer_rub_amt",
            "cc_ever_nflag",
        ),
        technical_columns=("row_update_dtime", "loading_id", "row_hash_val"),
        partition_column="report_dt",
        scd_columns=(),
        target_table="client_monthly_attrs_scd1",
    ),
    SourceContract(
        source_id=3,
        source_name="deb_cards_info",
        source_description="Monthly debit card source with SCD1 attributes.",
        update_frequency="monthly",
        business_columns=(
            "onl_bank_active_1m_nfalg",
            "auto_pay_active_qty",
            "cl_income_1m_amt",
            "dep_acc_1st_open_dt",
            "wdr_cash_6m_amt",
            "cash_op_6m_amt",
            "cash_3m_qty",
            "lst_balance_amt",
            "card_active_1m_nflag",
        ),
        technical_columns=("row_update_dtime", "loading_id", "row_hash_val"),
        partition_column="report_dt",
        scd_columns=(),
        target_table="client_monthly_attrs_scd1",
    ),
)

_ATTRS: list[AttributeContract] = []
_attr_id = 1
for _source in SOURCE_REGISTRY:
    _data_types = {
        "cc_stoplist": "Int8",
        "oi_total_amt": "Decimal(20,2)",
        "dep_acc_1st_open_dt": "Datetime",
    }
    for _column in _source.business_columns:
        _default_type = "Decimal(18,2)" if _column.endswith("_amt") else "Int32"
        _ATTRS.append(
            AttributeContract(
                attribute_id=_attr_id,
                attribute_name=_column,
                source_name=_source.source_name,
                source_id=_source.source_id,
                source_column=_column,
                update_frequency=_source.update_frequency,
                target_table=_source.target_table,
                data_type=_data_types.get(_column, _default_type),
            )
        )
        _attr_id += 1
ATTRIBUTE_REGISTRY = tuple(_ATTRS)

TABLE_CONTRACTS: tuple[TableContract, ...] = (
    TableContract(
        "dim_sources",
        ("source_id",),
        (
            "source_id",
            "source_name",
            "source_description",
            "update_frequency",
            "row_create_dtime",
            "row_update_dtime",
            "valid_from",
            "valid_to",
            "is_current",
        ),
    ),
    TableContract(
        "dim_attributes",
        ("attribute_id",),
        (
            "attribute_id",
            "attribute_name",
            "attribute_description",
            "data_type",
            "source_id",
            "update_frequency",
            "row_create_dtime",
            "row_update_dtime",
        ),
    ),
    TableContract(
        "client_monthly_attrs_scd1",
        ("client_id", "attribute_id", "report_dt"),
        (
            "client_id",
            "attribute_id",
            "report_dt",
            "attribute_value",
            "source_id",
            "row_update_dtime",
            "row_loading_id",
            "row_hash_val",
        ),
    ),
    TableContract(
        "client_daily_attrs_scd2",
        ("client_id", "attribute_id", "row_actual_from"),
        (
            "client_id",
            "attribute_id",
            "attribute_value",
            "row_actual_from",
            "row_actual_to",
            "source_id",
            "row_update_dtime",
            "row_loading_id",
            "row_hash_val",
        ),
    ),
    TableContract(
        "load_log",
        ("load_id",),
        (
            "load_id",
            "source_id",
            "source_report_dt",
            "load_start_dtime",
            "load_end_dtime",
            "target_table",
            "load_status",
            "rows_loaded",
            "error_message",
        ),
    ),
)


def get_source_contract(source_name: str) -> SourceContract:
    """Return the source contract for `source_name`."""

    for contract in SOURCE_REGISTRY:
        if contract.source_name == source_name:
            return contract
    raise KeyError(f"Unknown source: {source_name}")


def get_attribute_contracts(source_name: str) -> tuple[AttributeContract, ...]:
    """Return all attribute contracts that belong to a source."""

    return tuple(attr for attr in ATTRIBUTE_REGISTRY if attr.source_name == source_name)


def get_table_contract(table_name: str) -> TableContract:
    """Return the warehouse table contract by table name."""

    for contract in TABLE_CONTRACTS:
        if contract.table_name == table_name:
            return contract
    raise KeyError(f"Unknown table: {table_name}")
