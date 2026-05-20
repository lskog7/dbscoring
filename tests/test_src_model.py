"""Контракт физической модели данных из автономного notebook."""

from __future__ import annotations

from tests.notebook_loader import SPARK_NOTEBOOK_PATH, load_namespace


NOTEBOOK_NAMESPACE = load_namespace(SPARK_NOTEBOOK_PATH)
SOURCES = NOTEBOOK_NAMESPACE["SOURCES"]


def build_attribute_id_by_name() -> dict[str, int]:
    all_attributes = [
        column_name
        for source_meta in SOURCES.values()
        for column_name in source_meta["columns"]
    ]
    return {attribute_name: attribute_id for attribute_id, attribute_name in enumerate(all_attributes, start=1)}


def test_source_registry_matches_lab_sources():
    assert set(SOURCES) == {"client_cards_daily", "credit_cards_info", "deb_cards_info"}
    assert SOURCES["client_cards_daily"]["update_frequency"] == "daily"
    assert SOURCES["client_cards_daily"]["target_table"] == "client_daily_attrs_scd2"
    assert SOURCES["credit_cards_info"]["update_frequency"] == "monthly"
    assert SOURCES["deb_cards_info"]["target_table"] == "client_monthly_attrs_scd1"


def test_business_attributes_are_vertical_model_attributes_only():
    attribute_id_by_name = build_attribute_id_by_name()
    all_attributes = [
        column_name
        for source_meta in SOURCES.values()
        for column_name in source_meta["columns"]
    ]

    assert len(all_attributes) == 24
    assert "client_id" not in all_attributes
    assert "row_update_dtime" not in all_attributes
    assert set(attribute_id_by_name) == set(all_attributes)
    assert len(set(attribute_id_by_name.values())) == len(all_attributes)


def test_warehouse_contains_task_1_to_3_tables():
    # Таблицы проверяем через функции notebook, а не через отдельный глобальный словарь из модулей.
    schema_names = {
        "dim_sources",
        "dim_attributes",
        "load_log",
        "tech_source_partitions",
        "client_monthly_attrs_scd1",
        "client_daily_attrs_scd2",
    }
    build_spark_schema = NOTEBOOK_NAMESPACE["build_spark_schema"]

    for table_name in schema_names:
        assert build_spark_schema(table_name) is not None
