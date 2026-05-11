from dbscoring.contracts import (
    ATTRIBUTE_REGISTRY,
    CANONICAL_CLIENT_ID_TYPE,
    LOAD_STATUSES,
    SOURCE_REGISTRY,
    TABLE_CONTRACTS,
    get_attribute_contracts,
    get_source_contract,
    get_table_contract,
)


def test_source_registry_is_complete() -> None:
    assert [source.source_name for source in SOURCE_REGISTRY] == [
        "client_cards_daily",
        "credit_cards_info",
        "deb_cards_info",
    ]


def test_attribute_registry_has_exact_business_attribute_count() -> None:
    assert len(ATTRIBUTE_REGISTRY) == 24
    assert len({attribute.attribute_id for attribute in ATTRIBUTE_REGISTRY}) == 24


def test_attribute_registry_matches_source_business_columns() -> None:
    for source in SOURCE_REGISTRY:
        attributes = get_attribute_contracts(source.source_name)
        assert (
            tuple(attribute.source_column for attribute in attributes)
            == source.business_columns
        )
        assert {attribute.source_id for attribute in attributes} == {source.source_id}


def test_client_id_is_string_by_project_contract() -> None:
    assert CANONICAL_CLIENT_ID_TYPE == "STRING"


def test_load_statuses_are_strict() -> None:
    assert LOAD_STATUSES == ("started", "success", "failed", "skipped")


def test_table_contracts_have_primary_keys_inside_columns() -> None:
    assert len(TABLE_CONTRACTS) == 5
    for contract in TABLE_CONTRACTS:
        assert set(contract.primary_key) <= set(contract.columns)
        assert get_table_contract(contract.table_name) == contract


def test_known_sources_map_to_expected_targets() -> None:
    assert (
        get_source_contract("client_cards_daily").target_table
        == "client_daily_attrs_scd2"
    )
    assert (
        get_source_contract("credit_cards_info").target_table
        == "client_monthly_attrs_scd1"
    )
    assert (
        get_source_contract("deb_cards_info").target_table
        == "client_monthly_attrs_scd1"
    )
