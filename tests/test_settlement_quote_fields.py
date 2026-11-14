import json
from datetime import timedelta

import pytest
from test_kalshi_parse_store import MARKETS, envelope, write_envelope
from test_settlements import SERIES, START, acquisition, all_rows

from kwb.parse.kalshi import parse as parse_markets
from kwb.parse.settlements import parse
from kwb.schemas import SettlementInput
from kwb.store import Store

QUOTE_FIELDS = frozenset(
    [
        "yes_bid_dollars",
        "yes_ask_dollars",
        "no_bid_dollars",
        "no_ask_dollars",
        "last_price_dollars",
        "previous_yes_bid_dollars",
        "previous_yes_ask_dollars",
        "previous_price_dollars",
        "notional_value_dollars",
        "volume_fp",
        "volume_24h_fp",
        "open_interest_fp",
        "yes_bid_size_fp",
        "yes_ask_size_fp",
        "liquidity_dollars",
    ]
)


def edit_quote(field, value):
    def edit(name, envelope):
        payload = json.loads(envelope["body"])
        payload["markets"][0][field] = value
        envelope["body"] = json.dumps(payload)

    return edit


def test_the_same_negative_quote_size_in_a_live_snapshot_still_raises_with_its_locator(tmp_path):
    value = envelope(MARKETS)
    edit_quote("yes_bid_size_fp", "-276.00")("live", value)
    source = write_envelope(tmp_path / "raw", value)
    store = Store(tmp_path / "store")
    with pytest.raises(ValueError, match=source.name) as error:
        parse_markets([source], store)
    assert "yes_bid_size_fp" in str(error.value)
    assert not (store.root / "manifest.json").exists()


@pytest.mark.parametrize("field", sorted(QUOTE_FIELDS))
@pytest.mark.parametrize("value", ["-276.00", "123456789.0000"])
def test_settlement_quotes_have_no_sign_or_range_checks_and_are_not_stored(tmp_path, field, value):
    inputs = acquisition(tmp_path / "raw", edits=edit_quote(field, value))
    store = Store(tmp_path / "store")
    assert parse(inputs, store).settlements == 6
    rows = all_rows(store)
    assert len(rows) == 6
    assert all(not (QUOTE_FIELDS & set(row)) for row in rows)
    assert (
        sum(row.label == "yes" for row in store.get_settlements(SERIES, START + timedelta(hours=1)))
        == 1
    )


@pytest.mark.parametrize("field", sorted(QUOTE_FIELDS))
def test_a_malformed_fixed_point_quote_in_any_settlement_quote_field_raises(tmp_path, field):
    inputs = acquisition(tmp_path / "raw", edits=edit_quote(field, "1e3"))
    with pytest.raises(ValueError, match=inputs[2].name) as error:
        parse(inputs, Store(tmp_path / "store"))
    assert field in str(error.value)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "+1.0",
        "1.",
        ".2",
        "--1.0",
        " 1.0",
        "1.0 ",
        "NaN",
        "Infinity",
        "1/2",
        "١.٢",
        12,
        0.5,
        True,
    ],
)
def test_settlement_quote_validation_rejects_non_fixed_point_notation_without_coercion(
    tmp_path, value
):
    inputs = acquisition(tmp_path / "raw", edits=edit_quote("yes_bid_size_fp", value))
    with pytest.raises(ValueError, match="yes_bid_size_fp"):
        parse(inputs, Store(tmp_path / "store"))


@pytest.mark.parametrize(
    "field,value",
    [
        ("result", "maybe"),
        ("result", ""),
        ("settlement_value_dollars", "-0.01"),
        ("settlement_value_dollars", "1.01"),
        ("settlement_value_dollars", "0.5000"),
        ("settlement_value_dollars", "1e0"),
        ("settlement_floor_dollars", "-0.01"),
        ("settlement_floor_dollars", "1.01"),
        ("settlement_bounds_type", 1),
        ("settlement_ts", None),
        ("settlement_ts", "2026-11-14T00:00:00"),
        ("settlement_ts", "2026-11-14T00:00:00.1234567Z"),
        ("expiration_value", None),
        ("expiration_value", 80),
        ("strike_type", "unknown"),
        ("floor_strike", "80"),
        ("cap_strike", "80"),
        ("floor_strike", 999),
        ("rules_primary", ""),
        ("rules_primary", "not recognizable temperature bounds"),
        ("rules_secondary", 7),
    ],
)
def test_a_settlement_label_field_violation_still_raises(tmp_path, field, value):
    inputs = acquisition(tmp_path / "raw", edits=edit_quote(field, value))
    store = Store(tmp_path / "store")
    with pytest.raises(ValueError, match=inputs[2].name):
        parse(inputs, store)
    assert not (store.root / "manifest.json").exists()


def test_unknown_settlement_fields_remain_errors_and_known_optional_quotes_remain_optional(
    tmp_path,
):
    from test_settlements import body

    record = body("live")["markets"][0]
    for key in ("yes_bid_size_fp", "yes_ask_size_fp", "liquidity_dollars"):
        record.pop(key, None)
    SettlementInput.model_validate(record)
    record["unknown_quote_fp"] = "1.00"
    with pytest.raises(ValueError, match="unknown_quote_fp"):
        SettlementInput.model_validate(record)
