"""Strict Kalshi input and stored-record boundaries; all availability is fetch time."""

import re
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, field_validator


def utc_timestamp(value: str | datetime) -> datetime:
    if isinstance(value, str):
        if (
            re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)", value)
            is None
        ):
            raise ValueError("Expected an ISO UTC timestamp with at most microsecond precision")
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Timestamp must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError("Timestamp must be timezone-aware UTC")
    return value.astimezone(UTC)


def fixed_point(value: str) -> str:
    # Preserve the lexeme. Decimal validates it; it never becomes a binary float.
    if not isinstance(value, str) or re.fullmatch(r"\d+(?:\.\d+)?", value) is None:
        raise ValueError("Expected a nonnegative fixed-point string")
    if not Decimal(value).is_finite():
        raise ValueError("Expected a finite fixed-point string")
    return value


def price(value: str) -> str:
    value = fixed_point(value)
    if Decimal(value) > 1:
        raise ValueError("Binary price must be between zero and one dollar")
    return value


UTCTimestamp = Annotated[datetime, BeforeValidator(utc_timestamp)]
FixedPoint = Annotated[str, BeforeValidator(fixed_point)]
Price = Annotated[str, BeforeValidator(price)]
Nonempty = Annotated[str, Field(min_length=1)]
Int32 = Annotated[int, Field(ge=-(2**31), le=2**31 - 1)]


class Boundary(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class Envelope(Boundary):
    request_url: Nonempty
    request_params: dict[str, str | int | list[str]]
    http_status: Annotated[int, Field(ge=100, le=599)]
    requested_at: UTCTimestamp
    received_at: UTCTimestamp
    body: str
    body_encoding: Literal["base64"] | None = None


class PriceRange(Boundary):
    start: Price
    end: Price
    step: FixedPoint


class MarketInput(Boundary):
    """Observed v2 market fields, including explicitly supported optional metadata.

    Optional provider fields stay optional; required prices and counts never get
    fabricated defaults. Unknown fields require a conscious schema update.
    """

    ticker: Nonempty
    event_ticker: Nonempty
    market_type: Literal["binary"]
    title: str
    subtitle: str | None = None
    yes_sub_title: str
    no_sub_title: str
    created_time: UTCTimestamp
    updated_time: UTCTimestamp
    open_time: UTCTimestamp
    close_time: UTCTimestamp
    expected_expiration_time: UTCTimestamp | None = None
    expiration_time: UTCTimestamp | None = None
    latest_expiration_time: UTCTimestamp
    occurrence_datetime: UTCTimestamp | None = None
    settlement_timer_seconds: int
    status: Literal[
        "initialized",
        "inactive",
        "active",
        "closed",
        "determined",
        "disputed",
        "amended",
        "finalized",
        "paused",
    ]
    yes_bid_dollars: Price
    yes_ask_dollars: Price
    no_bid_dollars: Price
    no_ask_dollars: Price
    last_price_dollars: Price
    previous_yes_bid_dollars: Price
    previous_yes_ask_dollars: Price
    previous_price_dollars: Price
    notional_value_dollars: FixedPoint
    volume_fp: FixedPoint
    volume_24h_fp: FixedPoint
    open_interest_fp: FixedPoint
    yes_bid_size_fp: FixedPoint | None = None
    yes_ask_size_fp: FixedPoint | None = None
    liquidity_dollars: FixedPoint | None = None
    result: Literal["", "yes", "no", "scalar"]
    expiration_value: str
    can_close_early: bool
    early_close_condition: str | None = None
    rules_primary: Nonempty
    rules_secondary: str
    strike_type: Literal["less", "greater", "between"] | None = None
    floor_strike: Int32 | None = None
    cap_strike: Int32 | None = None
    functional_strike: str | None = None
    custom_strike: dict[str, Any] | None = None
    price_level_structure: str
    price_ranges: list[PriceRange]
    settlement_bounds_type: str | None = None
    settlement_value_dollars: Price | None = None
    settlement_floor_dollars: Price | None = None
    settlement_ts: UTCTimestamp | None = None
    fee_waiver_expiration_time: UTCTimestamp | None = None
    fractional_trading_enabled: bool | None = None
    exchange_index: int | None = None
    primary_participant_key: str | None = None
    is_provisional: bool | None = None
    mve_collection_ticker: str | None = None
    mve_selected_legs: list[dict[str, Any]] | None = None


class MarketsInput(Boundary):
    cursor: str
    markets: list[MarketInput]


class BookInput(Boundary):
    yes_dollars: list[tuple[Price, FixedPoint]]
    no_dollars: list[tuple[Price, FixedPoint]]

    @field_validator("yes_dollars", "no_dollars", mode="before")
    @classmethod
    def accept_json_pairs(cls, value):
        if not isinstance(value, list) or any(not isinstance(pair, list) for pair in value):
            raise ValueError("Expected an array of price/quantity arrays")
        return [tuple(pair) for pair in value]


class BatchBookInput(Boundary):
    ticker: Nonempty
    orderbook_fp: BookInput


class BooksInput(Boundary):
    orderbooks: list[BatchBookInput]


class SingleBookInput(Boundary):
    orderbook_fp: BookInput


class Provenance(Boundary):
    source_id: Nonempty
    source_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    requested_at: UTCTimestamp
    available_at: UTCTimestamp
    available_date: date


class MarketSnapshot(Provenance):
    ticker: Nonempty
    event_ticker: Nonempty
    series_ticker: Nonempty
    target_date: date
    market_type: Literal["binary"]
    status: str
    strike_type: Literal["less", "greater", "between"] | None
    floor_strike: Int32 | None
    cap_strike: Int32 | None
    lower_bound_f: Int32 | None
    upper_bound_f: Int32 | None
    lower_inclusive: bool
    upper_inclusive: bool
    bounds_source: Literal["structured", "rules"]
    rules_primary: str
    rules_secondary: str
    created_time: UTCTimestamp
    updated_time: UTCTimestamp
    open_time: UTCTimestamp
    close_time: UTCTimestamp
    yes_bid_dollars: Price
    yes_ask_dollars: Price
    no_bid_dollars: Price
    no_ask_dollars: Price
    last_price_dollars: Price
    previous_yes_bid_dollars: Price
    previous_yes_ask_dollars: Price
    previous_price_dollars: Price
    notional_value_dollars: FixedPoint
    volume_fp: FixedPoint
    volume_24h_fp: FixedPoint
    open_interest_fp: FixedPoint
    yes_bid_size_fp: FixedPoint | None
    yes_ask_size_fp: FixedPoint | None
    liquidity_dollars: FixedPoint | None
    price_level_structure: str
    price_ranges: list[PriceRange]


class OrderBookHeader(Provenance):
    ticker: Nonempty
    yes_level_count: Annotated[int, Field(ge=0)]
    no_level_count: Annotated[int, Field(ge=0)]


class OrderBookLevel(Provenance):
    ticker: Nonempty
    side: Literal["yes", "no"]
    level_index: Annotated[int, Field(ge=0)]
    price_dollars: Price
    quantity_fp: FixedPoint


class OrderBookSnapshot(Boundary):
    header: OrderBookHeader
    levels: list[OrderBookLevel]
