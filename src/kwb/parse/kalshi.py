"""Parse standard archived Kalshi envelopes on the Mac, never on the collector VM."""

import argparse
import base64
import json
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from urllib.parse import unquote, urlsplit

from kwb.schemas import (
    BookInput,
    MarketInput,
    MarketSnapshot,
    OrderBookHeader,
    OrderBookLevel,
    SingleBookInput,
)
from kwb.store import (
    MODELS,
    RawEnvelope,
    Store,
    StoreError,
    atomic_json,
    configured_series,
    raw_hours,
    read_envelopes,
    strict_json,
)

MONTHS = {
    name: i
    for i, name in enumerate(
        ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), 1
    )
}
EVENT = re.compile(r"([A-Z0-9]+)-(\d{2})([A-Z]{3})(\d{2})")
NUMBER = r"(-?\d+)"


@dataclass
class RunReport:
    envelopes: int = 0
    duplicate_envelopes: int = 0
    non_200: dict[str, int] = field(default_factory=dict)
    raw_only_market_rows: int = 0
    raw_only_books: int = 0
    discovery_envelopes: int = 0
    market_snapshots: int = 0
    orderbook_snapshots: int = 0
    orderbook_levels: int = 0
    published_hours: int = 0
    errors: list[str] = field(default_factory=list)


class IncompleteListing(StoreError):
    """Hold publication until the next input hour can complete this listing."""


def event_identity(event: str) -> tuple[str, date]:
    match = EVENT.fullmatch(event)
    if match is None or match[3] not in MONTHS:
        raise ValueError(f"Unexpected daily event ticker {event!r}")
    return match[1], date(2000 + int(match[2]), MONTHS[match[3]], int(match[4]))


def rules_bounds(rules: str) -> tuple[int | None, int | None, bool, bool]:
    text = rules.casefold()
    match = re.search(r"is between\s+" + NUMBER + r"\s*(?:[-–]|and)\s*" + NUMBER, text)
    if match:
        return int(match[1]), int(match[2]), True, True
    for phrase, lower in (("greater than", True), ("less than", False)):
        match = re.search(r"is\s+" + phrase + r"\s+" + NUMBER + r"\s*°", text)
        if match:
            bound = int(match[1])
            return (bound, None, False, False) if lower else (None, bound, False, False)
    raise ValueError("Unrecognized literal temperature bounds in primary rules")


def market_row(market: MarketInput, raw: RawEnvelope, series: str) -> MarketSnapshot:
    actual_series, target = event_identity(market.event_ticker)
    if actual_series != series or not market.ticker.startswith(market.event_ticker + "-"):
        raise ValueError("Market identity disagrees with requested series/event")
    interval = rules_bounds(market.rules_primary)
    if market.strike_type is None:
        if market.floor_strike is not None and market.floor_strike != interval[0]:
            raise ValueError("floor_strike disagrees with primary rules")
        if market.cap_strike is not None and market.cap_strike != interval[1]:
            raise ValueError("cap_strike disagrees with primary rules")
        bounds_source = "rules"
    else:
        expected = {
            "less": (None, market.cap_strike, False, False),
            "greater": (market.floor_strike, None, False, False),
            "between": (market.floor_strike, market.cap_strike, True, True),
        }[market.strike_type]
        if interval != expected:
            raise ValueError("Structured bounds disagree with literal primary rules")
        # A bound on the wrong side is an error even if the right side matches.
        if (market.floor_strike, market.cap_strike) != interval[:2]:
            raise ValueError("Unexpected structured bound")
        bounds_source = "structured"
    fields = market.model_dump()
    values = {key: fields[key] for key in MarketSnapshot.model_fields if key in fields}
    return MarketSnapshot(
        **values,
        **raw.provenance(),
        series_ticker=series,
        target_date=target,
        lower_bound_f=interval[0],
        upper_bound_f=interval[1],
        lower_inclusive=interval[2],
        upper_inclusive=interval[3],
        bounds_source=bounds_source,
    )


def validate_partition(rows: list[MarketSnapshot]) -> None:
    if len(rows) != 6 or len({row.ticker for row in rows}) != 6:
        raise ValueError("Event must contain exactly six distinct markets")
    low = [row for row in rows if row.lower_bound_f is None]
    high = [row for row in rows if row.upper_bound_f is None]
    middle = sorted(
        (row.lower_bound_f, row.upper_bound_f)
        for row in rows
        if row.lower_bound_f is not None and row.upper_bound_f is not None
    )
    if len(low) != 1 or len(high) != 1 or len(middle) != 4:
        raise ValueError("Event must have four brackets and two strict tails")
    if any(row.lower_inclusive or row.upper_inclusive for row in low + high):
        raise ValueError("Event tails must be strict")
    if any(
        not row.lower_inclusive or not row.upper_inclusive
        for row in rows
        if row.lower_bound_f is not None and row.upper_bound_f is not None
    ):
        raise ValueError("Interior brackets must be inclusive")
    if (
        any(b != a + 1 for a, b in middle)
        or any(middle[i + 1][0] != middle[i][1] + 1 for i in range(3))
        or low[0].upper_bound_f != middle[0][0]
        or high[0].lower_bound_f != middle[-1][1]
    ):
        raise ValueError("Event is not a gap-free six-market integer-Fahrenheit partition")


def body_json(raw: RawEnvelope) -> dict:
    envelope = raw.envelope
    body = (
        base64.b64decode(envelope.body, validate=True) if envelope.body_encoding else envelope.body
    )
    result = strict_json(body)
    if not isinstance(result, dict):
        raise ValueError("Expected a JSON response object")
    return result


def book_rows(ticker: str, book: BookInput, raw: RawEnvelope, rows: dict) -> None:
    rows["orderbook_snapshots"].append(
        OrderBookHeader(
            **raw.provenance(),
            ticker=ticker,
            yes_level_count=len(book.yes_dollars),
            no_level_count=len(book.no_dollars),
        )
    )
    for side in ("yes", "no"):
        levels = getattr(book, side + "_dollars")
        prices = [Decimal(price) for price, _ in levels]
        if prices != sorted(set(prices)):
            raise ValueError("Order-book prices must be unique and ascending")
        for index, (price, quantity) in enumerate(levels):
            if Decimal(quantity) <= 0:
                raise ValueError("Resting quantity must be positive")
            rows["orderbook_levels"].append(
                OrderBookLevel(
                    **raw.provenance(),
                    ticker=ticker,
                    side=side,
                    level_index=index,
                    price_dollars=price,
                    quantity_fp=quantity,
                )
            )


def parse_hour(envelopes: list[RawEnvelope], configured: frozenset[str], report: RunReport) -> dict:
    rows = {name: [] for name in MODELS}
    pending = {}  # A paginated listing may split an event; validate only complete chains.
    for raw in envelopes:
        envelope = raw.envelope
        report.envelopes += 1
        if envelope.http_status != 200:
            status = str(envelope.http_status)
            report.non_200[status] = report.non_200.get(status, 0) + 1
            continue
        try:
            url = urlsplit(envelope.request_url)
            if url.scheme != "https" or url.netloc not in {
                "external-api.kalshi.com",
                "api.elections.kalshi.com",
            }:
                raise ValueError("Unexpected Kalshi API origin")
            if not url.path.startswith("/trade-api/v2/"):
                raise ValueError("Unexpected Kalshi API version/path")
            path = url.path.removeprefix("/trade-api/v2")
            params = envelope.request_params
            if path == "/series":
                report.discovery_envelopes += 1
                continue
            if path == "/markets":
                series = params["series_ticker"]
                body = body_json(raw)
                if (
                    set(body) != {"cursor", "markets"}
                    or not isinstance(body["markets"], list)
                    or not isinstance(body["cursor"], str)
                ):
                    raise ValueError("Expected markets array and cursor string")
                if series not in configured:
                    report.raw_only_market_rows += len(body["markets"])
                    continue
                if params.get("status") != "open" or set(params) - {
                    "series_ticker",
                    "status",
                    "limit",
                    "cursor",
                }:
                    raise ValueError("Expected a standard open-markets listing")
                cursor = params.get("cursor", "")
                if cursor:
                    if series not in pending or cursor != pending[series][0]:
                        raise ValueError("Missing or out-of-order markets pagination page")
                    listing = pending[series][1]
                else:
                    if series in pending:
                        raise ValueError("Incomplete previous markets pagination chain")
                    listing = []
                for item in body["markets"]:
                    row = market_row(MarketInput.model_validate(item), raw, series)
                    listing.append(row)
                if body["cursor"]:
                    if body["cursor"] == cursor:
                        raise ValueError("Markets pagination repeated its cursor")
                    pending[series] = body["cursor"], listing, raw.locator
                else:
                    events = defaultdict(list)
                    for row in listing:
                        events[row.event_ticker].append(row)
                    for event, event_rows in events.items():
                        try:
                            validate_partition(event_rows)
                        except ValueError as exc:
                            raise ValueError(f"{event}: {exc}") from exc
                    rows["market_snapshots"].extend(listing)
                    pending.pop(series, None)
            elif path == "/markets/orderbooks":
                if set(params) != {"tickers"}:
                    raise ValueError("Unexpected batch order-book request parameters")
                body = body_json(raw)
                if set(body) != {"orderbooks"} or not isinstance(body["orderbooks"], list):
                    raise ValueError("Expected orderbooks array")
                requested = params["tickers"]
                if (
                    not isinstance(requested, list)
                    or not requested
                    or len(requested) > 100
                    or any(not isinstance(t, str) or not t for t in requested)
                    or len(set(requested)) != len(requested)
                ):
                    raise ValueError("Expected distinct request_params tickers")
                books = body["orderbooks"]
                returned = [book["ticker"] for book in books]
                if any(not isinstance(t, str) or not t for t in returned) or Counter(
                    returned
                ) != Counter(requested):
                    raise ValueError("Order-book chunk tickers differ from request_params tickers")
                # Validate completeness BEFORE the configured-series filter.
                for book in books:
                    ticker = book["ticker"]
                    if ticker.split("-", 1)[0] not in configured:
                        report.raw_only_books += 1
                        continue
                    if set(book) != {"ticker", "orderbook_fp"}:
                        raise ValueError("Unexpected order-book fields")
                    book_rows(ticker, BookInput.model_validate(book["orderbook_fp"]), raw, rows)
            elif re.fullmatch(r"/markets/[^/]+/orderbook", path):
                ticker = unquote(path.split("/")[2])
                if ticker.split("-", 1)[0] not in configured:
                    report.raw_only_books += 1
                    continue
                if (
                    set(params) != {"depth"}
                    or type(params["depth"]) is not int
                    or params["depth"] != 0
                ):
                    raise ValueError("Expected an untruncated depth=0 single-book request")
                body = SingleBookInput.model_validate(body_json(raw))
                book_rows(ticker, body.orderbook_fp, raw, rows)
            else:
                raise ValueError(f"Unexpected collector endpoint {path}")
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise StoreError(f"{raw.locator}: {exc}") from exc
    if pending:
        _, _, locator = next(iter(pending.values()))
        raise IncompleteListing(f"{locator}: incomplete markets pagination chain")
    return rows


def parse(
    paths: list[Path],
    store: Store,
    *,
    config: Path = Path("config/nws_stations.csv"),
    report: RunReport | None = None,
) -> RunReport:
    report = RunReport() if report is None else report
    try:
        configured = configured_series(config)
        buffered = []
        buffered_sources = {}
        last_incomplete = None
        for hour, files in raw_hours(paths).items():
            envelopes, duplicates = read_envelopes(files)
            report.duplicate_envelopes += duplicates
            buffered.extend(envelopes)
            buffered_sources[hour] = {raw.source_id: raw.sha256 for raw in envelopes}
            trial = RunReport()
            try:
                rows = parse_hour(buffered, configured, trial)
            except IncompleteListing as exc:
                last_incomplete = exc
                continue
            except ValueError:
                _merge_report(report, trial)
                raise
            _merge_report(report, trial)
            for output_hour, sources in buffered_sources.items():
                hour_rows = {
                    name: [
                        row
                        for row in records
                        if row.available_at.strftime("%Y-%m-%dT%H") == output_hour
                    ]
                    for name, records in rows.items()
                }
                store.publish_hour(output_hour, hour_rows, sources)
                for table, records in hour_rows.items():
                    setattr(report, table, getattr(report, table) + len(records))
                report.published_hours += 1
            buffered.clear()
            buffered_sources.clear()
            last_incomplete = None
        if last_incomplete:
            _merge_report(report, trial)
            raise last_incomplete
    except (ValueError, OSError) as exc:
        report.errors.append(str(exc))
        raise
    return report


def _merge_report(report: RunReport, trial: RunReport) -> None:
    for name in ("envelopes", "raw_only_market_rows", "raw_only_books", "discovery_envelopes"):
        setattr(report, name, getattr(report, name) + getattr(trial, name))
    for status, count in trial.non_200.items():
        report.non_200[status] = report.non_200.get(status, 0) + count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--store", type=Path, default=Path("data/kalshi"))
    parser.add_argument("--config", type=Path, default=Path("config/nws_stations.csv"))
    parser.add_argument("--report", type=Path, default=Path("state/kalshi-parse-report.json"))
    args = parser.parse_args()
    report = RunReport()
    try:
        parse(args.inputs, Store(args.store), config=args.config, report=report)
    finally:
        atomic_json(args.report, asdict(report))
    print(json.dumps(asdict(report), sort_keys=True))


if __name__ == "__main__":
    main()
