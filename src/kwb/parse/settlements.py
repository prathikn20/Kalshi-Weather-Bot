"""Parse complete standard settlement acquisitions on the Mac, including revisions."""

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

from kwb.parse.kalshi import body_json, event_identity, market_row, validate_partition
from kwb.schemas import Provenance, Settlement, SettlementInput
from kwb.store import Store, StoreError, atomic_json, configured_series, raw_hours, read_envelopes

LABEL_FIELDS = (
    "result",
    "expiration_value",
    "settlement_value_dollars",
    "settlement_floor_dollars",
    "settlement_bounds_type",
    "settlement_ts",
    "rules_primary",
    "rules_secondary",
    "lower_bound_f",
    "upper_bound_f",
    "lower_inclusive",
    "upper_inclusive",
)
REVISION_FIELDS = tuple(
    name
    for name in Settlement.model_fields
    if name not in Provenance.model_fields and name not in {"label", "label_status"}
)


@dataclass
class SettlementReport:
    envelopes: int = 0
    duplicate_envelopes: int = 0
    non_200: dict[str, int] = field(default_factory=dict)
    raw_only_market_rows: int = 0
    settlements: int = 0
    exceptions: list[str] = field(default_factory=list)
    revisions: list[dict] = field(default_factory=list)
    coverage_gaps: list[dict] = field(default_factory=list)
    completed_runs: int = 0
    errors: list[str] = field(default_factory=list)


def exception_events(path):
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != ["event_ticker", "reason", "evidence"]:
            raise StoreError(f"{path}: invalid settlement exception columns")
        rows = list(reader)
    events = set()
    for row in rows:
        if set(row) != {"event_ticker", "reason", "evidence"} or any(
            not isinstance(value, str) or not value.strip() for value in row.values()
        ):
            raise StoreError(f"{path}: invalid settlement exception")
        event_identity(row["event_ticker"])
        if row["event_ticker"] in events:
            raise StoreError(f"{path}: duplicate exception {row['event_ticker']}")
        events.add(row["event_ticker"])
    return events


def endpoint(raw):
    url = urlsplit(raw.envelope.request_url)
    if (
        url.scheme != "https"
        or url.netloc not in {"external-api.kalshi.com", "api.elections.kalshi.com"}
        or not url.path.startswith("/trade-api/v2/")
    ):
        raise StoreError(f"{raw.locator}: unexpected Kalshi origin/path")
    return url.path.removeprefix("/trade-api/v2")


def parse_run(envelopes, configured, exceptions, report, previous_windows):
    pending, observations, events, windows = {}, [], defaultdict(dict), []
    for raw in envelopes:
        try:
            path, params = endpoint(raw), raw.envelope.request_params
            if path in {"/series", "/historical/cutoff"}:
                if params:
                    raise ValueError("Unexpected discovery/cutoff parameters")
                continue
            if path not in {"/markets", "/historical/markets"}:
                raise ValueError(f"Unexpected settlement endpoint {path}")
            expected = (
                {"series_ticker", "limit"}
                if path.startswith("/historical/")
                else {"series_ticker", "status", "limit", "min_settled_ts", "max_settled_ts"}
            )
            if "event_ticker" in params:
                expected = (
                    {"event_ticker", "limit"}
                    if path.startswith("/historical/")
                    else {"event_ticker"}
                )
            if set(params) - {"cursor"} != expected:
                raise ValueError("Unexpected settlement listing parameters")
            if "status" in params and params["status"] != "settled":
                raise ValueError("Expected status=settled")
            if "limit" in params and (
                type(params["limit"]) is not int or not 1 <= params["limit"] <= 1000
            ):
                raise ValueError("Invalid page limit")
            if "min_settled_ts" in params:
                lower, upper = params["min_settled_ts"], params["max_settled_ts"]
                if type(lower) is not int or type(upper) is not int or lower >= upper:
                    raise ValueError("Invalid settlement window")
            body = body_json(raw)
            if (
                set(body) != {"markets", "cursor"}
                or not isinstance(body["markets"], list)
                or not isinstance(body["cursor"], str)
            ):
                raise ValueError("Expected markets array and cursor string")
            key = (
                path,
                json.dumps({k: v for k, v in params.items() if k != "cursor"}, sort_keys=True),
            )
            cursor = params.get("cursor", "")
            if not isinstance(cursor, str):
                raise ValueError("Invalid request cursor")
            if key not in pending:
                if cursor:
                    raise ValueError("Missing first pagination page")
                pending[key] = {
                    "next": "",
                    "cursors": set(),
                    "tickers": set(),
                    "locator": raw.locator,
                }
            chain = pending[key]
            if cursor != chain["next"]:
                raise ValueError("Missing or out-of-order pagination page")
            for item in body["markets"]:
                if (
                    not isinstance(item, dict)
                    or not isinstance(item.get("ticker"), str)
                    or not isinstance(item.get("event_ticker"), str)
                ):
                    raise ValueError("Missing market identity")
                if item["ticker"] in chain["tickers"]:
                    raise ValueError("Repeated ticker/pagination page")
                chain["tickers"].add(item["ticker"])
                actual = item["event_ticker"].split("-", 1)[0]
                if actual not in configured:
                    report.raw_only_market_rows += 1
                    continue
                if "series_ticker" in params and actual != params["series_ticker"]:
                    raise ValueError("Configured market disagrees with requested series")
                if "event_ticker" in params and item["event_ticker"] != params["event_ticker"]:
                    raise ValueError("Market disagrees with requested event")
                served = SettlementInput.model_validate(item)
                snapshot = market_row(served, raw, actual)
                listed = served.event_ticker in exceptions
                if served.result not in {"yes", "no"} and not listed:
                    raise ValueError(
                        f"{served.event_ticker}: unlisted non-binary settlement result"
                    )
                row = Settlement(
                    **snapshot.model_dump(),
                    result=served.result,
                    expiration_value=served.expiration_value,
                    settlement_ts=served.settlement_ts,
                    settlement_value_dollars=served.settlement_value_dollars,
                    settlement_floor_dollars=served.settlement_floor_dollars,
                    settlement_bounds_type=served.settlement_bounds_type,
                    label_status="exception" if listed else "labeled",
                    label=None if listed else served.result,
                )
                if not listed and Decimal(row.settlement_value_dollars) != (
                    1 if row.result == "yes" else 0
                ):
                    raise ValueError(f"{row.event_ticker}: result and payout disagree")
                observations.append(row)
                events[row.event_ticker][row.ticker] = row
            next_cursor = body["cursor"]
            if next_cursor:
                if next_cursor in chain["cursors"] or next_cursor == cursor:
                    raise ValueError("Repeated pagination cursor")
                chain["cursors"].add(next_cursor)
                chain["next"] = next_cursor
            else:
                del pending[key]
                if "min_settled_ts" in params:
                    windows.append(
                        (
                            params["series_ticker"],
                            params["min_settled_ts"],
                            params["max_settled_ts"],
                            raw.locator,
                        )
                    )
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise StoreError(f"{raw.locator}: {exc}") from exc
    if pending:
        raise StoreError(
            f"{next(iter(pending.values()))['locator']}: incomplete settlement pagination chain"
        )
    for event, members in events.items():
        rows = list(members.values())
        try:
            validate_partition(rows)
            if event not in exceptions and (
                sum(row.result == "yes" for row in rows) != 1
                or any(row.result == "scalar" for row in rows)
            ):
                raise ValueError("Finalized event must have exactly one YES and five NO results")
        except ValueError as exc:
            source = next(row.source_id for row in rows)
            raise StoreError(f"{source}: {event}: {exc}") from exc
        if event in exceptions and event not in report.exceptions:
            report.exceptions.append(event)
    for series, lower, upper, locator in windows:
        previous = previous_windows.get(series)
        if previous is not None and lower > previous:
            gap = {"series": series, "start": previous, "end": lower, "locator": locator}
            report.coverage_gaps.append(gap)
            raise StoreError(
                f"{locator}: settlement fetch window gap for {series}: {previous}->{lower}"
            )
        previous_windows[series] = max(previous or upper, upper)
    return observations


def publish_run(envelopes, store, configured, exceptions, report, windows):
    rows = parse_run(envelopes, configured, exceptions, report, windows)
    earliest = min(raw.envelope.received_at for raw in envelopes) - timedelta(microseconds=1)
    previous = {
        row.ticker: row for series in configured for row in store.get_settlements(series, earliest)
    }
    for row in sorted(rows, key=lambda r: (r.available_at, r.source_id, r.ticker)):
        prior = previous.get(row.ticker)
        if prior:
            changes = {
                key: {
                    "before": prior.model_dump(mode="json")[key],
                    "after": row.model_dump(mode="json")[key],
                }
                for key in REVISION_FIELDS
                if getattr(prior, key) != getattr(row, key)
            }
            if changes:
                report.revisions.append(
                    {
                        "ticker": row.ticker,
                        "previous_source_id": prior.source_id,
                        "source_id": row.source_id,
                        "previous_available_at": prior.available_at.isoformat(),
                        "available_at": row.available_at.isoformat(),
                        "changes": changes,
                        "label_revision": any(key in LABEL_FIELDS for key in changes),
                    }
                )
        previous[row.ticker] = row
    by_hour = defaultdict(list)
    sources = defaultdict(dict)
    for raw in envelopes:
        sources[raw.envelope.received_at.strftime("%Y-%m-%dT%H")][raw.source_id] = raw.sha256
    for row in rows:
        by_hour[row.available_at.strftime("%Y-%m-%dT%H")].append(row)
    for hour in sorted(sources):
        store.publish_hour(hour, {"settlements": by_hour[hour]}, sources[hour])
    report.settlements += len(rows)
    report.completed_runs += 1


def parse(
    paths,
    store,
    *,
    config=Path("config/nws_stations.csv"),
    exceptions=Path("config/settlement_exceptions.csv"),
    report=None,
):
    report = report if report is not None else SettlementReport()
    try:
        configured, listed = configured_series(config), exception_events(exceptions)
        raw = []
        for files in raw_hours(paths).values():
            envelopes, duplicates = read_envelopes(files, source="kalshi_settlements")
            report.duplicate_envelopes += duplicates
            raw.extend(envelopes)
        windows, buffer, start_cutoff, failed_locator = {}, [], None, None
        # A cutoff followed by discovery starts an acquisition; the next successful
        # cutoff closes it. Failed attempts are counted before looking at their bodies.
        for index, envelope in enumerate(raw):
            report.envelopes += 1
            if envelope.envelope.http_status != 200:
                key = str(envelope.envelope.http_status)
                report.non_200[key] = report.non_200.get(key, 0) + 1
                if buffer:
                    failed_locator = envelope.locator
                continue
            path = endpoint(envelope)
            if path == "/historical/cutoff":
                cutoff = body_json(envelope)["market_settled_ts"]
                next_path = endpoint(raw[index + 1]) if index + 1 < len(raw) else None
                if next_path == "/series":
                    if buffer:
                        raise StoreError(
                            f"{buffer[0].locator}: incomplete acquisition before next run"
                        )
                    buffer, start_cutoff = [envelope], cutoff
                    failed_locator = None
                else:
                    if not buffer:
                        raise StoreError(f"{envelope.locator}: cutoff without acquisition start")
                    if cutoff != start_cutoff:
                        raise StoreError(
                            f"{envelope.locator}: historical cutoff moved; reconciliation required"
                        )
                    if failed_locator:
                        raise StoreError(
                            f"{failed_locator}: HTTP failure left acquisition incomplete"
                        )
                    buffer.append(envelope)
                    publish_run(buffer, store, configured, listed, report, windows)
                    buffer, start_cutoff = [], None
            else:
                if not buffer:
                    raise StoreError(f"{envelope.locator}: missing acquisition start cutoff")
                buffer.append(envelope)
        if buffer:
            # Validate chains first so an absent final page has a precise locator.
            parse_run(buffer, configured, listed, report, windows)
            raise StoreError(f"{buffer[0].locator}: incomplete acquisition; final cutoff missing")
    except (ValueError, OSError, KeyError, TypeError) as exc:
        report.errors.append(str(exc))
        raise
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--store", type=Path, default=Path("data/kalshi"))
    parser.add_argument("--config", type=Path, default=Path("config/nws_stations.csv"))
    parser.add_argument("--exceptions", type=Path, default=Path("config/settlement_exceptions.csv"))
    parser.add_argument("--report", type=Path, default=Path("state/settlement-parse-report.json"))
    args = parser.parse_args()
    report = SettlementReport()
    try:
        parse(
            args.inputs,
            Store(args.store),
            config=args.config,
            exceptions=args.exceptions,
            report=report,
        )
    finally:
        atomic_json(args.report, asdict(report))
    print(json.dumps(asdict(report), sort_keys=True))


if __name__ == "__main__":
    main()
