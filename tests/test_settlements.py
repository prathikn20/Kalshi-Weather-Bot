import copy
import gzip
import hashlib
import io
import json
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from test_download import Blob
from test_kalshi_parse_store import BATCH, MARKETS, archive, envelope, write_envelope
from test_sync import FakeBucket

from kwb.collectors.settlements import (
    WEEK,
    CollectionError,
    SettlementCollector,
    whole_request_deadline,
)
from kwb.ops import compact, download, sync
from kwb.parse.kalshi import parse as parse_markets
from kwb.parse.settlements import SettlementReport, parse
from kwb.schemas import Settlement
from kwb.store import Store, read_envelopes

FIXTURES = Path(__file__).parent / "fixtures/kalshi/settlements"
START = datetime(2026, 11, 14, 18, tzinfo=UTC)
SERIES = "KXHIGHLAX"


def fixture(name):
    return envelope(FIXTURES / (name + ".json.gz"))


def body(name):
    return json.loads(fixture(name)["body"])


def acquisition(tmp_path, *, at=START, names=("live",), edits=None):
    values = [fixture("cutoff-start"), fixture("discovery")]
    for name in names:
        value = fixture(name)
        if edits:
            edits(name, value)
        values.append(value)
    values.append(fixture("cutoff-end"))
    return [
        write_envelope(tmp_path, value, at=at + timedelta(seconds=i))
        for i, value in enumerate(values)
    ]


def all_rows(store, as_of=START + timedelta(days=30)):
    files = store._files(store._manifest(), "settlements", as_of)
    return store._query("SELECT * FROM read_parquet(?) ORDER BY ALL", [files]) if files else []


def test_every_settlement_uses_its_envelopes_received_at_and_is_invisible_before_fetch(tmp_path):
    inputs = acquisition(tmp_path / "raw", names=("historical-page-1", "historical-page-2"))
    store = Store(tmp_path / "store")
    parse(inputs, store)
    expected = {
        raw.source_id: raw.envelope.received_at
        for raw in read_envelopes(inputs, source="kalshi_settlements")[0]
    }
    rows = all_rows(store)
    assert len(rows) == 6
    assert all(row["available_at"] == expected[row["source_id"]] for row in rows)
    assert all(row["available_date"] == row["available_at"].date() for row in rows)
    assert all(row["available_at"].utcoffset() == timedelta(0) for row in rows)
    assert all(row["available_at"] - row["settlement_ts"] > timedelta(days=500) for row in rows)
    assert store.get_settlements("KXHIGHMIA", START) == []
    assert len(store.get_settlements("KXHIGHMIA", START + timedelta(seconds=2))) == 3
    assert len(store.get_settlements("KXHIGHMIA", START + timedelta(seconds=3))) == 6


@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_no_accessor_returns_future_rows_at_microsecond_boundaries(tmp_path, offset):
    store = Store(tmp_path / "store")
    # Keep the real open markets' date so their served close times remain future.
    first = datetime(2026, 11, 10, 22, 1, tzinfo=UTC)
    market = write_envelope(tmp_path / "market", envelope(MARKETS), at=first)
    book_at = first + timedelta(microseconds=1)
    book = write_envelope(tmp_path / "book", envelope(BATCH), at=book_at)
    parse_markets([market, book], store)
    files = acquisition(tmp_path / "settlements", at=first - timedelta(seconds=2))
    parse(files, store)
    cutoff = first + timedelta(microseconds=offset)
    latest = store.get_market_snapshots(SERIES, cutoff)
    history = store.get_market_history(SERIES, first - timedelta(seconds=5), cutoff)
    settlements = store.get_settlements(SERIES, cutoff)
    assert len(latest) == len(history) == len(settlements) == (0 if offset < 0 else 6)
    assert all(row.available_at <= cutoff for row in latest + history + settlements)
    batch = json.loads(envelope(BATCH)["body"])["orderbooks"]
    ticker = next(b["ticker"] for b in batch if b["ticker"].startswith("KXHIGHMIA-"))
    book_cutoff = book_at + timedelta(microseconds=offset)
    snapshot = store.get_orderbook(ticker, book_cutoff)
    assert (snapshot is None) == (offset < 0)
    if snapshot:
        assert snapshot.header.available_at <= book_cutoff
        assert all(row.available_at <= book_cutoff for row in snapshot.levels)


def test_each_unlisted_finalized_event_has_six_markets_and_exactly_one_yes_or_raises(tmp_path):
    event = body("live")["markets"][0]["event_ticker"]
    for mutation in ("zero", "two", "missing", "scalar"):

        def edit(name, value, mutation=mutation):
            payload = json.loads(value["body"])
            rows = payload["markets"]
            if mutation == "missing":
                rows.pop()
            elif mutation == "scalar":
                rows[0]["result"] = "scalar"
                rows[0]["settlement_value_dollars"] = "0.5000"
            else:
                for index, row in enumerate(rows):
                    row["result"] = "yes" if mutation == "two" and index < 2 else "no"
                    row["settlement_value_dollars"] = (
                        "1.0000" if row["result"] == "yes" else "0.0000"
                    )
            value["body"] = json.dumps(payload)

        inputs = acquisition(tmp_path / mutation, edits=edit)
        with pytest.raises(ValueError, match=event):
            parse(inputs, Store(tmp_path / (mutation + "-store")))


def test_a_listed_scalar_event_is_stored_without_any_label_and_unlisted_it_raises(tmp_path):
    inputs = acquisition(tmp_path / "raw", names=("scalar",))
    store = Store(tmp_path / "store")
    report = parse(inputs, store)
    rows = store.get_settlements("KXHIGHMIA", START + timedelta(hours=1))
    assert len(rows) == 6
    assert {row.label_status for row in rows} == {"exception"}
    assert all(row.label is None for row in rows)
    assert {row.settlement_value_dollars for row in rows} == {"0.0000", "0.0100", "0.9900"}
    assert report.exceptions == ["KXHIGHMIA-26APR11"]
    bad = rows[0].model_dump()
    bad["label"] = "no"
    with pytest.raises(ValueError, match="no label"):
        Settlement.model_validate(bad)
    empty = tmp_path / "no-exceptions.csv"
    empty.write_text("event_ticker,reason,evidence\n")
    with pytest.raises(ValueError, match="KXHIGHMIA-26APR11"):
        parse(inputs, Store(tmp_path / "unlisted"), exceptions=empty)


def test_an_unfinished_pagination_chain_never_becomes_successful_coverage(tmp_path):
    def complete_event_with_more_pages(name, value):
        payload = json.loads(value["body"])
        # The first page contains a whole event, but promises another page/event.
        # Event validation alone must not mistake this truncated chain for completion.
        payload["markets"] += body("historical-page-2")["markets"]
        value["body"] = json.dumps(payload)

    inputs = acquisition(
        tmp_path / "raw", names=("historical-page-1",), edits=complete_event_with_more_pages
    )
    store = Store(tmp_path / "store")
    with pytest.raises(ValueError, match="incomplete settlement pagination"):
        parse(inputs, store)
    assert not (store.root / "manifest.json").exists()
    inputs = acquisition(tmp_path / "second", names=("historical-page-2",))
    with pytest.raises(ValueError, match="Missing first pagination"):
        parse(inputs, store)


def test_a_gap_between_successful_fetch_windows_raises_with_its_locator(tmp_path):
    first = acquisition(tmp_path / "first")
    upper = fixture("live")["request_params"]["max_settled_ts"]

    def gap(name, value):
        value["request_params"]["min_settled_ts"] = upper + 1
        value["request_params"]["max_settled_ts"] = upper + 100

    second = acquisition(tmp_path / "second", at=START + timedelta(days=1), edits=gap)
    report = SettlementReport()
    with pytest.raises(ValueError, match="window gap"):
        parse(first + second, Store(tmp_path / "store"), report=report)
    assert report.coverage_gaps[0]["start"] == upper
    assert "second" in report.coverage_gaps[0]["locator"]


def test_repeat_fetches_remain_distinct_and_changed_settlements_are_reported_as_revisions(tmp_path):
    first = acquisition(tmp_path / "first")
    second = acquisition(tmp_path / "second", at=START + timedelta(days=1))

    def revision(name, value):
        payload = json.loads(value["body"])
        for row in payload["markets"]:
            row["expiration_value"] = "SYNTHETIC REVISION"
        value["body"] = json.dumps(payload)

    third = acquisition(tmp_path / "third", at=START + timedelta(days=2), edits=revision)
    store = Store(tmp_path / "store")
    report = parse(first + second + third, store)
    assert len(all_rows(store)) == 18
    assert len(report.revisions) == 6
    assert all(set(r["changes"]) == {"expiration_value"} for r in report.revisions)
    assert all(r["previous_source_id"] != r["source_id"] for r in report.revisions)
    assert {
        r.expiration_value
        for r in store.get_settlements(SERIES, START + timedelta(days=1, hours=1))
    } != {"SYNTHETIC REVISION"}
    assert {
        r.expiration_value for r in store.get_settlements(SERIES, START + timedelta(days=3))
    } == {"SYNTHETIC REVISION"}
    before = all_rows(store)
    parse(first + second + third, store)
    assert all_rows(store) == before
    # Publication of the old parser must retain all settlement observations.
    parse_markets([MARKETS], store)
    assert all_rows(store) == before


@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_market_snapshots_exclude_close_time_at_or_before_as_of_without_resurrection(
    tmp_path, offset
):
    value = envelope(MARKETS)
    cutoff = START + timedelta(microseconds=offset)

    def at_close(close):
        copied = copy.deepcopy(value)
        payload = json.loads(copied["body"])
        for row in payload["markets"]:
            row["close_time"] = close.isoformat().replace("+00:00", "Z")
        copied["body"] = json.dumps(payload)
        return copied

    early = write_envelope(
        tmp_path / "raw", at_close(START + timedelta(days=10)), at=START - timedelta(hours=2)
    )
    later = write_envelope(tmp_path / "raw", at_close(START), at=START - timedelta(hours=1))
    future = write_envelope(
        tmp_path / "raw", at_close(START + timedelta(days=1)), at=START + timedelta(hours=1)
    )
    store = Store(tmp_path / "store")
    parse_markets([early, later, future], store)
    rows = store.get_market_snapshots(SERIES, cutoff)
    assert len(rows) == (6 if offset < 0 else 0)
    assert all(row.close_time > cutoff and row.available_at <= cutoff for row in rows)
    assert len(store.get_market_snapshots(SERIES, START + timedelta(hours=1))) == 6
    assert store.get_market_snapshots(SERIES, START + timedelta(days=20)) == []


@pytest.mark.parametrize("mutation", ["missing", "unknown", "naive", "payout"])
def test_invalid_settlement_fields_raise_with_the_envelope_locator(tmp_path, mutation):
    def edit(name, value):
        payload = json.loads(value["body"])
        row = payload["markets"][0]
        if mutation == "missing":
            del row["settlement_ts"]
        elif mutation == "unknown":
            row["surprise"] = True
        elif mutation == "naive":
            row["settlement_ts"] = "2026-11-12T00:00:00"
        else:
            row["settlement_value_dollars"] = "0.1234"
        value["body"] = json.dumps(payload)

    inputs = acquisition(tmp_path / "raw", edits=edit)
    with pytest.raises(ValueError, match=inputs[2].name):
        parse(inputs, Store(tmp_path / "store"))


def test_non_200_settlement_envelopes_are_counted_without_parsing_their_bodies(tmp_path):
    value = fixture("live")
    value.update(http_status=429, body="INTENTIONALLY INVALID JSON")
    source = write_envelope(tmp_path / "raw", value, at=START)
    report = parse([source], Store(tmp_path / "store"))
    assert report.non_200 == {"429": 1}
    assert report.settlements == 0


def test_settlement_raw_packaging_reruns_and_namespaces_preserve_observations(tmp_path):
    inputs = acquisition(tmp_path / "raw", names=("live", "scalar"))
    packed = archive(tmp_path / "packed/2026-11-14T18.tar.zst", inputs[:3])
    late = archive(tmp_path / "packed/2026-11-14T18.late-1.tar.zst", inputs[3:])
    stores = [Store(tmp_path / name) for name in ("loose", "packed")]
    parse(inputs, stores[0])
    parse([packed, late] + inputs, stores[1])
    assert all_rows(stores[0]) == all_rows(stores[1])
    a = read_envelopes([inputs[2]])[0][0]
    b = read_envelopes([inputs[2]], source="kalshi_settlements")[0][0]
    assert a.sha256 == b.sha256 and a.source_id != b.source_id


def test_legacy_three_table_generations_are_readable_but_missing_new_shards_raise(tmp_path):
    store = Store(tmp_path / "store")
    parse_markets([MARKETS], store)
    path = store.root / "manifest.json"
    value = json.loads(path.read_text())
    item = next(iter(value["hours"].values()))
    del item["files"]["settlements"]
    del item["tables_version"]
    path.write_text(json.dumps(value))
    assert store.get_settlements(SERIES, START) == []
    assert store.get_market_snapshots(SERIES, datetime(2026, 11, 10, 23, tzinfo=UTC))
    item["tables_version"] = 2
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="invalid table inventory"):
        store.get_settlements(SERIES, START)


def test_compaction_sync_and_generation_verified_download_include_the_new_raw_source(tmp_path):
    root = tmp_path / "raw"
    sources = acquisition(root / "kalshi_settlements/2026-11-14")
    original = {p.name: gzip.decompress(p.read_bytes()) for p in sources}
    compact.compact(root, START + timedelta(hours=2))
    remote = FakeBucket()
    result = sync.sync(root, tmp_path / "state/sync.sqlite", remote)
    assert result.uploaded == 1
    name, data = next(iter(remote.objects.items()))
    assert name.startswith("raw/kalshi_settlements/")
    blob = Blob(name=name, payload=data)

    class Reader:
        def list_blobs(self, bucket, *, prefix):
            assert prefix == "raw/kalshi_settlements/"
            return iter([blob])

    destination = tmp_path / "download"
    download.download(
        destination, tmp_path / "state/download.json", client=Reader(), source="kalshi_settlements"
    )
    restored = read_envelopes([destination / name], source="kalshi_settlements")[0]
    assert len(restored) == len(original)
    assert {raw.source_id.rsplit("/", 1)[-1] + ".gz": raw.sha256 for raw in restored} == {
        name: hashlib.sha256(data).hexdigest() for name, data in original.items()
    }
    assert blob.calls[0]["if_generation_match"] == blob.generation


class Clock:
    def __init__(self):
        self.seconds = 0.0

    def now(self):
        return START + timedelta(seconds=self.seconds)

    def sleep(self, seconds):
        self.seconds += seconds

    def monotonic(self):
        return self.seconds


class Response(io.BytesIO):
    def __init__(self, payload, status=200):
        super().__init__(payload if isinstance(payload, bytes) else json.dumps(payload).encode())
        self.status = status


def collector(tmp_path, *, clock=None, status=200):
    clock = clock or Clock()
    calls = []
    config = tmp_path / "config.csv"
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text("series_ticker\nKXHIGHLAX\n")

    def open_url(request, timeout):
        url = urlsplit(request.full_url)
        params = parse_qs(url.query)
        calls.append((clock.seconds, url.path, params))
        if status != 200:
            return Response(b"INVALID JSON", status)
        if url.path.endswith("/cutoff"):
            payload = body("cutoff-start")
        elif url.path.endswith("/series"):
            payload = body("discovery")
        else:
            payload = {"markets": [], "cursor": ""}
        return Response(payload)

    value = SettlementCollector(
        tmp_path / "raw/kalshi_settlements",
        tmp_path / "state/collector.sqlite",
        config=config,
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        open_url=open_url,
    )
    return value, clock, calls


def test_collection_spaces_every_request_start_by_ten_seconds_and_never_retries_429(tmp_path):
    value, _, calls = collector(tmp_path / "good")
    assert value.run()["complete"]
    assert len(calls) == 5  # cutoff, discovery, both discovered live series, cutoff
    assert all(b[0] - a[0] >= 10 for a, b in zip(calls, calls[1:], strict=False))
    bad, _, bad_calls = collector(tmp_path / "limited", status=429)
    with pytest.raises(CollectionError, match="HTTP 429"):
        bad.run()
    assert len(bad_calls) == 1
    assert bad.report["rate_limited"] == 1 and not bad.report["complete"]
    raw = list(bad.raw_root.rglob("*.json.gz"))
    assert len(raw) == 1 and envelope(raw[0])["body"] == "INVALID JSON"
    with sqlite3.connect(bad.state) as db:
        assert (
            db.execute("SELECT count(*) FROM series WHERE upper_ts IS NOT NULL").fetchone()[0] == 0
        )


def test_historical_collection_backfills_configured_series_then_rescans_them_weekly(tmp_path):
    first, clock, calls = collector(tmp_path)
    report = first.run(backfill=True, configured_only=True)
    assert report["series"] == report["historical_series"] == [SERIES]
    assert len([c for c in calls if c[1].endswith("/historical/markets")]) == 1
    clock.seconds += 86400
    daily, _, daily_calls = collector(tmp_path, clock=clock)
    assert daily.run()["historical_series"] == []
    assert not any(c[1].endswith("/historical/markets") for c in daily_calls)
    clock.seconds += WEEK
    weekly, _, weekly_calls = collector(tmp_path, clock=clock)
    assert weekly.run()["historical_series"] == [SERIES]
    historical = [c for c in weekly_calls if c[1].endswith("/historical/markets")]
    assert len(historical) == 1 and historical[0][2]["series_ticker"] == [SERIES]


def test_outage_gaps_are_reported_and_failed_runs_keep_the_previous_watermark(tmp_path):
    first, clock, _ = collector(tmp_path)
    first.run()
    with sqlite3.connect(first.state) as db:
        db.execute("UPDATE series SET upper_ts=?", (100,))
    clock.seconds += 86400
    second, _, calls = collector(tmp_path, clock=clock)
    second.run()
    assert second.report["gaps"] and all(g["start"] == 100 for g in second.report["gaps"])
    listings = [c for c in calls if c[1].endswith("/markets")]
    assert all(c[2]["min_settled_ts"] == ["99"] for c in listings)
    before = second.report["upper"]
    clock.seconds += 86400
    third, _, _ = collector(tmp_path, clock=clock, status=429)
    with pytest.raises(CollectionError):
        third.run()
    with sqlite3.connect(first.state) as db:
        assert {r[0] for r in db.execute("SELECT upper_ts FROM series")} == {before}


def test_whole_request_deadline_interrupts_a_response_that_never_finishes():
    started = time.monotonic()
    with (
        pytest.raises(TimeoutError, match="whole-request deadline"),
        whole_request_deadline(0.03),
    ):
        time.sleep(0.3)
    assert time.monotonic() - started < 0.25


def test_an_http_failure_cannot_be_mistaken_for_an_empty_successful_acquisition(tmp_path):
    inputs = acquisition(tmp_path / "raw")
    value = envelope(inputs[2])
    value.update(http_status=500, body="NOT JSON")
    write_envelope(inputs[2].parent, value, at=START + timedelta(seconds=2))
    report = SettlementReport()
    store = Store(tmp_path / "store")
    with pytest.raises(ValueError, match=inputs[2].name):
        parse(inputs, store, report=report)
    assert report.non_200 == {"500": 1}
    assert report.completed_runs == 0
    assert not (store.root / "manifest.json").exists()


def test_unconfigured_legacy_events_returned_by_kx_queries_remain_raw_only(tmp_path):
    def legacy(name, value):
        payload = json.loads(value["body"])
        payload["markets"].append(
            {"ticker": "HIGHNY-24NOV27-T83", "event_ticker": "HIGHNY-24NOV27", "legacy_shape": True}
        )
        value["body"] = json.dumps(payload)

    inputs = acquisition(tmp_path / "raw", edits=legacy)
    store = Store(tmp_path / "store")
    report = parse(inputs, store)
    assert report.raw_only_market_rows == 1
    assert len(all_rows(store)) == 6


def test_pagination_replays_and_missing_run_endings_raise_with_provenance(tmp_path):
    for mutation in ("replay", "cursor", "end"):

        def edit(name, value, mutation=mutation):
            if name == "historical-page-2":
                payload = json.loads(value["body"])
                if mutation == "replay":
                    payload["markets"] = body("historical-page-1")["markets"]
                elif mutation == "cursor":
                    payload["cursor"] = value["request_params"]["cursor"]
                value["body"] = json.dumps(payload)

        files = acquisition(
            tmp_path / mutation, names=("historical-page-1", "historical-page-2"), edits=edit
        )
        if mutation == "end":
            files.pop()
        with pytest.raises(ValueError, match="pagination|incomplete acquisition"):
            parse(files, Store(tmp_path / (mutation + "-store")))


def test_a_moving_historical_cutoff_is_reported_without_advancing_coverage(tmp_path):
    value, _, _ = collector(tmp_path)
    responses = iter([1789430400, 1789516800])
    value.cutoff = lambda: next(responses)
    with pytest.raises(CollectionError, match="cutoff moved"):
        value.run(backfill=True, configured_only=True)
    with sqlite3.connect(value.state) as db:
        assert all(
            row == (None, None) for row in db.execute("SELECT upper_ts,historical_at FROM series")
        )


def test_deployment_keeps_fixed_pacing_and_the_approved_resource_and_timer_limits():
    import configparser

    root = Path(__file__).parents[1]
    service = configparser.ConfigParser(interpolation=None)
    service.read(root / "deploy/kalshi-settlements.service")
    values = service["Service"]
    assert values["Nice"] == "19"
    assert values["CPUQuota"] == "5%"
    assert values["CPUWeight"] == values["IOWeight"] == "1"
    assert values["MemoryMax"] == "128M" and values["TimeoutStartSec"] == "2h"
    assert "--no-sync" in values["ExecStart"] and "--backfill" not in values["ExecStart"]
    timer = configparser.ConfigParser(interpolation=None)
    timer.read(root / "deploy/kalshi-settlements.timer")
    assert timer["Timer"]["OnCalendar"] == "*-*-* 18:23:00 UTC"
    assert timer["Timer"]["Persistent"] == "true"


@pytest.mark.parametrize("invalid", [False, True])
def test_chunked_raw_serialization_preserves_unicode_boundaries_and_undecodable_bytes(
    tmp_path, invalid
):
    import base64

    value, _, _ = collector(tmp_path)
    value.state.parent.mkdir(parents=True)
    payload = b"x" * (48 * 1024 - 1) + '°"\\\n'.encode() + b"z" * 65536
    if invalid:
        payload += b"\xff"
    source = value.archive("https://example.test", {}, 500, START, START, payload)
    saved = envelope(source)
    restored = base64.b64decode(saved["body"], validate=True) if invalid else saved["body"].encode()
    assert restored == payload
    assert saved.get("body_encoding") == ("base64" if invalid else None)
    before = source.read_bytes()
    with pytest.raises(FileExistsError):
        value.archive("https://example.test", {}, 500, START, START, b"different")
    assert source.read_bytes() == before


def test_rapid_process_restarts_keep_the_ten_second_request_spacing(tmp_path):
    first, clock, calls = collector(tmp_path)
    first.run()
    last = calls[-1][0]
    second, _, later = collector(tmp_path, clock=clock)
    second.run()
    assert later[0][0] - last >= 10


def test_changed_served_metadata_is_reported_without_claiming_a_label_revision(tmp_path):
    first = acquisition(tmp_path / "first")

    def update(name, value):
        payload = json.loads(value["body"])
        for market in payload["markets"]:
            market["updated_time"] = "2026-11-14T00:00:00Z"
        value["body"] = json.dumps(payload)

    second = acquisition(tmp_path / "second", at=START + timedelta(days=1), edits=update)
    store = Store(tmp_path / "store")
    report = parse(first + second, store)
    assert len(report.revisions) == 6
    assert all(
        set(r["changes"]) == {"updated_time"} and not r["label_revision"] for r in report.revisions
    )
    assert len(all_rows(store)) == 12


def test_a_real_negative_settled_quote_size_parses_with_every_label_field_intact(tmp_path):
    def isolate_event(name, value):
        # Reframe the saved real event as a complete chain; served fields are untouched.
        value["request_params"].pop("cursor", None)
        payload = json.loads(value["body"])
        payload["cursor"] = ""
        value["body"] = json.dumps(payload)

    inputs = acquisition(tmp_path / "raw", names=("negative-size",), edits=isolate_event)
    store = Store(tmp_path / "store")
    report = parse(inputs, store)
    expected = {row["ticker"]: row for row in body("negative-size")["markets"]}
    assert expected["KXHIGHCHI-26MAY13-B60.5"]["yes_bid_size_fp"] == "-276.00"
    rows = store.get_settlements("KXHIGHCHI", START + timedelta(hours=1))
    assert report.settlements == len(rows) == 6
    fields = (
        "result",
        "expiration_value",
        "settlement_value_dollars",
        "settlement_floor_dollars",
        "settlement_bounds_type",
        "strike_type",
        "floor_strike",
        "cap_strike",
        "rules_primary",
        "rules_secondary",
    )
    for row in rows:
        served = expected[row.ticker]
        assert all(getattr(row, key) == served.get(key) for key in fields)
        assert row.settlement_ts == datetime.fromisoformat(served["settlement_ts"])
        assert row.label_status == "labeled" and row.label == row.result
        assert row.available_at == START + timedelta(seconds=2)
    assert sum(row.label == "yes" for row in rows) == 1
    from test_settlement_quote_fields import QUOTE_FIELDS

    assert not (QUOTE_FIELDS & set(Settlement.model_fields))
    assert not any(QUOTE_FIELDS & set(row) for row in all_rows(store))
