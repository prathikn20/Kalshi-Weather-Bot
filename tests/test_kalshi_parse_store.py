import copy
import gzip
import hashlib
import io
import json
import tarfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import zstandard
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import kwb.store as storage
from kwb.parse.kalshi import parse
from kwb.store import MODELS, Store, configured_series, read_envelopes

FIXTURES = Path(__file__).parent / "fixtures/kalshi"
MARKETS = FIXTURES / "2026-11-10T220044.749404Z.json.gz"
BATCH = FIXTURES / "2026-11-10T220056.444920Z.json.gz"
SINGLE = FIXTURES / "2026-10-07T143102.368711Z.json.gz"
LATER = datetime(2026, 11, 10, 23, tzinfo=UTC)
SERIES = "KXHIGHLAX"


def envelope(path):
    return json.loads(gzip.decompress(path.read_bytes()))


def payload(path):
    return json.loads(envelope(path)["body"])


def write_envelope(directory, value, *, at=None):
    value = copy.deepcopy(value)
    if at is not None:
        value["received_at"] = at.isoformat(timespec="microseconds").replace("+00:00", "Z")
        value["requested_at"] = (
            (at - timedelta(milliseconds=100)).isoformat().replace("+00:00", "Z")
        )
    received = datetime.fromisoformat(value["received_at"])
    name = received.strftime("%Y-%m-%dT%H%M%S.%fZ.json.gz")
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_bytes(gzip.compress(json.dumps(value, ensure_ascii=False).encode(), mtime=0))
    return path


def change_body(path, directory, change, *, at=None):
    value = envelope(path)
    body = json.loads(value["body"])
    change(body)
    value["body"] = json.dumps(body, ensure_ascii=False)
    return write_envelope(directory, value, at=at)


def archive(path, sources):
    path.parent.mkdir(parents=True, exist_ok=True)
    with (
        path.open("wb") as raw,
        zstandard.ZstdCompressor(write_checksum=True).stream_writer(raw) as output,
        tarfile.open(fileobj=output, mode="w|") as tar,
    ):
        for source in sources:
            data = gzip.decompress(source.read_bytes())
            info = tarfile.TarInfo(source.name.removesuffix(".gz"))
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return path


def contents(store):
    manifest = store._manifest()
    result = {}
    for name in MODELS:
        files = store._files(manifest, name, LATER)
        result[name] = (
            store._query(
                "SELECT * FROM read_parquet(?, hive_partitioning=false) ORDER BY ALL", [files]
            )
            if files
            else []
        )
    return result


def schema_types(store):
    manifest = store._manifest()
    return {
        name: store._query(
            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)",
            [store._files(manifest, name, LATER)],
        )
        for name in MODELS
    }


@pytest.fixture
def two_observations(tmp_path):
    market = envelope(MARKETS)
    book = envelope(BATCH)
    first = datetime(2026, 11, 10, 22, 1, tzinfo=UTC)
    second = first + timedelta(seconds=1)
    files = []
    for at, price in ((first, "0.1234"), (second, "0.7777")):
        body = json.loads(market["body"])
        for row in body["markets"]:
            row["yes_bid_dollars"] = price
        market["body"] = json.dumps(body)
        files.append(write_envelope(tmp_path / "input", market, at=at))
        files.append(write_envelope(tmp_path / "input", book, at=at + timedelta(microseconds=1)))
    store = Store(tmp_path / "store")
    parse(files, store)
    ticker = next(
        book["ticker"]
        for book in json.loads(book["body"])["orderbooks"]
        if book["ticker"].startswith("KXHIGHMIA-")
    )
    return store, first, second, ticker


def test_every_parsed_row_uses_its_envelopes_received_at_as_utc_available_at(tmp_path):
    sources = [MARKETS, BATCH, SINGLE]
    store = Store(tmp_path / "store")
    parse(sources, store)
    expected = {raw.source_id: raw.envelope.received_at for raw in read_envelopes(sources)[0]}
    for rows in contents(store).values():
        assert rows
        for row in rows:
            assert row["available_at"] == expected[row["source_id"]]
            assert row["available_at"].tzinfo is not None
            assert row["available_at"].utcoffset() == timedelta(0)
            assert row["available_date"] == row["available_at"].date()
    source = envelope(MARKETS)
    assert datetime.fromisoformat(
        payload(MARKETS)["markets"][0]["updated_time"]
    ) != datetime.fromisoformat(source["received_at"])


@settings(
    max_examples=18, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(
    offset=st.one_of(
        st.sampled_from([-1, 0, 1, 999999, 1000000, 1000001, 1000002]),
        st.integers(-2_000_000, 3_000_000),
    )
)
def test_no_store_accessor_returns_rows_available_after_as_of(two_observations, offset):
    store, first, _, ticker = two_observations
    as_of = first + timedelta(microseconds=offset)
    latest = store.get_market_snapshots(SERIES, as_of)
    history = store.get_market_history(SERIES, first - timedelta(seconds=3), as_of)
    assert all(row.available_at <= as_of for row in latest + history)
    book = store.get_orderbook(ticker, as_of)
    if book:
        assert book.header.available_at <= as_of
        assert all(row.available_at <= as_of for row in book.levels)


def test_rows_are_visible_exactly_at_their_availability_time(two_observations):
    store, first, second, ticker = two_observations
    assert store.get_market_snapshots(SERIES, first - timedelta(microseconds=1)) == []
    assert len(store.get_market_snapshots(SERIES, first)) == 6
    assert {
        row.yes_bid_dollars
        for row in store.get_market_snapshots(SERIES, second - timedelta(microseconds=1))
    } == {"0.1234"}
    assert {row.yes_bid_dollars for row in store.get_market_snapshots(SERIES, second)} == {"0.7777"}
    assert store.get_orderbook(ticker, first) is None
    assert store.get_orderbook(
        ticker, first + timedelta(microseconds=1)
    ).header.available_at == first + timedelta(microseconds=1)
    assert len(store.get_market_history(SERIES, first, second)) == 12
    assert len(store.get_market_history(SERIES, second, second)) == 6


def test_latest_snapshots_select_one_row_per_ticker_after_applying_the_cutoff(two_observations):
    store, first, second, _ = two_observations
    latest = store.get_market_snapshots(SERIES, first)
    assert len(latest) == len({row.ticker for row in latest}) == 6
    assert {row.available_at for row in latest} == {first}
    assert {row.available_at for row in store.get_market_snapshots(SERIES, second)} == {second}
    assert {row.available_at for row in store.get_market_history(SERIES, first, second)} == {
        first,
        second,
    }


def test_all_raw_packagings_produce_identical_typed_rows(tmp_path):
    full = archive(tmp_path / "full/2026-11-10T22.tar.zst", [MARKETS, BATCH])
    base = archive(tmp_path / "split/2026-11-10T22.tar.zst", [MARKETS])
    late = archive(tmp_path / "split/2026-11-10T22.late-1.tar.zst", [BATCH])
    expected = None
    for index, paths in enumerate(
        ([MARKETS, BATCH], [full], [late, base], [MARKETS, full, late, BATCH])
    ):
        store = Store(tmp_path / f"store-{index}")
        parse(list(paths), store)
        actual = contents(store)
        assert all(actual.values())
        if expected is None:
            expected = actual, schema_types(store)
        assert (actual, schema_types(store)) == expected


def test_rerunning_the_same_inputs_preserves_the_schema_and_exact_row_multiset(tmp_path):
    store = Store(tmp_path / "store")
    parse([MARKETS], store)
    parse([BATCH], store)  # A supplement-only rerun must retain earlier sources.
    before = contents(store)
    before_schema = schema_types(store)
    manifest = (store.root / "manifest.json").read_bytes()
    parse([BATCH, MARKETS], store)
    assert contents(store) == before
    assert schema_types(store) == before_schema
    assert (store.root / "manifest.json").read_bytes() == manifest
    assert all(row["series_ticker"] in configured_series() for row in before["market_snapshots"])
    for name, model in MODELS.items():
        assert list(before[name][0]) == list(model.model_fields)


@pytest.mark.parametrize(
    "mutation", ["missing", "wrong_type", "unknown", "null", "naive_time", "nanoseconds"]
)
def test_invalid_required_fields_raise_with_the_envelope_locator(tmp_path, mutation):
    value = envelope(MARKETS)
    body = json.loads(value["body"])
    market = body["markets"][0]
    if mutation == "missing":
        del market["volume_fp"]
    elif mutation == "wrong_type":
        market["volume_fp"] = 1.2
    elif mutation == "unknown":
        market["unexpected_price"] = "0.12"
    elif mutation == "null":
        market["yes_bid_dollars"] = None
    elif mutation == "naive_time":
        market["updated_time"] = "2026-11-10T22:00:00"
    else:
        market["updated_time"] = "2026-11-10T22:00:00.1234567Z"
    value["body"] = json.dumps(body)
    bad = write_envelope(tmp_path / "raw", value)
    store = Store(tmp_path / "store")
    with pytest.raises(ValueError, match=bad.name):
        parse([bad], store)
    assert not (store.root / "manifest.json").exists()


def test_non_200_envelopes_are_counted_without_becoming_data(tmp_path):
    value = envelope(MARKETS)
    value["http_status"] = 429
    value["body"] = "This is intentionally not JSON"
    source = write_envelope(tmp_path / "raw", value)
    report = parse([source], Store(tmp_path / "store"))
    assert report.non_200 == {"429": 1}
    assert report.market_snapshots == report.orderbook_levels == report.orderbook_snapshots == 0


def test_duplicate_json_fields_raise_instead_of_selecting_one_value(tmp_path):
    value = envelope(MARKETS)
    value["body"] = json.dumps(payload(MARKETS)).replace(
        '"volume_fp": "', '"volume_fp": "1.00", "volume_fp": "', 1
    )
    source = write_envelope(tmp_path / "raw", value)
    with pytest.raises(ValueError, match="Duplicate JSON field"):
        parse([source], Store(tmp_path / "store"))


@pytest.mark.parametrize("mutation", [None, "missing_market", "gap", "overlap", "width", "tail"])
def test_every_event_forms_the_required_six_market_integer_fahrenheit_partition(tmp_path, mutation):
    body = payload(MARKETS)
    if mutation:
        if mutation == "missing_market":
            body["markets"].pop()
        else:
            middle = next(row for row in body["markets"] if row["strike_type"] == "between")
            if mutation in {"gap", "overlap"}:
                delta = 2 if mutation == "gap" else -2
                middle["floor_strike"] += delta
                middle["cap_strike"] += delta
            elif mutation == "width":
                middle["cap_strike"] += 1
            else:
                middle = next(row for row in body["markets"] if row["strike_type"] == "less")
                middle["cap_strike"] -= 1
            if middle["strike_type"] == "between":
                middle["rules_primary"] = (
                    f"If the maximum temperature is between {middle['floor_strike']}-"
                    f"{middle['cap_strike']}° fahrenheit, then Yes."
                )
            else:
                middle["rules_primary"] = (
                    f"If the maximum temperature is less than {middle['cap_strike']}° "
                    "fahrenheit, then Yes."
                )
    value = envelope(MARKETS)
    value["body"] = json.dumps(body)
    source = write_envelope(tmp_path / "raw", value)
    store = Store(tmp_path / "store")
    if mutation:
        with pytest.raises(ValueError, match="partition|six"):
            parse([source], store)
    else:
        assert parse([source], store).market_snapshots == 6


def test_fixed_point_fields_round_trip_exactly_through_storage_and_accessors(tmp_path):
    value = envelope(MARKETS)
    body = json.loads(value["body"])
    for market in body["markets"]:
        market["volume_fp"] = "90071992547409.93"
        market["yes_bid_dollars"] = "0.1234"
    value["body"] = json.dumps(body)
    source = write_envelope(tmp_path / "raw", value)
    store = Store(tmp_path / "store")
    parse([source, BATCH], store)
    rows = store.get_market_snapshots(SERIES, LATER)
    assert {row.volume_fp for row in rows} == {"90071992547409.93"}
    expected = {row["ticker"]: row for row in body["markets"]}
    for row in rows:
        for key, served in expected[row.ticker].items():
            if key.endswith(("_dollars", "_fp")):
                assert getattr(row, key) == served
    for book in payload(BATCH)["orderbooks"]:
        if book["ticker"].split("-")[0] not in configured_series():
            continue
        stored = store.get_orderbook(book["ticker"], LATER)
        for side in ("yes", "no"):
            assert [
                (row.price_dollars, row.quantity_fp) for row in stored.levels if row.side == side
            ] == [tuple(pair) for pair in book["orderbook_fp"][side + "_dollars"]]


@pytest.mark.parametrize("mutation", [None, "missing", "extra", "duplicate"])
def test_each_orderbook_chunk_returns_every_requested_ticker_exactly_once(tmp_path, mutation):
    value = envelope(BATCH)
    body = json.loads(value["body"])
    if mutation == "missing":
        body["orderbooks"].pop()
    elif mutation == "extra":
        extra = copy.deepcopy(body["orderbooks"][0])
        extra["ticker"] = "UNCONFIGURED-EXTRA"
        body["orderbooks"].append(extra)
    elif mutation == "duplicate":
        body["orderbooks"][-1] = copy.deepcopy(body["orderbooks"][0])
    value["body"] = json.dumps(body)
    source = write_envelope(tmp_path / "raw", value)
    store = Store(tmp_path / "store")
    if mutation:
        with pytest.raises(ValueError, match="tickers differ"):
            parse([source], store)
    else:
        report = parse([source], store)
        expected = [
            book["ticker"]
            for book in body["orderbooks"]
            if book["ticker"].split("-")[0] in configured_series()
        ]
        assert {row["ticker"] for row in contents(store)["orderbook_snapshots"]} == set(expected)
        assert report.orderbook_snapshots + report.raw_only_books == len(body["orderbooks"])


def test_only_configured_series_are_parsed_and_other_series_remain_raw_only(tmp_path):
    value = envelope(MARKETS)
    value["request_params"]["series_ticker"] = "KXUNVERIFIED"
    value["body"] = json.dumps({"cursor": "", "markets": [{"unverified_format": True}]})
    source = write_envelope(tmp_path / "raw", value)
    store = Store(tmp_path / "store")
    report = parse([source, BATCH], store)
    assert report.raw_only_market_rows == 1
    assert report.raw_only_books > 0
    assert contents(store)["market_snapshots"] == []
    assert all(
        row["ticker"].split("-")[0] in configured_series()
        for row in contents(store)["orderbook_snapshots"]
    )


def test_an_empty_observed_book_replaces_an_older_nonempty_book(tmp_path):
    store = Store(tmp_path / "store")
    parse([BATCH], store)
    selected = next(
        book
        for book in payload(BATCH)["orderbooks"]
        if book["ticker"].split("-")[0] in configured_series()
    )
    ticker = selected["ticker"]
    assert store.get_orderbook(ticker, LATER).levels
    later = change_body(
        BATCH,
        tmp_path / "raw",
        lambda body: [
            book.update(orderbook_fp={"yes_dollars": [], "no_dollars": []})
            for book in body["orderbooks"]
        ],
        at=LATER - timedelta(minutes=1),
    )
    parse([later], store)
    book = store.get_orderbook(ticker, LATER)
    assert book is not None and book.levels == []
    assert book.header.yes_level_count == book.header.no_level_count == 0
    assert store.get_orderbook("KXHIGHNY-NEVER-SEEN", LATER) is None


def test_conflicting_envelope_bytes_raise_within_a_run_and_on_a_rerun(tmp_path):
    changed = change_body(
        MARKETS, tmp_path / "changed", lambda body: body["markets"][0].update(volume_fp="123.00")
    )
    store = Store(tmp_path / "store")
    with pytest.raises(ValueError, match="onflicting"):
        parse([MARKETS, changed], store)
    parse([MARKETS], store)
    before = contents(store)
    with pytest.raises(ValueError, match="conflicting"):
        parse([changed], store)
    assert contents(store) == before


def test_conflicting_simultaneous_observations_raise_instead_of_selecting_an_arbitrary_row(
    tmp_path,
):
    store = Store(tmp_path / "store")
    parse([MARKETS], store)
    row = store.get_market_snapshots(SERIES, LATER)[0]
    altered = row.model_copy(
        update={
            "source_id": row.source_id + "other",
            "source_sha256": "f" * 64,
            "volume_fp": "999.00",
        }
    )
    with pytest.raises(ValueError, match="conflicting simultaneous"):
        store.publish_hour(
            "2026-11-10T22",
            {name: [altered] if name == "market_snapshots" else [] for name in MODELS},
            {altered.source_id: altered.source_sha256},
        )


def test_failed_manifest_publication_leaves_the_previous_generation_visible(tmp_path, monkeypatch):
    store = Store(tmp_path / "store")
    parse([MARKETS], store)
    before = contents(store)
    later = change_body(
        MARKETS,
        tmp_path / "raw",
        lambda body: body["markets"][0].update(volume_fp="123.00"),
        at=LATER - timedelta(minutes=1),
    )

    original = storage.atomic_json

    def interrupted(path, value):
        if path.name == "manifest.json":
            raise OSError("simulated crash before manifest rename")
        original(path, value)

    monkeypatch.setattr(storage, "atomic_json", interrupted)
    with pytest.raises(OSError, match="simulated crash"):
        parse([later], store)
    assert contents(store) == before


def test_a_book_read_pins_one_manifest_for_both_its_header_and_its_levels(tmp_path, monkeypatch):
    store = Store(tmp_path / "store")
    parse([BATCH], store)
    ticker = next(
        book["ticker"]
        for book in payload(BATCH)["orderbooks"]
        if book["ticker"].split("-")[0] in configured_series()
    )
    original_query = store._query
    original_book = store.get_orderbook(ticker, LATER)
    later = change_body(
        BATCH,
        tmp_path / "raw",
        lambda body: [
            book.update(orderbook_fp={"yes_dollars": [], "no_dollars": []})
            for book in body["orderbooks"]
        ],
        at=LATER - timedelta(minutes=1),
    )
    replaced = False

    def replace_between_queries(sql, params):
        nonlocal replaced
        result = original_query(sql, params)
        if not replaced:
            replaced = True
            parse([later], store)
        return result

    monkeypatch.setattr(store, "_query", replace_between_queries)
    assert store.get_orderbook(ticker, LATER) == original_book
    assert store.get_orderbook(ticker, LATER).levels == []


def test_pagination_can_cross_hours_without_changing_any_rows_availability(tmp_path):
    value = envelope(MARKETS)
    body = json.loads(value["body"])
    first = copy.deepcopy(value)
    first["body"] = json.dumps({"markets": body["markets"][:3], "cursor": "next"})
    second = copy.deepcopy(value)
    second["request_params"]["cursor"] = "next"
    second["body"] = json.dumps({"markets": body["markets"][3:], "cursor": ""})
    at = datetime(2026, 11, 10, 22, 59, 59, 999999, tzinfo=UTC)
    a = write_envelope(tmp_path / "raw", first, at=at)
    b = write_envelope(tmp_path / "raw", second, at=at + timedelta(microseconds=1))
    store = Store(tmp_path / "store")
    assert parse([a, b], store).published_hours == 2
    assert len(store.get_market_snapshots(SERIES, at)) == 3
    assert len(store.get_market_snapshots(SERIES, at + timedelta(microseconds=1))) == 6
    assert {
        row.available_at
        for row in store.get_market_history(SERIES, at, at + timedelta(microseconds=1))
    } == {at, at + timedelta(microseconds=1)}
    with pytest.raises(ValueError, match="incomplete markets pagination"):
        parse([a], Store(tmp_path / "incomplete"))


@pytest.mark.parametrize("retained", [100, -1, -4])
def test_truncated_archives_raise_with_the_archive_path_and_publish_nothing(tmp_path, retained):
    path = archive(tmp_path / "2026-11-10T22.tar.zst", [MARKETS, BATCH])
    path.write_bytes(path.read_bytes()[:retained])
    store = Store(tmp_path / "store")
    with pytest.raises(ValueError, match=path.name):
        parse([path], store)
    assert not (store.root / "manifest.json").exists()


def test_query_boundaries_reject_naive_times_and_reversed_history_windows(tmp_path):
    store = Store(tmp_path)
    naive = datetime(2026, 11, 10, tzinfo=UTC).replace(tzinfo=None)
    for call in (
        lambda: store.get_market_snapshots(SERIES, naive),
        lambda: store.get_market_history(SERIES, naive, LATER),
        lambda: store.get_orderbook("ticker", naive),
    ):
        with pytest.raises(ValueError, match="timezone-aware"):
            call()
    with pytest.raises(ValueError, match="start"):
        store.get_market_history(SERIES, LATER, LATER - timedelta(microseconds=1))


def test_fixture_provenance_matches_the_committed_response_bytes():
    provenance = json.loads((FIXTURES / "provenance.json").read_text())
    for entry in provenance:
        path = FIXTURES / entry["fixture"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["fixture_file_sha256"]
        assert (
            hashlib.sha256(gzip.decompress(path.read_bytes())).hexdigest()
            == entry["fixture_envelope_sha256"]
        )
        assert len(entry["source_envelope_sha256"]) == 64
