import gzip
import io
import json
import logging
from datetime import UTC, datetime, timedelta
from http.client import IncompleteRead
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit

import pytest

from kwb.collectors import kalshi


class Clock:
    def __init__(self, origin: datetime | None = None) -> None:
        self.origin = origin or datetime(2026, 10, 7, 12, tzinfo=UTC)
        self.elapsed = 0.0
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.origin + timedelta(seconds=self.elapsed)

    def monotonic(self) -> float:
        return self.elapsed

    def sleep(self, seconds: float) -> None:
        assert seconds >= 0
        self.sleeps.append(seconds)
        self.elapsed += seconds


class Response(io.BytesIO):
    def __init__(self, body: bytes, status: int = 200) -> None:
        super().__init__(body)
        self.status = status


class TruncatedResponse(Response):
    def read(self, *args):
        raise IncompleteRead(b"partial", 20)


def series(ticker: str, title: str = "Highest temperature in NYC today?") -> dict:
    return {"ticker": ticker, "frequency": "daily", "title": title}


def markets(*tickers: str, cursor: str = "") -> dict:
    return {"markets": [{"ticker": ticker} for ticker in tickers], "cursor": cursor}


def books(params: dict) -> dict:
    return {
        "orderbooks": [
            {"ticker": ticker, "orderbook_fp": {"yes_dollars": [], "no_dollars": []}}
            for ticker in params["tickers"]
        ]
    }


def batched(calls: list) -> list[list[str]]:
    return [params["tickers"] for path, params, _ in calls if path == "/markets/orderbooks"]


def singles(calls: list) -> list[str]:
    return [path.split("/")[2] for path, _, _ in calls if path.endswith("/orderbook")]


def envelopes(root: Path) -> list[dict]:
    return [json.loads(gzip.decompress(path.read_bytes())) for path in sorted(root.rglob("*.gz"))]


@pytest.fixture
def setup_collector(tmp_path, monkeypatch):
    clock = Clock()
    collector = kalshi.KalshiCollector(
        tmp_path / "raw/kalshi", now=clock.now, monotonic=clock.monotonic, sleep=clock.sleep
    )
    monkeypatch.setattr(kalshi.random, "uniform", lambda *_: 0.0)
    return collector, clock


def mock_http(monkeypatch, clock, handler, *, latency: float = 0.01):
    calls = []

    def open_request(request, timeout):
        assert request.get_method() == "GET"
        assert timeout == kalshi.REQUEST_TIMEOUT
        assert not any(key.lower().startswith("kalshi-access") for key in request.headers)
        parsed = urlsplit(request.full_url)
        path = parsed.path.removeprefix("/trade-api/v2")
        params = {
            key: values if key == "tickers" else values[0]
            for key, values in parse_qs(parsed.query).items()
        }
        calls.append((path, params, clock.monotonic()))
        clock.elapsed += latency
        result = handler(path, params)
        if isinstance(result, Exception):
            raise result
        if isinstance(result, dict):
            return Response(json.dumps(result).encode())
        return result

    monkeypatch.setattr(kalshi, "urlopen", open_request)
    return calls


@pytest.mark.parametrize(
    ("frequency", "title", "expected"),
    [
        ("daily", "Highest temperature in NYC today?", True),
        ("daily", "High temperature in Miami", True),
        ("daily", "Maximum daily temperature in a new city", True),
        ("DAILY", "  HIGHEST   TEMPERATURE in Boston", True),
        ("daily", "Max temp in Austin", True),
        ("daily", "Lowest temperature in NYC", False),
        ("daily", "Rain in NYC today?", False),
        ("hourly", "Highest temperature in NYC", False),
        ("monthly", "Highest temperature in NYC this month", False),
        ("daily", "Highest temperature difference between cities", False),
    ],
)
def test_daily_high_matching(frequency, title, expected):
    record = {"ticker": "KXCITY", "frequency": frequency, "title": title}
    assert kalshi.is_daily_high_series(record) is expected


@pytest.mark.parametrize(
    "record",
    [
        {"ticker": "KXCITY", "frequency": "daily"},
        {"ticker": "KXCITY", "frequency": 42, "title": "High"},
        {"frequency": "daily", "title": "Daily high temp Tokyo"},
        {"ticker": "", "frequency": "daily", "title": "Daily high temp Tokyo"},
    ],
)
def test_missing_or_invalid_discovery_fields_raise(record):
    with pytest.raises((KeyError, ValueError)):
        kalshi.is_daily_high_series(record)


SERIES_FIXTURE = Path(__file__).parent / "fixtures/kalshi_series/2026-10-07T142603.591250Z.json.gz"
# Every daily-high series in the 2026-10-07 /series response (see PROVENANCE.md).
EXPECTED_DAILY_HIGH_SERIES = {
    # Matched by the original "<high word> temperature in" rule.
    *("HIGHAUS", "HIGHCHI", "HIGHMIA", "HIGHNY", "HIGHUS", "KXCITIESWEATHER", "KXDENHIGH"),
    *("KXHIGHAUS", "KXHIGHCHI", "KXHIGHDEN", "KXHIGHHOU", "KXHIGHLAX", "KXHIGHMIA"),
    *("KXHIGHNY", "KXHIGHOU", "KXHIGHPHIL", "KXHIGHUS", "KXHOUHIGH"),
    # Missed before: free word order, city first or last.
    *("HIGHNY0", "KXHIGHNY0", "KXHIGHTATL", "KXHIGHTBOS", "KXHIGHTCYYZ", "KXHIGHTDAL"),
    *("KXHIGHTDC", "KXHIGHTEBBR", "KXHIGHTEDDB", "KXHIGHTEDDF", "KXHIGHTEGLL", "KXHIGHTEHAM"),
    *("KXHIGHTEWR", "KXHIGHTHOU", "KXHIGHTKSAN", "KXHIGHTLFPG", "KXHIGHTLSGG", "KXHIGHTLTFM"),
    *("KXHIGHTLV", "KXHIGHTMIN", "KXHIGHTMMMX", "KXHIGHTNOLA", "KXHIGHTOKC", "KXHIGHTOMDB"),
    *("KXHIGHTPHX", "KXHIGHTRJTT", "KXHIGHTRKSI", "KXHIGHTSAN", "KXHIGHTSATX", "KXHIGHTSBGR"),
    *("KXHIGHTSDF", "KXHIGHTSEA", "KXHIGHTSFO", "KXHIGHTTTN", "KXHIGHTVABB", "KXHIGHTVHHH"),
    *("KXHIGHTWSSS", "KXHIGHTYSSY", "KXHIGHTZBAA", "KXHIGHTZSPD"),
    # High signal from the ticker: "Death Valley temperature".
    "KXDVHIGH",
}


def fixture_series_body() -> str:
    envelope = json.loads(gzip.decompress(SERIES_FIXTURE.read_bytes()))
    assert envelope["request_url"] == kalshi.BASE_URL + "/series"
    assert envelope["http_status"] == 200
    return envelope["body"]


def test_archived_series_response_matches_exactly_the_expected_daily_high_set():
    records = kalshi.response_records(fixture_series_body(), "series")
    assert len(records) == 14714
    matched = {record["ticker"] for record in records if kalshi.is_daily_high_series(record)}
    assert sorted(matched) == sorted(EXPECTED_DAILY_HIGH_SERIES)


def test_discovery_on_archived_response_collects_every_expected_series(
    setup_collector, monkeypatch
):
    collector, clock = setup_collector
    body = fixture_series_body()
    mock_http(monkeypatch, clock, lambda *_: Response(body.encode()))
    collector.refresh_series()
    assert collector.series_tickers == EXPECTED_DAILY_HIGH_SERIES


def test_discovery_finds_an_unseen_city_from_metadata_alone(setup_collector, monkeypatch):
    collector, clock = setup_collector
    catalogue = {
        "series": [
            series("ZZNEWCITYQ", "Reykjavik Daily Max Temp"),
            series("ZZNEWLOWQ", "Reykjavik Daily Min Temp"),
        ]
    }
    mock_http(monkeypatch, clock, lambda *_: catalogue)
    collector.refresh_series()
    assert collector.series_tickers == {"ZZNEWCITYQ"}


def test_discovery_module_contains_no_hardcoded_series_tickers():
    source = Path(kalshi.__file__).read_text()
    assert not [ticker for ticker in EXPECTED_DAILY_HIGH_SERIES if ticker in source]


@pytest.mark.parametrize(
    "title",
    [
        "Atlanta Max Temperature",
        "Boston Maximum Daily Temperature",
        "Washington DC Daily Max Temp",
        "Daily high temp Tokyo",
        "Daly high temp Sao Paulo",
        "Oklahoma City Maximum High Temperature",
        "New Orleans Max temp Daily",
        "HIGHEST Temperature SATX",
        "Highest temperature in NYC",
        "Temperature high, Reykjavik",
        "Max-temp (daily) for St. John's",
    ],
)
def test_daily_high_titles_match_regardless_of_word_order_or_city_position(title):
    assert kalshi.is_daily_high_series({"ticker": "KXCITY", "frequency": "daily", "title": title})


@pytest.mark.parametrize(
    "title",
    [
        "Lowest temperature in NYC",
        "Low Temperature Atlanta",
        "Daily low temp Tokyo",
        "Washington DC Daily Min Temp",
        "Daily Rain DC",
        "Where will it rain daily",
        "BTC max daily",
        "Biggest earthquake",
        "High and low temperature in NYC",
        "Highest temperature difference between cities",
    ],
)
def test_daily_lows_rain_and_non_temperature_titles_never_match(title):
    for ticker in ("KXCITY", "KXHIGHCITY"):
        record = {"ticker": ticker, "frequency": "daily", "title": title}
        assert not kalshi.is_daily_high_series(record)


@pytest.mark.parametrize("frequency", ["weekly", "hourly", "monthly", "custom", "one_off"])
def test_high_temperature_series_that_are_not_daily_never_match(frequency):
    record = {"ticker": "KXHIGHCITY", "frequency": frequency, "title": "Daily high temp Tokyo"}
    assert not kalshi.is_daily_high_series(record)


def test_high_in_ticker_counts_as_high_signal_only_with_a_temperature_title():
    def match(ticker, title):
        return kalshi.is_daily_high_series({"ticker": ticker, "frequency": "daily", "title": title})

    assert match("KXDVHIGH", "Death Valley temperature")
    assert not match("KXDV", "Death Valley temperature")
    assert not match("KXDVHIGH", "Death Valley rainfall")


def test_gzip_round_trip_exact_body_url_params_and_per_request_utc_times(
    setup_collector, monkeypatch
):
    collector, clock = setup_collector
    clock.origin = datetime(2026, 10, 7, 23, 59, 59, 900000, tzinfo=UTC)
    original = b'{ "markets": [], "cursor": "", "price": 0.1200, "city": "NYC" }\n'
    mock_http(monkeypatch, clock, lambda *_: Response(original), latency=0.2)
    collector.request("/markets", {"series_ticker": "NEW CITY", "status": "open"})
    collector.request("/markets/NEW/orderbook", {"depth": 0})

    files = sorted(collector.raw_root.rglob("*.json.gz"))
    assert [path.relative_to(collector.raw_root).as_posix() for path in files] == [
        "2026-10-08/2026-10-08T000000.100000Z.json.gz",
        "2026-10-08/2026-10-08T000000.300000Z.json.gz",
    ]
    first, second = envelopes(collector.raw_root)
    assert first == {
        "request_url": kalshi.BASE_URL + "/markets?series_ticker=NEW+CITY&status=open",
        "request_params": {"series_ticker": "NEW CITY", "status": "open"},
        "http_status": 200,
        "requested_at": "2026-10-07T23:59:59.900000Z",
        "received_at": "2026-10-08T00:00:00.100000Z",
        "body": original.decode(),
    }
    assert second["body"].encode() == original
    assert second["requested_at"] == first["received_at"]
    assert second["received_at"] != first["received_at"]


def test_every_endpoint_and_page_archived_and_all_cities_collected(setup_collector, monkeypatch):
    collector, clock = setup_collector

    def handler(path, params):
        if path == "/series":
            return {
                "series": [series("CITYA"), series("CITYB"), series("LOW", "Lowest temperature")]
            }
        if path == "/markets" and params["series_ticker"] == "CITYA":
            return markets("A2", "A1") if "cursor" in params else markets("A1", cursor="page2")
        if path == "/markets":
            return markets("B1")
        return books(params)

    calls = mock_http(monkeypatch, clock, handler)
    collector.run_cycle()
    saved = envelopes(collector.raw_root)
    assert len(saved) == len(calls) == 5
    assert collector.series_tickers == {"CITYA", "CITYB"}
    assert batched(calls) == [["A1", "A2", "B1"]]
    assert singles(calls) == []
    market_calls = [params for path, params, _ in calls if path == "/markets"]
    assert all(params["status"] == "open" and params["limit"] == "1000" for params in market_calls)
    assert market_calls[1]["cursor"] == "page2"
    assert all(
        following[2] - previous[2] >= kalshi.REQUEST_INTERVAL - 1e-9
        for previous, following in zip(calls, calls[1:], strict=False)
    )


@pytest.mark.parametrize("failure", ["http500", "http404", "network", "truncated"])
def test_failed_market_does_not_stop_other_markets_or_cities(
    setup_collector, monkeypatch, caplog, failure
):
    collector, clock = setup_collector

    def handler(path, params):
        if path == "/series":
            return {"series": [series("CITYA"), series("CITYB")]}
        if path == "/markets":
            return (
                markets("BAD", "GOOD") if params["series_ticker"] == "CITYA" else markets("OTHER")
            )
        if path == "/markets/orderbooks" or path == "/markets/BAD/orderbook":
            if failure == "network":
                return URLError("connection lost")
            if failure == "truncated":
                return TruncatedResponse(b"")
            status = 500 if failure == "http500" else 404
            return HTTPError(
                kalshi.BASE_URL + path, status, "failed", {}, io.BytesIO(b"upstream error")
            )
        return {"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}

    calls = mock_http(monkeypatch, clock, handler)
    with caplog.at_level(logging.INFO):
        collector.run_cycle()
    assert {path for path, _, _ in calls} >= {"/markets/GOOD/orderbook", "/markets/OTHER/orderbook"}
    attempts = 1 if failure == "http404" else kalshi.MAX_ATTEMPTS
    assert batched(calls) == [["BAD", "GOOD", "OTHER"]] * attempts
    bad_calls = [call for call in calls if call[0] == "/markets/BAD/orderbook"]
    assert len(bad_calls) == attempts
    assert "falling back per market" in caplog.text
    assert "Order book failed market=BAD; continuing" in caplog.text
    assert f"request_count={len(calls)}" in caplog.text
    assert "Cycle duration=" in caplog.text
    errors = [record for record in envelopes(collector.raw_root) if record["http_status"] != 200]
    assert len(errors) == (0 if failure in ("network", "truncated") else 2 * attempts)
    assert all(record["body"] == "upstream error" for record in errors)


def test_429_archived_then_retried_with_exponential_backoff(setup_collector, monkeypatch):
    collector, clock = setup_collector
    attempts = iter([429, 429, 200])

    def handler(path, params):
        status = next(attempts)
        if status == 429:
            return HTTPError(
                kalshi.BASE_URL + path,
                status,
                "limited",
                {},
                io.BytesIO(b'{"error":"too many requests"}'),
            )
        return Response(b'{"series":[]}')

    mock_http(monkeypatch, clock, handler)
    assert collector.request("/series") == '{"series":[]}'
    assert [record["http_status"] for record in envelopes(collector.raw_root)] == [429, 429, 200]
    assert 1.0 in clock.sleeps and 2.0 in clock.sleeps
    assert collector.request_count == 3


def test_series_refresh_hourly_logs_changes_and_still_fetches_markets_each_cycle(
    setup_collector, monkeypatch, caplog
):
    collector, clock = setup_collector
    discovered = iter([[series("CITYA")], [series("CITYB")]])

    def handler(path, params):
        return {"series": next(discovered)} if path == "/series" else markets()

    calls = mock_http(monkeypatch, clock, handler)
    with caplog.at_level(logging.INFO):
        collector.run_cycle()
        refresh_at = collector.next_series_refresh
        clock.elapsed = 60
        collector.run_cycle()
        clock.elapsed = refresh_at - 1
        collector.run_cycle()
        clock.elapsed = refresh_at
        collector.run_cycle()
    assert sum(path == "/series" for path, _, _ in calls) == 2
    assert sum(path == "/markets" for path, _, _ in calls) == 4
    assert "matched=['CITYA']" in caplog.text and "matched=['CITYB']" in caplog.text
    changes = [record for record in caplog.records if "series changed" in record.message]
    assert len(changes) == 1 and changes[0].levelno == logging.WARNING
    assert "added=['CITYB'] removed=['CITYA']" in changes[0].message


def test_refresh_logs_open_market_counts_sorted_including_zero(
    setup_collector, monkeypatch, caplog
):
    collector, clock = setup_collector

    def handler(path, params):
        if path == "/series":
            return {
                "series": [
                    series("CITYA"),
                    series("CITYB"),
                    series("ZERO"),
                    series("LOW", "Lowest temperature"),
                ]
            }
        if path == "/markets":
            if params["series_ticker"] == "CITYA":
                return markets("A1")
            if params["series_ticker"] == "CITYB":
                return markets("B3") if "cursor" in params else markets("B1", "B2", cursor="next")
            assert params["series_ticker"] == "ZERO"
            return markets()
        return books(params)

    calls = mock_http(monkeypatch, clock, handler)

    def summary_lines():
        records = [record for record in caplog.records if "Series open markets" in record.message]
        assert all(record.levelno == logging.INFO for record in records)
        return [record.message for record in records]

    expected = [
        "Series open markets ticker=CITYB count=3",
        "Series open markets ticker=CITYA count=1",
        "Series open markets ticker=ZERO count=0",
    ]
    with caplog.at_level(logging.INFO):
        collector.run_cycle()
        assert summary_lines() == expected
        assert collector.series_tickers == {"CITYA", "CITYB", "ZERO"}
        assert batched(calls) == [["A1", "B1", "B2", "B3"]]
        assert singles(calls) == []
        clock.elapsed = 60
        collector.run_cycle()
        assert summary_lines() == expected
        clock.elapsed = collector.next_series_refresh
        collector.run_cycle()
        assert summary_lines() == expected * 2
    assert [params["series_ticker"] for path, params, _ in calls if path == "/markets"] == [
        "CITYA",
        "CITYB",
        "CITYB",
        "ZERO",
    ] * 3


def test_failed_series_refresh_keeps_cache_and_collects(setup_collector, monkeypatch):
    collector, clock = setup_collector
    collector.series_tickers = {"CITYA"}

    def handler(path, params):
        return (
            URLError("offline")
            if path == "/series"
            else markets("A1")
            if path == "/markets"
            else books(params)
        )

    calls = mock_http(monkeypatch, clock, handler)
    collector.run_cycle()
    assert collector.series_tickers == {"CITYA"}
    assert batched(calls) == [["A1"]]
    assert collector.next_series_refresh > clock.monotonic()


@pytest.mark.parametrize("bad_body", [b"not JSON", b'{"series":null}', b'{"series":[42]}'])
def test_bad_discovery_body_is_archived_and_logged(setup_collector, monkeypatch, caplog, bad_body):
    collector, clock = setup_collector
    mock_http(monkeypatch, clock, lambda *_: Response(bad_body))
    collector.run_cycle()
    assert envelopes(collector.raw_root)[0]["body"].encode() == bad_body
    assert "Series refresh failed" in caplog.text
    assert collector.series_tickers is None


def test_bad_market_page_does_not_stop_next_city(setup_collector, monkeypatch, caplog):
    collector, clock = setup_collector

    def handler(path, params):
        if path == "/series":
            return {"series": [series("CITYA"), series("CITYB")]}
        if path == "/markets":
            return (
                {"markets": [{"ticker": 12}], "cursor": ""}
                if params["series_ticker"] == "CITYA"
                else markets("B1")
            )
        return books(params)

    calls = mock_http(monkeypatch, clock, handler)
    collector.run_cycle()
    assert batched(calls) == [["B1"]]
    assert "Markets failed series=CITYA; continuing" in caplog.text


def test_repeated_cursor_fails_loud_instead_of_looping(setup_collector, monkeypatch, caplog):
    collector, clock = setup_collector
    collector.series_tickers = {"CITYA"}
    collector.next_series_refresh = 3600
    calls = mock_http(monkeypatch, clock, lambda *_: markets(cursor="same"))
    collector.run_cycle()
    assert len(calls) == 2
    assert "repeated cursor" in caplog.text


@pytest.mark.parametrize(
    ("duration", "starts"), [(5, [0, 60, 120]), (65, [0, 120, 180]), (125, [0, 180, 240])]
)
def test_schedule_skips_elapsed_ticks_without_overlap(
    setup_collector, monkeypatch, caplog, duration, starts
):
    collector, clock = setup_collector
    observed = []

    def cycle():
        observed.append(clock.monotonic())
        if len(observed) == 3:
            raise KeyboardInterrupt
        clock.elapsed += duration if len(observed) == 1 else 5

    monkeypatch.setattr(collector, "run_cycle", cycle)
    with pytest.raises(KeyboardInterrupt):
        collector.run_forever()
    assert observed == starts
    assert ("skipped_ticks=" in caplog.text) is (duration > 60)


def test_timestamp_collision_never_overwrites_existing_archive(setup_collector):
    collector, clock = setup_collector
    args = (kalshi.BASE_URL + "/series", {}, 200, clock.now(), clock.now())
    collector.save_response(*args, '{"first":1}')
    with pytest.raises(FileExistsError):
        collector.save_response(*args, '{"second":2}')
    assert envelopes(collector.raw_root)[0]["body"] == '{"first":1}'


def test_non_utc_timestamps_rejected():
    with pytest.raises(ValueError, match="UTC"):
        kalshi.utc_timestamp(datetime(2026, 10, 7))  # noqa: DTZ001


def test_main_stops_cleanly_on_interrupt(monkeypatch, caplog):
    def stop(self):
        raise KeyboardInterrupt

    monkeypatch.setattr(kalshi.KalshiCollector, "run_forever", stop)
    with caplog.at_level(logging.INFO):
        kalshi.main()
    assert "Kalshi collector stopped" in caplog.text


def market_tickers(series_ticker: str, count: int) -> list[str]:
    return [f"{series_ticker}-M{index:03d}" for index in range(count)]


def catalogue_handler(open_markets: dict[str, list[str]], batch_failure=None):
    """Serve /series and /markets from open_markets; batches answer via books() unless failed."""

    def handler(path, params):
        if path == "/series":
            return {"series": [series(ticker) for ticker in open_markets]}
        if path == "/markets":
            return markets(*open_markets[params["series_ticker"]])
        if path == "/markets/orderbooks":
            failure = batch_failure(params["tickers"]) if batch_failure else None
            return failure if failure is not None else books(params)
        return {"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}

    return handler


@pytest.mark.parametrize(
    "failure",
    [
        HTTPError("x", 400, "bad request", {}, io.BytesIO(b'{"error":"bad ticker"}')),
        HTTPError("x", 500, "upstream", {}, io.BytesIO(b"upstream error")),
        URLError("connection lost"),
        Response(b"not JSON"),
        "missing ticker",
    ],
    ids=["http400", "http500", "network", "malformed", "missing-ticker"],
)
def test_failed_batch_falls_back_per_market_for_that_chunk_only(
    setup_collector, monkeypatch, caplog, failure
):
    collector, clock = setup_collector
    open_markets = {"CITYA": market_tickers("A", 60), "CITYB": market_tickers("B", 50)}
    tickers = open_markets["CITYA"] + open_markets["CITYB"]
    bad_chunk = tickers[50:100]

    def batch_failure(chunk):
        if chunk != bad_chunk:
            return None
        if failure == "missing ticker":
            return {"orderbooks": books({"tickers": chunk[1:]})["orderbooks"]}
        return failure

    calls = mock_http(monkeypatch, clock, catalogue_handler(open_markets, batch_failure))
    collector.run_cycle()
    assert [chunk for chunk in batched(calls) if chunk != bad_chunk] == [
        tickers[:50],
        tickers[100:],
    ]
    assert singles(calls) == bad_chunk
    assert "falling back per market" in caplog.text


def test_one_failing_ticker_in_fallback_never_loses_the_other_books(
    setup_collector, monkeypatch, caplog
):
    collector, clock = setup_collector
    open_markets = {"CITYA": ["A1", "BAD", "A3"]}
    batch_400 = HTTPError("x", 400, "bad request", {}, io.BytesIO(b"bad"))
    handler = catalogue_handler(open_markets, lambda chunk: batch_400)

    def with_bad_single(path, params):
        if path == "/markets/BAD/orderbook":
            return HTTPError("x", 404, "not found", {}, io.BytesIO(b"missing"))
        return handler(path, params)

    calls = mock_http(monkeypatch, clock, with_bad_single)
    collector.run_cycle()
    assert singles(calls) == ["A1", "BAD", "A3"]
    ok_books = [
        record["request_url"]
        for record in envelopes(collector.raw_root)
        if record["request_url"].endswith("/orderbook?depth=0") and record["http_status"] == 200
    ]
    assert [url.split("/")[-2] for url in ok_books] == ["A1", "A3"]
    assert "Order book failed market=BAD; continuing" in caplog.text


def test_batch_response_is_archived_with_url_and_exact_ticker_list(setup_collector, monkeypatch):
    collector, clock = setup_collector
    tickers = ["KXHIGHNY-26NOV08-T68", "KXHIGHTATL-26NOV08-B60.5", "A&B=C"]
    body = json.dumps(books({"tickers": tickers}), indent=1) + "\n"
    mock_http(monkeypatch, clock, lambda *_: Response(body.encode()))
    collector.collect_orderbooks(tickers)
    (envelope,) = envelopes(collector.raw_root)
    assert set(envelope) == {
        "request_url",
        "request_params",
        "http_status",
        "requested_at",
        "received_at",
        "body",
    }
    assert envelope["request_params"] == {"tickers": tickers}
    assert envelope["request_url"] == (
        kalshi.BASE_URL + "/markets/orderbooks?tickers=KXHIGHNY-26NOV08-T68"
        "&tickers=KXHIGHTATL-26NOV08-B60.5&tickers=A%26B%3DC"
    )
    assert envelope["body"] == body
    assert envelope["http_status"] == 200


@pytest.mark.parametrize("market_count", [0, 1, 49, 50, 51, 100, 101, 288])
def test_every_open_market_book_is_requested_exactly_once_per_cycle(
    setup_collector, monkeypatch, market_count
):
    collector, clock = setup_collector
    tickers = market_tickers("A", market_count)
    # The same markets listed under a second series must not be requested twice.
    open_markets = {"CITYA": tickers, "CITYB": tickers[:3]}
    calls = mock_http(monkeypatch, clock, catalogue_handler(open_markets))
    collector.run_cycle()
    chunks = batched(calls)
    requested = [ticker for chunk in chunks for ticker in chunk] + singles(calls)
    assert sorted(requested) == sorted(tickers)
    assert all(1 <= len(chunk) <= kalshi.ORDERBOOK_BATCH_SIZE for chunk in chunks)


def test_every_book_requested_exactly_once_when_a_chunk_falls_back(setup_collector, monkeypatch):
    collector, clock = setup_collector
    tickers = market_tickers("A", 120)
    batch_400 = HTTPError("x", 400, "bad", {}, io.BytesIO(b"bad"))
    handler = catalogue_handler(
        {"CITYA": tickers}, lambda chunk: batch_400 if chunk[0] == tickers[50] else None
    )
    calls = mock_http(monkeypatch, clock, handler)
    collector.run_cycle()
    good_chunks = [chunk for chunk in batched(calls) if chunk[0] != tickers[50]]
    requested = [ticker for chunk in good_chunks for ticker in chunk] + singles(calls)
    assert sorted(requested) == sorted(tickers)


def test_cycle_log_reports_requests_429s_and_token_estimate(setup_collector, monkeypatch, caplog):
    collector, clock = setup_collector
    collector.series_tickers = {"CITYA"}
    collector.next_series_refresh = 3600
    open_markets = {"CITYA": market_tickers("A", 60)}
    handler = catalogue_handler(open_markets)
    limited = iter([True, False])

    def with_one_429(path, params):
        if path == "/markets/orderbooks" and len(params["tickers"]) == 50 and next(limited):
            return HTTPError("x", 429, "limited", {}, io.BytesIO(b'{"error":"too many requests"}'))
        return handler(path, params)

    calls = mock_http(monkeypatch, clock, with_one_429)
    with caplog.at_level(logging.INFO):
        collector.run_cycle()
    # 1 markets page + 50-ticker batch twice (429 then 200) + 10-ticker batch.
    assert len(calls) == 4
    expected_tokens = kalshi.TOKEN_COST * (1 + 50 + 50 + 10)
    assert (collector.request_count, collector.rate_limited_count) == (4, 1)
    assert collector.token_estimate == expected_tokens
    assert f"request_count=4 rate_limited=1 token_estimate={expected_tokens}" in caplog.text
