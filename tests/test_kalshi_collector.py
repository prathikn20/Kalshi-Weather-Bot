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
        params = {key: values[0] for key, values in parse_qs(parsed.query).items()}
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
    assert kalshi.is_daily_high_series({"frequency": frequency, "title": title}) is expected


@pytest.mark.parametrize("record", [{"frequency": "daily"}, {"frequency": 42, "title": "High"}])
def test_missing_or_invalid_discovery_fields_raise(record):
    with pytest.raises((KeyError, ValueError)):
        kalshi.is_daily_high_series(record)


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
        return {"orderbook_fp": {"yes_dollars": [["0.1500", "100.00"]], "no_dollars": []}}

    calls = mock_http(monkeypatch, clock, handler)
    collector.run_cycle()
    saved = envelopes(collector.raw_root)
    assert len(saved) == len(calls) == 7
    assert collector.series_tickers == {"CITYA", "CITYB"}
    assert [path for path, _, _ in calls if path.endswith("/orderbook")] == [
        "/markets/A1/orderbook",
        "/markets/A2/orderbook",
        "/markets/B1/orderbook",
    ]
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
        if path == "/markets/BAD/orderbook":
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
    bad_calls = [call for call in calls if call[0] == "/markets/BAD/orderbook"]
    assert len(bad_calls) == (1 if failure == "http404" else kalshi.MAX_ATTEMPTS)
    assert "Order book failed market=BAD; continuing" in caplog.text
    assert f"request_count={len(calls)}" in caplog.text
    assert "Cycle duration=" in caplog.text
    errors = [record for record in envelopes(collector.raw_root) if record["http_status"] != 200]
    assert len(errors) == (0 if failure in ("network", "truncated") else len(bad_calls))
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
        return {}

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
        assert [path for path, _, _ in calls if path.endswith("/orderbook")] == [
            "/markets/A1/orderbook",
            "/markets/B1/orderbook",
            "/markets/B2/orderbook",
            "/markets/B3/orderbook",
        ]
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
            else {}
        )

    calls = mock_http(monkeypatch, clock, handler)
    collector.run_cycle()
    assert collector.series_tickers == {"CITYA"}
    assert any(path == "/markets/A1/orderbook" for path, _, _ in calls)
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
        return {}

    calls = mock_http(monkeypatch, clock, handler)
    collector.run_cycle()
    assert any(path == "/markets/B1/orderbook" for path, _, _ in calls)
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
