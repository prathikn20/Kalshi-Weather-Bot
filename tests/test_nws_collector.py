import csv
import gzip
import io
import json
import logging
from datetime import UTC, datetime, timedelta
from email.message import Message
from http.client import IncompleteRead
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import HTTPErrorProcessor

import pytest

from kwb.collectors import nws

FIXTURES = Path(__file__).parent / "fixtures/nws"


def saved(root):
    return [json.loads(gzip.decompress(path.read_bytes())) for path in sorted(root.rglob("*.gz"))]


@pytest.fixture(scope="module")
def real():
    records = saved(FIXTURES)
    result = {}
    for record in records:
        url = record["request_url"]
        if record["http_status"] >= 400:
            key = "error"
        elif "/stations/" in url:
            continue
        elif "/points/" in url:
            key = "points"
        elif url.endswith("/forecast/hourly"):
            key = "forecastHourly"
        elif url.endswith("/forecast"):
            key = "forecast"
        else:
            key = "forecastGridData"
        result[key] = record
    assert set(result) == {"points", "error", *nws.FORECAST_FIELDS}
    return result


class Clock:
    def __init__(self):
        self.origin = datetime(2040, 1, 1, 23, 59, 59, 900000, tzinfo=UTC)
        self.elapsed = 0.0
        self.sleeps = []

    def now(self):
        return self.origin + timedelta(seconds=self.elapsed)

    def monotonic(self):
        return self.elapsed

    def sleep(self, delay):
        assert delay >= 0
        self.sleeps.append(delay)
        self.elapsed += delay


class Response(io.BytesIO):
    def __init__(self, body, status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = headers or Message()

    def info(self):
        return self.headers


@pytest.fixture
def setup_collector(tmp_path, monkeypatch):
    clock = Clock()
    stations = (nws.Station("KNYC", 40.78333, -73.96667),)
    collector = nws.NWSCollector(
        stations,
        tmp_path / "raw/nws",
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    monkeypatch.setattr(nws.random, "uniform", lambda *_: 0.0)
    return collector, clock


def mock_http(collector, clock, monkeypatch, handler, *, latency=0.2):
    calls = []

    def open_request(request, timeout):
        assert timeout == nws.REQUEST_TIMEOUT
        calls.append((request.full_url, clock.monotonic(), request))
        clock.elapsed += latency
        result = handler(request.full_url)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(collector.opener, "open", open_request)
    return calls


def fixture_response(real, url):
    record = next(record for record in real.values() if record["request_url"] == url)
    return Response(record["body"].encode(), record["http_status"])


@pytest.mark.parametrize("endpoint", ["points", *nws.FORECAST_FIELDS, "error"])
def test_every_http_response_is_archived_unchanged_before_points_navigation(
    setup_collector,
    monkeypatch,
    real,
    endpoint,
):
    collector, clock = setup_collector
    record = real[endpoint]
    body = record["body"].encode()
    status = record["http_status"]

    def handler(url):
        if status >= 400:
            return HTTPError(url, status, "Real NWS error", Message(), io.BytesIO(body))
        return Response(body, status)

    mock_http(collector, clock, monkeypatch, handler)
    original_loads = json.loads

    def guarded_loads(text, *args, **kwargs):
        if text == record["body"]:
            files = list(collector.raw_root.rglob("*.json.gz"))
            assert files, "Points were parsed before archiving the response"
        return original_loads(text, *args, **kwargs)

    monkeypatch.setattr(nws.json, "loads", guarded_loads)
    requested_at = clock.now()
    if endpoint == "points":
        collector.forecast_urls(collector.stations[0])
    elif status >= 400:
        with pytest.raises(nws.RequestFailed):
            collector.request(record["request_url"])
    else:
        assert collector.request(record["request_url"]) == record["body"]
    (envelope,) = saved(collector.raw_root)
    assert envelope == {
        "request_url": collector.stations[0].points_url
        if endpoint == "points"
        else record["request_url"],
        "request_params": {},
        "http_status": status,
        "requested_at": nws.utc_timestamp(requested_at),
        "received_at": nws.utc_timestamp(clock.now()),
        "body": record["body"],
    }
    assert envelope["body"].encode() == body


def test_received_at_is_the_utc_response_arrival_time_regardless_of_source_timestamps(
    setup_collector,
    monkeypatch,
    real,
):
    collector, clock = setup_collector

    class SlowBody(Response):
        def read(self, *args):
            clock.elapsed += 0.15
            return super().read(*args)

    mock_http(
        collector,
        clock,
        monkeypatch,
        lambda _: SlowBody(real["forecast"]["body"].encode()),
        latency=0.05,
    )
    original_save = collector.save_response

    def slow_save(*args):
        clock.elapsed += 10
        original_save(*args)

    monkeypatch.setattr(collector, "save_response", slow_save)
    collector.request(real["forecast"]["request_url"])
    files = list(collector.raw_root.rglob("*.json.gz"))
    assert [path.relative_to(collector.raw_root).as_posix() for path in files] == [
        "2040-01-02/2040-01-02T000000.100000Z.json.gz"
    ]
    (envelope,) = saved(collector.raw_root)
    assert envelope["requested_at"] == "2040-01-01T23:59:59.900000Z"
    assert envelope["received_at"] == "2040-01-02T00:00:00.100000Z"
    assert envelope["body"] == real["forecast"]["body"]


@pytest.mark.parametrize("failure", ["timeout", "5xx", "bad_json", "bad_shape", "truncated"])
def test_a_station_failure_does_not_stop_other_stations_or_later_cycles(
    setup_collector,
    monkeypatch,
    real,
    caplog,
    failure,
):
    collector, clock = setup_collector
    bad = nws.Station("KAUS", 30.18304, -97.67987)
    collector.stations = (bad, *collector.stations)

    def handler(url):
        if url == bad.points_url:
            if failure == "timeout":
                return URLError(TimeoutError("timed out"))
            if failure == "5xx":
                return HTTPError(
                    url, 503, "unavailable", Message(), io.BytesIO(real["error"]["body"].encode())
                )
            if failure == "truncated":
                return IncompleteRead(b"partial", 42)
            return Response(b"not JSON" if failure == "bad_json" else b'{"properties":[]}')
        return fixture_response(real, url)

    calls = mock_http(collector, clock, monkeypatch, handler)
    with caplog.at_level(logging.INFO):
        collector.run_forever(cycles=2)
    for field in nws.FORECAST_FIELDS:
        assert sum(url == real[field]["request_url"] for url, _, _ in calls) == 2
    assert sum("Cycle duration=" in record.message for record in caplog.records) == 2
    assert "Station failed station=KAUS; continuing" in caplog.text
    if failure == "5xx":
        assert sum(row["http_status"] == 503 for row in saved(collector.raw_root)) == 6


@pytest.mark.parametrize(
    ("duration", "starts"),
    [
        (5, [0, 1800, 3600]),
        (1800, [0, 1800, 3600]),
        (1801, [0, 3600, 5400]),
        (3601, [0, 5400, 7200]),
    ],
)
def test_cycles_start_every_thirty_minutes_and_skip_missed_ticks(
    setup_collector,
    monkeypatch,
    duration,
    starts,
    caplog,
):
    collector, clock = setup_collector
    observed = []

    def cycle():
        observed.append(clock.monotonic())
        clock.elapsed += duration if len(observed) == 1 else 5

    monkeypatch.setattr(collector, "run_cycle", cycle)
    collector.run_forever(cycles=3)
    assert observed == starts
    assert ("skipped_ticks=" in caplog.text) is (duration > 1800)


def test_every_request_identifies_the_application_and_provides_contact_information(
    setup_collector,
    monkeypatch,
    real,
):
    collector, clock = setup_collector
    calls = mock_http(collector, clock, monkeypatch, lambda url: fixture_response(real, url))
    collector.run_cycle()
    assert len(calls) == 4
    for _, _, request in calls:
        assert request.get_method() == "GET"
        assert request.get_header("Accept") == "application/geo+json"
        agent = request.get_header("User-agent")
        assert "Kalshi-Weather-Bot/" in agent
        assert "https://github.com/prathikn20/Kalshi-Weather-Bot" in agent
        assert request.get_header("Authorization") is None
    assert all(b[1] - a[1] >= 1 for a, b in zip(calls, calls[1:], strict=False))


def test_points_mappings_are_cached_refreshed_and_archived_periodically(
    setup_collector,
    monkeypatch,
    real,
):
    collector, clock = setup_collector
    properties = json.loads(real["points"]["body"])["properties"]
    changed = {
        "properties": {
            key: value.replace("/OKX/", "/BOX/")
            for key, value in properties.items()
            if key in nws.FORECAST_FIELDS
        }
    }
    points_calls = 0

    def handler(url):
        nonlocal points_calls
        if "/points/" in url:
            points_calls += 1
            return Response(
                real["points"]["body"].encode()
                if points_calls == 1
                else json.dumps(changed).encode()
            )
        return Response(b"opaque forecast")

    calls = mock_http(collector, clock, monkeypatch, handler)
    collector.run_cycle()
    assert points_calls == 1
    station = collector.stations[0]
    refresh = collector.next_points_refresh[station.station_id]
    assert refresh == pytest.approx(0.2 + 86400)
    clock.elapsed = refresh - 1
    assert collector.forecast_urls(station) == tuple(properties[key] for key in nws.FORECAST_FIELDS)
    assert points_calls == 1
    clock.elapsed = refresh
    collector.run_cycle()
    assert points_calls == 2
    assert [url for url, _, _ in calls[-3:]] == [
        changed["properties"][key] for key in nws.FORECAST_FIELDS
    ]
    assert sum("/points/" in row["request_url"] for row in saved(collector.raw_root)) == 2


def test_failed_points_refresh_keeps_verified_cache_and_retries_next_cycle(
    setup_collector,
    monkeypatch,
    real,
    caplog,
):
    collector, clock = setup_collector
    calls = mock_http(collector, clock, monkeypatch, lambda url: fixture_response(real, url))
    collector.run_cycle()
    station = collector.stations[0]
    old = collector.points_cache[station.station_id]
    clock.elapsed = collector.next_points_refresh[station.station_id]
    original_request = collector.request
    monkeypatch.setattr(collector, "request", lambda _: "not JSON")
    assert collector.forecast_urls(station) == old
    assert collector.next_points_refresh[station.station_id] == clock.elapsed
    assert "Points refresh failed" in caplog.text
    assert len(calls) == 4
    clock.elapsed += 1800
    monkeypatch.setattr(collector, "request", original_request)
    calls = mock_http(collector, clock, monkeypatch, lambda url: fixture_response(real, url))
    collector.run_cycle()
    assert [url for url, _, _ in calls] == [station.points_url, *old]


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_redirect_responses_are_archived_warned_and_never_followed_or_retried(
    setup_collector,
    monkeypatch,
    real,
    status,
    caplog,
):
    collector, clock = setup_collector
    headers = Message()
    headers["Location"] = "https://api.weather.gov/redirect-target"
    calls = []
    processor = HTTPErrorProcessor()
    processor.parent = collector.opener

    def transport(request):
        calls.append(request.full_url)
        response = Response(b"original redirect body", status, headers)
        response.code, response.msg, response.url = status, "Redirect", request.full_url
        return processor.http_response(request, response)

    monkeypatch.setattr(collector.opener, "_open", lambda request, data: transport(request))
    # Exercise urllib's actual redirect/error handlers, rather than mocking HTTPError alone.
    with pytest.raises(nws.RequestFailed, match=f"HTTP {status}"):
        collector.request(real["forecast"]["request_url"])
    assert calls == [real["forecast"]["request_url"]]
    (envelope,) = saved(collector.raw_root)
    assert envelope["http_status"] == status
    assert envelope["body"] == "original redirect body"
    assert any(
        record.levelno == logging.WARNING and f"HTTP {status}" in record.message
        for record in caplog.records
    )


def test_a_failed_forecast_endpoint_does_not_prevent_its_siblings_from_being_archived(
    setup_collector,
    monkeypatch,
    real,
):
    collector, clock = setup_collector

    def handler(url):
        if url == real["forecast"]["request_url"]:
            return HTTPError(
                url, 404, "not found", Message(), io.BytesIO(real["error"]["body"].encode())
            )
        return fixture_response(real, url)

    mock_http(collector, clock, monkeypatch, handler)
    collector.run_cycle()
    assert {row["request_url"] for row in saved(collector.raw_root)} == {
        real[key]["request_url"] for key in ["points", *nws.FORECAST_FIELDS]
    }


def test_forecast_bodies_are_archived_without_parsing_or_normalizing_them(
    setup_collector,
    monkeypatch,
):
    collector, clock = setup_collector
    original = b"not JSON: \n  1.2000 \xc2\xb0F\n"
    mock_http(collector, clock, monkeypatch, lambda _: Response(original))
    collector.request("https://api.weather.gov/gridpoints/OKX/1,2/forecast")
    assert saved(collector.raw_root)[0]["body"].encode() == original


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_each_failed_retry_is_archived_and_retries_are_bounded(
    setup_collector,
    monkeypatch,
    real,
    status,
):
    collector, clock = setup_collector
    headers = Message()
    headers["Retry-After"] = "7"
    calls = mock_http(
        collector,
        clock,
        monkeypatch,
        lambda url: HTTPError(
            url,
            status,
            "failed",
            headers,
            io.BytesIO(real["error"]["body"].encode()),
        ),
    )
    with pytest.raises(nws.RequestFailed):
        collector.request(real["forecast"]["request_url"])
    assert len(calls) == 3
    assert [row["http_status"] for row in saved(collector.raw_root)] == [status] * 3
    assert clock.sleeps.count(7.0) == 2


def test_rate_limiting_without_retry_after_waits_at_least_five_seconds(
    setup_collector,
    monkeypatch,
    real,
):
    collector, clock = setup_collector
    calls = mock_http(
        collector,
        clock,
        monkeypatch,
        lambda url: HTTPError(
            url,
            429,
            "limited",
            Message(),
            io.BytesIO(real["error"]["body"].encode()),
        ),
    )
    with pytest.raises(nws.RequestFailed):
        collector.request(real["forecast"]["request_url"])
    assert all(b[1] - a[1] >= 5 for a, b in zip(calls, calls[1:], strict=False))


def test_archive_write_failure_is_fatal_instead_of_silently_losing_history(
    setup_collector,
    monkeypatch,
    real,
):
    collector, clock = setup_collector
    mock_http(collector, clock, monkeypatch, lambda url: fixture_response(real, url))

    def fail(*_):
        raise OSError("disk full")

    monkeypatch.setattr(collector, "save_response", fail)
    with pytest.raises(OSError, match="disk full"):
        collector.run_cycle()


def test_timestamp_collisions_do_not_overwrite_history(setup_collector):
    collector, clock = setup_collector
    args = ("https://api.weather.gov/points/1,2", 200, clock.now(), clock.now())
    collector.save_response(*args, "original")
    with pytest.raises(FileExistsError):
        collector.save_response(*args, "replacement")
    assert saved(collector.raw_root)[0]["body"] == "original"


def test_config_coordinates_and_site_identities_match_the_live_metadata_fixtures():
    stations = nws.load_stations()
    metadata = {
        json.loads(row["body"])["properties"]["stationIdentifier"]: json.loads(row["body"])
        for row in saved(FIXTURES)
        if "/stations/" in row["request_url"]
    }
    with nws.DEFAULT_CONFIG.open(newline="") as source:
        config = {row["station_id"]: row for row in csv.DictReader(source)}
    assert {station.station_id for station in stations} == {
        "KAUS",
        "KMDW",
        "KDEN",
        "KLAX",
        "KMIA",
        "KNYC",
        "KPHL",
    }
    for station in stations:
        payload = metadata[station.station_id]
        row = config[station.station_id]
        assert [station.longitude, station.latitude] == payload["geometry"]["coordinates"]
        assert row["station_name"] == payload["properties"]["name"]
        assert row["settlement_source"] == "The Weather Company"
        assert f"/stations/{station.station_id}" in row["site_verification_urls"]
        assert "product=CLI" in row["site_verification_urls"]
        assert row["verified_how"] and row["archive_path"] and row["market_ticker"]
    assert next(s for s in stations if s.station_id == "KNYC").points_url.endswith(
        "/points/40.7833,-73.9667"
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://api.weather.gov/gridpoints/OKX/1,2",
        "https://evil.example/gridpoints/OKX/1,2",
        "https://api.weather.gov@evil.example/gridpoints/OKX/1,2",
        "https://api.weather.gov/gridpoints/OKX/1,2?unexpected=1",
    ],
)
def test_unexpected_points_links_raise_instead_of_being_followed(url):
    with pytest.raises(ValueError, match="Unexpected forecast URL"):
        nws.validate_forecast_url(url)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("latitude", "nan"),
        ("latitude", "inf"),
        ("latitude", "91"),
        ("longitude", "-181"),
        ("longitude", "invalid"),
        ("station_id", "?"),
        ("settlement_source", ""),
        ("site_verification_urls", ""),
    ],
)
def test_invalid_station_config_values_raise_instead_of_being_defaulted(tmp_path, field, value):
    with nws.DEFAULT_CONFIG.open(newline="") as source:
        row = next(csv.DictReader(source))
    row[field] = value
    path = tmp_path / "stations.csv"
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    with pytest.raises(ValueError):
        nws.load_stations(path)


def test_empty_configs_and_duplicate_stations_raise_instead_of_silently_changing_coverage(tmp_path):
    with nws.DEFAULT_CONFIG.open(newline="") as source:
        row = next(csv.DictReader(source))
    path = tmp_path / "stations.csv"
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(row))
        writer.writeheader()
    with pytest.raises(ValueError, match="empty"):
        nws.load_stations(path)
    with path.open("a", newline="") as output:
        csv.DictWriter(output, fieldnames=list(row)).writerows([row, row])
    with pytest.raises(ValueError, match="duplicate"):
        nws.load_stations(path)
