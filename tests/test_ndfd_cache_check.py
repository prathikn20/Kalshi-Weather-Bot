"""Network policy uses a virtual clock; real XML fixtures test listing boundaries."""

import gzip
import hashlib
import io
import json
from datetime import UTC, date, datetime
from email.message import Message
from pathlib import Path
from types import SimpleNamespace
from urllib.error import URLError

import pytest

from kwb.collectors.ndfd_cache import DownloadFailed, HTTPClient, NDFDCache
from kwb.ops.ndfd_point_check import account_day, land_result, summarize, validate_accounting
from kwb.parse.ndfd import ObjectInfo

NOW = datetime(2026, 11, 15, tzinfo=UTC)
KEY = "wmo/maxt/2026/11/06/YGUZ98_KWBN_202611060016"


class Response(io.BytesIO):
    def __init__(self, status=200, body=b"data", headers=None):
        super().__init__(body)
        self.status = status
        self.headers = Message()
        for key, value in (headers or {}).items():
            self.headers[key] = value


class Clock:
    def __init__(self):
        self.time = 0.0

    def monotonic(self):
        return self.time

    def sleep(self, seconds):
        assert seconds >= 0
        self.time += seconds


class Opener:
    def __init__(self, clock, responses):
        self.clock, self.responses, self.starts, self.requests = clock, iter(responses), [], []

    def open(self, request, timeout):
        self.starts.append(self.clock.time)
        self.requests.append(request)
        item = next(self.responses)
        if isinstance(item, Exception):
            raise item
        return item


def client(responses):
    clock = Clock()
    opener = Opener(clock, responses)
    http = HTTPClient(monotonic=clock.monotonic, sleep=clock.sleep, now=lambda: NOW, opener=opener)
    return http, opener


def test_cached_objects_are_reused_only_when_the_complete_manifest_and_sha256_match(tmp_path):
    http, opener = client([Response(headers={"ETag": '"etag"'})])
    cache = NDFDCache(tmp_path, http=http)
    obj = ObjectInfo(KEY, 4, '"etag"')
    path = cache.fetch_object(obj)
    row = cache.db.execute("SELECT * FROM objects").fetchone()
    assert row == (KEY, 4, '"etag"', hashlib.sha256(b"data").hexdigest(), NOW.isoformat())
    assert cache.fetch_object(obj) == path
    assert len(opener.requests) == 1
    path.write_bytes(b"evil")
    with pytest.raises(ValueError, match="SHA-256"):
        cache.fetch_object(obj)
    assert path.read_bytes() == b"evil"
    assert len(opener.requests) == 1
    path.write_bytes(b"data")
    with pytest.raises(ValueError, match="Listing"):
        cache.fetch_object(ObjectInfo(KEY, 4, '"changed"'))
    path.unlink()
    with pytest.raises(ValueError, match="size or file"):
        cache.fetch_object(obj)
    cache.close()
    orphan = NDFDCache(tmp_path / "orphan", http=http)
    path = orphan.root / "objects" / KEY
    path.parent.mkdir(parents=True)
    path.write_bytes(b"data")
    with pytest.raises(ValueError, match="Unmanifested"):
        orphan.fetch_object(obj)
    orphan.close()


@pytest.mark.parametrize("retry_status", [408, 429, 500, 503, 599, "transport"])
def test_downloads_are_unsigned_paced_and_respect_retry_policy_and_retry_after(
    retry_status,
):
    first = (
        URLError("reset")
        if retry_status == "transport"
        else Response(retry_status, headers={"Retry-After": "7"})
    )
    http, opener = client([first, Response(), Response()])
    url = "https://noaa-ndfd-pds.s3.amazonaws.com/" + KEY
    http.get(url)
    http.get(url)
    assert all(b - a >= 1 for a, b in zip(opener.starts, opener.starts[1:], strict=False))
    if retry_status != "transport":
        assert opener.starts[1] >= 7
    assert all(
        r.full_url.startswith("https://noaa-ndfd-pds.s3.amazonaws.com/") for r in opener.requests
    )
    assert all(not any("authorization" in k.lower() for k in r.headers) for r in opener.requests)
    for status in (301, 400, 401, 403, 404):
        permanent, calls = client([Response(status), Response()])
        with pytest.raises(DownloadFailed):
            permanent.get(url)
        assert len(calls.starts) == 1
    exhausted, calls = client([URLError("reset") for _ in range(3)])
    with pytest.raises(DownloadFailed):
        exhausted.get(url)
    assert len(calls.starts) == 3
    dated, calls = client(
        [Response(503, headers={"Retry-After": "Sun, 15 Nov 2026 00:00:20 GMT"}), Response()]
    )
    dated.get(url)
    assert calls.starts[1] >= 20


def test_every_listing_page_is_validated_and_opaque_tokens_are_preserved(tmp_path):
    fixtures = sorted(Path("tests/fixtures/ndfd/2026-11-13").glob("*.json.gz"))
    envelopes = [json.loads(gzip.decompress(p.read_bytes())) for p in fixtures]
    bodies = [r["body"].encode() for r in envelopes]
    prefix = envelopes[0]["request_params"]["prefix"]
    # Original NOAA pages are truncated. End the second page without altering any objects.
    import xml.etree.ElementTree as ET

    from kwb.collectors.ndfd import NS

    root = ET.fromstring(bodies[1])
    root.find(NS + "IsTruncated").text = "false"
    token_node = root.find(NS + "NextContinuationToken")
    root.remove(token_node)
    final = ET.tostring(root)
    http, calls = client([Response(body=bodies[0]), Response(body=final)])
    cache = NDFDCache(tmp_path, http=http)
    day = date.fromisoformat(prefix[len("wmo/maxt/") :].strip("/").replace("/", "-"))
    rows = cache.list_objects(day, day, snapshot="test")
    assert len(rows) == 4
    assert "continuation-token=" in calls.requests[1].full_url
    assert all(r.size > 0 and r.etag for r in rows)
    assert cache.list_objects(day, day, snapshot="test") == rows
    assert len(calls.requests) == 2
    cache.close()
    repeat, _ = client([Response(body=bodies[0]), Response(body=bodies[0])])
    cache = NDFDCache(tmp_path / "repeat", http=repeat)
    with pytest.raises(ValueError, match="duplicate|Repeated"):
        cache.list_objects(day, day, snapshot="test")
    cache.close()


def test_every_station_day_has_exactly_one_accounting_status():
    days = [date(2026, 11, 13), date(2026, 11, 14), date(2026, 11, 15)]
    rows = [
        account_day("KLAX", days[0], 70, SimpleNamespace(native_fahrenheit=71)),
        account_day("KLAX", days[1], 69, None),
        account_day("KLAX", days[2], None, None),
    ]
    validate_accounting(rows, ["KLAX"], days)
    assert [r["status"] for r in rows] == ["scored", "no qualifying issuance", "no observed high"]
    assert summarize(rows)["accounting"] == {
        "scored": 1,
        "no qualifying issuance": 1,
        "no observed high": 1,
    }
    for broken in (rows[:-1], rows + [rows[0]]):
        with pytest.raises(ValueError, match="accounting"):
            validate_accounting(broken, ["KLAX"], days)
    with pytest.raises(ValueError, match="status"):
        validate_accounting([{**r, "status": "scored"} for r in rows], ["KLAX"], days)


def test_land_classification_is_indeterminate_within_the_shoreline_accuracy():
    assert land_result(21, 20) == "land"
    assert land_result(-21, 20) == "water"
    for value in (-20, -1, 0, 1, 20):
        assert land_result(value, 20) == "indeterminate"


def test_selected_issuance_age_distributions_and_older_fallbacks_are_reported():
    rows = [
        {"status": "scored", "error_f": e, "issuance_age_hours": a, "fallback": f}
        for e, a, f in [(1, 2.5, False), (-1, 3, True), (2, 3, True)]
    ]
    result = summarize(rows)
    assert result["issuance_age_hours"] == {
        "min": 2.5,
        "median": 3,
        "max": 3,
        "distribution": {2.5: 1, 3: 2},
    }
    assert result["older_issuance_fallbacks"] == 2


def test_final_cli_highs_come_from_the_real_saved_iem_response_and_completed_text():
    from kwb.ops.ndfd_point_check import cli_highs

    directory = Path("tests/fixtures/ndfd_points")
    payload = json.loads((directory / "iem_cli.json").read_text())
    text = (directory / "iem_cli.txt").read_bytes()

    class SavedCache:
        def cached_response(self, url, directory):
            return json.dumps(payload).encode() if "/json/cli.py?" in url else text

    highs, end = cli_highs(SavedCache(), "KLAX")
    row = payload["results"][0]
    assert end == date.fromisoformat(row["valid"])
    assert highs[end] == row["high"]
    row["high"] = "unexpected"
    with pytest.raises(ValueError, match="high"):
        cli_highs(SavedCache(), "KLAX")


def test_api_crosschecks_use_canonical_lookup_precision_and_full_station_coordinates():
    import grib2io

    from kwb.ops.ndfd_point_check import api_cell_crosschecks, load_stations
    from kwb.parse.ndfd_decode import grid_spec

    station = load_stations()[0]
    urls = []

    class PublicCache:
        def cached_response(self, url, directory):
            urls.append(url)
            if "/points/" in url:
                return json.dumps(
                    {
                        "properties": {
                            "forecastGridData": "https://api.weather.gov/gridpoints/EWX/1,1"
                        }
                    }
                ).encode()
            lon, lat = station.longitude, station.latitude
            return json.dumps(
                {
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [lon - 0.001, lat - 0.001],
                                [lon + 0.001, lat - 0.001],
                                [lon + 0.001, lat + 0.001],
                                [lon - 0.001, lat + 0.001],
                                [lon - 0.001, lat - 0.001],
                            ]
                        ],
                    }
                }
            ).encode()

    with grib2io.open(
        "tests/fixtures/ndfd_points/maxt.grib2", save_index=False, use_index=False
    ) as messages:
        grid = grid_spec(messages[0])
    rows = api_cell_crosschecks(PublicCache(), grid, [station])
    assert urls[0] == "https://api.weather.gov/points/30.1830,-97.6799"
    assert rows[0]["station_cell"]["row"] == 332
    assert rows[0]["station_cell"]["column"] == 986
