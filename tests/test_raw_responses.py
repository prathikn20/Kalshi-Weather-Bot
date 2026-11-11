import base64
import gzip
import io
import json
import tarfile
from datetime import UTC, datetime, timedelta
from email.message import Message
from pathlib import Path
from urllib.error import HTTPError

import pytest
import zstandard

from kwb.collectors import kalshi, nws
from kwb.ops import compact

INVALID_BODY = Path(__file__).parent / "fixtures/undecodable/upstream-error.bin"


@pytest.fixture(params=["kalshi", "nws"])
def collector_http(request, tmp_path, monkeypatch):
    source = request.param
    module = kalshi if source == "kalshi" else nws
    origin = datetime(2026, 11, 10, 12, tzinfo=UTC)
    ticks = iter(range(100))
    sleeps = []
    kwargs = {
        "now": lambda: origin + timedelta(seconds=next(ticks)),
        "monotonic": lambda: 0.0,
        "sleep": sleeps.append,
    }
    root = tmp_path / "raw" / source
    if source == "kalshi":
        collector = kalshi.KalshiCollector(root, **kwargs)
        path = "/markets/orderbooks"
        params = {"tickers": ["KXHIGHNY-26NOV10-T68", "KXHIGHNY-26NOV10-T69"]}
        url = module.BASE_URL + path + "?tickers=KXHIGHNY-26NOV10-T68&tickers=KXHIGHNY-26NOV10-T69"

        def fetch():
            return collector.request(path, params)

    else:
        collector = nws.NWSCollector((nws.Station("KNYC", 40.78333, -73.96667),), root, **kwargs)
        url = "https://api.weather.gov/gridpoints/OKX/33,37/forecast"
        params = {}

        def fetch():
            return collector.request(url)

    calls = []

    def serve(body, status):
        def open_response(request, timeout):
            calls.append(request.full_url)
            assert request.full_url == url
            if status >= 300:
                raise HTTPError(url, status, "upstream response", Message(), io.BytesIO(body))
            response = io.BytesIO(body)
            response.status = status
            response.headers = Message()
            return response

        if source == "kalshi":
            monkeypatch.setattr(module, "urlopen", open_response)
        else:
            monkeypatch.setattr(collector.opener, "open", open_response)

    monkeypatch.setattr(module.random, "uniform", lambda *_: 0.0)
    return collector, module, fetch, serve, calls, sleeps, origin, url, params


@pytest.mark.parametrize("status", [200, 302, 404, 408, 429, 500, 503])
def test_every_completed_undecodable_response_is_archived_with_exact_bytes_and_request_metadata(
    collector_http, status
):
    collector, module, fetch, serve, calls, sleeps, origin, url, params = collector_http
    body = INVALID_BODY.read_bytes()
    serve(body, status)
    error = UnicodeDecodeError if status == 200 else module.RequestFailed
    with pytest.raises(error):
        fetch()

    attempts = module.MAX_ATTEMPTS if status in (408, 429, 500, 503) else 1
    paths = sorted(collector.raw_root.rglob("*.json.gz"))
    assert len(paths) == len(calls) == attempts
    for index, path in enumerate(paths):
        requested_at = origin + timedelta(seconds=2 * index)
        received_at = requested_at + timedelta(seconds=1)
        envelope = json.loads(gzip.decompress(path.read_bytes()))
        assert envelope == {
            "request_url": url,
            "request_params": params,
            "http_status": status,
            "requested_at": module.utc_timestamp(requested_at),
            "received_at": module.utc_timestamp(received_at),
            "body": base64.b64encode(body).decode("ascii"),
            "body_encoding": "base64",
        }
        assert base64.b64decode(envelope["body"], validate=True) == body
        assert path.name == module.utc_timestamp(received_at).replace(":", "") + ".json.gz"
        assert path.parent.name == received_at.date().isoformat()
    assert len(sleeps) == 2 * attempts - 1
    if status == 429:
        assert collector.request_count == attempts
        if isinstance(collector, kalshi.KalshiCollector):
            assert collector.rate_limited_count == attempts


@pytest.mark.parametrize(
    "body", [b"", b'{ "value": 1.2000 }\n', "temperature: 20\N{DEGREE SIGN}C\n".encode()]
)
def test_utf8_responses_keep_the_legacy_envelope_and_return_the_exact_text(collector_http, body):
    collector, _, fetch, serve, _, _, _, _, _ = collector_http
    serve(body, 200)
    assert fetch() == body.decode("utf-8")
    (path,) = collector.raw_root.rglob("*.json.gz")
    envelope = json.loads(gzip.decompress(path.read_bytes()))
    assert set(envelope) == {
        "request_url",
        "request_params",
        "http_status",
        "requested_at",
        "received_at",
        "body",
    }
    assert envelope["body"].encode("utf-8") == body


def test_compaction_preserves_legacy_and_base64_envelopes_and_recovers_the_exact_response_bytes(
    collector_http,
):
    collector, _, fetch, serve, _, _, origin, _, _ = collector_http
    invalid = INVALID_BODY.read_bytes()
    legacy = b'{ "legacy": true }\n'
    serve(legacy, 200)
    fetch()
    serve(invalid, 200)
    with pytest.raises(UnicodeDecodeError):
        fetch()
    expected = {
        path.name.removesuffix(".gz"): gzip.decompress(path.read_bytes())
        for path in collector.raw_root.rglob("*.json.gz")
    }
    assert len(expected) == 2

    compact.compact(collector.raw_root.parent, origin + timedelta(hours=2))

    assert not list(collector.raw_root.rglob("*.json.gz"))
    (archive,) = collector.raw_root.rglob("*.tar.zst")
    with (
        archive.open("rb") as source,
        zstandard.ZstdDecompressor().stream_reader(source) as reader,
        tarfile.open(fileobj=reader, mode="r|") as tar,
    ):
        actual = {member.name: tar.extractfile(member).read() for member in tar}
    assert actual == expected
    recovered = []
    for payload in actual.values():
        envelope = json.loads(payload)
        if "body_encoding" in envelope:
            assert envelope["body_encoding"] == "base64"
            recovered.append(base64.b64decode(envelope["body"], validate=True))
        else:
            recovered.append(envelope["body"].encode("utf-8"))
    assert recovered == [legacy, invalid]
