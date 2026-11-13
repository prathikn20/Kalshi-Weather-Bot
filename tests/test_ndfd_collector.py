import base64
import configparser
import gzip
import io
import json
import logging
import shutil
import sqlite3
import tarfile
from datetime import UTC, datetime, timedelta
from email.message import Message
from http.client import IncompleteRead
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
from xml.sax.saxutils import escape

import google_crc32c
import pytest
import zstandard

from kwb.collectors import ndfd
from kwb.ops import compact, sync

FIXTURES = Path(__file__).parent / "fixtures/ndfd"
PREFIX = "wmo/maxt/2026/11/12/"


def saved(root):
    return [json.loads(gzip.decompress(p.read_bytes())) for p in sorted(root.rglob("*.json.gz"))]


def body_bytes(envelope):
    if envelope.get("body_encoding") == "base64":
        return base64.b64decode(envelope["body"], validate=True)
    return envelope["body"].encode("utf-8")


def page(prefix=PREFIX, *, keys=(), token=None):
    contents = "".join(f"<Contents><Key>{escape(key)}</Key></Contents>" for key in keys)
    continuation = (
        f"<NextContinuationToken>{escape(token)}</NextContinuationToken>" if token else ""
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"<Name>noaa-ndfd-pds</Name><Prefix>{escape(prefix)}</Prefix>"
        f"<KeyCount>{len(keys)}</KeyCount>"
        f"<IsTruncated>{'true' if token else 'false'}</IsTruncated>"
        f"{contents}{continuation}</ListBucketResult>"
    ).encode()


class Clock:
    def __init__(self):
        self.origin = datetime(2026, 11, 12, 23, 59, 59, 900000, tzinfo=UTC)
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


@pytest.fixture
def setup_collector(tmp_path, monkeypatch):
    clock = Clock()
    collector = ndfd.NDFDCollector(
        tmp_path / "raw/ndfd", now=clock.now, monotonic=clock.monotonic, sleep=clock.sleep
    )
    monkeypatch.setattr(ndfd.random, "uniform", lambda *_: 0.0)
    return collector, clock


def mock_http(collector, clock, monkeypatch, handler):
    calls = []

    def open_request(request, timeout):
        assert request.get_method() == "GET"
        assert urlsplit(request.full_url).path == "/", "A GRIB object must never be fetched"
        assert not request.has_header("Authorization")
        assert timeout == ndfd.REQUEST_TIMEOUT
        calls.append((request.full_url, clock.monotonic()))
        clock.elapsed += 0.2
        result = handler(request)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(collector.opener, "open", open_request)
    return calls


@pytest.mark.parametrize(
    ("status", "body"),
    [
        (200, page()),
        (200, b"<broken"),
        (200, b"\xff"),
        (301, b"redirect"),
        (403, b"<Error>AccessDenied</Error>"),
        (429, b"slow down"),
        (503, b"<Error/>"),
    ],
)
def test_every_http_response_is_archived_before_xml_parsing_or_error_handling(
    setup_collector, monkeypatch, status, body
):
    collector, clock = setup_collector
    attempts = []

    def handler(request):
        assert len(saved(collector.raw_root)) == len(attempts), "A retry lost its previous response"
        attempts.append(request.full_url)
        if status != 200:
            return HTTPError(request.full_url, status, "S3 error", Message(), io.BytesIO(body))
        return Response(body)

    mock_http(collector, clock, monkeypatch, handler)
    original_parse = ndfd.ET.fromstring

    def parse_after_archive(payload):
        records = saved(collector.raw_root)
        assert records and body_bytes(records[-1]) == payload, "XML parsed before archival"
        return original_parse(payload)

    monkeypatch.setattr(ndfd.ET, "fromstring", parse_after_archive)
    if status == 200 and body == page():
        collector.collect_prefix(PREFIX)
    else:
        with pytest.raises((ndfd.RequestFailed, ndfd.ET.ParseError)):
            collector.collect_prefix(PREFIX)
    expected = ndfd.MAX_ATTEMPTS if status in (429, 503) else 1
    records = saved(collector.raw_root)
    assert len(records) == expected
    for record, url in zip(records, attempts, strict=True):
        assert record["request_url"] == url
        assert record["request_params"] == {"list-type": "2", "prefix": PREFIX, "max-keys": "1000"}
        assert record["http_status"] == status
        assert body_bytes(record) == body
        assert datetime.fromisoformat(record["requested_at"]) < datetime.fromisoformat(
            record["received_at"]
        )
    # Receipt crosses midnight; raw directories describe receipt, not the prefix or send date.
    assert {p.parent.name for p in collector.raw_root.rglob("*.json.gz")} == {"2026-11-13"}


def test_every_pagination_page_is_archived_before_parsing_and_requesting_the_next_page(
    setup_collector, monkeypatch
):
    collector, clock = setup_collector
    tokens = ("p&+/%", "last", None)
    bodies = [page(keys=(PREFIX + f"object-{i}",), token=t) for i, t in enumerate(tokens)]
    requests = []

    def handler(request):
        index = len(requests)
        records = saved(collector.raw_root)
        assert [body_bytes(r) for r in records] == bodies[:index], "Page skipped before next GET"
        query = parse_qs(urlsplit(request.full_url).query)
        assert query["list-type"] == ["2"] and query["prefix"] == [PREFIX]
        assert query["max-keys"] == ["1000"]
        assert query.get("continuation-token") == (None if index == 0 else [tokens[index - 1]])
        requests.append(request)
        return Response(bodies[index])

    calls = mock_http(collector, clock, monkeypatch, handler)
    original_parse = ndfd.ET.fromstring

    def parse_after_archive(body):
        assert body_bytes(saved(collector.raw_root)[-1]) == body, "Page parsed before archival"
        return original_parse(body)

    monkeypatch.setattr(ndfd.ET, "fromstring", parse_after_archive)
    collector.collect_prefix(PREFIX)
    assert [body_bytes(r) for r in saved(collector.raw_root)] == bodies
    assert len(calls) == 3
    assert all(b[1] - a[1] >= 1 for a, b in zip(calls, calls[1:], strict=False))


@pytest.mark.parametrize(
    "origin",
    [
        datetime(2026, 11, 12, 23, 59, 59, 900000, tzinfo=UTC),
        datetime(2026, 12, 31, 23, 59, 59, 900000, tzinfo=UTC),
        datetime(2028, 2, 29, 23, 59, 59, 900000, tzinfo=UTC),
    ],
)
def test_polls_across_midnight_include_current_and_previous_utc_prefixes(
    setup_collector, monkeypatch, origin
):
    collector, clock = setup_collector
    clock.origin = origin
    previous_prefix = f"wmo/maxt/{origin:%Y/%m/%d}/"
    late_key = previous_prefix + "late-object"

    def handler(request):
        prefix = parse_qs(urlsplit(request.full_url).query)["prefix"][0]
        keys = (late_key,) if clock.elapsed >= 300 and prefix == previous_prefix else ()
        return Response(page(prefix, keys=keys))

    calls = mock_http(collector, clock, monkeypatch, handler)
    assert collector.run_cycle()
    clock.sleep(300)
    assert collector.run_cycle()
    expected_days = [
        origin.date(),
        origin.date() - timedelta(days=1),
        origin.date() + timedelta(days=1),
        origin.date(),
    ]
    assert [parse_qs(urlsplit(url).query)["prefix"][0] for url, _ in calls] == [
        f"wmo/maxt/{day:%Y/%m/%d}/" for day in expected_days
    ]
    records = saved(collector.raw_root)
    arrivals = [
        arrival
        for r in records
        for arrival in ndfd.parse_listing(
            body_bytes(r), r["request_params"]["prefix"], datetime.fromisoformat(r["received_at"])
        ).arrivals
    ]
    assert [key for key, _ in arrivals] == [late_key]


@pytest.mark.parametrize(
    "error", [URLError("reset"), TimeoutError("timed out"), IncompleteRead(b"x")]
)
def test_transport_failures_are_logged_without_archives_and_do_not_stop_the_next_poll(
    setup_collector, monkeypatch, caplog, error
):
    collector, clock = setup_collector
    failing = True

    def handler(request):
        if failing:
            return error
        prefix = parse_qs(urlsplit(request.full_url).query)["prefix"][0]
        return Response(page(prefix))

    calls = mock_http(collector, clock, monkeypatch, handler)
    monkeypatch.setattr(ndfd, "NDFDCollector", lambda *_: collector)
    with caplog.at_level(logging.WARNING):
        assert ndfd.main(["--raw-root", str(collector.raw_root)]) == 1
    assert len(calls) == 2 * ndfd.MAX_ATTEMPTS
    assert saved(collector.raw_root) == []
    assert any(
        "Request failed" in r.message and type(error).__name__ in r.message for r in caplog.records
    )
    failing = False
    clock.sleep(300)
    assert ndfd.main(["--raw-root", str(collector.raw_root)]) == 0
    assert len(saved(collector.raw_root)) == 2
    assert len(calls) == 2 * ndfd.MAX_ATTEMPTS + 2


def test_a_failed_prefix_does_not_prevent_the_other_prefix_from_being_archived(
    setup_collector, monkeypatch
):
    collector, clock = setup_collector

    def handler(request):
        prefix = parse_qs(urlsplit(request.full_url).query)["prefix"][0]
        if prefix == PREFIX:
            return HTTPError(request.full_url, 403, "denied", Message(), io.BytesIO(b"denied"))
        return Response(page(prefix))

    mock_http(collector, clock, monkeypatch, handler)
    assert collector.run_cycle() is False
    assert [r["http_status"] for r in saved(collector.raw_root)] == [403, 200]


@pytest.mark.parametrize("status", [429, 503])
@pytest.mark.parametrize("retry_after", ["7", "Fri, 13 Nov 2026 00:00:07 GMT"])
def test_retryable_responses_are_archived_and_retry_after_delays_are_respected(
    setup_collector, monkeypatch, status, retry_after
):
    collector, clock = setup_collector
    attempts = []

    def handler(request):
        attempts.append(request)
        if len(attempts) == 1:
            headers = Message()
            headers["Retry-After"] = retry_after
            return HTTPError(request.full_url, status, "slow", headers, io.BytesIO(b"slow"))
        return Response(page())

    calls = mock_http(collector, clock, monkeypatch, handler)
    collector.collect_prefix(PREFIX)
    assert calls[1][1] - calls[0][1] >= 7
    assert [r["http_status"] for r in saved(collector.raw_root)] == [status, 200]


def test_archive_write_failures_escape_instead_of_being_misreported_as_transport_failures(
    setup_collector, monkeypatch
):
    collector, clock = setup_collector
    calls = mock_http(collector, clock, monkeypatch, lambda _: Response(page()))

    def fail_write(*_):
        raise OSError("disk full")

    monkeypatch.setattr(collector, "save_response", fail_write)
    with pytest.raises(OSError, match="disk full"):
        collector.run_cycle()
    assert len(calls) == 1


def test_timestamp_collisions_raise_without_overwriting_an_archived_response(setup_collector):
    collector, clock = setup_collector
    args = (ndfd.BASE_URL, {}, 200, clock.now(), clock.now())
    collector.save_response(*args, b"original")
    with pytest.raises(FileExistsError):
        collector.save_response(*args, b"replacement")
    assert body_bytes(saved(collector.raw_root)[0]) == b"original"


@pytest.mark.parametrize("kind", ["missing", "repeated"])
def test_missing_or_repeated_continuation_tokens_fail_loudly_after_archiving(
    setup_collector, monkeypatch, kind
):
    collector, clock = setup_collector
    body = page(token="loop")
    if kind == "missing":
        body = body.replace(b"<NextContinuationToken>loop</NextContinuationToken>", b"")
    mock_http(collector, clock, monkeypatch, lambda _: Response(body))
    with pytest.raises(ValueError, match="NextContinuationToken|repeated"):
        collector.collect_prefix(PREFIX)
    assert len(saved(collector.raw_root)) == (1 if kind == "missing" else 2)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (b"ListBucketResult", b"Error"),
        (b"<Name>noaa-ndfd-pds</Name>", b"<Name>other</Name>"),
        (PREFIX.encode(), b"wrong-prefix/"),
        (b"<IsTruncated>false</IsTruncated>", b"<IsTruncated>maybe</IsTruncated>"),
        (b"<IsTruncated>false</IsTruncated>", b""),
        (b"<KeyCount>0</KeyCount>", b"<KeyCount>1</KeyCount>"),
        (b"<KeyCount>0</KeyCount>", b"<KeyCount>0</KeyCount><KeyCount>0</KeyCount>"),
        (b"</ListBucketResult>", b"<EncodingType>url</EncodingType></ListBucketResult>"),
        (b"</ListBucketResult>", b"<CommonPrefixes/></ListBucketResult>"),
    ],
)
def test_unexpected_listing_metadata_raises_instead_of_silently_truncating_collection(old, new):
    with pytest.raises(ValueError):
        ndfd.parse_listing(page().replace(old, new), PREFIX, datetime(2026, 11, 13, tzinfo=UTC))


@pytest.mark.parametrize("keys", [("elsewhere/object",), (PREFIX + "object",) * 2])
def test_unexpected_or_duplicate_listing_keys_raise_instead_of_being_silently_dropped(keys):
    with pytest.raises(ValueError, match="key"):
        ndfd.parse_listing(page(keys=keys), PREFIX, datetime(2026, 11, 13, tzinfo=UTC))


@pytest.mark.parametrize("offset", [None, timedelta(hours=-5)])
def test_listing_arrival_timestamps_must_be_timezone_aware_utc(offset):
    from datetime import timezone

    zone = None if offset is None else timezone(offset)
    with pytest.raises(ValueError, match="UTC"):
        ndfd.parse_listing(page(), PREFIX, datetime(2026, 11, 13, tzinfo=zone))


def test_real_listing_keys_use_their_earliest_fetch_time_as_first_seen_at():
    records = saved(FIXTURES)
    assert len(records) == 2
    # Later repeats must not replace the first observation. Process them out of order too.
    later = {**records[0], "received_at": "2026-11-13T06:00:00.000000Z"}
    first_seen = {}
    for record in [later, *reversed(records), records[0]]:
        listing = ndfd.parse_listing(
            body_bytes(record),
            record["request_params"]["prefix"],
            datetime.fromisoformat(record["received_at"]),
        )
        assert listing.next_token  # Both exact fixture pages are truncated by max-keys=2.
        for key, observed_at in listing.arrivals:
            first_seen[key] = min(first_seen.get(key, observed_at), observed_at)
    first = datetime(2026, 11, 13, 5, 18, 28, 714285, tzinfo=UTC)
    second = datetime(2026, 11, 13, 5, 18, 28, 834849, tzinfo=UTC)
    assert sorted(first_seen.items()) == [
        (PREFIX + "YGAZ97_KWBN_202611120534", first),
        (PREFIX + "YGAZ97_KWBN_202611121133", first),
        (PREFIX + "YGAZ97_KWBN_202611121734", second),
        (PREFIX + "YGAZ97_KWBN_202611122133", second),
    ]


@pytest.mark.parametrize("include_late", [False, True])
def test_existing_compaction_and_sync_preserve_real_ndfd_listing_envelopes(tmp_path, include_late):
    root = tmp_path / "raw"
    shutil.copytree(FIXTURES / "2026-11-13", root / "ndfd/2026-11-13")
    expected = {
        p.name.removesuffix(".gz"): gzip.decompress(p.read_bytes()) for p in root.rglob("*.json.gz")
    }
    now = datetime(2026, 11, 13, 6, 5, tzinfo=UTC)
    compact.compact(root, now)
    if include_late:
        record = saved(FIXTURES)[0]
        fetched = datetime(2026, 11, 13, 5, 30, tzinfo=UTC)
        ndfd.NDFDCollector(root / "ndfd").save_response(
            record["request_url"],
            record["request_params"],
            200,
            fetched,
            fetched,
            body_bytes(record),
        )
        loose = next(root.rglob("*.json.gz"))
        expected[loose.name.removesuffix(".gz")] = gzip.decompress(loose.read_bytes())
        compact.compact(root, now)
    assert list(root.rglob("*.json.gz")) == []

    class Remote:
        def __init__(self):
            self.objects = {}

        def check_write_once(self):
            pass

        def create(self, name, path):
            assert name not in self.objects
            data = path.read_bytes()
            self.objects[name] = data
            checksum = base64.b64encode(google_crc32c.value(data).to_bytes(4, "big")).decode()
            return sync.RemoteObject(len(data), checksum, 1)

    remote = Remote()
    ledger = tmp_path / "state/sync.sqlite"
    stats = sync.sync(root, ledger, remote)
    names = ["raw/ndfd/2026-11-13/2026-11-13T05.tar.zst"]
    if include_late:
        names.append("raw/ndfd/2026-11-13/2026-11-13T05.late-1.tar.zst")
    assert sorted(remote.objects) == sorted(names)
    assert stats.uploaded == len(names) and stats.pending == 0
    restored = {}
    for name, data in remote.objects.items():
        assert data == (tmp_path / name).read_bytes()
        with (
            zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)) as stream,
            tarfile.open(fileobj=stream, mode="r|") as archive,
        ):
            for member in archive:
                with archive.extractfile(member) as payload:
                    restored[member.name] = payload.read()
    assert restored == expected
    with sqlite3.connect(ledger) as db:
        assert {r[0] for r in db.execute("SELECT name FROM uploaded")} == set(names)
    assert sync.sync(root, ledger, remote).uploaded == 0


def test_the_ndfd_timer_retries_every_five_minutes_with_collector_resource_defaults():
    deploy = Path(__file__).resolve().parents[1] / "deploy"

    def config(name):
        result = configparser.ConfigParser(interpolation=None)
        result.read(deploy / name)
        return result

    service = config("ndfd-collector.service")["Service"]
    assert service["Type"] == "oneshot"
    assert service["TimeoutStartSec"] == "4min"
    assert service.get("RemainAfterExit", "no") == "no"
    assert service.get("Restart", "no") == "no"
    assert service["ExecStart"].endswith(" -m kwb.collectors.ndfd")
    for existing in ("kalshi", "nws"):
        reference = config(f"{existing}-collector.service")["Service"]
        for setting in (
            "User",
            "WorkingDirectory",
            "Environment",
            "CPUQuota",
            "CPUWeight",
            "IOWeight",
            "Nice",
            "MemoryMax",
            "MemoryHigh",
        ):
            assert service.get(setting) == reference.get(setting)
    timer = config("ndfd-collector.timer")["Timer"]
    assert timer["OnCalendar"] == "*-*-* *:0/5:00 UTC"
    assert timer["Unit"] == "ndfd-collector.service"
    assert timer["Persistent"] == "true" and timer["AccuracySec"] == "1s"
