import configparser
import gzip
import json
import logging
import os
import shutil
import subprocess
import sys
import traceback
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from kwb.ops import compact
from kwb.ops import heartbeat as hb

FIXTURES = Path(__file__).parent / "fixtures/kalshi"
MARKET = "2026-11-10T220044.749404Z.json.gz"
BATCH = "2026-11-10T220056.444920Z.json.gz"
SINGLE = "2026-10-07T143102.368711Z.json.gz"
NOW = datetime(2026, 11, 15, 12, tzinfo=UTC)
SECRET = "https://hc-ping.com/12345678-1234-1234-1234-123456789abc"
REPO = Path(__file__).resolve().parents[1]


def saved_envelope(name):
    return json.loads(gzip.decompress((FIXTURES / name).read_bytes()))


def put(root, source, at, **changes):
    envelope = saved_envelope(source)
    envelope.update(
        received_at=at.isoformat(), requested_at=(at - timedelta(seconds=1)).isoformat()
    )
    envelope.update(changes)
    path = root / at.date().isoformat() / hb.filename(at)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(json.dumps(envelope).encode()))
    return path


@pytest.fixture
def healthy(tmp_path, monkeypatch):
    root = tmp_path / "kalshi"
    put(root, MARKET, NOW - timedelta(seconds=20))
    put(root, BATCH, NOW - timedelta(seconds=10))
    monkeypatch.setenv("KWB_HEARTBEAT_PING_URL", SECRET)
    calls = []
    monkeypatch.setattr(hb, "send_ping", lambda url, reason: calls.append((url, reason)))
    return root, calls


@pytest.mark.parametrize("book", [BATCH, SINGLE])
@pytest.mark.parametrize("age", [0, 299, 300])
def test_success_requires_fresh_2xx_market_and_order_book_envelopes_using_received_at(
    tmp_path, monkeypatch, book, age
):
    root = tmp_path / "kalshi"
    market_age = max(0, age - 0.5)
    book_age = age if age else 0.1
    put(root, MARKET, NOW - timedelta(seconds=market_age), http_status=201)
    put(root, book, NOW - timedelta(seconds=book_age), http_status=202)
    for path in root.rglob("*.gz"):
        os.utime(path, (0, 0))
    monkeypatch.setenv("KWB_HEARTBEAT_PING_URL", SECRET)
    calls = []
    monkeypatch.setattr(hb, "send_ping", lambda url, reason: calls.append(reason))
    assert hb.run_once(root, NOW) == 0
    assert calls == [None]


@pytest.mark.parametrize(
    "case",
    [
        "stale",
        "future",
        "missing_market",
        "missing_book",
        "market_status",
        "book_status",
        "mtime",
        "empty_markets",
        "empty_orderbooks",
        "unparseable_market",
        "unparseable_book",
        "market_list_type",
        "book_list_type",
        "fallback_empty_object",
        "fallback_invalid_level",
        "wrong_endpoint",
        "timestamp_mismatch",
        "naive_timestamp",
    ],
)
def test_invalid_status_timestamps_endpoints_and_response_bodies_never_count_as_evidence(
    healthy, case
):
    root, calls = healthy
    market = root / NOW.date().isoformat() / hb.filename(NOW - timedelta(seconds=20))
    book = root / NOW.date().isoformat() / hb.filename(NOW - timedelta(seconds=10))
    if case in {"stale", "future", "mtime"}:
        market.unlink()
        at = NOW + timedelta(seconds=1) if case == "future" else NOW - timedelta(seconds=301)
        stale = put(root, MARKET, at)
        os.utime(stale, (NOW.timestamp(), NOW.timestamp()))
    elif case == "missing_market":
        market.unlink()
    elif case == "missing_book":
        book.unlink()
    elif case == "market_status":
        put(root, MARKET, NOW - timedelta(seconds=20), http_status=500)
    elif case == "book_status":
        put(root, BATCH, NOW - timedelta(seconds=10), http_status=429)
    elif case.startswith("fallback"):
        book.unlink()
        body = (
            {"orderbook_fp": {}}
            if case.endswith("object")
            else {"orderbook_fp": {"yes_dollars": [["0.5", "bad"]], "no_dollars": []}}
        )
        put(root, SINGLE, NOW - timedelta(seconds=10), body=json.dumps(body))
    else:
        path = (
            market
            if case
            in {
                "empty_markets",
                "unparseable_market",
                "market_list_type",
                "timestamp_mismatch",
                "naive_timestamp",
            }
            else book
        )
        envelope = json.loads(gzip.decompress(path.read_bytes()))
        if case == "empty_markets":
            envelope["body"] = '{"markets": []}'
        elif case == "empty_orderbooks":
            envelope["body"] = '{"orderbooks": []}'
        elif case.startswith("unparseable"):
            envelope["body"] = "not JSON"
        elif case == "market_list_type":
            envelope["body"] = '{"markets": {"ticker": "X"}}'
        elif case == "book_list_type":
            envelope["body"] = '{"orderbooks": {"ticker": "X"}}'
        elif case == "wrong_endpoint":
            envelope["request_url"] = "https://external-api.kalshi.com/trade-api/v2/series"
        elif case == "timestamp_mismatch":
            envelope["received_at"] = (NOW - timedelta(seconds=400)).isoformat()
        elif case == "naive_timestamp":
            envelope["received_at"] = "2026-11-15T11:59:40"
        path.write_bytes(gzip.compress(json.dumps(envelope).encode()))
    assert hb.run_once(root, NOW) == 1
    assert calls and calls[0][1] is not None


@pytest.mark.parametrize(
    "case",
    [
        "empty",
        "missing",
        "gzip",
        "json",
        "envelope",
        "unreadable",
        "exception",
        "scan_exception",
        "duplicate",
        "nonfinite",
    ],
)
def test_every_unhealthy_or_exceptional_check_sends_failure_or_nothing_and_never_success(
    healthy, monkeypatch, case
):
    root, calls = healthy
    if case == "empty":
        shutil.rmtree(root / NOW.date().isoformat())
    elif case == "missing":
        shutil.rmtree(root)
    elif case in {"gzip", "json", "envelope", "duplicate", "nonfinite"}:
        if case == "gzip":
            # Under the new policy, unreadable input is unhealthy only when no
            # usable evidence remains. Healthy-data masking has its own test.
            for original in root.rglob("*.json.gz"):
                original.unlink()
        path = root / NOW.date().isoformat() / hb.filename(NOW - timedelta(seconds=5))
        payload = {
            "json": b"{",
            "envelope": b"{}",
            "duplicate": b'{"a":1,"a":2}',
            "nonfinite": b'{"x":NaN}',
        }.get(case, b"broken gzip")
        path.write_bytes(payload if case == "gzip" else gzip.compress(payload))
    else:

        def fail(*args, **kwargs):
            raise PermissionError("unreadable") if case == "unreadable" else RuntimeError("failure")

        monkeypatch.setattr(
            hb,
            "os" if case == "scan_exception" else "read_envelope",
            hb.os if case == "scan_exception" else fail,
        )
        if case == "scan_exception":
            monkeypatch.setattr(hb.os, "scandir", fail)
    assert hb.run_once(root, NOW) == 1
    assert calls and all(reason is not None for _, reason in calls)


def test_fresh_envelopes_from_yesterday_count_after_utc_midnight(tmp_path):
    now = datetime(2026, 11, 15, 0, 0, 30, tzinfo=UTC)
    put(tmp_path, MARKET, now - timedelta(seconds=90))
    put(tmp_path, BATCH, now - timedelta(seconds=60))
    hb.inspect_collection(tmp_path, now)


def test_the_freshness_window_never_exceeds_compaction_grace():
    assert hb.WINDOW_SECONDS == 300
    assert timedelta(seconds=hb.WINDOW_SECONDS) <= compact.GRACE


def test_compaction_cannot_remove_envelopes_inside_the_freshness_window(tmp_path):
    """Compaction groups by filename stamp; the filename stamp equals received_at.

    A previous hour's stamps strictly precede its end. At end + grace every
    envelope in that hour is older than the window, even when window == grace.
    Assert the stamp/received_at equality on real, unmodified saved envelopes.
    """
    root = tmp_path / "raw"
    directory = root / "kalshi/2026-11-10"
    directory.mkdir(parents=True)
    for name in (MARKET, BATCH):
        envelope = saved_envelope(name)
        received = hb.timestamp(envelope["received_at"])
        assert hb.filename(received) == name
        assert compact.ORIGINAL.fullmatch(name)
        shutil.copyfile(FIXTURES / name, directory / name)
    end = datetime(2026, 11, 10, 23, tzinfo=UTC)
    before = end + compact.GRACE - timedelta(microseconds=1)
    compact.compact(root, before)
    assert len(list(directory.glob("*.json.gz"))) == 2
    for name in (MARKET, BATCH):
        assert end + compact.GRACE - hb.timestamp(saved_envelope(name)["received_at"]) > timedelta(
            seconds=hb.WINDOW_SECONDS
        )
    # A fresh envelope in the next hour must remain loose at the boundary.
    fresh = put(root / "kalshi", MARKET, end + compact.GRACE - timedelta(seconds=1))
    compact.compact(root, end + compact.GRACE)
    assert fresh.exists()
    assert not (directory / MARKET).exists()
    assert not (directory / BATCH).exists()


def test_the_check_scans_only_window_dates_and_stops_reading_when_evidence_is_complete(
    healthy, monkeypatch
):
    root, calls = healthy
    old = root / "2000-01-01"
    old.mkdir()
    (old / "corrupt.json.gz").write_bytes(b"bad")
    # An older malformed envelope and an archive in today's directory are not needed.
    earlier = root / NOW.date().isoformat() / hb.filename(NOW - timedelta(seconds=200))
    earlier.write_bytes(b"bad")
    (earlier.parent / "2026-11-15T11.tar.zst").write_bytes(b"bad")
    scanned, opened = [], []
    real_scan, real_read = hb.os.scandir, hb.read_envelope

    def scan(path):
        scanned.append(Path(path))
        assert Path(path) == root / NOW.date().isoformat()
        return real_scan(path)

    def read(path):
        opened.append(path)
        assert path != earlier
        return real_read(path)

    monkeypatch.setattr(hb.os, "scandir", scan)
    monkeypatch.setattr(hb, "read_envelope", read)
    assert hb.run_once(root, NOW) == 0
    assert len(scanned) == 1
    assert len(opened) == 2
    assert calls == [(SECRET, None)]


@pytest.mark.parametrize(
    "budget",
    ["MAX_DIRECTORY_ENTRIES", "MAX_ENVELOPES", "MAX_ENVELOPE_BYTES", "CHECK_BUDGET_SECONDS"],
)
def test_exhausting_any_check_budget_prevents_success(healthy, monkeypatch, budget):
    root, calls = healthy
    monkeypatch.setattr(hb, budget, 0)
    assert hb.run_once(root, NOW) == 1
    assert calls[0][1] is not None


def test_importing_the_heartbeat_does_not_load_the_analytical_store_or_dataframe_libraries():
    code = """
import sys
import kwb.ops.heartbeat
for prefix in ('kwb.store', 'duckdb', 'pyarrow', 'polars'):
    assert not any(m == prefix or m.startswith(prefix + '.') for m in sys.modules), prefix
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("stage", ["configuration", "inspection", "transport"])
def test_the_ping_secret_never_appears_in_logs_exception_messages_or_failure_bodies(
    healthy, monkeypatch, caplog, stage
):
    root, calls = healthy
    caplog.set_level(logging.INFO)

    def fail(*args, **kwargs):
        raise RuntimeError(SECRET)

    if stage == "configuration":
        monkeypatch.setenv("KWB_HEARTBEAT_PING_URL", SECRET + "invalid")
    elif stage == "inspection":
        monkeypatch.setattr(hb, "inspect_collection", fail)
    else:
        monkeypatch.setattr(hb, "send_ping", fail)
    assert hb.run_once(root, NOW) == 1
    assert SECRET not in caplog.text
    assert all(SECRET not in (reason or "") for _, reason in calls)
    # The transport helper also sanitizes exceptions when called directly.
    monkeypatch.undo()
    opener = MagicMock()
    opener.open.side_effect = RuntimeError(SECRET)
    monkeypatch.setattr(hb, "build_opener", lambda *args: opener)
    with pytest.raises(hb.CheckError) as caught:
        hb.send_ping(SECRET, "test")
    assert SECRET not in str(caught.value)
    assert SECRET not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize("failure", ["timeout", "http", "not_found", "rate_limited", "redirect"])
def test_ping_failures_exit_nonzero_without_retries(healthy, monkeypatch, caplog, failure):
    root, _ = healthy
    monkeypatch.undo()
    monkeypatch.setenv("KWB_HEARTBEAT_PING_URL", SECRET)
    opener = MagicMock()
    response = opener.open.return_value.__enter__.return_value
    response.status = 200 if failure in {"not_found", "rate_limited"} else 302
    response.read.return_value = b"not found" if failure == "not_found" else b"rate limited"
    if failure in {"timeout", "http"}:
        opener.open.side_effect = TimeoutError(SECRET)
    monkeypatch.setattr(hb, "build_opener", lambda *args: opener)
    assert hb.run_once(root, NOW) == 1
    assert opener.open.call_count == 1
    assert opener.open.call_args.kwargs["timeout"] == hb.HTTP_TIMEOUT_SECONDS <= 5
    assert "Heartbeat ping failed" in caplog.text
    assert SECRET not in caplog.text
    assert hb.NoRedirects().redirect_request(None, None, None, None, None, SECRET) is None


def test_failure_pings_use_the_fail_endpoint_and_a_bounded_reason_body(monkeypatch):
    opener = MagicMock()
    response = opener.open.return_value.__enter__.return_value
    response.status = 200
    response.read.return_value = b"OK"
    monkeypatch.setattr(hb, "build_opener", lambda *args: opener)
    hb.send_ping(SECRET, "x" * 2000)
    request = opener.open.call_args.args[0]
    assert request.full_url == SECRET + "/fail"
    assert request.method == "POST"
    assert len(request.data) == hb.MAX_REASON_BYTES < 100_000


def unit(name):
    config = configparser.ConfigParser(interpolation=None)
    config.read(REPO / "deploy" / name)
    return config


def test_the_heartbeat_unit_preserves_collection_priority_and_uses_the_external_secret_file():
    service = unit("kalshi-heartbeat.service")
    compaction = unit("raw-compaction.service")
    for option in ("CPUQuota", "CPUWeight", "IOWeight", "Nice"):
        assert service["Service"][option] == compaction["Service"][option]
    assert service["Service"]["EnvironmentFile"] == "/etc/kwb/heartbeat.env"
    assert hb.PING_URL_VARIABLE == "KWB_HEARTBEAT_PING_URL"
    assert service["Service"]["ExecStart"] == (
        "/home/prathik/.local/bin/uv run python -m kwb.ops.heartbeat"
    )
    assert service["Service"]["TimeoutStartSec"] == "120s"
    assert "kalshi-collector" not in (REPO / "deploy/kalshi-heartbeat.service").read_text()
    assert "Restart" not in service["Service"]


def test_freshness_and_timer_behavior_use_the_named_thresholds(healthy):
    root, calls = healthy
    assert hb.run_once(root, NOW, window=1) == 1
    assert calls[0][1] is not None
    assert hb.run_once(root, NOW, window=hb.WINDOW_SECONDS) == 0
    timer = unit("kalshi-heartbeat.timer")
    assert timer["Timer"]["OnCalendar"] == "*-*-* *:0/5:00 UTC"
    assert timer["Timer"]["Persistent"] == "true"
    assert timer["Timer"]["AccuracySec"] == "1s"
    assert timer["Timer"]["Unit"] == "kalshi-heartbeat.service"


def test_the_cli_uses_the_named_default_window_and_returns_the_check_exit_status(monkeypatch):
    observed = []
    monkeypatch.setattr(hb, "WINDOW_SECONDS", 42)
    monkeypatch.setattr(hb, "run_once", lambda root, now, window: observed.append(window) or 1)
    assert hb.main([]) == 1
    assert observed == [42]


def test_empty_listing_responses_do_not_hide_other_fresh_nonempty_evidence(healthy):
    root, calls = healthy
    put(root, MARKET, NOW - timedelta(seconds=2), body='{"markets": []}')
    put(root, BATCH, NOW - timedelta(seconds=1), body='{"orderbooks": []}')
    assert hb.run_once(root, NOW) == 0
    assert calls == [(SECRET, None)]


def test_a_well_formed_fallback_book_with_no_liquidity_counts_as_evidence(healthy):
    root, calls = healthy
    put(
        root,
        SINGLE,
        NOW - timedelta(seconds=1),
        body=json.dumps({"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}),
    )
    assert hb.run_once(root, NOW) == 0
    assert calls == [(SECRET, None)]


def test_even_check_errors_containing_the_secret_are_replaced_by_a_fixed_reason(
    healthy, monkeypatch
):
    root, calls = healthy

    def fail(*args):
        raise hb.CheckError(SECRET)

    monkeypatch.setattr(hb, "inspect_collection", fail)
    assert hb.run_once(root, NOW) == 1
    assert calls == [(SECRET, "collection_check_exception")]


def small_market(root, at):
    """Project a real market to the ticker field needed by this liveness check."""
    market = json.loads(saved_envelope(MARKET)["body"])["markets"][0]
    return put(root, MARKET, at, body=json.dumps({"markets": [{"ticker": market["ticker"]}]}))


def test_an_oversized_real_series_envelope_between_fresh_markets_and_older_books_preserves_success(
    tmp_path, monkeypatch, caplog
):
    assert hb.MAX_ENVELOPE_BYTES == 16 * 1024 * 1024
    monkeypatch.setattr(hb, "MAX_ENVELOPE_BYTES", 1500)
    monkeypatch.setenv("KWB_HEARTBEAT_PING_URL", SECRET)
    calls = []
    monkeypatch.setattr(hb, "send_ping", lambda url, reason: calls.append(reason))
    caplog.set_level(logging.INFO)
    market = small_market(tmp_path, NOW - timedelta(seconds=2))
    # This committed real /series response retains two complete series records.
    series = put(tmp_path, "settlements/discovery.json.gz", NOW - timedelta(seconds=5))
    book = put(tmp_path, SINGLE, NOW - timedelta(seconds=10))
    assert len(gzip.decompress(market.read_bytes())) <= hb.MAX_ENVELOPE_BYTES
    assert len(gzip.decompress(book.read_bytes())) <= hb.MAX_ENVELOPE_BYTES
    assert len(gzip.decompress(series.read_bytes())) > hb.MAX_ENVELOPE_BYTES
    assert hb.run_once(tmp_path, NOW) == 0
    assert calls == [None]
    assert "Heartbeat reported success skipped_oversized=1" in caplog.text
    assert SECRET not in caplog.text
    assert str(tmp_path) not in caplog.text


def test_oversized_only_order_book_candidates_still_fail_with_missing_fresh_evidence(
    tmp_path, monkeypatch, caplog
):
    monkeypatch.setattr(hb, "MAX_ENVELOPE_BYTES", 1500)
    monkeypatch.setenv("KWB_HEARTBEAT_PING_URL", SECRET)
    calls = []
    monkeypatch.setattr(hb, "send_ping", lambda url, reason: calls.append(reason))
    caplog.set_level(logging.INFO)
    small_market(tmp_path, NOW - timedelta(seconds=2))
    put(tmp_path, "settlements/discovery.json.gz", NOW - timedelta(seconds=5))
    for age in (10, 15):
        book = put(tmp_path, BATCH, NOW - timedelta(seconds=age))
        assert len(gzip.decompress(book.read_bytes())) > hb.MAX_ENVELOPE_BYTES
    stats = hb.CheckStats()
    with pytest.raises(hb.CheckError, match="^missing_fresh_market_or_orderbook_evidence$"):
        hb.inspect_collection(tmp_path, NOW, stats=stats)
    assert stats.skipped_oversized == 3
    assert hb.run_once(tmp_path, NOW) == 1
    assert calls == ["missing_fresh_market_or_orderbook_evidence"]
    assert (
        "Heartbeat reported failure: missing_fresh_market_or_orderbook_evidence skipped_oversized=3"
        in caplog.text
    )
    assert SECRET not in caplog.text
    assert str(tmp_path) not in caplog.text


INVALID_BATCH_BODIES = {
    "malformed_json": {"body": "{"},
    "nesting_limit": {"body": "[" * 2000 + "0" + "]" * 2000},
    "base64": {"body": "/w==", "body_encoding": "base64"},
    "non_object": {"body": "[]"},
    "missing_list": {"body": "{}"},
    "wrong_list": {"body": '{"orderbooks": {}}'},
    "empty_list": {"body": '{"orderbooks": []}'},
    "bad_record": {"body": '{"orderbooks": [null]}'},
    "bad_ticker": {"body": '{"orderbooks": [{"ticker": 42, "orderbook_fp": {}}]}'},
    "bad_book": {"body": '{"orderbooks": [{"ticker": "X", "orderbook_fp": {}}]}'},
    "bad_level": {
        "body": json.dumps(
            {
                "orderbooks": [
                    {
                        "ticker": "X",
                        "orderbook_fp": {"yes_dollars": [["0.5", "bad"]], "no_dollars": []},
                    }
                ]
            }
        )
    },
    "duplicate_json": {"body": '{"orderbooks": [], "orderbooks": []}'},
    "nonfinite_json": {"body": '{"orderbooks": NaN}'},
}


@pytest.mark.parametrize("case", INVALID_BATCH_BODIES)
def test_invalid_batch_bodies_do_not_mask_valid_fallback_books_and_markets(healthy, caplog, case):
    root, calls = healthy
    caplog.set_level(logging.INFO)
    # Arrange the real collector flow: markets, failed batch, then fallback.
    put(root, MARKET, NOW - timedelta(seconds=30))
    put(root, BATCH, NOW - timedelta(seconds=20), **INVALID_BATCH_BODIES[case])
    put(root, SINGLE, NOW - timedelta(seconds=10))
    assert hb.run_once(root, NOW) == 0
    assert calls == [(SECRET, None)]
    assert (
        "Heartbeat reported success skipped_oversized=0 "
        "skipped_invalid_body=1 skipped_unreadable=0" in caplog.text
    )
    assert SECRET not in caplog.text
    assert str(root) not in caplog.text


@pytest.mark.parametrize("case", INVALID_BATCH_BODIES)
def test_a_window_of_only_invalid_response_bodies_fails_with_missing_fresh_evidence(
    healthy, caplog, case
):
    root, calls = healthy
    caplog.set_level(logging.INFO)
    put(root, MARKET, NOW - timedelta(seconds=20), **INVALID_BATCH_BODIES[case])
    put(root, BATCH, NOW - timedelta(seconds=10), **INVALID_BATCH_BODIES[case])
    assert hb.run_once(root, NOW) == 1
    assert calls == [(SECRET, "missing_fresh_market_or_orderbook_evidence")]
    assert (
        "Heartbeat reported failure: missing_fresh_market_or_orderbook_evidence "
        "skipped_oversized=0 skipped_invalid_body=2 skipped_unreadable=0" in caplog.text
    )


@pytest.mark.parametrize("damage", ["not_gzip", "truncated", "crc", "permission"])
def test_unreadable_gzip_does_not_mask_older_valid_evidence(healthy, monkeypatch, caplog, damage):
    root, calls = healthy
    caplog.set_level(logging.INFO)
    bad = put(root, BATCH, NOW - timedelta(seconds=5))
    data = bad.read_bytes()
    if damage == "not_gzip":
        bad.write_bytes(b"broken gzip")
    elif damage == "truncated":
        bad.write_bytes(data[:-4])
    elif damage == "crc":
        corrupt = bytearray(data)
        corrupt[-8] ^= 1
        bad.write_bytes(corrupt)
    else:
        real_open = hb.gzip.open

        def open_or_fail(path, *args, **kwargs):
            if path == bad:
                raise PermissionError(SECRET)
            return real_open(path, *args, **kwargs)

        monkeypatch.setattr(hb.gzip, "open", open_or_fail)
    assert hb.run_once(root, NOW) == 0
    assert calls == [(SECRET, None)]
    assert (
        "Heartbeat reported success skipped_oversized=0 "
        "skipped_invalid_body=0 skipped_unreadable=1" in caplog.text
    )
    assert SECRET not in caplog.text
    assert str(root) not in caplog.text


def test_a_window_of_only_unreadable_gzip_fails_with_missing_fresh_evidence(healthy, caplog):
    root, calls = healthy
    caplog.set_level(logging.INFO)
    for path in root.rglob("*.json.gz"):
        path.write_bytes(path.read_bytes()[:-4])
    assert hb.run_once(root, NOW) == 1
    assert calls == [(SECRET, "missing_fresh_market_or_orderbook_evidence")]
    assert (
        "Heartbeat reported failure: missing_fresh_market_or_orderbook_evidence "
        "skipped_oversized=0 skipped_invalid_body=0 skipped_unreadable=2" in caplog.text
    )


@pytest.mark.parametrize(
    "fault",
    [
        "missing_key",
        "extra_key",
        "timestamp",
        "origin",
        "non_file",
        "envelope_json",
        "encoding",
        "body_type",
    ],
)
def test_structural_archive_faults_still_fail_when_older_valid_evidence_exists(
    healthy, caplog, fault
):
    root, calls = healthy
    caplog.set_level(logging.INFO)
    newest = put(root, SINGLE, NOW - timedelta(seconds=1))
    envelope = json.loads(gzip.decompress(newest.read_bytes()))
    expected_reason = "malformed_envelope"
    if fault == "missing_key":
        del envelope["request_params"]
    elif fault == "extra_key":
        envelope["unexpected"] = "field"
    elif fault == "timestamp":
        envelope["received_at"] = (NOW - timedelta(seconds=2)).isoformat()
        expected_reason = "inconsistent_envelope_timestamp"
    elif fault == "origin":
        envelope["request_url"] = "https://example.invalid/trade-api/v2/markets/X/orderbook"
        expected_reason = "unexpected_envelope_origin"
    elif fault == "non_file":
        newest.unlink()
        newest.mkdir()
        expected_reason = "unexpected_envelope_type"
    elif fault == "encoding":
        envelope["body_encoding"] = "unknown"
    elif fault == "body_type":
        envelope["body"] = {}
    if fault == "envelope_json":
        newest.write_bytes(gzip.compress(b"{"))
        expected_reason = "unreadable_or_malformed_evidence"
    elif fault != "non_file":
        newest.write_bytes(gzip.compress(json.dumps(envelope).encode()))
    assert hb.run_once(root, NOW) == 1
    assert calls == [(SECRET, expected_reason)]
    assert "skipped_oversized=0 skipped_invalid_body=0 skipped_unreadable=0" in caplog.text


def test_all_skip_counts_survive_a_later_ping_failure(healthy, monkeypatch, caplog):
    root, calls = healthy
    caplog.set_level(logging.INFO)
    put(root, BATCH, NOW - timedelta(seconds=5), body="{")
    bad = put(root, BATCH, NOW - timedelta(seconds=6))
    bad.write_bytes(bad.read_bytes()[:-4])

    def fail(*args):
        raise RuntimeError(SECRET)

    monkeypatch.setattr(hb, "send_ping", fail)
    assert hb.run_once(root, NOW) == 1
    assert (
        "Heartbeat ping failed skipped_oversized=0 "
        "skipped_invalid_body=1 skipped_unreadable=1" in caplog.text
    )
    assert SECRET not in caplog.text
