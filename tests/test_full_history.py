"""Boundary tests for the real-data full-history verification scripts."""

import copy
import importlib
import inspect
import json
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from test_kalshi_parse_store import BATCH, MARKETS, archive, envelope, write_envelope
from test_settlements import acquisition

from kwb.parse.kalshi import parse
from kwb.parse.settlements import parse as parse_settlements
from kwb.store import Store

ROOT = Path(__file__).resolve().parents[1]


def script(name):
    # scripts/ is deliberately outside the installed model/trading package.
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    return importlib.import_module("scripts." + name)


oracle = script("full_history_oracle")
audit = script("check_full_history")
FIRST = datetime(2026, 11, 10, 22, 0, 44, 749404, tzinfo=UTC)
SERIES = ["KXHIGHAUS", "KXHIGHCHI", "KXHIGHDEN", "KXHIGHLAX", "KXHIGHMIA", "KXHIGHNY", "KXHIGHPHIL"]


@pytest.fixture
def audit_data(tmp_path):
    raw = tmp_path / "raw/kalshi/2026-11-10"
    first = write_envelope(raw, envelope(MARKETS))
    book = write_envelope(raw, envelope(BATCH))
    later = copy.deepcopy(envelope(MARKETS))
    body = json.loads(later["body"])
    for row in body["markets"]:
        row["yes_bid_dollars"] = "0.123456789"
    later["body"] = json.dumps(body)
    second = write_envelope(raw, later, at=FIRST + timedelta(minutes=4))
    database = tmp_path / "oracle.sqlite"
    oracle.build_index(tmp_path / "raw/kalshi", database, SERIES)
    store = Store(tmp_path / "data/kalshi")
    parse([first, book, second], store)
    output = tmp_path / "output"
    output.mkdir()
    return tmp_path, store, database, output


def test_the_coverage_ledger_includes_every_minute_gap_and_hour_source(audit_data):
    root, _, database, output = audit_data
    result = oracle.coverage(database, SERIES, root / "raw/kalshi", [], output)
    rows = (output / "minutes.csv").read_text().splitlines()
    assert len(rows) == 1 + 5 * len(SERIES)
    assert any("22:02:00+00:00,KXHIGHLAX,0,0" in row for row in rows)
    gap = next(g for g in result["gaps"] if g["series"] == "KXHIGHLAX" and g["feed"] == "markets")
    assert gap["duration_seconds"] == 240
    assert gap["start"] == FIRST.isoformat()
    assert gap["end"] == (FIRST + timedelta(minutes=4)).isoformat()
    assert "Mac-local only" in (output / "hours.csv").read_text()
    assert result["series"]["KXHIGHMIA"]["markets"]["first"] is None
    assert result["series"]["KXHIGHMIA"]["markets"]["absent_while_others_present"]
    assert result["series"]["KXHIGHLAX"]["markets"]["first"] == FIRST.isoformat()
    assert (
        result["series"]["KXHIGHLAX"]["markets"]["last"]
        == (FIRST + timedelta(minutes=4)).isoformat()
    )


def test_every_successful_configured_envelope_reconciles_to_rows_or_an_explained_empty(audit_data):
    _, store, database, output = audit_data
    result = audit.reconciliation(store, database, output)
    assert result["rows"]["market_snapshots"] == 12
    assert result["unmatched"] == 0
    original = store._audit_rows

    def missing_row():
        skipped = False
        for item in original():
            if item[1] == "market_snapshots" and not skipped:
                skipped = True
                continue
            yield item

    store._audit_rows = missing_row
    with pytest.raises(AssertionError, match="row count"):
        audit.reconciliation(store, database, output)


def test_the_independent_raw_oracle_matches_point_in_time_market_and_book_results(audit_data):
    root, _, database, output = audit_data
    baseline = output / "baseline"
    baseline.mkdir()
    tasks = [
        (str(root), str(database), SERIES, at, str(baseline))
        for at in (
            FIRST - timedelta(microseconds=1),
            FIRST,
            FIRST + timedelta(minutes=2),
            FIRST + timedelta(minutes=4),
            FIRST + timedelta(days=4),
        )
    ]
    for task in tasks:
        result = audit.compare_moment(task)
        assert result["counts"]["market_calls"] == len(SERIES)
    db = oracle.connect(database, readonly=True)
    at = FIRST + timedelta(minutes=4)
    expected = oracle.expected(db, "markets", "KXHIGHLAX", at)
    assert {row["yes_bid_dollars"] for row in expected.values()} == {"0.123456789"}
    db.close()


def test_no_accessor_returns_future_rows_at_the_audit_moments(audit_data):
    root, store, database, output = audit_data
    baseline = output / "baseline"
    baseline.mkdir()
    # Raw-backed comparisons exercise every public accessor and level cutoff.
    audit.compare_moment((str(root), str(database), SERIES, FIRST, str(baseline)))
    later = store.get_market_snapshots("KXHIGHLAX", FIRST + timedelta(minutes=4))
    assert later
    with pytest.raises(AssertionError, match="Future row"):
        audit.require_visible(later, FIRST)


def test_a_second_full_parse_adds_no_rows_and_preserves_the_manifest_bytes(audit_data):
    root, store, database, output = audit_data
    audit.reconciliation(store, database, output)
    parse([root / "raw/kalshi"], store)
    assert audit.verify_rerun(store, output)["manifest_byte_identical"]
    manifest = store.root / "manifest.json"
    manifest.write_text(manifest.read_text() + "\n")
    with pytest.raises(AssertionError, match="manifest bytes"):
        audit.verify_rerun(store, output)


def test_full_history_market_parses_preserve_all_settlement_accessor_results(tmp_path):
    store = Store(tmp_path / "data/kalshi")
    parse_settlements(acquisition(tmp_path / "settlements", at=FIRST - timedelta(seconds=4)), store)
    later = FIRST + timedelta(days=2)
    before = store.get_settlements("KXHIGHLAX", later)
    assert len(before) == 6
    parse([MARKETS, BATCH], store)
    assert store.get_settlements("KXHIGHLAX", later) == before
    parse([MARKETS, BATCH], store)
    assert store.get_settlements("KXHIGHLAX", later) == before


def test_the_audit_inventory_is_private_has_no_as_of_and_has_no_production_callers():
    method = Store._audit_rows
    assert method.__name__.startswith("_")
    assert list(inspect.signature(method).parameters) == ["self"]
    assert "Audit-only" in method.__doc__
    offenders = [
        str(path)
        for path in (ROOT / "src/kwb").rglob("*.py")
        if path.name != "store.py" and "_audit_rows" in path.read_text()
    ]
    assert offenders == []


def test_the_raw_oracle_imports_without_any_production_module():
    code = """
import builtins, runpy, sys
original=builtins.__import__
def guarded(name,*args,**kwargs):
    if name=='kwb' or name.startswith('kwb.'):
        raise AssertionError('Oracle depends on production code')
    return original(name,*args,**kwargs)
builtins.__import__=guarded
runpy.run_path(sys.argv[1])
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(ROOT / "scripts/full_history_oracle.py")],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_identical_raw_members_are_counted_once_and_conflicting_copies_raise(tmp_path):
    raw = write_envelope(tmp_path / "raw/2026-11-10", envelope(MARKETS))
    packed = archive(tmp_path / "raw/2026-11-10/2026-11-10T22.tar.zst", [raw])
    members = list(oracle.envelopes([raw, packed]))
    assert len(members) == 2 and sum(env is None for _, _, env in members) == 1
    env = envelope(raw)
    env["body"] += " "
    changed = write_envelope(tmp_path / "other/2026-11-10", env)
    with pytest.raises(ValueError, match="conflicting duplicate"):
        list(oracle.envelopes([raw, changed]))


def test_empty_markets_and_empty_books_are_explicitly_reconciled(tmp_path):
    raw = tmp_path / "raw/kalshi/2026-11-10"
    market = envelope(MARKETS)
    market["body"] = json.dumps({"markets": [], "cursor": ""})
    write_envelope(raw, market)
    books = envelope(BATCH)
    body = json.loads(books["body"])
    for item in body["orderbooks"]:
        item["orderbook_fp"] = {"yes_dollars": [], "no_dollars": []}
    books["body"] = json.dumps(body)
    write_envelope(raw, books)
    dbpath = tmp_path / "oracle.sqlite"
    oracle.build_index(tmp_path / "raw/kalshi", dbpath, SERIES)
    db = sqlite3.connect(dbpath)
    assert (
        db.execute("SELECT explained_empty FROM envelopes WHERE kind='markets'").fetchone()[0] == 1
    )
    assert (
        db.execute("SELECT sum(explained_empty) FROM envelopes WHERE kind='books'").fetchone()[0]
        > 0
    )
    db.close()
    store = Store(tmp_path / "store")
    parse([raw], store)
    result = audit.reconciliation(store, dbpath, tmp_path)
    assert result["rows"]["market_snapshots"] == 0
    assert result["rows"]["orderbook_levels"] == 0
    assert result["rows"]["orderbook_snapshots"] > 0


def test_uniform_sampling_is_repeatable_and_respects_the_approved_floor_and_budget():
    coverage = {
        "first": FIRST.isoformat(),
        "last": (FIRST + timedelta(days=10)).isoformat(),
        "gaps": [{"start": FIRST.isoformat(), "end": (FIRST + timedelta(hours=2)).isoformat()}],
    }
    moments = audit.audit_moments(coverage, 100)
    assert moments == audit.audit_moments(coverage, 100)
    assert len(moments) == 103
    assert FIRST in moments and FIRST + timedelta(hours=2) in moments
    assert audit.choose_count(1000, 30) == 100
    assert audit.choose_count(1, 30) == 300
    assert audit.choose_count(30, 30) == 174


def test_single_book_fallbacks_and_later_empty_books_match_the_raw_oracle(tmp_path):
    from test_kalshi_parse_store import SINGLE

    raw = tmp_path / "raw/kalshi/2026-10-07"
    value = envelope(SINGLE)
    first = write_envelope(raw, value)
    at = oracle.timestamp(value["received_at"])
    value["body"] = json.dumps({"orderbook_fp": {"yes_dollars": [], "no_dollars": []}})
    second = write_envelope(raw, value, at=at + timedelta(seconds=1))
    database = tmp_path / "oracle.sqlite"
    oracle.build_index(tmp_path / "raw/kalshi", database, SERIES)
    store = Store(tmp_path / "data/kalshi")
    parse([first, second], store)
    baseline = tmp_path / "baseline"
    baseline.mkdir()
    for moment in (at - timedelta(microseconds=1), at, at + timedelta(seconds=1)):
        audit.compare_moment((str(tmp_path), str(database), SERIES, moment, str(baseline)))
    db = oracle.connect(database, readonly=True)
    result = oracle.expected(db, "books", "KXHIGHCHI", at + timedelta(seconds=1))
    assert len(result) == 1 and next(iter(result.values()))["levels"] == []
    db.close()


def test_a_shortened_latest_close_time_does_not_resurrect_an_older_market_schedule(audit_data):
    root, store, _, output = audit_data
    value = envelope(MARKETS)
    body = json.loads(value["body"])
    at = FIRST + timedelta(minutes=5)
    for row in body["markets"]:
        row["close_time"] = (at - timedelta(seconds=1)).isoformat()
    value["body"] = json.dumps(body)
    path = write_envelope(root / "raw/kalshi/2026-11-10", value, at=at)
    parse([path], store)
    database = output / "revised.sqlite"
    oracle.build_index(root / "raw/kalshi", database, SERIES)
    baseline = output / "baseline"
    baseline.mkdir()
    audit.compare_moment((str(root), str(database), SERIES, at, str(baseline)))
    db = oracle.connect(database, readonly=True)
    assert oracle.expected(db, "markets", "KXHIGHLAX", at) == {}
    db.close()


def test_every_successful_http_status_is_included_in_the_raw_envelope_accounting(tmp_path):
    value = envelope(MARKETS)
    value["http_status"] = 201
    write_envelope(tmp_path / "raw/kalshi/2026-11-10", value)
    database = tmp_path / "oracle.sqlite"
    oracle.build_index(tmp_path / "raw/kalshi", database, SERIES)
    db = oracle.connect(database, readonly=True)
    assert db.execute("SELECT configured_items FROM envelopes WHERE status=201").fetchone()[0] == 6
    db.close()


def test_the_original_manifest_capture_is_immutable_even_after_the_store_changes(audit_data):
    root, store, _, output = audit_data
    (root / "state").mkdir(exist_ok=True)
    (root / "state/raw-download.json").write_text(json.dumps({"objects": {}}))
    audit.capture(root, output)
    original = (store.root / "manifest.json").read_bytes()
    assert (output / "settlements-before/manifest.json").read_bytes() == original
    (store.root / "manifest.json").write_text(original.decode() + "\n")
    with pytest.raises(ValueError, match="refusing to replace"):
        audit.capture(root, output)
    assert (output / "settlements-before/manifest.json").read_bytes() == original
