"""Deliberately violate audit properties in isolation, require failure, restore.

Only checkout code is mutated. Every pytest store is temporary; published data
and the collector VM are never modified. Run this script alone, not with pytest.
"""

import hashlib
import importlib.util
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT = Path("/Users/prathik/code/Kalshi-Weather-Bot/state/full-history-parse-2026-11-16/proofs")
TESTS = "tests/test_full_history.py::"
CASES = [
    (
        "1-coverage",
        "scripts/full_history_oracle.py",
        "if (end - start).total_seconds() > 120:",
        "if (end - start).total_seconds() > 10**20:",
        "test_the_coverage_ledger_includes_every_minute_gap_and_hour_source",
    ),
    (
        "2-reconciliation",
        "scripts/check_full_history.py",
        "if actual != expected:",
        "if False and actual != expected:",
        "test_every_successful_configured_envelope_reconciles_to_rows_or_an_explained_empty",
    ),
    (
        "3-oracle",
        "scripts/full_history_oracle.py",
        "ORDER BY at DESC,id DESC LIMIT 1",
        "ORDER BY at ASC,id ASC LIMIT 1",
        "test_the_independent_raw_oracle_matches_point_in_time_market_and_book_results",
    ),
    (
        "4-leakage",
        "src/kwb/store.py",
        "WHERE series_ticker = ? AND available_at <= ?",
        "WHERE series_ticker = ? AND (? IS NOT NULL)",
        "test_no_accessor_returns_future_rows_at_the_audit_moments",
    ),
    (
        "5-rerun",
        "src/kwb/store.py",
        "if unchanged:",
        "if False and unchanged:",
        "test_a_second_full_parse_adds_no_rows_and_preserves_the_manifest_bytes",
    ),
    (
        "6-settlements",
        "src/kwb/store.py",
        'if record["source_id"] not in sources',
        'if record["source_id"] not in sources and name != "settlements"',
        "test_full_history_market_parses_preserve_all_settlement_accessor_results",
    ),
    (
        "duplicate-conflict",
        "scripts/full_history_oracle.py",
        "if seen[identity] != digest:",
        "if False and seen[identity] != digest:",
        "test_identical_raw_members_are_counted_once_and_conflicting_copies_raise",
    ),
    (
        "oracle-budget",
        "scripts/check_full_history.py",
        "selected = estimate(100)",
        "selected = estimate(1)",
        "test_the_call_budget_uses_actual_ticker_visibility_and_preserves_the_sampling_floor",
    ),
]


def run(test):
    return subprocess.run(
        ["uv", "run", "--locked", "--group", "ndfd", "pytest", "-q", TESTS + test],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )


def clear_bytecode(path):
    Path(importlib.util.cache_from_source(str(path))).unlink(missing_ok=True)


def main():
    REPORT.mkdir(parents=True, exist_ok=True)
    results = []
    for name, relative, old, new, test in CASES:
        path = ROOT / relative
        original = path.read_bytes()
        before = run(test)
        if before.returncode:
            raise AssertionError(f"Baseline fails: {test}\n{before.stdout}\n{before.stderr}")
        try:
            source = original.decode()
            if old not in source:
                raise AssertionError(f"Mutation anchor absent: {relative}: {old}")
            path.write_text(source.replace(old, new, 1))
            clear_bytecode(path)
            broken = run(test)
            (REPORT / (name + ".log")).write_text(broken.stdout + broken.stderr)
            if broken.returncode != 1 or "FAILED" not in broken.stdout:
                raise AssertionError(f"Mutant did not fail behaviorally: {name}\n{broken.stdout}")
        finally:
            path.write_bytes(original)
            clear_bytecode(path)
        restored = run(test)
        if restored.returncode:
            raise AssertionError(f"Restored test failed: {test}\n{restored.stdout}")
        result = {
            "property": name,
            "test": TESTS + test,
            "mutant_exit": broken.returncode,
            "restored_exit": restored.returncode,
            "original_sha256": hashlib.sha256(original).hexdigest(),
        }
        results.append(result)
        print(json.dumps(result), flush=True)
    # Architecture violation is a new production module, removed even if pytest crashes.
    path = ROOT / "src/kwb/ops/full_history_scope_mutant.py"
    if path.exists():
        raise AssertionError("Scope mutant path already exists")
    try:
        path.write_text("from kwb.store import Store\nreference = Store._audit_rows\n")
        test = "test_the_audit_inventory_is_private_has_no_as_of_and_has_no_production_callers"
        broken = run(test)
        (REPORT / "audit-scope.log").write_text(broken.stdout + broken.stderr)
        if broken.returncode != 1 or "FAILED" not in broken.stdout:
            raise AssertionError("Production audit reference was not detected")
    finally:
        path.unlink(missing_ok=True)
    restored = run(test)
    if restored.returncode:
        raise AssertionError("Restored architecture guard failed")
    results.append(
        {"property": "audit-scope", "test": TESTS + test, "mutant_exit": 1, "restored_exit": 0}
    )
    (REPORT / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print(f"Observed and restored {len(results)} deliberate failures", flush=True)


if __name__ == "__main__":
    main()
