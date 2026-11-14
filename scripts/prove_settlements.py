"""Run alone from the repo root: deliberately break properties and restore every source."""

import json
import subprocess
from pathlib import Path

root = Path.cwd()
out = root / "state/settlements/proofs"
out.mkdir(parents=True, exist_ok=True)
store = "src/kwb/store.py"
parser = "src/kwb/parse/settlements.py"
collector = "src/kwb/collectors/settlements.py"
cases = []


def case(name, file, old, new, test, section=None):
    cases.append((name, file, old, new, test, section))


base = "tests/test_settlements.py::"
case(
    "availability",
    parser,
    "**snapshot.model_dump(),",
    '**{**snapshot.model_dump(), "available_at": served.settlement_ts, '
    '"available_date": served.settlement_ts.date()},',
    base + "test_every_settlement_uses_its_envelopes_received_at_and_is_invisible_before_fetch",
)
for method in ["get_market_snapshots", "get_market_history", "get_orderbook", "get_settlements"]:
    case(
        "leakage-" + method,
        store,
        "available_at <= ?",
        "available_at <= ? + INTERVAL 1 DAY",
        base + "test_no_accessor_returns_future_rows_at_microsecond_boundaries",
        method,
    )
    case(
        "equality-" + method,
        store,
        "available_at <= ?",
        "available_at < ?",
        base + "test_no_accessor_returns_future_rows_at_microsecond_boundaries",
        method,
    )
case(
    "event-consistency",
    parser,
    "if event not in exceptions and (",
    "if False and event not in exceptions and (",
    base + "test_each_unlisted_finalized_event_has_six_markets_and_exactly_one_yes_or_raises",
)
case(
    "exception-label",
    parser,
    "label=None if listed else served.result,",
    "label=served.result,",
    base + "test_a_listed_scalar_event_is_stored_without_any_label_and_unlisted_it_raises",
)
case(
    "pagination",
    parser,
    "if pending:",
    "if False and pending:",
    base + "test_an_unfinished_pagination_chain_never_becomes_successful_coverage",
)
case(
    "coverage-gap",
    parser,
    "if previous is not None and lower > previous:",
    "if False and previous is not None and lower > previous:",
    base + "test_a_gap_between_successful_fetch_windows_raises_with_its_locator",
)
case(
    "repeat-observations",
    store,
    "record = model.model_validate(record).model_dump()",
    "record = model.model_validate(record).model_dump()\n                    if name == "
    '"settlements" and self.get_settlements(record["series_ticker"], '
    'record["available_at"]):\n                        continue',
    base + "test_repeat_fetches_remain_distinct_and_changed_settlements_are_reported_as_revisions",
)
case(
    "revision-report",
    parser,
    "if changes:",
    "if False and changes:",
    base + "test_repeat_fetches_remain_distinct_and_changed_settlements_are_reported_as_revisions",
)
case(
    "closed-market",
    store,
    "WHERE close_time > ?",
    "WHERE ? IS NOT NULL",
    base + "test_market_snapshots_exclude_close_time_at_or_before_as_of_without_resurrection",
)
case(
    "required-field",
    parser,
    "served = SettlementInput.model_validate(item)",
    'item.setdefault("settlement_ts", raw.envelope.received_at.isoformat())\n'
    "                served = SettlementInput.model_validate(item)",
    base + "test_invalid_settlement_fields_raise_with_the_envelope_locator[missing]",
)
case(
    "unknown-field",
    "src/kwb/schemas.py",
    'extra="forbid"',
    'extra="ignore"',
    base + "test_invalid_settlement_fields_raise_with_the_envelope_locator[unknown]",
)
case(
    "non-200",
    parser,
    "if envelope.envelope.http_status != 200:",
    "if False and envelope.envelope.http_status != 200:",
    base + "test_non_200_settlement_envelopes_are_counted_without_parsing_their_bodies",
)
case(
    "pacing",
    collector,
    "REQUEST_INTERVAL = 10.0",
    "REQUEST_INTERVAL = 0.1",
    base + "test_collection_spaces_every_request_start_by_ten_seconds_and_never_retries_429",
)
results = []
for name, file, old, new, test, section in cases:
    path = root / file
    original = path.read_text()
    mutant = original
    if section:
        start = original.index("    def " + section + "(")
        end = original.find("\n    def ", start + 1)
        end = len(original) if end < 0 else end
        piece = original[start:end]
        assert old in piece, (name, old)
        mutant = original[:start] + piece.replace(old, new) + original[end:]
    else:
        assert original.count(old) == 1, (name, original.count(old))
        mutant = original.replace(old, new)
    try:
        path.write_text(mutant)
        for cache in (root / "src/kwb").rglob("*.pyc"):
            cache.unlink()
        result = subprocess.run(
            ["uv", "run", "--locked", "pytest", "-q", test], capture_output=True, text=True
        )
        log = result.stdout + result.stderr
        (out / (name + ".txt")).write_text(log)
        assert result.returncode == 1 and "FAILED" in log and "ERROR collecting" not in log, (
            name,
            result.returncode,
            log[-1500:],
        )
        summary = next((line for line in reversed(log.splitlines()) if "failed" in line), "")
        results.append(
            {
                "mutation": name,
                "file": file,
                "test": test,
                "change": {"before": old, "after": new},
                "exit_code": result.returncode,
                "summary": summary,
            }
        )
        print(name, summary, flush=True)
    finally:
        path.write_text(original)
for cache in (root / "src/kwb").rglob("*.pyc"):
    cache.unlink()
(out / "results.json").write_text(json.dumps(results, indent=2) + "\n")
