"""Run the full-history audit on the Mac; all artifacts go under ignored state/."""

import argparse
import hashlib
import itertools
import json
import math
import pickle
import random
import sqlite3
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from kwb.store import Store, configured_series
from scripts import full_history_oracle as oracle

DEFAULT_ROOT = Path("/Users/prathik/code/Kalshi-Weather-Bot")


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def capture(root, output):
    """Pin original immutable generations before parsing; never replace a baseline."""
    baseline = output / "settlements-before"
    if baseline.exists():
        raise ValueError("Original store baseline already exists; refusing to replace it")
    baseline.mkdir(parents=True)
    (baseline / "manifest.json").write_bytes((root / "data/kalshi/manifest.json").read_bytes())
    (baseline / "generations").symlink_to(
        root / "data/kalshi/generations", target_is_directory=True
    )
    downloaded = json.loads((root / "state/raw-download.json").read_text())
    inventory = {
        key: value for key, value in downloaded["objects"].items() if key.startswith("raw/kalshi/")
    }
    save(output / "bucket-inventory.json", inventory)


def normalize(value):
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    return value


def market_actual(row):
    value = normalize(row.model_dump() if hasattr(row, "model_dump") else row)
    fields = [*oracle.MARKET_FIELDS, "source_id", "source_sha256", "available_at", "requested_at"]
    return {key: value[key] for key in fields}


def book_actual(book):
    def row(value):
        result = normalize(value.model_dump() if hasattr(value, "model_dump") else value)
        result.pop("available_date")
        return result

    return {"header": row(book.header), "levels": [row(level) for level in book.levels]}


def require_equal(actual, expected, context):
    if actual != expected:
        raise AssertionError(f"{context}: actual={actual!r}, expected={expected!r}")


def require_visible(rows, as_of):
    for row in rows:
        if oracle.timestamp(row.available_at) > as_of:
            raise AssertionError(f"Future row {row.source_id} at {row.available_at}, as_of={as_of}")


def reconciliation(store, database, output):
    manifest_bytes = (store.root / "manifest.json").read_bytes()
    (output / "manifest-first.bytes").write_bytes(manifest_bytes)
    save(output / "manifest-first.json", json.loads(manifest_bytes))
    db = oracle.connect(database, readonly=True)
    seen_path = output / "reconciled.sqlite"
    seen_path.unlink(missing_ok=True)
    seen = sqlite3.connect(seen_path)
    seen.execute("CREATE TABLE matched(table_name,id,ticker, PRIMARY KEY(table_name,id,ticker))")
    counts = {
        name: 0
        for name in ("market_snapshots", "orderbook_snapshots", "orderbook_levels", "settlements")
    }
    # Hash trusted typed rows directly; never deserialize pickle. Both passes
    # use the pinned runtime/schema, preserving types and strings without
    # converting 100M+ level timestamps back to text.
    digest = hashlib.sha256()

    def key(item):
        return item[0], item[1], item[2]["source_id"], item[2]["ticker"]

    for (hour, table, identity, ticker), group in itertools.groupby(store._audit_rows(), key):
        records = []
        for _, _, row in group:
            digest.update(pickle.dumps((hour, table, row), protocol=5))
            counts[table] += 1
            records.append(row)
        if table == "settlements":
            continue
        kind = "markets" if table == "market_snapshots" else "books"
        found = db.execute(
            "SELECT body FROM observations WHERE kind=? AND id=? AND ticker=?",
            (kind, identity, ticker),
        ).fetchone()
        if found is None:
            raise AssertionError(f"Unaccounted stored observation: {table}/{identity}/{ticker}")
        expected = oracle.decode_body(found[0])
        if kind == "markets":
            actual = [market_actual(row) for row in records]
            target = [expected]
        else:
            target = [expected["header"]] if table == "orderbook_snapshots" else expected["levels"]
            header = expected["header"]
            times = {
                field: oracle.timestamp(header[field]) for field in ("available_at", "requested_at")
            }
            times["available_date"] = times["available_at"].date()
            target = [{**row, **times} for row in target]
            actual = records
        require_equal(actual, target, f"{table}/{identity}/{ticker}")
        seen.execute("INSERT INTO matched VALUES (?,?,?)", (table, identity, ticker))
    seen.commit()
    expected_counts = {
        "market_snapshots": db.execute(
            "SELECT count(*) FROM observations WHERE kind='markets'"
        ).fetchone()[0],
        "orderbook_snapshots": db.execute(
            "SELECT count(*) FROM observations WHERE kind='books'"
        ).fetchone()[0],
        "orderbook_levels": db.execute(
            "SELECT coalesce(sum(levels),0) FROM observations WHERE kind='books'"
        ).fetchone()[0],
    }
    for table, total in expected_counts.items():
        require_equal(counts[table], total, f"{table} row count")
    require_equal(
        (store.root / "manifest.json").read_bytes(),
        manifest_bytes,
        "Store manifest changed during audit",
    )
    stats = db.execute(
        "SELECT kind,count(*),sum(configured_items),sum(explained_empty) "
        "FROM envelopes WHERE status BETWEEN 200 AND 299 GROUP BY kind"
    ).fetchall()
    result = {
        "rows": counts,
        "expected_rows": expected_counts,
        "successful_envelopes": stats,
        "configured_envelopes": db.execute(
            "SELECT kind,count(*),sum(configured_items),sum(explained_empty) "
            "FROM envelopes WHERE id IN (SELECT id FROM evidence) GROUP BY kind"
        ).fetchall(),
        "http_statuses": db.execute(
            "SELECT status,count(*) FROM envelopes GROUP BY status"
        ).fetchall(),
        "full_row_sha256": digest.hexdigest(),
        "unmatched": 0,
    }
    save(output / "reconciliation.json", result)
    db.close()
    seen.close()
    return result


def fingerprint(store):
    digest = hashlib.sha256()
    counts = {
        name: 0
        for name in ("market_snapshots", "orderbook_snapshots", "orderbook_levels", "settlements")
    }
    for hour, table, row in store._audit_rows():
        digest.update(pickle.dumps((hour, table, row), protocol=5))
        counts[table] = counts.get(table, 0) + 1
    return {"rows": counts, "full_row_sha256": digest.hexdigest()}


def verify_rerun(store, output):
    before = json.loads((output / "reconciliation.json").read_text())
    after = fingerprint(store)
    require_equal(after["rows"], before["rows"], "Rerun row counts")
    require_equal(after["full_row_sha256"], before["full_row_sha256"], "Rerun full row digest")
    require_equal(
        (store.root / "manifest.json").read_bytes(),
        (output / "manifest-first.bytes").read_bytes(),
        "Rerun manifest bytes",
    )
    result = {**after, "new_rows": 0, "manifest_byte_identical": True}
    save(output / "rerun.json", result)
    return result


def audit_moments(coverage, count):
    first, last = (oracle.timestamp(coverage[key]) for key in ("first", "last"))
    span = (last - first) // timedelta(microseconds=1)
    rng = random.Random(oracle.SEED)
    uniform = [first + timedelta(microseconds=rng.randrange(span + 1)) for _ in range(count)]
    endpoints = [oracle.timestamp(gap[key]) for gap in coverage["gaps"] for key in ("start", "end")]
    return sorted(set([*uniform, *endpoints, first, last]))


def timed_call(latencies, name, function, *args):
    start = time.perf_counter()
    result = function(*args)
    latencies.setdefault(name, []).append(time.perf_counter() - start)
    return result


def compare_moment(task):
    root, database, series_list, as_of, baseline = task
    store = Store(Path(root) / "data/kalshi")
    before = Store(Path(baseline))
    db = oracle.connect(database, readonly=True)
    latency = {}
    counts = {
        "market_calls": 0,
        "book_calls": 0,
        "market_rows": 0,
        "book_levels": 0,
        "settlement_calls": 0,
        "history_calls": 0,
    }
    for series in series_list:
        expected = oracle.expected(db, "markets", series, as_of)
        actual = timed_call(
            latency, "get_market_snapshots", store.get_market_snapshots, series, as_of
        )
        require_visible(actual, as_of)
        require_equal(
            {row.ticker: market_actual(row) for row in actual},
            expected,
            f"Markets/{series}/{as_of}",
        )
        counts["market_calls"] += 1
        counts["market_rows"] += len(actual)
        books = oracle.expected(db, "books", series, as_of)
        # Query the union, including book-only, closed, and never-seen-book tickers.
        for ticker in sorted(set(books) | set(expected)):
            book = timed_call(latency, "get_orderbook", store.get_orderbook, ticker, as_of)
            if book is not None:
                require_visible([book.header, *book.levels], as_of)
            require_equal(
                book_actual(book) if book else None, books.get(ticker), f"Book/{ticker}/{as_of}"
            )
            counts["book_calls"] += 1
            counts["book_levels"] += len(book.levels) if book else 0
        # A bounded history call at every moment verifies the fourth public accessor
        # without materializing millions of rows from the beginning of the window.
        history = store.get_market_history(series, as_of - timedelta(minutes=1), as_of)
        require_visible(history, as_of)
        counts["history_calls"] += 1
        settlements = store.get_settlements(series, as_of)
        require_visible(settlements, as_of)
        require_equal(
            settlements, before.get_settlements(series, as_of), f"Settlements/{series}/{as_of}"
        )
        counts["settlement_calls"] += 1
    db.close()
    return {"as_of": as_of.isoformat(), "counts": counts, "latencies": latency}


def latency_summary(values):
    values = sorted(values)
    return {
        "calls": len(values),
        "p50_seconds": values[math.ceil(0.5 * len(values)) - 1],
        "p95_seconds": values[math.ceil(0.95 * len(values)) - 1],
        "max_seconds": max(values),
    }


def run_comparisons(tasks, workers, progress):
    start = time.perf_counter()
    totals = {}
    latencies = {}
    moments = []
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for result in pool.map(compare_moment, tasks, chunksize=1):
            moments.append(result["as_of"])
            for key, value in result["counts"].items():
                totals[key] = totals.get(key, 0) + value
            for key, values in result["latencies"].items():
                latencies.setdefault(key, []).extend(values)
            save(
                progress,
                {
                    "finished": len(moments),
                    "total": len(tasks),
                    "counts": totals,
                    "elapsed_seconds": time.perf_counter() - start,
                },
            )
            print(
                oracle.encode({"finished": len(moments), "total": len(tasks), "counts": totals}),
                flush=True,
            )
    return {
        "counts": totals,
        "moments": moments,
        "wall_seconds": time.perf_counter() - start,
        "latency": {key: latency_summary(values) for key, values in latencies.items()},
    }


def choose_count(seconds_per_moment, mandatory_count, *, budget_seconds=7200):
    # Retain the approved floor even if pilot cost makes the target unattainable.
    affordable = math.floor(budget_seconds / seconds_per_moment * 0.85) - mandatory_count
    return max(100, min(300, affordable))


def settlement_boundaries(root, baseline, configured, output):
    before, after = Store(baseline), Store(root / "data/kalshi")
    times = {
        row["available_at"] for _, table, row in before._audit_rows() if table == "settlements"
    }
    moments = {at + timedelta(microseconds=delta) for at in times for delta in (-1, 0, 1)}
    moments.add(datetime.max.replace(tzinfo=UTC))
    counts = 0
    for moment in sorted(moments):
        for series in configured:
            actual = after.get_settlements(series, moment)
            require_visible(actual, moment)
            require_equal(
                actual,
                before.get_settlements(series, moment),
                f"Settlement boundary/{series}/{moment}",
            )
            counts += 1
    result = {"moments": len(moments), "accessor_comparisons": counts, "unchanged": True}
    save(output / "settlement-boundaries.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=[
            "capture",
            "index",
            "coverage",
            "reconcile",
            "pilot",
            "compare",
            "rerun",
            "settlements",
        ],
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    output = args.root / "state/full-history-parse-2026-11-16"
    database = output / "oracle.sqlite"
    configured = sorted(configured_series())
    baseline = output / "settlements-before"
    coverage_path = output / "coverage.json"
    if args.action == "capture":
        capture(args.root, output)
    elif args.action == "index":
        save(
            output / "index.json",
            oracle.build_index(args.root / "raw/kalshi", database, configured),
        )
        inventory = json.loads((output / "bucket-inventory.json").read_text())
        oracle.coverage(database, configured, args.root / "raw/kalshi", inventory, output)
    elif args.action == "coverage":
        inventory = json.loads((output / "bucket-inventory.json").read_text())
        oracle.coverage(database, configured, args.root / "raw/kalshi", inventory, output)
    elif args.action == "reconcile":
        reconciliation(Store(args.root / "data/kalshi"), database, output)
    elif args.action == "rerun":
        verify_rerun(Store(args.root / "data/kalshi"), output)
    elif args.action == "settlements":
        settlement_boundaries(args.root, baseline, configured, output)
    else:
        coverage = json.loads(coverage_path.read_text())
        if args.action == "pilot":
            first, last = (oracle.timestamp(coverage[key]) for key in ("first", "last"))
            pilot = [first + (last - first) * fraction for fraction in (0.1, 0.5, 0.75, 0.99)]
            tasks = [(str(args.root), str(database), configured, at, str(baseline)) for at in pilot]
            result = run_comparisons(tasks, args.workers, output / "pilot-progress.json")
            mandatory = len(audit_moments(coverage, 0))
            per_moment = result["wall_seconds"] / len(tasks)
            count = choose_count(per_moment, mandatory)
            result.update(
                uniform_moments=count,
                mandatory_moments=mandatory,
                workers=args.workers,
                estimated_seconds=(count + mandatory) * per_moment,
                seed=oracle.SEED,
            )
            save(output / "pilot.json", result)
        else:
            pilot = json.loads((output / "pilot.json").read_text())
            count = pilot["uniform_moments"]
            moments = audit_moments(coverage, count)
            tasks = [
                (str(args.root), str(database), configured, at, str(baseline)) for at in moments
            ]
            result = run_comparisons(tasks, args.workers, output / "oracle-progress.json")
            result.update(
                uniform_moments=count, seed=oracle.SEED, mismatches=0, leakage_violations=0
            )
            save(output / "oracle-results.json", result)


if __name__ == "__main__":
    main()
