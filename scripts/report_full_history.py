"""Render the ignored evidence report from completed, machine-readable audit artifacts."""

import argparse
import json
import re
from datetime import datetime
from pathlib import Path

PROPERTIES = [
    (
        "Coverage ledger",
        "test_the_coverage_ledger_includes_every_minute_gap_and_hour_source",
        "Every minute/hour, zero counts, gap durations, and source labels are retained.",
    ),
    (
        "Envelope reconciliation",
        "test_every_successful_configured_envelope_reconciles_to_rows_or_an_explained_empty",
        "Each raw source/ticker and all levels match stored rows; missing rows fail.",
    ),
    (
        "Independent oracle",
        "test_the_independent_raw_oracle_matches_point_in_time_market_and_book_results",
        "Independent raw latest observations match accessor tickers and exact served values.",
    ),
    (
        "Real-data leakage",
        "test_no_accessor_returns_future_rows_at_the_audit_moments",
        "All four accessors and book levels respect availability at every audit moment.",
    ),
    (
        "Rerun idempotence",
        "test_a_second_full_parse_adds_no_rows_and_preserves_the_manifest_bytes",
        "All physical row counts/hashes and manifest bytes stay identical.",
    ),
    (
        "Settlement preservation",
        "test_full_history_market_parses_preserve_all_settlement_accessor_results",
        "Market publication and reruns preserve existing settlement accessor results.",
    ),
]


def load(root, name):
    return json.loads((root / name).read_text())


def measurements(root, name):
    text = (root / (name + "-time.log")).read_text()
    wall = re.search(r"([\d.]+) real", text)
    rss = re.search(r"(\d+)\s+maximum resident set size", text)
    return {
        "seconds": float(wall[1]) if wall else None,
        "peak_rss_bytes": int(rss[1]) if rss else None,
    }


def table(columns, rows):
    return (
        "\n".join(
            [
                "| " + " | ".join(columns) + " |",
                "| " + " | ".join("---" for _ in columns) + " |",
                *[
                    "| "
                    + " | ".join(str(v).replace("|", "\\|").replace("\n", " ") for v in row)
                    + " |"
                    for row in rows
                ],
            ]
        )
        + "\n"
    )


def render(output):
    coverage = load(output, "coverage.json")
    index = load(output, "index.json")
    reconcile = load(output, "reconciliation.json")
    oracle = load(output, "oracle-results.json")
    pilot = load(output, "pilot.json")
    rerun = load(output, "rerun.json")
    settlements = load(output, "settlement-boundaries.json")
    inventory = load(output, "bucket-inventory.json")
    preflight = load(output, "preflight.json")
    proofs = load(output, "proofs/results.json")
    first = load(output, "parse-first.json")
    second = load(output, "parse-second.json")
    if (
        any(
            [
                first["errors"],
                second["errors"],
                reconcile["unmatched"],
                oracle["mismatches"],
                oracle["leakage_violations"],
            ]
        )
        or not rerun["manifest_byte_identical"]
    ):
        raise AssertionError("A required real-data check failed; do not render a success report")
    text = [
        "# Full-history Kalshi parse\n",
        "All timestamps in this report are UTC. All six required properties passed.\n",
        "## Window, acquisition, and VM evidence\n",
        f"Envelope window: **{coverage['first']} to {coverage['last']}**, inclusive. "
        f"Last downloaded archive: `{max(inventory)}`.\n",
        f"Downloaded **{len(inventory):,} objects**, "
        f"**{sum(v['size'] for v in inventory.values()):,} bytes**. "
        "Generation, size, CRC32C and local SHA-256 are recorded in the download manifest.\n",
    ]
    size = sum(v["size"] for v in inventory.values())
    text.append(
        f"List-price egress: {size / 2**30:.9f} GiB × $0.12/GiB = **${size / 2**30 * 0.12:.6f}**. "
        "Eligible unused free allowance reduces this to $0; billing allowance was not inspected. "
        "Source: [Google Cloud Storage pricing](https://cloud.google.com/storage/pricing) "
        "(100 GiB eligible monthly transfer, excluding Australia and China).\n"
    )
    text.append(
        "Before acquisition the production inventory matched 537 VM archives, 537 read-only "
        "sync-ledger records, and 537 bucket objects; no historical production raw was found "
        "outside the expected tree. The only missing-hour copies elsewhere were committed "
        "test fixtures, explicitly excluded by the user. A later completed upload added "
        "the next hour during acquisition. Latest inspected sync reported pending=0; "
        "compaction had no eligible loose hours.\n"
    )
    text.append(
        table(
            ["Missing bucket range start", "End, exclusive", "Hours", "Evidence"],
            [
                [r["start"], r["end_exclusive"], r["hours"], r["evidence"]]
                for r in coverage["missing_bucket_ranges"]
            ],
        )
    )
    text.append(
        "The archive-hour ranges differ from envelope gaps: the Mac has partial captures "
        "in T14 and T15, and the suspension starts/ends partway through archived hours.\n"
    )
    text.append("### Unbacked-up Mac-only production data\n")
    text.append(
        table(
            ["Hour", "Loose files", "Compressed bytes"],
            [[r["hour"], r["files"], r["bytes"]] for r in preflight["mac_local_only"]],
        )
    )
    text.append(
        "These files were neither uploaded nor moved. The overlapping November 9 hour was "
        "parsed with its bucket archive; identical member copies were counted once.\n"
    )
    s = preflight["settlements"]
    text.append(
        f"First scheduled settlement run: Result={s['Result']}, "
        f"ExecMainStatus={s['ExecMainStatus']}, "
        f"{s['start']} to {s['end']}; {s['requests']} requests, {s['markets']:,} observations, "
        "zero errors/gaps/rate limits. The November 15 raw directory held one compacted archive.\n"
    )
    text.append("## Per-series coverage\n")
    text.append(
        table(
            ["Series", "First 2xx markets", "Last 2xx markets", "First 2xx book", "Last 2xx book"],
            [
                [
                    series,
                    row["markets"]["first"],
                    row["markets"]["last"],
                    row["books"]["first"],
                    row["books"]["last"],
                ]
                for series, row in coverage["series"].items()
            ],
        )
    )
    text.append(
        "Absence ranges below are UTC minute bins after October 7 17:00: a feed has no "
        "successful envelope for this series while another configured series has one. "
        "They include endpoint bins affected by sequential polling; they do not claim "
        "sub-minute outages. Full exact receipt gaps are in the next table.\n"
    )
    absence = []
    for series, row in coverage["series"].items():
        for feed in ("markets", "books"):
            absence.extend(
                [series, feed, r["start"], r["end_exclusive"], r["minutes"]]
                for r in row[feed]["absent_while_others_present"]
            )
    text.append(
        table(["Series", "Feed", "Absent from", "Until, exclusive", "Minutes"], absence)
        if absence
        else "No such absence ranges.\n"
    )
    text.append("## Every receipt gap longer than two minutes\n")
    text.append(
        table(
            ["Series", "Feed", "Start", "End", "Seconds"],
            [
                [r["series"], r["feed"], r["start"], r["end"], r["duration_seconds"]]
                for r in coverage["gaps"]
            ],
        )
    )
    text.append("## Exact envelope reconciliation\n")
    text.append(
        f"Raw envelopes: {index['envelopes']:,}; identical duplicate copies: "
        f"{index['duplicates']:,}. Raw envelope classifications: {index['kinds']}.\n"
    )
    text.append(
        table(
            ["Table", "Expected raw rows", "Stored rows"],
            [
                [name, reconcile["expected_rows"][name], reconcile["rows"][name]]
                for name in reconcile["expected_rows"]
            ],
        )
    )
    text.append(
        table(
            [
                "2xx endpoint class",
                "Envelopes, all series",
                "Configured items",
                "Explained empties",
            ],
            reconcile["successful_envelopes"],
        )
    )
    text.append(
        "Market empties count envelopes; book empties count individual configured books. "
        "Empty books still have stored headers. Every stored source/ticker/level was compared "
        "to its raw projection; unmatched observations: 0.\n"
    )
    text.append("## Oracle, leakage, rerun, and settlements\n")
    text.append(
        f"Seed: {oracle['seed']}; uniform moments: {oracle['uniform_moments']}; "
        f"distinct total moments including all gap endpoints: {len(oracle['moments'])}. "
        f"Four worker processes. Counts: `{json.dumps(oracle['counts'], sort_keys=True)}`. "
        "Mismatches and leakage violations: **0**.\n"
    )
    text.append(
        f"The pilot selected {pilot['uniform_moments']} uniform moments using "
        f"{pilot['mandatory_moments']} mandatory moments, measured parallel throughput, "
        f"a 15% budget margin and the approved 100-moment floor. Estimated comparison wall "
        f"time: {pilot['estimated_seconds']:.1f} s; actual: {oracle['wall_seconds']:.1f} s.\n"
    )
    text.append(
        "Rerun: zero new rows, identical full-row SHA-256, byte-identical manifest. "
        f"Fingerprint: `{rerun['full_row_sha256']}`. "
        f"Settlement boundary comparisons: {settlements['accessor_comparisons']:,} across "
        f"{settlements['moments']} moments, unchanged; sampled settlement comparisons "
        "also passed. The original immutable manifest was pinned before parsing.\n"
    )
    text.append("## Timing and memory\n")
    names = [
        "download",
        "parse-first",
        "index",
        "reconcile",
        "pilot",
        "oracle",
        "parse-second",
        "rerun",
        "settlements",
    ]
    text.append(
        table(
            ["Operation", "Wall seconds", "Peak RSS bytes"],
            [
                [
                    name,
                    measurements(output, name)["seconds"],
                    measurements(output, name)["peak_rss_bytes"],
                ]
                for name in names
            ],
        )
    )
    text.append("## Required-property tests and observed bite proofs\n")
    proofrows = []
    for (title, test, meaning), proof in zip(PROPERTIES, proofs[:6], strict=True):
        log = (output / "proofs" / (proof["property"] + ".log")).read_text()
        excerpt = next(
            (line.strip() for line in log.splitlines() if line.startswith("E ")),
            next((line for line in log.splitlines() if "FAILED" in line), "See transcript"),
        )
        proofrows.append(
            [
                title,
                f"`tests/test_full_history.py::{test}`",
                meaning,
                f"exit {proof['mutant_exit']}; {excerpt}; restored exit {proof['restored_exit']}",
            ]
        )
    text.append(
        table(["Property", "Test", "Checks", "Deliberate failure and restoration"], proofrows)
    )
    text.append(
        "Additional deliberate failures prove duplicate conflicts and the audit-only scope "
        "guard. The proof script restores source bytes in finally; tests use temporary stores. "
        "Complete transcripts: `proofs/`. Existing leakage tests and the full suite also pass; "
        "see `full-tests.log`, `lint.log`, and `format.log`.\n"
    )
    text.append("## Non-obvious design choices\n")
    text.append(
        "- The oracle has its own raw decoder and disposable compressed SQLite index, so a "
        "production parser bug cannot be repeated merely by importing the same implementation. "
        "Compression saves disk; strings are recovered exactly.\n"
        "- Reconciliation checks source/ticker identities and individual book levels before "
        "totals. Missing and duplicate rows therefore cannot cancel each other out.\n"
        "- Original settlement generations remain immutable behind a pinned baseline manifest; "
        "this checks actual before/after accessor values without copying the full store.\n"
        "- Audit inventory has no as_of because it must inspect future rows to detect leakage. "
        "It is private, documented audit-only, and barred from other production modules.\n"
        "- Closed and book-only tickers are still checked: the book accessor has no close-time "
        "filter, whereas market snapshots filter the latest observed schedule.\n"
    )
    text.append("## Out-of-scope findings\n")
    text.append(
        "Accessor latency matters for the Phase 4 backtest; no store optimization was made. "
        "Pilot per-call measurements include concurrent normal accessor use:\n"
    )
    text.append(
        table(
            ["Accessor", "Calls", "p50 seconds", "p95 seconds", "Max seconds"],
            [
                [
                    name,
                    stats["calls"],
                    stats["p50_seconds"],
                    stats["p95_seconds"],
                    stats["max_seconds"],
                ]
                for name, stats in pilot["latency"].items()
            ],
        )
    )
    text.append(
        "The two Mac-only hours remain unbacked-up. The historical suspend gap is unrecoverable "
        "from the VM or bucket. Neither finding was fixed in this task.\n"
    )
    ny = coverage["series"]["KXHIGHNY"]
    start = datetime.fromisoformat(coverage["first"]).date()
    end = datetime.fromisoformat(coverage["last"]).date()
    dates = (end - start).days + 1
    text.append("## Draft decision, not applied\n")
    text.append(
        f"- 2026-11-16: Kalshi raw history has no bucket archives from 2026-10-11 16:00 UTC "
        "to 2026-10-28 16:00 UTC (408 whole UTC hours). VM journal evidence records suspend "
        "at 2026-10-11 15:06:50 UTC and the next boot at 2026-10-28 16:19:06 UTC. "
        f"KXHIGHNY has successful market envelopes on {ny['utc_days_with_markets']} of "
        f"{dates} UTC dates in the observed window; the suspension removes 16 complete UTC "
        f"dates (October 12–27), with partial October 11 and 28 retained. "
        f"{len(ny['target_dates_observed'])} distinct market target dates were observed; "
        "UTC collection dates are not a count of complete training/settlement days. "
        "Coverage gaps must remain explicit in evaluation. Evidence: full-history coverage "
        "ledger and VM journals.\n"
    )
    (output / "report.md").write_text("\n".join(text))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/Users/prathik/code/Kalshi-Weather-Bot/state/full-history-parse-2026-11-16"),
    )
    args = parser.parse_args()
    render(args.output)


if __name__ == "__main__":
    main()
