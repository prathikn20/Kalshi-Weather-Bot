# Kalshi collector heartbeat

The VM checks archived Kalshi envelopes every five minutes. It sends success to
Healthchecks.io only after finding both kinds of usable evidence with
`received_at` between the UTC check time minus 300 seconds and the check time:

- A 2xx `/markets` response with a non-empty JSON `markets` list of market objects.
- A 2xx `/markets/orderbooks` response with a non-empty `orderbooks` list of
  well-formed books, or a per-market `/markets/<ticker>/orderbook` response with
  a well-formed `orderbook_fp` object. A book with no liquidity remains valid.

This checks data arriving on disk, rather than a process merely being alive.
It does not prove every series was collected, complete pagination, or complete
batch membership; those are parser responsibilities. Non-2xx responses and
irrelevant endpoints cannot provide evidence. The reader stops once both kinds
of evidence have been established; it does not audit older files for corruption.

Three kinds of non-evidence candidates are skipped and counted:

- `skipped_oversized`: an envelope exceeds the unchanged 16 MiB read limit.
- `skipped_invalid_body`: a 2xx market/book body is base64-encoded, cannot be
  parsed as strict JSON, or fails its list/record checks. Empty lists fail the
  heartbeat's non-empty-list requirement and are counted here too.
- `skipped_unreadable`: a gzip stream cannot be opened or read completely,
  including a missing file, permission error, invalid header, truncated stream,
  decompression error or CRC failure. The collector writes final filenames
  directly, so a read can race a write.

A skipped candidate supplies no evidence. A window containing only unusable
responses still sends `/fail` with `missing_fresh_market_or_orderbook_evidence`.
Complete but malformed envelope JSON/keys/types, invalid timestamps or filename
agreement, unexpected origin, non-file entries and exhausted scan budgets remain
fatal structural/operational errors even when valid older responses exist.
Only gzip stream I/O and relevant response-body validation use skippable error
types; the check does not catch all archive errors and continue.

All three counts appear in success and failure logs, including configuration
and ping failures. Only fixed diagnostics and counts are logged; no ping secrets,
raw response text or arbitrary exception details are forwarded. A structural
fault, exhausted budget, or unexpected check exception sends a sanitized `/fail`
reason or leaves the external check to detect silence.
Failure to send a ping is logged and returns nonzero. There are no retries or
redirects and no success fallback after an error. The transport requires HTTP 200
and the body `OK`; HTTP 200 containing `not found` or `rate limited` is an error.

## Why the alert is external

Healthchecks.io notices missing pings even if the VM, collector or timer dies.
An alert sent only by the VM cannot report that the VM itself has disappeared.
Email delivery and Healthchecks.io remain dependencies. Anyone with the ping URL
could conceal an outage, so treat it as a secret.

Create a UUID check with a **Simple schedule, period 5 minutes, grace 10 minutes**,
and enable email notifications. Silence triggers an alert roughly 15 minutes
after the last success; a `/fail` signal marks the check down without waiting
for that silence deadline. A later normal success closes the incident. Ensure
request-body keyword filtering is disabled so it cannot reinterpret signals.

Verified against the current provider documentation:

- [Pinging API](https://healthchecks.io/docs/http_api/): POST
  `https://hc-ping.com/<uuid>` for success, append `/fail` for failure. No more than
  five pings per minute per check; this timer sends one every five minutes.
- [Attaching logs](https://healthchecks.io/docs/attaching_logs/): up to 100,000 bytes
  are retained from POST bodies, with a possible `Ping-Body-Limit` response header
  describing the check's limit. This client caps reasons at 512 ASCII bytes.
- [Configuring checks](https://healthchecks.io/docs/configuring_checks/): period
  specifies the expected interval, grace is additional time before a late alert.

## Cost and compaction

`src/kwb/ops/heartbeat.py` imports only the standard library. It reads raw envelopes
directly, like other ops jobs, without importing the analytical store or dataframe
libraries. It lists only the UTC date directories touched by the freshness window
(one normally, two near midnight), filters names before opening files, and reads
newest first until sufficient evidence exists. It never lists historical date
directories, opens compaction archives, or holds compaction's lock.

A flat date directory has no ordered index: the check must enumerate its entries.
It does not decompress or stat old entries. Named limits are 250,000 directory
entries, 1,024 recent candidate envelopes, 16 MiB decompressed per envelope and a
10-second check budget, checked between filesystem operations. Oversized envelopes
are skipped after reading at most the limit plus one byte; the cap is not raised
for discovery. Exceeding any other budget fails conservatively. The unit's 120-second timeout also bounds stalled I/O or
network operations; the HTTP timeout is five seconds.

The 300-second freshness window equals compaction's five-minute grace. Compaction
groups by filename stamp, and the collector stamps each file with its envelope's
`received_at`; the reader verifies that equality. When an hour becomes eligible,
every timestamp in that hour strictly precedes its end, so every envelope in it
is older than 300 seconds. Fresh evidence remains loose. A concurrent file-read failure is skipped;
without other usable evidence the run fails, rather than consulting archives or
retrying.

## Install on the VM

From `/home/prathik/Kalshi-Weather-Bot`, after pulling the merged main branch:

```sh
sudo test -f /etc/kwb/heartbeat.env
sudo cp deploy/kalshi-heartbeat.service deploy/kalshi-heartbeat.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now kalshi-heartbeat.timer
systemctl list-timers kalshi-heartbeat.timer
journalctl -u kalshi-heartbeat.service --no-pager
```

If the presence check fails, stop and have the owner create the file. Do not print
its contents. `/etc/kwb/heartbeat.env` is root:root, mode 0600, and contains exactly
one assignment: `KWB_HEARTBEAT_PING_URL` set to the check's HTTPS UUID URL. Systemd
loads the file before dropping to `User=prathik`. Do not place the URL in a command,
unit, repository file, log or exception message.

The service uses plain `uv run python -m kwb.ops.heartbeat`, matching the existing
units. It has compaction's `CPUQuota=20%`, `CPUWeight=20`, `IOWeight=10`, `Nice=10`.
The timer persists across downtime, runs at five-minute UTC boundaries, and never
starts overlapping instances of its oneshot service. It has no relationship to
the collector service's lifecycle and never restarts collection.

## Test the failure email without touching the collector

1. Stop only `kalshi-heartbeat.timer` and allow any active heartbeat run to finish.
2. Run one manual instance under systemd with the same EnvironmentFile and resource
   limits, adding `--window-seconds 1`. Use a quiet moment between collector cycles
   so the forced window has insufficient evidence. Exit 1 with `Heartbeat reported
   failure` confirms an acknowledged failure ping; `Heartbeat ping failed` does not.
3. **Stop and ask the owner to confirm the failure email arrived.** Leave the
   heartbeat timer stopped and do not send recovery before that confirmation.
4. After confirmation, start `kalshi-heartbeat.service` normally. Confirm
   `Heartbeat reported success`, verify recovery, then resume the timer.

Never stop or restart `kalshi-collector.service` to test this alert. The supported
window override can shorten the check; values above the default are rejected so
an operator cannot accidentally exceed compaction's grace.

Measure wall time, CPU and peak RSS for a normal run under the service limits,
including `uv` startup. `/usr/bin/time -v` works when installed; otherwise a
standard-library wrapper can measure elapsed time with `time.perf_counter()` and
child CPU/RSS with `resource.getrusage(resource.RUSAGE_CHILDREN)` around
`subprocess.run`. Keep the URL in systemd's EnvironmentFile in either case.

Compare the collector journal's cycle durations, skipped ticks and rate-limited
counts with the preceding baseline for at least 30 minutes after enabling.
If collection degrades, disable only the new heartbeat timer and service and
report the evidence. Do not modify collection or compaction to compensate.

## Tests and deliberate failure proofs

See the table below for the behavioral tests and actual mutation results. Each
mutation is reverted before the next proof; the full suite and Ruff checks run
again on restored code. Tests reuse the committed real market, batch-book and
single-book envelopes in `tests/fixtures/kalshi/`.

Every mutation below exited **1** with a behavioral test failure, then was reverted.
These are the historical PR #20 proofs; the gzip failure policy is superseded by
the skip policy and new proofs below. The `test_` names are in
`tests/test_heartbeat.py`.

| Property | Test and what it checks | Broken behavior and observed failure |
| --- | --- | --- |
| 1-status | `test_invalid_status_timestamps_endpoints_and_response_bodies_never_count_as_evidence[market_status]` — Requires successful HTTP status for both evidence types. | Accept HTTP error statuses; **1 failed in 0.39s**, exit 1. |
| 1-time | `test_invalid_status_timestamps_endpoints_and_response_bodies_never_count_as_evidence[timestamp_mismatch]` — Rejects stale received_at even with a recent filename. | Disable filename/received_at agreement and the age check; **1 failed in 0.08s**, exit 1. |
| 1-bodies | `test_invalid_status_timestamps_endpoints_and_response_bodies_never_count_as_evidence` — Requires usable non-empty bodies; malformed or empty bodies cannot qualify. | Accept a matching endpoint before parsing its body; **8 failed, 10 passed in 0.25s**, exit 1. |
| 2-errors | `test_every_unhealthy_or_exceptional_check_sends_failure_or_nothing_and_never_success[gzip]` — Errors produce failure or silence, never success. | Treat an unreadable envelope as success; **1 failed in 0.08s**, exit 1. |
| 3-midnight | `test_fresh_envelopes_from_yesterday_count_after_utc_midnight` — Fresh evidence from yesterday remains usable after midnight. | Scan only today; **1 failed in 0.08s**, exit 1. |
| 4-grace | `test_the_freshness_window_never_exceeds_compaction_grace` — Freshness cannot exceed the actual compaction grace. | Increase the window to 301 s; **1 failed in 0.07s**, exit 1. |
| 4-boundary | `test_compaction_cannot_remove_envelopes_inside_the_freshness_window` — Real filename stamps equal received_at; compaction respects hour eligibility. | Make compaction eligible one second early; **1 failed in 0.09s**, exit 1. |
| 5-traversal | `test_the_check_scans_only_window_dates_and_stops_reading_when_evidence_is_complete` — Only date directories inside the window are listed. | List the history root; **1 failed in 0.09s**, exit 1. |
| 5-reading | `test_the_check_scans_only_window_dates_and_stops_reading_when_evidence_is_complete` — Reading stops after both required observations are found. | Continue reading after sufficient evidence; **1 failed in 0.08s**, exit 1. |
| 5-imports | `test_importing_the_heartbeat_does_not_load_the_analytical_store_or_dataframe_libraries` — Importing heartbeat leaves store, DuckDB, PyArrow and Polars unloaded. | Import kwb.store; **1 failed in 0.22s**, exit 1. |
| 5-budget | `test_exhausting_any_check_budget_prevents_success[CHECK_BUDGET_SECONDS]` — Exhausting the check budget prevents success. | Remove the deadline check; **1 failed in 0.08s**, exit 1. |
| 6-secret | `test_the_ping_secret_never_appears_in_logs_exception_messages_or_failure_bodies[configuration]` — Secret-bearing errors cannot leak into diagnostics. | Log the configured URL during an error; **1 failed in 0.08s**, exit 1. |
| 7-timeout | `test_ping_failures_exit_nonzero_without_retries[timeout]` — Ping attempts use a short HTTP timeout. | Increase HTTP timeout to 50 s; **1 failed in 0.08s**, exit 1. |
| 7-exit | `test_ping_failures_exit_nonzero_without_retries[http]` — A failed ping returns nonzero without a retry. | Return zero after a ping failure; **1 failed in 0.08s**, exit 1. |
| 7-priority | `test_the_heartbeat_unit_preserves_collection_priority_and_uses_the_external_secret_file` — Resource limits match compaction and preserve collector priority. | Increase heartbeat CPUQuota to 100%; **1 failed in 0.07s**, exit 1. |
| 8-window | `test_the_cli_uses_the_named_default_window_and_returns_the_check_exit_status` — The CLI takes its default from the named freshness constant. | Hard-code the CLI default to 300; **1 failed in 0.07s**, exit 1. |
| 8-timer | `test_freshness_and_timer_behavior_use_the_named_thresholds` — The installed timer checks every five minutes. | Schedule every ten minutes; **1 failed in 0.08s**, exit 1. |

The body mutant caused eight failing cases, including empty market and batch lists,
unparseable market and book bodies, invalid list types, and malformed fallback
objects/levels. A typical failure was `assert 0 == 1`: broken code reported
success where the test required failure. The midnight mutant raised
`missing_fresh_market_or_orderbook_evidence`; the grace mutant failed
`assert timedelta(seconds=hb.WINDOW_SECONDS) <= compact.GRACE`.

Full mutation transcripts are retained locally under the main checkout's ignored
`state/heartbeat/proofs/` directory. No deliberately broken code is committed.


## Oversized discovery incident and regression tests (2026-11-15)

At 17:40:05 UTC, the heartbeat sent `/fail` with `envelope_size_exhausted` while
collection was healthy. Hourly discovery fetches unfiltered `/series`; a real
2026-11-09 envelope decompresses to 21,503,883 bytes, above the 16 MiB cap.
During that discovery cycle, newest-first inspection reached `/series` before
older order-book evidence and aborted instead of continuing.

The fix keeps the 16 MiB cap and skips/counts any oversized envelope. Its endpoint
and contents are unknown because it was not fully read, so it can supply no
market or book evidence. A run succeeds only if other fresh, fully validated
market and book envelopes exist. A window containing only oversized book
candidates still reports `missing_fresh_market_or_orderbook_evidence`. Per-run
counts survive later check or ping failures without retaining payloads, and no
paths or ping secrets are added to logs. The subsequent body/read policy below
extends these skips while retaining structural validation.

The two tests reuse the real trimmed `/series` response at
`tests/fixtures/kalshi/settlements/discovery.json.gz` (its provenance is recorded
beside it). They patch the cap to 1,500 bytes, rebase collection timestamps into
the test window, and place discovery between newer markets and older books.
The market body is projected from a real response onto the required ticker;
the single-book response is retained intact apart from its envelope timestamps.

| Test | What it checks | Deliberate break and observed failure |
| --- | --- | --- |
| `test_an_oversized_real_series_envelope_between_fresh_markets_and_older_books_preserves_success` | Oversized discovery cannot hide a fresh market and an older fresh book; success logs `skipped_oversized=1`. | Restore raising `envelope_size_exhausted`: **1 failed**, `assert 1 == 0`. Remove the success count: **1 failed**, expected count missing from logs. |
| `test_oversized_only_order_book_candidates_still_fail_with_missing_fresh_evidence` | Oversized candidates never count as book evidence; failure logs all three skips and the missing-evidence reason. | Restore raising `envelope_size_exhausted`: **1 failed**, the expected missing-evidence exception did not match. Remove the failure count: **1 failed**, expected count missing from logs. |

All four mutations exited 1 with behavioral failures and were reverted. Full
transcripts are retained locally under the main checkout's ignored
`state/heartbeat-oversized/proofs/` directory. Bytecode caches are invalidated
between mutations so equal-sized edits within one second cannot reuse a prior
mutant's compiled module.

## Discovery and fallback survey

The survey inserted each newer candidate ahead of otherwise valid fresh market
and book evidence. This follows both the collector's discovery path and its
batch-to-single fallback path; no collector code changes are part of this fix.

| Newer candidate or failure path | Behavior and finding |
| --- | --- |
| 301/404/429/500 response from discovery, markets, batch or fallback; invalid JSON body | Already ignored after envelope validation because the status is non-2xx. It does not hide older evidence. |
| Base64 body on non-2xx response, or on 2xx `/series` | Already ignored. Body parsing/decoding is not attempted for non-evidence endpoints/statuses. |
| Non-JSON 2xx `/series` body | Already ignored; discovery is not evidence. |
| Empty `markets`/`orderbooks` listing | Skipped and counted as `skipped_invalid_body` because the non-empty-list check fails. Other fresh non-empty evidence may qualify. |
| Valid per-market fallback, including a book with no liquidity | Qualifies as book evidence. No masking failure. |
| Oversized envelope from any endpoint/status, including the above cases | Now skipped and counted. It cannot provide evidence; only other validated envelopes can make the run succeed. |
| Base64 2xx market/book body; invalid/duplicate/nonfinite or excessively nested JSON; missing or invalid market/book structure, tickers or levels | Skipped and counted as `skipped_invalid_body`; only valid older/fallback responses can qualify. An invalid-only window still fails for missing fresh evidence. |
| File disappears, cannot be read, or has bad/truncated gzip | Skipped and counted as `skipped_unreadable`; an unreadable-only window still fails for missing fresh evidence. |
| Complete gzip containing malformed envelope JSON, duplicate fields, missing/extra/invalid fields or encoding; wrong origin, invalid/non-UTC timestamp, request-after-receipt or filename mismatch | Structural archive faults still fail even when older valid evidence exists. The skippable read handler ends before envelope parsing and validation. |
| Unexpected candidate filename, non-file/symlink, invalid raw directory; directory/candidate-count/deadline budget exhaustion | Existing operational guards remain fatal, including when older evidence exists. Work remains bounded; this follow-up does not relax those guards. |
| Partial non-empty batch membership that triggers fallback | Heartbeat only checks liveness and usable book shapes; complete requested-ticker membership remains the parser's responsibility. This is not a new failure or a completeness guarantee. |

A separate timing finding affects live verification: discovery's next refresh
is scheduled from completion, not a fixed wall-clock minute. The production
journal shows refresh messages at 15:37:38, 16:38:42 and 17:39:48 UTC on Nov 15.
Consequently, a future `:40` heartbeat may run before discovery and correctly
report zero skips. The collector's cadence is not changed to force the check to
encounter discovery.


## Invalid response and incomplete gzip follow-up (2026-11-15)

A malformed batch can be followed by healthy per-market fallback books. Its
invalid body is rejected as evidence without preventing the check from reaching
the valid fallback and market snapshots. Likewise, an incomplete gzip file can
be a live write in progress; it supplies no evidence but need not invalidate
older complete responses inside the existing freshness window. Persistent
unusable input still alerts when no valid pair is available.

The implementation distinguishes `InvalidBody` and `UnreadableEnvelope` from
structural `CheckError` failures. The gzip-only exception boundary wraps opening,
reading and closing the stream; complete envelope parsing and origin/timestamp
validation stay outside it. All skip paths still check the existing deadline.
One per-run stats object supplies all three fields to every outcome log.

The original gzip-only failure test initially failed because it contained valid
older evidence. Its failure assertion is retained for a window containing only
unreadable gzip. New tests cover the complementary healthy-data case across bad
headers, truncation, CRC failure and a permission error containing a fake secret.
The structural error cases and their failure assertions remain in place.

| Test | What it checks | Deliberate break and observed failure |
| --- | --- | --- |
| `test_invalid_batch_bodies_do_not_mask_valid_fallback_books_and_markets` | A bad batch between markets and valid fallback books is skipped and counted across 13 body failure shapes. | Restore fatal body handling: **13 failed in 0.41s**, exit 1; broken code returned 1 where success was required. |
| `test_a_window_of_only_invalid_response_bodies_fails_with_missing_fresh_evidence` | Invalid bodies never become evidence; the check sends the missing-evidence reason and both skips are logged. | Restore fatal handling: **13 failed in 0.69s**, exit 1, wrong failure reason. Accept bodies without validating: **13 failed in 0.22s**, exit 1, false success rejected. |
| `test_unreadable_gzip_does_not_mask_older_valid_evidence` | Bad header, truncation, CRC and permission failures are skipped while good older responses establish health. | Restore fatal gzip handling: **4 failed in 0.75s**, exit 1; broken code returned 1 where success was required. |
| `test_a_window_of_only_unreadable_gzip_fails_with_missing_fresh_evidence` | Incomplete input alone cannot establish health and logs two unreadable skips. | Restore fatal handling: **1 failed in 0.09s**, exit 1, wrong reason. Return success without evidence: **1 failed in 0.10s**, exit 1. |
| `test_structural_archive_faults_still_fail_when_older_valid_evidence_exists` | Own-archive keys, timestamps, origin, entry type, encoding and complete-envelope JSON/type faults remain fatal. | Swallow all read validation errors: **6 failed, 2 passed in 0.21s**. Swallow origin errors: **1 failed in 0.09s**. Ignore non-files: **1 failed in 0.10s**. Each mutant exited 1. |
| `test_all_skip_counts_survive_a_later_ping_failure` | Body and unreadable counters remain visible when the later HTTP ping fails, without leaking a secret-bearing exception. | Remove the invalid-body increment: **1 failed in 0.11s**. Remove the unreadable increment: **1 failed in 0.11s**. Each mutant exited 1. |

All 11 mutations were reverted, invalidating bytecode caches between edits.
Full transcripts are retained under the main checkout's ignored
`state/heartbeat-invalid/proofs/` directory. The full repository tests and Ruff
checks run on restored code before delivery. The existing import-isolation,
freshness, compaction, cost-budget and secret tests remain active.

The owner confirmed the Healthchecks.io dashboard was Up at 18:55 UTC after the
real 18:40:56 success with `skipped_oversized=1`. No additional status credential
or badge is used. The next deployment is verified from the natural `:40` timer
journal line, including all three skip counts; the collector is never restarted.
