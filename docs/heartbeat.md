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
batch membership; those are parser responsibilities. Empty listing responses and
non-2xx responses do not count. Unreadable or malformed inspected evidence fails
the run. The reader stops once both kinds of evidence have been established;
it does not audit older files for corruption.

On stale, missing, malformed or unreadable evidence, exhausted budgets, or an
unexpected check exception, it sends `/fail` with a fixed, sanitized reason.
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
10-second check budget, checked between filesystem operations. Exceeding a limit
fails conservatively. The unit's 120-second timeout also bounds stalled I/O or
network operations; the HTTP timeout is five seconds.

The 300-second freshness window equals compaction's five-minute grace. Compaction
groups by filename stamp, and the collector stamps each file with its envelope's
`received_at`; the reader verifies that equality. When an hour becomes eligible,
every timestamp in that hour strictly precedes its end, so every envelope in it
is older than 300 seconds. Fresh evidence remains loose. A concurrent filesystem
error fails conservatively rather than consulting archives or retrying.

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
The `test_` names are in `tests/test_heartbeat.py`.

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
