# Kalshi settlements

The VM archives responses; the Mac parses them. `config/nws_stations.csv` selects
parsed event series. A KX-series query can also return legacy HIGH events, so the
parser filters by the actual event ticker before domain validation. Collection
retains those responses unchanged.

## API and acquisition policy

Verified on 2026-11-14:

- [Public market data](https://docs.kalshi.com/getting_started/quick_start_market_data):
  production base `https://external-api.kalshi.com/trade-api/v2`; these reads need no
  credentials. The historical routes have no security requirement in the current
  [OpenAPI specification](https://docs.kalshi.com/openapi.yaml).
- [Series](https://docs.kalshi.com/api-reference/market/get-series-list): unfiltered
  `GET /series`, no pagination. Reuse the live collector's discovery predicate,
  retain previously discovered series, and include configured series.
- [Live markets](https://docs.kalshi.com/api-reference/market/get-markets):
  `GET /markets` with `series_ticker`, `status=settled`, `min_settled_ts`,
  `max_settled_ts`, `limit=1000`, and the exact continuation `cursor`.
- [Historical markets](https://docs.kalshi.com/api-reference/historical/get-historical-markets):
  `GET /historical/markets` with `series_ticker`, `limit=1000`, and `cursor`.
  There is no historical time-window or status filter. Full scans therefore
  continue through an explicit empty cursor, not a previously seen ticker.
- [Cutoff](https://docs.kalshi.com/api-reference/historical/get-historical-cutoff-timestamps):
  archive `GET /historical/cutoff` before discovery and after the listings. Use
  `market_settled_ts`, not a presumed retention duration. A cutoff change fails the
  run without advancing its watermark; rerun the acquisition to reconcile it.
- Supplemental `GET /markets?event_ticker=E` and historical `event_ticker=E`
  requests complete configured events split by a time window or tier boundary.
  They preserve every response too.
- [Pagination](https://docs.kalshi.com/getting_started/pagination): missing pages,
  repeated cursors, and repeated tickers/pages raise. A malformed cursor can
  silently return the first page, which is why a cursor check alone is insufficient.

Daily runs fetch the live settled window for every discovered series. The lower
bound is the earlier of the previous successful upper bound minus one second and
current historical cutoff minus one second. Rereading the live tier catches
revisions whose settlement timestamp did not change. The upper bound is fixed
before listings, sixty seconds behind current time to allow for response caching.
Timestamp-boundary overlap is deliberate: the docs say before/after but do not
promise a precise fractional-second inclusion convention.

One explicit configured backfill fetches both tiers. Later daily runs rescan the
configured historical tier only when seven days have elapsed since its successful
scan. Newly configured series report `historical_backfill_required` until explicitly
backfilled. An outage extending behind the current cutoff is reported in `gaps`,
including whether this run includes historical recovery. Unconfigured old history
is not silently claimed as recovered.

## Raw and state

Responses use the existing standard envelope: request URL and exact parameters,
HTTP status, requested/received UTC timestamps, and exact body text (or lossless
base64 for undecodable bytes). Paths are:

```text
raw/kalshi_settlements/YYYY-MM-DD/YYYY-MM-DDTHHMMSS.ffffffZ.json.gz
state/kalshi-settlements.sqlite
state/kalshi-settlements-report.json
```

The received timestamp determines both filename and UTC directory. Completed
files are fsynced outside raw, then linked into raw without overwriting. This
prevents compaction from observing half-written JSON and requires state and raw
to be on the same filesystem. Bodies are archived before navigation/decoding.
Transport failures have no fabricated response. HTTP failures are archived and
end the run; any 429 is counted and stops immediately, with no retry burst.

The SQLite ledger stores the discovered series registry, previous successful live
window end, historical scan time, and per-run reports. Failed runs retain previous
coverage. A separate atomic request clock preserves ten-second spacing across
rapid process restarts. Restarting a failed acquisition begins its pagination at the first page;
raw observations from the failed attempt remain available for diagnosis.

Compaction and sync already enumerate all raw source directories; no maintenance
code change is needed. Bucket names preserve `raw/kalshi_settlements/`. The Mac
SDK downloader explicitly supports this source while keeping generation, size,
CRC32C and local SHA-256 verification. No raw data is transferred over SSH.

## Storage, labels, and revisions

`SettlementInput` requires finalized status, settlement timestamp and served payout.
`Settlement` retains market metadata/brackets, exact settlement strings, provenance,
and `label_status` / `label`. The table is `settlements`, partitioned by UTC
`available_date`, in immutable hourly generations, sorted by series, ticker,
availability, and source identity. Source identities include `kalshi_settlements`,
so coincident filenames in the two raw sources cannot collide.

Every row's `available_at` is its own envelope's `received_at`. Fetching a 2024
settlement in 2026 does not make it visible to a 2024 backtest. Rows on different
pages retain different receipt times, including across hour boundaries. Event
validation uses complete acquisitions; an as-of accessor can correctly return a
partially observed event before its later pages arrived.

The parser checks six-market integer-Fahrenheit partitions and exactly one YES
with five NO results. Unlisted scalar or otherwise inconsistent events raise with
the event ticker and source. `config/settlement_exceptions.csv` is planning-owned:
listed events retain served results/payouts but all six rows carry
`label_status="exception"` and `label=None`. Never train on `result` without checking
label status. A model validator prevents storing a label on an exception.

The first exception is KXHIGHMIA-26APR11: four NO results plus scalar payouts
0.9900 and 0.0100, no YES. This is distinct from KXHIGHMIA-24NOV27's CLI discrepancy;
a fresh read still serves its T83 market as YES with expiration value 84.00.
No real Kalshi settlement revision pair was found among 2,814 overlapping cached
tickers checked during planning. Revision tests explicitly alter a real fixture;
they are synthetic before/after pairs, not claimed provider revisions.

Repeat fetches retain separate source/ticker observations. Rerunning an identical
source is idempotent. The parser reports changes in all retained served fields with both source
identities and observation times; old rows are retained. `label_revision` identifies
changes to result, payout, expiration value, settlement time, bounds or rules, so a
trading-metadata revision is distinguishable from a label revision. Existing three-table generations remain readable; a new generation
must contain all four shards. Open-market reruns preserve settlement rows and vice
versa. DuckDB and Arrow remain lazy imports, keeping collection/maintenance light.

`get_settlements(series, as_of)` filters availability before latest-per-ticker
selection. `get_market_snapshots` likewise selects the latest visible observation,
then requires its served `close_time > as_of`; filtering close time first could
resurrect an older schedule. Close time is known before settlement: Kalshi's
[creation message](https://docs.kalshi.com/websockets/market-and-event-lifecycle)
includes `close_ts`, and [lifecycle documentation](https://docs.kalshi.com/getting_started/market_lifecycle)
defines it as the trading stop time and describes close-date updates. This query
uses the latest schedule actually observed, not a subsequently revised close time.

## Commands

On the VM, with the new service's resource limits:

```sh
uv run --no-sync python -m kwb.collectors.settlements --backfill --configured-only
```

Normal timer execution omits those flags. It runs daily at 18:23 UTC with
`Persistent=true`, so weekly historical work is selected from the ledger rather
than a second overlapping service. The collector has a nonblocking process lock.

On the Mac:

```sh
uv run --locked python -m kwb.ops.download --source kalshi_settlements
uv run --locked python -m kwb.parse.settlements raw/kalshi_settlements
```

Explicit loose files and tar.zst archives, including late supplements, also work.
The parser validates complete pagination and cutoff bookends, reports fetch-window
gaps, and writes its report on failure. A partial download is not a complete parse
input; download the remaining archive hours before parsing cross-hour acquisitions.
Non-200 envelopes are counted before body decoding and cannot become data.

## Resource and deployment verification

Requests start at least ten seconds apart, including all endpoints and pages.
Each HTTP attempt has a twenty-second whole-request deadline, covering a response
that keeps trickling bytes. No request retries run inside a collection invocation.
There is no scheduling dependency on the live collector's journal.

The new unit uses Nice=19, CPUQuota=5%, CPUWeight=1, IOWeight=1,
MemoryMax=128M and TimeoutStartSec=2h. The unfiltered series response exceeds 20 MB,
so discovery decodes one series at a time and raw JSON serialization is streamed.
Parsing runs only on the Mac.

At ten-second pacing, the measured 55 listing pages for seven configured series
plus discovery and two cutoffs imply about 570 seconds of start-to-start spacing.
Their cached bodies total 111.61 MB; standard-envelope gzip sizing was 3.13 MB.
These are sizing estimates, not deployment measurements. Daily discovered-series
volume is measured and reported each run.

The recorded pre-change VM baseline has 35 cycles: min 15.693 s, median 15.735 s,
max 17.541 s, zero 429s and zero skipped ticks. Public/IP quotas are not numerically
documented; authenticated Basic's 200 tokens/s is not a public quota guarantee.
See [rate limits](https://docs.kalshi.com/getting_started/rate_limits).

Deployment first captures existing service invocation IDs. Pull main, then run
`nice -n 19 uv sync --locked` explicitly under a low-priority CPU-limited scope;
this installs DuckDB, PyArrow and Pydantic into the shared .venv. Check live cycles
before, during and after installation. Verify the new units with
`systemd-analyze verify`, install them, and daemon-reload without restarting any
existing service. Run configured backfill under the same new-unit limits before
enabling the timer. Compare baseline/before/during/after cycle distributions,
429s and skipped ticks in both services. If contention or new throttling appears,
disable only the new units, preserving history and checkpoints.

## Test proofs

`tests/test_settlements.py` exercises the required properties, including real
historical pagination and the scalar exception. `tests/fixtures/kalshi/settlements/provenance.json`
records the fresh API sources, exact body hashes, fixture hashes and projections.
A fresh local preflight acquired all seven configured series on 2026-11-14:
58 requests, 55 listing pages, 47,952 market observations, 131,960,096 response-body
bytes (including discovery/cutoffs), 5,182,065 gzip bytes, 570.363 s, zero 429s.

The full-history parse **failed loudly**, publishing no acquisition, at
`raw/kalshi_settlements/2026-11-14/2026-11-14T202619.896508Z.json.gz`:
`KXHIGHCHI-26MAY13-B60.5` serves `yes_bid_size_fp="-276.00"`. This was the only
configured observation failing strict input validation in an audit of the acquired
history. The negative count has not been coerced, ignored or added to the planning-owned
label exception file. Its real six-market event is committed as `negative-size.json.gz`
and a regression test requires the locator-bearing failure. A policy for this
terminal trading-metadata anomaly needs a separate planning decision.

The scalar-event fixture is independently parsed and stored with all six labels
absent. Thus the approved scalar exception works, while the full acquired history
remains blocked by the separate negative-size boundary failure.

Deliberate failures and restored-code checks follow. Run proofs alone with
`uv run --locked python scripts/prove_settlements.py`; the script temporarily mutates
production files, requires behavioral failures and restores each file in `finally`.
It clears local bytecode caches between mutants to avoid timestamp/size cache reuse.
Full transcripts and structured results are saved under ignored
`state/settlements/proofs/`.

| Deliberate violation | Test | Observed failure (exit 1) |
|---|---|---|
| availability | `test_every_settlement_uses_its_envelopes_received_at_and_is_invisible_before_fetch` | 1 failed in 0.55s |
| leakage-get_market_snapshots | `test_no_accessor_returns_future_rows_at_microsecond_boundaries` | 1 failed, 2 passed in 0.77s |
| equality-get_market_snapshots | `test_no_accessor_returns_future_rows_at_microsecond_boundaries` | 1 failed, 2 passed in 1.00s |
| leakage-get_market_history | `test_no_accessor_returns_future_rows_at_microsecond_boundaries` | 1 failed, 2 passed in 0.76s |
| equality-get_market_history | `test_no_accessor_returns_future_rows_at_microsecond_boundaries` | 1 failed, 2 passed in 0.71s |
| leakage-get_orderbook | `test_no_accessor_returns_future_rows_at_microsecond_boundaries` | 1 failed, 2 passed in 0.74s |
| equality-get_orderbook | `test_no_accessor_returns_future_rows_at_microsecond_boundaries` | 1 failed, 2 passed in 0.73s |
| leakage-get_settlements | `test_no_accessor_returns_future_rows_at_microsecond_boundaries` | 1 failed, 2 passed in 0.73s |
| equality-get_settlements | `test_no_accessor_returns_future_rows_at_microsecond_boundaries` | 1 failed, 2 passed in 0.71s |
| event-consistency | `test_each_unlisted_finalized_event_has_six_markets_and_exactly_one_yes_or_raises` | 1 failed in 0.23s |
| exception-label | `test_a_listed_scalar_event_is_stored_without_any_label_and_unlisted_it_raises` | 1 failed in 0.21s |
| pagination | `test_an_unfinished_pagination_chain_never_becomes_successful_coverage` | 1 failed in 0.24s |
| coverage-gap | `test_a_gap_between_successful_fetch_windows_raises_with_its_locator` | 1 failed in 0.33s |
| repeat-observations | `test_repeat_fetches_remain_distinct_and_changed_settlements_are_reported_as_revisions` | 1 failed in 0.51s |
| revision-report | `test_repeat_fetches_remain_distinct_and_changed_settlements_are_reported_as_revisions` | 1 failed in 0.40s |
| closed-market | `test_market_snapshots_exclude_close_time_at_or_before_as_of_without_resurrection` | 3 failed in 0.37s |
| required-field | `test_invalid_settlement_fields_raise_with_the_envelope_locator[missing]` | 1 failed in 0.23s |
| unknown-field | `test_invalid_settlement_fields_raise_with_the_envelope_locator[unknown]` | 1 failed in 0.28s |
| non-200 | `test_non_200_settlement_envelopes_are_counted_without_parsing_their_bodies` | 1 failed in 0.21s |
| pacing | `test_collection_spaces_every_request_start_by_ten_seconds_and_never_retries_429` | 1 failed in 0.20s |

Representative failure excerpts (each mutation was then reverted):

`availability`:

```text
E       assert 0 == 6
E        +  where 0 = len([])
```

`leakage-get_settlements`:

```text
E        +  where 0 = len([])
E        +  and   6 = len([Settlement(source_id='kalshi_settlements/2026-11-10/2026-11-10T220100.000000Z.json', source_sha256='90332bcb8a3d2dc79...dollars='0.0000', settlement_floor_dollars=None, settlement_bounds_type='default', label_status='labeled', label='no')])
```

`event-consistency`:

```text
E           Failed: DID NOT RAISE ValueError
```

`exception-label`:

```text
E                 Value error, An excepted event must carry no label [type=value_error, input_value={'source_id': 'kalshi_set...ception', 'label': 'no'}, input_type=dict]
E                   For further information visit https://errors.pydantic.dev/2.14/v/value_error
```

`pagination`:

```text
E       Failed: DID NOT RAISE ValueError
```

`coverage-gap`:

```text
E       Failed: DID NOT RAISE ValueError
```

`repeat-observations`:

```text
E        +  where 6 = len([{'source_id': 'kalshi_settlements/2026-11-14/2026-11-14T180002.000000Z.json', 'source_sha256': '59c339b3c095eace1d541...Info(key='UTC')), 'available_at': datetime.datetime(2026, 11, 14, 18, 0, 2, tzinfo=zoneinfo.ZoneInfo(key='UTC')), ...}])
E        +    where [{'source_id': 'kalshi_settlements/2026-11-14/2026-11-14T180002.000000Z.json', 'source_sha256': '59c339b3c095eace1d541...Info(key='UTC')), 'available_at': datetime.datetime(2026, 11, 14, 18, 0, 2, tzinfo=zoneinfo.ZoneInfo(key='UTC')), ...}] = all_rows(<kwb.store.Store object at 0x10d24d3d0>)
```

`revision-report`:

```text
E        +  where 0 = len([])
E        +    where [] = SettlementReport(envelopes=12, duplicate_envelopes=0, non_200={}, raw_only_market_rows=0, settlements=18, exceptions=[], revisions=[], coverage_gaps=[], completed_runs=3, errors=[]).revisions
```

`closed-market`:

```text
E       AssertionError: assert 6 == 0
E        +  where 6 = len([MarketSnapshot(source_id='kalshi/2026-11-14/2026-11-14T170000.000000Z.json', source_sha256='9ebc6302d42ec32bb96797f89...ars=None, price_level_structure='linear_cent', price_ranges=[PriceRange(start='0.0000', end='1.0000', step='0.0100')])])
```

`required-field`:

```text
E       Failed: DID NOT RAISE ValueError
```

`unknown-field`:

```text
E       Failed: DID NOT RAISE ValueError
```

`non-200`:

```text
E                       kwb.store.StoreError: /private/var/folders/_p/p3pxv245413bctjpxywq4d400000gn/T/pytest-of-prathik/pytest-325/test_non_200_settlement_envelo0/raw/2026-11-14T180000.000000Z.json.gz: missing acquisition start cutoff
```

`pacing`:

```text
E       assert False
E        +  where False = all(<generator object test_collection_spaces_every_request_start_by_ten_seconds_and_never_retries_429.<locals>.<genexpr> at 0x10b235fc0>)
```

## Required-property coverage and restored checks

| Property | Test(s), all in `tests/test_settlements.py` | What the test checks |
|---|---|---|
| 1. Point in time | `test_every_settlement_uses_its_envelopes_received_at_and_is_invisible_before_fetch` | Every stored receipt matches its source; years-old results are invisible before their fetch. |
| 2. Leakage | `test_no_accessor_returns_future_rows_at_microsecond_boundaries` | All four accessors, including book headers and levels, obey equality and ±1 microsecond cutoffs. |
| 3. Event consistency | `test_each_unlisted_finalized_event_has_six_markets_and_exactly_one_yes_or_raises`; `test_a_listed_scalar_event_is_stored_without_any_label_and_unlisted_it_raises` | Unlisted six-market/YES violations raise with the event; the listed real scalar event keeps payouts and no label. |
| 4. Completeness | `test_an_unfinished_pagination_chain_never_becomes_successful_coverage`; `test_a_gap_between_successful_fetch_windows_raises_with_its_locator`; `test_outage_gaps_are_reported_and_failed_runs_keep_the_previous_watermark` | A whole event on a nonterminal page is insufficient; missing intervals are visible and failures cannot advance coverage. |
| 5. Repeat fetches | `test_repeat_fetches_remain_distinct_and_changed_settlements_are_reported_as_revisions`; `test_changed_served_metadata_is_reported_without_claiming_a_label_revision` | Repeated observations survive, changed values report both sources, and as-of reads retain the earlier version. |
| 6. Closed markets | `test_market_snapshots_exclude_close_time_at_or_before_as_of_without_resurrection` | Strict close-time boundaries apply after latest selection, including shortened and reopened schedules. |
| 7. Fail loud | `test_invalid_settlement_fields_raise_with_the_envelope_locator`; `test_non_200_settlement_envelopes_are_counted_without_parsing_their_bodies`; `test_a_real_negative_terminal_bid_size_raises_with_the_envelope_locator` | Required/unknown/invalid values retain their locator; HTTP errors are counted without JSON decoding; the real anomaly raises. |

After all 20 mutants were restored: `uv sync --locked` succeeded;
`uv run --locked pytest -q` reported **388 passed in 12.69 s**;
`uv run --locked ruff check --fix .`, `uv run --locked ruff format .`,
`uv run --locked ruff check .`, `uv run --locked ruff format --check .`
and `git diff --check` all passed.
