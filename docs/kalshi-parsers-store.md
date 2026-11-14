# Kalshi parsing and point-in-time storage

Run these commands from the repository root on the Mac. Parsing is not a VM job.
Only series named in `config/nws_stations.csv` are parsed. Other series remain
raw-only and are counted in the report; the complete batch ticker set is checked
before that filter. Settlements and the spike cache are outside this parser.

## Google sign-in and downloads

Verified against Google's [macOS installation instructions](https://cloud.google.com/sdk/docs/install-sdk)
and [local ADC instructions](https://cloud.google.com/docs/authentication/set-up-adc-local-dev-environment)
on 2026-11-14. On this Apple Silicon Mac, the installed SDK is 593.0.0.
Its downloaded archive matched Google's published SHA-256:
`563b2102362a54506836b73ef3485b6abfe112c1e48559022bbb5dbf45f55e4e`.

```sh
cd "$HOME"
curl -fLO https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-darwin-arm.tar.gz
tar -xf google-cloud-cli-darwin-arm.tar.gz
./google-cloud-sdk/install.sh --quiet --usage-reporting=false --install-python=false --path-update=false --command-completion=false
source "$HOME/google-cloud-sdk/path.zsh.inc"
gcloud auth application-default login
```

The installer flags were checked using `install.sh --help`. This uses the existing
supported Python and enables `gcloud` in the current Terminal without editing shell
profiles. Do not extract over an existing SDK installation; these are first-install
commands. The sign-in completed during implementation, and a Python SDK download
and repeat verification both succeeded. ADC currently has no quota project; those
bucket reads succeeded without one.

```sh
uv sync --locked
uv run --locked python -m kwb.ops.download
```

The SDK lists `raw/kalshi/` in `kalshi-weather-bot-raw-prathikn20`, including late
supplements. It only reads the bucket, pins each download to the listed generation,
and independently checks size and CRC32C. `state/raw-download.json` records object
name, generation, size, CRC32C and locally computed SHA-256. Reuse checks local
bytes again; changed immutable metadata, missing recorded files and corrupt files
raise. A crash after file publication but before manifest publication is reconciled
only when the file matches the object's verification metadata.

`--root` selects the destination above `raw/`; `--manifest` must be outside raw
history. `--max-objects` bounds the number of objects processed, including reuses,
for a small smoke check. Partial downloads have hidden temporary names, and only
verified files receive final names. There is no SSH transfer path.

See the [SDK download reference](https://docs.cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob)
for generation preconditions, raw downloads and checksum behavior. Standard GCS
operation and egress rates are in [Google's pricing table](https://cloud.google.com/storage/pricing).

## Parse and query

After the compaction backlog drains, the future full-history workflow is:

```sh
uv run --locked python -m kwb.parse.kalshi raw/kalshi
```

The full-history parse was deliberately not run in this task. The parser also
accepts explicit loose `.json.gz` or `.tar.zst` paths, in any input order, and reads
all numbered late supplements. Defaults are `data/kalshi/` for the store and
`state/kalshi-parse-report.json` for the report. The report is written on failure
too. Counts describe the supplied envelopes and parsed rows, including reruns;
they are not counts of newly appended observations.

```python
from datetime import UTC, datetime
from kwb.store import Store

store = Store()
as_of = datetime(2026, 11, 10, 22, 30, tzinfo=UTC)
latest = store.get_market_snapshots("KXHIGHLAX", as_of)
history = store.get_market_history("KXHIGHLAX", datetime(2026, 11, 10, 22, tzinfo=UTC), as_of)
book = store.get_orderbook(latest[0].ticker, as_of)
```

`get_market_snapshots` returns the latest available record per ticker, including
older tickers that no longer appear in open-market collection. It does not infer
closures from absence. `get_market_history` includes both boundaries:
`start <= available_at <= as_of`. `get_orderbook` returns the latest complete
observation, an empty snapshot for an observed empty book, or `None` if never seen.
Query timestamps must be aware and are normalized to UTC; reversed windows raise.

## Schema and design choices

The exact strict models are in `src/kwb/schemas.py`; the explicit Arrow schemas are
in `src/kwb/store.py`. The three tables are:

| Table | Main columns |
| --- | --- |
| `market_snapshots` | ticker, event, series, target date, binary market type, served status, structured and normalized bracket bounds/inclusivity, bounds source, rules, source lifecycle times, served prices/counts, price grid |
| `orderbook_snapshots` | ticker, YES level count, NO level count |
| `orderbook_levels` | ticker, YES/NO side, original level index, served dollar price and fixed-point quantity |

Every row also has `source_id`, `source_sha256`, `requested_at`, `available_at`, and
`available_date`. Timestamps use Arrow microseconds with UTC timezone and DuckDB
`TIMESTAMPTZ`; dates use `date32`; bounds, counts and indices use int32; inclusivity
uses booleans; prices/counts and identities use strings. Price grids are lists of
start/end/step string structs. Model nullability is reflected in Arrow fields.

- **Availability:** Every row gets its own envelope's `received_at`, never a body
  timestamp. Target date comes from the daily event identifier; it is a date label,
  not a new assumption about TWC's intraday aggregation window.
- **Exactness:** Prices, quantities, volume and open interest retain their served
  strings, validated with `Decimal`. No fixed storage scale can round later values.
  Current fields are checked against [market documentation](https://docs.kalshi.com/api-reference/market/get-markets),
  [batch books](https://docs.kalshi.com/api-reference/market/get-multiple-market-orderbooks),
  [single books](https://docs.kalshi.com/api-reference/market/get-market-orderbook),
  and the [fixed-point specification](https://docs.kalshi.com/getting_started/fixed_point_migration).
- **Brackets:** Each configured event must have four inclusive two-integer brackets
  and two strict tails, covering all integers without gaps or overlaps. Explicit
  literal rules can recover absent structured bounds; disagreements raise.
- **Empty books:** Headers preserve an observation even when both level arrays are
  empty. Without a header, a latest-book query could accidentally reuse old liquidity.
- **Deduplication:** Identity is `kalshi/date/decompressed-member-name`, not the
  gzip wrapper, archive path or response body. An unchanged market seen again later
  remains a distinct observation; conflicting bytes for an existing identity raise.
- **Raw integrity:** Files and archive members are streamed without path extraction.
  Gzip, tar and zstd errors include the locator; an explicit zstd EOF check detects
  even a missing final checksum. Duplicate JSON keys and nonfinite JSON numbers raise.
- **Pagination:** Complete listing chains are validated together, buffering across
  an hour boundary when necessary. Rows keep their original individual fetch times.
- **Storage:** Immutable hourly shards sit under UTC `available_date` partitions in
  generation directories. Markets sort by series/ticker/availability/source; books
  by ticker/availability/source, with levels then ordered by side/index.
- **Import cost:** DuckDB and PyArrow load only when a query or publication needs
  them. Eager imports increased the Linux compactor test subprocess memory baseline;
  deferring them preserves the existing memory test and its 128 MiB limit unchanged.
- **Publication:** A writer lock serializes writers. All three shards are written,
  reopened, verified and fsynced before atomic manifest replacement. Per-hour source
  inventories keep the top-level manifest small. A failed hour leaves its previous
  generation visible; earlier successfully committed hours in the run remain visible.
- **Reads:** Each accessor pins one manifest and applies the availability cutoff
  before latest selection or a book join. Only `store.py` opens raw or Parquet data
  in production; parsers consume its validated envelope iterator.
- **Reruns:** Supplied sources are recomputed while previously stored supplements
  remain. Equivalence means identical schema and sorted typed row multiset, including
  exact strings and provenance. Identical reruns also retain the existing manifest;
  byte identity across different Parquet library versions is not required.

Unreferenced generations are retained so readers can finish against a pinned
manifest. Automatic garbage collection is outside this task. The implementation
pins DuckDB 2.0.0, PyArrow 26.0.1, Pydantic 2.14.3 and development-only Hypothesis
6.170.15; each version was verified against its PyPI release before installation.

## Real-data verification

Fixtures under `tests/fixtures/kalshi/` retain a complete six-market LAX event, an
intact mixed-series 50-ticker chunk, and an existing real single-book response.
`provenance.json` records source/member paths, envelope/container hashes, fixture
hashes and projections. No spike-envelope adapter is present.

The production implementation parsed the two previously copied hours on the M4:

| Input hour | Market rows | Book headers | Book levels | Parquet bytes |
| --- | ---: | ---: | ---: | ---: |
| 2026-11-10T22, compacted | 5,040 | 5,040 | 224,543 | 323,632 |
| 2026-11-13T15, loose | 5,040 | 5,040 | 250,787 | 443,971 |

The run processed 7,802 envelopes, counted 24,480 raw-only market rows and 24,480
raw-only books, and reported zero errors. It took 9.76 seconds wall time, including
startup; macOS `time -l` reported about 1.37 GiB maximum RSS. The top-level manifest
was 1,168 bytes. A LAX query at 22:30 returned 12 latest tickers; its first-hour
history returned 720 records. Only these samples were parsed, not full history.

## Property tests and deliberate failures

Each mutation below was applied to production code, run against the named test,
and reverted in a `finally` block. Every mutant exited 1 with a behavioral test
failure. The full unmodified suite is run after restoration. Full local transcripts
are retained under ignored `state/parsers-store-2026-11-14/proofs/` in the main checkout.

### 1-availability

`tests/test_kalshi_parse_store.py::test_every_parsed_row_uses_its_envelopes_received_at_as_utc_available_at`

Checks every table against its envelope received_at, UTC timezone and availability date.

Mutation: Use request start instead of received_at. Result: **1 failed in 0.24s**, exit code 1.

```text
E               AssertionError: assert datetime.datetime(2026, 11, 10, 22, 0, 44, 563692, tzinfo=zoneinfo.ZoneInfo(key='UTC')) == datetime.datetime(2026, 11, 10, 22, 0, 44, 749404, tzinfo=datetime.timezone.utc)
```

### 2-leakage

`tests/test_kalshi_parse_store.py::test_no_store_accessor_returns_rows_available_after_as_of`

Checks latest snapshots, inclusive history and book headers/levels at generated cutoffs, including microseconds.

Mutation: Remove every availability cutoff. Result: **1 failed in 0.74s**, exit code 1.

```text
AssertionError: future book available at 22:01:01.000001 was returned for as_of 22:01:01; generated market/history cutoff also failed.
```

### 2-equality

`tests/test_kalshi_parse_store.py::test_rows_are_visible_exactly_at_their_availability_time`

Checks inclusion exactly at received_at and exclusion one microsecond earlier.

Mutation: Make equality invisible. Result: **1 failed in 0.38s**, exit code 1.

```text
E       AssertionError: assert 0 == 6
```

### 3-packaging

`tests/test_kalshi_parse_store.py::test_all_raw_packagings_produce_identical_typed_rows`

Compares schema and exact typed rows across loose, base, supplemented and overlapping inputs.

Mutation: Ignore late supplements. Result: **1 failed in 0.35s**, exit code 1.

```text
E           AssertionError: assert False
```

### 3-reruns

`tests/test_kalshi_parse_store.py::test_rerunning_the_same_inputs_preserves_the_schema_and_exact_row_multiset`

Checks source-subset reruns retain older sources without duplicating rows or changing schema.

Mutation: Append reruns without source/row deduplication. Result: **1 failed in 0.31s**, exit code 1.

```text
E       AssertionError: assert {'market_snap...), ...}, ...]} == {'market_snap...), ...}, ...]}
```

### 4-required

`tests/test_kalshi_parse_store.py::test_invalid_required_fields_raise_with_the_envelope_locator[missing]`

Checks missing required values raise with the envelope locator rather than receiving defaults.

Mutation: Default a missing required volume to zero. Result: **1 failed in 0.13s**, exit code 1.

```text
E       Failed: DID NOT RAISE ValueError
```

### 4-status

`tests/test_kalshi_parse_store.py::test_non_200_envelopes_are_counted_without_becoming_data`

Checks a non-200 response is counted without attempting to decode its invalid data body.

Mutation: Attempt to parse an HTTP error body. Result: **1 failed in 0.11s**, exit code 1.

```text
E           json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
```

### 5-brackets

`tests/test_kalshi_parse_store.py::test_every_event_forms_the_required_six_market_integer_fahrenheit_partition[missing_market]`

Checks real six-market events and deliberately broken event partitions.

Mutation: Disable the six-market partition check. Result: **1 failed in 0.14s**, exit code 1.

```text
E           Failed: DID NOT RAISE ValueError
```

### 6-exactness

`tests/test_kalshi_parse_store.py::test_fixed_point_fields_round_trip_exactly_through_storage_and_accessors`

Checks every served numeric string through Parquet and accessors, including sub-cent prices, fractional sizes and values beyond float precision.

Mutation: Round fixed-point values through float. Result: **1 failed in 0.20s**, exit code 1.

```text
E       AssertionError: assert {'90071992547409.94'} == {'90071992547409.93'}
```

### 7-completeness

`tests/test_kalshi_parse_store.py::test_each_orderbook_chunk_returns_every_requested_ticker_exactly_once[missing]`

Checks all requested tickers appear exactly once, before the configured-series filter.

Mutation: Disable chunk ticker equality. Result: **1 failed in 0.16s**, exit code 1.

```text
E           Failed: DID NOT RAISE ValueError
```

### configured-only

`tests/test_kalshi_parse_store.py::test_only_configured_series_are_parsed_and_other_series_remain_raw_only`

Checks unverified series stay raw-only and do not enter domain validation/storage.

Mutation: Parse an unconfigured series. Result: **1 failed in 0.11s**, exit code 1.

```text
E                       pydantic_core._pydantic_core.ValidationError: 33 validation errors for MarketInput
```

### latest-per-ticker

`tests/test_kalshi_parse_store.py::test_latest_snapshots_select_one_row_per_ticker_after_applying_the_cutoff`

Checks latest selection yields exactly one eligible row per ticker.

Mutation: Return multiple historical instants per ticker. Result: **1 failed in 0.24s**, exit code 1.

```text
E       assert {datetime.dat...timezone.utc)} == {datetime.dat...timezone.utc)}
```

### history-start

`tests/test_kalshi_parse_store.py::test_rows_are_visible_exactly_at_their_availability_time`

Checks the inclusive history start filters out earlier observations.

Mutation: Ignore the history start boundary. Result: **1 failed in 0.30s**, exit code 1.

```text
E       AssertionError: assert 12 == 6
```

### empty-book

`tests/test_kalshi_parse_store.py::test_an_empty_observed_book_replaces_an_older_nonempty_book`

Checks a newer empty observation replaces older nonempty liquidity.

Mutation: Select the oldest book header. Result: **1 failed in 0.24s**, exit code 1.

```text
E       AssertionError: assert (OrderBookSnapshot(header=OrderBookHeader(source_id='kalshi/2026-11-10/2026-11-10T220056.444920Z.json', source_sha256='..., 11, 10), ticker='KXHIGHMIA-26NOV11-B87.5', side='yes', level_index=5, price_dollars='0.0600', quantity_fp='121.00')]) is not None and [OrderBookLev...880.00'), ...] == []
```

### download-checksum

`tests/test_download.py::test_failed_downloads_publish_neither_final_archives_nor_manifest_entries[checksum]`

Checks corrupt downloads publish neither an archive nor a manifest entry.

Mutation: Publish a download with a mismatched checksum. Result: **1 failed in 0.20s**, exit code 1.

```text
E       Failed: DID NOT RAISE any of (ValueError, OSError)
```

## Restored-code checks

After reverting all 15 mutants, `uv sync --locked` succeeded, `uv run --locked
pytest -q` reported **354 passed**, and Ruff lint and formatting checks passed.
The supplied plan/decision/ignore edits were applied verbatim. No collector,
settlement parser, spike adapter, live-order code or full-history parse was added.
