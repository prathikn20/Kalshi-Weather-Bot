# Decisions

One line per decision: date, decision, reason. Newest at the bottom. Append-only: to change a decision, add a new line naming what it supersedes and the evidence; never edit an old line.

## Settled

- 2026-09-28: Market: Kalshi daily-high temperature. Fast daily settlement, structurally beatable, runs on an M4 Mac.
- 2026-09-28: Primary metric is calibration vs market-implied probabilities (CRPS, Brier, reliability). P&L is the last test.
- 2026-09-28: One city first; add cities later to grow the sample.
- 2026-09-28: Taker mode first, market making is phase 6.
- 2026-10-02: Point-in-time reads filter on `available_at` (our fetch time), not `issued_at`. Source issue time precedes publication by hours; filtering on it lets backtests use forecasts before anyone could have had them.
- 2026-10-02: Raw-first storage. Collect unparsed JSON immediately; parse later so parser bugs never lose history.
- 2026-10-02: Backtest and live trading share one pure `decide()`.
- 2026-10-02: Python tooling: uv, ruff, pytest. Storage: Parquet queried with DuckDB.
- 2026-10-02: Real money only after backtest plus several weeks of paper trading. Small fixed stake, never topped up after losses.
- 2026-10-05: Python pinned to 3.12 (`>=3.12,<3.13`); ruff line-length 100 with DTZ rules to enforce timezone-aware datetimes.
- 2026-10-07: Kalshi raw collection covers all cities using daily-frequency high-temperature title matching, hourly series discovery, and 60-second market/book ticks that skip overruns; archive every HTTP response in a UTC per-request envelope with the exact body text, preserving discovery, prices, volume, errors, and first availability for later parsing.
- 2026-10-07: Require property tests with demonstrated failures and real raw parser fixtures; run uv, ruff, and pytest on Python 3.12 for every push and pull request to enforce the testing rules automatically.
- 2026-10-28: Hourly raw compaction bundles each completed UTC hour (5-minute grace) into `YYYY-MM-DDTHH.tar.zst` holding decompressed JSON named with `.gz` removed; decompressed content, not the gzip wrapper, is the preserved artifact, and originals are deleted only after the reopened final archive matches every SHA-256. Small files waste 4 KB blocks and gzip hides cross-file redundancy: a real hour shrank 8x versus its gzip bytes and about 50x on disk.
- 2026-11-07 (recorded; adopted earlier): Collectors run as systemd services on a free-tier GCP e2-micro (us-east1, shared vCPU, ~1 GB RAM). Free and always on; the shared CPU means every background job on the VM must be bounded (see compaction, below).
- 2026-11-07 (recorded; adopted earlier): Backup is daily persistent-disk snapshots instead of plan.md's nightly object-storage sync. About $0.10/month and no sync code. Limit: snapshots live in the same project and account, so they do not protect against losing either. Revisit before any real money. Reason for choosing snapshots over object storage: not recorded at the time.
- 2026-11-07: The VM pulls the repo over SSH with a read-only deploy key scoped to this repo. GitHub rejects password auth; a deploy key limits a VM compromise to reading this one repo.
- 2026-11-07: KX* daily-high series settle on The Weather Company's reported maximum for a named NWS climate site (e.g. "New York City (CLINYC) ... according to The Weather Company"); legacy HIGH* series cite the NWS CLI report. NWS CLI is therefore a feature and cross-check, not settlement truth; training labels come from Kalshi's settled results. Evidence: archived rules_primary 2026-10-07; live KXHIGHTATL rules 2026-11-07.
- 2026-11-07: Supersedes the matching rule in 2026-10-07. Discovery matches a daily series when its title has a temperature word, a high signal (high/highest/max/maximum in the title, or HIGH in the ticker), and no low word (low/lowest/min/minimum) or derived word (difference/change/anomaly). The old phrase regex required "<high word> temperature in" and missed 41 of 59 live series ("Atlanta Max Temperature", "Daily high temp Tokyo"), losing their order books since collection began. A test asserts the exact 59-ticker set from a real /series fixture, so any rule change shows up as a diff. Principle: over-collecting is cheap and filterable at parse time; a missed series is unrecoverable.
- 2026-11-07: /series is fetched unfiltered and inactive series stay polled. A category filter would silently drop HIGHNY0/KXHIGHNY0, which Kalshi files under World; inactive series cost ~35 cheap requests per cycle and may reopen.
- 2026-11-07: Order books come from GET /markets/orderbooks in chunks of 50 tickers, sent as repeated tickers= params, each response archived as one envelope with the exact ticker list in request_params. A chunk counts as failed on any error or when the returned ticker set differs from the request (unknown tickers return HTTP 200 with an empty book), and then falls back to per-market requests for that chunk only. Evidence: 59 series with single-book requests took 347 requests and 43-44 s cycles on a 60 s tick, projected ~73 s once international markets list; batching gives ~66 requests and 16-18 s on the VM. 50 not 100: batch items are billed per ticker (10 tokens each), and 100 would exceed the 600-token burst.
- 2026-11-07: /markets requests are spaced 0.25 s apart; all other requests 0.1 s. Back-to-back /markets calls hit an undocumented unauthenticated limit of roughly 4-5 req/s (1-4 429s per cycle); spacing gave 0 429s for about 2 s per cycle. Repeatedly exceeding an undocumented limit risks an IP block.
- 2026-11-07: NWS collector archives /points, forecast, forecastHourly and forecastGridData every 30 min for each Kalshi settlement site, using the same raw envelope as Kalshi. Sites live in a checked-in config/nws_stations.csv derived from archived rules_primary; each CLI-to-station mapping is verified against api.weather.gov/stations and the NWS CLI page, never inferred from the code's letters. /points is cached, refreshed every 24 h, and the last valid mapping is kept when a refresh fails. One request per second; NWS publishes no numeric limit. Redirects are archived and not followed. Started before the spike because past NWS point forecasts are not known to be archived anywhere else.
- 2026-11-07: Transport failures with no HTTP response (timeouts, resets) are logged, not archived as synthetic envelopes, in both collectors. Gaps in raw/ are explained by logs until the planned heartbeat exists.
- 2026-11-07: Collection always has priority over maintenance jobs. Compaction's first production run hit a month of backlog at zstd level 19, used ~100% CPU and 317 MB RSS, and pushed the Kalshi cycle from ~12 s to 95 s, skipping ticks. The timer was disabled until compaction has a per-run time budget, streaming memory, a measured compression level and a hard CPUQuota.

## Open (answer during the spike)

- Settlement value: how TWC's reported maximum relates to the NWS CLI value, and when KX* series switched:
- Bracket structure per city:
- Climate day boundary in the Daily Climate Report:
- Kalshi API history available and rate limits:
- GEFS on AWS: coverage, format, publication delay:
- Archive of past NWS point forecasts:
- City:
- Weekly hours:
