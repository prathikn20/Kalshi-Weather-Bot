# Plan

Assumes 8 to 10 hours per week (unconfirmed). Interfaces for all phases are fixed in CLAUDE.md. Implementation is planned one phase ahead.

## Phase 0 and 1: collection and spike (in progress)

Raw-first storage means collection can start before the schema is known. Order books are not archived anywhere we know of, so every day without collection is lost backtest data. Start the dumpers first; run the spike in parallel.

### Raw dumpers (done)

1. Done. `collectors/kalshi.py`: every 60 seconds, fetch open markets and order books for every city's daily-high series. Write raw JSON. Live on the VM (2026-10-07, 2026-11-07).
2. Done. `collectors/nws.py`: every 30 minutes, fetch the `api.weather.gov` forecast for each Kalshi settlement site. Write raw JSON. Live on the VM (2026-11-07).
3. Done. `deploy/`: systemd services for both on a GCP e2-micro (2026-11-07). Bounded hourly compaction of `raw/` (2026-11-10). Backup is daily persistent-disk snapshots (2026-11-07) plus an hourly write-once upload of compaction archives to a GCS bucket (2026-11-11). This data cannot be re-downloaded, so the backup is required.
4. Done. CI runs uv, ruff and pytest, including property tests, on every push and pull request (2026-10-07), and fails on a stale uv.lock (2026-11-12).
5. Done. NDFD arrivals collector: logs when each AWS NDFD MaxT file first appears, to measure when archived NWS forecasts became available. Live on the VM (2026-11-13).

Done when both dumpers have run unattended on the VM for 24 hours and the backup has synced. Met.

### Spike (notebooks only)

Each question ends with a one-line answer in `docs/decisions.md`.

1. Answered (2026-11-13). Bracket structure: six-market integer-°F partitions (four inclusive two-integer brackets plus two strict tails) in every KX event, both CLI and TWC periods. `notebooks/03_market_structure.ipynb`.
2. Answered (2026-11-13). Climate day: NWS CLI uses local standard time year-round; TWC's window is not established.
3. Kalshi API: what history it returns (trades, candles, settled markets, order books) and rate limits.
4. Deferred to January 2027 with Baseline 2 (2026-11-13). GEFS on AWS Open Data: contents, how far back, and the measured delay from run initialization to files appearing.
5. Answered. Whether an archive of past NWS forecasts exists: AWS NDFD (`noaa-ndfd-pds`), evaluated in `notebooks/02_nws_forecast_archive.ipynb` (2026-11-13, source switch).
6. Answered (2026-11-13). City choice: Los Angeles (KXHIGHLAX), NYC runner-up, by TWC-period volume, conditional on the KLAX NDFD cell check in Phase 2.
7. Answered (2026-11-12, refining 2026-11-07). Settlement source: what KX* series settle on, how The Weather Company's maximum relates to the NWS CLI value, and when the series switched.

### Parsers, store and heartbeat (scope cut 2026-11-14)

Only what the mid-December milestone needs runs before Phase 2.

1. `schemas.py` and Kalshi parsers for the configured series: raw markets and order books to Parquet, partitioned by date, rerunnable over all history, reading loose files and compaction archives alike.
2. `store.py` accessors, with a test that no row with `available_at > as_of` is ever returned.
3. Settlement collector: a daily raw-first fetch of settled markets (`/markets?status=settled`, plus `/historical/markets` past Kalshi's historical cutoff) in the standard envelope, with its parser and `get_settlements`.
4. Heartbeat for the Kalshi collector and a daily staleness alert.
5. After the milestone: `collectors/obs.py` (station observations) and `collectors/climate.py` (Daily Climate Report). Phase 2 uses IEM history for observations and CLI highs; `nws_forecasts` is built with NDFD point extraction in Phase 2.

Done when a DuckDB query returns, for any past moment in the collection window, every Kalshi market snapshot and order book the bot could have seen at that moment, and the leakage test passes.

## Phase 2: baselines and scoring (by mid-December 2026)

Backfill NDFD MaxT and station history. Point extraction selects the CONUS grid and the KLAX cell; confirm the cell's centre is on land and its MaxT bias is not anomalous versus KNYC before committing to LA (2026-11-13). Baseline 1: bias-corrected NWS with a spread, using NWS forecasts from the AWS NDFD archive for both backfill and live. Baseline 2 (GEFS member counting) is deferred to January 2027 with Phase 3 (2026-11-13); the mid-December milestone needs only Baseline 1. Build `eval/scoring.py` (CRPS, per-bracket Brier, reliability diagrams) before any neural work.

Done when there is a number for whether Baseline 1 beats market-implied probabilities on collected days. Milestone: mid-December 2026.

## Phase 3: neural post-processor (January to February 2027)

Implements the same `Forecaster` protocol. Trained with CRPS or NLL on archived seasons (reference: Rasp and Lerch, 2018). Split by date. PyTorch (MPS) vs MLX decided here.

Done when it beats both baselines on CRPS on a held-out season, or the reason it doesn't is understood and logged.

## Phase 4: pricing, decisions, backtest (unscheduled)

`pricing.py` and `decide.py` as pure functions. EV after fees, minimum edge threshold, capped sizing. Fill simulator fills only at prices present in stored order books.

Done when the backtest runs the exact `decide()` that live trading will.

## Phase 5: paper trading (unscheduled)

`decide()` runs live on a schedule timed to forecast releases, logs every intended order with its inputs, executes nothing. Monitoring for feed staleness and calibration drift, plus kill switch. Small capped real stake only after several weeks of paper trading that matches backtest behavior.

## Phase 6: market-making mode (unscheduled)

Quotes around fair value, widened under uncertainty and ahead of forecast releases, with inventory limits.
