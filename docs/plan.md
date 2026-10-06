# Plan

Assumes 8 to 10 hours per week (unconfirmed). Interfaces for all phases are fixed in CLAUDE.md. Implementation is planned one phase ahead.

## Phase 0 and 1: collection and spike (weeks 1 to 2)

Raw-first storage means collection can start before the schema is known. Order books and NWS point forecasts are not archived anywhere we know of, so every day without collection is lost backtest data. Start the dumpers first; run the spike in parallel.

### Days 1 to 3: raw dumpers live

1. `collectors/kalshi.py`: every 60 seconds, fetch open markets and order books for every candidate city's daily-high series. Write raw JSON.
2. `collectors/nws.py`: every 30 minutes, fetch the `api.weather.gov` forecast for each candidate city's settlement station. Write raw JSON.
3. `deploy/`: systemd timers for both on a small VM, plus a nightly sync of `raw/` to object storage. This data cannot be re-downloaded, so the backup is required.

Done when both dumpers have run unattended on the VM for 24 hours and the backup has synced.

### Days 2 to 5: spike (notebooks only)

Each question ends with a one-line answer in `docs/decisions.md`.

1. Bracket structure per city: mutually exclusive brackets plus tails, or thresholds.
2. Climate day boundary in the NWS Daily Climate Report: local standard time year-round, or not.
3. Kalshi API: what history it returns (trades, candles, settled markets, order books) and rate limits.
4. GEFS on AWS Open Data: contents, how far back, and the measured delay from run initialization to files appearing.
5. Whether an archive of past NWS point forecasts exists (check Iowa Environmental Mesonet).
6. City choice: highest weather-market volume, probably NYC (verify).

### Days 6 to 14: parsers and remaining collectors

1. `schemas.py` from spike findings. Tables: `market_snapshots`, `nws_forecasts` (`issued_at`, `available_at`, `target_date`), `observations`, `settlements`.
2. Parsers: raw to Parquet, partitioned by date, rerunnable over all history.
3. `collectors/obs.py` (station observations, every 10 minutes) and `collectors/climate.py` (Daily Climate Report, daily).
4. `store.py` accessors, with a test that no row with `available_at > as_of` is ever returned.
5. Heartbeat per collector and a daily staleness alert.

Done when a DuckDB query returns, for any past moment in the collection window, every forecast and order book snapshot the bot could have seen at that moment, and the leakage test passes.

## Phase 2: baselines and scoring (weeks 3 to 4)

Backfill GEFS and station history. Baseline 1: bias-corrected NWS with a spread. Baseline 2: GEFS member counting. Build `eval/scoring.py` (CRPS, per-bracket Brier, reliability diagrams) before any neural work.

Done when there is a number for whether Baseline 1 beats market-implied probabilities on collected days.

## Phase 3: neural post-processor (weeks 5 to 7)

Implements the same `Forecaster` protocol. Trained with CRPS or NLL on archived seasons (reference: Rasp and Lerch, 2018). Split by date. PyTorch (MPS) vs MLX decided here.

Done when it beats both baselines on CRPS on a held-out season, or the reason it doesn't is understood and logged.

## Phase 4: pricing, decisions, backtest (weeks 8 to 9)

`pricing.py` and `decide.py` as pure functions. EV after fees, minimum edge threshold, capped sizing. Fill simulator fills only at prices present in stored order books.

Done when the backtest runs the exact `decide()` that live trading will.

## Phase 5: paper trading (weeks 10 to 12)

`decide()` runs live on a schedule timed to forecast releases, logs every intended order with its inputs, executes nothing. Monitoring for feed staleness and calibration drift, plus kill switch. Small capped real stake only after several weeks of paper trading that matches backtest behavior.

## Phase 6: market-making mode

Quotes around fair value, widened under uncertainty and ahead of forecast releases, with inventory limits.
