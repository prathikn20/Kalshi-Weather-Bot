# Kalshi Weather Bot

Calibrated probabilistic forecaster for Kalshi daily-high temperature markets, plus a decision layer that trades when the model's fair value beats the market price after fees. Primary metric is calibration versus market-implied probabilities (CRPS, Brier, reliability). P&L is the last test.

## Running on the VM

- `kwb.collectors.kalshi`: `deploy/kalshi-collector.service`; open markets and order books every 60 seconds, with hourly series discovery.
- `kwb.collectors.nws`: `deploy/nws-collector.service`; point forecasts for configured settlement sites every 30 minutes.
- `kwb.collectors.ndfd`: `deploy/ndfd-collector.service` and `deploy/ndfd-collector.timer`; MaxT arrival listings every five minutes UTC, without GRIB downloads.
- `kwb.ops.compact`: `deploy/raw-compaction.service` and `deploy/raw-compaction.timer`; compact completed raw hours at minute :10 each hour, with a 120-second budget.
- `kwb.ops.sync`: `deploy/raw-sync.service` and `deploy/raw-sync.timer`; upload published archives to GCS with write-once creation at minute :40 each hour, with a 300-second budget.

Collectors preserve HTTP responses under `raw/<source>/` before parsing. Kalshi access is read-only.

## Install and check

Use Python 3.12 and uv from the repository root:

```sh
uv sync
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
```

## Project documentation

- [Plan and phase scope](docs/plan.md)
- [Settled decisions and open questions](docs/decisions.md)
- [NDFD arrival collector](docs/ndfd-collector.md)
- [NWS forecast collector](docs/nws-collector.md)
