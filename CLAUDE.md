# Kalshi Weather Bot

Calibrated probabilistic forecaster for Kalshi daily-high temperature markets, plus a decision layer that trades when the model's fair value beats the market price after fees. Primary metric is calibration versus market-implied probabilities (CRPS, Brier, reliability). P&L is the last test.

Before starting any task, read `docs/plan.md` for phase and scope, and `docs/decisions.md` for what has been settled. Do not re-open a logged decision without being asked.

## Commands

- Install: `uv sync`
- Tests: `uv run pytest`
- Lint and format: `uv run ruff check --fix . && uv run ruff format .`
- Run tests and lint before declaring any task done.

## Layout

```
src/kwb/
  schemas.py      Pydantic models for every boundary
  store.py        Parquet writes, DuckDB reads, get_*(as_of) accessors
  collectors/     raw API dumpers, one per source
  parse/          raw JSON -> Parquet, one per source
  forecast/       base.py (protocols), baselines.py, neural/
  pricing.py      Distribution + brackets -> fair values
  decide.py       pure decision function
  eval/           scoring.py, backtest.py
  ops/            heartbeat, staleness alerts, kill switch
notebooks/        exploration only, never imported by src
deploy/           systemd units, VM setup
```

## Contracts (do not change without explicit approval)

Changes to `src/kwb/schemas.py`, `src/kwb/forecast/base.py`, or any signature below must be proposed in plan mode with the reason, and wait for approval.

1. **Point-in-time reads.** Every stored record has `available_at`: the time our system first had it (fetch time), not the time the source says it was issued. All data reads outside `store.py` go through accessors shaped like `get_forecasts(station, as_of)` that return only rows with `available_at <= as_of`. No other module reads Parquet or raw files directly.
2. **Forecaster.** Every model, including baselines, implements `predict(station: str, target_date: date, as_of: datetime) -> Distribution`. `Distribution` exposes `cdf(x: float) -> float`. Downstream code sees only `Distribution`.
3. **Pricing.** `fair_values(dist, brackets) -> dict[str, float]` is pure. Fair values across a market's brackets sum to 1 within tolerance; a test enforces this.
4. **Decisions.** `decide(fair_values, order_book, position, config) -> list[IntendedOrder]` is pure. Backtest and live trading call this same function. Only the executor differs (fill simulator vs Kalshi API).

## Rules

- **Raw first.** Collectors save the unmodified API response as gzipped JSON under `raw/<source>/YYYY-MM-DD/<fetched_at>.json.gz` before any parsing. Parsers are rerunnable over all raw history.
- **Fail loud at boundaries.** Unexpected values raise. Never silently drop, coerce, or default a field to keep a pipeline running.
- **Time.** Store all timestamps as timezone-aware UTC. Convert to local time only for display or for settlement-day logic, and document which.
- **Splits.** Train/validation/test splits are by date, never random.
- **Secrets.** API keys and private keys live in `.env` or a gitignored `secrets/` directory. Never print, log, or commit them. The repo is public.
- **No real orders.** Nothing places a live Kalshi order unless the environment variable `KWB_LIVE=1` is set and the kill switch file is absent. Until Phase 5, the Kalshi client is read-only.
- **External APIs.** Verify endpoints, auth, and rate limits against the provider's current docs before writing client code; do not rely on memory. NWS (`api.weather.gov`) requires a descriptive `User-Agent` header.
- **Scope.** Do the task asked. If you notice something out of scope, note it at the end instead of fixing it.
- **Decisions.** When a task settles a design question, add one line to `docs/decisions.md` with the date and the reason.
