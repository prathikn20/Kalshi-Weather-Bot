# Decisions

One line per decision: date, decision, reason. Newest at the bottom.

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

## Open (answer during the spike)

- Bracket structure per city:
- Climate day boundary in the Daily Climate Report:
- Kalshi API history available and rate limits:
- GEFS on AWS: coverage, format, publication delay:
- Archive of past NWS point forecasts:
- City:
- Weekly hours:
