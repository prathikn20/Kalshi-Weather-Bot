# Kalshi-Weather-Bot

Start the public, read-only Kalshi raw collector from the repository root:

```sh
uv run python -m kwb.collectors.kalshi
```

It discovers daily-high temperature series across cities hourly, then fetches
their open markets (all pages) and full order books every 60 seconds. Discovery
matches daily-frequency series with high/highest/maximum/max temperature titles
followed by "in" a city. Matched tickers are logged at each refresh; changes warn.
Failed discovery retains the previous series and retries on a later tick.
HTTP and network failures have bounded retries with exponential backoff and jitter;
one failed market does not stop collection of other markets. Requests are paced at
at most 10 per second. Cycles run sequentially and skip elapsed ticks on overruns.
Each cycle logs its duration and HTTP attempt count. Stop with Ctrl-C.

Every HTTP response, including discovery pages and HTTP errors, is saved under
`raw/kalshi/YYYY-MM-DD/<received_at>.json.gz`. Both the date directory and filename
use UTC receipt time. Each gzip file contains a JSON envelope with `request_url`
(including its query string), `request_params`, `http_status`, `requested_at`,
`received_at`, and `body`. The body is the original UTF-8 text, including whitespace
and numeric formatting; a later parser can decode it as JSON. `received_at` is the
first time our system had the complete response. Transport errors have no response
to save and are logged. Storage failures raise rather than discard raw history.

No API credentials are needed. API documentation:
[market data](https://docs.kalshi.com/getting_started/quick_start_market_data),
[series discovery](https://docs.kalshi.com/api-reference/market/get-series-list),
[markets](https://docs.kalshi.com/api-reference/market/get-markets),
[order books](https://docs.kalshi.com/api-reference/market/get-market-orderbook), and
[rate limits](https://docs.kalshi.com/getting_started/rate_limits).
