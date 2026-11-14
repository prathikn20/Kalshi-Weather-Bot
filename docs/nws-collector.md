# NWS forecast collector

Run continuously from the repository root with `uv run python -m kwb.collectors.nws`.
Use `--cycles 1` for one scheduled cycle, `--config` to override
`config/nws_stations.csv`, and `--raw-root` to override `raw/nws/`.
The first cycle starts immediately; subsequent cycles start every 30 minutes.
Cycles are sequential and skip elapsed ticks after an overrun. Stop with Ctrl-C.

## Provider verification (2026-11-14)

- The [NWS API documentation](https://www.weather.gov/documentation/services-web-api)
  identifies `https://api.weather.gov` as the API base and requires an identifying
  `User-Agent`. The collector sends
  `Kalshi-Weather-Bot/0.1 (https://github.com/prathikn20/Kalshi-Weather-Bot)`
  and `Accept: application/geo+json`.
- The same documentation describes `/points/{latitude},{longitude}` and its
  `forecast`, `forecastHourly`, and `forecastGridData` links. They lead to
  `/gridpoints/{office}/{x},{y}/forecast`, the `/forecast/hourly` variant, and
  `/gridpoints/{office}/{x},{y}`. The collector follows those returned links after
  validating HTTPS, the exact API host, and the expected paths without queries or
  fragments. It formats configured coordinates to four decimal places for `/points`.
- NWS permits caching the office/grid mapping but says to refresh it periodically
  because it can change. The collector caches all three URLs in memory per station
  and refreshes `/points` 24 hours after a successful lookup. A failed refresh retains
  the last valid mapping and remains due for retry on the next cycle. Without a
  cached mapping, that station is skipped for the cycle. Restarting clears the cache.
- NWS does not publish its numeric rate limit. One request start per second and
  30-minute forecast polling are our policies, not provider quotas. Requests have
  a ten-second timeout and at most three attempts. Transport failures, HTTP 408,
  429, and 5xx retry with exponential backoff and jitter; 429 waits at least five
  seconds, and valid `Retry-After` values can lengthen the delay.

## Preservation and availability

Every complete HTTP response is archived before `/points` JSON is decoded or HTTP
status is handled, including errors and retry attempts. Redirect responses are
archived and not followed. Forecast bodies remain opaque; only `/points` is
decoded to discover URLs. Transport failures and incomplete body reads are logged
without synthetic envelopes. Archive-write failures escape rather than being
treated as network failures. Station or forecast request failures are logged while
other stations and endpoints continue.

Files are `raw/nws/YYYY-MM-DD/<received_at-without-colons>.json.gz`. The directory
date and timestamp use UTC receipt time. Each gzip contains a JSON envelope with
`request_url`, empty `request_params`, `http_status`, `requested_at`, `received_at`,
and `body`. UTF-8 bodies preserve the original text; undecodable bytes use base64
with `body_encoding="base64"`. Exclusive creation refuses filename collisions.
`received_at` records when the complete response arrived, not the source issue time.

Station provenance lives in `config/nws_stations.csv`: each row records the Kalshi
series, CLI code, settlement source, archived primary-rules path and market ticker,
station identity and coordinates, verification URLs, method, and date. The mappings
were checked against NWS CLI products and `/stations/{stationId}` records, rather
than inferred from CLI-code letters. The collector validates required fields,
unique four-character station IDs, coordinate ranges, and a nonempty config at
startup; it does not reverify the provenance URLs at runtime.

## Deployment

Install the continuous service and enable it:

```sh
sudo cp deploy/nws-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now nws-collector.service
journalctl -u nws-collector.service
```

The unit runs as `prathik` in `/home/prathik/Kalshi-Weather-Bot` using
`uv run python -m kwb.collectors.nws`. It restarts after exit with a ten-second
delay. The loop supplies the 30-minute cadence; there is no NWS timer unit.
The unit has no explicit CPU, memory, or I/O limits. No service restart is needed
for this documentation change.

## Limits

This collector preserves point forecasts, not station observations or CLI reports.
Failures can leave gaps until a later cycle, and its URL cache is not persistent.
It does not parse forecast values or provide a historical backfill. Per the
2026-11-13 source decision in [decisions.md](decisions.md), AWS NDFD supplies live
and historical model inputs; this NWS collector continues as a cross-check.
