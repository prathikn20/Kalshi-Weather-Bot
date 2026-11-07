# Kalshi /series fixture

`2026-10-07T142603.591250Z.json.gz` is a byte-for-byte copy of a raw envelope
written by `kwb.collectors.kalshi` on the development Mac.

- Source: `raw/kalshi/2026-10-07/2026-10-07T142603.591250Z.json.gz`
- SHA-256: `dc3571ee84ba1f3ac7738376e485944b93e27d95b6fcf152b6e4eb7234356aa9`
- Request: `GET https://external-api.kalshi.com/trade-api/v2/series`, no params
- HTTP status: 200
- `requested_at`: `2026-10-07T14:25:58.111270Z`
- `received_at`: `2026-10-07T14:26:03.591250Z`
- Contents: 14,714 series, 323 with `frequency == "daily"`

## Why it is not trimmed

The repo rule is to trim parser fixtures small. This one is kept whole on
purpose. Discovery filters on frequency and title across the entire catalogue,
so the test needs every non-matching series (other frequencies, lows, rain,
non-weather products, high-temperature titles at weekly or custom frequency)
to prove nothing outside the expected set slips through. Trimming would choose
the negatives by hand and hide exactly the misses this fixture exists to catch.
