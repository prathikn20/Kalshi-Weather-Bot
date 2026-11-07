# Live NWS fixtures

Captured on 2026-11-07 through the collector HTTP/archive path. Bodies and UTC envelopes are unchanged. The ocean-point 404 was intentional and separate from the two-cycle acceptance run.

| File | HTTP | Request URL | Body UTF-8 bytes |
|---|---|---|---|
| [2026-11-07T205204.377126Z.json.gz](2026-11-07/2026-11-07T205204.377126Z.json.gz) | 200 | https://api.weather.gov/stations/KAUS | 2141 |
| [2026-11-07T205205.283201Z.json.gz](2026-11-07/2026-11-07T205205.283201Z.json.gz) | 200 | https://api.weather.gov/stations/KMDW | 2134 |
| [2026-11-07T205206.316188Z.json.gz](2026-11-07/2026-11-07T205206.316188Z.json.gz) | 200 | https://api.weather.gov/stations/KDEN | 2140 |
| [2026-11-07T205207.350053Z.json.gz](2026-11-07/2026-11-07T205207.350053Z.json.gz) | 200 | https://api.weather.gov/stations/KLAX | 2150 |
| [2026-11-07T205208.302717Z.json.gz](2026-11-07/2026-11-07T205208.302717Z.json.gz) | 200 | https://api.weather.gov/stations/KMIA | 2135 |
| [2026-11-07T205209.308555Z.json.gz](2026-11-07/2026-11-07T205209.308555Z.json.gz) | 200 | https://api.weather.gov/stations/KNYC | 2126 |
| [2026-11-07T205210.562177Z.json.gz](2026-11-07/2026-11-07T205210.562177Z.json.gz) | 200 | https://api.weather.gov/stations/KPHL | 2150 |
| [2026-11-07T205244.804396Z.json.gz](2026-11-07/2026-11-07T205244.804396Z.json.gz) | 200 | https://api.weather.gov/points/40.7833,-73.9667 | 3917 |
| [2026-11-07T205245.806926Z.json.gz](2026-11-07/2026-11-07T205245.806926Z.json.gz) | 200 | https://api.weather.gov/gridpoints/OKX/34,45/forecast | 13220 |
| [2026-11-07T205246.875196Z.json.gz](2026-11-07/2026-11-07T205246.875196Z.json.gz) | 200 | https://api.weather.gov/gridpoints/OKX/34,45/forecast/hourly | 162419 |
| [2026-11-07T205247.865086Z.json.gz](2026-11-07/2026-11-07T205247.865086Z.json.gz) | 200 | https://api.weather.gov/gridpoints/OKX/34,45 | 192506 |
| [2026-11-07T205248.881495Z.json.gz](2026-11-07/2026-11-07T205248.881495Z.json.gz) | 404 | https://api.weather.gov/points/0.0000,0.0000 | 291 |

Recapture: `uv run python scripts/capture_nws_fixtures.py`.
