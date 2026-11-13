# NDFD MaxT arrival collector

Run one poll with `uv run python -m kwb.collectors.ndfd`. The default output root is
`raw/ndfd/`; `--raw-root` overrides it. The command exits 0 only when both daily listings
complete, and 1 when a request or listing-validation failure prevents either completion.
It downloads listings only, never GRIB objects. The existing NWS collector keeps running.

## Provider verification (2026-11-13)

- The [NOAA AWS registry](https://registry.opendata.aws/noaa-ndfd/) documents public,
  unsigned access to `noaa-ndfd-pds` in `us-east-1`, with no AWS account required. It gives
  the layout `wmo/<parameter>/<year>/<month>/<day>/<WMO-filename>`. The MaxT parameter is
  `maxt`, also verified in the spike and through unsigned listing requests. The endpoint is
  `https://noaa-ndfd-pds.s3.amazonaws.com/`.
- [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
  uses `list-type=2`, `prefix`, and `max-keys` (at most 1,000 results). While `IsTruncated`
  is true, send the opaque `NextContinuationToken` as `continuation-token`, URL-encoded as
  one query value. No delimiter or StartAfter is used: every poll scans the full prefixes.
  A 200 response can contain invalid XML, so successful HTTP status does not bypass validation.
- No bucket-specific numeric LIST quota was found in the reviewed registry/API documentation.
  [AWS performance guidance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)
  describes `503 Slow Down` during scaling; its GET/HEAD throughput figures are not a LIST
  quota. One request start per second is our conservative policy. Requests time out after
  ten seconds and have at most three attempts, with exponential backoff and jitter for
  transport failures, 408, 429, and 5xx; Retry-After is respected when supplied. The systemd
  service's four-minute timeout also bounds slow reads, pagination, and long server delays.
- Two unsigned pages were fetched with the returned continuation token during plan review.
  Two further real responses are saved under `tests/fixtures/ndfd/`, with URLs and SHA-256s
  in `provenance.json`. They are small because the requests specify `max-keys=2`; the XML
  bodies are unchanged. Both pages are truncated; tests separately exercise final-page
  termination, token encoding, and missing/repeated tokens.

## Preservation and availability

Each complete HTTP response, including redirects, errors, and retry attempts, is saved
before XML parsing or error handling. The envelope matches Kalshi/NWS: `request_url`,
`request_params`, `http_status`, `requested_at`, `received_at`, and `body`. Invalid UTF-8
uses the shared `body_encoding="base64"` representation. Redirects are not followed.
Transport failures and incomplete body reads are logged without synthetic envelopes.
Archive-write failures escape as fatal errors rather than being retried as network failures.

Filenames are `raw/ndfd/YYYY-MM-DD/<received_at-without-colons>.json.gz`. The directory date
is the UTC receipt date, which may differ from the listed prefix or request date. Exclusive
creation refuses timestamp collisions. Existing compaction and sync discover source
directories generically, so neither needs a new source allowlist or other code change.

The parser returns per-page `(key, received_at)` observations plus pagination metadata.
`first_seen_at` is the minimum `received_at` for a key across the archived responses;
repeated sightings never move it forward. S3 LastModified, WMO filename time, and GRIB
reference time do not supply live availability. The fixture test reconstructs these rows
from saved envelopes, without introducing a persistent arrival table or store contract.

Both today's and yesterday's UTC directory prefixes are listed on every poll, across all
MaxT domains and product families. Keeping yesterday all day avoids an arbitrary midnight
grace cutoff. The pair is pinned at poll start; when a poll crosses midnight, the following
poll includes the new date. A failure for either prefix is logged while the other is still
attempted. Each new poll starts at the beginning, including after partial pagination.

The `maxt` prefix contains multiple domains; downstream parsing must select CONUS explicitly.

## Deployment

Install `deploy/ndfd-collector.service` and `deploy/ndfd-collector.timer` alongside the
existing units, reload systemd, and enable the timer:

```sh
sudo cp deploy/ndfd-collector.service deploy/ndfd-collector.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now ndfd-collector.timer
systemctl list-timers ndfd-collector.timer
journalctl -u ndfd-collector.service
```

The timer runs at minutes 00, 05, ..., 55 UTC, with one-second accuracy and persistent
catch-up after downtime. It does not replay every missed poll. A failed oneshot does not
disable the timer; the next tick starts another poll. A four-minute start timeout leaves
time before the next tick, and systemd does not overlap instances of the same service.
User, working directory, environment, and resource defaults match the Kalshi/NWS collector
units. Those units have no explicit CPU/memory/I/O limits; maintenance-job quotas are not
applied to collection. No units were installed or enabled by this repository change.

## Limits

Arrival observations measure when this collector first received a listing containing a
key, with five-minute sampling and possible outages. The startup inventory is left-censored:
an already existing key is first seen at startup, not necessarily when it arrived in S3.
The two-date policy does not claim recovery of unobserved publication times during longer
outages or detection of later changes to older prefixes. Objects are identified by key;
this collector does not detect in-place replacement of an existing key.

Choosing the backfill lag L, excluding censored observations, forecasting, GRIB downloads,
and a historical backfill remain separate work. The source and availability decisions are
recorded in `docs/decisions.md` (2026-11-13).
