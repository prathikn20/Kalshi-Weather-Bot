# NDFD point extraction and cell check

This is a Mac extraction prerequisite and a one-off diagnostic, not a forecast
store, historical backfill, or model. Nothing imports a notebook. The arrival
collector and VM remain unchanged.

## Setup and exact commands

From any checkout, with uv, git and a C compiler installed:

```sh
bash scripts/setup_ndfd.sh
uv run --locked --group ndfd pytest
uv run --locked --group ndfd ruff check .
uv run --locked --group ndfd ruff format --check .
uv run --locked --group ndfd pytest tests/test_ndfd_points.py::test_the_real_grib_fixture_decodes_to_the_recorded_station_value_sha256
```

The setup command builds g2c once at `~/.local/share/kwb/g2c-2.3.0`, outside
all checkouts. It verifies the installed-file hashes, pinned commit and ability
to load the library, reusing a valid install and rebuilding only an invalid one.
The install is intentionally retained when a worktree is removed.

Setup preserves other settings in `${XDG_CONFIG_HOME:-$HOME/.config}/uv/uv.toml`
and sets only `extra-build-variables.grib2io.G2C_DIR` to the absolute stable
prefix. [uv explicitly documents that extra build variables are tracked in its
cache](https://docs.astral.sh/uv/concepts/projects/config/#build-isolation):
changing this location triggers a reinstall and rebuild, rather than reusing a
wheel linked to the old location. This machine-specific setting also applies
to the main checkout and other worktrees. The checked-in package config disables
interpolation. No global library search-path environment variables are needed.

`ndfd` is an optional group containing grib2io and geometry dependencies.
`notebooks` includes `ndfd`; it has no separate grib2io dependency. Neither is a
default group. These commands and their test enforce that plain VM installs do
not include grib2io:

```sh
uv export --locked --no-default-groups --no-hashes
uv export --locked --no-hashes
```

CI runs the same setup command under its home directory, followed by the exact
group-qualified pytest command above. There are no optional decoder skips.
Setup prints elapsed seconds so native/Python installation overhead can be
measured independently of the full CI job. It does not require a native cache.
After working in the main checkout, restore notebooks with:

```sh
uv sync --locked --group notebooks
```

## Verified toolchain and source pin

Provider and tooling documentation were checked on 2026-11-15:

- [grib2io 2.8.2 release files](https://pypi.org/project/grib2io/2.8.2/#files)
  contain only an sdist, with no PyPI wheels for macOS arm64 or Linux x86_64.
  [Installation instructions](https://noaa-mdl.github.io/grib2io/) require an
  external g2c. grib2io is pinned at 2.8.2 and build requirements are constrained
  in pyproject.toml; uv.lock pins Python runtime packages.
- [g2c v2.3.0 release](https://github.com/NOAA-EMC/NCEPLIBS-g2c/releases/tag/v2.3.0)
  has no uploaded assets or published asset checksum. Setup therefore fetches the
  git tag and requires exact commit
  `acbccb8a894255cd30056b722781a7c9f7b1fe3c`; it does not trust the bytes of a
  generated GitHub archive. [Build documentation](https://github.com/NOAA-EMC/NCEPLIBS-g2c/blob/v2.3.0/README.md)
  and the tagged CMake options define the build.
- CMake 3.31.6 builds shared g2c with PNG, Jasper, OpenJPEG, AEC, upstream tests
  and utility programs disabled. The decoder explicitly supports only complex
  packing templates 5.2/5.3. Unsupported packing fails rather than silently
  producing values. Interpolation is disabled because extraction selects a cell.
- [NOAA's AWS registry](https://registry.opendata.aws/noaa-ndfd/) recommends
  grib2io to handle NDFD scanning correctly and documents unsigned access.

The committed real fixture is one complete, unchanged GRIB message from
`wmo/maxt/2026/11/06/YGUZ98_KWBN_202611060016`, copied at byte offset 80,
length **1,043,517 bytes**. Removing the other two complete messages keeps it
small without repacking, cropping or synthesizing fields. The provenance JSON
records source, source hash, subset hash, and expected station-value hash.
The digest encoding is compact ASCII JSON in station order, with
`[station, row, column, float(kelvin).hex(), native_fahrenheit]`. It avoids
platform byte order and float display-rounding ambiguities.

## Modules and selection rules

- `parse/ndfd.py`: local immutable extraction records and pure key, projection,
  climate-day, native-value and point-in-time selection functions.
- `parse/ndfd_decode.py`: grib2io file boundary, metadata inspection and
  field-at-a-time extraction. A decoded field serves all seven stations.
- `collectors/ndfd_cache.py`: bounded HTTP client, listings and integrity cache.
- `ops/ndfd_point_check.py`: one-off diagnostic orchestration and accounting.
  `scripts/check_ndfd_points.py` is its entry point.

Only `YGUZ98_KWBN_<filename-time>` (days 1–3) and
`YGUZ97_KWBN_<filename-time>` (days 4–7) are selected. Legacy YGUZ87/88 and
known other domains are separately counted. Unknown products, including
unrecognized CONUS-looking names, raise. A prefix date is never an issue time.
The [NOAA product mapping](https://noaa-ndfd-pds.s3.amazonaws.com/NDFDelem_fullres_202206.xls)
identifies the operational products.

A station's cell comes from its coordinates in `config/nws_stations.csv`,
projected using the GRIB's own earth model and Lambert parameters. Half-cell
boundaries determine containment, with deterministic eastern/northern assignment
on an exact shared boundary. Template 3.30 and all eight scanning flags are
validated; grib2io normalizes the alternating-row scan. API gridX/gridY values
are never CONUS indices. The API polygon centroid is projected separately and
its cell compared with the station cell for every station.
Only the API lookup coordinates are formatted to four decimals, following the
[NWS API FAQ](https://github.com/weather-gov/api/blob/master/general-faqs.md).
The extraction continues to use the full configured station coordinates.

`config/station_timezones.csv` is the sole station-to-IANA-zone mapping.
Standard offsets are derived with zoneinfo as `utcoffset - dst` on January 15;
tests assert all seven offsets and fixed UTC day boundaries during both 2026
DST transitions. A climate day runs midnight to midnight standard time.

PDT 4.8's explicit end fields and encoded duration define each period, including
already-started periods. Never substitute `reference + leadTime`. Exactly one
12-hour period must fit inside the selected climate day; zero or several raise.

Raw Kelvin, continuous Fahrenheit, packing quantum and native integer Fahrenheit
are retained. Reconstruct native Fahrenheit only if exactly one integer lies
inside the packing-uncertainty interval, including float32 decoder error.
Bitmap masks, nonfinite values, and missing-value sentinels fail at extraction.

## Cache and availability

`data/ndfd/objects/<S3-key>` holds unchanged NOAA objects.
`data/ndfd/manifest.sqlite` records key, size, ETag, SHA-256 and UTC received_at.
Each reuse verifies size and SHA-256, and reconciles listing size/ETag.
Corrupt, missing or unmanifested files raise without automatic replacement.
Temporary downloads are published before manifest insertion: an interrupted
publication may leave an orphan, which is deliberately an error requiring
inspection. ETag is audit metadata, not an assumed MD5 digest.

Frozen paginated listing responses live under `data/ndfd/listings/<snapshot>/`.
Other public responses live under `data/ndfd-point-check/`, with exact bodies,
URLs, headers, hashes and UTC receipt times. Changing a check window or obtaining
new final CLI reports requires a new snapshot directory, not silent mutation
of the old cached evidence. The NOAA bucket is the raw GRIB archive; no GRIB
copies go to the VM or backup bucket.

All HTTP starts, including retries and different endpoints, share a serial
one-second gate. Unsigned NOAA object requests use only
`https://noaa-ndfd-pds.s3.amazonaws.com/`. At most three attempts retry transport
errors, 408, 429 and 5xx with backoff/jitter; Retry-After seconds and dates are
respected. Redirects and other statuses fail. NWS requests have a descriptive
User-Agent. [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
rules follow `docs/ndfd-collector.md`: opaque encoded tokens, complete pagination,
no delimiter/StartAfter, XML validation even on HTTP 200, and repeated-token
rejection. Existing real NOAA listing fixtures exercise this reader.

Every forecast-bearing API requires an explicit lag, with no default.
`available_at = filename time + lag`; reference time, LastModified and cache
receipt time never replace this reconstruction. Pure selection chooses the
latest qualifying issuance whose period covers D. Newer issuances without that
period do not hide older qualifying issuances. No qualifying issuance raises.
Missing values and malformed periods are errors, not availability fallbacks.

## One-off diagnostic method

Only after tests pass:

```sh
uv run --locked --group ndfd python scripts/check_ndfd_points.py --lag-hours 2
```

The output is `state/ndfd-point-check-2026-11-15/report.md`, with a full ledger
and selected-issuance age distributions in `results.json`. Every cached forecast
field extracts all seven stations, while only KLAX and KNYC are scored.

The window starts on 2026-10-08 and ends on the latest common completed CLI day.
[IEM's documented CLI endpoint](https://mesonet.agron.iastate.edu/json/cli.py?help)
is queried by station/year, as in notebook 01. Linked original CLI text verifies
completed-day status and climate date; same-day partial reports are not final
observations. Integer highs remain IEM's stored values, including revisions;
no replacement from a different source is made.

Primary as_of is D−1 18:00 UTC; secondary is D 12:00 UTC. Inventory includes the
seven-day forecast horizon plus an adjacent-prefix margin. Candidate objects
are inspected newest first; once a qualifying target period is found, older
objects are unnecessary. Download bytes are shared across stations. This is
why actual payload downloads can be below the conservative two-bundle estimate.

For every station/day/as_of, accounting assigns exactly one category:
missing final observed high first, otherwise no qualifying issuance, otherwise
scored. A separate ledger retains forecast presence and fallback details even
when an observation is missing. Counts, n, mean forecast-minus-CLI error, MAE
and sample error SD are reported. Every selected issuance contributes its age
(as_of minus filename time), and older-issuance fallbacks are counted.

### Shoreline and land

The verified [NDFD access documentation](https://vlab.noaa.gov/web/mdl/ndfd-grid-data),
[operational CONUS directory](https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.conus/VP.001-003/),
product mapping and [spatial-reference documentation](https://vlab.noaa.gov/web/mdl/ndfd-spatial-reference-system)
do not expose a downloadable operational 2.5 km NDFD land-sea mask. The separate
high-resolution-window model's mask is not established as NDFD's mask and is
not substituted. This is a bounded search finding, not proof no internal mask exists.

Use [NOAA CUSP](https://shoreline.noaa.gov/cusp.html), whose
[official metadata](https://www.fisheries.noaa.gov/inport/item/60812) describes
contemporary, high-resolution NAD83 shoreline. The current NOAA explorer links
its N30W120 tile at `https://nsde.ngs.noaa.gov/downloads/N30W120.zip`.
Preserve that exact archive and its readme/CRS; its per-segment HOR_ACC attribute
supplies horizontal accuracy in metres. Reject unknown or ≥100 m accuracy.
Natural Earth is not used to determine land.

Within a bounding box around KLAX, node the CUSP linework and polygonize it with
the box boundary. The land component must contain the airport station and
exclude an offshore control. This gives a shoreline-based classification of
the cell centre, not a claim about an unpublished NDFD mask. Measure distance
to the nearest CUSP segment in a local azimuthal-equidistant metric projection,
and station distance geodesically. Signed distance is positive on airport land,
negative over water. A margin within the nearest segment's stated accuracy is
**indeterminate**. Record the exact source/accuracy attributes and dataset date.

Apply the pre-registered primary criteria literally: cell centre not on land;
absolute KLAX bias greater than absolute KNYC bias plus 2°F; KLAX MAE greater
than 1.5 times KNYC MAE. Indeterminate land or unavailable metrics stay
unevaluated. The report does not decide which city to use.

## Failure proofs and limits

```sh
uv run --locked --group ndfd python scripts/prove_ndfd_points.py
```

This temporarily mutates source code for properties 1–9 and the shoreline/age
review additions, requires the selected test to fail, restores original bytes,
then requires the test to pass. Exact pytest failures and restored passes are
saved to the ignored check directory's `mutation-evidence.json`.

Limits: L=2 hours is provisional, not measured from arrival logs. TWC's own
climate-day window remains unestablished. Notebook 02's strict API overlap
control failed; these forecasts are not validated reproductions of API
forecastGridData. IEM stores latest CLI revisions, not immutable settlement-time
truth. CUSP accuracy varies by source and its shoreline may predate this check.
No forecast table, store accessor, calibrated model, historical backfill or
live-order capability is introduced.
