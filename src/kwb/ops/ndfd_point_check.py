"""One-off Mac diagnostic, not a model input table or historical backfill."""

import argparse
import csv
import hashlib
import io
import json
import math
import resource
import statistics
import sys
import zipfile
from collections import Counter
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

from kwb.collectors.ndfd_cache import NDFDCache
from kwb.parse.ndfd import (
    NoQualifyingIssuance,
    Station,
    climate_day,
    containing_cell,
    period_for_day,
    select_forecast,
    select_objects,
)
from kwb.parse.ndfd_decode import decode_grib, grid_spec, inspect_periods


def load_stations() -> tuple[Station, ...]:
    with Path("config/station_timezones.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    zones = {r["station"]: r["timezone"] for r in rows}
    if len(zones) != len(rows):
        raise ValueError("Duplicate station timezone")
    with Path("config/nws_stations.csv").open() as stream:
        sites = list(csv.DictReader(stream))
    if set(zones) != {r["station_id"] for r in sites}:
        raise ValueError("Station timezone table differs from settlement sites")
    return tuple(
        Station(
            r["station_id"], float(r["latitude"]), float(r["longitude"]), zones[r["station_id"]]
        )
        for r in sites
    )


def account_day(station: str, day: date, observed: int | None, forecast) -> dict:
    status = (
        "no observed high"
        if observed is None
        else "no qualifying issuance"
        if forecast is None
        else "scored"
    )
    return {
        "station": station,
        "day": day.isoformat(),
        "status": status,
        "observed_f": observed,
        "forecast_f": forecast.native_fahrenheit if forecast is not None else None,
        "error_f": forecast.native_fahrenheit - observed if status == "scored" else None,
    }


def validate_accounting(rows: list[dict], stations: list[str], days: list[date]) -> None:
    expected = {(s, d.isoformat()) for s in stations for d in days}
    keys = [(r["station"], r["day"]) for r in rows]
    if len(keys) != len(set(keys)) or set(keys) != expected:
        raise ValueError("Missing or duplicate station-day accounting")
    for row in rows:
        if row["status"] not in ("scored", "no qualifying issuance", "no observed high"):
            raise ValueError("Unknown accounting status")
        expected_status = (
            "no observed high"
            if row["observed_f"] is None
            else "no qualifying issuance"
            if row["forecast_f"] is None
            else "scored"
        )
        if row["status"] != expected_status:
            raise ValueError("Inconsistent accounting status")


def summarize(rows: list[dict]) -> dict:
    errors = [r["error_f"] for r in rows if r["status"] == "scored"]
    ages = sorted(r["issuance_age_hours"] for r in rows if r.get("issuance_age_hours") is not None)
    age_distribution = dict(Counter(ages))
    return {
        "n": len(errors),
        "mean_error_f": statistics.mean(errors) if errors else None,
        "mae_f": statistics.mean(map(abs, errors)) if errors else None,
        "error_sd_f": statistics.stdev(errors) if len(errors) > 1 else None,
        "accounting": {
            status: sum(r["status"] == status for r in rows)
            for status in ("scored", "no qualifying issuance", "no observed high")
        },
        "issuance_age_hours": {
            "min": min(ages) if ages else None,
            "median": statistics.median(ages) if ages else None,
            "max": max(ages) if ages else None,
            "distribution": age_distribution,
        },
        "older_issuance_fallbacks": sum(r.get("fallback", False) for r in rows),
    }


def land_result(signed_distance_m: float, accuracy_m: float) -> str:
    if not math.isfinite(accuracy_m) or accuracy_m < 0:
        raise ValueError("Invalid shoreline accuracy")
    if abs(signed_distance_m) <= accuracy_m:
        return "indeterminate"
    return "land" if signed_distance_m > 0 else "water"


def shoreline_check(cache: NDFDCache, station: Station, cell) -> dict:
    import shapefile
    from pyproj import Geod, Transformer
    from shapely.geometry import Point, box, shape
    from shapely.ops import polygonize, transform, unary_union

    url = "https://nsde.ngs.noaa.gov/downloads/N30W120.zip"
    body = cache.cached_response(url, Path("data/ndfd-point-check/shoreline"))
    archive = zipfile.ZipFile(io.BytesIO(body))
    base = "N30W120"
    reader = shapefile.Reader(
        shp=io.BytesIO(archive.read(base + ".shp")),
        shx=io.BytesIO(archive.read(base + ".shx")),
        dbf=io.BytesIO(archive.read(base + ".dbf")),
    )
    bounds = box(-118.6, 33.75, -118.2, 34.1)
    features = [
        (shape(r.shape.__geo_interface__).intersection(bounds), r.record.as_dict())
        for r in reader.iterShapeRecords(bbox=bounds.bounds)
    ]
    to_metric = Transformer.from_crs(
        "EPSG:4269", "+proj=aeqd +lat_0=33.94 +lon_0=-118.39 +datum=WGS84 +units=m", always_xy=True
    )
    metric = [
        (transform(to_metric.transform, geom), attrs)
        for geom, attrs in features
        if not geom.is_empty
    ]
    # Classify the connected shoreline-delimited land component containing the airport.
    polygons = list(polygonize(unary_union([g for g, _ in features] + [bounds.boundary])))
    airport = Point(station.longitude, station.latitude)
    offshore = Point(-118.59, 33.94)
    land = [p for p in polygons if p.covers(airport)]
    if len(land) != 1 or land[0].covers(offshore):
        raise ValueError("CUSP linework does not separate airport land from offshore control")
    centre = Point(cell.centre_longitude, cell.centre_latitude)
    centre_metric = transform(to_metric.transform, centre)
    nearest, attrs = min(metric, key=lambda item: item[0].distance(centre_metric))
    distance = nearest.distance(centre_metric)
    accuracy = float(attrs["HOR_ACC"])
    if not 0 <= accuracy < 100:
        raise ValueError("CUSP nearest segment has insufficient or unknown accuracy")
    signed = distance if land[0].covers(centre) else -distance
    _, _, station_distance = Geod(ellps="WGS84").inv(
        station.longitude, station.latitude, cell.centre_longitude, cell.centre_latitude
    )
    return {
        "result": land_result(signed, accuracy),
        "signed_shoreline_distance_m": signed,
        "shoreline_accuracy_m": accuracy,
        "station_distance_m": station_distance,
        "cell": asdict(cell),
        "nearest_segment_attributes": attrs,
        "url": url,
        "archive_sha256": hashlib.sha256(body).hexdigest(),
        "prj": archive.read(base + ".prj").decode(),
        "readme": archive.read(base + "-readme.txt").decode(errors="strict"),
    }


def cli_highs(cache: NDFDCache, station: str) -> tuple[dict[date, int | None], date]:
    url = "https://mesonet.agron.iastate.edu/json/cli.py?" + urlencode(
        {"station": station, "year": 2026}
    )
    directory = Path("data/ndfd-point-check/iem")
    data = json.loads(cache.cached_response(url, directory))
    highs, final_days = {}, []
    for row in data["results"]:
        day = date.fromisoformat(row["valid"])
        if (
            row["station"] != station
            or day in highs
            or (type(row["high"]) is not int and row["high"] != "M")
        ):
            raise ValueError("Unexpected IEM CLI station/date/high")
        if day < date(2026, 10, 8):
            continue
        text = cache.cached_response(
            "https://mesonet.agron.iastate.edu" + row["link"], directory
        ).decode()
        # Completed CLI reports describe YESTERDAY; same-day partial summaries say TODAY.
        completed = "YESTERDAY" in text and "VALID AS OF" not in text
        headline = f"{day.strftime('%B').upper()} {day.day} {day.year}"
        if headline not in " ".join(text.split()):
            raise ValueError("CLI text climate date differs from IEM row")
        highs[day] = row["high"] if completed and type(row["high"]) is int else None
        if highs[day] is not None:
            final_days.append(day)
    if not final_days:
        raise ValueError("No final CLI high")
    return highs, max(final_days)


def api_cell_crosschecks(cache: NDFDCache, grid, stations) -> list[dict]:
    from shapely.geometry import shape

    rows = []
    for station in stations:
        url = f"https://api.weather.gov/points/{station.latitude:.4f},{station.longitude:.4f}"
        directory = Path("data/ndfd-point-check/nws")
        points = json.loads(cache.cached_response(url, directory))
        data = json.loads(
            cache.cached_response(points["properties"]["forecastGridData"], directory)
        )
        centroid = shape(data["geometry"]).centroid
        own = containing_cell(grid, station.latitude, station.longitude)
        api = containing_cell(grid, centroid.y, centroid.x)
        rows.append(
            {
                "station": station.station,
                "station_cell": asdict(own),
                "api_centroid_cell": asdict(api),
                "agree": (own.row, own.column) == (api.row, api.column),
            }
        )
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lag-hours", type=float, required=True)
    args = parser.parse_args(argv)
    lag = timedelta(hours=args.lag_hours)
    stations = load_stations()
    cache = NDFDCache(Path("data/ndfd"))
    output = Path("state/ndfd-point-check-2026-11-15")
    output.mkdir(parents=True, exist_ok=True)
    observed = {}
    ends = []
    for name in ("KLAX", "KNYC"):
        observed[name], end = cli_highs(cache, name)
        ends.append(end)
    start, end = date(2026, 10, 8), min(ends)
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    selection = select_objects(
        cache.list_objects(start - timedelta(days=8), end, snapshot="point-check-2026-11-15")
    )
    objects = sorted(selection.selected, key=lambda o: o.issued_at, reverse=True)
    decoded, metadata, downloaded, records = {}, {}, set(), []
    grid = None
    for label in ("primary", "secondary"):
        rows = []
        for day in days:
            as_of = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + (
                timedelta(hours=-6) if label == "primary" else timedelta(hours=12)
            )
            skipped = 0
            chosen = None
            for obj in objects:
                if obj.issued_at + lag > as_of:
                    continue
                if obj.issued_at < datetime.combine(
                    day, datetime.min.time(), tzinfo=UTC
                ) - timedelta(days=7):
                    break
                if obj.key not in metadata:
                    path = cache.fetch_object(obj)
                    downloaded.add(obj.key)
                    metadata[obj.key] = inspect_periods(path)
                candidates = []
                for s in stations:
                    begin, finish = climate_day(s, day)
                    candidates.extend(
                        p for p in metadata[obj.key] if begin <= p.start and p.end <= finish
                    )
                if not candidates:
                    skipped += 1
                    continue
                periods = tuple(set(candidates))
                key = (obj.key, periods)
                if key not in decoded:
                    path = cache.fetch_object(obj)
                    decoded[key] = tuple(decode_grib(path, obj, stations, lag=lag, periods=periods))
                    if grid is None:
                        import grib2io

                        with grib2io.open(str(path), save_index=False, use_index=False) as messages:
                            grid = grid_spec(messages[0])
                chosen = decoded[key]
                break
            for name in ("KLAX", "KNYC"):
                forecast = None
                if chosen is not None:
                    try:
                        forecast = select_forecast(chosen, name, day, as_of, lag=lag)
                    except NoQualifyingIssuance:
                        # A malformed per-station mapping is not a missing-issuance fallback.
                        raise ValueError(
                            "Chosen object did not cover a configured station"
                        ) from None
                    period_for_day(
                        [r.period for r in chosen if r.station.station == name],
                        forecast.station,
                        day,
                    )
                row = account_day(name, day, observed[name].get(day), forecast)
                row.update(
                    as_of=as_of.isoformat(),
                    decision=label,
                    fallback=forecast is not None and skipped > 0,
                    newer_issuances_without_period=skipped,
                    issuance_age_hours=(as_of - forecast.obj.issued_at).total_seconds() / 3600
                    if forecast
                    else None,
                    key=forecast.obj.key if forecast else None,
                )
                rows.append(row)
            print(f"{label} {day}: {obj.key if chosen else 'no qualifying issuance'}", flush=True)
        validate_accounting(rows, ["KLAX", "KNYC"], days)
        records.extend(rows)
    summaries = {
        f"{name}/{label}": summarize(
            [r for r in records if r["station"] == name and r["decision"] == label]
        )
        for name in ("KLAX", "KNYC")
        for label in ("primary", "secondary")
    }
    if grid is None:
        raise ValueError("No decoded fields")
    lax = next(s for s in stations if s.station == "KLAX")
    land = shoreline_check(cache, lax, containing_cell(grid, lax.latitude, lax.longitude))
    crosschecks = api_cell_crosschecks(cache, grid, stations)
    la, ny = summaries["KLAX/primary"], summaries["KNYC/primary"]
    criteria = {
        "cell_not_on_land": land["result"] == "water"
        if land["result"] != "indeterminate"
        else None,
        "absolute_bias_exceeds_KNYC_plus_2F": abs(la["mean_error_f"]) > abs(ny["mean_error_f"]) + 2
        if la["n"] and ny["n"]
        else None,
        "MAE_exceeds_1.5_times_KNYC": la["mae_f"] > 1.5 * ny["mae_f"]
        if la["n"] and ny["n"]
        else None,
    }
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (
        1024**2 if sys.platform == "darwin" else 1024
    )
    result = {
        "window": [start.isoformat(), end.isoformat()],
        "lag_hours": args.lag_hours,
        "summaries": summaries,
        "criteria_triggered": criteria,
        "land": land,
        "cell_crosschecks": crosschecks,
        "listing_selected": len(selection.selected),
        "listing_excluded": selection.excluded,
        "grib_objects_used": len(downloaded),
        "grib_bytes_used": sum(o.size for o in objects if o.key in downloaded),
        "http_requests_this_execution": cache.http.requests,
        "http_bytes": cache.http.bytes,
        "peak_rss_mib": rss,
        "records": records,
    }
    (output / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    lines = [
        "# NDFD point check",
        "",
        f"Window: {start} through {end}; L = {args.lag_hours} hours.",
        "Primary: D-1 18:00 UTC. Secondary: D 12:00 UTC. Error = native forecast "
        "minus IEM CLI high.",
        "",
        "| Station/as_of | n | Mean error °F | MAE °F | Sample error SD °F | "
        "Accounting | Issuance age hours (min/median/max) | Older fallbacks |",
        "|---|---:|---:|---:|---:|---|---|---:|",
    ]
    for key, s in summaries.items():
        ages = s["issuance_age_hours"]
        lines.append(
            f"| {key} | {s['n']} | {s['mean_error_f']} | {s['mae_f']} | "
            f"{s['error_sd_f']} | {s['accounting']} | "
            f"{ages['min']}/{ages['median']}/{ages['max']} | "
            f"{s['older_issuance_fallbacks']} |"
        )
    lines.extend(
        [
            "",
            "Criteria triggered (True means the supplied failure criterion is met; null "
            "means indeterminate):",
            json.dumps(criteria, indent=2),
            "",
            "Land check (NOAA CUSP, not an NDFD land-mask classification):",
            json.dumps(land, indent=2),
            "",
            "Station versus API polygon-centroid cells:",
            json.dumps(crosschecks, indent=2),
            "",
            f"GRIB objects: {len(downloaded)}; bytes: {result['grib_bytes_used']}; HTTP "
            f"requests: {cache.http.requests}; HTTP bytes: {cache.http.bytes}; "
            f"peak RSS: {rss:.2f} MiB.",
            "",
            "The full selected-issuance age distributions and station-day ledger are in "
            "results.json.",
            "No city decision is made. L is provisional; TWC's day is unestablished; "
            "the notebook's API overlap failed.",
            "IEM stored CLI values are latest revisions, not settlement-time labels. "
            "NOAA's cache is not a point-in-time observation archive.",
            "",
            "Sources: [IEM](https://mesonet.agron.iastate.edu/json/cli.py?help), [CUSP "
            "metadata](https://www.fisheries.noaa.gov/inport/item/60812), [NOAA CUSP](https://shoreline.noaa.gov/cusp.html).",
        ]
    )
    (output / "report.md").write_text("\n".join(lines) + "\n")
    cache.close()
    print(
        json.dumps(
            {k: v for k, v in result.items() if k not in ("records", "land", "cell_crosschecks")},
            indent=2,
        )
    )
    return 0
