"""Audit archived Kalshi discovery and derive US CLI sites (one-off research).

Run: uv run python scripts/derive_nws_stations.py
Add --write-config config/nws_stations.csv to fetch/archive NWS station metadata.
The hand-verified mapping below uses named NWS sites, never CLI suffix guessing.
"""

import argparse
import csv
import gzip
import json
import re
import tarfile
from pathlib import Path
from urllib.parse import urlsplit

import zstandard

from kwb.collectors.kalshi import is_daily_high_series, required_string, response_records
from kwb.collectors.nws import BASE_URL, NWSCollector

PRODUCT = "https://forecast.weather.gov/product.php?product=CLI&issuedby={}&site={}"
# CLI reports establish site identity, not the market's settlement data provider.
VERIFIED_SITES = {
    "CLIAUS": ("KAUS", "Austin-Bergstrom International Airport", PRODUCT.format("AUS", "EWX")),
    "CLIMDW": ("KMDW", "Chicago, Chicago Midway Airport", PRODUCT.format("MDW", "LOT")),
    "CLIDEN": (
        "KDEN",
        "Denver, Denver International Airport",
        PRODUCT.format("DEN", "BOU") + " | https://www.weather.gov/bou/local_climate",
    ),
    "CLILAX": (
        "KLAX",
        "Los Angeles, Los Angeles International Airport",
        PRODUCT.format("LAX", "LOX"),
    ),
    "CLIMIA": (
        "KMIA",
        "Miami, Miami International Airport",
        PRODUCT.format("MIA", "MFL") + " | https://www.weather.gov/mfl/mia_1",
    ),
    "CLINYC": ("KNYC", "New York City, Central Park", PRODUCT.format("NYC", "OKX")),
    "CLIPHL": (
        "KPHL",
        "Philadelphia, Philadelphia International Airport",
        PRODUCT.format("PHL", "PHI") + " | https://www.weather.gov/phi/climate_archive_phl",
    ),
}
# No non-US sites occur in the current archive. New ones need explicit verification.
VERIFIED_NON_US_SERIES: dict[str, str] = {}


def archived_envelopes(root: Path):
    for path in sorted(root.rglob("*")):
        if path.name.endswith(".json.gz"):
            yield str(path), json.loads(gzip.decompress(path.read_bytes()))
        elif path.name.endswith(".tar.zst"):
            with (
                path.open("rb") as raw,
                zstandard.ZstdDecompressor().stream_reader(raw) as reader,
                tarfile.open(fileobj=reader, mode="r|") as archive,
            ):
                for member in archive:
                    if not member.isfile() or not member.name.endswith(".json"):
                        raise ValueError(f"Unexpected archive member: {path}:{member.name}")
                    body = archive.extractfile(member)
                    assert body is not None
                    yield f"{path}:{member.name}", json.load(body)


def derive(root: Path) -> dict:
    snapshots = []
    markets = []
    for path, envelope in archived_envelopes(root):
        if envelope["http_status"] != 200:
            continue
        endpoint = urlsplit(envelope["request_url"]).path
        if endpoint.endswith("/series"):
            metadata = response_records(envelope["body"], "series")
            present = {required_string(item, "ticker") for item in metadata}
            matched = {
                required_string(item, "ticker") for item in metadata if is_daily_high_series(item)
            }
            candidates = {
                item["ticker"]
                for item in metadata
                if item["frequency"].casefold() == "daily"
                and "temp" in item["title"].casefold()
                and re.search(r"\b(highest|high|maximum|max)\b", item["title"].casefold())
            }
            snapshots.append(
                {
                    "archive_path": path,
                    "received_at": envelope["received_at"],
                    "metadata_series_count": len(present),
                    "metadata_kx_series_count": sum(ticker.startswith("KX") for ticker in present),
                    "collector_matched_count": len(matched),
                    "collector_matched_kx_count": sum(
                        ticker.startswith("KX") for ticker in matched
                    ),
                    "collector_matched_series": sorted(matched),
                    "unmatched_high_temperature_candidates": sorted(candidates - matched),
                    "unmatched_high_temperature_candidate_count": len(candidates - matched),
                }
            )
        elif endpoint.endswith("/markets"):
            markets.extend((path, item) for item in response_records(envelope["body"], "markets"))
    if not snapshots:
        raise ValueError("No successful archived /series metadata to audit discovery")
    matched = {ticker for snapshot in snapshots for ticker in snapshot["collector_matched_series"]}
    observed = set()
    rows = {}
    skipped = {}
    unresolved = {}
    for path, market in markets:
        ticker = required_string(market, "ticker")
        series = ticker.split("-", 1)[0]
        if not series.startswith("KX"):
            continue
        observed.add(series)
        if series in VERIFIED_NON_US_SERIES:
            skipped[series] = VERIFIED_NON_US_SERIES[series]
            continue
        rules = required_string(market, "rules_primary")
        cli_codes = set(re.findall(r"\bCLI[A-Z0-9]{3}\b", rules))
        source = re.search(r"according to (.+?), then the market resolves", rules)
        if len(cli_codes) != 1 or source is None:
            unresolved[series] = {"market_ticker": ticker, "rules_primary": rules}
            continue
        cli = cli_codes.pop()
        if cli not in VERIFIED_SITES:
            unresolved[series] = {"cli_code": cli, "market_ticker": ticker}
            continue
        key = (series, cli, source.group(1))
        rows.setdefault(
            key,
            {
                "series_ticker": series,
                "cli_code": cli,
                "settlement_source": source.group(1),
                "archive_path": path,
                "market_ticker": ticker,
                "rules_primary": rules,
            },
        )
    for series in observed:
        variants = [key for key in rows if key[0] == series]
        if len(variants) > 1:
            raise ValueError(f"Conflicting settlement sites or sources for {series}: {variants}")
    return {
        "series_snapshots": sorted(snapshots, key=lambda item: item["received_at"]),
        "collector_matched_series_count": len(matched),
        "archived_market_series_count": len(observed),
        "matched_without_archived_markets": sorted(matched - observed),
        "archived_market_series_not_matched": sorted(observed - matched),
        "skipped_non_us_series": skipped,
        "unresolved_series": unresolved,
        "sites": [rows[key] for key in sorted(rows)],
    }


def write_config(report: dict, destination: Path, raw_root: Path) -> None:
    if report["unresolved_series"]:
        raise ValueError(f"Unverified sites require research: {report['unresolved_series']}")
    collector = NWSCollector((), raw_root)
    rows = []
    for site in report["sites"]:
        station_id, expected_name, site_urls = VERIFIED_SITES[site["cli_code"]]
        station_url = f"{BASE_URL}/stations/{station_id}"
        payload = json.loads(collector.request(station_url))
        properties = payload["properties"]
        if properties["stationIdentifier"] != station_id or properties["name"] != expected_name:
            raise ValueError(f"Station identity changed: {station_url}")
        if payload["geometry"]["type"] != "Point":
            raise ValueError(f"Expected station Point geometry: {station_url}")
        longitude, latitude = payload["geometry"]["coordinates"]
        rows.append(
            {
                **{key: value for key, value in site.items() if key != "rules_primary"},
                "station_id": station_id,
                "station_name": expected_name,
                "latitude": latitude,
                "longitude": longitude,
                "site_verification_urls": f"{site_urls} | {station_url}",
                "verified_at": collector.now().date().isoformat(),
                "verified_how": (
                    "Archived rules identify CLI code; NWS CLI report and linked climate history "
                    "identify site; NWS stationIdentifier/name and Point geometry verify ASOS "
                    "identity and coordinates. settlement_source is copied verbatim "
                    "from rules_primary."
                ),
            }
        )
    if not rows:
        raise ValueError("No verified US sites")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=Path("raw/kalshi"))
    parser.add_argument("--write-config", type=Path)
    parser.add_argument("--nws-raw-root", type=Path, default=Path("raw/nws"))
    args = parser.parse_args()
    report = derive(args.raw_root)
    print(json.dumps(report, indent=2))
    if args.write_config:
        write_config(report, args.write_config, args.nws_raw_root)


if __name__ == "__main__":
    main()
