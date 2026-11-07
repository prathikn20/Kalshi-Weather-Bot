"""Capture unchanged live NWS envelopes for offline property tests.

Run: uv run python scripts/capture_nws_fixtures.py
An ocean /points lookup deliberately captures a real NWS error, not a fake body.
"""

import gzip
import json
import shutil
from contextlib import suppress
from pathlib import Path

from kwb.collectors.nws import BASE_URL, NWSCollector, RequestFailed, load_stations


def main() -> None:
    root = Path("tests/fixtures/nws")
    station = next(station for station in load_stations() if station.station_id == "KNYC")
    collector = NWSCollector((station,), root)
    for url in collector.forecast_urls(station):
        collector.request(url)
    with suppress(RequestFailed):
        collector.request(f"{BASE_URL}/points/0.0000,0.0000")
    # Metadata fetched by the derivation script has the identical raw envelope.
    for path in sorted(Path("raw/nws").rglob("*.json.gz")):
        envelope = json.loads(gzip.decompress(path.read_bytes()))
        if "/stations/" in envelope["request_url"]:
            destination = root / path.parent.name / path.name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, destination)
    print(f"Captured fixtures under {root}")


if __name__ == "__main__":
    main()
