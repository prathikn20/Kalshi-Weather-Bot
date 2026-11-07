"""Raw NWS collector: ``uv run python -m kwb.collectors.nws``.

Archive response text before navigating /points. Forecast bodies are opaque.
received_at is the UTC instant the complete body arrives, never its issue time.
See docs/nws-collector.md for provider documentation and station provenance.
"""

import argparse
import csv
import gzip
import json
import logging
import math
import random
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from kwb.collectors.kalshi import required_string, utc_now, utc_timestamp

LOGGER = logging.getLogger(__name__)
BASE_URL = "https://api.weather.gov"
USER_AGENT = "Kalshi-Weather-Bot/0.1 (https://github.com/prathikn20/Kalshi-Weather-Bot)"
POLL_INTERVAL = 1800.0
POINTS_REFRESH_INTERVAL = 86400.0
REQUEST_INTERVAL = 1.0  # Our conservative policy; NWS publishes no numeric quota.
REQUEST_TIMEOUT = 10.0
MAX_ATTEMPTS = 3
FORECAST_FIELDS = ("forecast", "forecastHourly", "forecastGridData")
DEFAULT_CONFIG = Path("config/nws_stations.csv")


class RequestFailed(Exception):
    """A request failed permanently or exhausted its bounded retries."""


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # urllib raises HTTPError with the original response, which we archive.
        return None


@dataclass(frozen=True)
class Station:
    station_id: str
    latitude: float
    longitude: float

    @property
    def points_url(self) -> str:
        return f"{BASE_URL}/points/{self.latitude:.4f},{self.longitude:.4f}"


def load_stations(path: Path = DEFAULT_CONFIG) -> tuple[Station, ...]:
    stations = []
    seen = set()
    with path.open(newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            for field in (
                "series_ticker",
                "cli_code",
                "station_id",
                "station_name",
                "latitude",
                "longitude",
                "settlement_source",
                "site_verification_urls",
                "verified_how",
                "verified_at",
                "archive_path",
                "market_ticker",
            ):
                required_string(row, field)
            station_id = row["station_id"]
            if not re.fullmatch(r"[A-Z0-9]{4}", station_id) or station_id in seen:
                raise ValueError(f"Invalid or duplicate station: {station_id}")
            latitude, longitude = float(row["latitude"]), float(row["longitude"])
            if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
                raise ValueError(f"Invalid coordinates for {station_id}")
            stations.append(Station(station_id, latitude, longitude))
            seen.add(station_id)
    if not stations:
        raise ValueError("Station config is empty")
    return tuple(stations)


def validate_forecast_url(url: str) -> str:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "api.weather.gov"
        or not re.fullmatch(r"/gridpoints/[A-Z]{3}/\d+,\d+(?:/forecast(?:/hourly)?)?", parsed.path)
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"Unexpected forecast URL: {url}")
    return url


class NWSCollector:
    def __init__(
        self,
        stations: tuple[Station, ...],
        raw_root: Path = Path("raw/nws"),
        *,
        now: Callable[[], datetime] = utc_now,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.stations = stations
        self.raw_root = raw_root
        self.now, self.monotonic, self.sleep = now, monotonic, sleep
        self.opener = build_opener(NoRedirects())
        self.points_cache: dict[str, tuple[str, ...]] = {}
        self.next_points_refresh: dict[str, float] = {}
        self.next_request = 0.0
        self.request_count = 0

    def save_response(
        self,
        url: str,
        status: int,
        requested_at: datetime,
        received_at: datetime,
        body: str,
    ) -> None:
        envelope = {
            "request_url": url,
            "request_params": {},
            "http_status": status,
            "requested_at": utc_timestamp(requested_at),
            "received_at": utc_timestamp(received_at),
            "body": body,
        }
        directory = self.raw_root / received_at.date().isoformat()
        directory.mkdir(parents=True, exist_ok=True)
        filename = utc_timestamp(received_at).replace(":", "") + ".json.gz"
        compressed = gzip.compress(json.dumps(envelope, ensure_ascii=False).encode("utf-8"))
        with (directory / filename).open("xb") as output:
            output.write(compressed)

    def request(self, url: str) -> str:
        request = Request(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "application/geo+json"},
            method="GET",
        )
        for attempt in range(MAX_ATTEMPTS):
            self.sleep(max(0.0, self.next_request - self.monotonic()))
            requested_at = self.now()
            self.next_request = self.monotonic() + REQUEST_INTERVAL
            self.request_count += 1
            retry_after = None
            status = None
            try:
                try:
                    response = self.opener.open(request, timeout=REQUEST_TIMEOUT)
                except HTTPError as error:
                    response = error
                with response:
                    body_bytes = response.read()
                    received_at = self.now()
                    body = body_bytes.decode("utf-8")
                    status = response.status
                    retry_after = response.headers.get("Retry-After")
            except (OSError, HTTPException) as error:
                failure = f"{type(error).__name__}: {error}"
                retryable = True
            else:
                # Keep archive failures outside the network exception handler: data loss is fatal.
                self.save_response(url, status, requested_at, received_at, body)
                if 200 <= status < 300:
                    return body
                failure = f"HTTP {status}"
                retryable = status in (408, 429) or 500 <= status < 600

            LOGGER.warning("Request failed url=%s attempt=%d: %s", url, attempt + 1, failure)
            if not retryable or attempt + 1 == MAX_ATTEMPTS:
                raise RequestFailed(f"{url}: {failure}")
            backoff = min(30.0, 2.0**attempt)
            if status == 429:
                backoff = max(5.0, backoff)
            if retry_after is not None:
                try:
                    delay = float(retry_after)
                    if not math.isfinite(delay) or delay < 0:
                        raise ValueError("Invalid delay")
                except ValueError:
                    try:
                        delay = (parsedate_to_datetime(retry_after) - received_at).total_seconds()
                    except (ValueError, TypeError, OverflowError):
                        LOGGER.warning("Invalid Retry-After url=%s value=%r", url, retry_after)
                        delay = 0.0
                backoff = max(backoff, delay)
            self.sleep(backoff + random.uniform(0.0, backoff / 4))
        raise AssertionError("Unreachable retry state")

    def forecast_urls(self, station: Station) -> tuple[str, ...]:
        station_id = station.station_id
        if self.monotonic() >= self.next_points_refresh.get(station_id, 0.0):
            try:
                payload = json.loads(self.request(station.points_url))
                if not isinstance(payload, dict) or not isinstance(payload.get("properties"), dict):
                    raise ValueError("Expected points properties object")
                properties = payload["properties"]
                urls = tuple(
                    validate_forecast_url(required_string(properties, field))
                    for field in FORECAST_FIELDS
                )
            except (RequestFailed, ValueError, KeyError):
                # This method runs once per station per scheduled cycle. Keep it due
                # so a slow failed refresh does not accidentally skip the next tick.
                self.next_points_refresh[station_id] = self.monotonic()
                LOGGER.exception("Points refresh failed station=%s; retaining cache", station_id)
                if station_id not in self.points_cache:
                    raise
            else:
                self.points_cache[station_id] = urls
                self.next_points_refresh[station_id] = self.monotonic() + POINTS_REFRESH_INTERVAL
        return self.points_cache[station_id]

    def run_cycle(self) -> None:
        started = self.monotonic()
        self.request_count = 0
        try:
            for station in self.stations:
                try:
                    urls = self.forecast_urls(station)
                except (RequestFailed, ValueError, KeyError):
                    LOGGER.exception("Station failed station=%s; continuing", station.station_id)
                    continue
                for field, url in zip(FORECAST_FIELDS, urls, strict=True):
                    try:
                        self.request(url)
                    except (RequestFailed, ValueError):
                        LOGGER.exception(
                            "Forecast failed station=%s endpoint=%s; continuing",
                            station.station_id,
                            field,
                        )
        finally:
            LOGGER.info(
                "Cycle duration=%.3fs request_count=%d",
                self.monotonic() - started,
                self.request_count,
            )

    def run_forever(self, *, cycles: int | None = None) -> None:
        next_tick = self.monotonic()
        completed = 0
        while cycles is None or completed < cycles:
            self.sleep(max(0.0, next_tick - self.monotonic()))
            self.run_cycle()
            completed += 1
            next_tick += POLL_INTERVAL
            overdue = self.monotonic() - next_tick
            if overdue > 0:
                skipped = math.ceil(overdue / POLL_INTERVAL)
                next_tick += skipped * POLL_INTERVAL
                LOGGER.warning("Cycle overran schedule; skipped_ticks=%d", skipped)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--raw-root", type=Path, default=Path("raw/nws"))
    parser.add_argument("--cycles", type=int, help="Stop after this many scheduled cycles")
    args = parser.parse_args()
    if args.cycles is not None and args.cycles < 1:
        parser.error("--cycles must be positive")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    try:
        NWSCollector(load_stations(args.config), args.raw_root).run_forever(cycles=args.cycles)
    except KeyboardInterrupt:
        LOGGER.info("NWS collector stopped")


if __name__ == "__main__":
    main()
