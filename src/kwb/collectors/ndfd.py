"""Archive one poll of public NDFD MaxT listings: ``uv run python -m kwb.collectors.ndfd``.

The five-minute systemd timer schedules polls. No GRIB objects are downloaded.
Every complete HTTP body is saved before XML parsing or HTTP error handling.
See docs/ndfd-collector.md for provider documentation and availability semantics.
"""

import argparse
import gzip
import json
import logging
import math
import random
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, build_opener

from kwb.collectors.kalshi import utc_now, utc_timestamp
from kwb.collectors.nws import NoRedirects
from kwb.collectors.raw import body_fields

LOGGER = logging.getLogger(__name__)
BASE_URL = "https://noaa-ndfd-pds.s3.amazonaws.com/"
USER_AGENT = "Kalshi-Weather-Bot/0.1 (https://github.com/prathikn20/Kalshi-Weather-Bot)"
REQUEST_INTERVAL = 1.0  # Client policy; no bucket-specific numeric LIST quota is published.
REQUEST_TIMEOUT = 10.0
MAX_ATTEMPTS = 3
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


class RequestFailed(Exception):
    """An HTTP request failed permanently or exhausted its retries."""


@dataclass(frozen=True)
class Listing:
    # Per-page observations, not a persistent first-seen index. Across raw history,
    # min(received_at) per key is first_seen_at; repeated listings never move it.
    arrivals: tuple[tuple[str, datetime], ...]
    next_token: str | None


def parse_listing(body: bytes, prefix: str, received_at: datetime) -> Listing:
    """Parse an already archived page; arrival timestamps come only from our fetch."""
    utc_timestamp(received_at)  # Reject naive/non-UTC timestamps at the boundary.
    root = ET.fromstring(body)
    if root.tag != NS + "ListBucketResult":
        raise ValueError("Expected an S3 ListBucketResult")

    def required(element: ET.Element, name: str) -> str:
        values = element.findall(NS + name)
        if len(values) != 1 or not values[0].text or not values[0].text.strip():
            raise ValueError(f"Expected exactly one nonempty {name}")
        return values[0].text

    if required(root, "Name") != "noaa-ndfd-pds" or required(root, "Prefix") != prefix:
        raise ValueError("Listing bucket or prefix differs from the request")
    if any(root.find(NS + field) is not None for field in ("CommonPrefixes", "EncodingType")):
        raise ValueError("Unexpected grouped or encoded listing")
    truncated = required(root, "IsTruncated")
    if truncated not in ("true", "false"):
        raise ValueError("IsTruncated must be true or false")
    arrivals = []
    seen = set()
    for obj in root.findall(NS + "Contents"):
        key = required(obj, "Key")
        if not key.startswith(prefix) or key in seen:
            raise ValueError(f"Unexpected or duplicate key: {key}")
        arrivals.append((key, received_at))
        seen.add(key)
    count = required(root, "KeyCount")
    if not count.isascii() or not count.isdigit() or int(count) != len(arrivals):
        raise ValueError("KeyCount differs from the returned objects")
    token = required(root, "NextContinuationToken") if truncated == "true" else None
    if truncated == "false" and root.find(NS + "NextContinuationToken") is not None:
        raise ValueError("An untruncated listing has a continuation token")
    return Listing(tuple(arrivals), token)


class NDFDCollector:
    def __init__(
        self,
        raw_root: Path = Path("raw/ndfd"),
        *,
        now: Callable[[], datetime] = utc_now,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.raw_root = raw_root
        self.now, self.monotonic, self.sleep = now, monotonic, sleep
        self.opener = build_opener(NoRedirects())
        self.next_request = 0.0
        self.request_count = 0

    def save_response(
        self,
        url: str,
        params: dict[str, str],
        status: int,
        requested_at: datetime,
        received_at: datetime,
        body: bytes,
    ) -> None:
        envelope = {
            "request_url": url,
            "request_params": params,
            "http_status": status,
            "requested_at": utc_timestamp(requested_at),
            "received_at": utc_timestamp(received_at),
            **body_fields(body),
        }
        directory = self.raw_root / received_at.date().isoformat()
        directory.mkdir(parents=True, exist_ok=True)
        filename = utc_timestamp(received_at).replace(":", "") + ".json.gz"
        compressed = gzip.compress(json.dumps(envelope, ensure_ascii=False).encode("utf-8"))
        with (directory / filename).open("xb") as output:
            output.write(compressed)

    def request(self, params: dict[str, str]) -> tuple[bytes, datetime]:
        url = BASE_URL + "?" + urlencode(params)
        request = Request(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "application/xml"},
            method="GET",
        )  # Unsigned: no AWS account, credentials, or SDK required.
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
                    body = response.read()
                    received_at = self.now()
                    status = response.status
                    headers = response.headers
            except (OSError, HTTPException) as error:
                failure = f"{type(error).__name__}: {error}"
                retryable = True
            else:
                # Outside the network handler: an archive-write error must escape.
                self.save_response(url, params, status, requested_at, received_at, body)
                if status == 200:
                    return body, received_at
                failure = f"HTTP {status}"
                retryable = status in (408, 429) or 500 <= status < 600
                retry_after = headers.get("Retry-After")

            LOGGER.warning("Request failed url=%s attempt=%d: %s", url, attempt + 1, failure)
            if not retryable or attempt + 1 == MAX_ATTEMPTS:
                raise RequestFailed(f"{url}: {failure}")
            backoff = max(5.0 if status == 429 else 1.0, 2.0**attempt)
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

    def collect_prefix(self, prefix: str) -> None:
        params = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
        tokens: set[str] = set()
        while True:
            body, received_at = self.request(params)
            listing = parse_listing(body, prefix, received_at)
            token = listing.next_token
            if token is None:
                return
            if token in tokens:
                raise ValueError("S3 repeated a continuation token")
            tokens.add(token)
            params = {**params, "continuation-token": token}

    def run_cycle(self) -> bool:
        """Attempt both prefixes even if one fails; return whether both completed."""
        started = self.monotonic()
        now = self.now()
        utc_timestamp(now)
        today = now.date()
        completed = 0
        self.request_count = 0
        try:
            # Pin the pair for the poll. The next poll includes a date crossed mid-poll.
            for day in (today, today - timedelta(days=1)):
                prefix = f"wmo/maxt/{day:%Y/%m/%d}/"
                try:
                    self.collect_prefix(prefix)
                except (RequestFailed, ValueError, ET.ParseError):
                    LOGGER.exception("Listing failed prefix=%s; continuing", prefix)
                else:
                    completed += 1
        finally:
            LOGGER.info(
                "Cycle duration=%.3fs request_count=%d success=%s",
                self.monotonic() - started,
                self.request_count,
                completed == 2,
            )
        return completed == 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-root", type=Path, default=Path("raw/ndfd"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    return 0 if NDFDCollector(args.raw_root).run_cycle() else 1


if __name__ == "__main__":
    raise SystemExit(main())
