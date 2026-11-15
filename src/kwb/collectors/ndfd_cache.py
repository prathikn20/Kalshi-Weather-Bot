"""Unsigned, rate-limited NOAA cache with a fail-closed integrity manifest."""

import hashlib
import json
import math
import random
import sqlite3
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from http.client import HTTPException
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, build_opener

from kwb.collectors.ndfd import BASE_URL, NS, USER_AGENT, parse_listing
from kwb.collectors.nws import NoRedirects
from kwb.parse.ndfd import ObjectInfo


class DownloadFailed(RuntimeError):
    pass


class HTTPClient:
    def __init__(
        self,
        *,
        monotonic=time.monotonic,
        sleep=time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        opener=None,
    ):
        self.monotonic, self.sleep, self.now = monotonic, sleep, now
        self.opener = opener or build_opener(NoRedirects())
        self.next_start = 0.0
        self.requests = 0
        self.bytes = 0

    def get(self, url: str) -> tuple[bytes, dict[str, str], datetime]:
        request = Request(url, headers={"User-Agent": USER_AGENT}, method="GET")
        for attempt in range(3):
            self.sleep(max(0, self.next_start - self.monotonic()))
            self.next_start = self.monotonic() + 1.000001
            self.requests += 1
            status, headers = None, {}
            try:
                try:
                    response = self.opener.open(request, timeout=30)
                except HTTPError as error:
                    response = error
                with response:
                    body = response.read()
                    status = response.status
                    headers = {k.lower(): v for k, v in response.headers.items()}
                    received_at = self.now()
                    self.bytes += len(body)
            except (OSError, HTTPException) as error:
                reason, retryable = str(error), True
            else:
                if status == 200:
                    if "content-length" in headers and int(headers["content-length"]) != len(body):
                        raise ValueError("Incomplete HTTP body")
                    return body, headers, received_at
                reason = f"HTTP {status}"
                retryable = status in (408, 429) or 500 <= status < 600
            if not retryable or attempt == 2:
                raise DownloadFailed(f"{url}: {reason}")
            delay = max(5.0 if status == 429 else 1.0, 2.0**attempt)
            if "retry-after" in headers:
                value = headers["retry-after"]
                try:
                    retry_delay = float(value)
                except ValueError:
                    retry_delay = (parsedate_to_datetime(value) - received_at).total_seconds()
                if not math.isfinite(retry_delay):
                    raise ValueError("Invalid Retry-After")
                delay = max(delay, retry_delay)
            self.sleep(delay + random.uniform(0, 0.25))
        raise AssertionError("Unreachable retry state")


class NDFDCache:
    def __init__(self, root: Path, *, http: HTTPClient | None = None):
        self.root = root
        self.http = http or HTTPClient()
        root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(root / "manifest.sqlite")
        self.db.execute("""CREATE TABLE IF NOT EXISTS objects (
            key TEXT PRIMARY KEY, size INTEGER NOT NULL, etag TEXT NOT NULL,
            sha256 TEXT NOT NULL, received_at TEXT NOT NULL)""")

    def close(self):
        self.db.close()

    def fetch_object(self, obj: ObjectInfo) -> Path:
        key = PurePosixPath(obj.key)
        if key.is_absolute() or ".." in key.parts or not obj.key.startswith("wmo/maxt/"):
            raise ValueError("Unsafe NDFD key")
        path = self.root / "objects" / obj.key
        row = self.db.execute(
            "SELECT size,etag,sha256,received_at FROM objects WHERE key=?", (obj.key,)
        ).fetchone()
        if row is not None:
            if not path.is_file() or path.stat().st_size != row[0]:
                raise ValueError("Cache size or file does not match manifest")
            if hashlib.file_digest(path.open("rb"), "sha256").hexdigest() != row[2]:
                raise ValueError("Cache SHA-256 does not match manifest")
            if obj.size != row[0] or obj.etag != row[1]:
                raise ValueError("Listing differs from cached manifest")
            return path
        if path.exists():
            raise ValueError("Unmanifested cache file; refusing to overwrite")
        body, headers, received = self.http.get(BASE_URL + obj.key)
        if len(body) != obj.size or headers.get("etag") != obj.etag:
            raise ValueError("Downloaded object differs from listing size/ETag")
        digest = hashlib.sha256(body).hexdigest()
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".partial")
        with temporary.open("xb") as stream:
            stream.write(body)
        temporary.replace(path)
        with self.db:
            self.db.execute(
                "INSERT INTO objects VALUES (?,?,?,?,?)",
                (obj.key, len(body), obj.etag, digest, received.isoformat()),
            )
        return path

    def cached_response(self, url: str, directory: Path) -> bytes:
        """Frozen exact public response plus URL, receipt time, headers and hash."""
        directory.mkdir(parents=True, exist_ok=True)
        stem = hashlib.sha256(url.encode()).hexdigest()
        path, meta = directory / (stem + ".body"), directory / (stem + ".json")
        if path.exists() or meta.exists():
            record = json.loads(meta.read_text())
            body = path.read_bytes()
            if record["url"] != url or record["sha256"] != hashlib.sha256(body).hexdigest():
                raise ValueError("Cached response integrity mismatch")
            return body
        body, headers, received = self.http.get(url)
        path.write_bytes(body)
        meta.write_text(
            json.dumps(
                {
                    "url": url,
                    "headers": headers,
                    "received_at": received.isoformat(),
                    "size": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                },
                indent=2,
            )
        )
        return body

    def list_objects(self, start: date, end: date, *, snapshot: str) -> tuple[ObjectInfo, ...]:
        objects = {}
        day = start
        while day <= end:
            prefix = f"wmo/maxt/{day:%Y/%m/%d}/"
            params = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
            seen_tokens = set()
            while True:
                url = BASE_URL + "?" + urlencode(params)
                body = self.cached_response(url, self.root / "listings" / snapshot)
                listing = parse_listing(body, prefix, datetime.now(UTC))
                root = ET.fromstring(body)
                for item in root.findall(NS + "Contents"):

                    def required(name, element=item):
                        nodes = element.findall(NS + name)
                        if len(nodes) != 1 or not nodes[0].text:
                            raise ValueError(f"Expected exactly one {name}")
                        return nodes[0].text

                    obj = ObjectInfo(required("Key"), int(required("Size")), required("ETag"))
                    if obj.size <= 0 or obj.key in objects:
                        raise ValueError("Invalid size or duplicate listing key")
                    objects[obj.key] = obj
                token = listing.next_token
                if token is None:
                    break
                if token in seen_tokens:
                    raise ValueError("Repeated continuation token")
                seen_tokens.add(token)
                params["continuation-token"] = token
            day += timedelta(days=1)
        return tuple(objects.values())
