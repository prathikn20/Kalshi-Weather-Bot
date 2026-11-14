"""Slow raw-first settled-market acquisition; no orders and no parsing dependencies."""

import argparse
import base64
import codecs
import csv
import fcntl
import gzip
import io
import json
import logging
import os
import re
import signal
import sqlite3
import tempfile
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

from kwb.collectors.kalshi import BASE_URL, is_daily_high_series, utc_timestamp

LOGGER = logging.getLogger(__name__)
REQUEST_INTERVAL = 10.0
REQUEST_DEADLINE = 20.0
WEEK = 7 * 86400


class CollectionError(ValueError):
    """Acquisition is incomplete; its successful watermark must not advance."""


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


@contextmanager
def whole_request_deadline(seconds):
    def expired(signum, frame):
        raise TimeoutError("whole-request deadline exceeded")

    previous = signal.signal(signal.SIGALRM, expired)
    old_timer = signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, *old_timer)
        signal.signal(signal.SIGALRM, previous)


def config_series(path):
    with path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    series = [row["series_ticker"] for row in rows]
    if (
        not series
        or len(series) != len(set(series))
        or any(re.fullmatch(r"KX[A-Z0-9]+", ticker) is None for ticker in series)
    ):
        raise CollectionError(f"{path}: invalid configured series")
    return set(series)


def discovered_series(body):
    """Decode one series at a time: the unfiltered response can exceed 20 MB."""
    match = re.match(r'\s*\{\s*"series"\s*:\s*\[', body)
    if not match:
        raise CollectionError("Expected a series array")
    position, matched = match.end(), set()
    decoder = json.JSONDecoder()
    first = True
    while True:
        position = re.compile(r"\s*").match(body, position).end()
        if body[position : position + 1] == "]":
            if body[position + 1 :].strip() != "}":
                raise CollectionError("Unexpected series response fields")
            return matched
        if not first:
            if body[position : position + 1] != ",":
                raise CollectionError("Expected a series separator")
            position = re.compile(r"\s*").match(body, position + 1).end()
        item, position = decoder.raw_decode(body, position)
        if not isinstance(item, dict):
            raise CollectionError("Expected a series object")
        if is_daily_high_series(item):
            matched.add(item["ticker"])
        first = False


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
        temp = Path(stream.name)
        try:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
            os.replace(temp, path)
        finally:
            temp.unlink(missing_ok=True)


def write_response_json(stream, envelope, body):
    """Escape bounded chunks, avoiding several full Unicode copies of /series."""
    chunk_size = 48 * 1024  # Also divisible by three for independent base64 chunks.
    decoder = codecs.getincrementaldecoder("utf-8")()
    try:
        for start in range(0, len(body), chunk_size):
            decoder.decode(body[start : start + chunk_size])
        decoder.decode(b"", final=True)
        encoding = None
    except UnicodeDecodeError:
        encoding = "base64"
    stream.write(json.dumps(envelope, ensure_ascii=False)[:-1] + ', "body": "')
    decoder = codecs.getincrementaldecoder("utf-8")()
    for start in range(0, len(body), chunk_size):
        chunk = body[start : start + chunk_size]
        if encoding:
            stream.write(base64.b64encode(chunk).decode("ascii"))
        else:
            stream.write(json.dumps(decoder.decode(chunk), ensure_ascii=False)[1:-1])
    if encoding:
        stream.write('", "body_encoding": "base64"}')
    else:
        stream.write(json.dumps(decoder.decode(b"", final=True), ensure_ascii=False)[1:-1])
        stream.write('"}')


class SettlementCollector:
    def __init__(
        self,
        raw_root=Path("raw/kalshi_settlements"),
        state=Path("state/kalshi-settlements.sqlite"),
        *,
        config=Path("config/nws_stations.csv"),
        now=lambda: datetime.now(UTC),
        monotonic=time.monotonic,
        sleep=time.sleep,
        open_url=None,
    ):
        self.raw_root, self.state, self.config = Path(raw_root), Path(state), Path(config)
        if self.state.resolve().is_relative_to(self.raw_root.resolve()):
            raise CollectionError("Collector state must be outside raw history")
        self.now, self.monotonic, self.sleep = now, monotonic, sleep
        self.open_url = open_url or build_opener(NoRedirect()).open
        self.next_request = 0.0
        self.request_clock = self.state.with_suffix(".request.json")
        self.report = {
            "requests": 0,
            "body_bytes": 0,
            "gzip_bytes": 0,
            "rate_limited": 0,
            "pages": 0,
            "markets": 0,
            "gaps": [],
            "errors": [],
            "complete": False,
        }
        self.last_locator = None

    def archive(self, url, params, status, requested, received, body):
        envelope = {
            "request_url": url,
            "request_params": params,
            "http_status": status,
            "requested_at": utc_timestamp(requested),
            "received_at": utc_timestamp(received),
        }
        directory = self.raw_root / received.date().isoformat()
        directory.mkdir(parents=True, exist_ok=True)
        name = utc_timestamp(received).replace(":", "") + ".json.gz"
        path = directory / name
        # Stage outside raw so compaction never mistakes a partial envelope for history.
        with tempfile.NamedTemporaryFile(dir=self.state.parent, delete=False) as stream:
            temp = Path(stream.name)
            try:
                with (
                    gzip.GzipFile(fileobj=stream, mode="wb", mtime=0) as compressed,
                    io.TextIOWrapper(compressed, encoding="utf-8") as text,
                ):
                    write_response_json(text, envelope, body)
                stream.flush()
                os.fsync(stream.fileno())
                os.link(temp, path)  # Complete file, same filesystem, refuse collisions.
                fd = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
                self.report["gzip_bytes"] += path.stat().st_size
            finally:
                temp.unlink(missing_ok=True)
        self.last_locator = str(path)
        return path

    def request(self, path, params=None):
        params = {} if params is None else params
        url = BASE_URL + path + ("?" + urlencode(params) if params else "")
        persisted_delay = 0.0
        if self.request_clock.exists():
            saved = json.loads(self.request_clock.read_text())
            if set(saved) != {"requested_at"}:
                raise CollectionError(f"{self.request_clock}: invalid request clock")
            last = datetime.fromisoformat(saved["requested_at"].replace("Z", "+00:00"))
            if last.tzinfo is None or last.utcoffset() != timedelta(0):
                raise CollectionError(f"{self.request_clock}: request time must be aware UTC")
            persisted_delay = REQUEST_INTERVAL - (self.now() - last).total_seconds()
        self.sleep(max(0.0, persisted_delay, self.next_request - self.monotonic()))
        self.next_request = self.monotonic() + REQUEST_INTERVAL
        requested = self.now()
        write_json(self.request_clock, {"requested_at": utc_timestamp(requested)})
        self.report["requests"] += 1
        try:
            with whole_request_deadline(REQUEST_DEADLINE):
                request = Request(
                    url, headers={"User-Agent": "Kalshi-Weather-Bot/0.1 (settlement archive)"}
                )
                try:
                    response = self.open_url(request, timeout=REQUEST_DEADLINE)
                except HTTPError as exc:
                    response = exc
                with response:
                    body, status = response.read(), response.status
                received = self.now()
        except (OSError, HTTPException) as exc:
            raise CollectionError(f"{url}: {type(exc).__name__}: {exc}") from exc
        self.report["body_bytes"] += len(body)
        locator = self.archive(url, params, status, requested, received, body)
        if status == 429:
            self.report["rate_limited"] += 1
        if status != 200:
            raise CollectionError(f"{locator}: HTTP {status}; stopping without retries")
        try:
            return body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CollectionError(f"{locator}: invalid UTF-8") from exc

    def listing(self, path, params):
        cursor, seen_cursors, seen_tickers, records = "", set(), set(), []
        while True:
            body = json.loads(
                self.request(path, {**params, **({"cursor": cursor} if cursor else {})})
            )
            if (
                set(body) != {"markets", "cursor"}
                or not isinstance(body["markets"], list)
                or not isinstance(body["cursor"], str)
            ):
                raise CollectionError(f"{self.last_locator}: expected markets and cursor")
            self.report["pages"] += 1
            self.report["markets"] += len(body["markets"])
            for item in body["markets"]:
                ticker, event = item["ticker"], item["event_ticker"]
                if (
                    not isinstance(ticker, str)
                    or not ticker
                    or not isinstance(event, str)
                    or not event
                ):
                    raise CollectionError(f"{self.last_locator}: invalid market identity")
                if ticker in seen_tickers:
                    raise CollectionError(f"{self.last_locator}: repeated ticker/page {ticker}")
                seen_tickers.add(ticker)
                records.append((ticker, event))
            next_cursor = body["cursor"]
            if not next_cursor:
                return records
            if next_cursor in seen_cursors or next_cursor == cursor:
                raise CollectionError(f"{self.last_locator}: repeated pagination cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor

    def cutoff(self):
        body = json.loads(self.request("/historical/cutoff"))
        value = datetime.fromisoformat(body["market_settled_ts"].replace("Z", "+00:00"))
        if value.tzinfo is None or value.utcoffset() != timedelta(0):
            raise CollectionError(f"{self.last_locator}: invalid historical cutoff")
        return int(value.timestamp())

    def run(self, *, backfill=False, configured_only=False):
        started = self.monotonic()
        self.state.parent.mkdir(parents=True, exist_ok=True)
        self.report["started_at"] = utc_timestamp(self.now())
        with self.state.with_suffix(".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with sqlite3.connect(self.state) as db:
                db.executescript(
                    "CREATE TABLE IF NOT EXISTS series("
                    "ticker TEXT PRIMARY KEY, upper_ts INTEGER, historical_at INTEGER);"
                    "CREATE TABLE IF NOT EXISTS runs("
                    "started_at TEXT PRIMARY KEY, report TEXT NOT NULL);"
                )
                try:
                    configured = config_series(self.config)
                    cutoff = self.cutoff()
                    body = self.request("/series")
                    discovered = discovered_series(body)
                    del body
                    if not discovered:
                        raise CollectionError(
                            f"{self.last_locator}: discovery returned no daily-high series"
                        )
                    db.executemany(
                        "INSERT OR IGNORE INTO series(ticker) VALUES (?)",
                        [(s,) for s in discovered | configured],
                    )
                    db.commit()
                    registry = {
                        r[0]: r[1:]
                        for r in db.execute("SELECT ticker,upper_ts,historical_at FROM series")
                    }
                    selected = configured if configured_only else set(registry)
                    upper = int(self.now().timestamp()) - 60
                    history = (
                        configured
                        if backfill
                        else {
                            s
                            for s in configured
                            if registry[s][1] is not None and upper - registry[s][1] >= WEEK
                        }
                    )
                    self.report["historical_backfill_required"] = sorted(
                        s for s in configured if registry[s][1] is None and not backfill
                    )
                    self.report.update(
                        series=sorted(selected),
                        historical_series=sorted(history),
                        cutoff=cutoff,
                        upper=upper,
                    )
                    for series in sorted(selected):
                        previous = registry[series][0]
                        lower = min(
                            previous - 1 if previous is not None else cutoff - 1, cutoff - 1
                        )
                        if previous is not None and previous < cutoff:
                            self.report["gaps"].append(
                                {
                                    "series": series,
                                    "start": previous,
                                    "end": cutoff,
                                    "recovery": "historical scan"
                                    if series in history
                                    else "requires historical recovery",
                                }
                            )
                        live = self.listing(
                            "/markets",
                            {
                                "series_ticker": series,
                                "status": "settled",
                                "min_settled_ts": lower,
                                "max_settled_ts": upper,
                                "limit": 1000,
                            },
                        )
                        historical = (
                            self.listing(
                                "/historical/markets", {"series_ticker": series, "limit": 1000}
                            )
                            if series in history
                            else []
                        )
                        # Only configured event structure is established. Complete split events,
                        # without ever filtering the series listing's raw response.
                        events = {}
                        for ticker, event in live + historical:
                            if event.split("-", 1)[0] in configured:
                                events.setdefault(event, set()).add(ticker)
                        for event, tickers in sorted(events.items()):
                            if len(tickers) != 6:
                                self.listing("/markets", {"event_ticker": event})
                                self.listing(
                                    "/historical/markets", {"event_ticker": event, "limit": 1000}
                                )
                    end_cutoff = self.cutoff()
                    if end_cutoff != cutoff:
                        raise CollectionError(
                            f"Historical cutoff moved {cutoff}->{end_cutoff}; rerun to reconcile"
                        )
                    for series in selected:
                        db.execute("UPDATE series SET upper_ts=? WHERE ticker=?", (upper, series))
                    for series in history & selected:
                        db.execute(
                            "UPDATE series SET historical_at=? WHERE ticker=?", (upper, series)
                        )
                    self.report["complete"] = True
                except (ValueError, KeyError, TypeError, OSError) as exc:
                    message = f"{self.last_locator or self.state}: {exc}"
                    self.report["errors"].append(message)
                    raise CollectionError(message) from exc
                finally:
                    self.report["duration_seconds"] = self.monotonic() - started
                    self.report["finished_at"] = utc_timestamp(self.now())
                    db.execute(
                        "INSERT INTO runs VALUES (?,?)",
                        (self.report["started_at"], json.dumps(self.report, sort_keys=True)),
                    )
                    db.commit()
                    LOGGER.info("Settlement collection %s", json.dumps(self.report, sort_keys=True))
        return self.report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=Path("raw/kalshi_settlements"))
    parser.add_argument("--state", type=Path, default=Path("state/kalshi-settlements.sqlite"))
    parser.add_argument("--config", type=Path, default=Path("config/nws_stations.csv"))
    parser.add_argument("--report", type=Path, default=Path("state/kalshi-settlements-report.json"))
    parser.add_argument("--backfill", action="store_true")
    parser.add_argument("--configured-only", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    collector = SettlementCollector(args.raw_root, args.state, config=args.config)
    try:
        collector.run(backfill=args.backfill, configured_only=args.configured_only)
    finally:
        write_json(args.report, collector.report)


if __name__ == "__main__":
    main()
