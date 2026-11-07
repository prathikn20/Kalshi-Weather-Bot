"""Public Kalshi raw collector: ``uv run python -m kwb.collectors.kalshi``.

Archive every HTTP response before inspecting discovery fields. ``body`` is
the original UTF-8 response text, including error bodies, not normalized JSON.
``received_at`` is our first availability time and determines the UTC directory
and filename. Discovery only navigates series metadata, tickers, and cursors;
market prices and order books are never parsed into domain records here.

Order books are fetched in batches via ``GET /markets/orderbooks`` after every
series' open markets are listed. A batch envelope's ``request_params`` holds the
exact ``tickers`` list, so a parser can tell batch bodies from single-market
bodies by the request URL. A failed batch falls back to single-market requests
for that chunk only.
"""

import gzip
import json
import logging
import math
import random
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

LOGGER = logging.getLogger(__name__)
Params = dict[str, str | int | list[str]]
BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
POLL_INTERVAL = 60.0
SERIES_REFRESH_INTERVAL = 3600.0
REQUEST_INTERVAL = 0.1  # Conservative 10 requests/s; public quota is undocumented.
REQUEST_TIMEOUT = 10.0
MAX_ATTEMPTS = 3
# Default token cost per request; GET /account/endpoint_costs lists no override for
# market or order book reads. Kalshi bills batch items individually.
TOKEN_COST = 10
# 50 tickers x 10 tokens = 500, inside the 600-token Basic read burst (200/s x 3 s).
ORDERBOOK_BATCH_SIZE = 50
# Discovery vocabulary, compared against the set of alphabetic words in a series title.
HIGH_WORDS = frozenset({"high", "highest", "max", "maximum"})
TEMPERATURE_WORDS = frozenset({"temp", "temperature", "temperatures"})
LOW_WORDS = frozenset({"low", "lowest", "min", "minimum"})
# Titles about a derived quantity (e.g. "highest temperature difference") are not daily highs.
DERIVED_WORDS = frozenset({"difference", "change", "anomaly"})
TITLE_WORD = re.compile(r"[a-z]+")


class RequestFailed(Exception):
    """A request failed permanently or exhausted its bounded retries."""


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("Collector timestamps must be timezone-aware UTC")
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def required_string(record: dict, field: str) -> str:
    value = record[field]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Expected a nonempty string for {field}")
    return value


def is_daily_high_series(series: dict) -> bool:
    """Match daily series whose metadata names a daily high/max temperature.

    Match metadata rather than city tickers so new cities are discovered.
    Kalshi titles vary freely ("Atlanta Max Temperature", "Daily high temp
    Tokyo"), so the title is treated as a set of words: word order and city
    position do not matter. A series matches when it is daily, the title has a
    temperature word, a high signal comes from the title or from "HIGH" in the
    ticker (the Death Valley series title omits "high"), and the title has no
    low or derived-quantity word. Daily frequency excludes hourly, weekly, and
    custom records. Missing fields raise.
    """
    frequency = required_string(series, "frequency").casefold()
    ticker = required_string(series, "ticker").casefold()
    words = set(TITLE_WORD.findall(required_string(series, "title").casefold()))
    has_high_signal = bool(words & HIGH_WORDS) or "high" in ticker
    return (
        frequency == "daily"
        and has_high_signal
        and bool(words & TEMPERATURE_WORDS)
        and not words & (LOW_WORDS | DERIVED_WORDS)
    )


def response_records(body: str, field: str) -> list[dict]:
    payload = json.loads(body)
    if not isinstance(payload, dict) or not isinstance(payload.get(field), list):
        raise ValueError(f"Expected a JSON object containing a {field} list")
    records = payload[field]
    if any(not isinstance(record, dict) for record in records):
        raise ValueError(f"Expected objects in {field}")
    return records


class KalshiCollector:
    def __init__(
        self,
        raw_root: Path = Path("raw/kalshi"),
        *,
        now: Callable[[], datetime] = utc_now,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.raw_root = raw_root
        self.now = now
        self.monotonic = monotonic
        self.sleep = sleep
        self.series_tickers: set[str] | None = None
        self.next_series_refresh = 0.0
        self.next_request = 0.0
        self.request_count = 0
        self.rate_limited_count = 0
        self.token_estimate = 0

    def save_response(
        self,
        url: str,
        params: Params,
        status: int,
        requested_at: datetime,
        received_at: datetime,
        body: str,
    ) -> None:
        envelope = {
            "request_url": url,
            "request_params": params,
            "http_status": status,
            "requested_at": utc_timestamp(requested_at),
            "received_at": utc_timestamp(received_at),
            "body": body,
        }
        directory = self.raw_root / received_at.date().isoformat()
        directory.mkdir(parents=True, exist_ok=True)
        filename = utc_timestamp(received_at).replace(":", "") + ".json.gz"
        compressed = gzip.compress(json.dumps(envelope, ensure_ascii=False).encode("utf-8"))
        # Fail loudly rather than overwrite history on a timestamp collision.
        with (directory / filename).open("xb") as output:
            output.write(compressed)

    def request(self, path: str, params: Params | None = None, *, tokens: int = TOKEN_COST) -> str:
        """GET with bounded retries, archiving every response.

        ``tokens`` is this request's estimated rate-limit cost, counted on every
        attempt. List values are sent as repeated query parameters.
        """
        params = {} if params is None else params
        url = BASE_URL + path
        request_url = url + ("?" + urlencode(params, doseq=True) if params else "")
        request = Request(
            request_url,
            headers={"User-Agent": "Kalshi-Weather-Bot/0.1 (public weather data collector)"},
            method="GET",
        )
        for attempt in range(MAX_ATTEMPTS):
            self.sleep(max(0.0, self.next_request - self.monotonic()))
            requested_at = self.now()
            self.next_request = self.monotonic() + REQUEST_INTERVAL
            self.request_count += 1
            self.token_estimate += tokens
            try:
                try:
                    response = urlopen(request, timeout=REQUEST_TIMEOUT)
                except HTTPError as error:
                    response = error  # Archive HTTP error bodies as well.
                with response:
                    body_bytes = response.read()
                    received_at = self.now()
                    body = body_bytes.decode("utf-8")
                    status = response.status
            except (OSError, HTTPException) as error:
                failure = f"{type(error).__name__}: {error}"
                retryable = True
            else:
                self.save_response(request_url, params, status, requested_at, received_at, body)
                if status == 429:
                    self.rate_limited_count += 1
                if 200 <= status < 300:
                    return body
                failure = f"HTTP {status}"
                retryable = status in (408, 429) or 500 <= status < 600

            LOGGER.warning(
                "Request failed url=%s attempt=%d: %s", request_url, attempt + 1, failure
            )
            if not retryable or attempt + 1 == MAX_ATTEMPTS:
                raise RequestFailed(f"{request_url}: {failure}")
            backoff = min(30.0, 2.0**attempt)
            self.sleep(backoff + random.uniform(0.0, backoff / 4))
        raise AssertionError("Unreachable retry state")

    def refresh_series(self) -> None:
        body = self.request("/series")
        records = response_records(body, "series")
        matched = {
            required_string(series, "ticker") for series in records if is_daily_high_series(series)
        }
        LOGGER.info("Daily-high series refresh matched=%s", sorted(matched))
        if self.series_tickers is not None and matched != self.series_tickers:
            LOGGER.warning(
                "Daily-high series changed added=%s removed=%s",
                sorted(matched - self.series_tickers),
                sorted(self.series_tickers - matched),
            )
        if not matched:
            LOGGER.warning("No daily-high series matched; check discovery metadata")
        self.series_tickers = matched
        self.next_series_refresh = self.monotonic() + SERIES_REFRESH_INTERVAL

    def collect_markets(self, series_ticker: str, pending: list[str], seen: set[str]) -> int:
        """List a series' open markets, appending tickers not yet seen this cycle to pending."""
        params: Params = {
            "series_ticker": series_ticker,
            "status": "open",
            "limit": 1000,
        }
        cursors: set[str] = set()
        market_count = 0
        while True:
            body = self.request("/markets", params)
            records = response_records(body, "markets")
            market_count += len(records)
            for market in records:
                ticker = required_string(market, "ticker")
                if ticker not in seen:
                    seen.add(ticker)
                    pending.append(ticker)
            cursor = json.loads(body)["cursor"]
            if not isinstance(cursor, str):
                raise ValueError("Expected a string markets cursor")
            if not cursor:
                return market_count
            if cursor in cursors:
                raise ValueError("Markets pagination returned a repeated cursor")
            cursors.add(cursor)
            params = {**params, "cursor": cursor}

    def collect_orderbooks(self, tickers: list[str]) -> None:
        """Request each ticker's book once: in a batch, or singly if its chunk fails."""
        for start in range(0, len(tickers), ORDERBOOK_BATCH_SIZE):
            chunk = tickers[start : start + ORDERBOOK_BATCH_SIZE]
            try:
                body = self.request(
                    "/markets/orderbooks", {"tickers": chunk}, tokens=TOKEN_COST * len(chunk)
                )
                returned = [
                    required_string(book, "ticker") for book in response_records(body, "orderbooks")
                ]
                if sorted(returned) != sorted(chunk):
                    raise ValueError(
                        f"Batch returned missing={sorted(set(chunk) - set(returned))} "
                        f"extra={sorted(set(returned) - set(chunk))} count={len(returned)}"
                    )
            except (RequestFailed, ValueError, KeyError):
                LOGGER.exception(
                    "Order book batch failed size=%d first=%s; falling back per market",
                    len(chunk),
                    chunk[0],
                )
                for ticker in chunk:
                    try:
                        self.request(f"/markets/{quote(ticker, safe='')}/orderbook", {"depth": 0})
                    except (RequestFailed, ValueError):
                        LOGGER.exception("Order book failed market=%s; continuing", ticker)

    def run_cycle(self) -> None:
        started = self.monotonic()
        self.request_count = 0
        self.rate_limited_count = 0
        self.token_estimate = 0
        refreshed = False
        try:
            if started >= self.next_series_refresh:
                try:
                    self.refresh_series()
                    refreshed = True
                except (RequestFailed, ValueError, KeyError):
                    LOGGER.exception(
                        "Series refresh failed; retaining previously discovered series"
                    )
                    # Retry failed discovery on a later tick, not in a tight loop.
                    self.next_series_refresh = self.monotonic() + POLL_INTERVAL
            seen: set[str] = set()
            pending: list[str] = []
            market_counts: dict[str, int | None] = {}
            for series_ticker in sorted(self.series_tickers or ()):
                market_counts[series_ticker] = None
                try:
                    market_counts[series_ticker] = self.collect_markets(
                        series_ticker, pending, seen
                    )
                except (RequestFailed, ValueError, KeyError):
                    LOGGER.exception("Markets failed series=%s; continuing", series_ticker)
            self.collect_orderbooks(pending)
            if refreshed:
                for ticker, count in sorted(
                    market_counts.items(),
                    key=lambda item: (
                        -item[1] if item[1] is not None else math.inf,
                        item[0],
                    ),
                ):
                    LOGGER.info(
                        "Series open markets ticker=%s count=%s",
                        ticker,
                        count if count is not None else "unknown",
                    )
        finally:
            LOGGER.info(
                "Cycle duration=%.3fs request_count=%d rate_limited=%d token_estimate=%d",
                self.monotonic() - started,
                self.request_count,
                self.rate_limited_count,
                self.token_estimate,
            )

    def run_forever(self) -> None:
        next_tick = self.monotonic()
        while True:
            self.sleep(max(0.0, next_tick - self.monotonic()))
            self.run_cycle()
            next_tick += POLL_INTERVAL
            overdue = self.monotonic() - next_tick
            if overdue > 0:
                skipped = math.ceil(overdue / POLL_INTERVAL)
                next_tick += skipped * POLL_INTERVAL
                LOGGER.warning("Cycle overran schedule; skipped_ticks=%d", skipped)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    try:
        KalshiCollector().run_forever()
    except KeyboardInterrupt:
        LOGGER.info("Kalshi collector stopped")


if __name__ == "__main__":
    main()
