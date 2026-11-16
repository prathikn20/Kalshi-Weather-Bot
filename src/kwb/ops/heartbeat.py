"""Check recent loose Kalshi envelopes and report to an external dead-man's switch.

Only the standard library is imported: maintenance must not load the analytical
store or compete with collection. No exception text or remote response is logged.
"""

import argparse
import gzip
import json
import logging
import math
import os
import re
import time
import zlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

LOGGER = logging.getLogger(__name__)
WINDOW_SECONDS = 300
HTTP_TIMEOUT_SECONDS = 5
CHECK_BUDGET_SECONDS = 10
MAX_DIRECTORY_ENTRIES = 250_000
MAX_ENVELOPES = 1024
MAX_ENVELOPE_BYTES = 16 * 1024 * 1024
MAX_REASON_BYTES = 512
PING_URL_VARIABLE = "KWB_HEARTBEAT_PING_URL"
STAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{6}\.\d{6}Z\.json\.gz")
UTC_STAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)")
PING_URL = re.compile(
    r"https://hc-ping\.com/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)
FIXED_POINT = re.compile(r"\d+(?:\.\d+)?")


class CheckError(ValueError):
    """A fixed diagnostic code, safe to send and log without exception details."""


class InvalidBody(CheckError):
    """A response cannot supply evidence, but other fresh responses may."""


class UnreadableEnvelope(CheckError):
    """A gzip read failed, possibly because collection is still writing it."""


@dataclass
class CheckStats:
    """Per-run counts survive inspection failures without keeping raw payloads."""

    skipped_oversized: int = 0
    skipped_invalid_body: int = 0
    skipped_unreadable: int = 0

    def log_fields(self) -> str:
        return (
            f"skipped_oversized={self.skipped_oversized} "
            f"skipped_invalid_body={self.skipped_invalid_body} "
            f"skipped_unreadable={self.skipped_unreadable}"
        )


def filename(stamp: datetime) -> str:
    return stamp.strftime("%Y-%m-%dT%H%M%S.%fZ.json.gz")


def timestamp(value) -> datetime:
    if not isinstance(value, str) or UTC_STAMP.fullmatch(value) is None:
        raise CheckError("invalid_timestamp")
    return datetime.fromisoformat(value).astimezone(UTC)


def strict_json(value):
    def fields(pairs):
        result = {}
        for key, item in pairs:
            if key in result:
                raise CheckError("duplicate_json_field")
            result[key] = item
        return result

    def invalid_constant(value):
        raise CheckError("nonfinite_json_number")

    return json.loads(value, object_pairs_hook=fields, parse_constant=invalid_constant)


def check_budget(deadline: float) -> None:
    if time.monotonic() >= deadline:
        raise CheckError("check_budget_exhausted")


def newest_candidates(
    raw_root: Path, as_of: datetime, window: float, deadline: float
) -> list[Path]:
    """List only window dates, never the root's historical directory inventory."""
    if not raw_root.is_dir() or raw_root.is_symlink():
        raise CheckError("missing_raw_directory")
    cutoff = as_of - timedelta(seconds=window)
    dates = {cutoff.date(), as_of.date()}
    lower, upper = filename(cutoff), filename(as_of)
    candidates = []
    entries = 0
    for day in sorted(dates, reverse=True):
        directory = raw_root / day.isoformat()
        if directory.is_symlink():
            raise CheckError("unexpected_directory_symlink")
        try:
            paths = os.scandir(directory)
        except FileNotFoundError:
            continue
        with paths:
            for entry in paths:
                entries += 1
                check_budget(deadline)
                if entries > MAX_DIRECTORY_ENTRIES:
                    raise CheckError("directory_budget_exhausted")
                # Old filenames and archives require neither stat nor decompression.
                if not lower <= entry.name <= upper:
                    continue
                if STAMP.fullmatch(entry.name) is None:
                    if entry.name.endswith(".json.gz"):
                        raise CheckError("malformed_filename")
                    continue
                if not entry.is_file(follow_symlinks=False):
                    raise CheckError("unexpected_envelope_type")
                candidates.append(Path(entry.path))
                if len(candidates) > MAX_ENVELOPES:
                    raise CheckError("envelope_budget_exhausted")
    return sorted(candidates, reverse=True)


def read_envelope(path: Path) -> dict | None:
    try:
        if path.stat().st_size == 0:
            raise UnreadableEnvelope("empty_gzip")
        with gzip.open(path, "rb") as stream:
            payload = stream.read(MAX_ENVELOPE_BYTES + 1)
    except (OSError, EOFError, zlib.error):
        # Only stream I/O is skippable. Parsing the envelope itself stays strict.
        raise UnreadableEnvelope("unreadable_gzip") from None
    if len(payload) > MAX_ENVELOPE_BYTES:
        # An unread envelope cannot supply evidence. Keep looking within the
        # existing window instead of letting large discovery responses mask it.
        return None
    envelope = strict_json(payload)
    required = {
        "request_url",
        "request_params",
        "http_status",
        "requested_at",
        "received_at",
        "body",
    }
    if (
        not isinstance(envelope, dict)
        or not required <= envelope.keys()
        or envelope.keys() - required - {"body_encoding"}
        or not isinstance(envelope["request_url"], str)
        or not isinstance(envelope["request_params"], dict)
        or type(envelope["http_status"]) is not int
        or not 100 <= envelope["http_status"] <= 599
        or not isinstance(envelope["body"], str)
        or envelope.get("body_encoding") not in (None, "base64")
    ):
        raise CheckError("malformed_envelope")
    received = timestamp(envelope["received_at"])
    if filename(received) != path.name or timestamp(envelope["requested_at"]) > received:
        raise CheckError("inconsistent_envelope_timestamp")
    return envelope


def valid_book(book) -> bool:
    """Validate the served fixed-point shape; empty liquidity is a valid book."""
    if not isinstance(book, dict) or set(book) != {"yes_dollars", "no_dollars"}:
        return False
    for side in ("yes_dollars", "no_dollars"):
        if not isinstance(book[side], list):
            return False
        for level in book[side]:
            if (
                not isinstance(level, list)
                or len(level) != 2
                or any(not isinstance(v, str) or not FIXED_POINT.fullmatch(v) for v in level)
                or Decimal(level[0]) > 1
                or Decimal(level[1]) <= 0
            ):
                return False
    return True


def qualifying_endpoint(envelope: dict) -> str | None:
    url = urlsplit(envelope["request_url"])
    if url.scheme != "https" or url.netloc != "external-api.kalshi.com":
        raise CheckError("unexpected_envelope_origin")
    path = url.path
    markets = path == "/trade-api/v2/markets"
    batch = path == "/trade-api/v2/markets/orderbooks"
    single = re.fullmatch(r"/trade-api/v2/markets/[^/]+/orderbook", path) is not None
    if not (markets or batch or single) or not 200 <= envelope["http_status"] < 300:
        return None
    if envelope.get("body_encoding") is not None:
        raise InvalidBody("unreadable_response_body")
    try:
        body = strict_json(envelope["body"])
    except (ValueError, RecursionError):
        raise InvalidBody("invalid_response_json") from None
    if not isinstance(body, dict):
        raise InvalidBody("malformed_response_body")
    if markets:
        records = body.get("markets")
        if not isinstance(records, list):
            raise InvalidBody("invalid_markets_list")
        if not records:
            raise InvalidBody("empty_response_list")
        if any(
            not isinstance(r, dict) or not isinstance(r.get("ticker"), str) or not r["ticker"]
            for r in records
        ):
            raise InvalidBody("malformed_market_record")
        return "markets"
    if batch:
        records = body.get("orderbooks")
        if not isinstance(records, list):
            raise InvalidBody("invalid_orderbooks_list")
        if not records:
            raise InvalidBody("empty_response_list")
        if any(
            not isinstance(r, dict)
            or not isinstance(r.get("ticker"), str)
            or not r["ticker"]
            or not valid_book(r.get("orderbook_fp"))
            for r in records
        ):
            raise InvalidBody("malformed_orderbook_record")
    elif not valid_book(body.get("orderbook_fp")):
        raise InvalidBody("malformed_orderbook")
    return "orderbooks"


def inspect_collection(
    raw_root: Path,
    as_of: datetime,
    window: float = WINDOW_SECONDS,
    stats: CheckStats | None = None,
) -> None:
    stats = stats if stats is not None else CheckStats()
    if as_of.tzinfo is None or as_of.utcoffset() != timedelta(0):
        raise CheckError("check_time_must_be_utc")
    if not math.isfinite(window) or not 0 < window <= WINDOW_SECONDS:
        raise CheckError("invalid_freshness_window")
    deadline = time.monotonic() + CHECK_BUDGET_SECONDS
    candidates = newest_candidates(raw_root, as_of, window, deadline)
    found = set()
    for path in candidates:
        check_budget(deadline)
        try:
            envelope = read_envelope(path)
        except UnreadableEnvelope:
            stats.skipped_unreadable += 1
            check_budget(deadline)
            continue
        if envelope is None:
            stats.skipped_oversized += 1
            check_budget(deadline)
            continue
        age = (as_of - timestamp(envelope["received_at"])).total_seconds()
        if not 0 <= age <= window:
            raise CheckError("envelope_outside_freshness_window")
        try:
            endpoint = qualifying_endpoint(envelope)
        except InvalidBody:
            stats.skipped_invalid_body += 1
            check_budget(deadline)
            continue
        if endpoint:
            found.add(endpoint)
        check_budget(deadline)
        if found == {"markets", "orderbooks"}:
            return
    raise CheckError("missing_fresh_market_or_orderbook_evidence")


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def send_ping(url: str, reason: str | None) -> None:
    body = b"" if reason is None else reason.encode("utf-8")[:MAX_REASON_BYTES]
    request = Request(
        url if reason is None else url + "/fail",
        data=body,
        headers={"Content-Type": "text/plain; charset=utf-8", "User-Agent": "kwb-heartbeat/0.1"},
        method="POST",
    )
    try:
        with build_opener(NoRedirects()).open(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            if response.status != 200 or response.read(64).strip() != b"OK":
                raise CheckError("ping_not_acknowledged")
    except Exception:
        raise CheckError("ping_transport_failed") from None


def run_once(raw_root: Path, as_of: datetime, window: float = WINDOW_SECONDS) -> int:
    stats = CheckStats()
    try:
        url = os.environ[PING_URL_VARIABLE]
        if PING_URL.fullmatch(url) is None:
            raise CheckError("invalid_ping_configuration")
    except Exception:
        LOGGER.error(
            "Heartbeat configuration unavailable or invalid %s",
            stats.log_fields(),
        )
        return 1
    reason = None
    try:
        inspect_collection(raw_root, as_of, window, stats)
    except CheckError as error:
        # Only literal codes generated here may cross the secret-bearing boundary.
        safe_codes = {
            "invalid_timestamp",
            "duplicate_json_field",
            "nonfinite_json_number",
            "check_budget_exhausted",
            "missing_raw_directory",
            "unexpected_directory_symlink",
            "directory_budget_exhausted",
            "malformed_filename",
            "unexpected_envelope_type",
            "envelope_budget_exhausted",
            "malformed_envelope",
            "inconsistent_envelope_timestamp",
            "unexpected_envelope_origin",
            "unreadable_response_body",
            "malformed_response_body",
            "invalid_markets_list",
            "malformed_market_record",
            "invalid_orderbooks_list",
            "malformed_orderbook_record",
            "malformed_orderbook",
            "check_time_must_be_utc",
            "invalid_freshness_window",
            "envelope_outside_freshness_window",
            "missing_fresh_market_or_orderbook_evidence",
        }
        reason = str(error) if str(error) in safe_codes else "collection_check_exception"
    except (OSError, EOFError, ValueError):
        reason = "unreadable_or_malformed_evidence"
    except Exception:
        reason = "collection_check_exception"
    try:
        send_ping(url, reason)
    except Exception:
        LOGGER.error("Heartbeat ping failed %s", stats.log_fields())
        return 1
    if reason is not None:
        LOGGER.error("Heartbeat reported failure: %s %s", reason, stats.log_fields())
        return 1
    LOGGER.info("Heartbeat reported success %s", stats.log_fields())
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-root", type=Path, default=Path("raw/kalshi"))
    parser.add_argument("--window-seconds", type=float, default=WINDOW_SECONDS)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    return run_once(args.raw_root, datetime.now(UTC), args.window_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
