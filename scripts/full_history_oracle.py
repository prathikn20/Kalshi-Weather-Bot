"""Independent audit reconstruction from archived bytes; no kwb imports allowed.

This is verification tooling, never a model input. SQLite is a disposable index
of raw observations, not a second production store. All monetary strings survive
unchanged. No production parser, schemas, raw iterator, or query code is reused.
"""

import base64
import csv
import gzip
import hashlib
import json
import re
import sqlite3
import tarfile
import zlib
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import unquote, urlsplit

import zstandard

SEED = 20261116
MARKET_FIELDS = [
    "ticker",
    "event_ticker",
    "market_type",
    "status",
    "strike_type",
    "floor_strike",
    "cap_strike",
    "rules_primary",
    "rules_secondary",
    "created_time",
    "updated_time",
    "open_time",
    "close_time",
    "yes_bid_dollars",
    "yes_ask_dollars",
    "no_bid_dollars",
    "no_ask_dollars",
    "last_price_dollars",
    "previous_yes_bid_dollars",
    "previous_yes_ask_dollars",
    "previous_price_dollars",
    "notional_value_dollars",
    "volume_fp",
    "volume_24h_fp",
    "open_interest_fp",
    "yes_bid_size_fp",
    "yes_ask_size_fp",
    "liquidity_dollars",
    "price_level_structure",
    "price_ranges",
]
TIME_FIELDS = {
    "created_time",
    "updated_time",
    "open_time",
    "close_time",
    "available_at",
    "requested_at",
}
LOOSE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{6}\.\d{6}Z\.json\.gz")
ARCHIVE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}(?:\.late-[1-9]\d*)?\.tar\.zst")


def timestamp(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError(f"Expected aware UTC timestamp: {value!r}")
    return result.astimezone(UTC)


def stamp(value):
    return timestamp(value).isoformat(timespec="microseconds")


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def decode_body(value):
    return json.loads(zlib.decompress(value) if isinstance(value, bytes) else value)


def strict_json(payload):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"Duplicate JSON key {key}")
            result[key] = value
        return result

    def invalid(value):
        raise ValueError(f"Nonfinite JSON number {value}")

    return json.loads(payload, object_pairs_hook=pairs, parse_constant=invalid)


def members(path):
    if LOOSE.fullmatch(path.name):
        yield path.name[:-3], gzip.decompress(path.read_bytes())
    elif ARCHIVE.fullmatch(path.name):
        # tar streaming retains at most one envelope, never extracts paths.
        with (
            path.open("rb") as source,
            zstandard.ZstdDecompressor().stream_reader(source) as stream,
        ):
            with tarfile.open(fileobj=stream, mode="r|") as archive:
                for info in archive:
                    if not info.isfile() or not LOOSE.fullmatch(info.name + ".gz"):
                        raise ValueError(f"{path}: invalid member {info.name}")
                    if info.name[:13] != path.name[:13]:
                        raise ValueError(f"{path}: member is in another hour")
                    with archive.extractfile(info) as item:
                        yield info.name, item.read()
                    archive.members.clear()
            while stream.read(65536):
                pass
        # zstd's streaming reader alone accepts truncated frames at EOF.
        decoder = zstandard.ZstdDecompressor().decompressobj()
        with path.open("rb") as source:
            while chunk := source.read(16384):
                if decoder.eof:
                    raise ValueError(f"{path}: trailing bytes")
                decoder.decompress(chunk)
                if decoder.unused_data:
                    raise ValueError(f"{path}: trailing bytes")
        if not decoder.eof:
            raise ValueError(f"{path}: truncated frame")
    else:
        raise ValueError(f"Unexpected raw file {path}")


def raw_inputs(root):
    hours = defaultdict(list)
    for path in sorted(Path(root).rglob("*")):
        if path.is_dir() or path.name.startswith("."):
            continue
        if path.is_symlink() or not (LOOSE.fullmatch(path.name) or ARCHIVE.fullmatch(path.name)):
            raise ValueError(f"Unexpected raw path {path}")
        if path.parent.name != path.name[:10]:
            raise ValueError(f"Wrong raw date directory {path}")
        hours[path.name[:13]].append(path)
    return dict(sorted(hours.items()))


def envelopes(paths):
    seen = {}
    for path in paths:
        for name, payload in members(path):
            identity = f"kalshi/{name[:10]}/{name}"
            digest = hashlib.sha256(payload).hexdigest()
            if identity in seen:
                if seen[identity] != digest:
                    raise ValueError(f"{path}!{name}: conflicting duplicate")
                yield identity, digest, None
                continue
            seen[identity] = digest
            env = strict_json(payload)
            at = datetime.strptime(name[:-5], "%Y-%m-%dT%H%M%S.%fZ").replace(tzinfo=UTC)
            if timestamp(env["received_at"]) != at:
                raise ValueError(f"{path}!{name}: filename disagrees with received_at")
            if timestamp(env["requested_at"]) > at:
                raise ValueError(f"{path}!{name}: reversed envelope times")
            yield identity, digest, env


def provenance(identity, digest, env):
    return {
        "source_id": identity,
        "source_sha256": digest,
        "available_at": stamp(env["received_at"]),
        "requested_at": stamp(env["requested_at"]),
    }


def market_projection(item, origin):
    result = {key: item.get(key) for key in MARKET_FIELDS}
    for key in TIME_FIELDS & result.keys():
        result[key] = stamp(result[key])
    return {**result, **origin}


def book_projection(ticker, book, origin):
    levels = []
    for side in ("no", "yes"):
        for index, pair in enumerate(book[side + "_dollars"]):
            if (
                not isinstance(pair, list)
                or len(pair) != 2
                or not all(isinstance(value, str) for value in pair)
            ):
                raise ValueError("Book price and quantity must be served strings")
            levels.append(
                {
                    "ticker": ticker,
                    "side": side,
                    "level_index": index,
                    "price_dollars": pair[0],
                    "quantity_fp": pair[1],
                    **origin,
                }
            )
    header = {
        "ticker": ticker,
        "yes_level_count": len(book["yes_dollars"]),
        "no_level_count": len(book["no_dollars"]),
        **origin,
    }
    return {"header": header, "levels": levels}


def connect(path, *, readonly=False):
    if readonly:
        return sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    db = sqlite3.connect(path)
    db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS envelopes(
          id TEXT PRIMARY KEY, sha TEXT, at TEXT, kind TEXT, status INTEGER,
          configured_items INTEGER, explained_empty INTEGER);
        CREATE TABLE IF NOT EXISTS observations(
          kind TEXT, id TEXT, ticker TEXT, series TEXT, at TEXT, body BLOB, levels INTEGER,
          PRIMARY KEY(kind,id,ticker));
        CREATE INDEX IF NOT EXISTS lookup ON observations(kind,series,ticker,at DESC,id DESC);
        CREATE TABLE IF NOT EXISTS evidence(
          id TEXT, series TEXT, kind TEXT, at TEXT, PRIMARY KEY(id,series,kind));
        CREATE INDEX IF NOT EXISTS evidence_times ON evidence(series,kind,at);
    """)
    return db


def build_index(root, database, configured):
    if Path(database).exists():
        raise ValueError("Oracle index already exists; use a fresh output path")
    db = connect(database)
    duplicates = 0
    counts = defaultdict(int)
    for hour, paths in raw_inputs(root).items():
        for identity, digest, env in envelopes(paths):
            if env is None:
                duplicates += 1
                continue
            status = env["http_status"]
            at = stamp(env["received_at"])
            kind = "other"
            total = empty = 0
            observed = set()
            rows = []
            if 200 <= status < 300:
                url = urlsplit(env["request_url"])
                if url.netloc not in {"external-api.kalshi.com", "api.elections.kalshi.com"}:
                    raise ValueError(f"{identity}: unexpected origin")
                route = url.path.removeprefix("/trade-api/v2")
                params = env["request_params"]
                if (
                    route == "/markets"
                    or route == "/markets/orderbooks"
                    or route.endswith("/orderbook")
                ):
                    body = strict_json(
                        base64.b64decode(env["body"], validate=True)
                        if env.get("body_encoding")
                        else env["body"]
                    )
                    origin = provenance(identity, digest, env)
                    if route == "/markets":
                        kind = "markets"
                        series = params["series_ticker"]
                        if series in configured:
                            observed.add(series)
                            empty = int(not body["markets"])
                            for item in body["markets"]:
                                if item["event_ticker"].split("-")[0] != series:
                                    raise ValueError(f"{identity}: wrong market series")
                                rows.append(
                                    (
                                        "markets",
                                        item["ticker"],
                                        series,
                                        market_projection(item, origin),
                                    )
                                )
                    else:
                        kind = "books"
                        if route == "/markets/orderbooks":
                            books = body["orderbooks"]
                            requested = params["tickers"]
                            returned = [item["ticker"] for item in books]
                            if len(returned) != len(set(returned)) or sorted(returned) != sorted(
                                requested
                            ):
                                raise ValueError(f"{identity}: incomplete batch")
                        else:
                            ticker = unquote(route.split("/")[2])
                            books = [{"ticker": ticker, "orderbook_fp": body["orderbook_fp"]}]
                        for item in books:
                            ticker = item["ticker"]
                            series = ticker.split("-")[0]
                            if series in configured:
                                observed.add(series)
                                projection = book_projection(ticker, item["orderbook_fp"], origin)
                                empty += int(not projection["levels"])
                                rows.append(("books", ticker, series, projection))
            total = len(rows)
            db.execute(
                "INSERT INTO envelopes VALUES (?,?,?,?,?,?,?)",
                (identity, digest, at, kind, status, total, empty),
            )
            for row_kind, ticker, series, projection in rows:
                db.execute(
                    "INSERT INTO observations VALUES (?,?,?,?,?,?,?)",
                    (
                        row_kind,
                        identity,
                        ticker,
                        series,
                        at,
                        zlib.compress(encode(projection).encode(), level=1),
                        len(projection["levels"]) if row_kind == "books" else 0,
                    ),
                )
            for series in observed:
                db.execute("INSERT INTO evidence VALUES (?,?,?,?)", (identity, series, kind, at))
            counts[kind] += 1
        db.commit()
        print(encode({"indexed_hour": hour, "envelopes": sum(counts.values())}), flush=True)
    db.close()
    return {"envelopes": sum(counts.values()), "duplicates": duplicates, "kinds": dict(counts)}


def expected(db, kind, series, as_of):
    # Index-only ticker discovery followed by one bounded raw-index lookup per
    # ticker avoids loading every historical body for each sampled moment.
    tickers = db.execute(
        "SELECT DISTINCT ticker FROM observations WHERE kind=? AND series=?", (kind, series)
    ).fetchall()
    result = {}
    for (ticker,) in tickers:
        row = db.execute(
            "SELECT body FROM observations WHERE kind=? AND series=? AND ticker=? AND at<=? "
            "ORDER BY at DESC,id DESC LIMIT 1",
            (kind, series, ticker, stamp(as_of)),
        ).fetchone()
        if row is not None:
            result[ticker] = decode_body(row[0])
    if kind == "markets":
        result = {
            ticker: row for ticker, row in result.items() if timestamp(row["close_time"]) > as_of
        }
    return result


def contiguous_ranges(values, step=timedelta(minutes=1)):
    result = []
    for value in sorted(set(values)):
        if result and result[-1]["end_exclusive"] == value:
            result[-1]["end_exclusive"] = value + step
            result[-1]["minutes"] += int(step.total_seconds() / 60)
        else:
            result.append(
                {
                    "start": value,
                    "end_exclusive": value + step,
                    "minutes": int(step.total_seconds() / 60),
                }
            )
    return [
        {k: v.isoformat() if isinstance(v, datetime) else v for k, v in row.items()}
        for row in result
    ]


def coverage(database, configured, raw_root, bucket_objects, output):
    db = connect(database, readonly=True)
    first, last = db.execute("SELECT min(at),max(at) FROM envelopes").fetchone()
    first, last = timestamp(first), timestamp(last)
    evidence = defaultdict(list)
    minutes = defaultdict(lambda: defaultdict(int))
    for series, kind, at in db.execute("SELECT series,kind,at FROM evidence ORDER BY at"):
        at = timestamp(at)
        evidence[(series, kind)].append(at)
        minutes[(at.replace(second=0, microsecond=0), series)][kind] += 1
    gaps = []
    spans = {}
    for series in configured:
        spans[series] = {}
        for kind in ("markets", "books"):
            times = evidence[(series, kind)]
            spans[series][kind] = {
                "first": times[0].isoformat() if times else None,
                "last": times[-1].isoformat() if times else None,
            }
            edges = [first, *times, last]
            for start, end in zip(edges, edges[1:], strict=False):
                if (end - start).total_seconds() > 120:
                    gaps.append(
                        {
                            "series": series,
                            "feed": kind,
                            "start": start.isoformat(),
                            "end": end.isoformat(),
                            "duration_seconds": (end - start).total_seconds(),
                        }
                    )
    absent = defaultdict(list)
    minute = first.replace(second=0, microsecond=0)
    with (output / "minutes.csv").open("w") as stream:
        writer = csv.writer(stream)
        writer.writerow(["minute_utc", "series", "markets_2xx", "orderbooks_2xx"])
        while minute <= last:
            for series in configured:
                counts = minutes[(minute, series)]
                writer.writerow([minute.isoformat(), series, counts["markets"], counts["books"]])
                if minute >= datetime(2026, 10, 7, 17, tzinfo=UTC):
                    for kind in ("markets", "books"):
                        if not counts[kind] and any(
                            minutes[(minute, other)][kind] for other in configured
                        ):
                            absent[(series, kind)].append(minute)
            minute += timedelta(minutes=1)
    bucket_hours = {Path(obj).name[:13] for obj in bucket_objects}
    inputs = raw_inputs(raw_root)
    missing_hours = []
    with (output / "hours.csv").open("w") as stream:
        writer = csv.writer(stream)
        writer.writerow(["hour_utc", "source", "archive_count", "loose_count", "no_bucket_archive"])
        hour = first.replace(minute=0, second=0, microsecond=0)
        while hour <= last:
            key = hour.strftime("%Y-%m-%dT%H")
            if key not in bucket_hours:
                missing_hours.append(hour)
            paths = inputs.get(key, [])
            loose = sum(p.name.endswith(".gz") for p in paths)
            source = (
                ("both" if loose else "bucket")
                if key in bucket_hours
                else ("Mac-local only" if paths else "missing")
            )
            writer.writerow(
                [
                    key,
                    source,
                    sum(p.name.endswith(".zst") for p in paths),
                    loose,
                    key not in bucket_hours,
                ]
            )
            hour += timedelta(hours=1)
    preflight = output / "preflight.json"
    journal = (
        json.loads(preflight.read_text())["missing_bucket_ranges"] if preflight.exists() else []
    )
    missing_ranges = []
    for entry in contiguous_ranges(missing_hours, timedelta(hours=1)):
        note = next(
            (
                item["note"]
                for item in journal
                if timestamp(item["start"]) == timestamp(entry["start"])
                and timestamp(item["end_exclusive"]) == timestamp(entry["end_exclusive"])
            ),
            "",
        )
        missing_ranges.append(
            {
                "start": entry["start"],
                "end_exclusive": entry["end_exclusive"],
                "hours": entry["minutes"] // 60,
                "evidence": note,
            }
        )
    with (output / "missing-bucket-ranges.csv").open("w") as stream:
        writer = csv.DictWriter(stream, fieldnames=["start", "end_exclusive", "hours", "evidence"])
        writer.writeheader()
        writer.writerows(missing_ranges)
    for series in configured:
        for kind in ("markets", "books"):
            spans[series][kind]["absent_while_others_present"] = contiguous_ranges(
                absent[(series, kind)]
            )
        days = {at.date().isoformat() for at in evidence[(series, "markets")]}
        spans[series]["utc_dates_with_markets"] = sorted(days)
        spans[series]["utc_days_with_markets"] = len(days)
        labels = db.execute(
            "SELECT DISTINCT substr(ticker,instr(ticker,'-')+1,7) "
            "FROM observations WHERE kind='markets' AND series=?",
            (series,),
        )
        spans[series]["target_dates_observed"] = sorted(
            datetime.strptime(label, "%y%b%d").replace(tzinfo=UTC).date().isoformat()
            for (label,) in labels
        )
    result = {
        "first": first.isoformat(),
        "last": last.isoformat(),
        "gaps": gaps,
        "series": spans,
        "missing_bucket_ranges": missing_ranges,
    }
    (output / "coverage.json").write_text(json.dumps(result, indent=2) + "\n")
    with (output / "gaps.csv").open("w") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=["series", "feed", "start", "end", "duration_seconds"]
        )
        writer.writeheader()
        writer.writerows(gaps)
    db.close()
    return result
