"""Raw iteration and transactional Parquet storage with point-in-time DuckDB reads.

Readers pin one manifest. Immutable shard generations are published before
its atomic replacement, so a query never combines half of a book with a new
header. Failed writes can leave unreferenced files, never visible rows.
"""

from __future__ import annotations

import base64
import csv
import fcntl
import gzip
import hashlib
import json
import os
import re
import tarfile
import uuid
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING

import google_crc32c
import zstandard

from kwb.schemas import (
    Envelope,
    MarketSnapshot,
    OrderBookHeader,
    OrderBookLevel,
    OrderBookSnapshot,
    Provenance,
)

if TYPE_CHECKING:
    import pyarrow as pa

STAMP = r"\d{4}-\d{2}-\d{2}T\d{2}"
LOOSE = re.compile(STAMP + r"\d{4}\.\d{6}Z\.json\.gz")
ARCHIVE = re.compile(STAMP + r"(?:\.late-[1-9]\d*)?\.tar\.zst")
MODELS = {
    "market_snapshots": MarketSnapshot,
    "orderbook_snapshots": OrderBookHeader,
    "orderbook_levels": OrderBookLevel,
}
NULLABLE = {
    "strike_type",
    "floor_strike",
    "cap_strike",
    "lower_bound_f",
    "upper_bound_f",
    "yes_bid_size_fp",
    "yes_ask_size_fp",
    "liquidity_dollars",
}
TIMESTAMPS = {
    "available_at",
    "requested_at",
    "created_time",
    "updated_time",
    "open_time",
    "close_time",
}
INTEGERS = {
    "floor_strike",
    "cap_strike",
    "lower_bound_f",
    "upper_bound_f",
    "yes_level_count",
    "no_level_count",
    "level_index",
}


class StoreError(ValueError):
    """Raw or stored data cannot satisfy the storage contract."""


def strict_json(value: str | bytes):
    def object_fields(pairs):
        record = {}
        for key, item in pairs:
            if key in record:
                raise StoreError(f"Duplicate JSON field {key!r}")
            record[key] = item
        return record

    def invalid_constant(value):
        raise StoreError(f"Nonfinite JSON number {value}")

    return json.loads(
        value, parse_float=Decimal, object_pairs_hook=object_fields, parse_constant=invalid_constant
    )


@cache
def arrow_schema(model: type[Provenance]) -> pa.Schema:
    import pyarrow as pa

    fields = []
    for name in model.model_fields:
        dtype = pa.string()
        if name in TIMESTAMPS:
            dtype = pa.timestamp("us", tz="UTC")
        elif name in {"available_date", "target_date"}:
            dtype = pa.date32()
        elif name in INTEGERS:
            dtype = pa.int32()
        elif name in {"lower_inclusive", "upper_inclusive"}:
            dtype = pa.bool_()
        elif name == "price_ranges":
            dtype = pa.list_(pa.struct([(key, pa.string()) for key in ("start", "end", "step")]))
        fields.append(pa.field(name, dtype, nullable=name in NULLABLE))
    return pa.schema(fields)


def configured_series(path: Path = Path("config/nws_stations.csv")) -> frozenset[str]:
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    series = [row["series_ticker"] for row in rows]
    if (
        not series
        or len(set(series)) != len(series)
        or any(re.fullmatch(r"KX[A-Z0-9]+", item) is None for item in series)
    ):
        raise StoreError(f"{path}: expected unique configured KX series")
    return frozenset(series)


def file_checksums(path: Path) -> tuple[int, str, str]:
    """Return size, GCS's base64 CRC32C, and SHA-256 without loading the file."""
    size = 0
    crc = google_crc32c.Checksum()
    sha = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            size += len(chunk)
            crc.update(chunk)
            sha.update(chunk)
    return size, base64.b64encode(crc.digest()).decode("ascii"), sha.hexdigest()


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temp.open("x", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        temp.unlink(missing_ok=True)


@dataclass(frozen=True)
class RawEnvelope:
    source_id: str
    sha256: str
    locator: str
    envelope: Envelope

    def provenance(self) -> dict:
        return {
            "source_id": self.source_id,
            "source_sha256": self.sha256,
            "requested_at": self.envelope.requested_at,
            "available_at": self.envelope.received_at,
            "available_date": self.envelope.received_at.date(),
        }


def raw_hours(paths: Iterable[Path]) -> dict[str, list[Path]]:
    hours = defaultdict(set)
    for path in paths:
        files = sorted(path.rglob("*")) if path.is_dir() else [path]
        for item in files:
            if item.is_dir():
                continue
            if item.name.startswith("."):
                continue  # Unpublished transport/compaction state is not raw history.
            if item.is_symlink() or not (
                LOOSE.fullmatch(item.name) or ARCHIVE.fullmatch(item.name)
            ):
                raise StoreError(f"{item}: unexpected raw pathname")
            if (
                re.fullmatch(r"\d{4}-\d{2}-\d{2}", item.parent.name)
                and item.parent.name != item.name[:10]
            ):
                raise StoreError(f"{item}: raw date directory disagrees with filename")
            hours[item.name[:13]].add(item)
    return {hour: sorted(files) for hour, files in sorted(hours.items())}


def _raw_members(path: Path) -> Iterator[tuple[str, bytes]]:
    if LOOSE.fullmatch(path.name):
        with gzip.open(path, "rb") as stream:
            yield path.name.removesuffix(".gz"), stream.read()
        return
    with (
        path.open("rb") as raw,
        zstandard.ZstdDecompressor().stream_reader(raw) as reader,
        tarfile.open(fileobj=reader, mode="r|") as archive,
    ):
        for info in archive:
            if (
                not info.isfile()
                or not LOOSE.fullmatch(info.name + ".gz")
                or info.name[:13] != path.name[:13]
            ):
                raise StoreError(f"{path}!{info.name}: unexpected archive member")
            with archive.extractfile(info) as member:
                yield info.name, member.read()
            archive.members.clear()
        while reader.read(64 * 1024):
            pass  # Drain the zstd frame, including its checksum.
    # stream_reader accepts a truncated frame at EOF. Check the explicit EOF
    # indicator separately so losing just the checksum/footer cannot look valid.
    decoder = zstandard.ZstdDecompressor().decompressobj()
    with path.open("rb") as stream:
        while chunk := stream.read(16 * 1024):
            if decoder.eof:
                raise StoreError(f"{path}: bytes after the zstd frame")
            decoder.decompress(chunk)
            if decoder.unused_data:
                raise StoreError(f"{path}: bytes after the zstd frame")
    if not decoder.eof:
        raise StoreError(f"{path}: truncated zstd frame")


def read_envelopes(paths: Iterable[Path]) -> tuple[list[RawEnvelope], int]:
    """Read one hour, deduplicating decompressed envelope identities, not bodies."""
    seen = {}
    duplicates = 0
    for path in paths:
        locator = str(path)
        try:
            for name, payload in _raw_members(path):
                locator = f"{path}!{name}" if ARCHIVE.fullmatch(path.name) else str(path)
                source_id = f"kalshi/{name[:10]}/{name}"
                digest = hashlib.sha256(payload).hexdigest()
                if source_id in seen:
                    if seen[source_id].sha256 != digest:
                        raise StoreError("Conflicting bytes for the same envelope")
                    duplicates += 1
                    continue
                envelope = Envelope.model_validate(strict_json(payload))
                filename_time = datetime.strptime(
                    name.removesuffix(".json"), "%Y-%m-%dT%H%M%S.%fZ"
                ).replace(tzinfo=UTC)
                if envelope.received_at != filename_time:
                    raise StoreError("received_at disagrees with the raw filename")
                if envelope.requested_at > envelope.received_at:
                    raise StoreError("requested_at is later than received_at")
                seen[source_id] = RawEnvelope(source_id, digest, locator, envelope)
        except (ValueError, OSError, EOFError, tarfile.TarError, zstandard.ZstdError) as exc:
            raise StoreError(f"{locator}: {exc}") from exc
    return sorted(seen.values(), key=lambda item: item.source_id), duplicates


def aware_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise StoreError("Query timestamps must be timezone-aware")
    return value.astimezone(UTC)


class Store:
    def __init__(self, root: Path = Path("data/kalshi")) -> None:
        self.root = Path(root)

    def _manifest(self) -> dict:
        path = self.root / "manifest.json"
        if not path.exists():
            return {"version": 1, "hours": {}}
        manifest = json.loads(path.read_text())
        if manifest.get("version") != 1 or set(manifest) != {"version", "hours"}:
            raise StoreError(f"{path}: unsupported manifest")
        return manifest

    def _files(self, manifest: dict, table: str, as_of: datetime) -> list[str]:
        return [
            str(self.root / item["files"][table])
            for hour, item in sorted(manifest["hours"].items())
            if hour[:10] <= as_of.date().isoformat()
        ]

    def _query(self, sql: str, params: list) -> list[dict]:
        import duckdb

        with duckdb.connect() as db:
            db.execute("SET TimeZone='UTC'")
            return db.sql(sql, params=params).to_arrow_table().to_pylist()

    def get_market_snapshots(self, series: str, as_of: datetime) -> list[MarketSnapshot]:
        as_of = aware_utc(as_of)
        files = self._files(self._manifest(), "market_snapshots", as_of)
        if not files:
            return []
        rows = self._query(
            """
            WITH visible AS (
                SELECT * FROM read_parquet(?, hive_partitioning=false)
                WHERE series_ticker = ? AND available_at <= ?
            )
            SELECT * FROM visible
            QUALIFY row_number() OVER (
                PARTITION BY ticker ORDER BY available_at DESC, source_id DESC
            ) = 1 ORDER BY ticker
        """,
            [files, series, as_of],
        )
        return [MarketSnapshot.model_validate(row) for row in rows]

    def get_market_history(
        self, series: str, start: datetime, as_of: datetime
    ) -> list[MarketSnapshot]:
        start, as_of = aware_utc(start), aware_utc(as_of)
        if start > as_of:
            raise StoreError("History start must not be after as_of")
        files = self._files(self._manifest(), "market_snapshots", as_of)
        if not files:
            return []
        rows = self._query(
            """
            SELECT * FROM read_parquet(?, hive_partitioning=false)
            WHERE series_ticker = ? AND available_at >= ? AND available_at <= ?
            ORDER BY available_at, ticker, source_id
        """,
            [files, series, start, as_of],
        )
        return [MarketSnapshot.model_validate(row) for row in rows]

    def get_orderbook(self, ticker: str, as_of: datetime) -> OrderBookSnapshot | None:
        as_of = aware_utc(as_of)
        manifest = self._manifest()  # Pin the same generation for the header and levels.
        files = self._files(manifest, "orderbook_snapshots", as_of)
        if not files:
            return None
        headers = self._query(
            """
            SELECT * FROM read_parquet(?, hive_partitioning=false)
            WHERE ticker = ? AND available_at <= ?
            ORDER BY available_at DESC, source_id DESC LIMIT 1
        """,
            [files, ticker, as_of],
        )
        if not headers:
            return None
        header = OrderBookHeader.model_validate(headers[0])
        rows = self._query(
            """
            SELECT * FROM read_parquet(?, hive_partitioning=false)
            WHERE ticker = ? AND source_id = ? AND available_at = ? AND available_at <= ?
            ORDER BY side, level_index
        """,
            [
                self._files(manifest, "orderbook_levels", as_of),
                ticker,
                header.source_id,
                header.available_at,
                as_of,
            ],
        )
        levels = [OrderBookLevel.model_validate(row) for row in rows]
        if (sum(row.side == "yes" for row in levels), sum(row.side == "no" for row in levels)) != (
            header.yes_level_count,
            header.no_level_count,
        ):
            raise StoreError(f"{header.source_id}: book header and levels disagree")
        return OrderBookSnapshot(header=header, levels=levels)

    def publish_hour(
        self, hour: str, rows: dict[str, list[Provenance]], sources: dict[str, str]
    ) -> None:
        """Merge reruns/supplements by source identity, then atomically publish all tables."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / ".write.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            manifest = self._manifest()
            previous = manifest["hours"].get(hour)
            previous_sources = (
                json.loads((self.root / previous["sources_file"]).read_text()) if previous else {}
            )
            combined_sources = dict(previous_sources)
            for source, digest in sources.items():
                if source in combined_sources and combined_sources[source] != digest:
                    raise StoreError(f"{source}: conflicting envelope on rerun")
                combined_sources[source] = digest
            tables = {}
            for name, model in MODELS.items():
                existing = (
                    []
                    if not previous
                    else pq.ParquetFile(self.root / previous["files"][name]).read().to_pylist()
                )
                # Recompute supplied sources, retaining older supplements not supplied this run.
                existing = [record for record in existing if record["source_id"] not in sources]
                merged = {}
                observations = {}
                for record in existing + [row.model_dump() for row in rows[name]]:
                    record = model.model_validate(record).model_dump()
                    if (
                        record["available_at"].strftime("%Y-%m-%dT%H") != hour
                        or record["available_date"] != record["available_at"].date()
                    ):
                        raise StoreError(f"{record['source_id']}: wrong availability partition")
                    if combined_sources.get(record["source_id"]) != record["source_sha256"]:
                        raise StoreError(f"{record['source_id']}: row has no matching envelope")
                    identity = (
                        record["source_id"],
                        record["ticker"],
                        record.get("side"),
                        record.get("level_index"),
                    )
                    if identity in merged and merged[identity] != record:
                        raise StoreError(f"{record['source_id']}: conflicting duplicate row")
                    observed = (
                        record["ticker"],
                        record["available_at"],
                        record.get("side"),
                        record.get("level_index"),
                    )
                    content = {k: v for k, v in record.items() if k not in Provenance.model_fields}
                    if observed in observations and observations[observed] != content:
                        raise StoreError(
                            f"{record['source_id']}: conflicting simultaneous observations"
                        )
                    observations[observed] = content
                    merged[identity] = record
                sort = (
                    (
                        lambda row: (
                            row["series_ticker"],
                            row["ticker"],
                            row["available_at"],
                            row["source_id"],
                        )
                    )
                    if name == "market_snapshots"
                    else (
                        lambda row: (
                            row["ticker"],
                            row["available_at"],
                            row["source_id"],
                            row.get("side", ""),
                            row.get("level_index", 0),
                        )
                    )
                )
                tables[name] = pa.Table.from_pylist(
                    sorted(merged.values(), key=sort), schema=arrow_schema(model)
                )
            if previous and combined_sources == previous_sources:
                # Rows were still checked above: source hashes alone never mask a parser change.
                unchanged = all(
                    tables[name].equals(pq.ParquetFile(self.root / previous["files"][name]).read())
                    for name in MODELS
                )
                if unchanged:
                    return
            files = {}
            generation = uuid.uuid4().hex
            source_file = Path("generations") / generation / "sources.json"
            atomic_json(self.root / source_file, combined_sources)
            for name, table in tables.items():
                relative = (
                    Path("generations")
                    / generation
                    / name
                    / f"available_date={hour[:10]}"
                    / f"{hour}.parquet"
                )
                path = self.root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(
                    table,
                    path,
                    compression="zstd",
                    compression_level=3,
                    row_group_size=131072,
                    version="2.6",
                )
                with path.open("rb") as stream:
                    os.fsync(stream.fileno())
                if not pq.ParquetFile(path).read().equals(table):
                    raise StoreError(f"{path}: Parquet verification failed")
                directory = path.parent
                while directory != self.root:
                    fd = os.open(directory, os.O_RDONLY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                    directory = directory.parent
                files[name] = str(relative)
            manifest["hours"][hour] = {"sources_file": str(source_file), "files": files}
            atomic_json(self.root / "manifest.json", manifest)
