"""Compact completed UTC hours into immutable tar.zst archives.

The wall-clock budget gates starting hours, not finishing an hour. Every payload
and verification pass is streamed; SQLite keeps inventories and hashes off heap.
Late arrivals go into numbered immutable supplements. A single advisory lock
covers discovery, stale-temp cleanup, publication, and deletion.
"""

import argparse
import fcntl
import gzip
import hashlib
import logging
import math
import os
import re
import sqlite3
import tarfile
import time as clock
from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import zstandard

LOGGER = logging.getLogger(__name__)
GRACE = timedelta(minutes=5)
ZSTD_LEVEL = 9  # Measured on the quota-limited VM; see benchmarks/bounded-compaction-review.md.
TIME_BUDGET_SECONDS = 120.0
CHUNK_SIZE = 64 * 1024
LOCK_NAME = ".compact.lock"
INDEX_NAME = ".compact-index.sqlite"
STAMP = r"(\d{4}-\d{2}-\d{2})T(\d{2})"
ORIGINAL = re.compile(STAMP + r"\d{4}\.\d{6}Z\.json\.gz")
ARCHIVE = re.compile(STAMP + r"(?:\.late-([1-9]\d*))?\.tar\.zst")
TEMP = re.compile(r"\." + ARCHIVE.pattern + r"\.tmp")


class CompactionError(Exception):
    """An archive failed verification or the raw tree holds unexpected data."""


def hour_start(day: str, hour: str) -> datetime:
    return datetime.combine(date.fromisoformat(day), time(int(hour)), tzinfo=UTC)


def compression_parameters(level: int, long_distance: bool):
    # LDM explicitly uses a 128 MiB window, rather than an implicit library default.
    overrides = {"window_log": 27} if long_distance else {}
    return zstandard.ZstdCompressionParameters.from_level(
        level, enable_ldm=int(long_distance), threads=0, write_checksum=1, **overrides
    )


def digest_stream(stream) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    while chunk := stream.read(CHUNK_SIZE):
        size += len(chunk)
        digest.update(chunk)
    return size, digest.hexdigest()


def read_archive(path: Path) -> Iterator[tuple[str, int, str]]:
    """Yield hashes, never payloads. Drain the frame to check its checksum."""
    with (
        path.open("rb") as raw,
        zstandard.ZstdDecompressor().stream_reader(raw) as reader,
        tarfile.open(fileobj=reader, mode="r|") as tar,
    ):
        for info in tar:
            match = ORIGINAL.fullmatch(info.name + ".gz")
            if not info.isfile() or match is None or info.name[:13] != path.name.lstrip(".")[:13]:
                raise CompactionError(f"{path}: unexpected member {info.name!r}")
            extracted = tar.extractfile(info)
            assert extracted is not None
            with extracted:
                size, digest = digest_stream(extracted)
            if size != info.size:
                raise CompactionError(f"{path}: truncated member {info.name}")
            # Python 3.12 caches TarInfo even in stream mode.
            tar.members.clear()
            yield info.name, size, digest
        while reader.read(CHUNK_SIZE):
            pass


def fsync_dir(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_member(tar: tarfile.TarFile, original: Path, size: int) -> None:
    info = tarfile.TarInfo(original.name.removesuffix(".gz"))
    info.size = size
    info.mode = 0o644
    info.mtime = 0
    with gzip.open(original, "rb") as stream:
        tar.addfile(info, stream)
    tar.members.clear()


def build_archive(
    temp: Path, db: sqlite3.Connection, archive: Path, level: int, long_distance: bool
) -> None:
    compressor = zstandard.ZstdCompressor(
        compression_params=compression_parameters(level, long_distance)
    )
    with temp.open("xb") as output:
        with (
            compressor.stream_writer(output, closefd=False) as writer,
            tarfile.open(fileobj=writer, mode="w|", format=tarfile.PAX_FORMAT) as tar,
        ):
            for original, size in db.execute(
                "SELECT original, size FROM expected WHERE archive=? ORDER BY name", (str(archive),)
            ):
                write_member(tar, Path(original), size)
        output.flush()
        os.fsync(output.fileno())


def verify_archive(path: Path, db: sqlite3.Connection, archive: Path | None = None) -> None:
    """Reopen the file and match exactly the disk-backed expected member set."""
    key = str(archive or path)
    db.execute("UPDATE expected SET seen=0 WHERE archive=?", (key,))
    for name, size, digest in read_archive(path):
        row = db.execute(
            "SELECT size, digest, seen FROM expected WHERE archive=? AND name=?", (key, name)
        ).fetchone()
        if row is None:
            raise CompactionError(f"{path}: extra member {name}")
        if row[2]:
            raise CompactionError(f"{path}: duplicate member {name}")
        if row[:2] != (size, digest):
            raise CompactionError(f"{path}: changed member {name}")
        db.execute("UPDATE expected SET seen=1 WHERE name=?", (name,))
    missing = db.execute(
        "SELECT name FROM expected WHERE archive=? AND seen=0 LIMIT 1", (key,)
    ).fetchone()
    if missing:
        raise CompactionError(f"{path}: missing member {missing[0]}")


def publish(temp: Path, archive: Path) -> None:
    # The lock serializes compactor runs. Never overwrite a published pathname.
    if archive.exists():
        raise CompactionError(f"Refusing to replace immutable archive {archive}")
    os.rename(temp, archive)
    fsync_dir(archive.parent)


def delete_originals(originals: Iterable[Path]) -> None:
    parent = None
    for path in originals:
        path.unlink()
        parent = path.parent
    if parent is not None:
        fsync_dir(parent)


def compact_hour(
    db: sqlite3.Connection, directory: Path, key: str, level: int, long_distance: bool
) -> None:
    db.execute("DELETE FROM loose")
    db.execute("DELETE FROM expected")
    for (name,) in db.execute(
        "SELECT name FROM entries WHERE directory=? AND hour=? AND kind='original' ORDER BY name",
        (str(directory), key),
    ):
        original = directory / name
        # Tar headers need sizes first. A separate bounded pass also checks gzip CRC.
        with gzip.open(original, "rb") as stream:
            size, digest = digest_stream(stream)
        db.execute(
            "INSERT INTO loose VALUES (?, ?, ?, ?)", (name[:-3], size, digest, str(original))
        )

    sequence = -1
    for name, number in db.execute(
        "SELECT name, sequence FROM entries WHERE directory=? AND hour=? AND kind='archive'",
        (str(directory), key),
    ):
        sequence = max(sequence, number)
        archive = directory / name
        for member, size, digest in read_archive(archive):
            try:
                db.execute(
                    "INSERT INTO expected(name,size,digest,archive) VALUES (?,?,?,?)",
                    (member, size, digest, str(archive)),
                )
            except sqlite3.IntegrityError as exc:
                raise CompactionError(f"{archive}: duplicate member {member}") from exc
    changed = db.execute(
        "SELECT loose.name FROM loose JOIN expected USING(name) "
        "WHERE loose.size != expected.size OR loose.digest != expected.digest LIMIT 1"
    ).fetchone()
    if changed:
        raise CompactionError(f"{directory}/{key}: changed original {changed[0]}")

    pending = db.execute(
        "SELECT count(*) FROM loose WHERE name NOT IN (SELECT name FROM expected)"
    ).fetchone()[0]
    if pending:
        suffix = "" if sequence < 0 else f".late-{sequence + 1}"
        archive = directory / f"{key}{suffix}.tar.zst"
        temp = directory / f".{archive.name}.tmp"
        db.execute(
            "INSERT INTO expected(name,size,digest,archive,original) "
            "SELECT name,size,digest,?,original FROM loose "
            "WHERE name NOT IN (SELECT name FROM expected)",
            (str(archive),),
        )
        build_archive(temp, db, archive, level, long_distance)
        verify_archive(temp, db, archive)
        publish(temp, archive)

    # Deletion gate: reopen ALL final archives, including any matching retry copies.
    for (name,) in db.execute("SELECT DISTINCT archive FROM expected ORDER BY archive"):
        verify_archive(Path(name), db)
    delete_originals(Path(row[0]) for row in db.execute("SELECT original FROM loose ORDER BY name"))
    count, size = db.execute("SELECT count(*),coalesce(sum(size),0) FROM loose").fetchone()
    LOGGER.info("Compacted hour=%s/%s files=%d json_bytes=%d", directory, key, count, size)


def initialize_index(db: sqlite3.Connection) -> None:
    db.executescript("""
        PRAGMA journal_mode=OFF;
        PRAGMA synchronous=OFF;
        PRAGMA cache_size=-2048;
        PRAGMA temp_store=FILE;
        CREATE TABLE directories(path TEXT PRIMARY KEY, day TEXT);
        CREATE TABLE entries(directory TEXT, hour TEXT, name TEXT, kind TEXT, sequence INTEGER);
        CREATE INDEX jobs ON entries(hour,directory,kind,name);
        CREATE TABLE loose(name TEXT PRIMARY KEY, size INTEGER, digest TEXT, original TEXT);
        CREATE TABLE expected(name TEXT PRIMARY KEY, size INTEGER, digest TEXT, archive TEXT,
                              original TEXT, seen INTEGER DEFAULT 0);
        CREATE INDEX archive_members ON expected(archive,name);
    """)


def inventory_day(
    db: sqlite3.Connection, day: str, now: datetime, *, cleanup_temps: bool = True
) -> None:
    db.execute("DELETE FROM entries")
    for (directory,) in db.execute(
        "SELECT path FROM directories WHERE day=? ORDER BY path", (day,)
    ):
        with os.scandir(directory) as paths:
            for path in paths:
                for pattern in (ORIGINAL, ARCHIVE, TEMP):
                    match = pattern.fullmatch(path.name)
                    if match:
                        break
                else:
                    raise CompactionError(f"Unexpected entry in raw tree: {path.path}")
                if not path.is_file(follow_symlinks=False) or match[1] != day:
                    raise CompactionError(f"Entry does not belong in {directory}: {path.name}")
                kind = {ORIGINAL: "original", ARCHIVE: "archive", TEMP: "temp"}[pattern]
                start = hour_start(match[1], match[2])
                if start + timedelta(hours=1) + GRACE > now:
                    continue
                if kind == "temp":
                    if cleanup_temps:
                        # Lock held; originals outlive any unverified temp.
                        Path(path.path).unlink()
                else:
                    number = int(match[3] or 0) if kind == "archive" else 0
                    db.execute(
                        "INSERT INTO entries VALUES (?,?,?,?,?)",
                        (directory, path.name[:13], path.name, kind, number),
                    )


def compact(
    raw_root: Path,
    now: datetime,
    *,
    time_budget_seconds: float = TIME_BUDGET_SECONDS,
    level: int = ZSTD_LEVEL,
    long_distance: bool = False,
) -> bool:
    """Stop starting hours at the deadline; return False only for lock contention."""
    if now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise ValueError("Compaction time must be timezone-aware UTC")
    if not math.isfinite(time_budget_seconds) or time_budget_seconds <= 0:
        raise ValueError("Time budget must be finite and positive")
    compression_parameters(level, long_distance)  # Validate even when there is no work.
    deadline = clock.monotonic() + time_budget_seconds
    with (raw_root / LOCK_NAME).open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            LOGGER.warning("Another compaction holds %s; skipping", raw_root / LOCK_NAME)
            return False
        index = raw_root / INDEX_NAME
        index.unlink(missing_ok=True)
        try:
            with sqlite3.connect(index) as db:
                initialize_index(db)
                with os.scandir(raw_root) as sources:
                    for source in sources:
                        if source.name in (LOCK_NAME, INDEX_NAME):
                            continue
                        if not source.is_dir(follow_symlinks=False):
                            raise CompactionError(f"Unexpected entry in raw tree: {source.path}")
                        with os.scandir(source.path) as days:
                            for directory in days:
                                date.fromisoformat(directory.name)
                                if not directory.is_dir(follow_symlinks=False):
                                    raise CompactionError(f"Unexpected entry: {directory.path}")
                                db.execute(
                                    "INSERT INTO directories VALUES (?,?)",
                                    (directory.path, directory.name),
                                )
                # Scan one date at a time so a month of backlog need not be inventoried first.
                for (day,) in db.execute("SELECT DISTINCT day FROM directories ORDER BY day"):
                    if clock.monotonic() >= deadline:
                        return True
                    inventory_day(db, day, now)
                    jobs = db.execute(
                        "SELECT directory,hour FROM entries WHERE kind='original' "
                        "GROUP BY hour,directory ORDER BY hour,directory"
                    )
                    for directory, key in jobs:
                        if clock.monotonic() >= deadline:
                            LOGGER.info(
                                "Time budget exhausted; leaving remaining hours for next run"
                            )
                            return True
                        compact_hour(db, Path(directory), key, level, long_distance)
        finally:
            # sqlite's connection context commits/rolls back but does not close.
            if "db" in locals():
                db.close()
            index.unlink(missing_ok=True)
    return True


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-root", type=Path, default=Path("raw"))
    parser.add_argument("--time-budget-seconds", type=float, default=TIME_BUDGET_SECONDS)
    parser.add_argument("--zstd-level", type=int, default=ZSTD_LEVEL)
    parser.add_argument("--long-distance", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    compact(
        args.raw_root,
        datetime.now(UTC),
        time_budget_seconds=args.time_budget_seconds,
        level=args.zstd_level,
        long_distance=args.long_distance,
    )


if __name__ == "__main__":
    main()
