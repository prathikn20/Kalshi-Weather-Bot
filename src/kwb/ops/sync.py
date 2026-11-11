"""Upload published hourly raw archives to Cloud Storage, write-once.

Only names compaction has published are uploaded; compaction verifies an archive
before atomically renaming it from a dot-prefixed temp file, so a matching name is
always complete. Objects are created with if_generation_match=0, and every run
refuses credentials with delete permission, including for SDK checksum cleanup.
A local SQLite ledger records each confirmed upload, so the bucket is never listed:
objects.list is a billed Class A operation, and the free tier allows 5,000 a month.
"""

import argparse
import base64
import fcntl
import logging
import math
import os
import sqlite3
import time as clock
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol

import google_crc32c

from kwb.ops import compact

LOGGER = logging.getLogger(__name__)
TIME_BUDGET_SECONDS = 300.0
CHUNK_SIZE = 1024 * 1024
OBJECT_PREFIX = "raw/"


class SyncError(Exception):
    """The raw tree or the bucket holds something the sync must not paper over."""


class AlreadyExists(Exception):
    """The create precondition failed: an object with this name is already stored."""


@dataclass(frozen=True)
class RemoteObject:
    size: int
    crc32c: str  # Base64 big-endian CRC32C, the form Cloud Storage reports.
    generation: int


class Remote(Protocol):
    def check_write_once(self) -> None:
        """Raise if the credential can delete objects or the check fails."""
        ...

    def create(self, name: str, path: Path) -> RemoteObject:
        """Store path under name only if no object has that name; else AlreadyExists."""
        ...

    def describe(self, name: str) -> RemoteObject | None: ...


class GcsRemote:
    def __init__(self, bucket_name: str, client=None) -> None:
        from google.cloud import storage

        self._bucket = (client or storage.Client()).bucket(bucket_name)

    def check_write_once(self) -> None:
        # No extra IAM permission is required; this is one Class B operation.
        # The SDK may attempt deletion on a resumable-upload checksum failure.
        granted = self._bucket.test_iam_permissions(["storage.objects.delete"])
        if "storage.objects.delete" in granted:
            raise SyncError("Refusing raw sync: credential holds storage.objects.delete")

    def create(self, name: str, path: Path) -> RemoteObject:
        from google.api_core.exceptions import PreconditionFailed

        blob = self._bucket.blob(name)
        try:
            # Generation 0 means "only if absent". The precondition also makes the
            # library's automatic retries safe: a retry after a lost response gets 412.
            blob.upload_from_filename(
                str(path),
                content_type="application/zstd",
                if_generation_match=0,
                checksum="crc32c",
            )
        except PreconditionFailed as exc:
            raise AlreadyExists(name) from exc
        return RemoteObject(blob.size, blob.crc32c, blob.generation)

    def describe(self, name: str) -> RemoteObject | None:
        blob = self._bucket.get_blob(name)
        if blob is None:
            return None
        return RemoteObject(blob.size, blob.crc32c, blob.generation)


@dataclass
class SyncStats:
    uploaded: int = 0
    reconciled: int = 0
    pending: int = 0


def local_crc32c(path: Path) -> str:
    checksum = google_crc32c.Checksum()
    with path.open("rb") as stream:
        while chunk := stream.read(CHUNK_SIZE):
            checksum.update(chunk)
    return base64.b64encode(checksum.digest()).decode()


def published_archives(raw_root: Path) -> list[Path]:
    """Every published archive, oldest hour first. Unknown entries raise."""
    found = []
    with os.scandir(raw_root) as sources:
        for source in sources:
            if source.name in (compact.LOCK_NAME, compact.INDEX_NAME):
                continue
            if not source.is_dir(follow_symlinks=False):
                raise SyncError(f"Unexpected entry in raw tree: {source.path}")
            with os.scandir(source.path) as days:
                for day in days:
                    date.fromisoformat(day.name)
                    if not day.is_dir(follow_symlinks=False):
                        raise SyncError(f"Unexpected entry in raw tree: {day.path}")
                    with os.scandir(day.path) as entries:
                        for entry in entries:
                            match = compact.ARCHIVE.fullmatch(entry.name)
                            if not (
                                match
                                or compact.ORIGINAL.fullmatch(entry.name)
                                or compact.TEMP.fullmatch(entry.name)
                            ):
                                raise SyncError(f"Unexpected entry in raw tree: {entry.path}")
                            if not entry.is_file(follow_symlinks=False):
                                raise SyncError(f"Entry is not a regular file: {entry.path}")
                            if match:
                                key = (match[1], match[2], int(match[3] or 0), source.name)
                                found.append((key, Path(entry.path)))
    return [path for _, path in sorted(found)]


def object_name(raw_root: Path, path: Path) -> str:
    return OBJECT_PREFIX + path.relative_to(raw_root).as_posix()


def open_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute(
        "CREATE TABLE IF NOT EXISTS uploaded("
        "name TEXT PRIMARY KEY, size INTEGER NOT NULL, crc32c TEXT NOT NULL, "
        "generation INTEGER NOT NULL, recorded_at TEXT NOT NULL)"
    )
    return db


def sync(
    raw_root: Path,
    ledger: Path,
    remote: Remote,
    *,
    time_budget_seconds: float = TIME_BUDGET_SECONDS,
) -> SyncStats:
    """Upload unrecorded archives until done or the budget is spent; never start late."""
    if not math.isfinite(time_budget_seconds) or time_budget_seconds <= 0:
        raise ValueError("Time budget must be finite and positive")
    if ledger.resolve().is_relative_to(raw_root.resolve()):
        raise SyncError("The ledger must be outside the raw tree")
    deadline = clock.monotonic() + time_budget_seconds
    stats = SyncStats()
    with (raw_root / compact.LOCK_NAME).open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            LOGGER.warning(
                "Another maintenance job holds %s; skipping", raw_root / compact.LOCK_NAME
            )
            return stats
        remote.check_write_once()
        db = open_ledger(ledger)
        try:
            recorded = {name for (name,) in db.execute("SELECT name FROM uploaded")}
            pending = [
                path
                for path in published_archives(raw_root)
                if object_name(raw_root, path) not in recorded
            ]
            stats.pending = len(pending)
            for path in pending:
                if clock.monotonic() >= deadline:
                    LOGGER.info("Time budget exhausted; leaving remaining archives for next run")
                    break
                name = object_name(raw_root, path)
                expected = (path.stat().st_size, local_crc32c(path))
                if clock.monotonic() >= deadline:
                    LOGGER.info("Time budget exhausted while hashing; leaving archive for next run")
                    break
                try:
                    stored = remote.create(name, path)
                except AlreadyExists:
                    # A crash or retry may leave an object without a ledger entry.
                    # Accept it only if identical to the local archive.
                    stored = remote.describe(name)
                    if stored is None:
                        raise SyncError(f"{name}: create refused but object not found") from None
                    stats.reconciled += 1
                else:
                    stats.uploaded += 1
                if (stored.size, stored.crc32c) != expected:
                    raise SyncError(
                        f"{name}: stored size/crc32c {stored.size}/{stored.crc32c} "
                        f"differs from local {expected[0]}/{expected[1]}"
                    )
                db.execute(
                    "INSERT INTO uploaded VALUES (?,?,?,?,?)",
                    (
                        name,
                        stored.size,
                        stored.crc32c,
                        stored.generation,
                        datetime.now(UTC).isoformat(),
                    ),
                )
                db.commit()
                stats.pending -= 1
        finally:
            db.close()
    LOGGER.info(
        "Sync uploaded=%d reconciled=%d pending=%d",
        stats.uploaded,
        stats.reconciled,
        stats.pending,
    )
    return stats


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--raw-root", type=Path, default=Path("raw"))
    parser.add_argument("--ledger", type=Path, default=Path("state/raw-sync.sqlite"))
    parser.add_argument("--time-budget-seconds", type=float, default=TIME_BUDGET_SECONDS)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    sync(
        args.raw_root,
        args.ledger,
        GcsRemote(args.bucket),
        time_budget_seconds=args.time_budget_seconds,
    )


if __name__ == "__main__":
    main()
