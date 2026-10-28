"""Hourly raw compaction: ``uv run python -m kwb.ops.compact [--raw-root raw]``.

Bundle each completed UTC hour of collector files under
``raw/<source>/YYYY-MM-DD/`` into ``YYYY-MM-DDTHH.tar.zst``: a tar whose
members are the originals' decompressed bytes, named by the original filename
with ``.gz`` removed. Originals are deleted only after the archive on disk is
reopened and every original's decompressed SHA-256 matches its member.

Crash safety: archives are written to a dot-prefixed temp name, fsynced,
verified, and atomically renamed into place, so a final archive is always
verified and an original is only ever deleted when a verified copy exists.
Reruns re-verify before deleting, and a late file for an archived hour is
merged into a rebuilt archive that must keep every existing member intact.
The hour is taken from the UTC timestamp in each filename (our fetch time).
"""

import argparse
import fcntl
import gzip
import hashlib
import io
import logging
import os
import re
import tarfile
from collections import defaultdict
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import zstandard

LOGGER = logging.getLogger(__name__)
# Collector writes can land just after the hour boundary; leave them alone.
GRACE = timedelta(minutes=5)
ZSTD_LEVEL = 19
LOCK_NAME = ".compact.lock"
ORIGINAL = re.compile(r"(\d{4}-\d{2}-\d{2})T(\d{2})\d{4}\.\d{6}Z\.json\.gz")
ARCHIVE = re.compile(r"(\d{4}-\d{2}-\d{2})T(\d{2})\.tar\.zst")
TEMP = re.compile(r"\.(\d{4}-\d{2}-\d{2})T(\d{2})\.tar\.zst\.tmp")


class CompactionError(Exception):
    """An archive failed verification or the raw tree holds something unexpected."""


def member_name(original: str) -> str:
    return original.removesuffix(".gz")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hour_start(day: str, hour: str) -> datetime:
    return datetime.combine(date.fromisoformat(day), time(int(hour)), tzinfo=UTC)


def build_archive(members: dict[str, bytes]) -> bytes:
    """Deterministic tar (sorted names, fixed metadata) compressed with zstd."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.PAX_FORMAT) as tar:
        for name in sorted(members):
            info = tarfile.TarInfo(name)
            info.size = len(members[name])
            info.mode = 0o644
            info.mtime = 0
            tar.addfile(info, io.BytesIO(members[name]))
    return zstandard.ZstdCompressor(level=ZSTD_LEVEL).compress(buffer.getvalue())


def read_archive(path: Path) -> dict[str, bytes]:
    """Read every member from disk. Duplicate or non-file members raise."""
    members: dict[str, bytes] = {}
    with (
        path.open("rb") as raw,
        zstandard.ZstdDecompressor().stream_reader(raw) as reader,
        tarfile.open(fileobj=reader, mode="r|") as tar,
    ):
        for info in tar:
            if not info.isfile() or "/" in info.name:
                raise CompactionError(f"{path}: unexpected member {info.name!r}")
            if info.name in members:
                raise CompactionError(f"{path}: duplicate member {info.name!r}")
            extracted = tar.extractfile(info)
            assert extracted is not None
            members[info.name] = extracted.read()
    return members


def fsync_dir(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_temp(path: Path, data: bytes) -> None:
    with path.open("xb") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())


def verify_archive(path: Path, expected: dict[str, str]) -> None:
    """Reopen ``path`` and require exactly the expected member names and SHA-256s."""
    actual = {name: sha256(data) for name, data in read_archive(path).items()}
    if actual != expected:
        missing = sorted(expected.keys() - actual.keys())
        extra = sorted(actual.keys() - expected.keys())
        changed = sorted(n for n in expected.keys() & actual.keys() if expected[n] != actual[n])
        raise CompactionError(
            f"{path}: verification failed missing={missing} extra={extra} changed={changed}"
        )


def publish(temp: Path, archive: Path) -> None:
    os.replace(temp, archive)
    fsync_dir(archive.parent)


def delete_originals(originals: list[Path]) -> None:
    for path in originals:
        path.unlink()
    if originals:
        fsync_dir(originals[0].parent)


def compact_hour(directory: Path, key: str, originals: list[Path]) -> None:
    archive = directory / f"{key}.tar.zst"
    temp = directory / f".{key}.tar.zst.tmp"
    # A temp file is never the only copy: originals outlive it until a final
    # archive verifies, so a leftover from an interrupted run is discarded.
    temp.unlink(missing_ok=True)

    # Corrupt gzip raises here, before anything is written or deleted.
    loose = {member_name(path.name): gzip.decompress(path.read_bytes()) for path in originals}
    loose_digests = {name: sha256(data) for name, data in loose.items()}
    existing = read_archive(archive) if archive.exists() else {}
    for name, digest in loose_digests.items():
        if name in existing and sha256(existing[name]) != digest:
            raise CompactionError(f"{archive}: {name} differs from the archived copy")

    if loose.keys() - existing.keys():
        # Old members' originals are already deleted; this archive is their only
        # copy, so verification requires every one of them unchanged.
        members = {**existing, **loose}
        write_temp(temp, build_archive(members))
        verify_archive(temp, {name: sha256(data) for name, data in members.items()})
        publish(temp, archive)

    # Deletion gate: reopen the final archive from disk, never trust memory.
    archived = {name: sha256(data) for name, data in read_archive(archive).items()}
    unverified = sorted(name for name, d in loose_digests.items() if archived.get(name) != d)
    if unverified:
        raise CompactionError(f"{archive}: not deleting, unverified members {unverified}")
    delete_originals(originals)

    input_bytes = sum(len(data) for data in loose.values())
    LOGGER.info(
        "Compacted hour=%s/%s files=%d json_bytes=%d archive_bytes=%d",
        directory,
        key,
        len(originals),
        input_bytes,
        archive.stat().st_size,
    )


def compact_directory(directory: Path, now: datetime) -> None:
    day = directory.name
    originals: dict[str, list[Path]] = defaultdict(list)
    temps: set[str] = set()
    for path in sorted(directory.iterdir()):
        for pattern in (ORIGINAL, ARCHIVE, TEMP):
            match = pattern.fullmatch(path.name)
            if match:
                break
        else:
            raise CompactionError(f"Unexpected entry in raw tree: {path}")
        if not path.is_file() or match[1] != day:
            raise CompactionError(f"Entry does not belong in {directory}: {path.name}")
        key = f"{match[1]}T{match[2]}"
        if hour_start(match[1], match[2]) + timedelta(hours=1) + GRACE > now:
            continue  # Current hour (or still within grace): never touched.
        if pattern is ORIGINAL:
            originals[key].append(path)
        elif pattern is TEMP:
            temps.add(key)
    for key in sorted(originals):
        compact_hour(directory, key, originals[key])
    for key in sorted(temps - originals.keys()):
        (directory / f".{key}.tar.zst.tmp").unlink()


def compact(raw_root: Path, now: datetime) -> bool:
    """Compact every eligible hour under ``raw_root``. False if another run holds the lock."""
    if now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise ValueError("Compaction time must be timezone-aware UTC")
    with (raw_root / LOCK_NAME).open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            LOGGER.warning("Another compaction holds %s; skipping", raw_root / LOCK_NAME)
            return False
        for source in sorted(raw_root.iterdir()):
            if source.name == LOCK_NAME:
                continue
            if not source.is_dir():
                raise CompactionError(f"Unexpected entry in raw tree: {source}")
            for directory in sorted(source.iterdir()):
                date.fromisoformat(directory.name)  # Non-date entries raise.
                if not directory.is_dir():
                    raise CompactionError(f"Unexpected entry in raw tree: {directory}")
                compact_directory(directory, now)
    return True


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-root", type=Path, default=Path("raw"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    compact(args.raw_root, datetime.now(UTC))


if __name__ == "__main__":
    main()
