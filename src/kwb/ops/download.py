"""Read-only GCS-to-Mac download of immutable Kalshi archives using user ADC."""

import argparse
import fcntl
import json
import os
import uuid
from pathlib import Path, PurePosixPath

import google.auth
from google.cloud import storage
from google.oauth2.credentials import Credentials

from kwb.store import ARCHIVE, StoreError, atomic_json, file_checksums

BUCKET = "kalshi-weather-bot-raw-prathikn20"
PREFIX = "raw/kalshi/"


def user_adc_client() -> storage.Client:
    credentials, project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/devstorage.read_only"]
    )
    if not isinstance(credentials, Credentials):
        raise StoreError("Downloader requires user Application Default Credentials")
    return storage.Client(project=project or "kwb-local", credentials=credentials)


def archive_path(name: str) -> Path:
    parts = PurePosixPath(name).parts
    if (
        len(parts) != 4
        or parts[:2] != ("raw", "kalshi")
        or not ARCHIVE.fullmatch(parts[3])
        or parts[2] != parts[3][:10]
    ):
        raise StoreError(f"Unexpected bucket object: {name}")
    return Path(*parts)


def download(
    root: Path,
    manifest_path: Path,
    *,
    client=None,
    bucket: str = BUCKET,
    max_objects: int | None = None,
) -> dict[str, int]:
    """List all published objects; fetch only missing generations, validating every reuse.

    GCS metadata supplies size/CRC32C/generation. SHA-256 is computed locally and
    retained for subsequent checks. No operation writes to the bucket.
    """
    if max_objects is not None and max_objects <= 0:
        raise StoreError("max_objects must be positive")
    root, manifest_path = Path(root), Path(manifest_path)
    if manifest_path.resolve().is_relative_to((root / "raw").resolve()):
        raise StoreError("Download manifest must be outside raw/")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        manifest = (
            json.loads(manifest_path.read_text())
            if manifest_path.exists()
            else {
                "version": 1,
                "bucket": bucket,
                "objects": {},
            }
        )
        if (
            set(manifest) != {"version", "bucket", "objects"}
            or manifest["version"] != 1
            or manifest["bucket"] != bucket
        ):
            raise StoreError(f"{manifest_path}: incompatible download manifest")
        client = user_adc_client() if client is None else client
        stats = {"downloaded": 0, "reused": 0, "reconciled": 0}
        for blob in client.list_blobs(bucket, prefix=PREFIX):
            relative = archive_path(blob.name)
            path = root / relative
            if any(parent.is_symlink() for parent in (path, *path.parents)):
                raise StoreError(f"{path}: symlink in download path")
            if (
                not isinstance(blob.generation, int)
                or blob.generation <= 0
                or not isinstance(blob.size, int)
                or blob.size < 0
                or not isinstance(blob.crc32c, str)
                or not blob.crc32c
            ):
                raise StoreError(f"{blob.name}: missing object verification metadata")
            remote = {"generation": blob.generation, "size": blob.size, "crc32c": blob.crc32c}
            entry = manifest["objects"].get(blob.name)
            if entry is not None and (
                set(entry) != {*remote, "sha256"}
                or any(entry[key] != value for key, value in remote.items())
            ):
                raise StoreError(f"{blob.name}: immutable object metadata changed")
            if path.exists():
                size, crc, sha = file_checksums(path)
                if (size, crc) != (blob.size, blob.crc32c) or (entry and entry["sha256"] != sha):
                    raise StoreError(
                        f"{path}: local archive differs from the download manifest/object"
                    )
                stats["reused" if entry else "reconciled"] += 1
            else:
                if entry:
                    raise StoreError(f"{path}: manifest claims a missing local archive")
                path.parent.mkdir(parents=True, exist_ok=True)
                temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
                try:
                    blob.download_to_filename(
                        str(temp),
                        if_generation_match=blob.generation,
                        checksum="crc32c",
                        raw_download=True,
                        timeout=60,
                    )
                    size, crc, sha = file_checksums(temp)
                    if (size, crc) != (blob.size, blob.crc32c):
                        raise StoreError(f"{blob.name}: downloaded size/CRC32C mismatch")
                    with temp.open("rb") as stream:
                        os.fsync(stream.fileno())
                    os.replace(temp, path)
                    fd = os.open(path.parent, os.O_RDONLY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                finally:
                    temp.unlink(missing_ok=True)
                stats["downloaded"] += 1
            manifest["objects"][blob.name] = {**remote, "sha256": sha}
            atomic_json(manifest_path, manifest)
            if max_objects is not None and sum(stats.values()) >= max_objects:
                break
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--manifest", type=Path, default=Path("state/raw-download.json"))
    parser.add_argument("--bucket", default=BUCKET)
    parser.add_argument("--max-objects", type=int)
    args = parser.parse_args()
    print(
        json.dumps(
            download(args.root, args.manifest, bucket=args.bucket, max_objects=args.max_objects),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
