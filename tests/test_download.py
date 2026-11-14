import base64
import hashlib
import json
from pathlib import Path
from unittest.mock import Mock

import google_crc32c
import pytest
from google.oauth2.credentials import Credentials

from kwb.ops import download

OBJECT = "raw/kalshi/2026-11-10/2026-11-10T22.tar.zst"


class Blob:
    def __init__(self, name=OBJECT, payload=b"real transport bytes", generation=7):
        self.name, self.payload, self.generation = name, payload, generation
        self.size = len(payload)
        self.crc32c = base64.b64encode(google_crc32c.value(payload).to_bytes(4, "big")).decode()
        self.calls = []
        self.corrupt = False
        self.interrupt = False

    def download_to_filename(self, filename, **kwargs):
        self.calls.append(kwargs)
        Path(filename).write_bytes(self.payload + (b"corruption" if self.corrupt else b""))
        if self.interrupt:
            raise OSError("interrupted transport")


class BucketReader:
    def __init__(self, *blobs):
        self.blobs = blobs

    def list_blobs(self, bucket, *, prefix):
        assert bucket == download.BUCKET
        assert prefix == "raw/kalshi/"
        return iter(self.blobs)


def test_downloaded_archives_are_generation_pinned_and_verified_in_the_manifest(tmp_path):
    blob = Blob()
    manifest = tmp_path / "state/download.json"
    assert download.download(tmp_path, manifest, client=BucketReader(blob)) == {
        "downloaded": 1,
        "reused": 0,
        "reconciled": 0,
    }
    assert (tmp_path / OBJECT).read_bytes() == blob.payload
    entry = json.loads(manifest.read_text())["objects"][OBJECT]
    assert entry == {
        "generation": 7,
        "size": blob.size,
        "crc32c": blob.crc32c,
        "sha256": hashlib.sha256(blob.payload).hexdigest(),
    }
    assert blob.calls == [
        {"if_generation_match": 7, "checksum": "crc32c", "raw_download": True, "timeout": 60}
    ]
    assert download.download(tmp_path, manifest, client=BucketReader(blob))["reused"] == 1
    assert len(blob.calls) == 1


@pytest.mark.parametrize("mutation", ["content", "generation", "crc32c", "missing"])
def test_changed_local_archives_or_immutable_object_metadata_raise(tmp_path, mutation):
    blob = Blob()
    manifest = tmp_path / "state/download.json"
    download.download(tmp_path, manifest, client=BucketReader(blob))
    if mutation == "content":
        (tmp_path / OBJECT).write_bytes(b"corrupt local copy")
    elif mutation == "missing":
        (tmp_path / OBJECT).unlink()
    elif mutation == "generation":
        blob.generation += 1
    else:
        blob.crc32c = "unexpected"
    with pytest.raises(ValueError):
        download.download(tmp_path, manifest, client=BucketReader(blob))
    assert len(blob.calls) == 1


@pytest.mark.parametrize("failure", ["checksum", "interrupt"])
def test_failed_downloads_publish_neither_final_archives_nor_manifest_entries(tmp_path, failure):
    blob = Blob()
    manifest = tmp_path / "state/download.json"
    blob.corrupt = failure == "checksum"
    blob.interrupt = failure == "interrupt"
    with pytest.raises((ValueError, OSError)):
        download.download(tmp_path, manifest, client=BucketReader(blob))
    assert not (tmp_path / OBJECT).exists()
    assert not manifest.exists()
    assert list((tmp_path / OBJECT).parent.glob(".*.tmp")) == []


def test_a_completed_download_without_a_manifest_entry_is_reconciled_without_network_transfer(
    tmp_path,
):
    blob = Blob()
    path = tmp_path / OBJECT
    path.parent.mkdir(parents=True)
    path.write_bytes(blob.payload)
    result = download.download(
        tmp_path, tmp_path / "state/download.json", client=BucketReader(blob)
    )
    assert result["reconciled"] == 1
    assert blob.calls == []


def test_late_supplements_are_discovered_on_subsequent_listings(tmp_path):
    manifest = tmp_path / "state/download.json"
    base = Blob()
    download.download(tmp_path, manifest, client=BucketReader(base))
    late = Blob(name=OBJECT.replace(".tar.zst", ".late-1.tar.zst"), payload=b"late")
    result = download.download(tmp_path, manifest, client=BucketReader(base, late))
    assert result == {"downloaded": 1, "reused": 1, "reconciled": 0}
    assert (tmp_path / late.name).read_bytes() == b"late"


@pytest.mark.parametrize(
    "name",
    [
        "../escape.tar.zst",
        "raw/kalshi/2026-11-11/2026-11-10T22.tar.zst",
        "raw/kalshi/2026-11-10/.2026-11-10T22.tar.zst.tmp",
        "/raw/kalshi/2026-11-10/2026-11-10T22.tar.zst",
    ],
)
def test_unpublished_or_unsafe_bucket_object_names_raise(tmp_path, name):
    with pytest.raises(ValueError, match="Unexpected bucket object"):
        download.download(
            tmp_path, tmp_path / "state/download.json", client=BucketReader(Blob(name=name))
        )


def test_the_download_manifest_must_remain_outside_raw_history(tmp_path):
    with pytest.raises(ValueError, match="outside raw"):
        download.download(tmp_path, tmp_path / "raw/manifest.json", client=BucketReader())


def test_the_sdk_uses_user_adc_and_rejects_service_account_credentials(monkeypatch):
    user = Credentials(token="synthetic-test-token")
    client = Mock()
    monkeypatch.setattr(download.google.auth, "default", lambda **kwargs: (user, None))
    monkeypatch.setattr(download.storage, "Client", client)
    download.user_adc_client()
    assert client.call_args.kwargs["credentials"] is user
    monkeypatch.setattr(download.google.auth, "default", lambda **kwargs: (object(), None))
    with pytest.raises(ValueError, match="user Application Default"):
        download.user_adc_client()
