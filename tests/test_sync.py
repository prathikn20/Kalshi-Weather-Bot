import base64
import configparser
import fcntl
import select
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import google_crc32c
import pytest
from google.api_core.exceptions import PreconditionFailed, ServiceUnavailable
from google.cloud import storage

from kwb.ops import compact, sync

FIXTURES = Path(__file__).parent / "fixtures/kalshi_raw/2026-10-07"
AFTER_GRACE = datetime(2026, 10, 7, 15, 5, tzinfo=UTC)


def crc(data: bytes) -> str:
    """Independent one-shot CRC32C, so tests don't trust sync.local_crc32c."""
    return base64.b64encode(google_crc32c.value(data).to_bytes(4, "big")).decode()


class FakeBucket:
    """Write-once store mirroring the bucket's IAM: create-if-absent and read, no delete."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.creates: list[str] = []
        self.corrupt = False

    def check_write_once(self) -> None:
        """This fake has no delete operation or permission."""

    def create(self, name: str, path: Path) -> sync.RemoteObject:
        self.creates.append(name)
        if name in self.objects:
            raise sync.AlreadyExists(name)
        data = path.read_bytes()
        self.objects[name] = data + b"x" if self.corrupt else data
        return self.describe(name)

    def describe(self, name: str) -> sync.RemoteObject | None:
        data = self.objects.get(name)
        if data is None:
            return None
        return sync.RemoteObject(len(data), crc(data), generation=len(self.creates))


@pytest.fixture
def raw_root(tmp_path: Path) -> Path:
    """A real compacted hour plus a late supplement, a loose original and a temp file."""
    root = tmp_path / "raw"
    shutil.copytree(FIXTURES, root / "kalshi/2026-10-07")
    compact.compact(root, AFTER_GRACE)
    day = root / "kalshi/2026-10-07"
    (day / "2026-10-07T14.late-1.tar.zst").write_bytes(b"late supplement")
    (day / "2026-10-07T150000.000000Z.json.gz").write_bytes(b"loose original")
    (day / ".2026-10-07T15.tar.zst.tmp").write_bytes(b"unverified temp")
    return root


@pytest.fixture
def ledger(tmp_path: Path) -> Path:
    return tmp_path / "state/raw-sync.sqlite"


ARCHIVES = [
    "raw/kalshi/2026-10-07/2026-10-07T14.tar.zst",
    "raw/kalshi/2026-10-07/2026-10-07T14.late-1.tar.zst",
]


def test_only_published_archives_are_uploaded_never_loose_originals_or_temp_files(raw_root, ledger):
    bucket = FakeBucket()
    sync.sync(raw_root, ledger, bucket)
    assert sorted(bucket.objects) == sorted(ARCHIVES)
    for name, data in bucket.objects.items():
        assert data == (raw_root.parent / name).read_bytes()


def test_an_unexpected_file_in_the_raw_tree_raises_instead_of_being_skipped(raw_root, ledger):
    (raw_root / "kalshi/2026-10-07/notes.txt").write_text("stray")
    bucket = FakeBucket()
    with pytest.raises(sync.SyncError, match="notes.txt"):
        sync.sync(raw_root, ledger, bucket)
    assert bucket.creates == []


def test_an_archive_recorded_in_the_ledger_is_never_uploaded_again(raw_root, ledger):
    bucket = FakeBucket()
    sync.sync(raw_root, ledger, bucket)
    stats = sync.sync(raw_root, ledger, bucket)
    assert sorted(bucket.creates) == sorted(ARCHIVES)
    assert (stats.uploaded, stats.reconciled, stats.pending) == (0, 0, 0)


def test_an_object_already_in_the_bucket_with_identical_content_is_recorded_not_replaced(
    raw_root, ledger
):
    # Simulates a crash after the upload succeeded but before the ledger write.
    bucket = FakeBucket()
    first = (raw_root.parent / ARCHIVES[0]).read_bytes()
    bucket.objects[ARCHIVES[0]] = first
    stats = sync.sync(raw_root, ledger, bucket)
    assert bucket.objects[ARCHIVES[0]] is first
    assert (stats.uploaded, stats.reconciled) == (1, 1)
    sync.sync(raw_root, ledger, bucket)
    assert bucket.creates.count(ARCHIVES[0]) == 1


def test_an_object_already_in_the_bucket_with_different_content_raises_and_is_not_recorded(
    raw_root, ledger
):
    bucket = FakeBucket()
    bucket.objects[ARCHIVES[0]] = b"something else"
    with pytest.raises(sync.SyncError, match="differs from local"):
        sync.sync(raw_root, ledger, bucket)
    with sync.open_ledger(ledger) as db:
        assert db.execute("SELECT count(*) FROM uploaded").fetchone() == (0,)


def test_an_upload_whose_stored_checksum_differs_from_the_local_file_is_not_recorded(
    raw_root, ledger
):
    bucket = FakeBucket()
    bucket.corrupt = True
    with pytest.raises(sync.SyncError, match="differs from local"):
        sync.sync(raw_root, ledger, bucket)
    with sync.open_ledger(ledger) as db:
        assert db.execute("SELECT count(*) FROM uploaded").fetchone() == (0,)


def test_no_upload_starts_after_the_time_budget_is_spent(raw_root, ledger, monkeypatch):
    now = [0.0]
    monkeypatch.setattr(sync.clock, "monotonic", lambda: now[0])
    bucket = FakeBucket()
    create = bucket.create

    def slow_create(name, path):
        result = create(name, path)
        now[0] = 10.0
        return result

    monkeypatch.setattr(bucket, "create", slow_create)
    stats = sync.sync(raw_root, ledger, bucket, time_budget_seconds=5)
    assert len(bucket.creates) == 1
    assert (stats.uploaded, stats.pending) == (1, 1)


def test_the_gcs_adapter_only_creates_objects_that_do_not_already_exist(tmp_path):
    path = tmp_path / "a.tar.zst"
    path.write_bytes(b"data")
    client = mock.Mock()
    blob = client.bucket.return_value.blob.return_value
    remote = sync.GcsRemote("bucket", client=client)

    remote.create("raw/a.tar.zst", path)
    assert blob.upload_from_filename.call_args.kwargs["if_generation_match"] == 0

    blob.upload_from_filename.side_effect = PreconditionFailed("exists")
    with pytest.raises(sync.AlreadyExists):
        remote.create("raw/a.tar.zst", path)


def test_the_local_checksum_matches_cloud_storages_crc32c_encoding(tmp_path):
    path = tmp_path / "a"
    path.write_bytes(b"Hello, world!" * 100_000)  # Spans several read chunks.
    assert sync.local_crc32c(path) == crc(path.read_bytes())


def test_no_upload_starts_if_hashing_uses_the_remaining_budget(raw_root, ledger, monkeypatch):
    now = [0.0]
    monkeypatch.setattr(sync.clock, "monotonic", lambda: now[0])
    checksum = sync.local_crc32c

    def slow_checksum(path):
        result = checksum(path)
        now[0] = 6.0
        return result

    monkeypatch.setattr(sync, "local_crc32c", slow_checksum)
    bucket = FakeBucket()
    stats = sync.sync(raw_root, ledger, bucket, time_budget_seconds=5)
    assert bucket.creates == []
    assert (stats.uploaded, stats.pending) == (0, 2)


@pytest.mark.parametrize("first_run_is_safe", [False, True])
def test_each_run_refuses_delete_permission_before_any_upload(
    raw_root, ledger, monkeypatch, first_run_is_safe
):
    client = mock.Mock()
    iam = client.bucket.return_value.test_iam_permissions
    iam.return_value = []
    remote = sync.GcsRemote("bucket", client=client)
    bucket = FakeBucket()
    monkeypatch.setattr(remote, "create", bucket.create)
    if first_run_is_safe:
        sync.sync(raw_root, ledger, remote)
        assert sorted(bucket.creates) == sorted(ARCHIVES)
        bucket.creates.clear()
    iam.return_value = ["storage.objects.delete"]
    with pytest.raises(sync.SyncError, match="credential holds storage.objects.delete"):
        sync.sync(raw_root, ledger, remote)
    assert bucket.creates == []
    assert iam.call_args_list == [mock.call(["storage.objects.delete"])] * (
        2 if first_run_is_safe else 1
    )
    if not first_run_is_safe:
        assert not ledger.exists()


def test_a_failed_permission_check_stops_the_run_before_creating_the_ledger(raw_root, ledger):
    client = mock.Mock()
    client.bucket.return_value.test_iam_permissions.side_effect = ServiceUnavailable("unavailable")
    remote = sync.GcsRemote("bucket", client=client)
    with pytest.raises(ServiceUnavailable):
        sync.sync(raw_root, ledger, remote)
    assert not ledger.exists()
    client.bucket.return_value.blob.assert_not_called()


def test_sync_warns_and_skips_without_work_when_compaction_holds_the_lock(raw_root, ledger, caplog):
    bucket = FakeBucket()
    with (raw_root / compact.LOCK_NAME).open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        stats = sync.sync(raw_root, ledger, bucket)
    assert bucket.creates == []
    assert not ledger.exists()
    assert stats == sync.SyncStats()
    assert any(
        record.levelname == "WARNING" and "skipping" in record.message for record in caplog.records
    )


@pytest.mark.parametrize("corrupt", [False, True])
def test_sync_holds_compactions_lock_during_upload_and_releases_it_afterward(
    raw_root, ledger, monkeypatch, corrupt
):
    bucket = FakeBucket()
    bucket.corrupt = corrupt
    create = bucket.create

    def create_while_compaction_is_excluded(name, path):
        assert compact.compact(raw_root, AFTER_GRACE) is False
        return create(name, path)

    monkeypatch.setattr(bucket, "create", create_while_compaction_is_excluded)
    if corrupt:
        with pytest.raises(sync.SyncError, match="differs from local"):
            sync.sync(raw_root, ledger, bucket)
    else:
        sync.sync(raw_root, ledger, bucket)
    with (raw_root / compact.LOCK_NAME).open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)


@pytest.mark.parametrize("placement", ["direct", "symlink_parent", "symlink_file"])
def test_a_resolved_ledger_inside_raw_is_rejected_before_it_is_created(raw_root, placement):
    target = raw_root / "kalshi/2026-10-07/2026-10-07T160000.000000Z.json.gz"
    if placement == "symlink_parent":
        alias = raw_root.parent / "raw-alias"
        alias.symlink_to(raw_root, target_is_directory=True)
        ledger = alias / target.relative_to(raw_root)
    elif placement == "symlink_file":
        ledger = raw_root.parent / "ledger-alias"
        ledger.symlink_to(target)
    else:
        ledger = target
    bucket = FakeBucket()
    with pytest.raises(sync.SyncError, match="ledger must be outside"):
        sync.sync(raw_root, ledger, bucket)
    assert not target.exists()
    assert bucket.creates == []


@pytest.mark.parametrize(
    "name",
    [
        "2026-10-07T16.tar.zst",
        "2026-10-07T160000.000000Z.json.gz",
        ".2026-10-07T16.tar.zst.tmp",
    ],
)
@pytest.mark.parametrize("kind", ["directory", "symlink", "dangling_symlink"])
def test_every_archive_original_and_temp_entry_is_a_regular_file_and_not_a_symlink(
    raw_root, ledger, name, kind
):
    entry = raw_root / "kalshi/2026-10-07" / name
    if kind == "directory":
        entry.mkdir()
    else:
        target = raw_root.parent / "outside-raw"
        if kind == "symlink":
            target.write_bytes(b"regular file outside raw")
        entry.symlink_to(target)
    bucket = FakeBucket()
    with pytest.raises(sync.SyncError, match="not a regular file"):
        sync.sync(raw_root, ledger, bucket)
    assert bucket.creates == []


@pytest.mark.parametrize("field", ["size", "crc32c"])
@pytest.mark.parametrize("already_exists", [False, True])
def test_size_and_crc32c_must_each_match_before_recording(
    raw_root, ledger, monkeypatch, field, already_exists
):
    bucket = FakeBucket()
    if already_exists:
        bucket.objects[ARCHIVES[0]] = (raw_root.parent / ARCHIVES[0]).read_bytes()
    describe = bucket.describe

    def changed_metadata(name):
        stored = describe(name)
        return sync.RemoteObject(
            stored.size + (1 if field == "size" else 0),
            "AAAAAA==" if field == "crc32c" else stored.crc32c,
            stored.generation,
        )

    monkeypatch.setattr(bucket, "describe", changed_metadata)
    with pytest.raises(sync.SyncError, match="differs from local"):
        sync.sync(raw_root, ledger, bucket)
    db = sync.open_ledger(ledger)
    try:
        assert db.execute("SELECT count(*) FROM uploaded").fetchone() == (0,)
    finally:
        db.close()


@pytest.mark.parametrize("setting", ["CPUQuota", "CPUWeight", "IOWeight", "Nice"])
def test_sync_has_the_same_resource_priority_as_compaction(setting):
    deploy = Path(__file__).resolve().parents[1] / "deploy"
    sync_service = configparser.ConfigParser(interpolation=None)
    compact_service = configparser.ConfigParser(interpolation=None)
    sync_service.read(deploy / "raw-sync.service")
    compact_service.read(deploy / "raw-compaction.service")
    assert sync_service["Service"][setting] == compact_service["Service"][setting]


def test_syncs_oneshot_service_has_a_fifteen_minute_start_timeout():
    service = configparser.ConfigParser(interpolation=None)
    service.read(Path(__file__).resolve().parents[1] / "deploy/raw-sync.service")
    assert service["Service"]["Type"] == "oneshot"
    assert service["Service"].get("TimeoutStartSec") == "15min"


@pytest.mark.parametrize("stop_method", ["terminate", "kill"])
def test_terminating_a_sync_process_releases_compactions_lock(raw_root, ledger, stop_method):
    script = """
import signal
import sys
from pathlib import Path
from kwb.ops import sync

class HungRemote:
    def check_write_once(self):
        pass

    def create(self, name, path):
        print("upload started", flush=True)
        signal.pause()

sync.sync(Path(sys.argv[1]), Path(sys.argv[2]), HungRemote())
"""
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(raw_root), str(ledger)],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert select.select([process.stdout], [], [], 5)[0], "Sync did not reach the upload"
        assert process.stdout.readline().strip() == "upload started"
        with (raw_root / compact.LOCK_NAME).open("a") as lock:
            with pytest.raises(BlockingIOError):
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            getattr(process, stop_method)()
            process.wait(timeout=5)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.stdout.close()


def test_the_gcs_adapter_never_calls_any_list_method_on_the_client_or_bucket(tmp_path):
    path = tmp_path / "archive.tar.zst"
    path.write_bytes(b"data")
    client = mock.Mock(spec=storage.Client)
    bucket = mock.Mock(spec=storage.Bucket)
    client.bucket.return_value = bucket
    bucket.test_iam_permissions.return_value = []
    for owner, sdk_class in ((client, storage.Client), (bucket, storage.Bucket)):
        for name in dir(sdk_class):
            if name.startswith("list_"):
                getattr(owner, name).side_effect = AssertionError(f"Unexpected listing: {name}")
    blob = bucket.blob.return_value
    blob.size, blob.crc32c, blob.generation = 4, crc(b"data"), 1
    bucket.get_blob.return_value = blob
    remote = sync.GcsRemote("bucket", client=client)
    remote.check_write_once()
    assert remote.create("raw/archive.tar.zst", path) == sync.RemoteObject(4, crc(b"data"), 1)
    assert remote.describe("raw/archive.tar.zst") == sync.RemoteObject(4, crc(b"data"), 1)
    for owner, sdk_class in ((client, storage.Client), (bucket, storage.Bucket)):
        for name in dir(sdk_class):
            if name.startswith("list_"):
                getattr(owner, name).assert_not_called()
