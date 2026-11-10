import gzip
import io
import json
import shutil
import signal
import subprocess
import sys
import tarfile
import time
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path

import pytest
import zstandard

from kwb.ops import compact

FIXTURES = Path(__file__).parent / "fixtures/kalshi_raw/2026-10-07"
# Real collector files from 14:00 UTC; the hour is eligible from 15:05 UTC.
AFTER_GRACE = datetime(2026, 10, 7, 15, 5, tzinfo=UTC)


@pytest.fixture
def raw_root(tmp_path: Path) -> Path:
    root = tmp_path / "raw"
    shutil.copytree(FIXTURES, root / "kalshi/2026-10-07")
    return root


@pytest.fixture
def day_dir(raw_root: Path) -> Path:
    return raw_root / "kalshi/2026-10-07"


def add_original(directory: Path, stamp: str, body: str = "{}") -> Path:
    path = directory / f"{stamp}.json.gz"
    path.write_bytes(gzip.compress(json.dumps({"body": body}).encode()))
    return path


def originals(directory: Path) -> dict[str, bytes]:
    """Expected archive contents: decompressed bytes keyed by name with .gz removed."""
    return {
        path.name.removesuffix(".gz"): gzip.decompress(path.read_bytes())
        for path in sorted(directory.glob("*.json.gz"))
    }


def archive_members(path: Path) -> dict[str, bytes]:
    """Independent reader so the tests don't trust compact.read_archive."""
    data = zstandard.ZstdDecompressor().stream_reader(io.BytesIO(path.read_bytes())).read()
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        return {info.name: tar.extractfile(info).read() for info in tar.getmembers()}


def recoverable(directory: Path) -> dict[str, bytes]:
    """Every content still recoverable from loose originals plus final archives."""
    found = originals(directory)
    for archive in directory.glob("*.tar.zst"):
        found.update(archive_members(archive))
    return found


def test_every_original_files_decompressed_content_is_recovered_byte_for_byte_under_its_original_filename_with_gz_removed(  # noqa: E501
    raw_root, day_dir
):
    expected = originals(day_dir)
    assert len(expected) == 3

    compact.compact(raw_root, AFTER_GRACE)

    assert list(day_dir.glob("*.json.gz")) == []
    assert archive_members(day_dir / "2026-10-07T14.tar.zst") == expected


def test_no_original_is_deleted_when_the_reopened_archive_is_missing_a_file(
    raw_root, day_dir, monkeypatch
):
    expected = originals(day_dir)
    real_write = compact.write_member
    first = True

    def drop_one(tar, original, size):
        nonlocal first
        if first:
            first = False
        else:
            real_write(tar, original, size)

    monkeypatch.setattr(compact, "write_member", drop_one)
    with pytest.raises(compact.CompactionError, match="missing"):
        compact.compact(raw_root, AFTER_GRACE)

    assert originals(day_dir) == expected


def test_no_original_is_deleted_when_an_archived_copy_has_a_different_sha256(
    raw_root, day_dir, monkeypatch
):
    expected = originals(day_dir)
    real_write = compact.write_member
    first = True

    def corrupt_one(tar, original, size):
        nonlocal first
        if first:
            first = False
            info = tarfile.TarInfo(original.name.removesuffix(".gz"))
            info.size = size + 1
            tar.addfile(info, io.BytesIO(gzip.decompress(original.read_bytes()) + b" "))
        else:
            real_write(tar, original, size)

    monkeypatch.setattr(compact, "write_member", corrupt_one)
    with pytest.raises(compact.CompactionError, match="changed"):
        compact.compact(raw_root, AFTER_GRACE)

    assert originals(day_dir) == expected


class Interrupted(Exception):
    pass


def interrupt_mid_temp_write(monkeypatch):
    real_write = compact.write_member

    def partial(tar, original, size):
        real_write(tar, original, size)
        raise Interrupted

    monkeypatch.setattr(compact, "write_member", partial)


def interrupt_before_verify(monkeypatch):
    def fail(path, expected, archive=None):
        raise Interrupted

    monkeypatch.setattr(compact, "verify_archive", fail)


def interrupt_before_rename(monkeypatch):
    def fail(temp, archive):
        raise Interrupted

    monkeypatch.setattr(compact, "publish", fail)


def interrupt_after_deleting(count):
    def install(monkeypatch):
        def partial(paths):
            for path in islice(paths, count):
                path.unlink()
            raise Interrupted

        monkeypatch.setattr(compact, "delete_originals", partial)

    return install


@pytest.mark.parametrize(
    "interrupt",
    [
        interrupt_mid_temp_write,
        interrupt_before_verify,
        interrupt_before_rename,
        interrupt_after_deleting(0),
        interrupt_after_deleting(1),
    ],
    ids=["mid-temp-write", "before-verify", "before-rename", "before-delete", "mid-delete"],
)
def test_an_interruption_at_any_step_leaves_every_original_recoverable_and_a_rerun_completes(
    raw_root, day_dir, interrupt
):
    expected = originals(day_dir)

    with pytest.MonkeyPatch.context() as patch:
        interrupt(patch)
        with pytest.raises(Interrupted):
            compact.compact(raw_root, AFTER_GRACE)
    assert recoverable(day_dir) == expected

    compact.compact(raw_root, AFTER_GRACE)
    assert sorted(path.name for path in day_dir.iterdir()) == ["2026-10-07T14.tar.zst"]
    assert archive_members(day_dir / "2026-10-07T14.tar.zst") == expected


def test_running_twice_produces_one_archive_per_hour_and_deletes_nothing_extra(raw_root, day_dir):
    add_original(day_dir, "2026-10-07T150000.000000Z")
    expected = originals(day_dir)

    compact.compact(raw_root, datetime(2026, 10, 7, 16, 5, tzinfo=UTC))
    first = {path.name: path.read_bytes() for path in day_dir.iterdir()}
    compact.compact(raw_root, datetime(2026, 10, 7, 16, 5, tzinfo=UTC))

    assert {path.name: path.read_bytes() for path in day_dir.iterdir()} == first
    assert sorted(first) == ["2026-10-07T14.tar.zst", "2026-10-07T15.tar.zst"]
    assert recoverable(day_dir) == expected


def test_late_files_are_verified_in_numbered_supplements_without_rewriting_published_archives(
    raw_root, day_dir
):
    expected = originals(day_dir)
    compact.compact(raw_root, AFTER_GRACE)
    archive = day_dir / "2026-10-07T14.tar.zst"
    published = {archive: (archive.read_bytes(), archive.stat().st_mtime_ns)}
    for number in (1, 2):
        late = add_original(day_dir, f"2026-10-07T145959.99999{number}Z", f"late {number}")
        late_expected = originals(day_dir)
        expected.update(late_expected)
        compact.compact(raw_root, AFTER_GRACE)
        assert not late.exists()
        supplement = day_dir / f"2026-10-07T14.late-{number}.tar.zst"
        assert archive_members(supplement) == late_expected
        for path, (content, mtime) in published.items():
            assert path.read_bytes() == content
            assert path.stat().st_mtime_ns == mtime
        published[supplement] = (supplement.read_bytes(), supplement.stat().st_mtime_ns)
    assert recoverable(day_dir) == expected
    assert sorted(day_dir.iterdir()) == sorted(published)


@pytest.mark.parametrize("damage", ["omit", "alter"])
def test_a_damaged_supplement_keeps_the_late_original_and_every_published_archive(
    raw_root, day_dir, monkeypatch, damage
):
    compact.compact(raw_root, AFTER_GRACE)
    archive = day_dir / "2026-10-07T14.tar.zst"
    old_archive = archive.read_bytes()
    late = add_original(day_dir, "2026-10-07T145959.999999Z", "late")
    late_bytes = late.read_bytes()

    def damage_member(tar, original, size):
        if damage == "alter":
            info = tarfile.TarInfo(original.name.removesuffix(".gz"))
            info.size = size + 1
            tar.addfile(info, io.BytesIO(gzip.decompress(original.read_bytes()) + b" "))

    monkeypatch.setattr(compact, "write_member", damage_member)
    with pytest.raises(compact.CompactionError):
        compact.compact(raw_root, AFTER_GRACE)

    assert archive.read_bytes() == old_archive
    assert late.read_bytes() == late_bytes
    assert not (day_dir / "2026-10-07T14.late-1.tar.zst").exists()


def test_files_in_the_current_hour_are_never_archived_or_deleted(raw_root, day_dir):
    current = add_original(day_dir, "2026-10-07T151500.000000Z")
    snapshot = {path.name: path.read_bytes() for path in day_dir.iterdir()}

    # 15:04:59 is in the 15:00 hour and inside the 14:00 hour's grace window.
    compact.compact(raw_root, datetime(2026, 10, 7, 15, 4, 59, tzinfo=UTC))
    assert {path.name: path.read_bytes() for path in day_dir.iterdir()} == snapshot

    compact.compact(raw_root, datetime(2026, 10, 7, 15, 59, 59, tzinfo=UTC))
    assert current.read_bytes() == snapshot[current.name]
    assert not (day_dir / "2026-10-07T15.tar.zst").exists()
    assert (day_dir / "2026-10-07T14.tar.zst").exists()


def test_a_corrupt_gzip_original_raises_and_deletes_nothing(raw_root, day_dir):
    bad = day_dir / "2026-10-07T145000.000000Z.json.gz"
    bad.write_bytes(b"not gzip")
    snapshot = {path.name: path.read_bytes() for path in day_dir.iterdir()}

    with pytest.raises(gzip.BadGzipFile):
        compact.compact(raw_root, AFTER_GRACE)

    assert {path.name: path.read_bytes() for path in day_dir.iterdir()} == snapshot


@pytest.mark.parametrize(
    "name", ["notes.txt", "2026-10-08T140000.000000Z.json.gz", "2026-10-07T14.tar.gz"]
)
def test_an_unexpected_file_in_a_date_directory_raises(raw_root, day_dir, name):
    (day_dir / name).write_bytes(b"x")
    snapshot = {path.name: path.read_bytes() for path in day_dir.iterdir()}

    with pytest.raises(compact.CompactionError):
        compact.compact(raw_root, AFTER_GRACE)

    assert {path.name: path.read_bytes() for path in day_dir.iterdir()} == snapshot


def test_no_new_hour_starts_after_the_budget_and_hours_are_globally_oldest_first(
    raw_root, day_dir, monkeypatch
):
    older = raw_root / "nws/2026-10-06"
    older.mkdir(parents=True)
    add_original(older, "2026-10-06T230000.000000Z")
    same_day_older = raw_root / "nws/2026-10-07"
    same_day_older.mkdir()
    add_original(same_day_older, "2026-10-07T130000.000000Z")
    add_original(day_dir, "2026-10-07T150000.000000Z")
    elapsed = 0
    calls = []
    real_hour = compact.compact_hour

    def consume_budget(db, directory, key, level, long_distance):
        nonlocal elapsed
        calls.append((key, directory.parent.name))
        real_hour(db, directory, key, level, long_distance)
        elapsed += 61

    monkeypatch.setattr(compact.clock, "monotonic", lambda: elapsed)
    monkeypatch.setattr(compact, "compact_hour", consume_budget)
    now = datetime(2026, 10, 7, 16, 5, tzinfo=UTC)
    compact.compact(raw_root, now, time_budget_seconds=120)
    assert calls == [("2026-10-06T23", "nws"), ("2026-10-07T13", "nws")]
    assert len(list(day_dir.glob("*.json.gz"))) == 4
    compact.compact(raw_root, now, time_budget_seconds=120)
    assert calls[2:] == [("2026-10-07T14", "kalshi"), ("2026-10-07T15", "kalshi")]
    assert not list(day_dir.glob("*.json.gz"))


@pytest.mark.parametrize("budget", [0, -1, float("inf"), float("nan")])
def test_invalid_budgets_raise_without_touching_originals(raw_root, day_dir, budget):
    snapshot = originals(day_dir)
    with pytest.raises(ValueError, match="budget"):
        compact.compact(raw_root, AFTER_GRACE, time_budget_seconds=budget)
    assert originals(day_dir) == snapshot


def test_publishing_uses_an_atomic_rename_only_after_verification(raw_root, day_dir, monkeypatch):
    expected = originals(day_dir)
    real_rename = compact.os.rename
    calls = []

    def inspect_rename(temp, final):
        assert not final.exists()
        assert originals(day_dir) == expected
        assert archive_members(temp) == expected
        calls.append((temp, final))
        real_rename(temp, final)

    monkeypatch.setattr(compact.os, "rename", inspect_rename)
    compact.compact(raw_root, AFTER_GRACE)
    assert len(calls) == 1
    assert not calls[0][0].exists()


@pytest.mark.parametrize("supplement", [False, True])
def test_a_process_killed_mid_archive_keeps_originals_and_the_next_run_cleans_its_temp_file(
    raw_root, day_dir, tmp_path, supplement
):
    published = {}
    if supplement:
        compact.compact(raw_root, AFTER_GRACE)
        published = {p: p.read_bytes() for p in day_dir.glob("*.tar.zst")}
        add_original(day_dir, "2026-10-07T145959.999999Z", "late")
    expected = originals(day_dir)
    marker = tmp_path / "writing"
    script = """
import os, signal, sys
from pathlib import Path
from datetime import UTC, datetime
from kwb.ops import compact
real_write = compact.write_member
def stop_mid_write(tar, original, size):
    real_write(tar, original, size)
    Path(sys.argv[2]).touch()
    os.kill(os.getpid(), signal.SIGSTOP)
compact.write_member = stop_mid_write
compact.compact(Path(sys.argv[1]), datetime(2026, 10, 7, 15, 5, tzinfo=UTC))
"""
    process = subprocess.Popen([sys.executable, "-c", script, str(raw_root), str(marker)])
    try:
        deadline = time.monotonic() + 10
        while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.exists(), "Child did not reach archive writing"
        process.kill()
        assert process.wait(timeout=10) == -signal.SIGKILL
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
    assert originals(day_dir) == expected
    temps = list(day_dir.glob(".*.tmp"))
    assert len(temps) == 1
    # A poisoned leftover proves the next run discarded rather than reused it.
    temps[0].write_bytes(b"stale partial frame")
    compact.compact(raw_root, AFTER_GRACE)
    assert not list(day_dir.glob(".*.tmp"))
    assert not list(day_dir.glob("*.json.gz"))
    assert not (raw_root / compact.INDEX_NAME).exists()
    for path, content in published.items():
        assert path.read_bytes() == content
    for name, content in expected.items():
        assert recoverable(day_dir)[name] == content


def test_retry_after_supplement_publication_reuses_it_without_creating_another_archive(
    raw_root, day_dir, monkeypatch
):
    compact.compact(raw_root, AFTER_GRACE)
    late = add_original(day_dir, "2026-10-07T145959.999999Z", "late")
    with monkeypatch.context() as patch:
        patch.setattr(compact, "delete_originals", lambda paths: None)
        compact.compact(raw_root, AFTER_GRACE)
    first = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in day_dir.glob("*.tar.zst")}
    compact.compact(raw_root, AFTER_GRACE)
    assert not late.exists()
    assert {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in day_dir.glob("*.tar.zst")} == first
    assert len(first) == 2


def test_a_changed_original_matching_a_published_member_raises_without_deletion(raw_root, day_dir):
    name = min(originals(day_dir)) + ".gz"
    compact.compact(raw_root, AFTER_GRACE)
    changed = day_dir / name
    changed.write_bytes(gzip.compress(b"changed"))
    with pytest.raises(compact.CompactionError, match="changed original"):
        compact.compact(raw_root, AFTER_GRACE)
    assert changed.exists()


def test_a_second_process_cannot_clean_temps_or_compact_while_the_lock_is_held(raw_root, day_dir):
    import fcntl

    temp = day_dir / ".2026-10-07T14.tar.zst.tmp"
    temp.write_bytes(b"in progress")
    snapshot = originals(day_dir)
    with (raw_root / compact.LOCK_NAME).open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert compact.compact(raw_root, AFTER_GRACE) is False
    assert temp.read_bytes() == b"in progress"
    assert originals(day_dir) == snapshot


def test_the_service_caps_cpu_and_yields_io_even_if_the_time_budget_is_removed():
    import configparser

    unit = configparser.ConfigParser(interpolation=None)
    unit.read(Path(__file__).parents[1] / "deploy/raw-compaction.service")
    service = unit["Service"]
    assert service["CPUQuota"] == "20%"
    assert service.getint("IOWeight") == 10
    assert service.getint("CPUWeight") == 20
    assert service.getint("Nice") == 10
    assert "--time-budget-seconds 120" in service["ExecStart"]
    assert "--zstd-level 9" in service["ExecStart"]
    assert "--long-distance" not in service["ExecStart"]


@pytest.mark.parametrize("shape", ["large-member", "many-members"])
def test_peak_rss_stays_below_128_mib_when_hour_bytes_or_member_count_grow(tmp_path, shape):
    roots = []
    for label, size, count in (
        ("small", 1024 * 1024, 3),
        (
            "large",
            256 * 1024 * 1024 if shape == "large-member" else 32,
            3 if shape == "large-member" else 24000,
        ),
    ):
        root = tmp_path / label
        day = root / "kalshi/2026-10-07"
        day.mkdir(parents=True)
        for number in range(count):
            original = day / f"2026-10-07T140000.{number:06d}Z.json.gz"
            with gzip.open(original, "wb", compresslevel=1) as output:
                remaining = size if number == 0 or shape == "many-members" else 32
                chunk = b"x" * min(64 * 1024, remaining)
                while remaining:
                    block = chunk[:remaining]
                    output.write(block)
                    remaining -= len(block)
        roots.append(root)
    script = """
import json, resource, sys
from datetime import UTC, datetime
from pathlib import Path
from kwb.ops import compact
root = Path(sys.argv[1])
compact.compact(root, datetime(2026, 10, 7, 15, 5, tzinfo=UTC), time_budget_seconds=120)
assert not list(root.glob("*/*/*.json.gz"))
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print(json.dumps(peak if sys.platform == "darwin" else peak * 1024))
"""
    peaks = []
    for root in roots:
        result = subprocess.run(
            [sys.executable, "-c", script, str(root)], capture_output=True, text=True, check=True
        )
        peaks.append(json.loads(result.stdout))
    print(f"{shape}: small_peak={peaks[0]} large_peak={peaks[1]} bytes")
    assert max(peaks) < 128 * 1024**2
    assert peaks[1] - peaks[0] < 16 * 1024**2


def test_originals_are_preserved_if_the_final_archive_changes_after_atomic_publication(
    raw_root, day_dir, monkeypatch
):
    expected = originals(day_dir)
    real_publish = compact.publish

    def corrupt_after_publication(temp, archive):
        real_publish(temp, archive)
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode="w") as tar:
            for name, content in expected.items():
                info = tarfile.TarInfo(name)
                info.size = len(content) + 1
                tar.addfile(info, io.BytesIO(content + b" "))
        archive.write_bytes(zstandard.ZstdCompressor().compress(data.getvalue()))

    monkeypatch.setattr(compact, "publish", corrupt_after_publication)
    with pytest.raises(compact.CompactionError, match="changed"):
        compact.compact(raw_root, AFTER_GRACE)
    assert originals(day_dir) == expected


def test_duplicate_members_in_a_published_archive_raise_and_preserve_late_originals(
    raw_root, day_dir
):
    compact.compact(raw_root, AFTER_GRACE)
    archive = day_dir / "2026-10-07T14.tar.zst"
    name, content = next(iter(archive_members(archive).items()))
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as tar:
        for _ in range(2):
            info = tarfile.TarInfo(name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    archive.write_bytes(zstandard.ZstdCompressor().compress(data.getvalue()))
    late = add_original(day_dir, "2026-10-07T145959.999999Z", "late")
    with pytest.raises(compact.CompactionError, match="duplicate"):
        compact.compact(raw_root, AFTER_GRACE)
    assert late.exists()


def test_the_cli_defaults_to_level_nine_without_ldm_and_a_120_second_budget(monkeypatch):
    calls = []
    monkeypatch.setattr(compact, "compact", lambda *args, **kwargs: calls.append(kwargs))
    compact.main([])
    assert calls == [{"time_budget_seconds": 120, "level": 9, "long_distance": False}]
