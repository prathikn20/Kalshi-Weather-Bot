import gzip
import io
import json
import shutil
import tarfile
from datetime import UTC, datetime
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
    real_build = compact.build_archive

    def drop_one(members):
        return real_build(dict(sorted(members.items())[1:]))

    monkeypatch.setattr(compact, "build_archive", drop_one)
    with pytest.raises(compact.CompactionError, match="missing"):
        compact.compact(raw_root, AFTER_GRACE)

    assert originals(day_dir) == expected


def test_no_original_is_deleted_when_an_archived_copy_has_a_different_sha256(
    raw_root, day_dir, monkeypatch
):
    expected = originals(day_dir)
    real_build = compact.build_archive

    def corrupt_one(members):
        name = min(members)
        return real_build({**members, name: members[name] + b" "})

    monkeypatch.setattr(compact, "build_archive", corrupt_one)
    with pytest.raises(compact.CompactionError, match="changed"):
        compact.compact(raw_root, AFTER_GRACE)

    assert originals(day_dir) == expected


class Interrupted(Exception):
    pass


def interrupt_mid_temp_write(monkeypatch):
    def partial(path, data):
        path.write_bytes(data[: len(data) // 2])
        raise Interrupted

    monkeypatch.setattr(compact, "write_temp", partial)


def interrupt_before_verify(monkeypatch):
    def fail(path, expected):
        raise Interrupted

    monkeypatch.setattr(compact, "verify_archive", fail)


def interrupt_before_rename(monkeypatch):
    def fail(temp, archive):
        raise Interrupted

    monkeypatch.setattr(compact, "publish", fail)


def interrupt_after_deleting(count):
    def install(monkeypatch):
        def partial(paths):
            for path in paths[:count]:
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


def test_a_late_file_for_an_archived_hour_is_merged_into_the_single_archive_before_deletion(
    raw_root, day_dir
):
    expected = originals(day_dir)
    compact.compact(raw_root, AFTER_GRACE)
    late = add_original(day_dir, "2026-10-07T145959.999999Z", "late")
    expected.update(originals(day_dir))

    compact.compact(raw_root, AFTER_GRACE)

    assert not late.exists()
    assert sorted(path.name for path in day_dir.iterdir()) == ["2026-10-07T14.tar.zst"]
    assert archive_members(day_dir / "2026-10-07T14.tar.zst") == expected


@pytest.mark.parametrize("damage", ["omit", "alter"])
def test_a_union_rebuild_that_would_drop_or_alter_an_existing_archive_member_raises_and_keeps_the_old_archive(  # noqa: E501
    raw_root, day_dir, monkeypatch, damage
):
    compact.compact(raw_root, AFTER_GRACE)
    archive = day_dir / "2026-10-07T14.tar.zst"
    old_archive = archive.read_bytes()
    old_names = set(archive_members(archive))
    late = add_original(day_dir, "2026-10-07T145959.999999Z", "late")
    late_bytes = late.read_bytes()
    real_build = compact.build_archive

    def damage_old_member(members):
        victim = min(old_names)
        if damage == "omit":
            return real_build({k: v for k, v in members.items() if k != victim})
        return real_build({**members, victim: members[victim] + b" "})

    monkeypatch.setattr(compact, "build_archive", damage_old_member)
    with pytest.raises(compact.CompactionError):
        compact.compact(raw_root, AFTER_GRACE)

    assert archive.read_bytes() == old_archive
    assert late.read_bytes() == late_bytes


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
