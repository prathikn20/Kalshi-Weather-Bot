import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts/benchmark_compaction.py"
spec = importlib.util.spec_from_file_location("benchmark_compaction", SCRIPT)
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


@pytest.mark.parametrize("wall,peak,passes", [(10, 40, True), (61, 40, False), (10, 129, False)])
def test_capacity_requires_twice_incoming_load_and_the_rss_bound_using_the_slowest_vm_trial(
    wall, peak, passes
):
    trials = []
    for level in (3, 9, 19):
        for ldm in (False, True):
            for measured_wall in (1, wall):
                trials.append(
                    {
                        "level": level,
                        "ldm": ldm,
                        "compaction_wall_seconds": measured_wall,
                        "compaction_cpu_seconds": measured_wall / 5,
                        "peak_rss_bytes": peak * 1024**2,
                    }
                )
    inputs = {"kalshi": {"json_bytes": 100_000_000}, "nws": {"json_bytes": 6_000_000}}
    report = benchmark.capacity_report(trials, inputs, 120, 2_300_000_000, 70_000_000_000)
    assert all(row["passes_2x_and_rss"] is passes for row in report["configs"])
    assert (report["recommended"] is not None) is passes
    for row in report["configs"]:
        assert row["worst_pair_wall_seconds"] == wall
        net = row["capacity_json_bytes_per_day"] - row["incoming_json_bytes_per_day"]
        assert row["drain_days"] == (70_000_000_000 / net if net > 0 else None)


def test_the_benchmark_exercises_all_six_configs_without_modifying_input_or_claiming_mac_capacity(
    tmp_path,
):
    fixture = Path(__file__).parent / "fixtures/kalshi_raw/2026-10-07"
    raw = tmp_path / "raw"
    for source in ("kalshi", "nws"):
        directory = raw / source / "2026-11-09"
        directory.mkdir(parents=True)
        for index, original in enumerate(sorted(fixture.glob("*.gz"))):
            (directory / f"2026-11-09T150000.{index:06d}Z.json.gz").write_bytes(
                original.read_bytes()
            )
    before = {str(p.relative_to(raw)): p.read_bytes() for p in raw.glob("*/*/*")}
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--local",
            "--raw-root",
            str(raw),
            "--repeats",
            "1",
            "--work-dir",
            str(tmp_path / "work"),
            "--output",
            str(tmp_path / "report.json"),
        ],
        text=True,
        capture_output=True,
        check=True,
    )
    report = json.loads(result.stdout)
    assert report == json.loads((tmp_path / "report.json").read_text())
    events = [
        json.loads(line) for line in (tmp_path / "report.trials.jsonl").read_text().splitlines()
    ]
    assert [event["trial"] for event in events if event["event"] == "trial"] == report["trials"]
    assert "Benchmark level=" not in result.stdout
    assert "Benchmark level=" in result.stderr
    assert report["hour"] == "2026-11-09T15"
    assert {(row["level"], row["ldm"]) for row in report["trials"]} == {
        (level, ldm) for level in (3, 9, 19) for ldm in (False, True)
    }
    assert len(report["trials"]) == 6
    assert "capacity" not in report
    for row in report["trials"]:
        assert "compaction_cpu_seconds" not in row
        assert "compaction_wall_seconds" not in row
        assert row["peak_rss_bytes"] > 0
        for source, data in row["sources"].items():
            assert data["archive_bytes"] > 0
            assert data["json_to_archive_ratio"] == (
                report["inputs"][source]["json_bytes"] / data["archive_bytes"]
            )
    assert {str(p.relative_to(raw)): p.read_bytes() for p in raw.glob("*/*/*")} == before
    checkpoint = (tmp_path / "report.json").read_bytes()
    journal = (tmp_path / "report.trials.jsonl").read_bytes()
    rerun = subprocess.run(result.args, text=True, capture_output=True)
    assert rerun.returncode != 0
    assert "choose a fresh --output path" in rerun.stderr
    assert (tmp_path / "report.json").read_bytes() == checkpoint
    assert (tmp_path / "report.trials.jsonl").read_bytes() == journal


def test_the_vm_benchmark_refuses_to_claim_capacity_without_the_required_quota(monkeypatch):
    monkeypatch.setattr(benchmark.sys, "platform", "darwin")
    with pytest.raises(ValueError, match="Linux"):
        benchmark.quota_state()


def test_capacity_accounts_for_backlog_discovery_before_starting_complete_hour_pairs():
    trials = [
        {
            "level": level,
            "ldm": ldm,
            "compaction_wall_seconds": 10,
            "compaction_cpu_seconds": 2,
            "peak_rss_bytes": 40 * 1024**2,
        }
        for level in (3, 9, 19)
        for ldm in (False, True)
    ]
    inputs = {"kalshi": {"json_bytes": 100_000_000}, "nws": {"json_bytes": 6_000_000}}
    report = benchmark.capacity_report(
        trials, inputs, 120, 2_300_000_000, 70_000_000_000, discovery_seconds=100
    )
    assert report["recommended"] is None
    assert all(row["complete_pairs_per_run"] == 0 for row in report["configs"])
    assert all(row["minimum_budget_for_2x_seconds"] == 134 for row in report["configs"])


def test_backlog_drain_time_accounts_for_many_small_files_as_well_as_json_bytes():
    trials = [
        {
            "level": level,
            "ldm": ldm,
            "compaction_wall_seconds": 10,
            "compaction_cpu_seconds": 2,
            "peak_rss_bytes": 40 * 1024**2,
        }
        for level in (3, 9, 19)
        for ldm in (False, True)
    ]
    inputs = {
        "kalshi": {"json_bytes": 100_000_000, "files": 3900},
        "nws": {"json_bytes": 6_000_000, "files": 42},
    }
    report = benchmark.capacity_report(
        trials, inputs, 120, 2_300_000_000, 1_000_000_000, backlog_files=50_000_000
    )
    for row in report["configs"]:
        assert row["drain_days"] == row["drain_days_by_file_count"]
        assert row["drain_days"] > row["drain_days_by_json_bytes"]


def test_backlog_estimation_uses_only_gzip_file_sizes_and_labels_the_estimate(
    tmp_path, monkeypatch
):
    raw = tmp_path / "raw"
    for source in ("kalshi", "nws"):
        directory = raw / source / "2026-11-09"
        directory.mkdir(parents=True)
        (directory / "2026-11-09T150000.000000Z.json.gz").write_bytes(b"x" * 11)
        (directory / "2026-11-09T160000.000000Z.json.gz").write_bytes(b"x" * 100)
        (directory / "2026-11-09T15.tar.zst").write_bytes(b"already archived")

    def forbidden(*args, **kwargs):
        pytest.fail("Backlog estimation must not open, decompress or hash payloads")

    monkeypatch.setattr(benchmark.gzip, "open", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)
    monkeypatch.setattr(benchmark.compact, "digest_stream", forbidden)
    inputs = {
        "kalshi": {"gzip_bytes": 10, "json_bytes": 101},
        "nws": {"gzip_bytes": 10, "json_bytes": 201},
    }
    result = benchmark.backlog_bytes(
        raw, benchmark.datetime(2026, 11, 9, 16, 5, tzinfo=benchmark.UTC), inputs
    )
    assert result["files"] == 2
    assert result["gzip_bytes"] == 22
    assert result["estimated_json_bytes"] == 112 + 222
    assert "json_bytes" not in result
    assert result["method"] == "gzip_file_sizes_scaled_by_benchmark_hour_per_source"


def test_capacity_keeps_the_selected_level_nine_instead_of_substituting_level_three():
    trials = [
        {
            "level": level,
            "ldm": ldm,
            "compaction_wall_seconds": 20 if level == 9 else 10,
            "compaction_cpu_seconds": 4 if level == 9 else 2,
            "peak_rss_bytes": 50 * 1024**2,
        }
        for level in (3, 9, 19)
        for ldm in (False, True)
    ]
    inputs = {"kalshi": {"json_bytes": 100_000_000}, "nws": {"json_bytes": 6_000_000}}
    result = benchmark.capacity_report(trials, inputs, 120, 2_300_000_000, 1_000_000_000)
    assert result["recommended"]["level"] == 9
    assert result["recommended"]["ldm"] is False
    for trial in trials:
        if trial["level"] == 9:
            trial["compaction_wall_seconds"] = 120
    result = benchmark.capacity_report(trials, inputs, 120, 2_300_000_000, 1_000_000_000)
    assert result["recommended"] is None
    assert result["status"] == "FAIL_DO_NOT_DEPLOY"


def test_each_trial_is_fsynced_and_checkpointed_before_starting_the_next_worker(
    tmp_path, monkeypatch
):
    inputs = {s: {"json_bytes": 100, "gzip_bytes": 10, "files": 1} for s in benchmark.SOURCES}
    monkeypatch.setattr(benchmark, "measure_inputs", lambda *args: inputs)
    monkeypatch.setattr(benchmark.platform, "platform", lambda: "test-host")
    output = tmp_path / "report.json"
    journal = output.with_suffix(".trials.jsonl")
    calls = 0
    synced = []
    synced_before_trial = 0
    real_fsync = os.fsync

    def record_fsync(fd):
        synced.append(os.fstat(fd).st_ino)
        real_fsync(fd)

    monkeypatch.setattr(benchmark.os, "fsync", record_fsync)

    def fake_worker(*args, **kwargs):
        nonlocal calls, synced_before_trial
        calls += 1
        if calls == 1:
            synced_before_trial = synced.count(journal.stat().st_ino)
        if calls == 2:
            report = json.loads(output.read_text())
            assert len(report["trials"]) == 1
            events = [json.loads(line) for line in journal.read_text().splitlines()]
            assert [event["trial"] for event in events if event["event"] == "trial"] == report[
                "trials"
            ]
            assert synced.count(journal.stat().st_ino) == synced_before_trial + 1
            raise RuntimeError("second worker interrupted")
        return subprocess.CompletedProcess(
            [],
            0,
            stdout=json.dumps(
                {
                    "level": 3,
                    "ldm": False,
                    "peak_rss_bytes": 100,
                    "sources": {s: {"archive_bytes": 5} for s in benchmark.SOURCES},
                }
            ),
        )

    monkeypatch.setattr(benchmark.subprocess, "run", fake_worker)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT),
            "--local",
            "--repeats",
            "1",
            "--output",
            str(output),
            "--work-dir",
            str(tmp_path / "work"),
        ],
    )
    with pytest.raises(RuntimeError, match="second worker interrupted"):
        benchmark.main()
    assert json.loads(output.read_text())["status"] == "failed"
    assert calls == 2


def test_killing_a_later_benchmark_worker_keeps_every_completed_trial(tmp_path):
    output = tmp_path / "report.json"
    marker = tmp_path / "second-worker"
    script = """
import importlib.util, json, os, signal, subprocess, sys
from pathlib import Path
spec = importlib.util.spec_from_file_location("benchmark", sys.argv[1])
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)
b.platform.platform = lambda: "test-host"
b.measure_inputs = lambda *args: {s: {"json_bytes": 100, "gzip_bytes": 10} for s in b.SOURCES}
calls = 0
marker = Path(sys.argv[3])
output = sys.argv[2]
def fake_worker(*args, **kwargs):
    global calls
    calls += 1
    if calls == 2:
        marker.touch()
        os.kill(os.getpid(), signal.SIGSTOP)
    return subprocess.CompletedProcess([], 0, stdout=json.dumps({
        "level": 3, "ldm": False, "peak_rss_bytes": 100,
        "sources": {s: {"archive_bytes": 5} for s in b.SOURCES},
    }))
b.subprocess.run = fake_worker
sys.argv = [sys.argv[1], "--local", "--repeats", "1", "--output", output,
            "--work-dir", sys.argv[4]]
b.main()
"""
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            script,
            str(SCRIPT),
            str(output),
            str(marker),
            str(tmp_path / "work"),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 10
        while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.exists()
        process.kill()
        assert process.wait(timeout=10) == -signal.SIGKILL
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
    report = json.loads(output.read_text())
    assert len(report["trials"]) == 1
    events = [
        json.loads(line) for line in output.with_suffix(".trials.jsonl").read_text().splitlines()
    ]
    assert [e["trial"] for e in events if e["event"] == "trial"] == report["trials"]


def test_level_nine_capacity_reproduces_the_vm_result_and_meets_twice_incoming_load():
    directory = SCRIPT.parents[1] / "benchmarks"
    measured = json.loads((directory / "compaction-vm-2026-11-09T15-report.json").read_text())
    recorded = json.loads((directory / "compaction-capacity-level9.json").read_text())
    capacity = benchmark.capacity_report(
        measured["trials"],
        measured["inputs"],
        120,
        2_300_000_000,
        measured["backlog"]["json_bytes"],
        measured["discovery"]["reserved_discovery_wall_seconds"],
        measured["backlog"]["files"],
        selected_level=9,
    )
    assert capacity == recorded["capacity"]
    selected = capacity["recommended"]
    assert (selected["level"], selected["ldm"]) == (9, False)
    assert selected["complete_pairs_per_run"] == 3
    assert selected["capacity_json_bytes_per_day"] >= 2 * selected["incoming_json_bytes_per_day"]
