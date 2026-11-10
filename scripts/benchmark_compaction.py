"""Read-only input benchmark; all compaction/deletion happens in disposable copies.

Run --local for ratios/RSS only. --vm requires Linux with an effective 20% CPU
quota and emits CPU/wall measurements, a conservative capacity gate, and backlog
drain days. Never enables the production timer or changes the production config.
"""

import argparse
import gzip
import hashlib
import json
import math
import os
import platform
import resource
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import zstandard

from kwb.ops import compact

HOUR = "2026-11-09T15"
SOURCES = ("kalshi", "nws")


def cpu_seconds():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


def quota_state():
    if sys.platform != "linux":
        raise ValueError("--vm requires Linux and cgroup v2 CPUQuota=20%")
    group = next(
        line.split("::", 1)[1]
        for line in Path("/proc/self/cgroup").read_text().splitlines()
        if line.startswith("0::")
    )
    directory = Path("/sys/fs/cgroup") / group.lstrip("/")
    quota, period = (directory / "cpu.max").read_text().split()
    if quota == "max" or not math.isclose(int(quota) / int(period), 0.2):
        raise ValueError("Benchmark must run with effective CPUQuota=20% (cpu.max)")
    if (directory / "cpu.weight").read_text().strip() != "20":
        raise ValueError("Benchmark must run with CPUWeight=20")
    if (directory / "io.weight").read_text().strip() != "default 10":
        raise ValueError("Benchmark must run with IOWeight=10")
    if os.getpriority(os.PRIO_PROCESS, 0) != 10:
        raise ValueError("Benchmark must run with nice priority 10")
    return {
        "nice": 10,
        "cpu.max": f"{quota} {period}",
        "io.weight": (directory / "io.weight").read_text().strip(),
        "cpu.weight": (directory / "cpu.weight").read_text().strip(),
        "cgroup": group,
    }


def measure_inputs(raw_root, hour):
    inputs = {}
    for source in SOURCES:
        directory = raw_root / source / hour[:10]
        files = sorted(directory.glob(f"{hour}*.json.gz"))
        if not files:
            raise ValueError(f"No benchmark input for {source}/{hour}")
        fingerprint = hashlib.sha256()
        gzip_bytes = json_bytes = 0
        for path in files:
            if compact.ORIGINAL.fullmatch(path.name) is None:
                raise ValueError(f"Invalid raw filename: {path}")
            fingerprint.update(path.name.encode())
            with path.open("rb") as stream:
                _, digest = compact.digest_stream(stream)
            fingerprint.update(digest.encode())
            gzip_bytes += path.stat().st_size
            with gzip.open(path, "rb") as stream:
                size, _ = compact.digest_stream(stream)
            json_bytes += size
        inputs[source] = {
            "files": len(files),
            "gzip_bytes": gzip_bytes,
            "json_bytes": json_bytes,
            "sha256_manifest": fingerprint.hexdigest(),
            "first": files[0].name,
            "last": files[-1].name,
        }
    return inputs


def worker(job_path):
    job = json.loads(Path(job_path).read_text())
    if job["vm"]:
        quota_state()
    with tempfile.TemporaryDirectory(prefix="kwb-bench-", dir=job["work_dir"]) as work:
        raw = Path(work) / "raw"
        for source in SOURCES:
            directory = raw / source / job["hour"][:10]
            directory.mkdir(parents=True)
            for path in (Path(job["raw_root"]) / source / job["hour"][:10]).glob(
                f"{job['hour']}*.json.gz"
            ):
                shutil.copyfile(path, directory / path.name)
        builds = {}
        original_build = compact.build_archive

        def timed_build(temp, db, archive, level, long_distance):
            cpu = cpu_seconds()
            wall = time.monotonic()
            original_build(temp, db, archive, level, long_distance)
            builds[archive.parent.parent.name] = {
                "compression_cpu_seconds": cpu_seconds() - cpu,
                "compression_wall_seconds": time.monotonic() - wall,
            }

        compact.build_archive = timed_build
        start_cpu = cpu_seconds()
        start_wall = time.monotonic()
        compact.compact(
            raw,
            datetime.fromisoformat(job["hour"] + ":00:00+00:00") + timedelta(hours=2),
            time_budget_seconds=86400,
            level=job["level"],
            long_distance=job["ldm"],
        )
        result = {
            "level": job["level"],
            "ldm": job["ldm"],
            "compaction_cpu_seconds": cpu_seconds() - start_cpu,
            "compaction_wall_seconds": time.monotonic() - start_wall,
            "sources": {},
        }
        for source in SOURCES:
            archive = raw / source / job["hour"][:10] / f"{job['hour']}.tar.zst"
            result["sources"][source] = {"archive_bytes": archive.stat().st_size, **builds[source]}
            if list(archive.parent.glob("*.json.gz")):
                raise RuntimeError("Benchmark compaction did not complete")
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        result["peak_rss_bytes"] = peak if sys.platform == "darwin" else peak * 1024
        if not job["vm"]:
            for field in ("compaction_cpu_seconds", "compaction_wall_seconds"):
                del result[field]
            for values in result["sources"].values():
                for field in ("compression_cpu_seconds", "compression_wall_seconds"):
                    del values[field]
        return result


def backlog_bytes(raw_root, now, inputs):
    """Stat eligible gzip files; estimate JSON using the measured per-source ratios."""
    sources = {}
    for source in SOURCES:
        sample = inputs[source]
        if sample["gzip_bytes"] <= 0 or sample["json_bytes"] <= 0:
            raise ValueError(f"Cannot estimate {source} backlog from an empty benchmark input")
        files = gzip_bytes = 0
        for directory in (raw_root / source).iterdir():
            with os.scandir(directory) as paths:
                for path in paths:
                    match = compact.ORIGINAL.fullmatch(path.name)
                    if match is None:
                        if compact.ARCHIVE.fullmatch(path.name) or compact.TEMP.fullmatch(
                            path.name
                        ):
                            continue
                        raise ValueError(f"Invalid raw filename: {path.path}")
                    if not path.is_file(follow_symlinks=False) or match[1] != directory.name:
                        raise ValueError(f"Invalid raw entry: {path.path}")
                    end = (
                        compact.hour_start(match[1], match[2]) + timedelta(hours=1) + compact.GRACE
                    )
                    if end > now:
                        continue
                    files += 1
                    gzip_bytes += path.stat(follow_symlinks=False).st_size
        # Round up using integer arithmetic; expansion ratios vary across historical data.
        estimated = (gzip_bytes * sample["json_bytes"] + sample["gzip_bytes"] - 1) // sample[
            "gzip_bytes"
        ]
        sources[source] = {
            "files": files,
            "gzip_bytes": gzip_bytes,
            "estimated_json_bytes": estimated,
            "json_to_gzip_ratio": sample["json_bytes"] / sample["gzip_bytes"],
        }
    return {
        "estimated_json_bytes": sum(row["estimated_json_bytes"] for row in sources.values()),
        "gzip_bytes": sum(row["gzip_bytes"] for row in sources.values()),
        "files": sum(row["files"] for row in sources.values()),
        "sources": sources,
        "method": "gzip_file_sizes_scaled_by_benchmark_hour_per_source",
        "as_of": now.isoformat(),
    }


def measure_discovery(raw_root, work_dir):
    """Read-only timing of the actual backlog's indexing cost, under the VM quota."""
    with tempfile.TemporaryDirectory(prefix="kwb-index-bench-", dir=work_dir) as work:
        db = sqlite3.connect(Path(work) / "index.sqlite")
        try:
            compact.initialize_index(db)
            start = time.monotonic()
            for source in SOURCES:
                for directory in (raw_root / source).iterdir():
                    if not directory.is_dir():
                        raise ValueError(f"Unexpected raw entry: {directory}")
                    db.execute(
                        "INSERT INTO directories VALUES (?,?)", (str(directory), directory.name)
                    )
            directory_seconds = time.monotonic() - start
            days = []
            for (day,) in db.execute("SELECT DISTINCT day FROM directories ORDER BY day"):
                start = time.monotonic()
                compact.inventory_day(db, day, datetime.now(UTC), cleanup_temps=False)
                elapsed = time.monotonic() - start
                originals = db.execute(
                    "SELECT count(*) FROM entries WHERE kind='original'"
                ).fetchone()[0]
                days.append(
                    {"day": day, "inventory_wall_seconds": elapsed, "original_files": originals}
                )
            # At most 24 pairs/run in the conservative model: at most two raw days.
            # Include every archive-only day, since those may precede the backlog.
            overhead = (
                directory_seconds
                + sum(row["inventory_wall_seconds"] for row in days if not row["original_files"])
                + 2 * max((row["inventory_wall_seconds"] for row in days), default=0)
            )
            return {
                "days": days,
                "directory_wall_seconds": directory_seconds,
                "reserved_discovery_wall_seconds": overhead,
            }
        finally:
            db.close()


def capacity_report(
    trials,
    inputs,
    budget,
    incoming_kalshi,
    backlog,
    discovery_seconds=0,
    backlog_files=0,
    selected_level=compact.ZSTD_LEVEL,
):
    """Use worst observed complete pair time; do not rely on final-hour overruns."""
    incoming = incoming_kalshi + inputs["nws"]["json_bytes"] * 24
    pair_bytes = sum(row["json_bytes"] for row in inputs.values())
    pair_files = sum(row.get("files", 0) for row in inputs.values())
    incoming_files = pair_files * 24
    rows = []
    for level in (3, 9, 19):
        for ldm in (False, True):
            selected = [row for row in trials if row["level"] == level and row["ldm"] == ldm]
            worst_wall = max(row["compaction_wall_seconds"] for row in selected)
            # Reserve 10% of each run for backlog discovery and load variation.
            pairs_per_run = min(
                24, max(0, math.floor((budget * 0.9 - discovery_seconds) / worst_wall))
            )
            per_day = pairs_per_run * 24 * pair_bytes
            peak = max(row["peak_rss_bytes"] for row in selected)
            passes = per_day >= 2 * incoming and peak < 128 * 1024**2
            net = per_day - incoming
            files_per_day = pairs_per_run * 24 * pair_files
            net_files = files_per_day - incoming_files
            byte_drain = backlog / net if net > 0 else None
            file_drain = backlog_files / net_files if net_files > 0 else None
            drain = max(byte_drain, file_drain or 0) if byte_drain is not None else None
            rows.append(
                {
                    "level": level,
                    "ldm": ldm,
                    "worst_pair_wall_seconds": worst_wall,
                    "worst_pair_cpu_seconds": max(r["compaction_cpu_seconds"] for r in selected),
                    "json_bytes_per_wall_second": pair_bytes / worst_wall,
                    "complete_pairs_per_run": pairs_per_run,
                    "capacity_json_bytes_per_day": per_day,
                    "incoming_json_bytes_per_day": incoming,
                    "passes_2x_and_rss": passes,
                    "capacity_files_per_day": files_per_day,
                    "incoming_files_per_day": incoming_files,
                    "drain_days_by_json_bytes": byte_drain,
                    "drain_days_by_file_count": file_drain,
                    "drain_days": drain,
                    "minimum_budget_for_2x_seconds": math.ceil(
                        (
                            math.ceil(2 * incoming / (24 * pair_bytes)) * worst_wall
                            + discovery_seconds
                        )
                        / 0.9
                    ),
                }
            )
    # Report the approved production choice, never silently substitute another level.
    recommended = next(
        (
            r
            for r in rows
            if r["level"] == selected_level and not r["ldm"] and r["passes_2x_and_rss"]
        ),
        None,
    )
    return {
        "configs": rows,
        "selected_level": selected_level,
        "selected_ldm": False,
        "recommended": recommended,
        "status": "PASS_REVIEW_BEFORE_DEPLOY" if recommended else "FAIL_DO_NOT_DEPLOY",
    }


def save_report(path, report):
    """Durable, atomic checkpoints: an interrupted run keeps its last complete report."""
    temp = path.with_name(path.name + ".tmp")
    with temp.open("w") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)
    compact.fsync_dir(path.parent)


def record_event(journal, event):
    journal.write(json.dumps(event) + "\n")
    journal.flush()
    os.fsync(journal.fileno())


def run_benchmark(args, report, journal):
    raw_root = args.raw_root.resolve()
    inputs = measure_inputs(raw_root, args.hour)
    report["inputs"] = inputs
    record_event(journal, {"event": "inputs", "inputs": inputs})
    save_report(args.output, report)
    with tempfile.TemporaryDirectory(prefix="kwb-bench-jobs-", dir=args.work_dir) as jobs:
        for level in (3, 9, 19):
            for ldm in (False, True):
                params = compact.compression_parameters(level, ldm)
                for repeat in range(args.repeats):
                    print(f"Benchmark level={level} ldm={ldm} repeat={repeat + 1}", file=sys.stderr)
                    job = Path(jobs) / "job.json"
                    job.write_text(
                        json.dumps(
                            {
                                "raw_root": str(raw_root),
                                "hour": args.hour,
                                "work_dir": str(args.work_dir.resolve()),
                                "vm": args.vm,
                                "level": level,
                                "ldm": ldm,
                            }
                        )
                    )
                    result = subprocess.run(
                        [sys.executable, str(Path(__file__).resolve()), "--worker", str(job)],
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    row = json.loads(result.stdout)
                    row["window_log"] = params.window_log
                    for source, data in row["sources"].items():
                        data["json_to_archive_ratio"] = (
                            inputs[source]["json_bytes"] / data["archive_bytes"]
                        )
                        data["gzip_to_archive_ratio"] = (
                            inputs[source]["gzip_bytes"] / data["archive_bytes"]
                        )
                    report["trials"].append(row)
                    record_event(journal, {"event": "trial", "repeat": repeat + 1, "trial": row})
                    save_report(args.output, report)
    if args.vm:
        print("Measuring discovery and estimating backlog from gzip file sizes", file=sys.stderr)
        report["discovery"] = measure_discovery(raw_root, args.work_dir)
        save_report(args.output, report)
        report["backlog"] = backlog_bytes(raw_root, datetime.now(UTC), inputs)
        save_report(args.output, report)
        report["capacity"] = capacity_report(
            report["trials"],
            inputs,
            args.budget_seconds,
            args.incoming_kalshi_json_bytes_per_day,
            report["backlog"]["estimated_json_bytes"],
            report["discovery"]["reserved_discovery_wall_seconds"],
            report["backlog"]["files"],
            selected_level=args.selected_level,
        )
    report["status"] = "complete"
    save_report(args.output, report)
    record_event(journal, {"event": "complete"})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--local", action="store_true")
    mode.add_argument("--vm", action="store_true")
    parser.add_argument("--raw-root", type=Path, default=Path("raw"))
    parser.add_argument("--hour", default=HOUR)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--budget-seconds", type=float, default=120)
    parser.add_argument("--incoming-kalshi-json-bytes-per-day", type=int, default=2_300_000_000)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument(
        "--selected-level", type=int, choices=(3, 9, 19), default=compact.ZSTD_LEVEL
    )
    parser.add_argument("--output", type=Path, default=Path("benchmarks/compaction-results.json"))
    args = parser.parse_args()
    if args.repeats <= 0 or not math.isfinite(args.budget_seconds) or args.budget_seconds <= 0:
        parser.error("repeats and budget must be positive and finite")
    if args.incoming_kalshi_json_bytes_per_day <= 0:
        parser.error("incoming load must be positive")
    datetime.strptime(args.hour, "%Y-%m-%dT%H").replace(tzinfo=UTC)
    limits = quota_state() if args.vm else None
    args.work_dir.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    journal_path = args.output.with_suffix(".trials.jsonl")
    if args.output.exists() or journal_path.exists():
        parser.error("output or trial journal already exists; choose a fresh --output path")
    report = {
        "status": "running",
        "mode": "vm" if args.vm else "local_ratios_and_rss_only",
        "hour": args.hour,
        "machine": platform.platform(),
        "python": platform.python_version(),
        "zstandard": zstandard.__version__,
        "zstd": zstandard.ZSTD_VERSION,
        "limits": limits,
        "budget_seconds": args.budget_seconds,
        "selected_level": args.selected_level,
        "selected_ldm": False,
        "ldm_window_log": 27,
        "threads": 0,
        "trials": [],
    }
    with journal_path.open("x") as journal:
        record_event(journal, {"event": "start", "run": report})
        compact.fsync_dir(journal_path.parent)
        save_report(args.output, report)
        try:
            run_benchmark(args, report, journal)
        except (Exception, KeyboardInterrupt) as exc:
            report["status"] = "failed"
            report["error"] = f"{type(exc).__name__}: {exc}"
            save_report(args.output, report)
            raise
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--worker":
        print(json.dumps(worker(sys.argv[2])))
    else:
        main()
