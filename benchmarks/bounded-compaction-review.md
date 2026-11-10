# Bounded compaction — measured configuration and validation

The approved configuration is zstd level **9 without LDM**, a **120-second wall-clock admission
budget per hourly run**, CPUQuota=20%, CPUWeight=20, IOWeight=10, and nice 10. Deployment and
enabling the production timer are separate from this PR.

## Measured VM capacity

Inputs are the complete **2026-11-09 15:00 UTC hour**, supplied under raw/ for both sources:
3,901 Kalshi files (96,921,020 JSON bytes / 8,911,181 gzip bytes) and 42 NWS files
(6,257,120 JSON bytes / 228,901 gzip bytes). No October 7 partial hour was benchmarked.
Historical real raw fixtures remain in the safety tests.

The VM ran Python 3.12.15, zstandard 0.25.0 / libzstd 1.5.7 under cpu.max=20000/100000,
cpu.weight=20, io.weight=default 10, nice=10, in system.slice. Input fingerprints match
[the Mac run](compaction-local-2026-11-09T15.json). The [normalized VM report](compaction-vm-2026-11-09T15-report.json)
removes only the leading stderr progress lines from the supplied report; measurement values
are preserved, including its original level-3 recommendation. The separate
[level-9 recalculation](compaction-capacity-level9.json) records the approved production choice.

Each row below uses the worst of three fresh worker processes. CPU/wall cover the entire
compaction pipeline: gzip sizing/hash pass, streamed archive write, both verification passes,
fsync, publication and deletion. They exclude preparing disposable input copies. RSS covers
the whole worker, including preparation and native zstd memory. Compression-only CPU per source
and both JSON/archive and gzip/archive ratios are also in the full VM report.

| Level | LDM | Kalshi archive bytes | JSON ratio | gzip ratio | NWS archive bytes | JSON ratio | gzip ratio | Full CPU s | Wall s | Peak RSS MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 3 | no | 3,697,528 | 26.21× | 2.41× | 196,002 | 31.92× | 1.17× | 3.979 | 19.902 | 49.75 |
| 3 | yes | 1,923,853 | 50.38× | 4.63× | 98,103 | 63.78× | 2.33× | 5.132 | 25.617 | 130.33 |
| 9 | no | 1,968,613 | 49.23× | 4.53× | 79,165 | 79.04× | 2.89× | 4.551 | 22.713 | 49.75 |
| 9 | yes | 1,591,983 | 60.88× | 5.60× | 88,961 | 70.34× | 2.57× | 5.403 | 27.083 | 147.61 |
| 19 | no | 1,220,966 | 79.38× | 7.30× | 65,899 | 94.95× | 3.47× | 31.582 | 159.012 | 121.03 |
| 19 | yes | 1,152,935 | 84.06× | 7.73× | 65,783 | 95.12× | 3.48× | 34.413 | 172.100 | 266.42 |

Level 9 without LDM nearly halves paired archive bytes (2,047,778 vs 3,893,530, **47.4% smaller**)
relative to level 3, while both fit three complete source-hour pairs per run. The level-9
worst pair costs **4.551 CPU seconds**, **22.713 wall seconds**, and **49.75 MiB peak RSS**.
It stays under the 128 MiB production memory target. All LDM variants exceed that target on
the VM. Level 19 without LDM fits the VM RSS target but fails the 120-second capacity model.

The level-9 calculation is:

```text
hour-pair JSON bytes = 96,921,020 + 6,257,120 = 103,178,140
usable wall seconds = 120 × 0.90 − 34.596748147 = 73.403251853
complete pairs/run = floor(73.403251853 / 22.713364690) = 3
capacity/day = 3 × 24 × 103,178,140 = 7,428,826,080 JSON bytes
incoming/day = 2,300,000,000 + 24 × 6,257,120 = 2,450,170,880 JSON bytes
2× requirement = 4,900,341,760 JSON bytes/day
capacity/incoming = 3.032×; net byte reduction/day = 4,978,655,200
capacity/day = 283,896 files; incoming/day = 94,632 files
net file reduction/day = 189,264
backlog = 18,080,239,538 JSON bytes; 1,956,467 files
byte-based drain = 3.63 days; file-count-based drain = 10.34 days
reported drain = max(byte-based, file-count-based) = 10.34 days
```

The 34.60-second discovery reserve comes from the real VM raw tree: directory discovery,
all archive-only dates, plus twice its slowest day inventory. The model reserves another 10%
of each run and takes no throughput credit for a final hour crossing the admission deadline.
It conservatively caps throughput at 24 complete pairs/run so at most two active date inventories
are needed per modeled run. A pair means one Kalshi hour plus one NWS hour; admission in the
actual compactor is at the individual source/hour boundary.

This is a projection for the recorded backlog cutoff (2026-11-10 00:35:08 UTC), after ongoing
arrivals. Historical small files and cold I/O can take longer than copied-hour trials; the
12-hour historical scan makes that limitation material. No production cold-backlog drain
rate was measured. The selected settings pass the measured current-load 2× requirement.
`test_level_nine_capacity_reproduces_the_vm_result_and_meets_twice_incoming_load` reproduces
the committed calculation from the committed measurements.

## Safety and memory design

- A monotonic wall deadline gates each new source/hour. An admitted hour finishes even if
  it crosses the deadline, while the systemd CPU cap remains effective. UTC dates/hours are
  processed globally oldest first across sources; the existing five-minute grace stays.
- A throwaway SQLite inventory with a 2 MiB page cache and disk-backed temporary tables keeps
  filenames and hashes off the Python heap. Each UTC date is inventoried just before work,
  avoiding a full month scan before the first hour. The index needs no durability because raw
  files and verified archives remain authoritative; a killed run's index is rebuilt.
- Tar headers require exact uncompressed sizes. A bounded gzip sizing/hash pass checks CRC
  before writing; the gzip trailer alone has only a 32-bit length. Payload and verification
  reads use 64 KiB chunks. TarInfo caches are explicitly cleared because Python 3.12 otherwise
  retains per-member metadata even in streaming mode.
- The writer fsyncs a dot-prefixed temporary archive, reopens and verifies it, atomically
  renames it on the same filesystem, then fsyncs the directory. Before deleting originals,
  all involved final archives are reopened and verified again. An advisory lock covers
  discovery, stale-temp cleanup, immutable name selection, publication and deletion.
- Late arrivals are verified into `.late-1`, `.late-2`, etc.; existing archive bytes and mtimes
  remain unchanged. A retry after publication verifies matching originals and deletes them
  without creating another supplement. A differing original raises and remains intact.
- The publisher refuses an existing final name. Stale primary and supplement temps are removed
  only under the lock for eligible hours. Corrupt gzip, corrupt archive membership, duplicate
  members, or mismatched hashes fail loudly and preserve originals.
- Memory tests cover a 256 MiB member and an hour with 24,000 members in fresh subprocesses.
  Both assert <128 MiB native peak RSS and <16 MiB growth against a small control hour.
  These bounds apply to the approved level-9/no-LDM configuration, not every opt-in zstd setting.
- CPUQuota=20% is a hard CPU bandwidth cap. IOWeight=10 is relative priority under contention;
  its effect depends on the VM's I/O controller and scheduler.

## Benchmark corrections

The original backlog scan decompressed and hashed every eligible file and held the final
report until the end; the user observed about 12 hours under the quota. That scan is removed.
Future runs stat gzip files and estimate JSON bytes with the measured per-source hour's
JSON/gzip ratio, rounded up and explicitly labelled `estimated_json_bytes`. Historical ratios
can differ, so file counts and gzip bytes remain separately available. The historical measured
backlog is reused for this PR's projection; no production rescan or new VM benchmark was run.

Each trial is appended to a JSONL journal, flushed and fsynced, then atomically checkpointed
in the JSON report before the next worker starts. Discovery and backlog stages are checkpointed
separately. Exceptions preserve a failed report; SIGKILL preserves completed trial records.
An existing report or journal is rejected instead of overwritten. Worker timing is unchanged.
The benchmark reports the explicitly selected level 9; it cannot silently substitute level 3
if the selected configuration fails the capacity gate.

The VM wrapper redirects sudo/systemd status and Python progress to a separate `.stderr.log`.
The report and `.trials.jsonl` journal are written by the script itself. Use a fresh output path:

```bash
bash benchmarks/run-compaction-benchmark.sh /home/prathik/compaction-vm-level9-rerun.json
```

That wrapper executes this command from `/home/prathik/Kalshi-Weather-Bot`:

```bash
sudo systemd-run --scope --slice=system.slice \
  -p CPUQuota=20% -p CPUWeight=20 -p IOWeight=10 \
  /usr/bin/nice -n 10 /home/prathik/.local/bin/uv run --no-sync --python 3.12 \
  python scripts/benchmark_compaction.py --vm \
  --raw-root /home/prathik/Kalshi-Weather-Bot/raw \
  --hour 2026-11-09T15 --repeats 3 --budget-seconds 120 --selected-level 9 \
  --incoming-kalshi-json-bytes-per-day 2300000000 \
  --work-dir /home/prathik/Kalshi-Weather-Bot/.compaction-benchmark-work \
  --output /home/prathik/compaction-vm-level9-rerun.json \
  2> /home/prathik/compaction-vm-level9-rerun.stderr.log
```

Inputs are read only; compaction/deletion happen on disposable copies. VM mode refuses to
proceed without the specified cgroup limits and nice priority. Local mode emits ratios/RSS
without claiming CPU performance or production capacity. The approved 2× capacity gate
continues to reject insufficient throughput or peak RSS ≥128 MiB.

## Property tests and proof they bite

Each experiment below temporarily changes production/script/unit code, runs the named test,
observes exit code 1, then restores the implementation. Exact before/after replacements and
complete failure outputs are in [compaction-mutation-evidence.json](compaction-mutation-evidence.json).
No failing safety test was weakened or deleted. The old late-file union expectations were
updated to the explicitly approved immutable-supplement behavior while retaining damaged
archive/original preservation assertions.

**deadline** — No new source/hour starts after the deadline, and the next run resumes.

Test: `tests/test_compact.py::test_no_new_hour_starts_after_the_budget_and_hours_are_globally_oldest_first`

Observed failure (exit 1; mutation reverted):

```text
E   AssertionError: assert [('2026-10-06...5', 'kalshi')] == [('2026-10-06...7T13', 'nws')]
E
```

**oldest-first** — Hour admission is oldest first across dates and sources.

Test: `tests/test_compact.py::test_no_new_hour_starts_after_the_budget_and_hours_are_globally_oldest_first`

Observed failure (exit 1; mutation reverted):

```text
E   AssertionError: assert [('2026-10-07...4', 'kalshi')] == [('2026-10-06...7T13', 'nws')]
E
```

**bounded-rss** — A 256 MiB member cannot cause hour-sized allocation growth.

Test: `tests/test_compact.py::test_peak_rss_stays_below_128_mib_when_hour_bytes_or_member_count_grow[large-member]`

Observed failure (exit 1; mutation reverted):

```text
E   assert 910229504 < (128 * (1024 ** 2))
E    +  where 910229504 = max([43220992, 910229504])
```

**measurement-matrix** — All six level/LDM configurations run on copies; local output preserves source data and separates progress.

Test: `tests/test_compaction_benchmark.py::test_the_benchmark_exercises_all_six_configs_without_modifying_input_or_claiming_mac_capacity`

Observed failure (exit 1; mutation reverted):

```text
E   assert {(3, False), ...e), (9, True)} == {(3, False), ...), (19, True)}
E
```

**capacity-2x** — Capacity must exceed twice incoming load and stay within the RSS target.

Test: `tests/test_compaction_benchmark.py::test_capacity_requires_twice_incoming_load_and_the_rss_bound_using_the_slowest_vm_trial`

Observed failure (exit 1; mutation reverted):

```text
E   assert False
E    +  where False = all(<generator object test_capacity_requires_twice_incoming_load_and_the_rss_bound_using_the_slowest_vm_trial.<locals>.<genexpr> at 0x10b687030>)
```

**capacity-discovery** — Actual discovery overhead reduces the usable run budget.

Test: `tests/test_compaction_benchmark.py::test_capacity_accounts_for_backlog_discovery_before_starting_complete_hour_pairs`

Observed failure (exit 1; mutation reverted):

```text
E   AssertionError: assert {'level': 9, 'ldm': False, 'worst_pair_wall_seconds': 10, 'worst_pair_cpu_seconds': 2, ...} is None
```

**vm-quota-guard** — A local process cannot claim quota-limited VM capacity.

Test: `tests/test_compaction_benchmark.py::test_the_vm_benchmark_refuses_to_claim_capacity_without_the_required_quota`

Observed failure (exit 1; mutation reverted):

```text
E   Failed: DID NOT RAISE ValueError
```

**cpu-quota** — The unit enforces CPUQuota independently of application budgeting.

Test: `tests/test_compact.py::test_the_service_caps_cpu_and_yields_io_even_if_the_time_budget_is_removed`

Observed failure (exit 1; mutation reverted):

```text
E   KeyError: 'CPUQuota'
```

**io-weight** — The unit explicitly yields I/O while retaining CPU/Nice priorities.

Test: `tests/test_compact.py::test_the_service_caps_cpu_and_yields_io_even_if_the_time_budget_is_removed`

Observed failure (exit 1; mutation reverted):

```text
E   AssertionError: assert None == 10
E    +  where None = functools.partial(<bound method SectionProxy.get of <Section: Service>>, _impl=<bound method RawConfigParser.getint of <configparser.ConfigParser object at 0x1077a28d0>>)('IOWeight')
```

**verify-before-delete-missing** — Missing members prevent deletion of every original.

Test: `tests/test_compact.py::test_no_original_is_deleted_when_the_reopened_archive_is_missing_a_file`

Observed failure (exit 1; mutation reverted):

```text
E   Failed: DID NOT RAISE CompactionError
```

**verify-before-delete-changed** — Changed member bytes prevent deletion of every original.

Test: `tests/test_compact.py::test_no_original_is_deleted_when_an_archived_copy_has_a_different_sha256`

Observed failure (exit 1; mutation reverted):

```text
E   Failed: DID NOT RAISE CompactionError
```

**atomic-rename** — Publication uses one atomic rename with a complete temp and intact originals.

Test: `tests/test_compact.py::test_publishing_uses_an_atomic_rename_only_after_verification`

Observed failure (exit 1; mutation reverted):

```text
E   assert 0 == 1
E    +  where 0 = len([])
```

**killed-originals** — SIGKILL during writing leaves every original intact.

Test: `tests/test_compact.py::test_a_process_killed_mid_archive_keeps_originals_and_the_next_run_cleans_its_temp_file[False]`

Observed failure (exit 1; mutation reverted):

```text
E   assert {'2026-10-07T....00\\"]]}}"}'} == {'2026-10-07T....00\\"]]}}"}'}
E
```

**stale-temp-cleanup** — The next run discards a poisoned partial archive and completes safely.

Test: `tests/test_compact.py::test_a_process_killed_mid_archive_keeps_originals_and_the_next_run_cleans_its_temp_file[False]`

Observed failure (exit 1; mutation reverted):

```text
E   FileExistsError: [Errno 17] File exists: '/private/var/folders/_p/p3pxv245413bctjpxywq4d400000gn/T/pytest-of-prathik/pytest-97/test_a_process_killed_mid_arch0/raw/kalshi/2026-10-07/.2026-10-07T14.tar.zst.tmp'
```

**immutable-supplements** — Two late rounds produce numbered supplements without rewriting any published bytes or mtime.

Test: `tests/test_compact.py::test_late_files_are_verified_in_numbered_supplements_without_rewriting_published_archives`

Observed failure (exit 1; mutation reverted):

```text
E   AssertionError: assert 1794322846038882499 == 1794322846036588332
E    +  where 1794322846038882499 = os.stat_result(st_mode=33188, st_ino=87233801, st_dev=16777234, st_nlink=1, st_uid=501, st_gid=20, st_size=833, st_atime=1794322846, st_mtime=1794322846, st_ctime=1794322846).st_mtime_ns
```

**reopen-final-before-delete** — Corruption after publication is caught by reopening the final archive.

Test: `tests/test_compact.py::test_originals_are_preserved_if_the_final_archive_changes_after_atomic_publication`

Observed failure (exit 1; mutation reverted):

```text
E   Failed: DID NOT RAISE CompactionError
```

**backlog-file-count** — Drain projections account for old small-file counts, not just JSON bytes.

Test: `tests/test_compaction_benchmark.py::test_backlog_drain_time_accounts_for_many_small_files_as_well_as_json_bytes`

Observed failure (exit 1; mutation reverted):

```text
E   assert 0.04348582362149939 == 58.721837006971455
```

**level-nine-default** — The actual CLI defaults to level 9, LDM off, and a 120-second budget.

Test: `tests/test_compact.py::test_the_cli_defaults_to_level_nine_without_ldm_and_a_120_second_budget`

Observed failure (exit 1; mutation reverted):

```text
E   AssertionError: assert [{'time_budge...ance': False}] == [{'time_budge...ance': False}]
E
```

**level-nine-unit** — The production unit explicitly passes the approved level and budget.

Test: `tests/test_compact.py::test_the_service_caps_cpu_and_yields_io_even_if_the_time_budget_is_removed`

Observed failure (exit 1; mutation reverted):

```text
E   AssertionError: assert '--zstd-level 9' in '/home/prathik/.local/bin/uv run python -m kwb.ops.compact --time-budget-seconds 120 --zstd-level 3'
```

**selected-capacity** — Capacity results retain level 9 and reject failure instead of falling back to level 3.

Test: `tests/test_compaction_benchmark.py::test_capacity_keeps_the_selected_level_nine_instead_of_substituting_level_three`

Observed failure (exit 1; mutation reverted):

```text
E   assert 3 == 9
```

**persist-trial** — Each completed trial is journaled and checkpointed before the next worker starts.

Test: `tests/test_compaction_benchmark.py::test_each_trial_is_fsynced_and_checkpointed_before_starting_the_next_worker`

Observed failure (exit 1; mutation reverted):

```text
E   AssertionError: assert [] == [{'level': 3,...: 2.0}}, ...}]
E
```

**trial-fsync** — The completed trial journal record is fsynced before the next worker starts.

Test: `tests/test_compaction_benchmark.py::test_each_trial_is_fsynced_and_checkpointed_before_starting_the_next_worker`

Observed failure (exit 1; mutation reverted):

```text
E   AssertionError: assert 0 == (0 + 1)
E    +  where 0 = <built-in method count of list object at 0x106a2e1c0>(87233863)
```

**killed-benchmark-checkpoint** — SIGKILL in a later worker preserves the first complete trial and journal.

Test: `tests/test_compaction_benchmark.py::test_killing_a_later_benchmark_worker_keeps_every_completed_trial`

Observed failure (exit 1; mutation reverted):

```text
E   assert 0 == 1
E    +  where 0 = len([])
```

**stderr-separation** — Progress never contaminates JSON stdout/report output.

Test: `tests/test_compaction_benchmark.py::test_the_benchmark_exercises_all_six_configs_without_modifying_input_or_claiming_mac_capacity`

Observed failure (exit 1; mutation reverted):

```text
E   json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
```

**backlog-no-decompression** — Backlog estimation uses metadata only, applies per-source ratios, and labels inferred JSON.

Test: `tests/test_compaction_benchmark.py::test_backlog_estimation_uses_only_gzip_file_sizes_and_labels_the_estimate`

Observed failure (exit 1; mutation reverted):

```text
E   Failed: Backlog estimation must not open, decompress or hash payloads
```

**bounded-member-metadata** — An hour with 24,000 members cannot accumulate unbounded TarInfo metadata.

Test: `tests/test_compact.py::test_peak_rss_stays_below_128_mib_when_hour_bytes_or_member_count_grow[many-members]`

Observed failure (exit 1; mutation reverted):

```text
E   assert (62750720 - 43368448) < (16 * (1024 ** 2))
```

The SIGKILL archive test also passes for supplementary archives. Existing byte-for-byte
preservation, grace/current-hour exclusion, interrupted deletion recovery, repeated-run
idempotence, corrupt gzip and unexpected-entry tests remain. Additional coverage verifies
changed retry originals, duplicate archive members and lock contention.

## Validation and scope

After restoring all 26 mutations, Python 3.12.15 passed ruff check, ruff format --check
(36 files), and pytest (199 passed in 7.95 seconds). git diff --check and bash -n
benchmarks/run-compaction-benchmark.sh also passed. No deployment or timer change
is performed. docs/ is untouched; the user's CLAUDE.md change and untracked understanding.md
remain outside the commit.
