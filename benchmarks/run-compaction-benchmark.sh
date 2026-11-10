#!/usr/bin/env bash
# Run from the repository root on the VM. Only disposable copies are compacted.
set -euo pipefail
set -o noclobber
cd /home/prathik/Kalshi-Weather-Bot
report_path="${1:-/home/prathik/compaction-vm-2026-11-09T15-rerun.json}"
# Keep sudo/systemd status and Python progress out of the JSON report and journal.
sudo systemd-run --scope --slice=system.slice \
  -p CPUQuota=20% -p CPUWeight=20 -p IOWeight=10 \
  /usr/bin/nice -n 10 /home/prathik/.local/bin/uv run --no-sync --python 3.12 \
  python scripts/benchmark_compaction.py --vm \
  --raw-root /home/prathik/Kalshi-Weather-Bot/raw \
  --hour 2026-11-09T15 --repeats 3 --budget-seconds 120 --selected-level 9 \
  --incoming-kalshi-json-bytes-per-day 2300000000 \
  --work-dir /home/prathik/Kalshi-Weather-Bot/.compaction-benchmark-work \
  --output "$report_path" 2> "${report_path%.json}.stderr.log"
