#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
uv run --no-project --python 3.12 --with cmake==3.31.6 --with tomli-w==1.2.0 python scripts/setup_ndfd.py
