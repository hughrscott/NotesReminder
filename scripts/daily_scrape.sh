#!/bin/sh
set -euo pipefail

if [ -z "${PYTHON_BIN:-}" ]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN=python3
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN=python
  else
    echo "python not found in PATH (set PYTHON_BIN to override)." >&2
    exit 1
  fi
fi

# Usage:
#   ./scripts/daily_scrape.sh --school westu-sor --start-date 2025-06-18 --end-date 2025-06-18 \
#     --to you@example.com manager@example.com
#
# Notes:
# - Requires .env with Pike13 + AWS + SMTP credentials.
# - Pass --no-email to skip sending the summary.

"$PYTHON_BIN" run_daily.py "$@"
