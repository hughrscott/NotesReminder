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
#   ./scripts/generate_reports.sh --db reminders.db

"$PYTHON_BIN" generate_call_reports.py "$@"
