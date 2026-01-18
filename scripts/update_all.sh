#!/bin/sh
set -euo pipefail

# End-to-end pipeline:
# 1) Scrape + update reminders.db (and sync to S3)
# 2) Import Dialpad + Pike13 client data
# 3) Generate call reports

if [ ! -f .env ]; then
  echo "Missing .env. Copy .env.example to .env and fill in credentials." >&2
  exit 1
fi

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

# Step 1: scrape and update DB
"$PYTHON_BIN" run_daily.py "$@"

# Step 2: import call logs (requires --clients and optionally --dialpad-dir/--db)
# Example:
#   ./scripts/update_all.sh --school westu-sor --start-date 2025-06-18 --end-date 2025-06-18 --to you@example.com \
#     --clients ClientList/your_clients.csv --dialpad-dir "Call Log" --db reminders.db
if [ -n "${CLIENTS_CSV:-}" ]; then
  "$PYTHON_BIN" import_call_data.py --clients "$CLIENTS_CSV" --dialpad-dir "${DIALPAD_DIR:-Call Log}" --db "${DB_PATH:-reminders.db}"
fi

# Step 3: generate reports (requires reminders.db)
"$PYTHON_BIN" generate_call_reports.py --db "${DB_PATH:-reminders.db}"
