#!/bin/sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-}"
if [ -z "$PYTHON_BIN" ]; then
  if [ -x "venv/bin/python" ]; then
    PYTHON_BIN="venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  else
    echo "python not found in PATH (set PYTHON_BIN to override)." >&2
    exit 1
  fi
fi

TARGET_DATE=""
PROFILE_DIR="browser_profiles/pike13"
LOGIN_TIMEOUT="900"
SKIP_NOTE_SCORING=""
NO_EMAIL=""

usage() {
  cat <<'EOF'
Usage:
  scripts/run_notes_local_mfa.sh [--date YYYY-MM-DD] [--profile-dir DIR] [--login-timeout SECONDS] [--skip-note-scoring] [--no-email]

Runs the production Notes Reminder pipeline locally for West U and The Heights
using one shared persistent Pike13 browser profile. Use this while Pike13 MFA
prevents GitHub Actions from completing login.

Defaults:
  --date yesterday in America/Chicago
  --profile-dir browser_profiles/pike13
  --login-timeout 900
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --date)
      TARGET_DATE="$2"
      shift 2
      ;;
    --profile-dir)
      PROFILE_DIR="$2"
      shift 2
      ;;
    --login-timeout)
      LOGIN_TIMEOUT="$2"
      shift 2
      ;;
    --skip-note-scoring)
      SKIP_NOTE_SCORING="--skip-note-scoring"
      shift
      ;;
    --no-email)
      NO_EMAIL="--no-email"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [ -z "$TARGET_DATE" ]; then
  TARGET_DATE="$("$PYTHON_BIN" - <<'PY'
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
print((datetime.now(ZoneInfo("America/Chicago")).date() - timedelta(days=1)).isoformat())
PY
)"
fi

if [ -f ".env" ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

mkdir -p outputs/db_backups "$PROFILE_DIR"

STAMP="$(date +%Y%m%d-%H%M%S)"
LOCAL_BACKUP="outputs/db_backups/reminders.db.${STAMP}.before-local-mfa-notes-run.bak"
S3_BACKUP_KEY="backups/reminders-before-local-mfa-notes-run-${STAMP}.db"

if [ -f reminders.db ]; then
  cp reminders.db "$LOCAL_BACKUP"
  echo "Local DB backup: $LOCAL_BACKUP"
else
  echo "No local reminders.db found before run; run_daily.py will download from S3."
fi

"$PYTHON_BIN" - <<PY
import boto3
bucket = "notesreminder-db"
source = "reminders.db"
backup = "$S3_BACKUP_KEY"
s3 = boto3.client("s3")
s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": source}, Key=backup)
print(f"S3 DB backup: s3://{bucket}/{backup}")
PY

COMMON_ARGS="
  --start-date $TARGET_DATE
  --end-date $TARGET_DATE
  --summary both
  --verbose
  --pike13-profile-dir $PROFILE_DIR
  --interactive-login
  --login-timeout $LOGIN_TIMEOUT
"

if [ -n "$SKIP_NOTE_SCORING" ]; then
  COMMON_ARGS="$COMMON_ARGS $SKIP_NOTE_SCORING"
fi
if [ -n "$NO_EMAIL" ]; then
  COMMON_ARGS="$COMMON_ARGS $NO_EMAIL"
fi

echo "Running West U notes pipeline for $TARGET_DATE..."
"$PYTHON_BIN" run_daily.py \
  --school westu-sor \
  $COMMON_ARGS \
  --to huscott@schoolofrock.com vscott@schoolofrock.com cabarnhill@schoolofrock.com

echo "Running The Heights notes pipeline for $TARGET_DATE..."
"$PYTHON_BIN" run_daily.py \
  --school theheights-sor \
  $COMMON_ARGS \
  --to huscott@schoolofrock.com vscott@schoolofrock.com ndees@schoolofrock.com agarza@schoolofrock.com

echo "Local MFA notes pipeline completed for $TARGET_DATE."
