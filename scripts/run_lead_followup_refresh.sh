#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${REMINDERS_DB_PATH:-reminders.db}"
LOAD_WINDOW_DAYS="${LOAD_WINDOW_DAYS:-7}"
PIKE13_LOOKAHEAD_DAYS="${PIKE13_LOOKAHEAD_DAYS:-30}"
HEADLESS_FLAG="${LEAD_REFRESH_HEADLESS---headless}"
HUBSPOT_LIMIT="${HUBSPOT_LEAD_LIMIT:-100}"
HUBSPOT_DETAIL_LIMIT="${HUBSPOT_DETAIL_LIMIT:-25}"
DIALPAD_THREAD_LIMIT="${DIALPAD_SMS_THREAD_LIMIT:-100}"
DIALPAD_VOICE_LIMIT="${DIALPAD_VOICE_LIMIT:-100}"
PIKE13_LIMIT="${PIKE13_LEAD_LIMIT:-100}"
PIKE13_BASE_URL="${PIKE13_BASE_URL:-https://westu-sor.pike13.com}"
PIKE13_SCHOOL="${PIKE13_SCHOOL:-West U}"

python3 lead_followup_schema.py --db "$DB_PATH"

WINDOW_START="$(python3 -c "from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc)-timedelta(days=int('$LOAD_WINDOW_DAYS'))).date().isoformat())")"

python3 scripts/extract_hubspot_leads.py \
  --db "$DB_PATH" \
  --profile-dir browser_profiles/hubspot \
  --limit "$HUBSPOT_LIMIT" \
  --detail-limit "$HUBSPOT_DETAIL_LIMIT" \
  --start-date "$WINDOW_START" \
  $HEADLESS_FLAG

python3 scripts/extract_dialpad_sms.py \
  --db "$DB_PATH" \
  --profile-dir browser_profiles/dialpad \
  --thread-limit "$DIALPAD_THREAD_LIMIT" \
  --start-date "$WINDOW_START" \
  $HEADLESS_FLAG

python3 scripts/extract_dialpad_voice.py \
  --db "$DB_PATH" \
  --profile-dir browser_profiles/dialpad \
  --limit-per-view "$DIALPAD_VOICE_LIMIT" \
  --start-date "$WINDOW_START" \
  $HEADLESS_FLAG

python3 scripts/extract_pike13_leads.py \
  --db "$DB_PATH" \
  --profile-dir browser_profiles/pike13-westu \
  --base-url "$PIKE13_BASE_URL" \
  --school "$PIKE13_SCHOOL" \
  --limit "$PIKE13_LIMIT" \
  --start-date "$WINDOW_START" \
  $HEADLESS_FLAG

python3 scripts/source_completeness_report.py \
  --db "$DB_PATH" \
  --window-days "$LOAD_WINDOW_DAYS" \
  --pike13-lookahead-days "$PIKE13_LOOKAHEAD_DAYS" \
  --pretty
