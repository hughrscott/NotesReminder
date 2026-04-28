# NotesReminder Data Pipeline

## Overview
This project maintains a single SQLite database (`reminders.db`) that is synced to S3. The daily scrape updates lesson notes; call logs + client imports add Dialpad/Pike13 data for reporting.

## Two-pipeline operating model

`reminders.db` is the single version of truth, but the current notes emails and the new lead intelligence work are separate operational pipelines.

- Production notes pipeline: GitHub Actions runs `run_daily.py`, downloads the S3 DB, scrapes Pike13 lesson notes, scores notes, sends the daily/weekly lesson-note emails, and uploads the DB back to S3.
- Lead intelligence pipeline: local/manual authenticated browser refresh writes additive HubSpot, Dialpad, and Pike13 lead tables into the same DB, then validates them with the source completeness report.
- Lead refresh work must not change the current daily/weekly email content until there is a separate plan and acceptance gate for adding lead insights to those summaries.
- Lead tables are additive. They must not change the meaning of the existing `reminders` table or note-score columns used by `run_daily.py`.

## Folder layout (default)
- `Call Log/` : Dialpad CSV exports (`Call_Logs*.csv`, `Voicemails*.csv`, etc.)
- `ClientList/` : Pike13 client CSV export
- `reminders.db` : Local SQLite database (synced to S3)
- `screenshots/` : Playwright screenshots for debugging Pike13 scraping

## Environment setup
1) Copy `.env.example` to `.env` and fill in credentials.
2) Install dependencies:

```bash
pip install -r requirements.txt
playwright install
```

Optional for transcription:
- Set `TRANSCRIBE_BUCKET` to the S3 bucket used for temporary audio + transcripts.

## Pipeline order
1) Daily/weekly scrape (updates notes + attendance):

```bash
./scripts/daily_scrape.sh --school westu-sor --start-date 2025-06-18 --end-date 2025-06-18 \
  --to you@example.com manager@example.com
```

2) Import Dialpad + Pike13 clients (updates call tables + matches):

```bash
./scripts/import_call_logs.sh \
  --clients ClientList/your_clients.csv \
  --dialpad-dir "Call Log" \
  --db reminders.db
```

3) Generate call reports:

```bash
./scripts/generate_reports.sh --db reminders.db
```

## Lead refresh safety checklist

Use this checklist before uploading a DB that has been touched by local authenticated lead refresh work:

1. Pull the latest Git state and confirm no unexpected tracked files are dirty.
2. Verify or download the latest S3 `reminders.db`.
3. Create a local backup of `reminders.db`.
4. Run the local authenticated HubSpot/Dialpad/Pike13 refresh scripts against the backup-backed working DB.
5. Run `python3 scripts/source_completeness_report.py --db reminders.db --window-days 7 --pike13-lookahead-days 30 --pretty`.
6. Generate the visible progress dashboard with `python3 scripts/progress_dashboard.py --db reminders.db --window-days 7 --pike13-lookahead-days 30`.
7. Validate that existing `reminders` row counts and note-score columns are still intact.
8. Confirm browser profiles, screenshots, raw discovery evidence, local DB backups, and customer-data exports are uncommitted.
9. Upload/sync the DB only after the source completeness report, progress dashboard, and notes-pipeline checks look correct.

## Lead intelligence progress dashboard

Generate a sanitized Markdown dashboard after each local lead refresh:

```bash
python3 scripts/progress_dashboard.py \
  --db reminders.db \
  --window-days 7 \
  --pike13-lookahead-days 30
```

The default output is `outputs/progress/lead_intelligence_status.md`. The dashboard is count/status oriented and intentionally excludes customer names, phone numbers, SMS bodies, transcripts, raw lesson notes, and call summaries.

Pike13 is split into two readiness tracks:

- Existing lesson visits/notes from `reminders`, used for note-quality and current-student operations.
- Rich lead outcomes from authenticated Pike13 extraction, used for trial attendance, no-shows, memberships/plans, and conversion attribution.

## Scripts overview
- `run_daily.py` : Scrape Pike13 lessons, update `reminders.db`, email summary, sync to S3.
- `backfill.py` : Multi-school historical scrape (no email by default).
- `import_call_data.py` : Import Dialpad + Pike13 client CSVs, build call matches, and refresh `call_logs`.
- `generate_call_reports.py` : Write voicemail/missed-call CSVs from call data.
- `build_reporting_schema.py` : Create/backfill reporting tables (`lessons`, `lesson_students`, etc.).
- `transcribe_recordings.py` : Download recordings, transcribe with AWS, store in `recording_transcripts`.
- `download_recordings_playwright.py` : Download Dialpad recordings via a logged-in browser session.
- `transcribe_recordings_whisper.py` : Transcribe local recordings with Whisper (CPU).
- `analyze_transcripts_openai.py` : Add intent/sentiment/topic/outcome tags via OpenAI.
- `scripts/daily_scrape.sh` : Shell wrapper for `run_daily.py`.
- `scripts/import_call_logs.sh` : Shell wrapper for `import_call_data.py`.
- `scripts/generate_reports.sh` : Shell wrapper for `generate_call_reports.py`.
- `scripts/update_all.sh` : End-to-end pipeline runner (scrape, import, reports).
- `scripts/smoke_test.sh` : Quick env/dependency check (no scrape).

## Smoke test
Validate env + Python dependencies without scraping:

```bash
./scripts/smoke_test.sh
```

## One-shot pipeline (optional)
`./scripts/update_all.sh` runs the pipeline in order. It expects a valid `.env`, and uses environment variables for call import:

```bash
export CLIENTS_CSV="ClientList/your_clients.csv"
export DIALPAD_DIR="Call Log"   # optional
export DB_PATH="reminders.db"    # optional

./scripts/update_all.sh --school westu-sor --start-date 2025-06-18 --end-date 2025-06-18 \
  --to you@example.com manager@example.com
```

## Scheduling (macOS)
Use `launchd` or `cron` to run `./scripts/daily_scrape.sh` on a schedule, and direct logs to a `logs/` folder.

Example `launchd` plist (`~/Library/LaunchAgents/com.notesreminder.daily.plist`):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.notesreminder.daily</string>
    <key>ProgramArguments</key>
    <array>
      <string>/bin/sh</string>
      <string>/Users/hughscott/Documents/Coding/NotesReminder/scripts/daily_scrape.sh</string>
      <string>--school</string>
      <string>westu-sor</string>
      <string>--to</string>
      <string>you@example.com</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
      <key>Hour</key>
      <integer>8</integer>
      <key>Minute</key>
      <integer>0</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>/Users/hughscott/Documents/Coding/NotesReminder/logs/daily.out.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/hughscott/Documents/Coding/NotesReminder/logs/daily.err.log</string>
  </dict>
</plist>
```

Load it:

```bash
launchctl load ~/Library/LaunchAgents/com.notesreminder.daily.plist
```

## Publish DB to S3 (manual)
If Claude auto-syncs from S3, upload your latest working DB first:

```bash
python3 scripts/publish_db_to_s3.py --db reminders.db
```

## Pre-sync overwrite guard
Before replacing a local DB with a downloaded/synced copy:

```bash
# check safety (blocks if incoming is older/smaller)
python3 scripts/db_guard.py verify --current reminders.db --incoming /tmp/incoming_reminders.db

# if safe, backup then replace
python3 scripts/db_guard.py replace --current reminders.db --incoming /tmp/incoming_reminders.db
```

This prevents accidental loss when a stale S3/MCP sync file appears.

## Legacy score recovery
Use the dedicated recovery utilities:

```bash
# local discovery
python3 scripts/recover_legacy_scores.py discover \
  --paths reminders.db reminders_mcp.db reminders.db.BAK2 reminders.dbBAK

# cloud discovery
python3 scripts/discover_db_sources.py --bucket notesreminder-db --key reminders.db

# compare + extract
python3 scripts/recover_legacy_scores.py compare --current-db reminders.db --source-db /path/to/scored_snapshot.db
python3 scripts/recover_legacy_scores.py extract --current-db reminders.db --source-db /path/to/scored_snapshot.db

# merge matched rows into lesson_note_scores_history
python3 scripts/merge_legacy_scores.py \
  --db reminders.db \
  --matched-csv outputs/matched_legacy_scores.csv \
  --source-db /path/to/scored_snapshot.db

# verify
sqlite3 reminders.db < scripts/sql/verify_scores.sql
```

## Data hygiene
- Keep new CSVs in dated subfolders under `Call Log/` and `ClientList/`.
- Avoid editing local DB copies in parallel; rely on S3 sync.

## Sanity checks (DB)
After `import_call_data.py`, confirm the call data shape:

```bash
python3 - <<'PY'
import sqlite3
conn = sqlite3.connect('reminders.db')
cur = conn.cursor()
cur.execute('SELECT school_code, COUNT(*) FROM call_logs GROUP BY school_code ORDER BY COUNT(*) DESC')
print('school_code distribution:')
for school_code, cnt in cur.fetchall():
    print(school_code, cnt)
cur.execute('SELECT COUNT(*) FROM call_logs WHERE voicemail_transcript IS NOT NULL')
print('voicemail transcript count:', cur.fetchone()[0])
cur.execute('SELECT COUNT(*) FROM call_logs WHERE recording_url IS NOT NULL')
print('recording url count:', cur.fetchone()[0])
conn.close()
PY
```

After `build_reporting_schema.py`, confirm reporting tables:

```bash
python3 - <<'PY'
import sqlite3
conn = sqlite3.connect('reminders.db')
cur = conn.cursor()
for name in ['lessons','lesson_students','lesson_notes','lesson_attendance','schools','instructors','students']:
    cur.execute(f"SELECT COUNT(*) FROM {name}")
    print(name, cur.fetchone()[0])
conn.close()
PY
```

After `transcribe_recordings.py`, confirm transcript coverage:

```bash
python3 - <<'PY'
import sqlite3
conn = sqlite3.connect('reminders.db')
cur = conn.cursor()
cur.execute('SELECT transcript_status, COUNT(*) FROM recording_transcripts GROUP BY transcript_status ORDER BY COUNT(*) DESC')
print('recording_transcripts status counts:')
for status, cnt in cur.fetchall():
    print(status, cnt)
conn.close()
PY
```

## Dialpad downloads without API access
If you cannot use Dialpad API tokens, you can download recordings using a logged-in
browser session:

```bash
python3 download_recordings_playwright.py --out-dir recordings --limit 5
```

Login-only (no downloads):

```bash
python3 download_recordings_playwright.py --out-dir recordings --limit 0
```

Download all pending recordings:

```bash
python3 download_recordings_playwright.py --out-dir recordings --all
```

The script opens a browser window, waits for you to log in, then downloads each
recording URL to `recordings/` and records status in `recording_downloads`.

## Local transcription (Whisper)
Transcribe downloaded recordings on CPU and store results in `recording_transcripts`:

```bash
python3 transcribe_recordings_whisper.py --recordings-dir recordings --model small --limit 2 --verbose
```

Install Whisper if needed:

```bash
pip install openai-whisper
```

## Transcript analysis (LLM)
Analyze completed transcripts and store structured tags:

```bash
python3 analyze_transcripts_openai.py --model gpt-4o-mini --limit 10
```

Set `OPENAI_API_KEY` in `.env` to enable API access.
