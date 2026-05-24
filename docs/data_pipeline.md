# NotesReminder Data Pipeline

## Overview
This project keeps the production lesson-note database (`reminders.db`) synced to S3. The current operating model is a single canonical local production database with additive lead-intelligence and reporting tables. Lead dashboards and scorecards remain shadow-mode outputs until their phase gates are approved.

## Two-pipeline operating model

`reminders.db` is the production source of truth. Notes emails and lead-intelligence work are separate operational workflows, but both now read and write approved additive tables in the main local database.

- Production notes pipeline: GitHub Actions runs `run_daily.py`, downloads the S3 DB, scrapes Pike13 lesson notes, scores notes, sends the daily/weekly lesson-note emails, and uploads the DB back to S3.
- Lead intelligence pipeline: local/manual authenticated browser refresh writes additive HubSpot, Dialpad, and Pike13 lead tables into `reminders.db`, then validates them with the source completeness report.
- Lead refresh work must not change the current daily/weekly email content until there is a separate plan and acceptance gate for adding lead insights to those summaries.
- Lead tables are additive. They must not change the meaning of the existing `reminders` table or note-score columns used by `run_daily.py`.

## Folder layout (default)
- `Call Log/` : Dialpad CSV exports (`Call_Logs*.csv`, `Voicemails*.csv`, etc.)
- `ClientList/` : Pike13 client CSV export
- `reminders.db` : Local SQLite database (synced to S3)
- `outputs/lead_intelligence/lead_intelligence_working.db` : Historical/staging lead proof DB, not the default production path
- `screenshots/` : Playwright screenshots for debugging Pike13 scraping
- `notesreminder/` : Package skeleton for new source, schema, report, orchestration, MCP, transcription, and shared utility modules

New code should go under `notesreminder/`. Existing root-level production entry points stay in place until the later repository layout migration.

## Environment setup
1) Copy `.env.example` to `.env` and fill in credentials.
2) Install dependencies:

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt  # optional, for local tests
playwright install
```

Optional for transcription:
- Set `TRANSCRIBE_BUCKET` to the S3 bucket used for temporary audio + transcripts.

## Test baseline
Run the active test suite from the repo root with the project environment:

```bash
python -m pytest
```

`pytest.ini` sets `testpaths = tests` and `pythonpath = .`, so normal collection excludes archived legacy tests and root-level modules import consistently.

## Notes pipeline health
Generate a small health dashboard before or after production notes runs:

```bash
python scripts/notes_pipeline_health.py --db reminders.db --lookback-days 7
```

The default outputs are:

- `outputs/progress/notes_pipeline_health.json`
- `outputs/progress/notes_pipeline_health.md`

The dashboard shows latest lesson coverage, latest `last_checked`, reportable/missing-note counts, and local email-delivery evidence from `logs/`.

## Pipeline order
1) Daily/weekly scrape (updates notes + attendance):

```bash
./scripts/daily_scrape.sh --school westu-sor --start-date 2025-06-18 --end-date 2025-06-18 \
  --to you@example.com manager@example.com
```

If Pike13 requires Okta/MFA, run the production notes pipeline locally with a shared persistent browser profile so you can approve the login once, then let the normal scrape/email/S3 sync continue for both schools:

```bash
scripts/run_notes_local_mfa.sh --date YYYY-MM-DD
```

The wrapper creates local and S3 backups, runs West U and The Heights with the normal recipients, sends the usual summary emails, and uploads the updated DB to S3. It uses `browser_profiles/pike13` by default. The GitHub Actions job still uses the non-interactive path and cannot satisfy a fresh MFA prompt by itself.

For local-only validation against a staging or promotion-candidate DB, run
`run_daily.py` with an explicit DB path and skip S3 sync:

```bash
venv/bin/python run_daily.py \
  --school westu-sor \
  --start-date YYYY-MM-DD \
  --end-date YYYY-MM-DD \
  --db-path outputs/lead_intelligence/unified_reminders_phase7_daily_test.db \
  --skip-s3-sync \
  --no-email \
  --skip-note-scoring \
  --pike13-profile-dir browser_profiles/pike13 \
  --verbose
```

This path does not download from S3, does not upload to S3, and does not send
email. Use it to prove the notes checker can read and update a candidate DB
before promotion.

After a successful production notes run, rebuild the local lead working DB so lead reports include the latest lesson-note evidence:

```bash
python3 scripts/rebuild_lead_working_db.py \
  --production-db reminders.db \
  --lead-proof-db outputs/lead_intelligence/lead_intelligence_working.db \
  --output outputs/lead_intelligence/lead_intelligence_working.db
```

For the first rebuild after the May 1 lead proof, use the preserved proof backup as the lead source:

```bash
python3 scripts/rebuild_lead_working_db.py \
  --production-db reminders.db \
  --lead-proof-db outputs/db_backups/reminders.db.20260501-211741.before-local-mfa-notes-run.bak \
  --output outputs/lead_intelligence/lead_intelligence_working.db
```

Phase 7 single-DB reconciliation is copy-first. Before replacing or uploading
`reminders.db`, create local backups of the production notes DB and lead working
DB, then generate a unified copy and verify the reconciliation report:

```bash
python3 scripts/migrate_lead_intel_to_production.py \
  --production-db reminders.db \
  --lead-db outputs/lead_intelligence/lead_intelligence_working.db \
  --output outputs/lead_intelligence/unified_reminders_phase7.db \
  --json
```

Run the same command a second time with the unified copy as both `--production-db`
and `--output` to prove idempotency. The command preserves production-owned
lesson/call tables, replaces lead-owned tables from the lead DB, merges shared
recording tables by primary key, and blocks if table counts or source-row
coverage do not reconcile. After the copy-mode gate passes and the unified DB is
promoted to `reminders.db`, MCP lead dashboard tools read from the main DB by
default. Use `LEAD_INTELLIGENCE_DB_PATH` only when you intentionally need to
point MCP at a separate staging DB. Do not upload the unified DB to S3 until an
S3 backup has been created and reviewed.

Promotion must preserve the old production DB. Treat promotion as a
replace-with-backup operation:

1. Keep the timestamped local backup of the pre-promotion `reminders.db` in
   `outputs/db_backups/`.
2. Keep the timestamped S3 backup under `s3://notesreminder-db/backups/`.
3. Replace local `reminders.db` with the reconciled unified copy.
4. Upload the reconciled unified copy to the production S3 key only after the
   local replacement validates.
5. If rollback is needed, copy the preserved backup back to `reminders.db` and
   re-upload that same backup to the production S3 key.

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

Use this checklist before uploading any DB that has been touched by local authenticated lead refresh work. After the Phase 7 single-DB promotion, authenticated lead refresh writes to the canonical local `reminders.db` in shadow mode, with a local DB backup created before broad source refreshes. Do not upload a lead-refreshed DB to S3 until the gate checks pass.

1. Pull the latest Git state and confirm no unexpected tracked files are dirty.
2. Verify or download the latest S3 `reminders.db`.
3. Create a local backup of `reminders.db`.
4. Run the local authenticated HubSpot/Dialpad/Pike13/Gmail refresh scripts against `reminders.db`.
5. Run `python3 scripts/source_completeness_report.py --db reminders.db --window-days 7 --pike13-lookahead-days 30 --pretty`.
6. Generate the visible progress dashboard with `python3 scripts/progress_dashboard.py --db reminders.db --window-days 7 --pike13-lookahead-days 30`.
7. Generate the lead-attention report with `python3 scripts/lead_attention_report.py --db reminders.db --school "West U" --window-days 7`.
8. Run `sqlite3 reminders.db "PRAGMA integrity_check;"`.
9. Validate that existing `reminders` row counts and note-score columns are still intact.
10. Confirm browser profiles, screenshots, raw discovery evidence, local DB backups, and customer-data exports are uncommitted.
11. Upload/sync a lead-refreshed DB only after the source gate is green.

Production merge gate:

- Production notes run succeeds for both schools.
- Lead working DB includes the latest `reminders` rows and note scores.
- Dialpad daily intake is repeatable.
- Lead reports are useful and sanitized.
- Pike13 rich outcomes have a clear readiness status.
- Local backups exist for both the production DB and lead working DB before any merge attempt.
- An S3 backup exists before any DB upload.
- The old production DB backup remains preserved after promotion for rollback.
- A unified copy reconciles with no source-row gaps, no production-owned table count changes, and `PRAGMA integrity_check = ok`.
- We explicitly approve uploading the unified DB to the production S3 key.

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

Run the West U linked-lead Pike13 outcome proof against `reminders.db`. Use the repo venv for headed MFA login on this machine:

```bash
venv/bin/python scripts/extract_pike13_leads.py \
  --db reminders.db \
  --profile-dir browser_profiles/pike13 \
  --base-url https://westu-sor.pike13.com \
  --school "West U" \
  --limit 5 \
  --interactive-login
```

If the browser pauses at the login checkpoint, complete Pike13 MFA manually, then press Enter in the terminal. Do not upload `reminders.db` to production S3 after a blocked or interrupted Pike13 run.

## Dialpad daily intake and unmatched inbound

Daily Dialpad refresh uses Conversation History as the primary browser route. The default window is 2 days so the daily run has overlap; use 7 days for proof/backfill.

```bash
python3 scripts/extract_dialpad_daily_intake.py \
  --db reminders.db \
  --school "West U" \
  --window-days 2 \
  --limit 100 \
  --profile-dir browser_profiles/dialpad \
  --interactive-login
```

If Conversation History loads but returns no rows, or if expected controls are blocked, the command records the import as `partial` or `blocked` and runs Dialpad route discovery for repair diagnostics. Do not upload/sync a DB after a blocked browser refresh without reviewing the dashboard and route-discovery output.

Generate the sanitized unmatched inbound report:

```bash
python3 scripts/unmatched_inbound_report.py \
  --db reminders.db \
  --school "West U" \
  --window-days 2
```

The default output is `outputs/progress/unmatched_inbound_report.md`. It flags inbound Dialpad communications without a trusted HubSpot phone match, including possible leads not in HubSpot and rows with no later outbound follow-up. The report is count/status oriented and excludes customer names, phone numbers, SMS bodies, transcripts, recaps, raw notes, and call summaries.

## Dialpad call reviews and lead attention

After Dialpad Conversation History has loaded call-review URLs, ingest the transcript/recap/action-item evidence without downloading audio by default:

```bash
python3 scripts/extract_dialpad_call_reviews.py \
  --db reminders.db \
  --profile-dir browser_profiles/dialpad \
  --limit 25 \
  --interactive-login
```

The call-review URL remains the durable pointer for transcript and audio access. The extractor stores transcript/recap/action-item text in `dialpad_call_reviews`, but sanitized reports do not print that content.

The 2026-05-21 live route proof confirmed the authenticated Dialpad profile can open a `/callhistory/callreview/<id>` page and extract recap, action-item, audio-availability, and transcript-turn evidence. The default operating model remains transcript/recap ingestion first, with audio download reserved for missing-transcript, important, or QA-selected calls.

Generate the first value report:

```bash
python3 scripts/lead_attention_report.py \
  --db reminders.db \
  --school "West U" \
  --window-days 7
```

The default output is `outputs/progress/lead_attention_report.md`. It shows deal IDs, stages, owners, risk reasons, matched communication counts, call-review transcript/recap availability, and source URLs. It intentionally excludes customer names, phone numbers, SMS bodies, transcripts, transcript summaries, raw lesson notes, and call summaries.

## Person identity layer

Refresh deterministic person identities after source refreshes when cross-source person lookup or journey work is needed:

```bash
python3 scripts/refresh_person_identities.py \
  --db reminders.db \
  --json
```

The resolver links exact normalized email, exact normalized phone, HubSpot contact/deal IDs, Pike13 person IDs, Dialpad phone rows, and school-email addresses into `persons` and `person_identities`. Duplicate source identities are written to `person_resolution_conflicts` for review instead of being hidden.

MCP tools:

- `refresh_person_identity_layer`
- `person_search(query, limit)`
- `person_details(person_id)`
- `person_journey(search, start_date, end_date, limit, include_sensitive)`
- `customer_lifecycle_summary(person_id)`

`person_journey` is sanitized by default and omits message bodies, transcript text, raw source URLs, and raw detail JSON unless `include_sensitive=true` is explicitly passed.

## Scripts overview
- `run_daily.py` : Scrape Pike13 lessons, update `reminders.db`, email summary, sync to S3.
- `backfill.py` : Multi-school historical scrape (no email by default).
- `scripts/run_notes_local_mfa.sh` : Local two-school production notes runner for Pike13 MFA periods.
- `scripts/rebuild_lead_working_db.py` : Rebuild ignored local lead working DB from production notes plus additive lead tables.
- `scripts/migrate_lead_intel_to_production.py` : Build and reconcile a unified production DB copy from `reminders.db` plus lead-intelligence tables.
- `import_call_data.py` : Import Dialpad + Pike13 client CSVs, build call matches, and refresh `call_logs`.
- `generate_call_reports.py` : Write voicemail/missed-call CSVs from call data.
- `build_reporting_schema.py` : Create/backfill reporting tables and views (`lessons`, `lesson_students`, `vw_note_quality_league_table`, etc.).
- `transcribe_recordings.py` : Download recordings, transcribe with AWS, store in `recording_transcripts`.
- `download_recordings_playwright.py` : Download Dialpad recordings via a logged-in browser session.
- `transcribe_recordings_whisper.py` : Transcribe local recordings with Whisper (CPU).
- `analyze_transcripts_openai.py` : Add intent/sentiment/topic/outcome tags via OpenAI.
- `scripts/daily_scrape.sh` : Shell wrapper for `run_daily.py`.
- `scripts/import_call_logs.sh` : Shell wrapper for `import_call_data.py`.
- `scripts/generate_reports.sh` : Shell wrapper for `generate_call_reports.py`.
- `scripts/progress_dashboard.py` : Generate the sanitized lead-intelligence readiness dashboard.
- `scripts/extract_dialpad_daily_intake.py` : Load recent Dialpad Conversation History rows with route-discovery fallback on failure.
- `scripts/extract_dialpad_call_reviews.py` : Ingest Dialpad call-review transcripts, recaps, action items, and access diagnostics.
- `scripts/unmatched_inbound_report.py` : Generate the sanitized unmatched inbound Dialpad report.
- `scripts/discover_pike13_routes.py` : Probe Pike13 routes and record sanitized route-capability diagnostics.
- `scripts/refresh_person_identities.py` : Rebuild deterministic `persons`, `person_identities`, and conflict rows.
- `scripts/lead_attention_report.py` : Generate the sanitized West U lead-attention report.
- `scripts/lead_operating_dashboard.py` : Generate sanitized daily, weekly, and monthly shadow operating dashboards from `reminders.db`.
- `scripts/management_scorecards.py` : Generate sanitized school and instructor note-quality scorecards in shadow mode.
- `scripts/cadence_runner.py` : Dry-run or run the approved shadow cadence scaffold and write run metadata.
- `scripts/replay_parse.py` : Replay supported local raw captures into a scratch DB for parser regression checks.
- `scripts/communication_insights.py` : Generate experimental, human-review communication insights with evidence links.
- `scripts/update_all.sh` : End-to-end pipeline runner (scrape, import, reports).
- `scripts/smoke_test.sh` : Quick env/dependency check (no scrape).

`run_daily.py` now syncs normalized reporting tables from `reminders` after notes writes by default. Use `--skip-reporting-sync` only for rollback/debugging of the shadow dual-write path; do not retire `reminders` or cut reports over without explicit approval.

Compare legacy `reminders` reads against normalized notes reads without cutting production reports over:

```bash
python3 scripts/notes_read_path_comparison.py \
  --db reminders.db \
  --start-date 2026-05-16 \
  --end-date 2026-05-22
```

The comparison checks base row coverage, school/day counts, instructor missing-note counts, and note-quality league-table rows. A nonzero mismatch exits with failure.

## Shadow operating dashboards

Generate daily, weekly, and monthly shadow dashboards from the canonical local database:

```bash
python3 scripts/lead_operating_dashboard.py \
  --db reminders.db \
  --school "West U" \
  --period all \
  --as-of YYYY-MM-DD \
  --output-dir outputs/progress/lead_operating_dashboard_westu
```

The dashboards include funnel counts, communication coverage, exception queues,
recording/transcription coverage, and notes-operation metrics from normalized
tables: reportable lessons, completed notes, missing notes, completion rate, and
league score. These dashboards are sanitized by default and remain shadow mode
until Hugh approves them for the normal management cadence.

## Shadow management scorecards

Generate note-quality league tables for school-vs-school and instructor
comparison:

```bash
python3 scripts/management_scorecards.py \
  --db reminders.db \
  --period mtd \
  --as-of YYYY-MM-DD \
  --output-dir outputs/progress/management_scorecards
```

Use `--school "West U"` or `--school "The Heights"` for a single-school
instructor league. Use `--period prior-week`, `--period prior-month`, or
`--period custom --start-date YYYY-MM-DD --end-date YYYY-MM-DD` for other
windows. The scorecard uses the same normalized reportable-lesson flag as the
notes operations dashboard: no-note lessons score `0`; scored notes contribute
`note_score / 10`; league score is `SUM(score_component) / reportable_lessons *
100`. Group and multi-student lessons remain excluded. The output is sanitized
and remains shadow mode until scorecard publication is approved.

## Raw capture and replay

Authenticated extractors can now save raw local evidence before or alongside
parsing. Raw files are written under git-ignored `raw/{source}/{YYYY-MM-DD}/`,
indexed in `raw_captures`, and retained locally for 90 days by default. Raw
captures are not archived to S3 unless that policy is approved later.

Replay supported captures into a scratch DB:

```bash
python3 scripts/replay_parse.py \
  --source-db reminders.db \
  --scratch-db outputs/progress/replay_scratch.db \
  --capture-type hubspot_deal_text
```

Supported replay types currently include HubSpot deal detail text and selected
Pike13 person/related-page text. Unsupported raw capture types remain indexed
for parser development and can be replay-enabled later.

## Experimental communication insights

Generate sanitized, human-review insight samples from stored communication text:

```bash
python3 scripts/communication_insights.py \
  --db reminders.db \
  --start-date YYYY-MM-DD \
  --end-date YYYY-MM-DD \
  --school "West U" \
  --limit 10 \
  --output-dir outputs/progress/communication_insights
```

The script stores rows in `communication_ai_insights` unless `--dry-run` is
used. Each row keeps model, prompt version, run ID, review status, and an
evidence JSON pointer back to the source table/source ID. Broad review output is
sanitized and excludes customer names, emails, phones, message bodies,
transcripts, source URLs, raw notes, and audio paths. These insights remain
experimental until Hugh reviews sample usefulness and approves any staff-facing
workflow.

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
The current Phase 16 scheduler path is a scaffold, not an enabled unattended
production schedule. Use it first in dry-run or shadow-report mode:

```bash
python3 scripts/cadence_runner.py --date YYYY-MM-DD
python3 scripts/cadence_runner.py --date YYYY-MM-DD --execute-shadow
python3 scripts/cadence_runner.py --date YYYY-MM-DD --simulate-expired-auth
```

The runner writes metadata under `outputs/progress/cadence_runs/`. Shadow
execution can generate health reports, source completeness, lead operating
dashboards, and note-quality scorecards. The production notes/email path is
listed in the plan but skipped unless `--execute-production` is passed after
explicit Hugh approval.

Do not install or load a production `launchd`/cron job until the unattended
production run promotion gate is approved.

Historical `launchd` plist template only (`~/Library/LaunchAgents/com.notesreminder.daily.plist`):

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

After approval, load an updated plist:

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

Phase 8 reporting views:

- `vw_missing_notes_by_instructor`
- `vw_note_completion_rate`
- `vw_missing_notes_by_school_day`
- `vw_note_quality_league_table`
- `vw_callback_speed`
- `vw_churn_candidates`

These views use the same private reportable lesson filter as the current notes
email logic; group lessons remain excluded from instructor note-quality and
missing-note league tables.

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
