# NotesReminder

## Required Environment Variables
Set these before running any scripts (e.g., in a local `.env` file that `dotenv` loads or via `export` in your shell):
Copy the template once with `cp .env.example .env`, then fill in your real values.

| Variable | Purpose |
| --- | --- |
| `PIKE13_USER` | Pike13 login email used by the Playwright scraper. |
| `PIKE13_PASSWORD` | Pike13 login password. |
| `AWS_ACCESS_KEY_ID` | AWS credential for syncing `reminders.db` to S3. |
| `AWS_SECRET_ACCESS_KEY` | Matching AWS secret. |
| `AWS_DEFAULT_REGION` | Region for the S3 client (e.g., `us-east-1`). |
| `OPENAI_API_KEY` | API key for OpenAI |
| `SENDER_EMAIL` | SMTP sender email address for the summary report. |
| `SENDER_PASSWORD` | SMTP password or app-specific password for the sender email. |

Optional: update `run_daily.py` if you need different SMTP server settings or recipient emails.

## Quick map
- `run_daily.py` : scrape Pike13 lessons, update `reminders.db`, email summary, sync to S3
- `noteschecker.py` : Playwright scraper used by `run_daily.py`
- `backfill.py` : multi-school historical scrape
- `import_call_data.py` : import Dialpad + Pike13 client CSVs, build call matches, refresh `call_logs`
- `generate_call_reports.py` : write voicemail/missed-call CSVs from call data
- `build_reporting_schema.py` : create/backfill reporting tables (`lessons`, `lesson_students`, etc.)
- `transcribe_recordings.py` : download recording URLs, transcribe with AWS, store in `recording_transcripts`
- `transcribe_recordings_whisper.py` : transcribe local recordings with Whisper (CPU)
- `transcribe_recordings_openai.py` : transcribe downloaded recordings with OpenAI Whisper
- `scripts/rebuild_recording_downloads.py` : rebuild `recording_downloads` from local `recordings/` files
- `analyze_transcripts_openai.py` : tag transcripts with intent/sentiment/topic/outcome via OpenAI
- `download_recordings_playwright.py` : download Dialpad recordings using a logged-in browser session
- `scripts/` : shell wrappers for the above and an end-to-end `update_all.sh`
- `docs/data_pipeline.md` : pipeline order, scheduling, sanity checks

## Pipeline architecture
1) Notes pipeline (daily + backfill)
   - `run_daily.py` (scrape + DB update + email + S3 sync)
   - `backfill.py` (multi-school historical run)
2) Call data pipeline (import + reports)
   - `import_call_data.py` (Dialpad + Pike13 import, matches, `call_logs`)
   - `generate_call_reports.py` (voicemail/missed-call reports)
3) Reporting schema pipeline
   - `build_reporting_schema.py` (business-friendly tables + lesson group flags)
4) Recording pipeline (download + transcribe + analyze)
   - `download_recordings_playwright.py` (browser-auth download)
   - `transcribe_recordings_whisper.py` (local Whisper)
   - `transcribe_recordings_openai.py` (OpenAI Whisper API)
   - `transcribe_recordings.py` (AWS Transcribe, optional)
   - `analyze_transcripts_openai.py` (LLM tags)
5) MCP query pipeline
   - `mcp_server.py` (read-only queries + import)

## Start here
1) Copy `.env.example` to `.env` and fill in credentials.
2) Install deps: `pip install -r requirements.txt` and `playwright install`.
   For local development and tests, use `pip install -r requirements-dev.txt`.
3) Run a daily scrape: `python run_daily.py --school westu-sor --start-date YYYY-MM-DD --end-date YYYY-MM-DD --to you@example.com`.
4) Import calls/clients: `python3 import_call_data.py --clients ClientList/your_clients.csv --dialpad-dir "Call Log" --db reminders.db`.
5) Build reporting tables: `python3 build_reporting_schema.py --db reminders.db`.
6) (Optional) Transcribe recordings: `python3 transcribe_recordings.py --bucket YOUR_BUCKET --delete-after`.
7) (Optional) Download recordings via browser: `python3 download_recordings_playwright.py --out-dir recordings --limit 5` (use `--all` for full download).
8) (Optional) Transcribe local recordings: `python3 transcribe_recordings_whisper.py --recordings-dir recordings --model small`.
8b) (Optional) Transcribe recordings via OpenAI: `python3 transcribe_recordings_openai.py --limit 100`.
9) (Optional) Analyze transcripts: `python3 analyze_transcripts_openai.py --model gpt-4o-mini --limit 10`.

If `recording_downloads` is missing but `recordings/` exists, rebuild it:
```bash
python3 scripts/rebuild_recording_downloads.py --recordings-dir recordings
```

Generated files:
- `outputs/` holds CSV exports and screenshots (e.g., `missed_signup_leads.csv`).
- `notebooks/` holds Jupyter notebooks.
- `archive/` holds legacy or backup files.

`transcribe_recordings_whisper.py` skips any `call_id` that already exists in
`recording_transcripts`. Use `--force` to re-transcribe.

Idempotency:
- Downloads skip `call_id` values already marked `success` in `recording_downloads`.
- Local transcription skips any `call_id` already present in `recording_transcripts` unless `--force`.
- Transcript analysis skips rows that already have `intent` unless `--force`.

Hardware acceleration (Apple Silicon):
```bash
python3 transcribe_recordings_whisper.py --device mps --workers 1 --model small
```

CPU parallelism (use smaller models to avoid memory pressure):
```bash
python3 transcribe_recordings_whisper.py --device cpu --workers 4 --model base
```

## CLI Usage
Run the help command anytime to see the full synopsis:

```bash
python run_daily.py --help
```

Key flags:

| Flag | Description |
| --- | --- |
| `--school` | Pike13 subdomain (default `westu-sor`). |
| `--start-date`, `--end-date` | Date range in `YYYY-MM-DD`. Defaults to the past 7 days if omitted. |
| `--init-db` | Rebuilds `reminders.db` using `init_db.py` and uploads it to S3. |
| `--verbose` | Enables detailed logging (Playwright progress, AWS sync info). |
| `--summary` | Controls email content: `none` (missing only), `notes`, `missing`, or `both`. |
| `--to` | Required list of primary recipients for the summary email. |
| `--cc` | Optional list of CC email addresses. |
| `--no-email` | Skip sending the summary email (useful for backfills). |
| `--skip-note-scoring` | Skip LLM note scoring for this run. |
| `--note-score-model` | Model used for note scoring (default `gpt-4o-mini`). |
| `--note-score-version` | Version label stored with each score. |

## What the Project Does
This repo automates “missing lesson notes” reminders for School of Rock locations:

1. `run_daily.py` is the main entry point. It:
   - Downloads `reminders.db` from S3 (or initializes it with `--init-db`).
   - Runs the Playwright scraper (`noteschecker.py`) for the requested school/date window to capture lesson details, note status, attendance, and room locations.
   - Updates the SQLite database with the latest scrape results.
   - Stores full note text and timestamps in the database for analysis.
   - Scores each completed note (1-10) and stores explanation text in DB fields (`note_score`, `note_score_explanation`).
   - Stores the Pike13 lesson id for stable uniqueness across schools.
   - Filters single-student lessons without notes for the selected dates and emails a grouped HTML/plain-text report through the configured SMTP server.
   - Re-uploads the refreshed database to S3 so subsequent runs stay in sync.
   - Optional flags: `--verbose` for detailed logging, `--summary` (`notes`, `missing`, or `both`) for CLI summaries, and `--init-db` to rebuild/upload an empty database.

2. `noteschecker.py` performs the Pike13 scrape using Playwright, taking screenshots/trace files for debugging and writing a CSV per run.

3. `init_db.py` (or `notesreminder.py`) creates the SQLite schema when you need a fresh `reminders.db`.

4. `instructormapping.py` provides instructor contact details if you extend the workflow to notify teachers directly.

## Session Notes
Use `docs/SESSION_NOTES.md` to resume the latest state and next steps.

## Tests
Install the development requirements, then run the active test suite from the repo root:

```bash
python -m pytest
```

`pytest.ini` limits normal collection to `tests/` and adds the repo root to the import path. Archived legacy tests under `archive/` are not part of the normal baseline.

## Running a Report
Install dependencies once:

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt  # optional, for tests
playwright install
```

If `llvmlite`/`numba` try to build from source, force wheels:

```bash
pip install --only-binary=:all: llvmlite numba
```

Example command (quiet mode):

```bash
python run_daily.py --school westu-sor --start-date 2025-06-18 --end-date 2025-06-18 \
  --to you@example.com manager@example.com
```

Add `--summary both` for CLI summaries or `--verbose` for scrape-level logs. After a successful run you’ll receive an email report and see the database synced back to S3.

## Backfill (multiple schools)
Use `backfill.py` to run a date range for both schools without sending email.

```bash
python backfill.py --start-date 2025-01-01 --end-date 2025-12-31 --no-email
```

## Dialpad + Pike13 import
CLI:
```bash
python import_call_data.py \
  --clients ClientList/pike13_2026-01-17_client_Clients__Clients__4f04c494-7049-4228-80c8-30b780f0fd26.csv \
  --dialpad-dir "Call Log" \
  --db reminders.db
```

MCP:
- Call `import_call_data` with `clients_csv` and optionally `dialpad_dir`/`db_path`.

Help:
```bash
python import_call_data.py --help
```

Notes:
- If MCP reports missing files, pass absolute paths (e.g., `/Users/.../NotesReminder/Call Log`).

## Call log visualizations
Generate four graphs from Dialpad call logs (business-hours callback time and outside-hours counts):

```bash
python call_viz.py --db reminders.db --out-dir call_viz_output
```

## MCP Server (Claude Desktop)
This repo includes a small MCP server that lets you query the synced SQLite database.

Install the MCP dependency:

```bash
pip install mcp
```

Claude Desktop config (macOS):

`~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "notesreminder": {
      "command": "python",
      "args": [
        "/Users/hughscott/Documents/Coding/NotesReminder/mcp_server.py"
      ],
      "env": {
        "AWS_ACCESS_KEY_ID": "YOUR_KEY",
        "AWS_SECRET_ACCESS_KEY": "YOUR_SECRET",
        "AWS_DEFAULT_REGION": "us-east-1",
        "REMINDERS_DB_PATH": "/Users/hughscott/Documents/Coding/NotesReminder/reminders_mcp.db",
        "REMINDERS_S3_BUCKET": "notesreminder-db",
        "REMINDERS_S3_KEY": "reminders.db"
      }
    }
  }
}
```

To avoid overwriting your working DB, publish a copy for MCP:
```bash
./scripts/publish_mcp_db.sh
```

To upload the latest working DB to S3 (so Claude sync sees it):
```bash
python3 scripts/publish_db_to_s3.py --db reminders.db
```

Before replacing a local DB from any downloaded/synced copy, run the guard:
```bash
# Verify incoming DB is not unexpectedly older/smaller
python3 scripts/db_guard.py verify --current reminders.db --incoming /tmp/incoming_reminders.db

# If allowed, backup + replace in one step
python3 scripts/db_guard.py replace --current reminders.db --incoming /tmp/incoming_reminders.db
```

## Legacy Score Recovery
Recover historical lesson-note scores from backup/cloud DB snapshots:

```bash
# 1) Inspect local DB files for score-like columns
python3 scripts/recover_legacy_scores.py discover \
  --paths reminders.db reminders_mcp.db reminders.db.BAK2 reminders.dbBAK

# 2) Discover candidate S3 snapshots/versions
python3 scripts/discover_db_sources.py --bucket notesreminder-db --key reminders.db

# 3) Compare a scored source DB against current reminders.db
python3 scripts/recover_legacy_scores.py compare --current-db reminders.db --source-db /path/to/scored_snapshot.db

# 4) Extract matched + unmatched rows for manual validation
python3 scripts/recover_legacy_scores.py extract --current-db reminders.db --source-db /path/to/scored_snapshot.db

# 5) Import matched rows into lesson_note_scores_history (idempotent)
python3 scripts/merge_legacy_scores.py \
  --db reminders.db \
  --matched-csv outputs/matched_legacy_scores.csv \
  --source-db /path/to/scored_snapshot.db

# 6) Run verification query pack
sqlite3 reminders.db < scripts/sql/verify_scores.sql
```

Default unmatched output:
- `outputs/unmatched_legacy_scores.csv`

Available tools:
- `sync_db_from_s3` downloads the latest DB from S3.
- `db_status` reports local DB status.
- `list_tables` and `describe_table` help explore the schema.
- `query_sql` runs read-only SELECT queries.
- `import_call_data` imports Dialpad + Pike13 client CSVs and rebuilds call matches.

Starter queries (use with `query_sql`):
```sql
-- Top instructors by missing notes
SELECT instructor_name, COUNT(*) AS missing_notes
FROM reminders
WHERE note_completed = 0
GROUP BY instructor_name
ORDER BY missing_notes DESC
LIMIT 20;

-- Missing notes by school and date
SELECT school, lesson_date, COUNT(*) AS missing_notes
FROM reminders
WHERE note_completed = 0
GROUP BY school, lesson_date
ORDER BY lesson_date DESC, missing_notes DESC
LIMIT 50;

-- Note completion rate by instructor
SELECT instructor_name,
       ROUND(100.0 * SUM(CASE WHEN note_completed = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) AS completion_rate,
       COUNT(*) AS total_lessons
FROM reminders
GROUP BY instructor_name
HAVING total_lessons >= 5
ORDER BY completion_rate ASC, total_lessons DESC
LIMIT 20;

-- Missing notes by lesson type
SELECT lesson_type, COUNT(*) AS missing_notes
FROM reminders
WHERE note_completed = 0
GROUP BY lesson_type
ORDER BY missing_notes DESC
LIMIT 20;

-- Recent lessons with missing notes
SELECT lesson_date, lesson_time, instructor_name, students, lesson_type, location
FROM reminders
WHERE note_completed = 0
ORDER BY lesson_date DESC, lesson_time DESC
LIMIT 50;
```

Churn/retention queries (assumes `students` contains a single name for 1-1 lessons, and uses the latest lesson date in the DB as "today"):
```sql
-- Define churned students:
-- at least 4 distinct weeks with 1-1 lessons, and no lessons in the last 3 weeks.
WITH params AS (
  SELECT 4 AS min_weeks, 3 AS inactive_weeks
),
max_date AS (
  SELECT MAX(lesson_date) AS max_lesson_date FROM reminders
),
student_weeks AS (
  SELECT
    students AS student_name,
    strftime('%Y-%W', lesson_date) AS lesson_week
  FROM reminders
  WHERE students IS NOT NULL
    AND students NOT LIKE '%,%'
),
week_counts AS (
  SELECT student_name, COUNT(DISTINCT lesson_week) AS weeks_1to1
  FROM student_weeks
  GROUP BY student_name
),
last_lesson AS (
  SELECT students AS student_name, MAX(lesson_date) AS last_date
  FROM reminders
  WHERE students IS NOT NULL
  GROUP BY students
),
churned AS (
  SELECT w.student_name, w.weeks_1to1, l.last_date
  FROM week_counts w
  JOIN last_lesson l ON l.student_name = w.student_name
  JOIN params p
  JOIN max_date m
  WHERE w.weeks_1to1 >= p.min_weeks
    AND date(l.last_date) <= date(m.max_lesson_date, '-' || (p.inactive_weeks * 7) || ' days')
)
SELECT * FROM churned
ORDER BY last_date DESC
LIMIT 200;

-- Teachers most associated with churned students (by lesson count)
WITH params AS (
  SELECT 4 AS min_weeks, 3 AS inactive_weeks
),
max_date AS (
  SELECT MAX(lesson_date) AS max_lesson_date FROM reminders
),
student_weeks AS (
  SELECT
    students AS student_name,
    strftime('%Y-%W', lesson_date) AS lesson_week
  FROM reminders
  WHERE students IS NOT NULL
    AND students NOT LIKE '%,%'
),
week_counts AS (
  SELECT student_name, COUNT(DISTINCT lesson_week) AS weeks_1to1
  FROM student_weeks
  GROUP BY student_name
),
last_lesson AS (
  SELECT students AS student_name, MAX(lesson_date) AS last_date
  FROM reminders
  WHERE students IS NOT NULL
  GROUP BY students
),
churned AS (
  SELECT w.student_name
  FROM week_counts w
  JOIN last_lesson l ON l.student_name = w.student_name
  JOIN params p
  JOIN max_date m
  WHERE w.weeks_1to1 >= p.min_weeks
    AND date(l.last_date) <= date(m.max_lesson_date, '-' || (p.inactive_weeks * 7) || ' days')
),
teacher_counts AS (
  SELECT r.students AS student_name, r.instructor_name, COUNT(*) AS lessons
  FROM reminders r
  JOIN churned c ON c.student_name = r.students
  GROUP BY r.students, r.instructor_name
),
ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY student_name ORDER BY lessons DESC) AS rn
  FROM teacher_counts
)
SELECT instructor_name, COUNT(*) AS students, SUM(lessons) AS lessons
FROM ranked
WHERE rn = 1
GROUP BY instructor_name
ORDER BY students DESC, lessons DESC
LIMIT 50;

-- Compare note completion for churned vs active students (by instructor)
WITH params AS (
  SELECT 4 AS min_weeks, 3 AS inactive_weeks
),
max_date AS (
  SELECT MAX(lesson_date) AS max_lesson_date FROM reminders
),
student_weeks AS (
  SELECT
    students AS student_name,
    strftime('%Y-%W', lesson_date) AS lesson_week
  FROM reminders
  WHERE students IS NOT NULL
    AND students NOT LIKE '%,%'
),
week_counts AS (
  SELECT student_name, COUNT(DISTINCT lesson_week) AS weeks_1to1
  FROM student_weeks
  GROUP BY student_name
),
last_lesson AS (
  SELECT students AS student_name, MAX(lesson_date) AS last_date
  FROM reminders
  WHERE students IS NOT NULL
  GROUP BY students
),
churned AS (
  SELECT w.student_name
  FROM week_counts w
  JOIN last_lesson l ON l.student_name = w.student_name
  JOIN params p
  JOIN max_date m
  WHERE w.weeks_1to1 >= p.min_weeks
    AND date(l.last_date) <= date(m.max_lesson_date, '-' || (p.inactive_weeks * 7) || ' days')
),
active AS (
  SELECT l.student_name
  FROM last_lesson l
  JOIN max_date m
  JOIN params p
  WHERE date(l.last_date) > date(m.max_lesson_date, '-' || (p.inactive_weeks * 7) || ' days')
),
tagged AS (
  SELECT r.*,
         CASE
           WHEN r.students IN (SELECT student_name FROM churned) THEN 'churned'
           WHEN r.students IN (SELECT student_name FROM active) THEN 'active'
           ELSE 'other'
         END AS cohort
  FROM reminders r
)
SELECT cohort,
       instructor_name,
       COUNT(*) AS lessons,
       ROUND(100.0 * SUM(CASE WHEN note_completed = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) AS note_completion_rate
FROM tagged
WHERE cohort IN ('churned', 'active')
GROUP BY cohort, instructor_name
ORDER BY cohort, note_completion_rate ASC, lessons DESC
LIMIT 100;

-- Churn propensity: students with only 1-1 lessons vs mixed (1-1 + group)
WITH params AS (
  SELECT 4 AS min_weeks, 3 AS inactive_weeks
),
max_date AS (
  SELECT MAX(lesson_date) AS max_lesson_date FROM reminders
),
student_weeks AS (
  SELECT
    students AS student_name,
    strftime('%Y-%W', lesson_date) AS lesson_week
  FROM reminders
  WHERE students IS NOT NULL
    AND students NOT LIKE '%,%'
),
week_counts AS (
  SELECT student_name, COUNT(DISTINCT lesson_week) AS weeks_1to1
  FROM student_weeks
  GROUP BY student_name
),
last_lesson AS (
  SELECT students AS student_name, MAX(lesson_date) AS last_date
  FROM reminders
  WHERE students IS NOT NULL
  GROUP BY students
),
churned AS (
  SELECT w.student_name
  FROM week_counts w
  JOIN last_lesson l ON l.student_name = w.student_name
  JOIN params p
  JOIN max_date m
  WHERE w.weeks_1to1 >= p.min_weeks
    AND date(l.last_date) <= date(m.max_lesson_date, '-' || (p.inactive_weeks * 7) || ' days')
),
student_mix AS (
  SELECT
    students AS student_name,
    MAX(CASE WHEN students LIKE '%,%' THEN 1 ELSE 0 END) AS has_group,
    MAX(CASE WHEN students NOT LIKE '%,%' THEN 1 ELSE 0 END) AS has_1to1
  FROM reminders
  WHERE students IS NOT NULL
  GROUP BY students
),
cohorts AS (
  SELECT
    m.student_name,
    CASE
      WHEN m.has_1to1 = 1 AND m.has_group = 0 THEN 'only_1to1'
      WHEN m.has_1to1 = 1 AND m.has_group = 1 THEN 'mixed'
      ELSE 'group_only'
    END AS lesson_mix,
    CASE WHEN m.student_name IN (SELECT student_name FROM churned) THEN 1 ELSE 0 END AS is_churned
  FROM student_mix m
)
SELECT lesson_mix,
       COUNT(*) AS students,
       SUM(is_churned) AS churned_students,
       ROUND(100.0 * SUM(is_churned) / COUNT(*), 1) AS churn_rate
FROM cohorts
GROUP BY lesson_mix
ORDER BY churn_rate DESC;
```
