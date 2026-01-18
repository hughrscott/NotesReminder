# NotesReminder Data Pipeline Plan

1) Make `reminders.db` the single source of truth
   - Always update locally, then sync to S3.
   - Don’t edit multiple copies in parallel.

2) Standardize the workflow (daily + weekly)
   - Daily/weekly scrape: `run_daily.py` for each school
   - Monthly/quarterly backfill: `backfill.py`
   - Call log + client import: `import_call_data.py`
   - Reporting: `generate_call_reports.py` or MCP queries

3) Put scripts in a clear pipeline order
   1. `run_daily.py` (notes + attendance)
   2. `import_call_data.py` (Dialpad + Pike13)
   3. `generate_call_reports.py` (analysis outputs)
   4. MCP queries (insights)

4) Make the CLI repeatable
   - Create a `scripts/` folder with shell wrappers (optional):
     - `scripts/daily_scrape.sh`
     - `scripts/import_call_logs.sh`
     - `scripts/generate_reports.sh`

5) Use a schedule
   - macOS cron or launchd to run daily/weekly.
   - Keep logs in `logs/` with timestamps.

6) Document the source of data
   - A short `docs/data_pipeline.md` explaining:
     - Where files live (`Call Log/`, `ClientList/`)
     - Which scripts to run
     - Where outputs go
     - How to update the MCP database

7) Minimal data hygiene
   - Keep new CSVs in dated folders.
   - Delete old DB copies; use S3 as backup.

8) Make the database more business friendly
   - Draft reporting schema (additive-first, keep `reminders` as raw source):
     - `schools` (school_id, school_code, school_name)
     - `instructors` (instructor_id, instructor_name, school_id)
     - `students` (student_id, student_name, school_id)
     - `lessons` (lesson_id, pike13_lesson_id, school_id, instructor_id, lesson_date, lesson_time, lesson_type, location)
     - `lesson_students` (lesson_id, student_id, is_primary)
     - `lesson_notes` (lesson_id, note_completed, notes_text, note_timestamp)
     - `lesson_attendance` (lesson_id, attendance_status)
     - `call_logs` (call_id, external_number, date_started, direction, category, name, is_internal, school_id)
     - `call_client_matches` (call_id, client_id, match_type, confidence, match_value, matched_on)
   - Add reporting views:
     - `vw_missing_notes_by_instructor`
     - `vw_note_completion_rate`
     - `vw_missing_notes_by_school_day`
     - `vw_callback_speed` (voicemail + missed calls)
     - `vw_churn_candidates` (based on lesson cadence rules)
   - Add stable business keys + display labels (e.g., `school_code` + `school_name`).
   - Prefer additive changes first (new tables/views), then migrate if needed.
   - Backfill approach: create new tables from `reminders` + call tables, keep legacy tables intact.

9) Plan for call recording transcription (separate pipeline)
   - Add a `recording_transcripts` table keyed by `call_id` to store:
     - recording_url, transcript_text, transcript_provider, transcript_confidence, created_at
   - Separate job: download each `dialpad_recordings.recording_url`, transcribe, and persist results.
   - Keep raw call data intact; link transcripts via `call_id` only.
