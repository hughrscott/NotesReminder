# Session Notes (Resume Here)

Last updated: 2026-05-20

## 2026-05-20 Phase 7 Copy-Mode Reconciliation

- Created local SQLite backups before Phase 7 work:
  - `outputs/db_backups/reminders.db.20260520-140002.before-phase-7-unified-db.bak`
  - `outputs/db_backups/lead_intelligence_working.db.20260520-140002.before-phase-7-unified-db.bak`
- Created S3 backup copy with boto3 after loading `.env` credentials:
  - `s3://notesreminder-db/backups/reminders.db.20260520-140002.before-phase-7-unified-db.bak`
  - size: `73,457,664` bytes
- Promotion rollback rule:
  - Do not delete or overwrite the old production DB backup after promotion.
  - If rollback is needed, restore `outputs/db_backups/reminders.db.20260520-140002.before-phase-7-unified-db.bak` to local `reminders.db`.
  - If the unified DB has been uploaded to S3, re-upload the preserved backup to `s3://notesreminder-db/reminders.db`.
- Added copy-first migration/reconciliation command:

```bash
venv/bin/python scripts/migrate_lead_intel_to_production.py \
  --production-db reminders.db \
  --lead-db outputs/lead_intelligence/lead_intelligence_working.db \
  --output outputs/lead_intelligence/unified_reminders_phase7.db \
  --json
```

- Real copy-mode reconciliation result:
  - Status: `ready`
  - Integrity: `ok`
  - Production-owned table count changes: none
  - Missing source rows: 0 for every copied/merged lead table
  - Shared recording tables merged to `recording_downloads = 4249` and `recording_transcripts = 4248`
- Idempotency check passed by running the same migration again against `outputs/lead_intelligence/unified_reminders_phase7.db`.
- Test and validation results:
  - `venv/bin/python -m pytest`: `92 passed`
  - `sqlite3 reminders.db "PRAGMA integrity_check;"`: `ok`
  - `sqlite3 outputs/lead_intelligence/unified_reminders_phase7.db "PRAGMA integrity_check;"`: `ok`
  - Notes health on unified copy: `ready`
  - MCP daily/weekly/monthly dashboard baseline comparison: matched lead working DB
- Source-completeness report on the unified copy still shows the pre-existing Dialpad readiness blocker, so Phase 7 is reconciled but production promotion should wait for the explicit promotion decision.
- MCP now defaults lead dashboard tools to `reminders.db` after promotion. Use `LEAD_INTELLIGENCE_DB_PATH` only to point MCP at a separate staging DB.
- Before final Phase 7 production promotion, Hugh needs to approve replacing local `reminders.db` and uploading the reconciled unified DB to `s3://notesreminder-db/reminders.db`. The old production DB backup must remain preserved for rollback.

## 2026-05-20 Phase 8 Business-Friendly Reporting Schema

- Phase 7 was tagged as `phase-7-promotion-pending-20260520`; production DB promotion remains pending Hugh approval.
- Created local DB backup before additive reporting schema changes:
  - `outputs/db_backups/reminders.db.20260520-141259.before-phase-8-reporting-schema.bak`
- Extended `build_reporting_schema.py` with idempotent schema upgrades and views:
  - `vw_missing_notes_by_instructor`
  - `vw_note_completion_rate`
  - `vw_missing_notes_by_school_day`
  - `vw_note_quality_league_table`
  - `vw_callback_speed`
  - `vw_churn_candidates`
- Added `lesson_notes` note-score fields and `lessons.lesson_is_reportable` using the current private reportable lesson filter. Group lessons remain excluded from note-quality and missing-note league tables.
- Ran `build_reporting_schema.py --db reminders.db` twice successfully.
- Reconciliation:
  - `distinct_reminder_lessons = 16979`
  - `lessons = 16979`
  - `lesson_notes = 16979`
  - `lesson_attendance = 16979`
  - West U May 2026 note-quality view matched direct calculation: `486` reportable lessons, `56.4` league score.
  - The Heights May 2026 note-quality view matched direct calculation: `339` reportable lessons, `41.8` league score.
  - May 19 daily parity matched legacy filter for both schools.
- Validation:
  - `sqlite3 reminders.db "PRAGMA integrity_check;"`: `ok`
  - `venv/bin/python -m pytest`: `93 passed`

## 2026-05-21 Unified DB Daily Notes Validation

- Added a safe local-only daily runner path:
  - `--db-path` selects the SQLite file to read/write.
  - `--skip-s3-sync` skips both S3 download and S3 upload.
- Created daily-test copy from the unified DB candidate:
  - `outputs/lead_intelligence/unified_reminders_phase7_daily_test.db`
- Ran local-only notes checker against the daily-test DB with `--no-email`, `--skip-note-scoring`, and `--skip-s3-sync`:
  - West U, 2026-05-19: Pike13 login succeeded, 39 schedule lessons processed, no S3 upload.
  - The Heights, 2026-05-19: Pike13 login succeeded, 41 schedule lessons processed, no S3 upload.
- Post-run validation on the daily-test DB:
  - `PRAGMA integrity_check`: `ok`
  - `build_reporting_schema.py`: passed
  - `notes_pipeline_health.py`: `ready`
  - `venv/bin/python -m pytest`: `93 passed`
- Promotion candidate for the actual DB replacement should be the daily-tested DB with reporting rebuilt:
  - `outputs/lead_intelligence/unified_reminders_phase7_daily_test.db`

## 2026-05-21 Unified DB Promotion

- Created fresh rollback backups immediately before promotion:
  - local: `outputs/db_backups/reminders.db.20260521-141611.before-unified-db-promotion.bak`
  - S3: `s3://notesreminder-db/backups/reminders.db.20260521-141611.before-unified-db-promotion.bak`
- Replaced local `reminders.db` with the daily-tested unified DB candidate.
- Ran local-only notes checker against promoted local `reminders.db` for 2026-05-20 before S3 upload:
  - West U: 35 schedule lessons processed, no email, no S3 sync.
  - The Heights: 30 schedule lessons processed, no email, no S3 sync.
- Rebuilt reporting schema after the May 20 local-only notes check.
- Promotion validation:
  - `PRAGMA integrity_check`: `ok`
  - `notes_pipeline_health.py --as-of 2026-05-21`: `ready`
  - `venv/bin/python -m pytest`: `93 passed`
  - `reminders = 17042`
  - `lessons = 17042`
  - `hubspot_deals = 25`
  - May 20 West U rows: `34`, notes: `23`, missing: `11`
  - May 20 The Heights rows: `29`, notes: `12`, missing: `17`
- Uploaded promoted `reminders.db` to `s3://notesreminder-db/reminders.db`.
  - local size: `89,931,776`
  - S3 size: `89,931,776`
  - S3 last modified: `2026-05-21T19:22:49+00:00`
- MCP lead dashboard tools now default to `reminders.db`; `LEAD_INTELLIGENCE_DB_PATH` remains available for staging overrides.

## Current State

- `main` is synced to GitHub through `f2f9691`.
- Current local `reminders.db` is the production notes DB after the May 1 local MFA run.
- The production notes DB was uploaded to `s3://notesreminder-db/reminders.db` after both schools completed.
- Current production DB sanity:
  - `pragma integrity_check`: `ok`
  - total `reminders`: `15,995`
  - latest lesson date: `2026-05-01`
  - May 1 West U rows: `26`, with notes: `14`
  - May 1 The Heights rows: `5`, with notes: `1`
- The current production DB does **not** contain the lead-intelligence additive tables, because `run_daily.py` downloads the S3 production DB at the start of each school run.
- The latest local lead-intelligence proof DB is preserved here:
  - `outputs/db_backups/reminders.db.20260501-211741.before-local-mfa-notes-run.bak`
  - contains `25` HubSpot deals
  - contains `117` Dialpad call-review rows
  - contains `59` source import runs
- The active local lead-intelligence working DB is:
  - `outputs/lead_intelligence/lead_intelligence_working.db`
  - seeded from current production `reminders.db`
  - merged with additive lead tables from the May 1 proof backup
  - contains `15,995` reminders, `25` HubSpot deals, `117` Dialpad call-review rows, and `59` source import runs

## May 1 Production Notes Run

- Command used:

```bash
scripts/run_notes_local_mfa.sh --date 2026-05-01
```

- Local DB backup:
  - `outputs/db_backups/reminders.db.20260501-211741.before-local-mfa-notes-run.bak`
- S3 DB backup:
  - `s3://notesreminder-db/backups/reminders-before-local-mfa-notes-run-20260501-211741.db`
- West U:
  - scraped `27` Pike13 lessons
  - wrote `26` unique DB rows after one duplicate lesson update
  - sent the normal daily email
  - uploaded DB to S3
- The Heights:
  - scraped `5` Pike13 lessons
  - wrote `5` DB rows
  - sent the normal daily email
  - uploaded DB to S3
- The shared Pike13 browser profile worked for both schools:
  - `browser_profiles/pike13`
  - West U needed login/MFA
  - The Heights reused the same browser session without a second MFA prompt

## Dialpad Proof Status

- Dialpad daily intake blocker was resolved.
- Conversation History pagination was added to `scripts/extract_dialpad_daily_intake.py`.
- The paginated proof loaded:
  - 2-day run: `100` rows seen, `76` inserted, `24` updated
  - 7-day run: `75` rows seen, `12` inserted, `63` updated
- Call-review ingestion reached `117` targets and inserted many new transcript/recap rows, but a retry pass later ran too long and was stopped.
- `scripts/extract_dialpad_call_reviews.py` now has:
  - retry around call-review page navigation
  - tolerant handling for Transcript-tab click timeouts
- Before the production notes run overwrote local lead tables, the progress dashboard showed:
  - HubSpot: `READY`
  - Dialpad: `READY`
  - Pike13: `PARTIAL`
  - first value report: `READY`

## DB Operating Model

The production notes pipeline and the lead-intelligence proof previously competed over the same `reminders.db` file:

- `run_daily.py` starts by downloading `s3://notesreminder-db/reminders.db`.
- That S3 DB is currently the production notes DB.
- Lead-intelligence tables may disappear locally after a production notes run if lead refreshes write directly to production `reminders.db`.

Current decision:

- Keep production notes in `reminders.db` and S3.
- Keep lead proof work in `outputs/lead_intelligence/lead_intelligence_working.db`.
- Rebuild the lead working DB after production notes runs so it has current lesson-note evidence.
- Treat this as temporary; lead intelligence must pass a production merge gate before production use because notes are key performance indicators.

Do **not** upload a lead-mutated DB to the production S3 key until the merge gate is explicitly reviewed.

## Next Steps

1. Confirm no leftover browser/process state:

```bash
ps -axo pid,ppid,etime,command | rg "run_daily.py|extract_dialpad|browser_profiles/(dialpad|pike13)"
```

2. Rebuild the lead working DB after each successful production notes run:

```bash
python3 scripts/rebuild_lead_working_db.py \
  --production-db reminders.db \
  --lead-proof-db outputs/lead_intelligence/lead_intelligence_working.db \
  --output outputs/lead_intelligence/lead_intelligence_working.db
```

3. Regenerate:

```bash
python3 scripts/progress_dashboard.py --db outputs/lead_intelligence/lead_intelligence_working.db --window-days 7 --pike13-lookahead-days 30
python3 scripts/lead_attention_report.py --db outputs/lead_intelligence/lead_intelligence_working.db --school "West U" --window-days 7
python3 scripts/unmatched_inbound_report.py --db outputs/lead_intelligence/lead_intelligence_working.db --school "West U" --window-days 2
```

4. Review Phase 3 check-in:
   - Dialpad intake is no longer blocked.
   - Pagination works.
   - Call-review ingestion needs progress logging and bounded retries before broad backfill.
5. Then move to Phase 4:
   - Pike13 rich lead/outcome hardening.
   - Use `venv/bin/python` for the headed Pike13 proof on this machine; system `python3` can run headless but the headed bundled browser crashes.
   - Current proof command:

```bash
venv/bin/python scripts/extract_pike13_leads.py \
  --db outputs/lead_intelligence/lead_intelligence_working.db \
  --profile-dir browser_profiles/pike13 \
  --base-url https://westu-sor.pike13.com \
  --school "West U" \
  --limit 5 \
  --interactive-login
```

   - The command waits at the login checkpoint; complete Pike13 MFA, then press Enter in the terminal.

## Files To Check In

These were checked in before this split-DB phase:

- `scripts/extract_dialpad_daily_intake.py`
- `scripts/extract_dialpad_call_reviews.py`
- `docs/SESSION_NOTES.md`

The split-DB phase should add/check in:

- `scripts/rebuild_lead_working_db.py`
- tests for the rebuild utility
- docs explaining the lead working DB and production merge gate

Leave these untracked files alone unless the user confirms they are intentional:

- `package.json`
- `package-lock.json`

## 2026-05-20 Plan Consolidation And Execution Start

Canonical plan/refactor document:

- `docs/master_plan.md`
- `PLAN.md`, `refactor.md`, and `docs/notesreminder_end_state_roadmap.md` were removed so there is one planning document.
- The merged plan was tightened with phase gates, execution modes, rollback paths, a test-harness phase, package-skeleton phase, data-dictionary phase, and formal business-rule gates.
- The merged plan now has `22` numbered phases: Phase 0 through Phase 21.

Phase 1 pre-flight baseline:

- SQLite integrity check:
  - `sqlite3 reminders.db "PRAGMA integrity_check;"` -> `ok`
- Current production DB freshness:
  - `theheights-sor`: `MAX(last_checked)=2026-05-20`, `MAX(lesson_date)=2026-05-19`, `6868` reminder rows
  - `westu-sor`: `MAX(last_checked)=2026-05-20`, `MAX(lesson_date)=2026-05-19`, `10111` reminder rows
- Latest outstanding daily send evidence:
  - `logs/outstanding-daily-send-20260520-121549.log`
  - `logs/outstanding-daily-send-resume-20260520-122552.log`
  - `logs/theheights-2026-05-17-check-20260520-124018.log`
- Phase 1 backups:
  - local: `outputs/db_backups/reminders.db.20260520-130607.phase-1-preflight.bak`
  - S3: `s3://notesreminder-db/backups/reminders-phase-1-preflight-20260520-130607.db`

Test baseline:

- Installed `pytest` into `venv` because the project venv had app dependencies but no test runner.
- Global Python test run is not a valid baseline on this machine:
  - without `PYTHONPATH=.`: collection fails because repo modules are not importable
  - with `PYTHONPATH=.`: collection reaches `82` items but fails on global Python package issues (`rpds` architecture mismatch, pandas/numpy import issue)
- Valid venv active-suite baseline:
  - `PYTHONPATH=. venv/bin/python -m pytest tests`
  - result: `85 passed`
- Full repo run with venv:
  - `PYTHONPATH=. venv/bin/python -m pytest`
  - result: `85 passed`, `1 failed`
  - failure is archived legacy async test: `archive/legacy/zzz_delete/test_attendance.py::test_attendance_detection`
  - likely needs either `pytest-asyncio` or exclusion from normal test collection; not part of active `tests/` suite.

MCP surface inventory from `mcp_server.py`:

- `sync_db_from_s3` -> `reminders.db`
- `db_status` -> `reminders.db` and `outputs/lead_intelligence/lead_intelligence_working.db`
- `list_tables` -> `reminders.db`
- `describe_table` -> `reminders.db`
- `query_sql` -> `reminders.db`
- `import_call_data` -> caller-provided DB, default `reminders.db`
- `initialize_lead_followup_schema` -> `reminders.db`
- `source_completeness` -> `reminders.db`
- `daily_snapshot` -> lead working DB
- `weekly_snapshot` -> lead working DB
- `monthly_snapshot` -> lead working DB
- `exception_queue` -> lead working DB
- `lead_evidence_timeline` -> lead working DB
- `stale_leads` -> `reminders.db`
- `lead_timeline` -> `reminders.db`
- `unanswered_messages` -> `reminders.db`
- `unanswered_communications` -> `reminders.db`
- `no_show_followup` -> `reminders.db`
- `lead_conversion_path` -> `reminders.db`

Immediate execution implication:

- Phase 1 is partially complete: backups, DB integrity, freshness checks, MCP inventory, and active test baseline are done.
- Before Phase 4 single-DB migration work, decide whether to clean up/exclude `archive/legacy/zzz_delete/test_attendance.py` or add async test support.
- Hugh decision on note-quality league tables:
  - Use the existing reportable-lesson filter for now.
  - Do not include group lessons or multi-student lessons in league-table scoring.
  - Revisit later if a separate group-lesson league view becomes useful.
- Hugh decision on league-table delivery:
  - Keep league tables in dashboards/MCP for now.
  - Do not add them to recurring emails yet.
  - Revisit email delivery after the dashboard version is accepted.
- Hugh decision on scheduling:
  - Target local `launchd`/cron first, using the existing Pike13 browser profile/MFA path.
  - Keep GitHub Actions for tests and non-authenticated jobs.
  - Revisit GitHub Actions production notes runs only if Pike13 auth becomes reliably non-interactive.
- Hugh decision on production DB merge gate:
  - Merge lead working DB data into production `reminders.db` only after the first management dashboards reconcile against source counts and are accepted.
  - Dialpad proof or Pike13 outcome hardening alone is not enough to promote the lead working DB into production.
- Hugh decision on raw customer/source content:
  - Store raw customer/source content in the main production `reminders.db` for now.
  - Keep broad dashboards and reports sanitized.
  - Revisit moving sensitive raw text to a separate archive/store later if needed.
- Hugh decision on repository layout:
  - Keep the large existing-file migration late in the plan.
  - Create the package skeleton early.
  - Put new code in the package layout going forward so the repo root does not get messier.

## 2026-05-20 Phase 2: Test Harness And Developer Environment

Goal:

- Make test execution repeatable before using tests as migration gates.

Changes made:

- Added `pytest.ini`:
  - `testpaths = tests`
  - `pythonpath = .`
- Added `requirements-dev.txt`:
  - includes runtime requirements via `-r requirements.txt`
  - adds `pytest>=9.0,<10`
- Updated `README.md` with development dependency installation and the canonical test command.
- Updated `docs/data_pipeline.md` with the test baseline and pytest configuration behavior.

Gate result:

- Required test command:

```bash
venv/bin/python -m pytest
```

- Result: `85 passed`
- Normal pytest collection now uses `tests/` only and excludes archived legacy tests under `archive/`.
- Repo-root imports are handled by `pytest.ini`; `PYTHONPATH=.` is no longer required for the normal test command.

Rollback path:

- Revert `pytest.ini`, `requirements-dev.txt`, and the docs updates.

Phase status:

- Phase 2 is complete.

## 2026-05-20 Phase 3: Package Skeleton

Goal:

- Create the future package shape early so new work lands in the right place without moving old production files yet.

Changes made:

- Added package skeleton:
  - `notesreminder/`
  - `notesreminder/extractors/`
  - `notesreminder/schema/`
  - `notesreminder/reports/`
  - `notesreminder/transcription/`
  - `notesreminder/orchestration/`
  - `notesreminder/mcp/`
  - `notesreminder/lib/`
- Added minimal `__init__.py` files to each package directory.
- Updated `README.md` and `docs/data_pipeline.md` to state that new code should go under `notesreminder/`, while existing root-level production entry points remain until the later layout migration.

Gate result:

- Existing production entry-point import smoke:

```bash
venv/bin/python - <<'PY'
import run_daily
import backfill
import mcp_server
print('entry point imports ok')
PY
```

- Result: `entry point imports ok`
- Required test command:

```bash
venv/bin/python -m pytest
```

- Result: `85 passed`

Rollback path:

- Remove the `notesreminder/` package skeleton and revert the README/data-pipeline documentation updates.

Phase status:

- Phase 3 is complete.

## 2026-05-20 Phase 4: Data Dictionary And Business Rules

Goal:

- Define table/view meaning, source ownership, sensitivity, freshness expectations, and business rules for note-quality scoring.

Changes made:

- Added `docs/data_dictionary.md`.
- Documented production `reminders.db` tables.
- Documented shadow lead-intelligence tables/views.
- Documented derived reporting schema targets.
- Documented future identity, journey, raw capture, and QuickBooks objects.
- Formalized reportable-lesson filtering.
- Formalized note-quality league scoring.
- Recorded current business decisions:
  - group/multi-student lessons are excluded from league-table scoring for now
  - raw customer/source content may live in production `reminders.db`
  - broad dashboards/reports remain sanitized by default

Gate result:

- Known May MTD league-score reproduction:
  - The Heights: `338` reportable lessons, `197` with notes, `141` missing, `score_sum=141.60`, `league_score=41.9`, `4.19/10`
  - West U: `486` reportable lessons, `397` with notes, `89` missing, `score_sum=273.90`, `league_score=56.4`, `5.64/10`
- Production schema dictionary coverage check:
  - current production tables: `call_client_matches`, `call_logs`, `dialpad_calls`, `dialpad_daily_stats`, `dialpad_recordings`, `dialpad_user_stats`, `dialpad_voicemails`, `pike13_clients`, `recording_downloads`, `recording_transcripts`, `reminders`
  - missing from `docs/data_dictionary.md`: `none`
- Required test command:

```bash
venv/bin/python -m pytest
```

- Result: `85 passed`

Rollback path:

- Revert `docs/data_dictionary.md` and this session-note update.

Phase status:

- Phase 4 is complete.

## 2026-05-20 Phase 5: Keep Production Notes Running

Goal:

- Make daily and weekly notes emails easier to monitor while larger work continues.

Changes made:

- Added read-only notes pipeline health package module:
  - `notesreminder/reports/notes_pipeline_health.py`
- Added script shim:
  - `scripts/notes_pipeline_health.py`
- Added tests:
  - `tests/test_notes_pipeline_health.py`
- Updated `README.md` and `docs/data_pipeline.md` with the health dashboard command.

Health dashboard command:

```bash
venv/bin/python scripts/notes_pipeline_health.py --db reminders.db --as-of 2026-05-20 --lookback-days 7
```

Generated local outputs:

- `outputs/progress/notes_pipeline_health.json`
- `outputs/progress/notes_pipeline_health.md`

Gate result:

- Live health dashboard status: `ready`
- Window: `2026-05-13` to `2026-05-19`
- West U:
  - latest lesson: `2026-05-19`
  - last checked: `2026-05-20`
  - reportable lessons in window: `162`
  - missing notes in window: `33`
- The Heights:
  - latest lesson: `2026-05-19`
  - last checked: `2026-05-20`
  - reportable lessons in window: `147`
  - missing notes in window: `80`
- `2026-05-17` shows no DB rows and no email evidence for both schools; this matches the manual check that there were no lessons that day.

Tests:

```bash
venv/bin/python -m pytest tests/test_notes_pipeline_health.py
```

- Result: `3 passed`

```bash
venv/bin/python -m pytest
```

- Result: `88 passed`

Rollback path:

- Revert `notesreminder/reports/notes_pipeline_health.py`, `scripts/notes_pipeline_health.py`, `tests/test_notes_pipeline_health.py`, and the README/data-pipeline/session-note updates.

Phase status:

- Phase 5 is complete.

## 2026-05-20 Phase 6: Code Checkpoint And Git Hygiene

Goal:

- Preserve a clean rollback path before structural changes.

Checkpoint state before Phase 6:

- Phase 2 committed and tagged:
  - commit `a136ade`
  - tag `phase-2-test-harness-20260520`
- Phase 3 committed and tagged:
  - commit `9d501a6`
  - tag `phase-3-package-skeleton-20260520`
- Phase 4 committed and tagged:
  - commit `34f4dff`
  - tag `phase-4-data-dictionary-20260520`
- Phase 5 committed and tagged:
  - commit `19fac01`
  - tag `phase-5-notes-health-20260520`

Gate checks:

```bash
venv/bin/python -m pytest
```

- Result: `88 passed`

```bash
venv/bin/python run_daily.py --help
```

- Result: command help rendered successfully, confirming the production notes entry point still imports and starts.

```bash
git status --short
```

- Result before this note: only unrelated untracked `package.json` and `package-lock.json` remained.

Rollback path:

- Revert the phase commits or check out one of the phase tags.

Phase status:

- Phase 6 gate checks passed.
