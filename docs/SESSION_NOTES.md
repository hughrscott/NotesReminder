# Session Notes (Resume Here)

Last updated: 2026-05-22

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

- `main` is synced to GitHub through Phase 10 30-day proof commit `4dd92fd` before the April 1 proof check-in.
- Current local `reminders.db` is the promoted single production database with notes, reporting schema, and lead-intelligence source tables in one file.
- Local production DB sanity after the April 1 controlled proof:
  - `PRAGMA integrity_check`: `ok`
  - running source imports: `0`
  - notes pipeline health: `ready`
  - source completeness/dashboard: source intake `ready`; first-value report `ready`
- Phase 10 has passed the 30-day proof and the April 1, 2026 forward proof. Do not widen to January 1, 2025 forward without explicit Hugh approval.
- Dialpad source access is working through Conversation History and Call Review pages. First-value reporting now counts stored Conversation History call-review URLs and is ready for limited proof.
- School-email extraction now supports Okta username/password from `.env`, sends the Okta Verify push, and pauses for Hugh approval. The durable authenticated state is the browser profile session, not a permanent HTTP header.

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

- Phase 7 promoted the reconciled single-DB model.
- `reminders.db` is now the current local production database and contains both the production notes data and additive lead-intelligence/source tables.
- `run_daily.py` can still sync with `s3://notesreminder-db/reminders.db`; preserve local and S3 backups before any widening or migration work.
- `outputs/lead_intelligence/lead_intelligence_working.db` is historical/staging context only unless a later phase explicitly reintroduces a split proof database.
- Raw source data currently lives in the main database by Hugh decision; broad dashboards and reports should remain sanitized unless a private operator view is explicitly requested.

## Next Steps

1. Stop before any January 1, 2025 forward backfill until Hugh explicitly approves that widening.
2. Execute Phase 11: Person Identity Layer.
3. Keep using `reminders.db` as the single production database unless a later raw-data archive decision changes that.
4. Preserve pre-proof backups for rollback.

## Files To Check In

Current pending check-in for the April 1 proof:

- `scripts/extract_school_emails.py`
- `tests/test_school_email.py`
- `docs/SESSION_NOTES.md`

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

## 2026-05-21 Dialpad Call Review Route Proof

Goal:

- Validate the Dialpad call-review page as the primary source for call recap, action items, transcript turns, and audio access evidence.

Changes made:

- Updated Dialpad authentication-page detection to recognize `/callhistory/callreview/<id>` pages.
- Updated call-review transcript parsing for the current Dialpad layout where speaker, time, and transcript text can appear on separate lines.
- Updated call-review extraction to read recap/action items before switching to the Transcript tab, then combine both views for parsing.
- Updated Dialpad discovery/source-completeness documentation with the live route proof.

Live proof:

- URL tested: `https://dialpad.com/callhistory/callreview/5646748416811008`
- Result: recap available, action items available, audio available, transcript available.
- Parsed proof counts: `1` action item and `19` transcript speaker turns.
- No call audio was downloaded.

Tests:

```bash
venv/bin/python -m pytest tests/test_dialpad_call_reviews.py tests/test_dialpad_extractors.py
```

- Result: `15 passed`

Rollback path:

- Revert `scripts/extract_dialpad_voice.py`, `scripts/extract_dialpad_call_reviews.py`, the Dialpad extractor tests, and the documentation updates from this checkpoint.

## 2026-05-21 Phase 9: Current Source Intake

Goal:

- Refresh and validate current HubSpot, Pike13, Dialpad, Gmail/school-email, and notes evidence in the canonical `reminders.db` without changing production notes email behavior.

Backup:

- Local pre-refresh DB backup:
  - `outputs/db_backups/reminders.db.20260521T201111Z.before-phase-9-source-refresh.bak`
- The date-window runner also created:
  - `reminders.date-window-backup-20260521T201121Z.db`

Changes made:

- Updated `scripts/run_date_window_lead_load.py` and `scripts/extract_school_emails.py` with explicit `--allow-production-db` support for the Phase 7 single-DB operating model.
- Fixed date-window email mailbox argument handling so passing a single mailbox does not also keep the default mailbox list.
- Updated Dialpad source-readiness logic so route-level Call Review link visibility plus stored call-review transcript/recap evidence satisfies source intake readiness, while first-value report wiring remains partial until row-level Conversation History call-review URL capture is improved.
- Updated `docs/data_pipeline.md` to describe `reminders.db` as the current lead-refresh target after Phase 7.

Source refresh proof:

- West U bounded refresh succeeded for HubSpot, Pike13, Dialpad daily intake, Dialpad voice, Dialpad SMS, Dialpad call reviews, and a direct West U school-email proof.
- The Heights bounded refresh succeeded for HubSpot, Pike13, Dialpad daily intake, Dialpad voice, Dialpad SMS, Dialpad call reviews, and school-email search.
- The Heights school-email search returned `0` visible rows for the proof window; this is recorded as a successful zero-row search rather than an auth failure.
- One timed-out school-email attempt and two stale historical `running` import rows were marked `error` with superseding-run notes.

Gate results:

- `sqlite3 reminders.db "PRAGMA integrity_check;"`: `ok`
- Running import rows after cleanup: `0`
- Source completeness, 7-day window: `overall_status=ready`
  - HubSpot: `ready`
  - Dialpad: `ready`
  - Pike13: `ready`
- Date-window report, West U (`2026-05-14` to `2026-05-21`):
  - HubSpot rows: `0`
  - Pike13 first-visit rows: `5`
  - Dialpad communication rows: `128`
  - School email rows: `5`
  - Notes/reminders rows: `191`
- Date-window report, The Heights (`2026-05-14` to `2026-05-21`):
  - HubSpot rows: `0`
  - Pike13 first-visit rows: `5`
  - Dialpad communication rows: `146`
  - School email rows: `0`
  - Notes/reminders rows: `191`
- Progress dashboard: `Overall status: READY`
- Full test suite: `97 passed`

Known remaining next action:

- First-value report remains `partial` because row-level Conversation History call-review URLs are not yet wired into lead-attention communications, even though Call Review pages and transcript/recap extraction are proven.

Rollback path:

- Restore `reminders.db` from `outputs/db_backups/reminders.db.20260521T201111Z.before-phase-9-source-refresh.bak` if the source-refresh DB changes need to be rolled back.
- Revert this phase's code/docs changes if the single-DB source-refresh behavior needs to be disabled.

Phase status:

- Phase 9 source-intake gate passed. Stop before first-value/report-wiring work unless continuing into the next phase.

## 2026-05-21 Phase 10: 30-Day Controlled Backfill Proof

Goal:

- Widen the trusted source-intake proof from 7 days to 30 days without corrupting production notes or trusted current data.

Backups:

- Local pre-window backup:
  - `outputs/db_backups/reminders.db.20260521T204048Z.before-phase-10-30day-backfill.bak`
- S3 pre-window backup:
  - `s3://notesreminder-db/backups/reminders.db.20260521T204048Z.before-phase-10-30day-backfill.bak`
  - S3 size: `90,320,896` bytes
- The first S3 upload attempt failed because the shell had not loaded `.env`; the retry after loading `.env` succeeded.

Source refresh proof:

- 30-day window: `2026-04-21` to `2026-05-21`
- West U bounded refresh succeeded for HubSpot, Pike13, Dialpad daily intake, Dialpad voice, Dialpad SMS, school email, and Dialpad call reviews.
- The Heights bounded refresh succeeded for HubSpot, Pike13, Dialpad daily intake, Dialpad voice, Dialpad SMS, school email, and Dialpad call reviews.

30-day source counts:

- West U:
  - HubSpot rows: `13`
  - Pike13 first-visit rows: `24`
  - Dialpad communication rows: `565`
  - School email rows: `101`
  - Notes/reminders rows: `839`
- The Heights:
  - HubSpot rows: `12`
  - Pike13 first-visit rows: `24`
  - Dialpad communication rows: `694`
  - School email rows: `3`
  - Notes/reminders rows: `839`

Gate results:

- 30-day source completeness: `overall_status=ready`
  - HubSpot: `ready`
  - Dialpad: `ready`
  - Pike13: `ready`
  - Dialpad daily intake tagged rows: `567`
  - Recent voice rows: `759`
- Progress dashboard, 30-day window: `Overall status: READY`
- `sqlite3 reminders.db "PRAGMA integrity_check;"`: `ok`
- Running import rows after cleanup: `0`
- Reporting schema rebuild: passed
- Notes pipeline health after the widening and local no-email notes smoke runs: `ready`
- Local no-email production notes smoke:
  - West U, `2026-05-20`: passed with `--skip-s3-sync`
  - The Heights, `2026-05-20`: passed with `--skip-s3-sync`
- Full test suite: `97 passed`

Known remaining next action:

- Do not widen beyond the 30-day proof without Hugh approval. The next controlled backfill target in the master plan is April 1, 2026 forward.
- First-value/report wiring is still partial because row-level Conversation History call-review URLs need to be matched into lead-attention communications.

Rollback path:

- Restore `reminders.db` from `outputs/db_backups/reminders.db.20260521T204048Z.before-phase-10-30day-backfill.bak`.
- If the widened DB had been uploaded to the production S3 key, re-upload the S3 backup above to `s3://notesreminder-db/reminders.db`.

Phase status:

- Phase 10 30-day proof gate passed. Stop here for Hugh approval before April 1, 2026 forward or January 1, 2025 forward backfill.

## 2026-05-22 Phase 10: April 1 Controlled Backfill Proof

Goal:

- Widen the controlled source-intake proof from the 30-day window to April 1, 2026 forward, then stop at the Phase 10 gate before any January 1, 2025 historical backfill.

Approval and backup:

- Hugh approved widening to April 1, 2026 forward.
- Local pre-window backup:
  - `outputs/db_backups/reminders.db.20260522T233522Z.before-phase-10-april1-backfill.bak`
- S3 pre-window backup:
  - `s3://notesreminder-db/backups/reminders.db.20260522T233522Z.before-phase-10-april1-backfill.bak`

Changes made:

- Added Okta-aware school-email login support in `scripts/extract_school_emails.py`.
- The extractor now reads Okta credentials from `.env`, fills username/password, sends the Okta Verify push, and prints an explicit confirmation message so Hugh knows the push came from NotesReminder.
- Added a focused test proving the Okta helpers are safe when credentials are absent.

Source refresh proof:

- Date-window commands used `--start-date 2026-04-01` and `--end-date 2026-05-22`.
- West U bounded refresh succeeded for HubSpot, Pike13, Dialpad daily intake, Dialpad voice, Dialpad SMS, school email after Okta approval, and Dialpad call reviews.
- The Heights bounded refresh succeeded for HubSpot, Pike13, Dialpad daily intake, Dialpad voice, Dialpad SMS, school email, and Dialpad call reviews.
- West U school-email initially hit an Okta login checkpoint; after the Okta helper was added and Hugh approved the push, the direct rerun wrote `20` visible school-email rows.

April 1 source counts:

- West U:
  - HubSpot rows: `13`
  - Pike13 first-visit rows: `47`
  - Dialpad communication rows: `909`
  - School email rows: `121` after the direct Okta-backed rerun
  - Notes/reminders rows: `1458`
- The Heights:
  - HubSpot rows: `12`
  - Pike13 first-visit rows: `47`
  - Dialpad communication rows: `1117`
  - School email rows: `7`
  - Notes/reminders rows: `1416`

Gate results:

- `sqlite3 reminders.db "PRAGMA integrity_check;"`: `ok`
- Running import rows after source refresh: `0`
- No leftover Playwright/Chrome-for-Testing/source-extractor processes.
- Source completeness after widening: `overall_status=ready`
- Progress dashboard after widening: `Overall status: READY`
- Reporting schema rebuild: passed
- Notes pipeline health after local no-email May 21 notes checks: `ready`
- Local no-email production notes checks:
  - West U, `2026-05-21`: passed with `--skip-s3-sync`
  - The Heights, `2026-05-21`: passed with `--skip-s3-sync`
- Focused test suite after Okta helper changes:
  - `venv/bin/python -m pytest tests/test_school_email.py tests/test_date_window_lead_load.py`: `10 passed`
- Full test suite after widening and reporting rebuild:
  - `venv/bin/python -m pytest`: `98 passed`

Known remaining next action:

- First-value report remains `partial` because row-level Conversation History call-review URLs are not yet matched into lead-attention communications.
- Dialpad access itself is not blocked: daily intake, voice/SMS, Call Review recap, Call Review transcripts, and school-filtered Conversation History access are proven.
- Do not widen to January 1, 2025 forward without explicit Hugh approval.

Rollback path:

- Restore `reminders.db` from `outputs/db_backups/reminders.db.20260522T233522Z.before-phase-10-april1-backfill.bak`.
- If the widened DB has been uploaded to the production S3 key, re-upload the S3 backup above to `s3://notesreminder-db/reminders.db`.

Phase status:

- Phase 10 April 1 proof gate passed. Stop before January 1, 2025 forward backfill unless Hugh approves the next widening.

## 2026-05-23 First-Value Report Wiring Gate

Goal:

- Resolve the Phase 10 carry-forward reporting gap before starting Phase 11 identity work.

Changes made:

- Updated `source_completeness.py` so first-value readiness counts stored `vw_dialpad_communications.source_url` call-review URLs in addition to the latest import-run metadata.
- Updated `scripts/lead_attention_report.py` so call-review transcript/recap evidence joins when Dialpad communication URLs include query parameters.
- Added a regression test proving stored Conversation History call-review URLs satisfy the first-value URL gate without exposing sensitive content.

Gate results:

- Live source completeness now reports:
  - First Value Report: `READY`
  - Report ready: `yes`
  - Call-review URL rows: `293`
  - Call-review transcript rows: `236`
  - Call-review recap rows: `236`
  - Blockers: `None`
- West U lead-attention report regenerated:
  - Candidate leads needing attention: `8`
  - Candidate leads with matched Dialpad communications: `6`
  - Candidate leads with call-review transcripts: `5`
  - Candidate leads with call-review recaps: `5`
- Focused tests:
  - `venv/bin/python -m pytest tests/test_lead_followup_schema.py tests/test_lead_attention_report.py tests/test_progress_dashboard.py`: `20 passed`

Known remaining next action:

- Start Phase 11: Person Identity Layer.

Rollback path:

- Revert `source_completeness.py`, `scripts/lead_attention_report.py`, and `tests/test_lead_followup_schema.py`.

Phase status:

- First-value report wiring gate passed.

## 2026-05-23 Phase 11: Person Identity Layer

Goal:

- Introduce a deterministic `persons` identity hub across HubSpot, Pike13, Dialpad, and school-email source rows.

Backup:

- Local post-refresh backup:
  - `outputs/db_backups/reminders.db.20260523T184457Z.phase-11-person-identity-post-refresh.bak`
- S3 post-refresh backup:
  - `s3://notesreminder-db/backups/reminders.db.20260523T184457Z.phase-11-person-identity-post-refresh.bak`
- Note: the first live identity refresh ran before this backup was taken. The operation is additive/rebuildable and can be rolled back by clearing the new person identity tables/columns or restoring the backup above.

Changes made:

- Added person identity schema:
  - `persons`
  - `person_identities`
  - `person_resolution_conflicts`
- Added nullable source links:
  - `hubspot_deals.person_id`
  - `hubspot_contacts.person_id`
  - `pike13_people.person_identity_id`
  - `pike13_visits.person_identity_id`
  - `pike13_plans_passes.person_identity_id`
  - `dialpad_sms_threads.person_id`
  - `dialpad_voice_events.person_id`
  - `school_email_messages.person_id`
- Added deterministic resolver:
  - exact normalized email
  - exact normalized phone
  - HubSpot contact/deal IDs
  - Pike13 person IDs
  - Dialpad phone rows
  - school-email external addresses
- Added `scripts/refresh_person_identities.py`.
- Added MCP tools:
  - `refresh_person_identity_layer`
  - `person_search`
  - `person_details`
- Updated data dictionary and pipeline docs.

Live refresh result:

- `persons`: `235`
- `person_identities`: `956`
- linked source rows: `480`
- conflicts for review: `7`
  - `multiple_hubspot_contact`: `5`
  - `multiple_pike13_person`: `2`
- Person source-link coverage:
  - HubSpot contacts: `29/29`
  - HubSpot deals: `25/25`
  - Pike13 people: `106/106`
  - Dialpad SMS threads: `5/6`
  - Dialpad voice events: `224/1297`
  - School email messages: `91/128`
- Placeholder Dialpad names such as `Loading` are excluded from person display-name selection.

Gate checks run so far:

- `venv/bin/python -m pytest tests/test_person_identity.py`: `3 passed`
- `venv/bin/python -m pytest tests/test_person_identity.py tests/test_lead_followup_schema.py`: `20 passed`
- MCP `person_search` smoke returned resolved person rows.
- Full test suite: `102 passed`
- `sqlite3 reminders.db "PRAGMA integrity_check;"`: `ok`
- Running import rows: `0`
- Identity idempotency rerun returned the same live summary:
  - `persons=235`
  - `person_identities=956`
  - `conflicts=7`
  - `linked_sources=480`
- Orphan `person_identities`: `0`

Known remaining next action:

- Review the `7` conflict rows before using person identity for management conclusions.

Rollback path:

- Restore the backup above, or clear `persons`, `person_identities`, and `person_resolution_conflicts` and null the source `person_id`/`person_identity_id` columns.

Phase status:

- Phase 11 gate passed.

## 2026-05-23 Phase 12: Customer Journey View

Goal:

- Add a chronological person journey view and MCP tools on top of the Phase 11 identity layer.

Backup:

- Local pre-view backup:
  - `outputs/db_backups/reminders.db.20260523T184956Z.before-phase-12-person-journey.bak`
- S3 pre-view backup:
  - `s3://notesreminder-db/backups/reminders.db.20260523T184956Z.before-phase-12-person-journey.bak`

Changes made:

- Added `vw_person_journey` with the planned row shape:
  - `person_id`
  - `event_at`
  - `event_type`
  - `source_system`
  - `source_id`
  - `summary`
  - `detail_json`
  - `school`
- Included current available branches:
  - HubSpot lead-created, task, and activity events
  - Dialpad SMS and voice events
  - School email events
  - Pike13 visit/trial/no-show/canceled events
  - Pike13 plan/pass events
- Added resolver helpers:
  - `person_journey`
  - `customer_lifecycle_summary`
- Added MCP tools:
  - `person_journey`
  - `customer_lifecycle_summary`
- Sanitized journey mode omits `detail_json`, message bodies, transcript text, and raw source URLs unless `include_sensitive=true`.
- Updated data dictionary and pipeline docs.

Live proof:

- `vw_person_journey` rows: `632`
- Distinct persons with journey rows: `232`
- Event-type counts:
  - `dialpad_call`: `209`
  - `pike13_plan_or_pass`: `107`
  - `pike13_trial_visit`: `103`
  - `school_email`: `91`
  - `dialpad_sms`: `80`
  - `lead_created`: `21`
  - `pike13_canceled_visit`: `9`
  - `dialpad_missed_call`: `8`
  - `dialpad_voicemail`: `4`
- Five high-source-count person journeys returned coherent chronological output with nonzero lifecycle summaries.
- MCP `person_journey` and `customer_lifecycle_summary` smoke checks returned expected JSON.

Gate checks run so far:

- `venv/bin/python -m pytest tests/test_person_journey.py tests/test_person_identity.py tests/test_lead_followup_schema.py`: `23 passed`
- Full test suite: `105 passed`
- `sqlite3 reminders.db "PRAGMA integrity_check;"`: `ok`
- Running import rows: `0`
- Source completeness after Phase 12: `overall_status=ready`; first-value report remains `ready`

Known remaining next action:

- Hugh should spot-check 5 real people before replacing older lead timeline tools.

Rollback path:

- Restore the backup above, or revert `vw_person_journey`, MCP tool additions, and helper/test changes.

Phase status:

- Phase 12 gate passed.

## 2026-05-23 Phase 13: Notes Normalization Shadow Dual-Write

Goal:

- Start the normalized notes write-path transition without cutting existing daily/weekly reads away from `reminders`.

Backup:

- Local pre-dual-write backup:
  - `outputs/db_backups/reminders.db.20260523T185628Z.before-phase-13-notes-dual-write.bak`
- S3 pre-dual-write backup:
  - `s3://notesreminder-db/backups/reminders.db.20260523T185628Z.before-phase-13-notes-dual-write.bak`

Changes made:

- `run_daily.py` now syncs normalized reporting tables from `reminders` after notes writes by default.
- Added rollback/debug flag:
  - `--skip-reporting-sync`
- Extended normalized reporting schema:
  - `lesson_students.person_id`
  - `idx_lesson_students_person`
- Backfilled `lesson_students.person_id` only when a unique exact `persons.display_name` + school match exists.
- Kept `reminders` as the production write/read source; no read-path cutover was made.

Live validation:

- Ran local no-email/no-S3 notes validation with reporting sync enabled:
  - West U, `2026-05-21`: passed
  - The Heights, `2026-05-21`: passed
  - West U, `2026-05-22`: passed
  - The Heights, `2026-05-22`: passed
- Normalized parity after the runs:
  - `reminders`: `17156`
  - `lessons`: `17156`
  - `lesson_notes`: `17156`
  - `lesson_attendance`: `17156`
  - reminders without lessons: `0`
  - reminders without lesson_notes: `0`
  - reminders without lesson_attendance: `0`
- May 21/May 22 school/day counts matched exactly between `reminders` and normalized `lessons`/`lesson_notes`.
- `lesson_students.person_id` exact backfill rows: `254` of `27794`.
- Notes pipeline health after May 22 local checks: `ready`.
- No leftover Playwright/Chrome-for-Testing/run_daily processes.

Gate results:

- `venv/bin/python -m pytest tests/test_reporting_schema.py tests/test_notes_pipeline_isolation.py`: `2 passed`
- Full test suite: `105 passed`
- `sqlite3 reminders.db "PRAGMA integrity_check;"`: `ok`
- Running import rows: `0`
- Source completeness after Phase 13: `overall_status=ready`; first-value report remains `ready`

Known remaining next action:

- Stop before any daily/weekly report or MCP read-path cutover from `reminders` to normalized notes tables. The master plan requires explicit Hugh approval before that cutover or any `reminders` retirement.

Rollback path:

- Use `run_daily.py --skip-reporting-sync` to disable the shadow normalized sync.
- Restore the backup above if the live DB needs to be reverted.

Phase status:

- Phase 13 shadow dual-write gate passed.
- Phase 13 read-path cutover is intentionally not started pending approval.

## 2026-05-23 Phase 13: Shadow Read-Path Comparison

Approval:

- Hugh approved building and running shadow read-path comparison only.
- Production reports/MCP reads are **not** approved to cut over from `reminders` yet.

Goal:

- Compare legacy `reminders` reads with normalized notes-table reads and fail on mismatches, without changing production report read paths.

Changes made:

- Added `notesreminder/reports/notes_read_path_comparison.py`.
- Added CLI wrapper:
  - `scripts/notes_read_path_comparison.py`
- Added tests:
  - `tests/test_notes_read_path_comparison.py`
- Updated docs with the comparison command.

Comparison coverage:

- Base row parity:
  - `reminders`
  - `lessons`
  - `lesson_notes`
  - `lesson_attendance`
- Missing normalized rows from `reminders`.
- School/day total, reportable, completed, and missing-note counts.
- Instructor missing-note counts.
- Note-quality league-table rows and scores.

Live comparison result:

- Command:

```bash
venv/bin/python scripts/notes_read_path_comparison.py \
  --db reminders.db \
  --start-date 2026-05-16 \
  --end-date 2026-05-22 \
  --output outputs/progress/phase13_notes_read_path_comparison.md \
  --json-output outputs/progress/phase13_notes_read_path_comparison.json
```

- Status: `ready`
- Mismatches: `0`
- Base counts:
  - `reminders`: `17156`
  - `lessons`: `17156`
  - `lesson_notes`: `17156`
  - `lesson_attendance`: `17156`
  - reminders missing lessons: `0`
  - reminders missing lesson_notes: `0`
  - reminders missing lesson_attendance: `0`

Gate checks run so far:

- `venv/bin/python -m pytest tests/test_notes_read_path_comparison.py tests/test_reporting_schema.py tests/test_notes_pipeline_health.py`: `6 passed`
- `venv/bin/python -m pytest tests/test_notes_read_path_comparison.py`: `2 passed`
- Full test suite: `107 passed`
- `sqlite3 reminders.db "PRAGMA integrity_check;"`: `ok`
- Running import rows: `0`
- Notes pipeline health: `ready`

Known remaining next action:

- Keep collecting daily shadow comparisons until 7 days of parity are available.
- Do not cut production reads over from `reminders` without explicit approval.

Rollback path:

- Revert the comparison script/module/test/docs. No production read path was changed.

Phase status:

- Shadow read-path comparison gate passed for `2026-05-16` through `2026-05-22`.
