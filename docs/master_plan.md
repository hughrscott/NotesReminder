# Master Plan: NotesReminder Operating System

## Summary

NotesReminder is becoming a trusted operating system for both schools: reliable notes reminders, repeatable source intake, evidence-backed lead follow-up reports, school and instructor scorecards, note-quality league tables, MCP investigation tools, and eventually AI-assisted coaching and decision support.

The north star is simple: Hugh, Vivian, or an MCP-backed assistant should be able to ask "What happened with this customer?" or "What needs attention today?" and get a trustworthy, auditable answer backed by Pike13, HubSpot, Dialpad, Gmail, lesson notes, recordings/transcripts, and later QuickBooks.

`reminders.db` is the production source of truth. Operational work should update it locally, sync it to S3, and avoid parallel edits to multiple database copies.

## Execution Modes

Every phase has an execution mode:

- `Production-safe`: does not change production data semantics or customer-facing workflows.
- `Shadow mode`: runs beside production and may read production data, but does not become the operational source of truth.
- `Migration`: changes canonical data location, write path, or read path; requires backups, explicit rollback, and Hugh approval before promotion.
- `Experimental`: proof-of-concept work that cannot drive management action until promoted.

## Phase Gate Template

Every phase must close with a gate before the next phase starts:

- `Mode`: one of the execution modes above.
- `Goal`: the measurable outcome.
- `Required tests`: exact commands or checks.
- `Backup requirement`: what must exist before changes.
- `Rollback path`: how to return to the previous known-good state.
- `Promotion rule`: what makes this phase allowed to affect production or management decisions.
- `Approval`: whether Hugh approval is required before merge/promotion.
- `Evidence`: where results are recorded, normally `docs/SESSION_NOTES.md`.

## Operating Rules

- Production notes obligations are non-breakable.
- Manual Pike13 MFA runs remain the production path until there is a stable non-interactive auth replacement.
- Browser profiles, local DB backups, screenshots, generated reports, exports, recordings, raw captures, and raw customer evidence are local operational state and must not be committed.
- Keep dashboards and broad reports sanitized by default: counts, IDs, timestamps, statuses, risk reasons, and source links are acceptable; raw customer content should stay out of broad reports.
- Current raw-data decision: raw customer/source content may live in the main production `reminders.db` for now; broad reports remain sanitized, and splitting sensitive raw text into a separate store can be revisited later.
- Prefer additive schema changes first: new tables, views, and tools before risky migrations.
- New code should be written in the future `notesreminder/` package layout once the package skeleton exists, even before old files are moved.
- Every phase ends with a check-in, updated notes in `docs/SESSION_NOTES.md`, and a commit or branch checkpoint when code/data structure changed.

## Canonical Test Command

Use the project venv and repo-root import path:

```bash
PYTHONPATH=. venv/bin/python -m pytest tests
```

Current baseline on 2026-05-20:

- Active suite: `85 passed`
- Full repo run includes one archived legacy async failure under `archive/legacy/zzz_delete/test_attendance.py`

The test harness phase below must make this permanent instead of relying on memory.

## Canonical Workflow

1. `run_daily.py` or `scripts/run_notes_local_mfa.sh`: notes, attendance, note text, note scores, daily email, S3 sync.
2. `backfill.py`: monthly/quarterly historical notes windows when needed.
3. `import_call_data.py`: Dialpad and Pike13 client imports.
4. `build_reporting_schema.py`: business-friendly reporting tables/views.
5. `generate_call_reports.py` and report scripts: analysis outputs.
6. MCP tools and dashboards: operational investigation and management views.

## Phase 0: Operating Target And Safety Baseline

Mode: `Production-safe`

Goal: lock the business outcomes, system architecture, and safety rules before more buildout.

Work:
- Define the core operating questions: daily follow-up, missed notes, no-shows, trial conversion, lead response time, churn risk, staff coaching, and revenue.
- Pick 5-10 golden customer journeys for regression checks.
- Inventory current MCP tools, dashboards, reports, data sources, and database targets.
- Confirm production notes obligations are documented as non-breakable.

Required tests:
- Confirm a recent notes email run for both schools.
- Confirm the plan has a single canonical document.

Backup requirement:
- None; docs/inventory only.

Rollback path:
- Revert documentation changes.

Promotion rule:
- No production promotion; this phase defines the target.

Approval:
- Hugh approval required for the operating target.

Done/Test:
- One agreed roadmap exists in this file.
- Golden customer journey checklist exists.
- MCP/report/source inventory exists.
- A recent notes email run is confirmed for both schools.

## Phase 1: Pre-Flight Production Checkpoint

Mode: `Production-safe`

Goal: start from a known-good production and repository state.

Work:
- Confirm both schools' production notes pipeline ran successfully in the last 7 days; fix that first if not.
- Snapshot `reminders.db` and any active lead working database to `outputs/db_backups/`.
- Upload the current `reminders.db` to S3 with a clearly named backup key.
- Run the active test suite.
- Read `mcp_server.py` and write a short tool inventory in `docs/SESSION_NOTES.md`.

Required tests:
- `sqlite3 reminders.db "PRAGMA integrity_check;"`
- `PYTHONPATH=. venv/bin/python -m pytest tests`
- MCP tool inventory recorded.

Backup requirement:
- Timestamped local DB backup.
- Timestamped S3 backup under `s3://notesreminder-db/backups/`.

Rollback path:
- Restore `reminders.db` from the local or S3 backup.
- Revert code/doc changes from the phase branch.

Promotion rule:
- Required before any migration or schema promotion.

Approval:
- Hugh approval required before proceeding to migration phases.

Done/Test:
- Backups exist locally and in S3.
- Test suite result is recorded.
- MCP inventory lists each tool, database, and dependent view/query.
- Production notes pipeline remains current.

## Phase 2: Test Harness And Developer Environment

Mode: `Production-safe`

Goal: make test execution repeatable before relying on tests as migration gates.

Work:
- Add test dependencies to the appropriate dev/test dependency location.
- Make `PYTHONPATH` behavior explicit through project config or test config.
- Exclude `archive/legacy/zzz_delete` from normal pytest collection, or add async test support if the archived test should remain active.
- Document the canonical test command in `README.md` and `docs/data_pipeline.md`.
- Decide whether archived legacy tests are kept, fixed, moved, or deleted.

Required tests:
- `PYTHONPATH=. venv/bin/python -m pytest tests`
- Full intended test command after config cleanup.

Backup requirement:
- None beyond git checkpoint; no production data changes.

Rollback path:
- Revert dependency/test-config changes.

Promotion rule:
- Future phases must use the canonical command defined here.

Approval:
- Hugh approval not required unless deleting legacy tests.

Done/Test:
- Active test suite runs without special memory of `PYTHONPATH`.
- Archived legacy tests no longer create false failures in the normal test baseline.
- Test setup is documented.

## Phase 3: Package Skeleton

Mode: `Production-safe`

Goal: create the future package shape early so new work lands in the right place without moving old production files yet.

Work:
- Create empty package directories: `notesreminder/extractors/`, `notesreminder/schema/`, `notesreminder/reports/`, `notesreminder/transcription/`, `notesreminder/orchestration/`, `notesreminder/mcp/`, and `notesreminder/lib/`.
- Add minimal `__init__.py` files.
- Keep existing top-level scripts untouched.
- Document that new modules should go into the package, while old modules move later in the repository layout phase.

Required tests:
- Active test suite still passes.
- Existing production entry points still import.

Backup requirement:
- None beyond git checkpoint; no production data changes.

Rollback path:
- Remove the package skeleton.

Promotion rule:
- New code may start using the package layout after this phase.

Approval:
- Hugh approval not required.

Done/Test:
- Package skeleton exists.
- No existing imports or scripts break.
- Future module location is documented.

## Phase 4: Data Dictionary And Business Rules

Mode: `Production-safe`

Goal: define the meaning, ownership, sensitivity, and freshness expectations for core tables and business calculations.

Work:
- Create `docs/data_dictionary.md`.
- For each table/view: define purpose, source system, primary key, natural key, freshness expectation, sensitive fields, and production/shadow/legacy status.
- Formalize the `reportable lesson` rule used by missing-note reports and note-quality league tables.
- Formalize note-quality scoring: missing note = `0`; completed scored note = `note_score / 10`; school/instructor score = `SUM(component) / reportable_lessons * 100`.
- Current decision: note-quality league tables use the existing reportable-lesson filter and do not include group lessons or multi-student lessons for now.
- Define data ownership for `reminders`, lead-intel tables, raw captures, recordings, transcripts, and future QuickBooks tables.

Required tests:
- Recompute a known May MTD league table and verify it matches the hand-checked sample.
- Review data dictionary against actual SQLite schema.

Backup requirement:
- None; documentation/business rules only.

Rollback path:
- Revert documentation/business-rule changes.

Promotion rule:
- No league-table or management-scorecard work can be promoted until the rules are documented.

Approval:
- Hugh approval required for reportable-lesson and league-table rules.

Done/Test:
- Data dictionary exists.
- Reportable-lesson rule is explicit.
- Sensitive fields are marked.
- League scoring rule is reproducible.

## Phase 5: Keep Production Notes Running

Mode: `Production-safe`

Goal: make daily and weekly notes emails reliable while bigger work continues.

Work:
- Use `scripts/run_notes_local_mfa.sh` as the production run path while Pike13 MFA blocks GitHub Actions.
- Keep one persistent Pike13 browser profile for both schools where possible.
- Keep GitHub Actions from running the broken Pike13 login path until there is a stable automation replacement.
- Add or confirm daily and weekly run checklists.
- Build a small notes pipeline health dashboard that shows missed runs, latest DB sync, and recent school/date coverage.

Required tests:
- Run one daily notes email for each school or verify a current run.
- Verify S3 upload after run.
- Verify logs/status files show success/failure clearly.

Backup requirement:
- Local and S3 DB backup before manual catch-up sends or run-path changes.

Rollback path:
- Restore DB backup if a run corrupts local data.
- Revert run-path changes.

Promotion rule:
- This remains production once tests pass because it is the current committed business workflow.

Approval:
- Hugh approval required before recipient-list or email-content changes.

Done/Test:
- Both schools' notes emails are current.
- A missed day can be detected and caught up from logs/status files.
- Note counts, missing counts, note text, and score columns appear correctly.
- `reminders.db` uploads to S3 after each production run.

## Phase 6: Code Checkpoint And Git Hygiene

Mode: `Production-safe`

Goal: preserve a clean rollback path before structural changes.

Work:
- Commit existing work in logical chunks: docs/test harness first, Pike13 MFA/manual-run reliability second, Dialpad intake/unmatched inbound/dashboard/reporting third.
- Leave unrelated `package.json` and `package-lock.json` untouched unless confirmed intentional.
- Do not commit `reminders.db`, browser profiles, screenshots, generated reports, backups, exports, recordings, raw captures, or raw customer evidence.
- Push to GitHub after the checkpoint.

Required tests:
- Active test suite.
- `git status --short` review.

Backup requirement:
- Git checkpoint is the rollback artifact.

Rollback path:
- Revert the commit or branch.

Promotion rule:
- No structural migration starts until the checkpoint exists.

Approval:
- Hugh approval required before merge to `main`.

Done/Test:
- `git status --short` shows only intended remaining work.
- Checkpoint commit(s) are pushed.
- Production notes command still runs after the checkpoint.

## Phase 7: Single Production Database

Mode: `Migration`

Goal: merge lead intelligence into `reminders.db` so there is one canonical database file.

Work:
- Confirm `ensure_lead_followup_schema()` is idempotent against an existing production `reminders.db`.
- Add `scripts/migrate_lead_intel_to_production.py` to copy lead-intel tables into `reminders.db` with idempotent upserts and count validation.
- Run the migration against a copy of `reminders.db` first.
- Update `mcp_server.py` so all tools read from `reminders.db`.
- Retire or deprecate `scripts/rebuild_lead_working_db.py`.
- Update `docs/data_pipeline.md` to describe the single-DB model.
- Current merge-gate decision: lead working DB data merges into production `reminders.db` only after the first management dashboards reconcile against source counts and are accepted.

Required tests:
- Active test suite.
- Migration dry run against a DB copy.
- Migration idempotency check by running twice.
- `sqlite3 reminders.db "PRAGMA integrity_check;"`
- MCP baseline query comparison.
- Production notes run against unified DB.

Backup requirement:
- Timestamped local backup of the current production `reminders.db`.
- Timestamped S3 backup of the current production `reminders.db`.
- Timestamped local backup of the lead working DB.
- Preserve the old production DB backups after promotion; do not delete or overwrite them during cleanup.

Rollback path:
- Restore `reminders.db` from the preserved Phase 7 production backup.
- If the unified DB was uploaded to the production S3 key, re-upload the preserved Phase 7 production backup to that key.
- Restore MCP server code from the previous commit.
- Leave lead working DB untouched until promotion is approved.

Promotion rule:
- Unified DB becomes production only after dry-run counts, MCP checks, notes run, and Hugh approval pass.
- Promotion is a replace-with-backup operation, not a destructive delete of the old DB.

Approval:
- Hugh approval required before uploading unified DB to the production S3 key.

Done/Test:
- Lead-intel tables exist and are populated in `reminders.db`.
- `reminders` row count and note-score columns are unchanged by migration.
- Running the migration twice does not duplicate rows.
- Every MCP tool from the Phase 1 inventory returns equivalent results to baseline.
- A production notes run succeeds against the unified DB.

## Phase 8: Business-Friendly Reporting Schema

Mode: `Shadow mode`

Goal: make the database easier and safer to query without breaking the legacy notes table.

Work:
- Keep `reminders` as the raw legacy source during the transition.
- Maintain/add reporting tables: `schools`, `instructors`, `students`, `lessons`, `lesson_students`, `lesson_notes`, `lesson_attendance`, `call_logs`, `call_client_matches`, and `recording_transcripts`.
- Maintain/add reporting views: `vw_missing_notes_by_instructor`, `vw_note_completion_rate`, `vw_missing_notes_by_school_day`, `vw_note_quality_league_table`, `vw_callback_speed`, and `vw_churn_candidates`.
- Add stable business keys and display labels such as `school_code` and `school_name`.

Required tests:
- Active test suite.
- `build_reporting_schema.py` idempotency.
- Row-count reconciliation for known date windows.
- Daily email count parity.

Backup requirement:
- Local DB backup before schema changes.

Rollback path:
- Restore DB backup or drop newly added reporting objects if additive rollback is sufficient.
- Revert reporting code.

Promotion rule:
- Reporting schema may be used by dashboards only after parity checks pass.

Approval:
- Hugh approval required before replacing existing report queries.

Done/Test:
- `build_reporting_schema.py` runs idempotently.
- Reporting row counts reconcile to `reminders`.
- Note quality and missing-note reports match daily email logic.
- Existing notes email behavior is unchanged.

## Phase 9: Complete Current Source Intake

Mode: `Shadow mode`

Goal: stop asking "do we have the data?" and start asking "what does the data tell us?"

Work:
- HubSpot: deals, contacts, stages, tasks, activities.
- Pike13: people, trials, visits, attendance, no-shows, cancellations, plans/passes, terms, enrollment/churn/hold timestamps.
- Dialpad: calls, missed calls, voicemails, SMS, call-review URLs, recordings metadata, transcripts/recaps where available.
- Gmail: inbound/outbound school mailbox emails.
- Notes: daily lesson notes, missing notes, scores.
- Use Dialpad Conversation History as the main intake route, with a 2-day rolling daily refresh and 7-day proof/backfill window.
- Preserve source timestamps, direction, participant phone/contact key, school/department, source URL, raw text, transcript/recap availability, and durable call-review/audio URLs.

Required tests:
- Source freshness report.
- Date-window load proof for both schools.
- Extractor idempotency tests.
- Dashboard source-count checks.

Backup requirement:
- DB backup before broad source refreshes.

Rollback path:
- Restore DB backup or remove rows from the specific import run ID if supported.
- Keep source intake shadow-mode until promoted.

Promotion rule:
- Source data can drive dashboards only after freshness and idempotency checks pass.

Approval:
- Hugh approval required before source data drives management decisions.

Done/Test:
- Each source has nonzero current data or a clear failure reason.
- Source freshness report shows refresh date, row counts, and failures.
- Daily Dialpad intake finds both matched lead communications and unmatched inbound communications.
- Extractors can be retried safely without duplicating rows.

## Phase 10: Controlled Backfill

Mode: `Shadow mode`

Goal: widen historical coverage without corrupting production notes or trusted current data.

Work:
- Widen only after the 7-day proof is trustworthy: 7 days, 30 days, April 1 2026 forward, then January 1 2025 forward.
- Before each widening, create local and S3 DB backups.
- Run source completeness checks and review dashboard blockers before proceeding.
- Keep each widening small enough to validate before moving to the next.

Required tests:
- Source completeness report per window.
- Row-count deltas per source.
- Production notes run after each widening.

Backup requirement:
- Local and S3 DB backup before each widening.

Rollback path:
- Restore the pre-window DB backup.
- Revert any extractor changes.

Promotion rule:
- A wider window becomes the new baseline only after row counts and reports are reviewed.

Approval:
- Hugh approval required before moving from 30-day proof to broad historical backfill.

Done/Test:
- Each wider window preserves source timestamps, matching quality, and report usefulness.
- Row counts and freshness checks are recorded.
- Production notes emails still run.

## Phase 11: Person Identity Layer

Mode: `Migration`

Goal: introduce `persons` as the resolved identity hub across HubSpot, Pike13, Dialpad, Gmail, and notes.

Work:
- Add `persons`, `person_identities`, and `person_resolution_conflicts`.
- Seed identities from exact email, normalized phone, HubSpot contact IDs, Pike13 person IDs, Dialpad phone, Gmail email, and school.
- Keep low-confidence matches separate for review instead of silently merging.
- Add nullable `person_id` columns to source-specific tables where useful.
- Add MCP tools: `person_search(query, limit)` and `person_details(person_id)`.

Required tests:
- Identity resolver idempotency tests.
- Duplicate/conflict tests.
- SQL checks for orphaned person IDs.
- Golden journey spot checks.

Backup requirement:
- Local and S3 DB backup before resolver writes.

Rollback path:
- Restore DB backup.
- Revert identity resolver code and MCP tools.

Promotion rule:
- `person_id` becomes a reporting key only after conflict rate and spot checks are acceptable.

Approval:
- Hugh approval required before identity resolution drives customer-facing or management conclusions.

Done/Test:
- `persons` table is populated with plausible row counts.
- Existing HubSpot contacts, Pike13 people, and Dialpad-identified parties either have `person_id` or a conflict/unresolved reason.
- `person_search` and `person_details` work via MCP.
- Ten real records are spot-checked across systems.

## Phase 12: Customer Journey View

Mode: `Shadow mode`

Goal: deliver the headline capability: a chronological timeline for a real person across every source.

Work:
- Add `vw_person_journey` with a consistent row shape: `person_id`, `event_at`, `event_type`, `source_system`, `source_id`, `summary`, `detail_json`, and `school`.
- Include lead, first response, trial booking, attendance, no-show, enrollment, communications, lesson notes, recordings/transcripts, and later billing.
- Add MCP tool `person_journey(search, start_date, end_date, limit, include_sensitive)`.
- Add MCP tool `customer_lifecycle_summary(person_id)`.
- Retire or alias older lead timeline tools after parity is confirmed.

Required tests:
- Journey ordering tests.
- Sanitized vs sensitive output tests.
- Golden journey review.
- MCP journey query checks.

Backup requirement:
- DB backup before adding views/tools if schema changes are needed.

Rollback path:
- Drop additive views/tools or restore DB backup.
- Revert MCP code.

Promotion rule:
- Customer journey output can be used operationally after golden journeys reconcile.

Approval:
- Hugh approval required before replacing old lead timeline tools.

Done/Test:
- `person_journey` returns coherent, chronological timelines for at least 5 golden customer journeys.
- `customer_lifecycle_summary` gives a sensible one-screen overview.
- Sanitized mode excludes sensitive text and raw URLs.
- Hugh can spot-check 5 real people and confirm the timeline matches reality.

## Phase 13: Normalize The Notes Write Path

Mode: `Migration`

Goal: move from the flat `reminders` table toward normalized notes tables as the primary read/write model.

Work:
- Confirm `build_reporting_schema.py` produces the schema `run_daily.py` should eventually write directly.
- Modify the notes writer to dual-write to `reminders` and normalized tables during a transition window.
- Add `lesson_students.person_id` and backfill via identity resolution.
- Move MCP tools and reports from `reminders` reads to normalized table reads.
- Keep `reminders` physically present until Hugh explicitly approves any retirement.

Required tests:
- Active test suite.
- Dual-write parity check for at least 7 days.
- Daily/weekly email count parity.
- Normalized-table report parity.

Backup requirement:
- Local and S3 DB backup before enabling dual-write.

Rollback path:
- Disable normalized dual-write flag/path.
- Restore DB backup if needed.
- Return reports to `reminders` reads.

Promotion rule:
- Normalized notes become primary only after 7 days of 100% dual-write parity.

Approval:
- Hugh approval required before any read-path cutover or `reminders` retirement.

Done/Test:
- Dual-write produces matching data between `reminders` and normalized tables across 100% of new rows for at least 7 days.
- All MCP tools and reports read from normalized tables where available.
- Note quality scoring continues to populate per-lesson scores.
- Daily/weekly notes emails still match expected counts.

## Phase 14: Operating Dashboards And MCP Tools

Mode: `Shadow mode`

Goal: turn the data platform into daily, weekly, and monthly operating views.

Work:
- Daily dashboard: yesterday/today exceptions, missing notes, unworked leads, no-shows, missed follow-up.
- Weekly dashboard: lead flow, trial attendance, conversion, response time, staff/source performance, note quality.
- Monthly dashboard: trends, funnel conversion, churn/no-show patterns, coaching themes, and revenue when available.
- Promote local reports into MCP-backed workflows: stale leads, lead timeline, unmatched inbound, unanswered communications, no-show follow-up, and conversion path.
- Keep dashboards generated from the same logic as MCP tools.

Required tests:
- Dashboard date-window tests.
- Dashboard/MCP count parity tests.
- Sanitization tests.
- Proof dashboards for daily, prior week, and MTD.

Backup requirement:
- None for read-only dashboard work; DB backup required if new persistent tables are added.

Rollback path:
- Revert dashboard/MCP code.
- Drop additive dashboard tables/views if created.

Promotion rule:
- Dashboards can drive management action only after parity and sanitization checks pass.

Approval:
- Hugh approval required before dashboards become part of normal management cadence.

Done/Test:
- Daily, weekly, and monthly dashboards generate predictably for known windows.
- Dashboard counts reconcile with MCP tools.
- Dashboards are sanitized by default.
- The system can answer which leads need attention today and why.

## Phase 15: Management Scorecards And Note League Tables

Mode: `Shadow mode`

Goal: measure school and instructor performance with reproducible scoring.

Work:
- Build school and staff scorecards for first response time, stale leads, unanswered inbound, no-show recovery, trial-to-enrollment rate, follow-up consistency, and note quality.
- Create a school and instructor note-quality league tracker:
  - Missing-note lessons score `0`.
  - Completed notes use `note_score / 10`.
  - School/instructor score is `SUM(score_component) / reportable_lessons * 100`.
  - Rank highest-to-lowest for month-to-date, weekly, prior month, and custom date windows.
  - Include lesson count, with-notes count, missing-note count, completion rate, score sum, league score, and average note score.
- Generate school-vs-school and instructor league tables from the same scoring logic.
- Use reportable-lesson filtering consistently with the notes reminder workflow unless explicitly changed.
- Current decision: group lessons and multi-student lessons are excluded from league-table scoring for now; revisit later as a separate group-lesson league view if needed.
- Current delivery decision: league tables live in dashboards/MCP for now; email delivery may be added later after the dashboard version is accepted.

Required tests:
- Known May MTD score reproduction.
- Date-window score tests.
- School/instructor rank determinism tests.
- Reportable-lesson filter tests.

Backup requirement:
- None for read-only scorecards; DB backup required if persistent scorecard tables are added.

Rollback path:
- Revert scorecard code/views.
- Remove scorecard outputs.

Promotion rule:
- Scorecards can be shared broadly only after Hugh approves the business rules and sample output.

Approval:
- Hugh approval required for reportable-lesson rules and scorecard publication.

Done/Test:
- League tables reconcile with source lesson rows.
- School and instructor rankings are reproducible for any date window.
- West U and The Heights can be compared with identical methodology.
- A known May MTD sample reproduces the hand-checked results.

## Phase 16: Automate The Cadence

Mode: `Migration`

Goal: make normal operation reliable without relying on memory.

Work:
- Schedule daily notes and data intake.
- Schedule weekly and monthly dashboard generation.
- Keep MFA-bound systems on manual-auth profiles where necessary.
- Current scheduling decision: use local `launchd`/cron first with the existing Pike13 browser profile/MFA path; keep GitHub Actions for tests and non-authenticated jobs, and revisit GitHub Actions production runs only if Pike13 auth becomes reliably non-interactive.
- Add failure alerts or delay notices.
- Keep logs and run metadata.

Required tests:
- Dry-run scheduled commands.
- Run metadata checks.
- Failure simulation for expired auth.
- Verify no DBs, profiles, reports, raw data, or recordings are committed.

Backup requirement:
- DB backup before changing scheduled production run paths.

Rollback path:
- Disable new schedule.
- Re-enable previous manual run path.
- Restore DB backup if schedule run corrupts data.

Promotion rule:
- Automated cadence becomes production only after repeated dry-run/supervised success.

Approval:
- Hugh approval required before enabling unattended production runs.

Done/Test:
- Dry-run scheduled commands succeed.
- Run metadata records start time, end time, status, source counts, and failures.
- Expired-auth simulation produces a visible actionable failure.
- Manual intervention is limited to MFA/session renewal and exceptions.

## Phase 17: Raw Capture And Replay

Mode: `Shadow mode`

Goal: protect against silent parser breakage by saving raw extractor inputs before transforming them.

Work:
- Introduce `raw/{source}/{YYYY-MM-DD}/{timestamp}-{name}.{html|json}` and keep `raw/` git-ignored.
- Modify extractors to write raw input before parsing.
- Add `raw_captures` with capture ID, source, captured time, file path, hash, parser version, parse status, and parsed time.
- Add `scripts/replay_parse.py` to rerun parsers against saved raw input into a scratch DB for diff comparison.
- Add a retention policy: keep 90 days locally by default; archive older captures to S3 if needed.

Required tests:
- Raw capture fixture tests.
- Replay parser test into scratch DB.
- Deliberate parser/selector-change validation against saved raw input.

Backup requirement:
- DB backup if adding `raw_captures` to production DB.

Rollback path:
- Disable raw capture writes.
- Drop additive `raw_captures` table if needed.
- Remove raw files created during the phase if they are not needed.

Promotion rule:
- Raw capture becomes normal intake behavior after storage, sensitivity, and retention rules are accepted.

Approval:
- Hugh approval required for retention/archive rules.

Done/Test:
- Every nightly extractor writes raw payloads to disk.
- `raw_captures` indexes the files and parse status.
- Parser changes can be validated against saved raw data without re-scraping live systems.

## Phase 18: QuickBooks Financial Dimension

Mode: `Shadow mode`

Goal: complete the customer journey with billing, payments, failed payments, and subscription lifecycle.

Work:
- Decide QuickBooks API vs Playwright extractor.
- Add `qb_customers`, `qb_invoices`, `qb_payments`, and `qb_subscriptions`.
- Resolve QB customers to `person_id`.
- Add financial events to `vw_person_journey`: invoices, payments, failed payments, subscription starts, and cancellations.
- Add MCP tools: `revenue_summary` and `customer_lifetime_value`.

Required tests:
- QB parser/extractor tests.
- QB identity-resolution tests.
- Revenue total comparison against QuickBooks.
- MCP `revenue_summary` and `customer_lifetime_value` checks.

Backup requirement:
- DB backup before loading QB data.

Rollback path:
- Restore DB backup or remove rows from the QB import run.
- Disable QB journey branches/tools.

Promotion rule:
- Financial data becomes operational only after revenue totals match QuickBooks spot checks.

Approval:
- Hugh approval required before financial data is used in dashboards.

Done/Test:
- QB data refreshes on a known cadence or reports a clear auth/data failure.
- Person journey timelines include billing events.
- Revenue summary matches QuickBooks for a spot-checked month.
- Customer lifetime value matches payments for spot-checked people.

## Phase 19: LLM Insight Layer

Mode: `Experimental`

Goal: produce useful, explainable insights after the journey data is trusted.

Work:
- Use LLMs on stored source text for sentiment, intent, urgency, call quality, follow-up quality, missed opportunity detection, and coaching recommendations.
- Store LLM outputs separately from raw source data with model/version/run metadata.
- Keep source evidence linked so every recommendation can be audited.
- Add human-reviewed recommendations first.
- No autonomous customer-facing action without explicit approval and guardrails.

Required tests:
- Prompt/version fixture tests.
- Evidence-link integrity checks.
- Sanitized output tests.
- Human review of sample insights.

Backup requirement:
- DB backup before storing generated insights in production DB.

Rollback path:
- Disable insight generation.
- Remove insight rows by run ID if needed.

Promotion rule:
- Insights can drive recommendations only after human review confirms usefulness and evidence quality.

Approval:
- Hugh approval required before any customer-facing or staff-facing AI recommendation workflow.

Done/Test:
- Insights are tied to source events.
- Recommendations are auditable back to HubSpot, Dialpad, Pike13, Gmail, or lesson-note evidence.
- Sanitized insight output does not expose raw customer content.
- Human review of sample insights confirms usefulness before automation.

## Phase 20: Repository Layout Migration

Mode: `Migration`

Goal: move existing Python code into the package structure without changing behavior.

Work:
- Move files into the package areas created in Phase 3.
- Update imports.
- Keep thin top-level shims for existing entry points: `run_daily.py`, `backfill.py`, and `mcp_server.py`.
- Resolve duplicate modules such as duplicate dashboards.
- Co-locate transcription variants while keeping each backend available.
- Update tests and path assumptions.
- Current layout decision: keep this large existing-file migration late; after Phase 3 creates the package skeleton, new code should use the package layout so the root does not get messier.

Required tests:
- Active test suite.
- Import smoke tests for top-level shims.
- Production notes run through the shim entry point.
- MCP server smoke test.

Backup requirement:
- Git checkpoint before moving files.
- DB backup not required unless behavior changes also touch production data.

Rollback path:
- Revert the repo-layout commit.

Promotion rule:
- Layout migration is complete only after shims and existing scripts still work.

Approval:
- Hugh approval required before merging the large file-move PR.

Done/Test:
- Top-level repo contains shims, configuration, docs, and the `notesreminder/` package rather than mixed-purpose modules.
- Existing shell scripts and GitHub workflows still work.
- Full active test suite passes.
- A production notes run succeeds through the shim entry point.
- No duplicate modules remain.

## Phase 21: Productize And Maintain

Mode: `Production-safe`

Goal: make NotesReminder durable enough to trust as an operating platform.

Work:
- Document operational runbooks.
- Add backup and restore instructions.
- Add source freshness SLAs.
- Keep phase tags and rollback points.
- Track unresolved identity/data issues as an operating backlog.
- Keep `docs/data_pipeline.md` aligned with the actual commands and architecture.

Required tests:
- Fresh clone setup check.
- Restore-from-backup drill.
- MCP smoke test.
- End-to-end notes plus dashboard generation.

Backup requirement:
- Backup/restore drill uses a non-production copy.

Rollback path:
- Revert docs/tooling changes.

Promotion rule:
- Platform is considered maintainable only after another engineer/agent can follow the docs.

Approval:
- Hugh approval required for final operating runbook.

Done/Test:
- A new engineer or agent can run, test, and debug the system from docs.
- The production DB can be restored from backup.
- The daily, weekly, and monthly operating rhythm is reliable.
- The architecture matches this plan and `docs/data_pipeline.md`.
- MCP smoke tests pass.

## Files And Tables Not To Break

- `run_daily.py`: must continue to produce daily/weekly notes email and sync `reminders.db` to S3 throughout every phase.
- `noteschecker.py`: Pike13 Playwright scraper; browser profile conventions must remain stable.
- `scripts/run_notes_local_mfa.sh`: current manual MFA path.
- `.github/workflows/`: update if entry points move; do not silently break.
- `reminders` table: do not drop without Hugh's explicit sign-off.
- `s3://notesreminder-db/reminders.db`: production S3 key.

## Open Questions For Hugh

- None currently. Add new questions here as implementation reveals real ambiguity.

## Final Definition Of Done

- `reminders.db` is the single canonical database.
- Every source refreshes on a known cadence or reports a clear auth/data failure.
- Customer events resolve to `person_id` or an explicit unresolved reason.
- `person_journey` and `customer_lifecycle_summary` work for real customers.
- Daily, weekly, and monthly dashboards drive school operations.
- Production notes emails continue reliably.
- Raw captures protect parsers.
- Recordings and transcripts are indexed and usable.
- QuickBooks financials are included.
- AI insights are evidence-backed and human-reviewable.
- Repo layout is clean and documented.
- Docs, tests, backups, and MCP tools support ongoing maintenance.
