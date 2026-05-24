# Tomorrow Close-Out TODO

Use this file together with `docs/master_plan.md` to restore context and finish the remaining work. The old `PLAN.md` was consolidated into `docs/master_plan.md`; that is the canonical plan.

## Current Status

- Completed, committed, tagged, and pushed through Phase 19.
- QuickBooks Phase 18 is intentionally deferred, not cancelled.
- Current branch: `main`.
- Latest completed commit:
  - commit: `5385f22`
  - message: `Skip scraper diagnostics outside verbose mode`
- Latest completed phase tag:
  - tag: `phase-19-experimental-insights-20260523`
- Tracked repo state was clean after Phase 19.
- Unrelated untracked files remain and should be left alone unless Hugh confirms they are intentional:
  - `package.json`
  - `package-lock.json`

## Important Safety Boundaries

- Do not enable customer-facing or staff-facing AI recommendations without Hugh approval.
- Do not enable unattended production notes/email/DB/S3 cadence without Hugh approval.
- Do not cut production reports over from `reminders` to normalized notes tables without Hugh approval.
- Do not archive raw captures to S3. Raw captures are local-only under git-ignored `raw/`.
- Do not delete or overwrite preserved DB backups.
- Do not start QuickBooks until Hugh chooses API access vs browser extraction.

## What Was Finished

- Phase 14: shadow operating dashboards.
- Phase 15: shadow note-quality scorecards.
- Phase 16: cadence runner scaffold, not installed as an unattended schedule.
- Phase 17: local raw capture and replay.
- Phase 19: experimental communication insights.

## Phase 19 State To Review

- Experimental communication insights were generated for `2026-05-16` through `2026-05-22`.
- Output files:
  - `outputs/progress/phase19_insights/westu_sample.md`
  - `outputs/progress/phase19_insights/heights_sample.md`
- DB rows written:
  - `communication_ai_insights`: `20`
  - all rows are `pending_human_review`
- Observation from the Phase 19 proof:
  - West U sample had more useful school-email signals.
  - The Heights sample mostly surfaced low-signal Dialpad voice rows.
  - Before promoting insights, improve source selection and transcript/recap coverage, especially for The Heights.
- Decision on 2026-05-24:
  - Option B approved. Keep Phase 19 experimental and do not promote AI insights into staff-facing or customer-facing workflows.
  - Return to the AI layer after the data is easier to explore through MCP.
  - Do not keep iterating on the current report script before Phase 20.

## Tomorrow Decision Checklist

1. Phase 19 decision is complete: defer AI-insight promotion until after MCP/data exploration.
2. If Phase 19 is resumed later, decide what should improve first:
   - better source selection
   - more Dialpad call-review transcript coverage
   - LLM-backed insight provider instead of deterministic heuristic
   - a human-review approval/reject workflow
3. Confirm QuickBooks remains deferred.
4. Proceed to Phase 20 repository layout migration.

## Daily Notes Follow-Up

- Completed on 2026-05-24 as a high-level smoke test with recipient override `huscott@schoolofrock.com` only.
- Ran the missing daily notes sends for both schools for:
  - `2026-05-20`
  - `2026-05-21`
  - `2026-05-22`
  - `2026-05-23`
- Run log:
  - `logs/smoke-daily-send-just-hugh-20260524-082913.log`
- The first attempted wrapper batch did not send email; it failed before `run_daily.py` accepted arguments and only created backups.
- Smoke validation after the direct runs:
  - `PRAGMA integrity_check;` returned `ok`
  - `scripts/notes_pipeline_health.py --db reminders.db --as-of 2026-05-24 --lookback-days 7` reported `Overall status: ready`
  - email evidence showed `delivered` for both schools for `2026-05-20` through `2026-05-23`
  - S3 object updated: `s3://notesreminder-db/reminders.db`, size `90607616`, last modified `2026-05-24T13:40:51+00:00`
- Operational note:
  - The just-Hugh smoke pattern is now proven useful for validating production mechanics without notifying staff.

## Weekly Missing-Notes Test Result

- Date window tested: `2026-05-18` through `2026-05-24`.
- Recipient override: `huscott@schoolofrock.com` only.
- West U email sent successfully: `Lesson notes summary for West U (2026-05-18 to 2026-05-24)`.
- The Heights email sent successfully: `Lesson notes summary for The Heights (2026-05-18 to 2026-05-24)`.
- Database validation after both runs:
  - SQLite integrity: `ok`
  - notes pipeline health: `ready`
  - S3 object updated: `s3://notesreminder-db/reminders.db`, size `90578944`, last modified `2026-05-24T03:44:22+00:00`
- Weekly reminders rows in `reminders.db` for the tested window:
  - `westu-sor`: `220` lessons, `160` with notes, `60` missing
  - `theheights-sor`: `179` lessons, `79` with notes, `100` missing

## Next Planned Work If QuickBooks Stays Deferred

### Phase 20: Repository Layout Migration

Reference: `docs/master_plan.md`, Phase 20.

Goal: move existing Python code into the `notesreminder/` package structure without changing behavior.

Status on 2026-05-24:
- Completed locally on branch `codex/phase-20-layout-migration`.
- Root Python files are now compatibility shims.
- Implementations moved into package areas:
  - `notesreminder/orchestration/`
  - `notesreminder/extractors/`
  - `notesreminder/schema/`
  - `notesreminder/reports/`
  - `notesreminder/transcription/`
  - `notesreminder/mcp/`
  - `notesreminder/lib/`
- MCP default DB path was corrected after the move so it still resolves to the repo-root `reminders.db`.
- `scripts/run_notes_local_mfa.sh` was tightened so common args work through both the shebang path and explicit `zsh` invocation.
- Added regression tests for root entrypoint shims.
- Gate checks:
  - `venv/bin/python -m pytest`: `125 passed`
  - `venv/bin/python run_daily.py --help`: passed
  - local-only notes smoke through root `run_daily.py` shim against `outputs/phase20_shim_notes_smoke.db`: passed, no email, no S3 sync
  - smoke DB integrity: `ok`
  - `venv/bin/python build_reporting_schema.py --db outputs/phase20_shim_notes_smoke.db`: passed
  - MCP shim smoke: `db_status` and `list_tables` returned repo-root `reminders.db` and included `reminders`

Required approval: Hugh approval before merging the large file-move phase.

Expected work:
- Move existing Python modules into package areas created earlier.
- Keep thin top-level shims for:
  - `run_daily.py`
  - `backfill.py`
  - `mcp_server.py`
- Update imports and tests.
- Resolve duplicate modules.
- Keep shell scripts and GitHub workflows working.

Required gate:
- Full active test suite.
- Import smoke tests for top-level shims.
- Production notes run through shim entry point.
- MCP server smoke test.

### Phase 21: Productize And Maintain

Reference: `docs/master_plan.md`, Phase 21.

Goal: make NotesReminder maintainable as an operating platform.

Expected work:
- Operational runbook.
- Backup and restore instructions.
- Source freshness SLAs.
- Fresh-clone setup check.
- Restore-from-backup drill on non-production copy.
- MCP smoke test.
- End-to-end notes plus dashboard generation.

## Suggested First Command Tomorrow

```bash
git status --short
git log --oneline --decorate -5
rg -n "^## Phase 20|^## Phase 21|QuickBooks|deferred" docs/master_plan.md
```

Then open:

```bash
sed -n '786,910p' docs/master_plan.md
sed -n '1,220p' todo.md
```

## Recent Phase Gate Results

- Phase 19 full suite: `122 passed`
- DB integrity: `ok`
- running source imports: `0`
- insight rows missing evidence: `0`
- insight rows not pending human review: `0`

## Recent Backups

- Phase 17 raw captures:
  - `outputs/db_backups/reminders.db.20260523-183256.before-phase-17-raw-captures.bak`
- Phase 19 insights:
  - `outputs/db_backups/reminders.db.20260523-203041.before-phase-19-insights.bak`
- 2026-05-24 daily notes smoke test:
  - local: `outputs/db_backups/reminders.db.20260524-082913.before-just-hugh-smoke-notes-run.bak`
  - S3: `s3://notesreminder-db/backups/reminders-before-just-hugh-smoke-notes-run-20260524-082913.db`

## Resume Prompt

When resuming, say:

> Read `todo.md` and `docs/master_plan.md`, confirm current git status, then continue from the remaining phases. QuickBooks is deferred unless I say otherwise.
