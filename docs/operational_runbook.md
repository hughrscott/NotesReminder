# NotesReminder Operational Runbook

This runbook is the day-to-day operating guide for the canonical production
database, `reminders.db`, and the tools that read or update it.

## Operating Principles

- `reminders.db` is the single local production database.
- `s3://notesreminder-db/reminders.db` is the production S3 sync target.
- Root Python files are compatibility entry points. The implementation code now
  lives under `notesreminder/`.
- Daily notes email remains the production-critical workflow.
- Lead dashboards, scorecards, and AI insights remain shadow or experimental
  until Hugh explicitly promotes them.
- Do not delete or overwrite timestamped DB backups.
- Do not archive raw captures to S3. Raw captures remain local under `raw/`.
- Do not enable unattended production notes/email/DB/S3 cadence without explicit
  Hugh approval.

## Directory Map

- `run_daily.py`, `backfill.py`, `mcp_server.py`: root compatibility entry points.
- `notesreminder/orchestration/`: notes runner, backfill, cadence, date-window orchestration.
- `notesreminder/extractors/`: Pike13/Dialpad/source extraction implementations.
- `notesreminder/schema/`: SQLite schema, migrations, reporting sync.
- `notesreminder/reports/`: health checks, dashboards, scorecards, source completeness.
- `notesreminder/transcription/`: recording download/transcription/analysis implementations.
- `notesreminder/mcp/`: MCP server tools.
- `notesreminder/lib/`: shared helpers.
- `scripts/`: command wrappers and operational CLI tools.
- `outputs/db_backups/`: preserved local DB rollback points.
- `outputs/progress/`: generated health, dashboard, scorecard, and cadence evidence.
- `logs/`: local run logs and email-delivery evidence.

## Daily Notes Workflow

Use the MFA-aware local wrapper when Pike13 requires interactive auth:

```bash
scripts/run_notes_local_mfa.sh --date YYYY-MM-DD
```

This wrapper:

- creates a local DB backup under `outputs/db_backups/`
- creates an S3 backup under `s3://notesreminder-db/backups/`
- runs both schools through `run_daily.py`
- sends the normal production recipient emails
- uploads the refreshed DB to `s3://notesreminder-db/reminders.db`

For a just-Hugh smoke test, run the root shim directly for each school/date with
`--to huscott@schoolofrock.com`. Do this when validating production mechanics
without notifying staff.

For local-only validation, never email or sync S3:

```bash
venv/bin/python run_daily.py \
  --school westu-sor \
  --start-date YYYY-MM-DD \
  --end-date YYYY-MM-DD \
  --db-path outputs/smoke_notes.db \
  --skip-s3-sync \
  --no-email \
  --skip-note-scoring \
  --pike13-profile-dir browser_profiles/pike13
```

Post-run checks:

```bash
sqlite3 reminders.db "PRAGMA integrity_check;"
venv/bin/python scripts/notes_pipeline_health.py --db reminders.db --as-of YYYY-MM-DD --lookback-days 7
```

Confirm:

- integrity returns `ok`
- notes health reports `Overall status: ready`
- expected email evidence is `delivered` in the health report
- S3 `reminders.db` `LastModified` changed after a production run

## Source Freshness SLAs

These are operating targets, not yet automated enforcement rules.

| Source | Target Freshness | Check |
| --- | ---: | --- |
| Notes/Pike13 lesson scrape | daily, expected lag <= 1 day | `scripts/notes_pipeline_health.py` |
| Dialpad daily intake | daily or next business day | `scripts/source_completeness_report.py --window-days 7` |
| Dialpad call reviews/transcripts | weekly for high-signal/recent rows | source completeness and lead dashboards |
| School email extraction | weekly or before lead-review work | source completeness and lead dashboards |
| HubSpot lead refresh | weekly or before lead-review work | source completeness |
| Pike13 lead outcome extraction | weekly during active proof periods | source completeness |
| Person identity refresh | after source refresh batches | `scripts/refresh_person_identities.py --json` |
| Dashboards/scorecards | shadow generation weekly or on demand | cadence shadow run |

If a source misses its target, keep production notes running and record the
source as `attention` or `blocked` in source completeness rather than silently
mixing stale data into operating decisions.

## Shadow Reporting Cadence

Dry-run the cadence plan:

```bash
venv/bin/python scripts/cadence_runner.py --date YYYY-MM-DD
```

Run shadow-only reports:

```bash
venv/bin/python scripts/cadence_runner.py --date YYYY-MM-DD --execute-shadow
```

Do not pass `--execute-production` unless Hugh has explicitly approved a
production notes/email run through the cadence runner.

## Unified Refresh Wrapper

Use the unified wrapper to plan or run the daily all-source workflow:

```bash
venv/bin/python scripts/refresh_all_sources.py \
  --mode daily \
  --date YYYY-MM-DD
```

Default daily mode is metadata-only dry-run. It writes a JSON plan under
`outputs/progress/refresh_all_sources/`.

To execute mutating source refreshes under supervision:

```bash
venv/bin/python scripts/refresh_all_sources.py \
  --mode daily \
  --date YYYY-MM-DD \
  --execute-refresh \
  --execute-verification \
  --interactive-login \
  --backup
```

Production notes email and S3 upload are still separately gated. To use the
normal two-school production notes wrapper inside the unified refresh, all three
flags are required:

```bash
--execute-production-notes --send-email --upload-s3
```

Use weekly completeness mode for read-only verification:

```bash
venv/bin/python scripts/refresh_all_sources.py \
  --mode weekly-completeness \
  --as-of YYYY-MM-DD \
  --execute-verification
```

Weekly completeness runs integrity, notes health, source completeness,
notes-read-path comparison, unmatched inbound reports, lead attention reports,
operating dashboards, and scorecards. It writes run metadata under
`outputs/progress/refresh_all_sources/` and report outputs under
`outputs/progress/weekly_completeness/{YYYY-MM-DD}/`.

Historical backfill should happen after this wrapper is proven on recent daily
and weekly windows, so backfilled data can be validated through the same
completeness checks.

## MCP Smoke Test

Run this from the repo root:

```bash
venv/bin/python - <<'PY'
import json
import mcp_server

status = json.loads(mcp_server.db_status())
tables = json.loads(mcp_server.list_tables())
assert status["reminders"]["exists"]
assert status["reminders"]["path"].endswith("reminders.db")
assert "reminders" in set(tables["tables"])
print("MCP smoke ok")
PY
```

Useful MCP tools after the smoke check:

- `db_status`
- `list_tables`
- `source_completeness`
- `daily_snapshot`
- `weekly_snapshot`
- `monthly_snapshot`
- `note_quality_scorecard`
- `person_search`
- `person_journey`

## Backup Procedure

Before any production DB mutation outside a normal `run_daily.py` S3 sync:

```bash
STAMP="$(date +%Y%m%d-%H%M%S)"
cp reminders.db "outputs/db_backups/reminders.db.${STAMP}.manual-backup.bak"
```

Create an S3 backup:

```bash
venv/bin/python - <<'PY'
import datetime as dt
import boto3

stamp = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
bucket = "notesreminder-db"
source = "reminders.db"
backup = f"backups/reminders.manual-backup.{stamp}.db"
s3 = boto3.client("s3")
s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": source}, Key=backup)
print(f"s3://{bucket}/{backup}")
PY
```

## Restore Drill

Run restore drills only against a non-production copy:

```bash
mkdir -p outputs/progress/restore_drill
cp reminders.db outputs/progress/restore_drill/restored_copy.db
sqlite3 outputs/progress/restore_drill/restored_copy.db "PRAGMA integrity_check;"
venv/bin/python build_reporting_schema.py --db outputs/progress/restore_drill/restored_copy.db
venv/bin/python scripts/notes_pipeline_health.py \
  --db outputs/progress/restore_drill/restored_copy.db \
  --as-of YYYY-MM-DD \
  --lookback-days 7
```

The drill passes when integrity is `ok`, reporting sync succeeds, and notes
health returns `Overall status: ready`.

## Fresh-Clone Setup Check

From a clean checkout:

```bash
cp .env.example .env
python3 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/pip install -r requirements-dev.txt
venv/bin/python -m playwright install
venv/bin/python -m pytest
venv/bin/python run_daily.py --help
venv/bin/python - <<'PY'
import run_daily, mcp_server
print("imports ok")
PY
```

Do not run a production notes command from a fresh clone until `.env`,
`browser_profiles/`, `reminders.db`, and S3 credentials have been verified.

## Incident Response

If production notes fails:

1. Preserve the log and screenshots.
2. Run `scripts/notes_pipeline_health.py`.
3. Check whether Pike13 auth/MFA expired.
4. Use `scripts/run_notes_local_mfa.sh --date YYYY-MM-DD` for supervised recovery.
5. Verify integrity, health, email delivery, and S3 `LastModified`.

If a source refresh fails:

1. Do not upload the touched DB to S3 until source completeness is reviewed.
2. Run source completeness and the relevant progress dashboard.
3. Preserve route-discovery/auth diagnostics.
4. Fix the extractor or auth state on a local DB copy first.

If DB integrity fails:

1. Stop writes.
2. Copy the failing DB to `outputs/db_backups/` for diagnosis.
3. Restore the most recent known-good local or S3 backup to a scratch copy.
4. Run `PRAGMA integrity_check`, reporting sync, and notes health on the scratch copy.
5. Replace production only after Hugh approves the rollback.

## Promotion Boundaries

- Normal notes emails remain the only staff-facing production email path.
- Dashboards and scorecards remain shadow until Hugh approves publication.
- Phase 19 AI insights remain experimental and human-review-only.
- QuickBooks remains deferred until the API/browser extraction path is chosen.
- Unattended cadence remains disabled until explicitly promoted.
