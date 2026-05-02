# Session Notes (Resume Here)

Last updated: 2026-05-01 evening

## Current State

- `main` is synced to GitHub through `962f38e` plus the next Dialpad cleanup commit should be checked in before stopping.
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

## Tonight's Production Notes Run

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

## Important Gotcha

The production notes pipeline and the lead-intelligence proof currently compete over the same `reminders.db` file:

- `run_daily.py` starts by downloading `s3://notesreminder-db/reminders.db`.
- That S3 DB is currently the production notes DB.
- Lead-intelligence tables may disappear locally after a production notes run unless they are merged back from a lead proof backup.

Tomorrow's first technical decision should be whether to:

1. keep one DB and add a safe merge step after notes runs, or
2. split production notes DB from local lead-intelligence proof DB until the lead pipeline is ready for S3.

Do **not** upload a lead-mutated DB to S3 until this operating model is explicit.

## Next Steps Tomorrow

1. Confirm no leftover browser/process state:

```bash
ps -axo pid,ppid,etime,command | rg "run_daily.py|extract_dialpad|browser_profiles/(dialpad|pike13)"
```

2. Decide DB operating model for lead proof vs production notes.
3. If continuing the lead proof locally, restore or merge from:

```bash
outputs/db_backups/reminders.db.20260501-211741.before-local-mfa-notes-run.bak
```

4. Regenerate:

```bash
python3 scripts/progress_dashboard.py --db reminders.db --window-days 7 --pike13-lookahead-days 30
python3 scripts/lead_attention_report.py --db reminders.db --school "West U" --window-days 7
python3 scripts/unmatched_inbound_report.py --db reminders.db --school "West U" --window-days 2
```

5. Review Phase 3 check-in:
   - Dialpad intake is no longer blocked.
   - Pagination works.
   - Call-review ingestion needs progress logging and bounded retries before broad backfill.
6. Then move to Phase 4:
   - Pike13 rich lead/outcome hardening.

## Files To Check In

These should be committed before stopping:

- `scripts/extract_dialpad_daily_intake.py`
- `scripts/extract_dialpad_call_reviews.py`
- `docs/SESSION_NOTES.md`

Leave these untracked files alone unless the user confirms they are intentional:

- `package.json`
- `package-lock.json`
