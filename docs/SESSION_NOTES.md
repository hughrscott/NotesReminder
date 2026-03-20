# Session Notes (Resume Here)

Last updated: 2026-03-09

## Current State
- Working DB: `reminders.db` (~67MB). Latest uploaded to S3 via `scripts/publish_db_to_s3.py`.
- MCP DB copy: `reminders_mcp.db` created via `scripts/publish_mcp_db.sh`.
- Claude config uses `REMINDERS_DB_PATH` = `reminders_mcp.db` (safe from overwrite).
- Call transcripts + analyses are in `recording_transcripts`.
- Intent buckets:
  - Rule-based: `intent_bucket` (from `scripts/map_intents.py`)
  - AI-based: `intent_bucket_ai`, `intent_bucket_ai_confidence`, `intent_bucket_ai_reason`, `intent_bucket_ai_run_id`, `intent_bucket_ai_version`, `intent_bucket_ai_updated_at`

## In Progress
- AI intent classification using:
  - `scripts/classify_intents_openai.py`
  - Uses direction-aware prompt + school-context requirement.
  - Run in batches (e.g., `--limit 250`) until `intent_bucket_ai` is filled for all rows.
  - Progress check:
    - `sqlite3 reminders.db "SELECT COUNT(*) FROM recording_transcripts WHERE intent_bucket_ai IS NOT NULL;"`

## Key Scripts
- `scripts/publish_mcp_db.sh` — copy working DB to MCP DB
- `scripts/publish_db_to_s3.py` — upload working DB to S3
- `scripts/db_guard.py` — verify/backup/replace guard to prevent stale DB overwrites
- `scripts/discover_db_sources.py` — list S3 versions/candidate DB keys
- `scripts/recover_legacy_scores.py` — discover/compare/extract legacy score rows
- `scripts/merge_legacy_scores.py` — import matched legacy scores into `lesson_note_scores_history`
- `scripts/rebuild_recording_downloads.py` — rebuild recording_downloads from `recordings/`
- `scripts/find_missed_signup_leads.py` — produce missed leads CSV (default outputs/)
- `scripts/classify_intents_openai.py` — AI bucket classification

## Outputs
- `outputs/missed_signup_leads.csv`
- `outputs/alt_contact_numbers.csv`
- `outputs/alt_contact_numbers_leads.csv`
- `outputs/alt_contact_numbers_leads_with_callback.csv`
- `outputs/matched_legacy_scores.csv`
- `outputs/unmatched_legacy_scores.csv`

## Known Gotchas
- Claude auto-sync from S3 can overwrite MCP DB; use `publish_mcp_db.sh` instead of syncing to working DB.
- `classify_intents_openai.py` default limit is 100; use `--limit 250` and rerun in batches.
- If API errors happen, check `logs/classify_intents_errors.log`.

## Next Steps
1) Finish AI classification in batches.
2) Regenerate missed signup leads using AI buckets.
3) Publish MCP DB (`publish_mcp_db.sh`) and optionally upload to S3.
