# Source Completeness Proof

The next gate before LLM insights is proving that the source data is complete enough for lead timelines.

HubSpot is the lead spine. Every lead/deal should eventually be loaded by create/update date regardless of status. Dialpad and Pike13 are date-indexed event streams that are matched back to HubSpot after ingestion.

## Test Window

Start with a small proof window:

- HubSpot: last 7 days of created or updated deals, all statuses.
- Dialpad: last 7 days of SMS, calls, missed calls, voicemails, recordings, and transcripts where available.
- Pike13: last 7 days plus upcoming 30 days for visits/plans where visible.

Do not run a full January 1, 2025 backfill until the 7-day and 30-day proofs show acceptable coverage and matching.

## Completeness Report

Run:

```bash
python3 scripts/source_completeness_report.py --db reminders.db --window-days 7 --pike13-lookahead-days 30 --pretty
```

The report returns:

- `overall_status`: `ready`, `partial`, or `blocked`.
- Per-source row counts, recent-window counts, latest timestamps, field fill rates, import-run status, and blockers.
- Dialpad diagnostics for SMS extraction source, inferred direction count, future timestamp count, transcript coverage, source ID visibility, and school/department coverage.
- Identity-match counts by match type.

Timestamp rule:

- Source event timestamps are business evidence and should be captured separately from import/update timestamps.
- Use source-specific event fields for when something happened in the source system, such as HubSpot `create_date`, Dialpad SMS `message_at`, Dialpad voice `event_at`, Pike13 visit `starts_at`, and lesson-note timestamps.
- Use `updated_at` only for when the local database row was imported or refreshed.
- If a source omits the year, normalize to the most recent non-future date unless the source explicitly represents a future appointment.

Latest 7-day proof results after HubSpot lead-spine and enrichment hardening:

- HubSpot status: `ready`.
- Visible HubSpot proof rows: 25 deals from the current list view.
- HubSpot readiness coverage: stage 100%, school 100%, source URL 100%, raw text 100%, create date 84.0%.
- HubSpot enrichment coverage: 24 trusted customer-contact rows, 24 trusted customer emails, 24 trusted phone rows, and 13 rows with rejected internal School of Rock emails recorded in `raw_json`.
- HubSpot enrichment validation: no curated HubSpot deal field contains known placeholder values such as `Details`, `- Deal`, `- Display Deal`, or `GA UTM Term - Deal`.
- HubSpot extraction note: the list view provides stage, owner, and school reliably; create date currently requires visiting deal detail pages unless a HubSpot view exposes it directly.
- HubSpot extractor note: use `--school "West University Place"` when running a school-specific proof from a mixed-school view.

Known remaining blockers are expected until the non-HubSpot extractors are hardened:

- Pike13 visit/outcome coverage.

Latest Dialpad communications hardening proof:

- Dialpad status: `ready`.
- SMS rows: 17; direction coverage 100%; message timestamp coverage 94.1%; no future SMS timestamps.
- Voice rows: 33,974 including existing historical calls/transcripts plus the current browser proof; direction coverage 100%; event timestamp coverage 100%; phone coverage 98.5%.
- The browser proof captured calls, missed calls, voicemails, voicemail transcript text where visible, and recording-route availability.
- Dialpad rows now preserve extraction diagnostics in `raw_json`, including thread-detail versus list fallback, observed versus inferred direction, source ID visibility, transcript visibility, source timestamp field, and import timestamp field.
- Latest import-run errors now count as Dialpad blockers because the refresh path must be proven, not just the stored historical data quality.
- Interactive login mode is available for both SMS and voice extractors when the Dialpad profile expires.

Latest live proof after interactive-login support:

- SMS interactive proof succeeded after login: 7 source pages processed, 119 rows written.
- SMS stored rows after cleanup: 32; direction coverage 100%; message timestamp coverage 100%; no future SMS timestamps.
- SMS extraction diagnostics: 32 rows from `message_list`, 26 inferred directions, 6 observed directions.
- Voice interactive proof succeeded on 2026-04-28: 44 visible voice rows written across `/history/all`, `/history/calls`, `/history/missed`, `/history/voicemails`, and `/history/recordings`.
- Voicemail proof captured visible voicemail transcript text: 2 voicemail rows from `/history/voicemails` with transcript text.
- Recording proof captured a recording route/link, but no visible call/recording transcript link. This remains the explicit Dialpad blocker and likely needs export/API or recording-detail discovery.
- Browser voice rows now report per-route proof metadata in the latest `source_import_runs.metadata_json`.
- Remaining Dialpad work before broad backfill: confirm whether call/recording transcripts are export-only, API-only, or available through a deeper recording detail page; confirm how many SMS rows can come from true thread detail pages.

The next planned phase is Pike13 outcome hardening after Dialpad communications remains stable: visits, trials, no-shows, memberships/plans, and source event timestamps.

## Matching Priority

Deterministic matching is refreshed when the report runs:

1. HubSpot deal direct Pike13 person ID.
2. Exact normalized email.
3. Exact normalized phone.
4. Exact name plus school, lower confidence.

These matches populate `identity_matches`; they do not overwrite source data.
