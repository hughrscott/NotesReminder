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
- Identity-match counts by match type.

Latest 7-day proof results after HubSpot lead-spine hardening:

- HubSpot status: `ready`.
- Visible HubSpot proof rows: 22 deals from the current list view, including 13 West University Place rows and 9 The Heights rows.
- HubSpot readiness coverage: stage 100%, school 100%, source URL 100%, raw text 100%, create date 95.5%.
- HubSpot extraction note: the list view provides stage, owner, and school reliably; create date currently requires visiting deal detail pages unless a HubSpot view exposes it directly.
- HubSpot extractor note: use `--school "West University Place"` when running a school-specific proof from a mixed-school view.

Known remaining blockers are expected until the non-HubSpot extractors are hardened:

- Dialpad SMS direction and valid message timestamp coverage.
- Pike13 visit/outcome coverage.

After HubSpot enrichment parsing is hardened, the next planned phase is Dialpad communications hardening: SMS direction, message timestamps, calls, missed calls, voicemails, voicemail transcripts, recordings, call transcripts, and follow-up evidence.

## Matching Priority

Deterministic matching is refreshed when the report runs:

1. HubSpot deal direct Pike13 person ID.
2. Exact normalized email.
3. Exact normalized phone.
4. Exact name plus school, lower confidence.

These matches populate `identity_matches`; they do not overwrite source data.
