# Dialpad Communications Discovery

Discovery date: 2026-04-25

Goal: prove where Dialpad communications data lives and how it should feed the lead follow-up database.

Ultimate goal: use the captured communications, lead, and outcome data with an LLM to produce actionable business-improvement insights, including follow-up quality, customer sentiment, missed opportunities, staff coaching opportunities, and whether process changes improve results over time.

## Current Findings

- Main authenticated history routes are available under `https://dialpad.com/app/history/...`.
- Confirmed routes:
  - `/messages`
  - `/all`
  - `/calls`
  - `/missed`
  - `/voicemails`
  - `/recordings`
- The left navigation exposes school/departments including `HEIGHTS` and `WESTU`.
- The history pages expose a visible `Download` control, but discovery has not yet proven whether it exports the current view, all history, or only the desktop app.
- `/voicemails` visibly includes voicemail transcript text in the page body.
- `/recordings` was reachable, but the West U profile showed no visible recordings in the captured sample.
- Existing SQLite data already includes historical `call_logs`, voicemail transcripts, recording URLs, and `recording_transcripts`; the missing piece is proving the repeatable fresh refresh source.

## Refresh Path Assessment

- SMS: browser extraction from `/messages` and feed detail pages is viable; direction/order needs DOM hardening.
- Calls and missed calls: browser extraction from `/calls`, `/missed`, and `/all` is viable for recent visible rows, but row IDs and timestamps need DOM hardening.
- Voicemails: browser extraction from `/voicemails` is viable and should preserve the full visible transcript text.
- Recordings: the UI route exists; the current captured West U sample did not show recordings. Existing recording download/transcription pipeline should remain the fallback until a fresh visible recording path is proven.
- Daily refresh: use a recent rolling-window browser scan first. Do not assume January 1, 2025 backfill is available through the UI until export behavior is proven.

## Data Preservation Rule

Capture the most complete raw artifact available for later LLM analysis:

- full SMS body
- full voicemail transcript when visible
- full recording transcript when available
- source URL and raw visible row/page text
- direction, timestamp, school/department, phone, contact, and call outcome when visible

MCP tools may return summaries by default, but the database should retain full text for later sentiment, intent, urgency, action-item, and outcome analysis.

LLM-derived insights should be stored separately from source data so prompts/models can be rerun without overwriting the original evidence.

## Open Items

- Prove what the Dialpad `Download` button exports for each history route.
- Find a visible recording detail page with download/transcript controls.
- Determine whether call detail pages expose stable call IDs in URLs or DOM attributes.
- Confirm whether department filters can be represented in URLs or must be selected through UI state.
