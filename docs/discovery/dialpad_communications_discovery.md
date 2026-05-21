# Dialpad Communications Discovery

Discovery date: 2026-04-25

Goal: prove where Dialpad communications data lives and how it should feed the lead follow-up database.

Ultimate goal: use the captured communications, lead, and outcome data with an LLM to produce actionable business-improvement insights, including follow-up quality, customer sentiment, missed opportunities, staff coaching opportunities, and whether process changes improve results over time.

## Current Findings

- Main authenticated history routes are available under `https://dialpad.com/app/history/...`.
- Conversation History is available at `https://dialpad.com/conversationhistory` and is the preferred call-transcript discovery route because it exposes date filters, recording/play actions, and Dialpad AI actions per call row.
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

- SMS: browser extraction from `/messages` and feed detail pages is viable. Rows now record whether they came from true thread detail, the message list, or fallback visible text, and whether direction was observed or inferred.
- Calls and missed calls: browser extraction from `/calls`, `/missed`, and `/all` is viable for recent visible rows. Rows preserve source event timestamps in `event_at` and record whether a source call ID was visible or a generated hash was used.
- Voicemails: browser extraction from `/voicemails` is viable and preserves the full visible transcript text when present.
- Recordings: the UI route exists; the current captured West U sample did not show recordings. Existing recording download/transcription pipeline should remain the fallback until a fresh visible recording path is proven.
- Daily refresh: use a recent rolling-window browser scan first. Do not assume January 1, 2025 backfill is available through the UI until export behavior is proven.
- Authentication: extractors support `--interactive-login` for headed Okta/Dialpad login when the persistent profile expires. Headless runs remain strict and fail if they land on the login page.
- Voice proof update: the 2026-04-28 interactive proof captured calls, missed calls, voicemails, visible voicemail transcripts, and a recording link. It did not expose a visible call/recording transcript link, so recording transcript refresh remains blocked pending export/API/deeper detail-page discovery.
- Call transcript strategy: prefer Dialpad AI transcripts from Conversation History when available; always preserve recording/play URL or access evidence so recordings can be downloaded and self-transcribed later for missing, important, or quality-audit calls.
- Conversation History proof update: the DOM extractor captured 25 dated call rows from `https://dialpad.com/conversationhistory`; 19 rows exposed stable `dialpad.com/callhistory/callreview/<id>` links and recording/play access. The call review page exposes the audio player, recap/action items, and transcript panel.
- 2026-05-21 Call Review route proof: the authenticated Dialpad profile opened `dialpad.com/callhistory/callreview/5646748416811008`, and the extractor detected recap, one action item, audio availability, and 19 transcript speaker turns. This validates the current primary transcript route without needing to download the call audio by default.

## Data Preservation Rule

Capture the most complete raw artifact available for later LLM analysis:

- full SMS body
- full voicemail transcript when visible
- full recording transcript when available
- recording URL/access evidence for every call where visible, even when we do not download audio immediately
- source URL and raw visible row/page text
- direction, timestamp, school/department, phone, contact, and call outcome when visible

MCP tools may return summaries by default, but the database should retain full text for later sentiment, intent, urgency, action-item, and outcome analysis.

LLM-derived insights should be stored separately from source data so prompts/models can be rerun without overwriting the original evidence.

Source timestamp rule:

- SMS uses `message_at` for when the customer/staff text was sent or received.
- Voice uses `event_at` for when the call, missed call, voicemail, or recording happened.
- `updated_at` is only the local import time and should not be used to measure response speed or follow-up behavior.
- When Dialpad omits the year, the extractor normalizes to the most recent non-future date.

## Open Items

- Prove what the Dialpad `Download` button exports for each history route.
- Find a visible recording detail page with download/transcript controls.
- Follow Conversation History call-review URLs, capture full transcript text plus recap/action items, and preserve the call-review URL as the recording/play access pointer for each call where Dialpad exposes one.
- Determine whether call detail pages expose stable call IDs in URLs or DOM attributes.
- Confirm whether department filters can be represented in URLs or must be selected through UI state.
