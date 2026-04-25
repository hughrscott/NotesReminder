# Dialpad Authenticated Discovery Checklist

Goal: determine how to load communications, especially full SMS threads.

## Inspect

- History pages: All, Calls, Missed, Voicemails, Recordings, Messages.
- Message list and message thread detail.
- Department filters for `HEIGHTS` and `WESTU`.
- Export/download options for calls, messages, voicemails, and recordings.
- Call detail pages for missed call, voicemail, answered inbound, outbound callback.
- Any visible contact profile IDs or thread IDs.

## Capture

- Screenshot or export evidence for each inspected area.
- Message fields: timestamp, direction, body, sender/recipient, department, read/unread, delivery status, attachments.
- Call fields not currently in `call_logs`: disposition, hangup reason, transfer chain, callback status, agent/user.
- Earliest available SMS history.
- Whether message exports can filter from `2025-01-01`.

## Decide

- Whether SMS initial load uses export, browser extraction, or later API access.
- Whether daily refresh can use date filters or must rescan recent threads.
- Whether department filters are reliable enough for school attribution.
- Whether existing call-log CSV import should remain the source for calls.
