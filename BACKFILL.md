# SOR Churn Model — Data Backfill Requirements

## Scrapers to fix (data exists but not captured)

### 1. SMS scraper: populate sender/recipient and person_id
**Script:** `scripts/extract_dialpad_daily_intake.py`
**Problem:**
- `sender` column: 0/10,813 populated
- `recipient` column: 0/10,813 populated
- `person_id` on threads: 8/2,459 populated
**Impact:** Can't identify who sent/received SMS without JOIN to threads table
**Fix:** Extract sender/recipient from Dialpad API response, populate person_id via phone matching
**Priority:** HIGH — SMS is our richest communication source (10,813 messages, current)

### 2. Voicemail scraper: backfill Jan–Jul 2026
**Script:** `scripts/extract_dialpad_voice.py`
**Problem:** 1,446 voicemail transcripts end Jan 2026. No data for Feb–Jul 2026.
**Impact:** 6 months of voicemail transcripts missing. Voicemails are the richest source for parent sentiment (actual spoken words, tone).
**Fix:** Run voicemail scraper with date range Jan–Jul 2026
**Priority:** MEDIUM — 1,446 transcripts already exist, just need 6 more months

### 3. Call reviews: fix student matching
**Script:** `scripts/extract_dialpad_call_reviews.py`
**Problem:** 302 call reviews with transcripts, 0 matched to students.
`call_client_matches` bridge may use different call_id format.
**Impact:** Call review transcripts are the most detailed communication records but can't be linked to students.
**Fix:** Debug call_id format mismatch in `call_client_matches`, or add voice_event_id matching
**Priority:** MEDIUM — 302 reviews exist, just can't match them

## Data to backfill (doesn't exist yet)

### 4. Pike13 lessons: extend beyond Jan 2025
**Script:** `backfill_orchestrator.py --source pike13`
**Problem:** 20,840 lessons from Jan 2025–Jul 2026 (18 months). Need pre-2025 for seasonal baseline computation.
**Why we need it:** NOT for training — for computing stable seasonal attendance baselines (3 years ideal). Current 19 months is tight for single-lap months (Aug–Dec only appears once).
**Impact:** Would fix the seasonal confound that breaks attendance feature signs.
**Fix:** Run Pike13 backfill with start_date=2023-01-01 or earlier. Requires Playwright browser auth (30–60 min/month).
**Priority:** HIGH for baseline computation, LOW for training data

### 5. Pike13 people: get contact info for the 61%
**Problem:** 653/1067 Pike13 people (61%) have NEITHER phone NOR email.
**Impact:** These students can NEVER be matched to communications, no matter how good our bridge is.
**Fix:** Unknown — may require Pike13 admin access to export full contact records
**Priority:** MEDIUM — hard cap on communication coverage

## Bridge gaps (data exists, matching broken)

### 6. Email person_id mismatch
**Problem:** `school_email_messages.person_id` uses UUID format (`person_e21c9d43f0730a5a`) but `pike13_people.person_id` uses numeric IDs (`10927697`).
**Impact:** 0/10,816 emails matched via person_id direct link. All matches via email normalization (slower, more fragile).
**Fix:** Normalize person_id format or add a mapping table
**Priority:** LOW — email normalization matching works adequately (1,358 matched)

### 7. SMS threads: populate person_id
**Related to #1 above** — same fix. Once person_id is populated on threads, SMS matching becomes direct instead of phone-based.
