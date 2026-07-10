# NotesReminder: Automation Proposal for Daily Data Population

**Date:** July 9, 2026  
**Author:** System Analysis  
**Status:** Proposal — pending approval

---

## Executive Summary

This document proposes a phased plan to fully automate daily data collection from **HubSpot**, **Gmail**, **Dialpad**, and **Pike13** (beyond lesson notes) so the NotesReminder SQLite database stays current without manual intervention. The Pike13 auto-MFA breakthrough (via IMAP email code reading) provides a proven pattern that can be adapted for other services. An existing orchestrator (`run_date_window_lead_load.py`) already runs all extractors as subprocesses but targets a separate lead intelligence DB and lacks automated auth for HubSpot, Dialpad, and Gmail.

---

## 1. Current State Analysis Per Data Source

### 1.1 HubSpot

**Current state:**  
- `scripts/extract_hubspot_leads.py` (746 lines) — mature Playwright scraper
- Extracts deals (list/board view + detail pages) and contacts from `app.hubspot.com/contacts`
- Parses deal stages, trial dates, instruments, lead sources, Pike13 person IDs
- Uses `browser_profiles/hubspot` as a persistent Playwright profile
- **No authentication handling at all** — assumes the profile is already logged in
- Extractor navigates directly to the contacts page; if the profile is expired, it silently scrapes a login page

**Auth status:**  
- HubSpot uses email-based login with MFA (likely email code or authenticator app)
- No credentials stored anywhere in the codebase
- The profile directory (`browser_profiles/hubspot`) contains whatever cookies were last saved — if expired, the extractor fails silently, producing zero rows in the DB
- HubSpot's session cookies typically last 24-48 hours; with "Remember this device" they can last 30 days

**Recommended approach:**  
- **Keep Playwright scraping** — HubSpot's API tier requires Enterprise plan and has rate limits. The existing scrapers are well-tested and extract richer data than the API exposes.
- **Add auto-MFA** using the same IMAP email code pattern proven with Pike13: snapshot inbox → login → detect MFA page → poll email for 6-digit code → enter code → store cookies
- Create `scripts/auto_auth_hubspot.py` with the full login+MFA flow

**New files to create:**
1. `scripts/auto_auth_hubspot.py` — auto-auth module (login + email-MFA)
2. `scripts/hubspot_session_manager.py` — credential store + profile health checks

**Files to modify:**
1. `scripts/extract_hubspot_leads.py` — add `--auto-auth` flag, call `auto_auth_hubspot` module before scraping
2. `/home/ubuntu/.hermes/SOR/.sorenv` — add `HUBSPOT_EMAIL` and `HUBSPOT_PASSWORD`
3. `scripts/run_date_window_lead_load.py` — add `--hubspot-auto-auth` flag  

**Estimated complexity:** **Medium**  
HubSpot's login page is straightforward (email + password + email code). The main risk is selector brittleness — HubSpot updates its UI more aggressively than Pike13. Mitigation: fall back to interactive login if auto-auth fails.

---

### 1.2 Dialpad

**Current state:**  
- Four extractor scripts, all sharing `browser_profiles/dialpad`:
  - `scripts/extract_dialpad_voice.py` (760 lines) — call history, voicemails, recordings, conversation history
  - `scripts/extract_dialpad_sms.py` (570 lines) — SMS threads and messages
  - `scripts/extract_dialpad_daily_intake.py` (367 lines) — daily conversation intake summary
  - `scripts/extract_dialpad_call_reviews.py` (368 lines) — call transcripts, recaps, action items
- All extractors have `wait_for_authenticated_page()` with `--interactive-login` support
- Extractors detect login pages and either fail or wait for manual login
- **No automated MFA** — relies on persistent browser profile cookies staying fresh

**Auth status:**  
- Dialpad uses email-based login with multiple SSO options (Google, Microsoft)
- Unknown whether Dialpad enforces MFA on every login or remembers devices
- The `browser_profiles/dialpad` directory stores cookies; if they expire, the extractor prompts for interactive login
- No credentials stored for Dialpad login

**Recommended approach:**  
- **Keep Playwright scraping** — Dialpad's API is limited and the scrapers already extract rich data (transcripts, call outcomes, SMS threads)
- **Adapt the Pike13 auto-MFA pattern** — but Dialpad's MFA may differ (could be Google SSO redirect, or email code)
- **Discovery step required:** Manually log in to Dialpad to determine the MFA method. If email-based, the IMAP pattern works directly. If Google SSO, we may need to handle the Google auth flow (which may already be in the Okta cookie chain)
- Create `scripts/auto_auth_dialpad.py`

**New files to create:**
1. `scripts/auto_auth_dialpad.py` — auto-auth module
2. `scripts/dialpad_session_manager.py` — health checks for the shared dialpad profile

**Files to modify:**
1. `scripts/extract_dialpad_voice.py` — add `--auto-auth` flag
2. `scripts/extract_dialpad_sms.py` — add `--auto-auth` flag
3. `scripts/extract_dialpad_daily_intake.py` — add `--auto-auth` flag  
4. `scripts/extract_dialpad_call_reviews.py` — add `--auto-auth` flag
5. `/home/ubuntu/.hermes/SOR/.sorenv` — add `DIALPAD_EMAIL` and `DIALPAD_PASSWORD`
6. `scripts/run_date_window_lead_load.py` — add `--dialpad-auto-auth` flag

**Estimated complexity:** **High**  
Dialpad's auth UX is the most uncertain. It may involve Google SSO redirects, enterprise Okta SSO, or custom MFA. A manual discovery session is needed before implementation. Fallback: keep `--interactive-login` as the safety net.

---

### 1.3 Gmail (School of Rock email)

**Current state:**  
- `scripts/extract_school_emails.py` (357 lines) — Playwright-based Gmail scraper
- Opens `mail.google.com`, navigates via Gmail search queries, clicks message rows, extracts email content
- Uses **Okta SSO** via `browser_profiles/sor_okta`
- Has auto-fill for Okta username/password from environment variables
- Handles Okta Verify push: fills credentials, prompts user to approve push on phone
- **No fully automated MFA** — requires manual Okta Verify approval

**Auth status:**  
- SOR Gmail uses Okta SSO (sor.okta.com → Google Workspace)
- Okta credentials already stored in `OKTA_USERNAME` and `OKTA_PASSWORD`
- The bottleneck is **Okta Verify push** — no automated way to approve it
- Okta may offer email-based MFA as an alternative (needs verification)

**Recommended approach:**  
- **Investigate Okta email-based MFA as alternative** — if Okta supports sending a code via email instead of push, the IMAP pattern works
- **If push-only:** Consider using Okta's TOTP (time-based one-time password) for programmatic auth — we'd need to extract the TOTP seed from Okta setup
- **Alternative:** Leverage the existing Okta cookies from `browser_profiles/sor_okta` — if the "Remember this device" option works, cookies may last weeks. Add cookie freshness monitoring with auto-refresh when stale
- **Least-effort path:** Accept that Gmail extraction requires occasional manual Okta push approval (e.g., every 30 days if cookies persist). Add alerting when cookies expire.

**New files to create:**
1. `scripts/gmail_session_monitor.py` — cookie health monitoring with alerting

**Files to modify:**
1. `scripts/extract_school_emails.py` — add cookie-health pre-check with clearer error messages
2. Possibly none if cookies are stable enough

**Estimated complexity:** **Medium-High**  
Fully automated Okta Verify push approval is not feasible. The practical path is cookie persistence + monitoring. If email-based MFA is available, complexity drops to **Medium**.

---

### 1.4 Pike13 (additional data beyond lesson notes)

**Current state:**  
- `run_daily.py` (1122 lines) handles the notes pipeline: login → scrape lessons → read notes → GPT-4o-mini scoring → email → S3 sync
- `noteschecker.py` (603 lines) has working auto-MFA via `pike13_auto_auth.py`
- `scripts/extract_pike13_leads.py` (1266 lines) extracts people and first visits from Pike13 First Visits report
- `scripts/extract_pike13_cookies.py` (156 lines) extracts Okta SSO cookies for Pike13

**What's already automated:**  
- Lesson notes scraping via `run_daily.py` (the main pipeline)
- Auto-MFA for Pike13 login (the breakthrough)

**What's NOT yet in the daily flow:**  
- Pike13 people/lead extraction (`extract_pike13_leads.py`) runs separately, not as part of `run_daily.py`
- First Visits report data (trial bookings, enrollment state, account manager info)
- Pike13 attendance data (beyond what's in lesson notes)
- Pike13 person identity linking (exists in `source_completeness.py` but runs ad-hoc)

**Recommended approach:**  
- **Integrate Pike13 leads into the daily flow** — since the auth is already handled by `pike13_auto_auth.py`, the extractor just needs to be called
- **Low-hanging fruit:** After the notes pipeline completes in `run_daily.py`, call `extract_pike13_leads.py` with the same authenticated context (or reuse cookies)
- **Additional Pike13 data sources to consider:**
  - People list (`/people`) — names, emails, phones, membership state
  - First Visits report — trial bookings with rich metadata
  - Attendance history — detailed attendance records including late cancels, no-shows
  - Account notes — staff notes on accounts (may require different permissions)

**Files to modify:**
1. `run_daily.py` — add optional call to `extract_pike13_leads.py` after the notes pipeline
2. `scripts/extract_pike13_leads.py` — add `--use-running-context` flag to accept an already-authenticated browser context

**Estimated complexity:** **Low**  
Auth is already solved. The main work is integration into the daily flow.

---

## 2. Unified Orchestration Plan

### 2.1 Current Architecture

There are **two separate orchestrators** operating on different databases:

```
run_daily.py                          run_date_window_lead_load.py
├── reminders.db (S3 primary)         ├── lead_intelligence_working.db (shadow)
├── Pike13 notes pipeline             ├── HubSpot deals + contacts
├── Note scoring + email              ├── Pike13 people + visits  
└── S3 sync (download → update       ├── Dialpad voice + SMS + reviews
    → upload)                         ├── Gmail email messages
                                      ├── Gap analysis report
                                      └── Outputs markdown + JSON reports
```

Both share the same lead-followup schema and source completeness infrastructure, but they target different databases. The proposal is to **unify them on reminders.db**.

### 2.2 Proposed Unified Pipeline

```python
# new: scripts/run_unified_daily.py
async def main():
    # Phase 0: Download DB from S3
    download_db_from_s3()

    # Phase 1: Pike13 lesson notes (existing, already automated)
    await run_pike13_notes_pipeline(school_subdomain, start_date, end_date)
    
    # Phase 2: Pike13 lead data (people + visits — same auth session)
    await run_pike13_leads_pipeline(school_subdomain, start_date, end_date)
    
    # Phase 3: HubSpot (needs auto-auth)
    run_hubspot_extraction(start_date, end_date)
    
    # Phase 4: Dialpad voice + SMS (needs auto-auth, shared profile)
    run_dialpad_extraction(start_date, end_date)
    
    # Phase 5: Gmail extraction (cookie-based, with monitoring)
    run_gmail_extraction(start_date, end_date)
    
    # Phase 6: Data integration
    refresh_identity_matches()
    sync_reporting_tables()
    
    # Phase 7: Source completeness check
    build_source_completeness_report()
    
    # Phase 8: Email summary report
    send_daily_summary_email()
    
    # Phase 9: Upload DB to S3
    upload_db_to_s3()
```

### 2.3 Order of Operations

| Step | Source | Dependencies | Auth Method | Parallelizable? |
|------|--------|-------------|-------------|-----------------|
| 0 | DB download | None | AWS SDK | — |
| 1 | Pike13 notes | DB available | Auto-MFA (IMAP) | Base |
| 2 | Pike13 leads | Pike13 auth from step 1 | Reuse cookies/context | With step 1 |
| 3 | HubSpot | DB available | Auto-auth (IMAP) | Parallel with 4,5 |
| 4 | Dialpad | DB available | Auto-auth (TBD) | Parallel with 3,5 |
| 5 | Gmail | DB available | Cookie persistence | Parallel with 3,4 |
| 6 | Identity matching | Steps 1-5 done | — | Serial |
| 7 | Reporting sync | Steps 1-5 done | — | Serial |
| 8 | Source completeness | Steps 1-7 done | — | Serial |
| 9 | Email report | Steps 1-8 done | SMTP | — |
| 10 | DB upload | Steps 1-9 done | AWS SDK | — |

**Key parallelism opportunity:** Steps 3 (HubSpot), 4 (Dialpad), and 5 (Gmail) can run concurrently since they use independent browser profiles and target different DB tables. This could cut total runtime from ~45 minutes to ~20 minutes.

### 2.4 Error Handling and Retry Strategy

```
For each source extraction:
  1. Pre-flight check: Verify browser profile exists, credentials present
     - If credentials missing → log warning, skip source, continue
     - If profile missing → create it, attempt auto-auth
  
  2. Auto-auth attempt (3 retries with exponential backoff):
     - Attempt 1: Fresh login + MFA
     - Attempt 2: Clear cookies, retry login (5 min wait)
     - Attempt 3: Headed browser fallback (15 min wait)
     - All failed → log error, skip source, flag in completeness report
  
  3. Data extraction (2 retries):
     - Attempt 1: Run extractor normally
     - Attempt 2: Refresh page, re-run
     - Both failed → log error, capture raw_capture for debugging
  
  4. Per-source outcome tracking:
     - Record in source_import_runs table (already exists)
     - Track: rows_seen, rows_written, error, duration
     - Source completeness report shows staleness per source
```

### 2.5 Credential Management

**Current state:** Scattered across `.sorenv`, `.env`, and nowhere (HubSpot, Dialpad).

**Proposed unified approach:**

```
/home/ubuntu/.hermes/SOR/.sorenv  (existing, expand)
├── AWS_ACCESS_KEY_ID              ✓
├── AWS_SECRET_ACCESS_KEY          ✓
├── PIKE13_USER                    ✓
├── PIKE13_PASSWORD                ✓
├── OKTA_USERNAME                  ✓
├── OKTA_PASSWORD                  ✓
├── SENDER_EMAIL                   ✓
├── SENDER_PASSWORD                ✓
├── OPENAI_API_KEY                 ✓
├── TRANSCRIBE_BUCKET              ✓
├── HUBSPOT_EMAIL                  NEW
├── HUBSPOT_PASSWORD               NEW
├── DIALPAD_EMAIL                  NEW
├── DIALPAD_PASSWORD               NEW
└── SOR_APP_PASSWORD               ✓ (in ~/.hermes/.env)

Browser profiles (persistent, stored on disk):
├── browser_profiles/hubspot/      (needs initial manual login)
├── browser_profiles/dialpad/      (needs initial manual login)
├── browser_profiles/pike13/       (auto-MFA keeps it fresh)
├── browser_profiles/sor_okta/     (Okta SSO for Gmail)
└── ~/.pike13_profile/             (fallback)
```

**Security notes:**
- Credentials in `.sorenv` are already excluded from git via `.gitignore`
- Browser profiles contain session cookies and should NOT be committed
- For CI/GitHub Actions: use GitHub Secrets for the env vars
- For local runs: `.sorenv` + `load_dotenv()`

---

## 3. Phased Implementation Plan

### Phase 1: Immediate Wins (Week 1)

**What can be done right now with existing code:**

#### 1.1 Integrate Pike13 leads into run_daily.py

Pike13 auth already works. The extractor just needs to be called.

**Changes to `run_daily.py`:**

```python
# After the notes CSV is read and DB updated (around line 1018):
# ... existing notes pipeline ...

# Phase 2: Pike13 lead extraction (runs subprocess with cookie reuse)
if not args.skip_pike13_leads:
    log("Extracting Pike13 leads (people + visits)...", force=True)
    import subprocess
    lead_result = subprocess.run([
        sys.executable,
        "scripts/extract_pike13_leads.py",
        "--db", DB_PATH,
        "--profile-dir", args.pike13_profile_dir or "browser_profiles/pike13",
        "--base-url", f"https://{school_subdomain}.pike13.com",
        "--school", format_school_label(school_subdomain),
        "--first-visits-start-date", start_date,
        "--first-visits-end-date", end_date,
        "--headless",
        "--reauth-if-needed",
    ], capture_output=True, text=True, timeout=600)
    log(f"Pike13 leads: {lead_result.stdout.strip()}", force=True)
    if lead_result.returncode != 0:
        log(f"⚠️ Pike13 leads extraction failed: {lead_result.stderr[:500]}")
```

**Files modified:** `run_daily.py` (add ~15 lines)  
**Complexity:** Low — auth is already solved

#### 1.2 Add source-completeness monitoring to the daily report

The `source_completeness.py` module already exists with comprehensive monitoring. Add a summary to the daily email.

**Changes to `run_daily.py`:**

```python
# Before sending the email report:
from source_completeness import build_source_completeness_report
completeness = build_source_completeness_report(conn, school_subdomain)
# Include completeness summary in email body
```

**Files modified:** `run_daily.py` (add ~5 lines)  
**Complexity:** Low — module already exists

#### 1.3 Run extractors in parallel subprocesses

The `run_date_window_lead_load.py` orchestrator already exists. Configure it to run daily alongside the notes pipeline.

**Create a cron wrapper that chains both pipelines:**

```python
# scripts/run_daily_full.py
#!/usr/bin/env python3
"""Unified daily pipeline: notes + leads."""
import subprocess, sys

# Step 1: Pike13 lesson notes (existing pipeline)
subprocess.run([sys.executable, "run_daily.py", "--school", "westu-sor", ...])

# Step 2: Lead extraction (existing orchestrator)
subprocess.run([
    sys.executable, "scripts/run_date_window_lead_load.py",
    "--db", "reminders.db",
    "--allow-production-db",
    "--headless",
])
```

**New files:** `scripts/run_daily_full.py`  
**Complexity:** Low — both components already exist

---

### Phase 2: Auto-Auth Flows (Weeks 2-3)

#### 2.1 HubSpot auto-auth

**Discovery step (manual):**
1. Log in to `app.hubspot.com` manually in a fresh browser
2. Observe the MFA flow: is it email code? Authenticator app? SMS?
3. Record the page URL patterns, input selectors, and button text

**Implementation (assuming email-based MFA — most common):**

Create `scripts/auto_auth_hubspot.py` following the Pike13 pattern:

```python
# scripts/auto_auth_hubspot.py
"""
HubSpot Auto-Auth: Login + MFA via email code.
Pike13 auto-auth pattern adapted for HubSpot.
"""

async def authenticate_hubspot(
    playwright,
    headless: bool = True,
    profile_dir: str = "browser_profiles/hubspot",
):
    # Step 0: Snapshot inbox for new email detection
    from pike13_auto_auth import snapshot_inbox, read_mfa_code_via_imap
    
    # HubSpot login URL
    login_url = "https://app.hubspot.com/login"
    
    # Step 1: Navigate to login
    # Step 2: Fill email + password
    # Step 3: Detect MFA page at /two-factor or /verify
    # Step 4: Poll inbox for HubSpot verification code
    # Step 5: Enter code and verify
    # Step 6: Return authenticated context
    
    # ... (mirrors pike13_auto_auth.py structure)
```

**Integrate into HubSpot extractor:**

```python
# scripts/extract_hubspot_leads.py
# In main(), after browser launch:
if args.auto_auth:
    from scripts.auto_auth_hubspot import authenticate_hubspot
    # Run auto-auth flow
    ...

# Navigate to contacts page
page.goto(args.url)
wait_until_ready(page)

# Add login-page detection:
body_text = page.locator("body").inner_text()
if "log in" in body_text.lower() and "@" in body_text:
    if args.auto_auth:
        raise RuntimeError("Auto-auth failed — still on login page")
    else:
        raise RuntimeError("HubSpot profile expired. Re-run with --auto-auth or login manually.")
```

**New files:** `scripts/auto_auth_hubspot.py`  
**Files modified:** `scripts/extract_hubspot_leads.py`, `.sorenv`  
**Complexity:** Medium

#### 2.2 Dialpad auto-auth

**Discovery step required first:**
1. Manually log in to `dialpad.com` in a fresh browser
2. Determine the auth flow: Does it use Google SSO redirect? Direct email login? SMS code?
3. If Google SSO: the flow goes to `accounts.google.com` — this is a different beast
4. If direct email login with MFA code: the Pike13 pattern works directly

**Implementation (best case — email-based MFA):**

```python
# scripts/auto_auth_dialpad.py
async def authenticate_dialpad(
    playwright,
    headless: bool = True,
    profile_dir: str = "browser_profiles/dialpad",
):
    from pike13_auto_auth import snapshot_inbox, read_mfa_code_via_imap
    
    login_url = "https://dialpad.com/login"
    
    # Step 1: Navigate to login
    # Step 2: Fill email + password  
    # Step 3: If Google SSO redirect detected, handle that flow
    # Step 4: Poll inbox for Dialpad verification code
    # Step 5: Enter code and verify
    # Step 6: Return authenticated context
```

**Worst case (Google SSO):**
If Dialpad exclusively uses Google SSO, the auth flow passes through `accounts.google.com`. We'd need to:
1. Handle the Google login page (email + password)
2. Handle Google's MFA (which may also use email codes)
3. Handle any consent screens

This is significantly more complex but still achievable with the IMAP pattern.

**Recommended strategy for Phase 2:**
- **Try auto-auth** with the IMAP pattern
- **If it fails** (Google SSO or unknown MFA), fall back to **interactive login** with a long cookie lifetime
- **Add profile health monitoring** — check cookies daily, alert when within 24 hours of expiry
- Accept that Dialpad auth may need manual intervention once every 2-4 weeks

**New files:** `scripts/auto_auth_dialpad.py`  
**Files modified:** All four Dialpad extractors, `.sorenv`  
**Complexity:** High (due to auth uncertainty)

---

### Phase 3: Production Hardening (Weeks 3-4)

#### 3.1 Unified daily orchestrator with scheduling

Replace the two-pipeline cron approach with a single unified orchestrator:

```python
# scripts/run_daily_unified.py (~200 lines)
#!/usr/bin/env python3
"""
Unified daily pipeline for NotesReminder.
Runs all data sources, handles auth, reports completeness.
"""

import asyncio
import subprocess
import sys
import time
import sqlite3
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = "reminders.db"
S3_BUCKET = "notesreminder-db"
S3_KEY = "reminders.db"

def run_extractor(name, command, timeout=900):
    """Run an extractor subprocess with timing and error capture."""
    start = time.time()
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    elapsed = time.time() - start
    return {
        "source": name,
        "status": "success" if result.returncode == 0 else "failed",
        "returncode": result.returncode,
        "duration_seconds": round(elapsed, 1),
        "stdout_tail": result.stdout.strip()[-500:],
        "stderr_tail": result.stderr.strip()[-500:],
    }

async def main():
    args = parse_args()
    
    # Phase 0: DB download
    log("📥 Downloading database from S3...")
    download_db_from_s3(DB_PATH, S3_BUCKET, S3_KEY)
    
    # Phase 1: Pike13 notes (serial — must complete first)
    log("📝 Running Pike13 notes pipeline...")
    notes_result = run_extractor("pike13_notes", [
        sys.executable, "run_daily.py",
        "--school", args.school,
        "--start-date", start_date,
        "--end-date", end_date,
        "--no-email",  # We'll send a unified email later
        "--db-path", DB_PATH,
    ], timeout=1200)
    
    # Phase 2: Pike13 leads (serial — shares Pike13 auth context)
    log("👥 Running Pike13 leads extraction...")
    leads_result = run_extractor("pike13_leads", [
        sys.executable, "scripts/extract_pike13_leads.py",
        "--db", DB_PATH,
        "--profile-dir", "browser_profiles/pike13",
        "--base-url", f"https://{args.school}.pike13.com",
        "--school", format_school_label(args.school),
        "--first-visits-start-date", start_date,
        "--first-visits-end-date", end_date,
        "--headless", "--reauth-if-needed",
    ])
    
    # Phase 3-5: HubSpot, Dialpad, Gmail (parallel)
    log("🔄 Running lead intelligence sources in parallel...")
    sources = []
    
    if not args.skip_hubspot:
        sources.append(("hubspot", [
            sys.executable, "scripts/extract_hubspot_leads.py",
            "--db", DB_PATH,
            "--profile-dir", "browser_profiles/hubspot",
            "--limit", "100", "--detail-limit", "50",
            "--start-date", start_date,
            "--headless",
        ]))
    
    if not args.skip_dialpad:
        sources.append(("dialpad", [
            sys.executable, "scripts/run_date_window_lead_load.py",
            "--db", DB_PATH,
            "--allow-production-db",
            "--headless",
            "--skip-hubspot", "--skip-pike13", "--skip-email",
        ]))
    
    if not args.skip_gmail:
        sources.append(("gmail", [
            sys.executable, "scripts/extract_school_emails.py",
            "--db", DB_PATH,
            "--profile-dir", "browser_profiles/sor_okta",
            "--start-date", start_date,
            "--end-date", end_date,
            "--headless",
        ]))
    
    # Run in parallel with thread pool
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(run_extractor, name, cmd): name 
                   for name, cmd in sources}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
    
    # Phase 6: Identity matching
    log("🔗 Running identity matching...")
    conn = sqlite3.connect(DB_PATH)
    ensure_lead_followup_schema(conn)
    from source_completeness import refresh_identity_matches
    matches = refresh_identity_matches(conn)
    conn.commit()
    conn.close()
    log(f"  {matches} identity matches created")
    
    # Phase 7: Reporting sync
    log("📊 Syncing reporting tables...")
    sync_reporting_tables(DB_PATH)
    
    # Phase 8: Source completeness
    log("📋 Building source completeness report...")
    # ... call source_completeness module
    
    # Phase 9: Unified email report
    log("📧 Sending daily summary...")
    send_unified_daily_report(results, completeness)
    
    # Phase 10: DB upload
    log("📤 Uploading database to S3...")
    upload_db_to_s3(DB_PATH, S3_BUCKET, S3_KEY)
    
    log("✅ Daily pipeline complete!")
```

**New files:** `scripts/run_daily_unified.py`  
**Complexity:** Medium — orchestration is straightforward

#### 3.2 Browser profile health monitoring

Add a pre-flight check that validates all browser profiles before extraction:

```python
# scripts/check_all_profiles.py
def check_profile_health(profile_dir, expected_url, auth_check_fn):
    """Open profile, navigate to expected URL, check for auth markers."""
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            profile_dir, headless=True, viewport={"width": 1440, "height": 900}
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(expected_url, wait_until="domcontentloaded", timeout=30000)
        is_auth = auth_check_fn(page)
        context.close()
        return is_auth
```

#### 3.3 Alerting for stale data

Add cron-based health checks:

```
# crontab addition:
0 6 * * * cd ~/projects/hughrscott/NotesReminder && python scripts/health_check.py --notify hughrscott@mac.com
```

The health check script:
- Verifies DB was updated today
- Checks source_import_runs for each source
- If a source is stale (>24 hours), sends alert email
- If a browser profile is expired, sends alert with instructions

---

## 4. Summary of All Changes

### New Files to Create (Phase 1-3)

| File | Phase | Purpose |
|------|-------|---------|
| `scripts/run_daily_full.py` | 1 | Cron wrapper chaining notes + leads |
| `scripts/run_daily_unified.py` | 3 | Full unified orchestrator |
| `scripts/auto_auth_hubspot.py` | 2 | HubSpot login + MFA automation |
| `scripts/auto_auth_dialpad.py` | 2 | Dialpad login + MFA automation |
| `scripts/gmail_session_monitor.py` | 1 | Cookie freshness monitoring |
| `scripts/health_check.py` | 3 | Daily health check + alerting |
| `scripts/check_all_profiles.py` | 3 | Pre-flight profile validation |
| `AUTOMATION_PROPOSAL.md` | — | This document |

### Files to Modify

| File | Phase | Changes |
|------|-------|---------|
| `run_daily.py` | 1 | Add Pike13 leads + completeness report |
| `scripts/extract_hubspot_leads.py` | 2 | Add auto-auth support |
| `scripts/extract_dialpad_voice.py` | 2 | Add auto-auth support |
| `scripts/extract_dialpad_sms.py` | 2 | Add auto-auth support |
| `scripts/extract_dialpad_daily_intake.py` | 2 | Add auto-auth support |
| `scripts/extract_dialpad_call_reviews.py` | 2 | Add auto-auth support |
| `scripts/extract_school_emails.py` | 2 | Add cookie health pre-check |
| `scripts/run_date_window_lead_load.py` | 2 | Add `--auto-auth` flag support |
| `/home/ubuntu/.hermes/SOR/.sorenv` | 2 | Add HubSpot + Dialpad credentials |

### Database Changes

No schema changes needed — all target tables already exist in `lead_followup_schema.py`:
- `hubspot_deals`, `hubspot_contacts`, `hubspot_tasks`, `hubspot_activities`
- `dialpad_voice_events`, `dialpad_call_reviews`, `dialpad_sms_threads`, `dialpad_sms_messages`
- `school_email_messages`
- `pike13_people`, `pike13_first_visits`
- `source_import_runs`, `identity_matches`

---

## 5. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| HubSpot UI changes break selectors | Medium | High | Fall back to interactive login; raw_capture saves page text for debugging |
| Dialpad uses Google SSO (harder MFA) | Medium | High | Accept manual intervention every 2-4 weeks; add cookie monitoring |
| Okta Verify push can't be automated | High | Medium | Rely on cookie persistence; add alerting when stale |
| Playwright memory leaks over long runs | Low | Medium | Each extractor runs in its own subprocess with timeout |
| S3 upload conflicts with concurrent runs | Low | High | Add advisory lock; only one unified pipeline runs at a time |
| Email inbox fills with MFA verification codes | Medium | Low | IMAP reader only looks at NEW emails (snapshot-based filtering) |
| Credentials leak via debug output | Low | High | Sanitize stderr/stdout before logging; never print passwords |

---

## 6. Recommended First Steps

1. **This week:** Add Pike13 leads integration to `run_daily.py` (Phase 1.1) — biggest ROI for least effort
2. **Next week:** Run HubSpot and Dialpad extractors manually to map their MFA flows — this determines Phase 2 complexity
3. **Week 3:** Implement auto-auth for HubSpot (simpler flow, higher confidence)
4. **Week 4:** Implement auto-auth for Dialpad (or accept interactive login with monitoring)
5. **Week 5:** Deploy unified orchestrator, configure cron, enable alerting

The Pike13 auto-MFA breakthrough is the foundation — the same IMAP-based email code pattern works wherever the target service sends verification emails to `huscott@schoolofrock.com`. HubSpot very likely uses this pattern. Dialpad and Gmail/Okta may need different strategies, but the architecture supports graceful degradation (skip a source if auth fails, alert the operator).
