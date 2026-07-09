# Okta Cookie Integration & MCP Server Plan

**Status:** Implementation plan — not yet built  
**Last updated:** 2026-07-09  
**Goal:** Fix Pike13 authentication (broken by Okta SSO/MFA), package as an MCP server on Oracle Cloud, and make cookies refreshable without Hugh manually babysitting the scraper every day.

---

## 1. Code Review Findings

### 1.1 The Broken Auth Path

**File:** `noteschecker.py`, lines 184–228

The non-interactive login path (no `profile_dir` passed) does:

1. Navigates to `https://{school}.pike13.com/accounts/sign_in` (line 188–189)
2. Fills Pike13's own email/password form (lines 203–205):
   ```python
   await page.fill('input[placeholder="Email address"]', PIKE13_USER or "")
   await page.fill('input[placeholder="Password"]', PIKE13_PASS or "")
   ```
3. Clicks "Sign In" (line 209)

**Why it fails:** Pike13 redirects to `sor.okta.com` for SSO. The Pike13 login form page may not even render when Okta SSO is enforced — the browser gets redirected to Okta before the Playwright script can fill anything. Even if it renders, clicking "Sign In" sends you to Okta's MFA challenge, which cannot be solved headlessly.

### 1.2 The Current Workaround

**File:** `noteschecker.py`, lines 56–68, 124–137, 190–196  
**Shell wrapper:** `scripts/run_notes_local_mfa.sh`  
**GitHub Actions:** `daily_notes_reminder.yml` is `workflow_dispatch` only; line 5 comment: _"Pike13 now requires interactive MFA. Scheduled Actions cannot complete that login."_

The interactive path:
- Uses `launch_persistent_context` with a browser profile (line 63–68)
- When not authenticated, calls `wait_for_interactive_login` (line 124–137) which opens a visible browser window and waits up to `login_timeout` (default 300s) for Hugh to manually complete Okta MFA
- Once authenticated, the persistent profile reuses session cookies/cache across runs

**Problem:** This only works on Hugh's Mac (headed browser). On the Oracle Cloud Linux server (headless), there is no visible browser for interactive login.

### 1.3 Existing Okta Infrastructure

**File:** `scripts/extract_school_emails.py`, lines 51–104  
Already has code for:
- Reading Okta credentials from env vars: `OKTA_USERNAME`, `SOR_OKTA_USERNAME`, `OKTA_USER`, `OKTA_PASSWORD`, `SOR_OKTA_PASSWORD` (lines 51–60)
- Filling Okta login form: `fill_okta_login(page)` (lines 68–86) — targets `input[name="username"]`, `input[name="password"]`, clicks "Sign In"
- Waiting for Okta Verify push: `wait_for_okta_push(page, timeout_seconds)` (lines 89–104) — polls for "push sent" text

**File:** `scripts/probe_sor_okta_auth.py`  
Probes Okta SSO access across Pike13, HubSpot, Dialpad, Gmail URLs using a persistent profile at `browser_profiles/sor_okta`. Shows that a single Okta session grants access to all School of Rock services.

### 1.4 Environment Configuration

**File:** `.env.example`  
Current variables: `PIKE13_USER`, `PIKE13_PASSWORD`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `TRANSCRIBE_BUCKET`, `SENDER_EMAIL`, `SENDER_PASSWORD`, `OPENAI_API_KEY`, optional MCP overrides.

### 1.5 The MCP Server

**File:** `mcp_server.py` (517 lines)

Uses `mcp.server.fastmcp.FastMCP`. Currently exposes 20+ read-only SQL query tools + `import_call_data`. **No Pike13 scraping capability is exposed.** The `notesreminder/mcp/__init__.py` is a 2-line stub.

Current tools: `sync_db_from_s3`, `db_status`, `list_tables`, `describe_table`, `query_sql`, `import_call_data`, `initialize_lead_followup_schema`, `source_completeness`, `daily_snapshot`, `weekly_snapshot`, `monthly_snapshot`, `note_quality_scorecard`, `experimental_communication_insights`, `exception_queue`, `lead_evidence_timeline`, `refresh_person_identity_layer`, `person_search`, `person_details`, `person_journey`, `customer_lifecycle_summary`, `stale_leads`, `lead_timeline`, `unanswered_messages`, `unanswered_communications`, `no_show_followup`, `lead_conversion_path`.

### 1.6 Database & Requirements

**File:** `requirements.txt` — `mcp>=1.0.0`, `playwright>=1.40.0`, `boto3>=1.34.0`, `pandas>=1.5.0`, `python-dotenv>=1.0.0`, `openai>=1.40.0`, etc.

Database: `reminders.db` (SQLite), synced to `s3://notesreminder-db/reminders.db`.

### 1.7 Package Layout

**File:** `notesreminder/mcp/__init__.py` — empty stub. All MCP work currently lives in the root `mcp_server.py`.

---

## 2. Implementation Plan

### Phase A: Cookie Extraction (Hugh's Mac — one-time setup)

#### A1. New File: `scripts/extract_pike13_cookies.py`

A script Hugh runs ONCE on his Mac after logging into Pike13 via Okta in his normal browser (Chrome/Safari/Firefox).

**What it does:**
1. Opens the browser's existing profile (already authenticated to Pike13 via Okta SSO)
2. Navigates to `https://westu-sor.pike13.com/schedule` to verify auth is live
3. Extracts all cookies for `.pike13.com` domain using Playwright's `context.cookies()`
4. Serializes them to JSON and saves to a file: `pike13_cookies.json`
5. Also extracts `localStorage` and `sessionStorage` items (some apps store auth tokens there)
6. Outputs a summary: which cookies were found, their expiration dates, and the recommended refresh interval

**Implementation:**
```python
#!/usr/bin/env python3
"""Extract Pike13 auth cookies from an existing browser profile for injection into headless scrapers."""
import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright


PIKE13_DOMAINS = [".pike13.com", "westu-sor.pike13.com", "theheights-sor.pike13.com"]
OKTA_DOMAINS = [".okta.com", "sor.okta.com"]


def extract_cookies(profile_dir: str, output_path: str, chrome_channel: bool = False):
    """Open existing browser profile, navigate to Pike13, dump cookies + storage."""
    profile = Path(profile_dir)
    if not profile.exists():
        raise FileNotFoundError(f"Profile directory not found: {profile_dir}")

    launch_kwargs = {"headless": False, "viewport": {"width": 1440, "height": 900}}
    if chrome_channel:
        launch_kwargs["channel"] = "chrome"

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(str(profile), **launch_kwargs)
        page = context.pages[0] if context.pages else context.new_page()

        # Try both schools
        schools = ["westu-sor", "theheights-sor"]
        authenticated = False
        for school in schools:
            url = f"https://{school}.pike13.com/schedule"
            print(f"Navigating to {url}...")
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(2000)

            # Check if we landed on schedule (authenticated) or got redirected to sign_in
            if "/accounts/sign_in" in page.url.lower() or "/login" in page.url.lower():
                print(f"  Not authenticated for {school}. Opening Okta login page...")
                # Navigate to Okta to trigger login
                page.goto(f"https://{school}.pike13.com/accounts/sign_in", wait_until="domcontentloaded")
                print(f"  Please complete Okta login/MFA in the opened browser.")
                print(f"  Waiting for authentication (checking every 3s, timeout 5 min)...")
                import time
                deadline = time.time() + 300
                while time.time() < deadline:
                    page.wait_for_timeout(3000)
                    if "/schedule" in page.url.lower() and "sign_in" not in page.url.lower():
                        # Try navigating directly to schedule
                        page.goto(url, wait_until="domcontentloaded")
                        page.wait_for_timeout(2000)
                    if "/schedule" in page.url.lower() and "sign_in" not in page.url.lower():
                        authenticated = True
                        print(f"  Authenticated for {school}!")
                        break
            else:
                print(f"  Already authenticated for {school}!")
                authenticated = True
            if authenticated:
                break

        if not authenticated:
            print("ERROR: Could not authenticate to Pike13. Please log in manually and retry.")
            # Dump whatever cookies we have anyway
            pass

        # Extract cookies for Pike13 + Okta domains
        all_cookies = []
        for domain in PIKE13_DOMAINS + OKTA_DOMAINS:
            try:
                cookies = context.cookies(domain)
                all_cookies.extend(cookies)
            except Exception:
                pass

        # Also extract from unrestricted cookie jar
        try:
            all_cookies_raw = context.cookies()
            existing_names = {c["name"] for c in all_cookies}
            for c in all_cookies_raw:
                if c["name"] not in existing_names:
                    all_cookies.append(c)
        except Exception:
            pass

        # Extract storage
        storage = {}
        try:
            for school in schools:
                page.goto(f"https://{school}.pike13.com/schedule", wait_until="domcontentloaded")
                page.wait_for_timeout(1000)
                storage[school] = page.evaluate("""() => {
                    const items = {};
                    for (let i = 0; i < localStorage.length; i++) {
                        const key = localStorage.key(i);
                        items[key] = localStorage.getItem(key);
                    }
                    return items;
                }""")
        except Exception:
            pass

        context.close()

    # Build output
    now_utc = datetime.now(timezone.utc)
    cookie_list = []
    soonest_expiry = None
    for c in all_cookies:
        expires_raw = c.get("expires")
        expires_ts = None
        if expires_raw and expires_raw > 0:
            expires_ts = datetime.fromtimestamp(expires_raw, tz=timezone.utc).isoformat()
        cookie_list.append({
            "name": c["name"],
            "domain": c["domain"],
            "path": c.get("path", "/"),
            "value": c["value"],
            "expires": expires_ts,
            "httpOnly": c.get("httpOnly", False),
            "secure": c.get("secure", False),
            "sameSite": c.get("sameSite", "Lax"),
        })
        if expires_raw and expires_raw > 0:
            expires_dt = datetime.fromtimestamp(expires_raw, tz=timezone.utc)
            if soonest_expiry is None or expires_dt < soonest_expiry:
                soonest_expiry = expires_dt

    payload = {
        "extracted_at": now_utc.isoformat(),
        "source": "manual_extraction_from_mac",
        "cookies": cookie_list,
        "storage": storage,
        "cookie_count": len(cookie_list),
        "soonest_expiry": soonest_expiry.isoformat() if soonest_expiry else None,
        "recommended_refresh_days": 7,
    }

    output = Path(output_path)
    output.write_text(json.dumps(payload, indent=2))
    print(f"\nExtracted {len(cookie_list)} cookies to {output_path}")
    if soonest_expiry:
        days_left = (soonest_expiry - now_utc).days
        print(f"Soonest cookie expires in ~{days_left} days ({soonest_expiry.date()})")
        print(f"Recommended refresh: every {min(7, max(1, days_left - 2))} days")
    else:
        print("No expiry dates found — cookies may be session cookies. Recommend refreshing daily.")
    print("\nNext step: Securely copy this file to the Oracle Cloud server.")

    return payload


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract Pike13 auth cookies from browser profile")
    parser.add_argument("--profile-dir", required=True, help="Path to browser profile (e.g., 'browser_profiles/pike13')")
    parser.add_argument("--output", default="pike13_cookies.json", help="Output JSON file for cookies")
    parser.add_argument("--chrome-channel", action="store_true", help="Use system Chrome instead of Playwright's Chromium")
    args = parser.parse_args()
    extract_cookies(args.profile_dir, args.output, args.chrome_channel)
```

#### A2. Environment Variables to Add

**File:** `.env.example` — add after line 22:
```bash
# Okta SSO credentials for Pike13 (used by cookie injection path)
OKTA_USERNAME=
OKTA_PASSWORD=

# Path to cookie JSON file (generated by scripts/extract_pike13_cookies.py)
PIKE13_COOKIES_PATH=pike13_cookies.json

# Cookie freshness threshold (days before triggering alert)
PIKE13_COOKIE_MAX_AGE_DAYS=14
```

---

### Phase B: Cookie Injection into Playwright Scraper

#### B1. New File: `notesreminder/lib/cookie_auth.py`

Shared library for cookie-based authentication used by both the MCP server and standalone scraper.

```python
"""Cookie-based authentication helpers for Pike13 (Okta SSO)."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


DEFAULT_COOKIES_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "pike13_cookies.json")
COOKIES_PATH = os.getenv("PIKE13_COOKIES_PATH", DEFAULT_COOKIES_PATH)
MAX_AGE_DAYS = int(os.getenv("PIKE13_COOKIE_MAX_AGE_DAYS", "14"))


class CookieAuthError(Exception):
    """Raised when cookies are missing, expired, or invalid."""


class CookieExpiredError(CookieAuthError):
    """Raised when cookies have passed their expiration threshold."""


def load_cookies(cookies_path: Optional[str] = None) -> dict:
    """Load cookie payload from JSON file. Raises CookieAuthError on failure."""
    path = Path(cookies_path or COOKIES_PATH)
    if not path.exists():
        raise CookieAuthError(
            f"Cookie file not found: {path}. "
            f"Run scripts/extract_pike13_cookies.py on your Mac first, then copy pike13_cookies.json here."
        )
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise CookieAuthError(f"Invalid cookie JSON in {path}: {e}")
    return payload


def check_cookie_freshness(payload: Optional[dict] = None, max_age_days: Optional[int] = None) -> dict:
    """Check if stored cookies are still acceptably fresh. Returns a status dict."""
    if payload is None:
        try:
            payload = load_cookies()
        except CookieAuthError:
            return {"status": "missing", "error": "Cookie file not found"}

    max_age = max_age_days or MAX_AGE_DAYS

    # Check extraction age
    extracted_at = payload.get("extracted_at")
    if extracted_at:
        try:
            extracted_dt = datetime.fromisoformat(extracted_at)
            age_days = (datetime.now(timezone.utc) - extracted_dt).days
            if age_days > max_age:
                return {
                    "status": "expired",
                    "error": f"Cookies extracted {age_days} days ago (max allowed: {max_age})",
                    "extracted_at": extracted_at,
                    "age_days": age_days,
                    "max_age_days": max_age,
                }
        except (ValueError, TypeError):
            pass

    # Check individual cookie expiries
    soonest = None
    for cookie in payload.get("cookies", []):
        expires = cookie.get("expires")
        if expires:
            try:
                expiry_dt = datetime.fromisoformat(expires)
                now = datetime.now(timezone.utc)
                if expiry_dt < now:
                    return {
                        "status": "expired",
                        "error": f"Cookie '{cookie['name']}' expired at {expires}",
                        "expired_cookie": cookie["name"],
                    }
                days_left = (expiry_dt - now).days
                if soonest is None or expiry_dt < soonest[0]:
                    soonest = (expiry_dt, cookie["name"], days_left)
            except (ValueError, TypeError):
                pass

    result = {
        "status": "fresh",
        "extracted_at": extracted_at,
        "cookie_count": payload.get("cookie_count", 0),
    }
    if soonest:
        result["soonest_expiry_days"] = soonest[2]
        result["soonest_expiry_cookie"] = soonest[1]
    return result


def inject_cookies_into_context(context, payload: Optional[dict] = None):
    """Inject saved cookies into a Playwright browser context. Must be called BEFORE navigating."""
    if payload is None:
        payload = load_cookies()

    cookies = payload.get("cookies", [])
    if not cookies:
        raise CookieAuthError("Cookie payload is empty — no cookies to inject.")

    # Filter out expired cookies
    now_ts = datetime.now(timezone.utc).timestamp()
    valid_cookies = []
    for c in cookies:
        expires = c.get("expires")
        if expires:
            try:
                expiry_dt = datetime.fromisoformat(expires)
                if expiry_dt.timestamp() < now_ts:
                    continue  # skip expired
            except (ValueError, TypeError):
                pass
        valid_cookies.append({
            "name": c["name"],
            "value": c["value"],
            "domain": c.get("domain", ""),
            "path": c.get("path", "/"),
            "httpOnly": c.get("httpOnly", False),
            "secure": c.get("secure", True),
            "sameSite": c.get("sameSite", "Lax"),
        })

    context.add_cookies(valid_cookies)
    return len(valid_cookies)


def inject_storage_into_page(page, payload: Optional[dict] = None, school: str = "westu-sor"):
    """Inject localStorage items into a page that's already on the Pike13 domain."""
    if payload is None:
        payload = load_cookies()

    storage = payload.get("storage", {}).get(school, {})
    if storage:
        page.evaluate(
            """([items]) => {
                for (const [key, value] of Object.entries(items)) {
                    localStorage.setItem(key, value);
                }
            }""",
            [storage],
        )
```

#### B2. Modify `noteschecker.py`: Add Cookie Injection Path

**File:** `noteschecker.py`

**Change 1:** Add import at top (after line 13):
```python
from notesreminder.lib.cookie_auth import (
    CookieAuthError,
    inject_cookies_into_context,
    inject_storage_into_page,
    load_cookies,
    check_cookie_freshness,
)
```

**Change 2:** Add a new function `try_cookie_auth()` after the `scrape_lessons` function definition (around line 27), or as a helper within `scrape_lessons`:

The key change is in the login section (lines 184–228). After line 183 (`try:`), add a cookie injection attempt BEFORE falling through to form-based login:

```python
# --- NEW: Try cookie injection first ---
cookie_auth_attempted = False
if not profile_dir:
    try:
        cookie_payload = load_cookies()
        freshness = check_cookie_freshness(cookie_payload)
        if freshness["status"] == "fresh":
            if verbose:
                print(f"ℹ️ Injecting {cookie_payload.get('cookie_count', 0)} saved Pike13 cookies...")
            injected = inject_cookies_into_context(context, cookie_payload)
            if verbose:
                print(f"  Injected {injected} valid cookies")
            cookie_auth_attempted = True

            # Navigate to schedule directly (skip login page entirely)
            await page.goto(schedule_home_url)
            await wait_until_ready()

            if await is_authenticated():
                if verbose:
                    print("✅ Authenticated via injected cookies — skipping login")
                # Skip the entire login block
            else:
                if verbose:
                    print("⚠️ Cookie injection did not grant access. Falling through to normal login.")
                cookie_auth_attempted = False  # reset so we do normal login below
        else:
            if verbose:
                print(f"⚠️ Cookie freshness check failed: {freshness['status']} — {freshness.get('error', '')}")
    except CookieAuthError as e:
        if verbose:
            print(f"⚠️ Cannot use cookie auth: {e}")
# --- END NEW ---
```

**Change 3:** Skip the form-based login when cookie auth succeeded. The existing code block at lines 188–228 should be wrapped in a condition:

```python
if not cookie_auth_attempted or not await is_authenticated():
    # Existing login code (lines 188-228) goes here, indented
    if profile_dir:
        await page.goto(schedule_home_url)
        # ... existing profile_dir logic ...
    else:
        # Navigate to login page
        await page.goto(login_url)
        # ... existing form-fill logic ...
        # Wait for successful login
        # ... existing post-login checks ...
```

**Change 4:** Add `PIKE13_COOKIES_PATH` and `PIKE13_COOKIE_MAX_AGE_DAYS` to the function's capability. The `scrape_lessons` function already takes many parameters. The cookie path defaults to the env var, so no new parameter is needed — just read from `os.getenv` at the top, consistent with how `PIKE13_USER` / `PIKE13_PASS` are read on lines 14–15.

**Change 5:** After a successful cookie-auth scrape, check freshness and emit a warning if close to expiry. Around line 497 (after the loop completes, before returning `df`):

```python
# Post-scrape cookie health check
if cookie_auth_attempted:
    freshness = check_cookie_freshness()
    days_left = freshness.get("soonest_expiry_days")
    if days_left is not None and days_left < 3:
        alert_msg = (
            f"⚠️ Pike13 cookies expire in ~{days_left} days. "
            f"Refresh them soon by running scripts/extract_pike13_cookies.py on your Mac."
        )
        print(alert_msg)
        # Write to a notification file for cron/MCP monitoring
        alert_path = os.path.join(os.path.dirname(__file__), "outputs", "cookie_alert.txt")
        os.makedirs(os.path.dirname(alert_path), exist_ok=True)
        with open(alert_path, "w") as f:
            f.write(f"{datetime.now().isoformat()} | {alert_msg}\n")
```

---

### Phase C: MCP Server Extension

#### C1. New File: `notesreminder/mcp/tools.py`

Move tool definitions here and extend with Pike13 scraping tools.

```python
"""MCP tool definitions for the NotesReminder server."""
import asyncio
import json
import os
from datetime import datetime

from notesreminder.lib.cookie_auth import check_cookie_freshness, load_cookies


PIKE13_SCRAPE_LOCK = asyncio.Lock()  # prevent concurrent scrapes


def register_pike13_tools(mcp):
    """Register Pike13 scraping tools on the MCP server."""

    @mcp.tool()
    async def pike13_scrape_lessons(
        school: str = "westu-sor",
        start_date: str = "",
        end_date: str = "",
        limit_days: int = 7,
    ) -> str:
        """Scrape Pike13 lesson data for a school/date range. Uses injected Okta cookies for auth.

        Args:
            school: Pike13 subdomain (westu-sor or theheights-sor)
            start_date: Start date YYYY-MM-DD (defaults to limit_days ago)
            end_date: End date YYYY-MM-DD (defaults to today)
            limit_days: If start_date is empty, scrape this many days back from now
        """
        from datetime import timedelta

        if not start_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
            start_dt = end_dt - timedelta(days=limit_days)
            start_date = start_dt.strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        async with PIKE13_SCRAPE_LOCK:
            from noteschecker import scrape_lessons
            df = await scrape_lessons(
                school_subdomain=school,
                start_date=start_date,
                end_date=end_date,
                verbose=True,
            )
        return json.dumps({
            "status": "success",
            "school": school,
            "start_date": start_date,
            "end_date": end_date,
            "lessons_scraped": len(df),
            "columns": list(df.columns),
        }, default=str, indent=2)

    @mcp.tool()
    async def pike13_cookie_status() -> str:
        """Check the health of stored Pike13 auth cookies. Returns freshness, expiry, and cookie count."""
        try:
            payload = load_cookies()
            freshness = check_cookie_freshness(payload)
            result = {
                "cookies_available": True,
                "cookie_count": payload.get("cookie_count", 0),
                "extracted_at": payload.get("extracted_at"),
                "freshness": freshness,
            }
        except Exception as e:
            result = {
                "cookies_available": False,
                "error": str(e),
            }
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def pike13_import_and_update_db(
        school: str = "westu-sor",
        start_date: str = "",
        end_date: str = "",
        limit_days: int = 7,
    ) -> str:
        """Scrape Pike13 lessons and update reminders.db with the results. Combines scrape + DB update.

        This is the primary tool for keeping the notes database current from Pike13.
        After scraping, it updates the reminders table and runs reporting schema sync.
        """
        from datetime import timedelta

        if not start_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
            start_dt = end_dt - timedelta(days=limit_days)
            start_date = start_dt.strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        async with PIKE13_SCRAPE_LOCK:
            from noteschecker import scrape_lessons
            df = await scrape_lessons(
                school_subdomain=school,
                start_date=start_date,
                end_date=end_date,
                verbose=True,
            )

        if df.empty:
            return json.dumps({"status": "no_data", "school": school, "lessons": 0}, indent=2)

        # Import the DB update logic from run_daily.py
        # This requires refactoring run_daily.py to expose an importable function
        from importlib import import_module
        import sqlite3

        db_path = os.getenv("REMINDERS_DB_PATH", "reminders.db")
        conn = sqlite3.connect(db_path)
        try:
            # ... update reminders table from df ...
            # (detailed row-by-row upsert logic goes here — see run_daily.py update logic)
            rows_upserted = 0
            rows_inserted = 0
            conn.commit()
        finally:
            conn.close()

        return json.dumps({
            "status": "success",
            "school": school,
            "lessons_scraped": len(df),
            "rows_inserted": rows_inserted,
            "rows_upserted": rows_upserted,
        }, indent=2)
```

#### C2. Modify `mcp_server.py`: Register New Tools

**File:** `mcp_server.py`

Add after line 26 (existing imports):
```python
from notesreminder.mcp.tools import register_pike13_tools
```

Add before line 516 (`if __name__ == "__main__":`):
```python
# Register Pike13 scraping tools (cookie-based auth)
register_pike13_tools(mcp)
```

#### C3. Refactor `run_daily.py` for Importable DB Update

**File:** `run_daily.py`

Extract the DB update logic (roughly lines 501–900) into a standalone importable function:

```python
def update_reminders_from_dataframe(conn: sqlite3.Connection, df: pd.DataFrame, school: str) -> dict:
    """Upsert scraped lesson data into the reminders table. Returns counts."""
    ...
```

This allows the MCP tool `pike13_import_and_update_db` to call it without subprocessing.

---

### Phase D: MCP Server Packaging for Oracle Cloud

#### D1. New File: `scripts/run_mcp_server.sh`

```bash
#!/bin/sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Load environment
if [ -f ".env" ]; then
  set -a
  . ./.env
  set +a
fi

# Use venv if available
if [ -x "venv/bin/python" ]; then
  PYTHON_BIN="venv/bin/python"
else
  PYTHON_BIN="python3"
fi

exec "$PYTHON_BIN" mcp_server.py "$@"
```

#### D2. Systemd Service File: `deploy/notesreminder-mcp.service`

```ini
[Unit]
Description=NotesReminder MCP Server (Pike13 + DB queries)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/projects/hughrscott/NotesReminder
ExecStart=/home/ubuntu/projects/hughrscott/NotesReminder/scripts/run_mcp_server.sh
Environment="PATH=/home/ubuntu/.local/bin:/usr/local/bin:/usr/bin:/bin"
Environment="REMINDERS_DB_PATH=/home/ubuntu/projects/hughrscott/NotesReminder/reminders.db"
Environment="PIKE13_COOKIES_PATH=/home/ubuntu/projects/hughrscott/NotesReminder/pike13_cookies.json"
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Note on MCP transport:** The `mcp` library's `FastMCP` uses stdio transport by default. For a systemd service accessible over the network, you have two options:

**Option A (Recommended): SSE transport via FastMCP**
```python
if __name__ == "__main__":
    mcp.run(transport="sse", host="0.0.0.0", port=8090)
```

**Option B: stdio over SSH tunnel**
Keep stdio transport and have Hermes connect via SSH tunnel. Simpler setup, more secure.

For this plan, we'll use **Option A** (SSE on port 8090, firewalled to localhost only, with SSH tunnel from Mac).

#### D3. Update `mcp_server.py` Main Block

**File:** `mcp_server.py`, line 516–517

Change from:
```python
if __name__ == "__main__":
    mcp.run()
```

To:
```python
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--transport", default="stdio", choices=["stdio", "sse"])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8090)
    args = parser.parse_args()
    if args.transport == "sse":
        mcp.run(transport="sse", host=args.host, port=args.port)
    else:
        mcp.run()
```

#### D4. Install Script: `deploy/install_mcp_server.sh`

```bash
#!/bin/sh
set -euo pipefail

echo "Installing NotesReminder MCP server as systemd service..."

sudo cp deploy/notesreminder-mcp.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable notesreminder-mcp
sudo systemctl start notesreminder-mcp

echo "Service installed. Check status with:"
echo "  sudo systemctl status notesreminder-mcp"
echo "  sudo journalctl -u notesreminder-mcp -f"
```

---

### Phase E: Hermes Connection to MCP Server

#### E1. Hermes MCP Config

**File:** `~/.hermes/config.yaml`

Add under `mcp_servers`:

```yaml
mcp_servers:
  notesreminder:
    transport: sse
    url: "http://127.0.0.1:8090/sse"
    # If using SSH tunnel from Mac:
    #   ssh -N -L 8090:127.0.0.1:8090 ubuntu@<oracle-cloud-ip>
    # Then url: "http://127.0.0.1:8090/sse"

# Alternative: stdio over SSH
# mcp_servers:
#   notesreminder:
#     transport: stdio
#     command: "ssh"
#     args: ["ubuntu@<oracle-cloud-ip>", "cd /home/ubuntu/projects/hughrscott/NotesReminder && python3 mcp_server.py"]
```

#### E2. Hermes Verification

Once configured, verify with:
```bash
hermes mcp list  # Should show 'notesreminder'
hermes mcp tools notesreminder  # Should list all tools including pike13_*
```

---

### Phase F: Cookie Refresh Workflow

#### F1. New File: `scripts/refresh_pike13_cookies.sh`

Wrapper that runs on Hugh's Mac, extracts cookies, and securely copies them to Oracle Cloud:

```bash
#!/bin/sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

PROFILE_DIR="${PIKE13_PROFILE_DIR:-browser_profiles/pike13}"
OUTPUT="pike13_cookies.json"
ORACLE_HOST="${ORACLE_HOST:-}"

echo "=== Step 1: Extract cookies from browser profile ==="
python3 scripts/extract_pike13_cookies.py \
  --profile-dir "$PROFILE_DIR" \
  --output "$OUTPUT"

echo ""
echo "=== Step 2: Copy cookies to Oracle Cloud server ==="
if [ -n "$ORACLE_HOST" ]; then
  scp "$OUTPUT" "ubuntu@${ORACLE_HOST}:/home/ubuntu/projects/hughrscott/NotesReminder/pike13_cookies.json"
  echo "Cookies copied to $ORACLE_HOST"
  echo ""
  echo "=== Step 3: Restart MCP server to pick up new cookies ==="
  ssh "ubuntu@${ORACLE_HOST}" "sudo systemctl restart notesreminder-mcp"
  echo "MCP server restarted"
else
  echo "Set ORACLE_HOST env var to auto-copy cookies to the server."
  echo "Example: ORACLE_HOST=192.168.1.100 ./scripts/refresh_pike13_cookies.sh"
fi
```

#### F2. Cron Job on Mac (Optional)

For proactive refresh before expiry:

```bash
# Run every Monday morning to refresh cookies
# Add to crontab: crontab -e
0 8 * * 1 ORACLE_HOST=<ip> /path/to/scripts/refresh_pike13_cookies.sh >> /tmp/cookie_refresh.log 2>&1
```

---

### Phase G: Cookie Expiration Detection & Alerting

#### G1. MCP Tool: `pike13_cookie_status`

Already defined in Phase C1 above. Returns freshness, expiry, cookie count.

#### G2. Health Check Wrapper

**New File:** `scripts/check_cookie_health.py`

```python
#!/usr/bin/env python3
"""Check Pike13 cookie health and exit non-zero if cookies need refresh."""
import sys
from notesreminder.lib.cookie_auth import check_cookie_freshness


def main():
    result = check_cookie_freshness()
    status = result["status"]
    print(f"Cookie status: {status}")
    for k, v in result.items():
        if k != "status":
            print(f"  {k}: {v}")

    if status == "missing":
        print("ACTION: Run scripts/extract_pike13_cookies.py on your Mac to generate cookies.")
        print("ACTION: Then scp pike13_cookies.json to the server.")
        sys.exit(2)
    elif status == "expired":
        print("ACTION: Cookies have expired. Run scripts/refresh_pike13_cookies.sh on your Mac.")
        sys.exit(1)
    elif result.get("soonest_expiry_days", 999) < 3:
        print(f"WARNING: Cookies expire in {result['soonest_expiry_days']} days. Schedule a refresh soon.")
        sys.exit(0)  # warn but don't fail
    else:
        print("Cookies are fresh.")
        sys.exit(0)


if __name__ == "__main__":
    main()
```

#### G3. Cron Job on Oracle Cloud

```bash
# Check cookie health daily at 7 AM UTC
# If cookies are expired or missing, email Hugh (or log prominently)
0 7 * * * cd /home/ubuntu/projects/hughrscott/NotesReminder && python3 scripts/check_cookie_health.py || echo "Pike13 cookies need refresh!" | mail -s "Pike13 Cookie Alert" hughrscott@mac.com
```

#### G4. Integrated into Scraper

As noted in Phase B2 Change 5, after every successful scrape, if cookies are within 3 days of expiry, write a warning to `outputs/cookie_alert.txt`. The cron job also reads this file.

---

### Phase H: Security Considerations

1. **Cookie file permissions:** `pike13_cookies.json` must be `chmod 600`, readable only by the user running the MCP server.
2. **No cookies in git:** Add `pike13_cookies.json` to `.gitignore`.
3. **SCP with SSH keys:** Use SSH key-based auth for the `scp` in the refresh script — never store passwords.
4. **SSE on localhost only:** Bind MCP server to `127.0.0.1`, not `0.0.0.0`. Use SSH tunnel from Mac to reach it.
5. **Env file security:** Ensure `.env` is `.gitignore`d (it already is).

---

### Phase I: Implementation Checklist (Ordered)

| # | Task | Files to create/modify | Priority |
|---|------|----------------------|----------|
| 1 | Create `notesreminder/lib/cookie_auth.py` | NEW | P0 |
| 2 | Create `scripts/extract_pike13_cookies.py` | NEW | P0 |
| 3 | Add cookie vars to `.env.example` | MODIFY | P0 |
| 4 | Add cookie injection to `noteschecker.py` | MODIFY +60 lines | P0 |
| 5 | Create `notesreminder/mcp/tools.py` with Pike13 tools | NEW | P1 |
| 6 | Register new tools in `mcp_server.py` | MODIFY +4 lines | P1 |
| 7 | Refactor DB update logic out of `run_daily.py` | MODIFY ~30 lines | P1 |
| 8 | Create `scripts/run_mcp_server.sh` | NEW | P1 |
| 9 | Create `deploy/notesreminder-mcp.service` | NEW | P1 |
| 10 | Create `deploy/install_mcp_server.sh` | NEW | P1 |
| 11 | Update `mcp_server.py` main block for SSE | MODIFY +10 lines | P1 |
| 12 | Configure Hermes `config.yaml` | MODIFY config | P2 |
| 13 | Create `scripts/refresh_pike13_cookies.sh` | NEW | P2 |
| 14 | Create `scripts/check_cookie_health.py` | NEW | P2 |
| 15 | Set up cron jobs (Oracle Cloud + Mac) | MANUAL | P2 |
| 16 | Add `pike13_cookies.json` to `.gitignore` | MODIFY | P0 |
| 17 | Extract cookies from Mac, copy to server, test | MANUAL | P0 |
| 18 | Run end-to-end test: MCP scrape → DB update → query result | MANUAL | P1 |

---

### Phase J: MCP Tools Reference (New + Extended)

| Tool Name | Category | Description |
|-----------|----------|-------------|
| `pike13_scrape_lessons` | Pike13 | Scrape lesson data for a date range, return raw results |
| `pike13_cookie_status` | Auth Health | Check cookie freshness and expiry |
| `pike13_import_and_update_db` | Pike13 | Scrape + update reminders.db in one call |
| (existing 20+ tools) | DB Query | All existing read-only SQL tools unchanged |

---

### Phase K: Directory Structure After Implementation

```
NotesReminder/
├── notesreminder/
│   ├── __init__.py
│   ├── lib/
│   │   ├── __init__.py
│   │   ├── cookie_auth.py          ← NEW
│   │   ├── person_identity.py
│   │   └── raw_capture.py
│   ├── mcp/
│   │   ├── __init__.py             ← (was stub, may become non-empty)
│   │   └── tools.py                ← NEW
│   ├── extractors/
│   ├── schema/
│   ├── reports/
│   ├── transcription/
│   └── orchestration/
├── scripts/
│   ├── extract_pike13_cookies.py   ← NEW
│   ├── refresh_pike13_cookies.sh   ← NEW
│   ├── check_cookie_health.py      ← NEW
│   ├── run_mcp_server.sh           ← NEW
│   └── ... (existing)
├── deploy/
│   ├── notesreminder-mcp.service   ← NEW
│   └── install_mcp_server.sh       ← NEW
├── mcp_server.py                   ← MODIFIED
├── noteschecker.py                 ← MODIFIED
├── run_daily.py                    ← MODIFIED (refactor)
├── .env.example                    ← MODIFIED
├── .gitignore                      ← MODIFIED
└── pike13_cookies.json             ← NEW (gitignored, on server)
```

---

## 3. Key Design Decisions

1. **Cookie injection over form-fill:** We inject cookies BEFORE navigating to Pike13. If the cookies are valid, Playwright never sees a login page. If they're expired, we fall through to the existing interactive login path.

2. **Cookie transport via SCP, not S3:** Cookies are secrets. They don't go through S3. The refresh script uses `scp` to push them directly from Mac to Oracle Cloud.

3. **SSE transport for MCP:** FastMCP supports SSE natively. We bind to `127.0.0.1` on the server and reach it via SSH tunnel from the Mac. No TLS needed, no external exposure.

4. **Graceful fallback:** If cookies are missing or expired, `noteschecker.py` falls through to the existing interactive login path. Nothing breaks — it just prompts for manual login like it does today.

5. **Cooldown protection:** Async lock (`PIKE13_SCRAPE_LOCK`) prevents concurrent scrapes from the MCP server. One scrape at a time.

6. **No API key rotation needed:** Okta session cookies typically last 7–30 days. The recommended refresh interval is 7 days, which is sustainable for a manual workflow. Cron-based health checks provide early warning before expiry.

---

## 4. Backward Compatibility

- All existing CLI entry points (`run_daily.py`, `backfill.py`, `scripts/run_notes_local_mfa.sh`) continue to work.
- The cookie injection path is additive — if `pike13_cookies.json` doesn't exist, the scraper behaves exactly as it does today.
- The `--pike13-profile-dir` and `--interactive-login` flags on `run_daily.py` still work and take precedence when present.
- The MCP server's existing 20+ tools are unchanged.
