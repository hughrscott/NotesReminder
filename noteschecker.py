__all__ = ['scrape_lessons']

import asyncio
import json
import pandas as pd
import re
from datetime import datetime, timedelta
from pathlib import Path
from playwright.async_api import async_playwright
import argparse
import asyncio
from datetime import datetime, timedelta
import os
import time

from notesreminder.lib.cookie_auth import (
    CookieAuthError,
    inject_cookies_into_context,
    inject_storage_into_page,
    load_cookies,
    check_cookie_freshness,
)

PIKE13_USER = os.environ.get("PIKE13_USER")
PIKE13_PASS = os.environ.get("PIKE13_PASSWORD")

# Auto-MFA support: import the auto-auth MFA handler
try:
    from pike13_auto_auth import read_mfa_code_via_imap, enter_mfa_code, snapshot_inbox
    _AUTO_MFA_AVAILABLE = True
except ImportError:
    _AUTO_MFA_AVAILABLE = False


async def scrape_lessons(
    school_subdomain,
    dates=None,
    start_date=None,
    end_date=None,
    verbose=False,
    profile_dir=None,
    interactive_login=False,
    login_timeout=300,
):
    if dates is None and start_date and end_date:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        delta = (end - start).days
        dates = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta + 1)]
    elif dates is None:
        raise ValueError("Provide either 'dates' or 'start_date' and 'end_date'.")

    lessons_data = []
    
    # Create screenshots directory if it doesn't exist
    os.makedirs('screenshots', exist_ok=True)

    async def goto_with_retry(target_url, attempts=3, wait_ms=2000):
        last_error = None
        for attempt in range(1, attempts + 1):
            try:
                await page.goto(target_url)
                return True
            except Exception as e:
                last_error = e
                if verbose:
                    print(f"⚠️ Page.goto failed (attempt {attempt}/{attempts}) for {target_url}: {e}")
                await page.wait_for_timeout(wait_ms)
        if verbose:
            print(f"❌ Giving up on {target_url}: {last_error}")
        return False

    async with async_playwright() as p:
        browser = None
        context_options = {
            "viewport": {'width': 1920, 'height': 1080},
            "user_agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        }
        if profile_dir:
            context = await p.chromium.launch_persistent_context(
                profile_dir,
                headless=not interactive_login,
                args=['--disable-dev-shm-usage'],
                **context_options,
            )
            # Seed from the saved storage_state JSON (valid cwr_u cookies), since
            # Playwright's persistent Default/Cookies file does not reliably persist
            # the Pike13 session on this profile. The JSON is written by
            # refresh_pike13_session.py after a successful login. Loaded post-launch
            # via add_cookies (storage_state launch-arg unsupported on this Playwright).
            storage_json = str(Path(profile_dir) / ".." / "sor_shared_storage.json")
            if Path(storage_json).exists():
                try:
                    import json as _json
                    _st = _json.load(open(storage_json))
                    _cookies = _st.get("cookies", [])
                    if _cookies:
                        await context.add_cookies(_cookies)
                        if verbose:
                            print(f"Seeded Pike13 context with {len(_cookies)} cookies from {storage_json}")
                except Exception as _e:
                    if verbose:
                        print(f"cookie seed failed: {_e}")
        else:
            # Check for cookies first — if available, we don't need PIKE13_USER/PASS
            from notesreminder.lib.cookie_auth import load_cookies, CookieAuthError
            try:
                load_cookies()
                # Cookies available — skip the env var requirement
            except CookieAuthError:
                if not PIKE13_USER or not PIKE13_PASS:
                    raise ValueError("Pike13 username or password not found in environment variables. Please set PIKE13_USER and PIKE13_PASSWORD, or provide cookies via pike13_cookies.json.")
            # Launch browser with more debugging options
            browser = await p.chromium.launch(
                headless=True,  # Keep headless for CI
                args=['--disable-dev-shm-usage']  # Helps with memory issues in CI
            )

            # Create a new context with tracing enabled
            context = await browser.new_context(**context_options)
        
        # Start tracing
        await context.tracing.start(screenshots=True, snapshots=True, sources=True)
        
        page = next((candidate for candidate in context.pages if not candidate.is_closed()), None)
        if page is None:
            page = await context.new_page()

        async def ensure_open_page():
            nonlocal page
            if page.is_closed():
                page = await context.new_page()
            return page

        async def wait_until_ready(timeout=30000):
            await ensure_open_page()
            try:
                await page.wait_for_load_state("load", timeout=timeout)
            except Exception as exc:
                if verbose:
                    print(f"⚠️ Pike13 load-state wait skipped: {exc}")
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception as exc:
                if verbose:
                    print(f"⚠️ Pike13 networkidle wait skipped: {exc}")

        async def is_authenticated():
            # Primary check: if URL contains /schedule (not sign_in/login), we're authenticated
            if "/schedule" in page.url and "/accounts/sign_in" not in page.url and "/login" not in page.url:
                return True
            if any(marker in page.url for marker in ("/accounts/sign_in", "/account/two_factor", "/login")):
                return False
            try:
                body_text = await page.locator("body").inner_text(timeout=5000)
            except Exception:
                body_text = ""
            lowered = body_text.lower()
            # Only fail on auth markers that indicate a login page, not nav links
            if any(marker in lowered for marker in ("two-factor", "two factor", "verification code", "password")):
                return False
            if "schedule" in lowered:
                return True
            try:
                return await page.locator('a:has-text("Schedule")').count() > 0
            except Exception:
                return False

        async def wait_for_interactive_login(target_url):
            if not interactive_login:
                return False
            print("Pike13 login/MFA required. Complete login in the opened browser window; scraping will continue automatically.")
            deadline = time.time() + login_timeout
            while time.time() < deadline:
                await ensure_open_page()
                await page.wait_for_timeout(2000)
                await handle_post_login_interstitial()
                if await is_authenticated():
                    await page.goto(target_url)
                    await wait_until_ready()
                    return True
            raise RuntimeError("Timed out waiting for Pike13 interactive login/MFA.")

        async def safe_screenshot(path, **kwargs):
            try:
                await page.screenshot(path=path, timeout=2000, **kwargs)
            except Exception as exc:
                if verbose:
                    print(f"⚠️ Screenshot skipped for {path}: {exc}")

        async def optional_text_content(selector, default="", timeout=5000):
            try:
                value = await page.text_content(selector, timeout=timeout)
                return value if value is not None else default
            except Exception as exc:
                if verbose:
                    print(f"⚠️ Optional Pike13 field missing for {selector}: {exc}")
                return default

        async def handle_post_login_interstitial():
            """
            Pike13 may show a 2FA setup interstitial after login.
            If present, click "Skip for Now" so scraping can continue.
            """
            try:
                skip_btn = page.get_by_role("button", name=re.compile(r"skip for now", re.I))
                if await skip_btn.count() > 0 and await skip_btn.first.is_visible():
                    if verbose:
                        print("ℹ️ Detected Pike13 security interstitial. Clicking 'Skip for Now'.")
                    await skip_btn.first.click()
                    await page.wait_for_timeout(1500)
                    return True
            except Exception:
                pass

            # Fallback selector in case the control is not exposed as a role button.
            try:
                fallback_skip = page.locator("text=Skip for Now").first
                if await fallback_skip.is_visible():
                    if verbose:
                        print("ℹ️ Clicking fallback 'Skip for Now' selector.")
                    await fallback_skip.click()
                    await page.wait_for_timeout(1500)
                    return True
            except Exception:
                pass
            return False

        try:
            if verbose:
                print(f"Logging into {school_subdomain}.pike13.com...")
            
            login_url = f"https://{school_subdomain}.pike13.com/accounts/sign_in"
            schedule_home_url = f"https://{school_subdomain}.pike13.com/schedule"

            # --- Try cookie injection first (Okta SSO bypass) ---
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

                        # Restore localStorage tokens from the cookie payload
                        inject_storage_into_page(page, cookie_payload, school=school_subdomain)

                        if await is_authenticated():
                            if verbose:
                                print("✅ Authenticated via injected cookies — skipping login")
                        else:
                            if verbose:
                                print("⚠️ Cookie injection did not grant access. Falling through to normal login.")
                            cookie_auth_attempted = False
                    else:
                        if verbose:
                            print(f"⚠️ Cookie freshness check failed: {freshness['status']} — {freshness.get('error', '')}")
                except CookieAuthError as e:
                    if verbose:
                        print(f"⚠️ Cannot use cookie auth: {e}")
            # --- End cookie injection ---

            if not cookie_auth_attempted or not await is_authenticated():
                if profile_dir:
                    await page.goto(schedule_home_url)
                    await wait_until_ready()
                    if not await is_authenticated():
                        await page.goto(login_url)
                        await safe_screenshot("screenshots/01_login_page.png")
                        await wait_for_interactive_login(schedule_home_url)
                else:
                    # Navigate to login page
                    await page.goto(login_url)
                    await safe_screenshot("screenshots/01_login_page.png")
                    
                    # Fill login form
                    await page.wait_for_selector('input[placeholder="Email address"]', timeout=30000)
                    await page.fill('input[placeholder="Email address"]', PIKE13_USER or "")
                    await page.fill('input[placeholder="Password"]', PIKE13_PASS or "")
                    await safe_screenshot("screenshots/02_login_form_filled.png")
                    
                    # Click login and wait for navigation
                    await page.click('button:has-text("Sign In")')
                    await page.wait_for_timeout(1500)
                    await handle_post_login_interstitial()
            
            # Wait for successful login
            try:
                # Auto-handle MFA if we hit the two_factor page
                if "/account/two_factor" in page.url and _AUTO_MFA_AVAILABLE:
                    if verbose:
                        print("MFA required — auto-handling via email code...")
                    existing_ids = snapshot_inbox()
                    resend_btn = page.locator('button:has-text("Resend")')
                    if await resend_btn.count() > 0:
                        await resend_btn.first.click()
                        await page.wait_for_timeout(2000)
                    code = await read_mfa_code_via_imap(
                        existing_ids=existing_ids,
                        timeout_s=240,
                        poll_interval=5,
                    )
                    if code:
                        await enter_mfa_code(page, code)
                        await page.wait_for_timeout(5000)
                    else:
                        if verbose:
                            print("Could not auto-read MFA code")
                # Interstitial can appear a bit later; try once more before failing login.
                try:
                    await page.wait_for_selector('a:has-text("Schedule")', timeout=15000)
                except Exception:
                    await handle_post_login_interstitial()
                    if not await wait_for_interactive_login(schedule_home_url):
                        await page.wait_for_selector('a:has-text("Schedule")', timeout=60000)
                if verbose:
                    print("✅ Logged in successfully")
                await safe_screenshot("screenshots/03_after_login.png")
            except Exception as e:
                print(f"⚠️ Login failed: {e}")
                await safe_screenshot("screenshots/03_login_failed.png")
                raise Exception("Login failed - check screenshots")

            for date in dates:
                schedule_url = f"https://{school_subdomain}.pike13.com/schedule#/list?dt={date}&lt=staff&el=1"
                if verbose:
                    print(f"\nNavigating to schedule for {date}...")
                if not await goto_with_retry(schedule_url):
                    if verbose:
                        print(f"⚠️ Skipping {date} due to repeated navigation failures.")
                    continue
                if not await is_authenticated():
                    await wait_for_interactive_login(schedule_url)
                
                # Wait for list view to load
                try:
                    await page.wait_for_selector("table", timeout=30000)
                    await wait_until_ready()
                    await page.wait_for_timeout(5000)
                    date_obj = datetime.strptime(date, "%Y-%m-%d")
                    await safe_screenshot(f"screenshots/schedule_{date}.png", full_page=True)
                    
                    # Print page title and URL for debugging
                    if verbose:
                        print(f"Page title: {await page.title()}")
                        print(f"Current URL: {page.url}")
                    
                    # ── Step 1: Get lesson IDs and metadata from Pike13 API ──
                    client_id = "WWgvG1fId8iDU3rgoFXvz4A2kLnxDBSsOFacfk8X"
                    from_ts = f"{date}T06:00:00Z"
                    date_parts = date.split("-")
                    next_day = f"{date_parts[0]}-{date_parts[1]}-{str(int(date_parts[2])+1).zfill(2)}"
                    to_ts = f"{next_day}T05:59:59Z"
                    api_url = f"https://{school_subdomain}.pike13.com/api/v2/front/event_occurrences.json?client_id={client_id}&from={from_ts}&to={to_ts}"
                    
                    api_occurrences = {}  # occ_id → {name, start_at, end_at, url}
                    try:
                        api_resp = await page.goto(api_url, timeout=15000)
                        if api_resp and api_resp.status == 200:
                            api_body = await page.evaluate("() => document.body.innerText")
                            api_data = json.loads(api_body)
                            for occ in api_data.get("event_occurrences", []):
                                occ_id = occ.get("id")
                                if occ_id:
                                    api_occurrences[occ_id] = {
                                        "name": occ.get("name", ""),
                                        "start_at": occ.get("start_at", ""),
                                        "end_at": occ.get("end_at", ""),
                                        "url": occ.get("url", ""),
                                    }
                            if verbose:
                                print(f"🔍 API returned {len(api_occurrences)} occurrences on {date}.")
                        else:
                            if verbose:
                                print(f"⚠️ API returned status {api_resp.status if api_resp else 'none'}")
                    except Exception as e:
                        if verbose:
                            print(f"⚠️ API call failed: {e}")
                    
                    if verbose:
                        print(f"🔍 Found {len(api_occurrences)} lessons on {date}.")
                    
                    # Process each lesson using API metadata + notes scraping
                    for idx, (occ_id, meta) in enumerate(api_occurrences.items(), start=1):
                        lesson_id = str(occ_id)
                        notes_url = f"https://{school_subdomain}.pike13.com/desk/e/{lesson_id}/notes"
                        
                        # Use API metadata directly (no detail page scraping needed)
                        lesson_type = meta.get("name", "")
                        start_at = meta.get("start_at", "")
                        
                        # Parse start_at into time and date
                        lesson_time = ""
                        if start_at:
                            try:
                                dt = datetime.strptime(start_at.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z")
                                # Convert to Central time
                                from datetime import timezone as tz_mod
                                central = dt.astimezone(tz_mod(timedelta(hours=-5)))
                                lesson_time = central.strftime("%I:%M%p").lstrip("0")
                            except Exception:
                                lesson_time = start_at
                        
                        try:
                            # Navigate to notes page
                            if not await goto_with_retry(notes_url):
                                if verbose:
                                    print(f"⚠️ Skipping notes for lesson {lesson_id}: navigation failed")
                                notes = "No notes"
                                note_timestamp = ""
                            else:
                                await page.wait_for_timeout(3000)
                                await safe_screenshot(f"screenshots/notes_{lesson_id}.png")
                                
                                # Extract notes — try multiple selectors
                                notes = "No notes"
                                for note_sel in [
                                    "div.richtext_output.unbordered",
                                    "div.richtext_output",
                                    "div[class*='note']",
                                    "div[class*='richtext']",
                                    ".note-content",
                                    "main p",
                                ]:
                                    try:
                                        note_el = await page.query_selector(note_sel)
                                        if note_el:
                                            raw = await note_el.text_content()
                                            if raw and raw.strip() and raw.strip() not in ("No notes", ""):
                                                notes = raw.strip()
                                                break
                                    except Exception:
                                        continue
                                
                                # Extract timestamp
                                note_timestamp = ""
                                for ts_sel in ["small.timestamp", "time", "[class*='timestamp']"]:
                                    try:
                                        ts_el = await page.query_selector(ts_sel)
                                        if ts_el:
                                            ts_text = (await ts_el.text_content() or "").strip()
                                            if ts_text:
                                                note_timestamp = ts_text
                                                break
                                    except Exception:
                                        continue
                            
                            lessons_data.append({
                                "School": school_subdomain,
                                "Lesson ID": lesson_id,
                                "Date": date,
                                "Time": lesson_time,
                                "Instructor": "",
                                "Students": "",
                                "Lesson Type": lesson_type,
                                "Notes": notes,
                                "Note Timestamp": note_timestamp,
                                "Attendance Status": "unknown",
                                "Location": ""
                            })
                            
                            if verbose:
                                print(f"✅ {date} | Processed lesson {idx}/{len(api_occurrences)}: {lesson_type}")
                        
                        except Exception as e:
                            if verbose:
                                print(f"⚠️ Error processing lesson {lesson_id} on {date}: {e}")
                            continue

                except Exception as e:
                    print(f"⚠️ Error loading schedule for {date}: {e}")
                    try:
                        await safe_screenshot(f"screenshots/error_{date}.png")
                    except Exception as screenshot_exc:
                        if verbose:
                            print(f"⚠️ Could not capture error screenshot for {date}: {screenshot_exc}")
                    continue

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

        finally:
            # Stop tracing and save trace
            await context.tracing.stop(path="screenshots/trace.zip")
            await context.close()
            if browser:
                await browser.close()

    df = pd.DataFrame(lessons_data)
    file_name = f"{school_subdomain}_lessons_{dates[0]}_to_{dates[-1]}.csv"
    df.to_csv(file_name, index=False)
    if verbose:
        print(f"📂 Data saved to {file_name}")
    
    return df

if __name__ == "__main__":
    # Test with date that has multiple attendance statuses
    asyncio.run(scrape_lessons("westu-sor", dates=["2025-06-19"], verbose=True))
