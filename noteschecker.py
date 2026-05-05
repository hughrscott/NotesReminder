__all__ = ['scrape_lessons']

import asyncio
import pandas as pd
import re
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import argparse
import asyncio
from datetime import datetime, timedelta
import os
import time

PIKE13_USER = os.environ.get("PIKE13_USER")
PIKE13_PASS = os.environ.get("PIKE13_PASSWORD")


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
        else:
            if not PIKE13_USER or not PIKE13_PASS:
                raise ValueError("Pike13 username or password not found in environment variables. Please set PIKE13_USER and PIKE13_PASSWORD.")
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
            if any(marker in page.url for marker in ("/accounts/sign_in", "/account/two_factor", "/login")):
                return False
            try:
                body_text = await page.locator("body").inner_text(timeout=5000)
            except Exception:
                body_text = ""
            lowered = body_text.lower()
            if any(marker in lowered for marker in ("two-factor", "two factor", "verification code", "sign in", "password")):
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
                schedule_url = f"https://{school_subdomain}.pike13.com/schedule#/day?dt={date}&lt=staff&el=1"
                if verbose:
                    print(f"\nNavigating to schedule for {date}...")
                if not await goto_with_retry(schedule_url):
                    if verbose:
                        print(f"⚠️ Skipping {date} due to repeated navigation failures.")
                    continue
                if not await is_authenticated():
                    await wait_for_interactive_login(schedule_url)
                
                # Wait for calendar to load
                try:
                    await page.wait_for_selector("div.calendar-lane", timeout=30000)
                    await wait_until_ready()
                    await page.wait_for_timeout(5000)  # Wait longer for JS to render
                    date_obj = datetime.strptime(date, "%Y-%m-%d")
                    date_label = date_obj.strftime("%b %d, %Y").replace(" 0", " ")
                    try:
                        await page.wait_for_selector(f"text={date_label}", timeout=15000)
                    except Exception as e:
                        if verbose:
                            print(f"⚠️ Could not confirm date label {date_label}: {e}")
                    await safe_screenshot(f"screenshots/schedule_{date}.png", full_page=True)
                    
                    # Print page title and URL for debugging
                    if verbose:
                        print(f"Page title: {await page.title()}")
                        print(f"Current URL: {page.url}")
                    
                    # Try waiting for a lesson block to appear
                    try:
                        await page.wait_for_selector('.calendar-lane .event', timeout=15000)
                    except Exception as e:
                        if verbose:
                            print(f"⚠️ Could not find any event blocks on {date}: {e}")
                            schedule_html = await page.inner_html("div.calendar-lane")
                            print(f"\n===== HTML for {date} =====\n{schedule_html}\n==========================\n")
                    
                    # Get all lesson links (broader search across the page)
                    lesson_links = await page.evaluate("""
                        () => Array.from(document.querySelectorAll("a[href*='/e/']"))
                                  .map(a => a.getAttribute("href"))
                                  .filter(href => href && href.includes('/e/'))
                    """)

                    # Fallback: click day-view events to extract lesson IDs
                    if not lesson_links:
                        events = page.locator(".calendar-lane .event")
                        event_count = await events.count()
                        if verbose:
                            print(f"🔍 Found {event_count} event blocks on {date}.")
                        for idx in range(event_count):
                            event = events.nth(idx)
                            try:
                                event_text = (await event.inner_text()).lower()
                            except Exception:
                                event_text = ""
                            if "unavailable" in event_text:
                                continue
                            try:
                                if await event.locator(".availability").count() > 0:
                                    continue
                            except Exception:
                                pass
                            try:
                                await event.click(force=True, timeout=5000)
                                await page.wait_for_url(re.compile(r"/e/\\d+"), timeout=8000)
                                match = re.search(r"/e/(\\d+)", page.url)
                                if match:
                                    lesson_links.append(f"/e/{match.group(1)}")
                                await page.goto(schedule_url)
                                await page.wait_for_selector("div.calendar-lane", timeout=30000)
                                await page.wait_for_timeout(2000)
                            except Exception as e:
                                if verbose:
                                    print(f"⚠️ Could not open event {idx + 1} on {date}: {e}")
                                try:
                                    await page.keyboard.press("Escape")
                                    await page.wait_for_timeout(500)
                                except Exception:
                                    pass
                                continue
                        lesson_links = list(dict.fromkeys(lesson_links))

                    # Fallback: try List view (tab click) if day view has no links
                    if not lesson_links:
                        switched = False
                        for selector in (
                            'button:has-text("List")',
                            'a:has-text("List")',
                            'role=tab[name="List"]',
                            'role=button[name="List"]',
                        ):
                            try:
                                await page.click(selector, timeout=5000)
                                await page.wait_for_timeout(3000)
                                switched = True
                                break
                            except Exception:
                                continue
                        if not switched and verbose:
                            print(f"⚠️ Could not click List tab on {date}.")

                    # Fallback: load list view by URL if still no links
                    if not lesson_links:
                        list_urls = [
                            f"https://{school_subdomain}.pike13.com/schedule#/list?dt={date}&lt=staff&el=1",
                            f"https://{school_subdomain}.pike13.com/schedule#/list?dt={date}",
                        ]
                        for list_url in list_urls:
                            try:
                                await page.goto(list_url)
                                await wait_until_ready()
                                await page.wait_for_timeout(3000)
                                lesson_links = await page.evaluate("""
                                    () => Array.from(document.querySelectorAll("a[href*='/e/']"))
                                              .map(a => a.getAttribute("href"))
                                              .filter(href => href && href.includes('/e/'))
                                """)
                                if lesson_links:
                                    break
                            except Exception as e:
                                if verbose:
                                    print(f"⚠️ Could not load list view {list_url}: {e}")
                    
                    if verbose:
                        print(f"🔍 Found {len(lesson_links)} lessons on {date}.")
                    
                    # Process each lesson
                    for idx, link in enumerate(lesson_links, start=1):
                        lesson_url = f"https://{school_subdomain}.pike13.com{link}"
                        lesson_id_match = re.search(r'/e/(\d+)', lesson_url)
                        if not lesson_id_match:
                            if verbose:
                                print(f"⚠️ Could not extract lesson ID from {lesson_url}")
                            continue

                        lesson_id = lesson_id_match.group(1)
                        notes_url = f"https://{school_subdomain}.pike13.com/desk/e/{lesson_id}/notes"

                        try:
                            if not await goto_with_retry(lesson_url):
                                if verbose:
                                    print(f"⚠️ Skipping lesson {lesson_id} on {date} due to navigation failures.")
                                continue
                            await page.wait_for_selector("span#title", timeout=15000)
                            await safe_screenshot(f"screenshots/lesson_{lesson_id}.png")

                            lesson_type = await page.text_content("span#title")
                            lesson_time_raw = await page.text_content("span#subtitle")
                            lesson_time_clean = lesson_time_raw or ""
                            lesson_time_clean = re.sub(r'\s+', ' ', lesson_time_clean).strip()
                            location = ""
                            if " - " in lesson_time_clean:
                                time_part, _, location_part = lesson_time_clean.rpartition(" - ")
                                lesson_time_clean = time_part.strip()
                                location = location_part.strip()
                            if " on " in lesson_time_clean:
                                lesson_time_clean = lesson_time_clean.split(" on ", 1)[0].strip()
                            lesson_time = lesson_time_clean
                            instructor = await optional_text_content(
                                ".sidebar_group.sidebar_menu li.person_menu_item a"
                            )

                            student_elements = await page.query_selector_all(".person-name a.name-link")
                            students = []
                            for elem in student_elements:
                                student_raw = await elem.text_content()
                                if student_raw:
                                    student_text = student_raw.replace('\n', ' ').strip()
                                    student_full_name = ' '.join(student_text.split())
                                    students.append(student_full_name)

                            students_str = ", ".join(students)

                            # Check for attendance status near student names
                            attendance_status = "unknown"
                            try:
                                # Look for attendance status elements near student information
                                # This will capture whatever status text appears (confirmed, canceled, complete, no show, etc.)
                                status_elements = await page.query_selector_all('.person-name, .attendance-status, .status, [class*="status"], [class*="attendance"]')
                                for elem in status_elements:
                                    elem_text = await elem.text_content()
                                    if elem_text:
                                        elem_text = elem_text.strip().lower()
                                        # Look for common status words
                                        status_words = ['confirmed', 'canceled', 'cancelled', 'complete', 'no show', 'pending', 'booked']
                                        for word in status_words:
                                            if word in elem_text:
                                                attendance_status = word.replace('cancelled', 'canceled')  # normalize spelling
                                                break
                                        if attendance_status != "unknown":
                                            break
                            except Exception as e:
                                if verbose:
                                    print(f"⚠️ Error checking attendance for lesson {lesson_id}: {e}")
                                attendance_status = "unknown"

                            await page.goto(notes_url)
                            await wait_until_ready()
                            await safe_screenshot(f"screenshots/notes_{lesson_id}.png")

                            notes_element = await page.query_selector("div.richtext_output.unbordered")
                            if notes_element:
                                notes_raw = await notes_element.text_content()
                                if notes_raw:
                                    try:
                                        notes = notes_raw.encode('latin1').decode('utf-8').strip()
                                    except UnicodeEncodeError:
                                        notes = notes_raw.strip()
                                else:
                                    notes = "No notes"
                            else:
                                notes = "No notes"

                            timestamp_element = await page.query_selector("small.timestamp")
                            note_timestamp = "No timestamp found"

                            if timestamp_element:
                                timestamp_raw = await timestamp_element.text_content()
                                timestamp_text = timestamp_raw.strip() if timestamp_raw else ""
                                match = re.search(r'on ([A-Za-z]{3}, [A-Za-z]{3} \d{1,2}, \d{4} at [\d:apm]+)', timestamp_text)
                                if match:
                                    note_timestamp = match.group(1).strip()

                            lessons_data.append({
                                "School": school_subdomain,
                                "Lesson ID": lesson_id,
                                "Date": date,
                                "Time": lesson_time.strip() if lesson_time else "",
                                "Instructor": instructor.strip() if instructor else "",
                                "Students": students_str,
                                "Lesson Type": lesson_type.strip() if lesson_type else "",
                                "Notes": notes,
                                "Note Timestamp": note_timestamp,
                                "Attendance Status": attendance_status,
                                "Location": location
                            })

                            if verbose:
                                print(f"✅ {date} | Processed lesson {idx}/{len(lesson_links)}")

                        except Exception as e:
                            if verbose:
                                print(f"⚠️ Error processing lesson {idx} on {date}: {e}")
                            continue

                except Exception as e:
                    print(f"⚠️ Error loading schedule for {date}: {e}")
                    try:
                        await safe_screenshot(f"screenshots/error_{date}.png")
                    except Exception as screenshot_exc:
                        if verbose:
                            print(f"⚠️ Could not capture error screenshot for {date}: {screenshot_exc}")
                    continue

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
