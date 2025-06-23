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

PIKE13_USER = os.environ.get("PIKE13_USER")
PIKE13_PASS = os.environ.get("PIKE13_PASSWORD")

if not PIKE13_USER or not PIKE13_PASS:
    raise ValueError("Pike13 username or password not found in environment variables. Please set PIKE13_USER and PIKE13_PASSWORD.")

async def scrape_lessons(school_subdomain, dates=None, start_date=None, end_date=None, verbose=True):
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

    async with async_playwright() as p:
        # Launch browser with more debugging options
        browser = await p.chromium.launch(
            headless=True,  # Keep headless for CI
            args=['--disable-dev-shm-usage']  # Helps with memory issues in CI
        )
        
        # Create a new context with tracing enabled
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        )
        
        # Start tracing
        await context.tracing.start(screenshots=True, snapshots=True, sources=True)
        
        page = await context.new_page()

        try:
            if verbose:
                print(f"Logging into {school_subdomain}.pike13.com...")
            
            # Navigate to login page
            await page.goto(f"https://{school_subdomain}.pike13.com/accounts/sign_in")
            await page.screenshot(path="screenshots/01_login_page.png")
            
            # Fill login form
            await page.wait_for_selector('input[placeholder="Email address"]', timeout=30000)
            await page.fill('input[placeholder="Email address"]', PIKE13_USER)
            await page.fill('input[placeholder="Password"]', PIKE13_PASS)
            await page.screenshot(path="screenshots/02_login_form_filled.png")
            
            # Click login and wait for navigation
            await page.click('button:has-text("Sign In")')
            
            # Wait for successful login
            try:
                await page.wait_for_selector('a:has-text("Schedule")', timeout=60000)
                if verbose:
                    print("✅ Logged in successfully")
                await page.screenshot(path="screenshots/03_after_login.png")
            except Exception as e:
                print(f"⚠️ Login failed: {e}")
                await page.screenshot(path="screenshots/03_login_failed.png")
                raise Exception("Login failed - check screenshots")

            for date in dates:
                schedule_url = f"https://{school_subdomain}.pike13.com/schedule#/day?dt={date}&lt=staff&el=1"
                print(f"\nNavigating to schedule for {date}...")
                await page.goto(schedule_url)
                
                # Wait for calendar to load
                try:
                    await page.wait_for_selector("div.calendar-lane", timeout=30000)
                    await page.wait_for_timeout(10000)  # Wait longer for JS to render
                    await page.screenshot(path=f"screenshots/schedule_{date}.png", full_page=True)
                    
                    # Print page title and URL for debugging
                    print(f"Page title: {await page.title()}")
                    print(f"Current URL: {page.url}")
                    
                    # Try waiting for a staff name or lesson block
                    try:
                        await page.wait_for_selector('text=Zach Jones', timeout=15000)
                    except Exception as e:
                        print(f"⚠️ Could not find staff name on {date}: {e}")
                        # Print the HTML of the schedule area for debugging
                        schedule_html = await page.inner_html("div.calendar-lane")
                        print(f"\n===== HTML for {date} =====\n{schedule_html}\n==========================\n")
                    
                    # Get all lesson links
                    lesson_links = await page.evaluate("""
                        () => Array.from(document.querySelectorAll(".calendar-lane .event a"))
                                  .map(a => a.getAttribute("href"))
                                  .filter(href => href && href.includes('/e/'))
                    """)
                    
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
                            await page.goto(lesson_url)
                            await page.wait_for_selector("span#title", timeout=15000)
                            await page.screenshot(path=f"screenshots/lesson_{lesson_id}.png")

                            lesson_type = await page.text_content("span#title")
                            lesson_time = await page.text_content("span#subtitle")
                            instructor = await page.text_content(".sidebar_group.sidebar_menu li.person_menu_item a")

                            student_elements = await page.query_selector_all(".person-name a.name-link")
                            students = []
                            for elem in student_elements:
                                student_text = (await elem.text_content()).replace('\n', ' ').strip()
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
                            await page.wait_for_load_state("networkidle", timeout=30000)
                            await page.screenshot(path=f"screenshots/notes_{lesson_id}.png")

                            notes_element = await page.query_selector("div.richtext_output.unbordered")
                            if notes_element:
                                notes_raw = await notes_element.text_content()
                                try:
                                    notes = notes_raw.encode('latin1').decode('utf-8').strip()
                                except UnicodeEncodeError:
                                    notes = notes_raw.strip()
                            else:
                                notes = "No notes"

                            timestamp_element = await page.query_selector("small.timestamp")
                            note_timestamp = "No timestamp found"

                            if timestamp_element:
                                timestamp_text = (await timestamp_element.text_content()).strip()
                                match = re.search(r'on ([A-Za-z]{3}, [A-Za-z]{3} \d{1,2}, \d{4} at [\d:apm]+)', timestamp_text)
                                if match:
                                    note_timestamp = match.group(1).strip()

                            lessons_data.append({
                                "School": school_subdomain,
                                "Date": date,
                                "Time": lesson_time.strip() if lesson_time else "",
                                "Instructor": instructor.strip() if instructor else "",
                                "Students": students_str,
                                "Lesson Type": lesson_type.strip() if lesson_type else "",
                                "Notes": notes,
                                "Note Timestamp": note_timestamp,
                                "Attendance Status": attendance_status
                            })

                            if verbose:
                                print(f"✅ {date} | Processed lesson {idx}/{len(lesson_links)}")

                        except Exception as e:
                            if verbose:
                                print(f"⚠️ Error processing lesson {idx} on {date}: {e}")
                            continue

                except Exception as e:
                    print(f"⚠️ Error loading schedule for {date}: {e}")
                    await page.screenshot(path=f"screenshots/error_{date}.png")
                    continue

        finally:
            # Stop tracing and save trace
            await context.tracing.stop(path="screenshots/trace.zip")
            await browser.close()

    df = pd.DataFrame(lessons_data)
    file_name = f"{school_subdomain}_lessons_{dates[0]}_to_{dates[-1]}.csv"
    df.to_csv(file_name, index=False)
    if verbose:
        print(f"📂 Data saved to {file_name}")
    
    return df

if __name__ == "__main__":
    # Test with date that has multiple attendance statuses
    asyncio.run(scrape_lessons("westu-sor", dates=["2025-06-19"]))