__all__ = ['scrape_lessons']

import asyncio
import pandas as pd
import re
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import argparse
import asyncio
from datetime import datetime, timedelta

PIKE13_USER = "huscott@schoolofrock.com"
PIKE13_PASS = "Coventry12$"

async def scrape_lessons(school_subdomain, dates=None, start_date=None, end_date=None, verbose=True):
    if dates is None and start_date and end_date:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        delta = (end - start).days
        dates = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta + 1)]
    elif dates is None:
        raise ValueError("Provide either 'dates' or 'start_date' and 'end_date'.")

    lessons_data = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        if verbose:
            print(f"Logging into {school_subdomain}.pike13.com...")
        await page.goto(f"https://{school_subdomain}.pike13.com/accounts/sign_in")
        await page.wait_for_selector('input[placeholder="Email address"]')
        await page.fill('input[placeholder="Email address"]', PIKE13_USER)
        await page.fill('input[placeholder="Password"]', PIKE13_PASS)
        await page.click('button:has-text("Sign In")')
        await page.wait_for_selector('a:has-text("Schedule")', timeout=60000)
        if verbose:
            print("✅ Logged in successfully")
            await page.screenshot(path="after_login.png", full_page=True)

        await page.screenshot(path="login_attempt.png")

        for date in dates:
            schedule_url = f"https://{school_subdomain}.pike13.com/schedule#/day?dt={date}&lt=staff&el=1"
            await page.goto(schedule_url)
            await page.wait_for_selector("div.calendar-lane", timeout=20000)
            await page.wait_for_timeout(7000)  # Wait longer for JS to render
            await page.screenshot(path=f"schedule_{date}.png", full_page=True)

            # Try waiting for a staff name or lesson block (e.g., Zach Jones)
            try:
                await page.wait_for_selector('text=Zach Jones', timeout=5000)
            except Exception as e:
                print(f"⚠️ Could not find staff name on {date}: {e}")

            # Take a full-page screenshot of the schedule page for this date
            await page.screenshot(path=f"schedule_{date}_full.png", full_page=True)

            # Print out the HTML of the schedule area
            schedule_html = await page.inner_html("div.calendar-lane")
            print(f"\n===== HTML for {date} =====\n{schedule_html}\n==========================\n")

            lesson_links = await page.evaluate("""
                () => Array.from(document.querySelectorAll(".calendar-lane .event a"))
                          .map(a => a.getAttribute("href"))
                          .filter(href => href && href.includes('/e/'))
            """)

            if verbose:
                print(f"🔍 Found {len(lesson_links)} lessons on {date}.")

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

                    await page.goto(notes_url)
                    await page.wait_for_load_state("networkidle", timeout=30000)

                    notes_element = await page.query_selector("div.richtext_output.unbordered")
                    if notes_element:
                        notes_raw = await notes_element.text_content()
                        # Robust encoding fix
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
                        "Note Timestamp": note_timestamp
                    })

                    if verbose:
                        print(f"✅ {date} | Processed lesson {idx}/{len(lesson_links)}")

                except Exception as e:
                    if verbose:
                        print(f"⚠️ Error processing lesson {idx} on {date}: {e}")
                    continue

        await browser.close()

    df = pd.DataFrame(lessons_data)
    file_name = f"{school_subdomain}_lessons_{dates[0]}_to_{dates[-1]}.csv"
    df.to_csv(file_name, index=False)
    if verbose:
        print(f"📂 Data saved to {file_name}")

if __name__ == "__main__":
    # Optionally add CLI usage here in the future
    pass