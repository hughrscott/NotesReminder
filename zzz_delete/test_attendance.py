#!/usr/bin/env python3
"""Simple test script to verify attendance status detection"""

import asyncio
import os
from playwright.async_api import async_playwright

PIKE13_USER = os.environ.get("PIKE13_USER")
PIKE13_PASS = os.environ.get("PIKE13_PASSWORD")

async def test_attendance_detection():
    """Test attendance status detection on a single lesson"""
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Visible for debugging
        context = await browser.new_context()
        page = await context.new_page()

        try:
            # Login
            print("Logging in...")
            await page.goto("https://westu-sor.pike13.com/accounts/sign_in")
            await page.fill('input[placeholder="Email address"]', PIKE13_USER)
            await page.fill('input[placeholder="Password"]', PIKE13_PASS)
            await page.click('button:has-text("Sign In")')
            await page.wait_for_selector('a:has-text("Schedule")', timeout=30000)
            print("✅ Logged in")

            # Go to specific lesson (you can replace this with a known lesson ID)
            # For now, let's go to the schedule and grab the first lesson
            await page.goto("https://westu-sor.pike13.com/schedule#/day?dt=2025-06-19&lt=staff&el=1")
            await page.wait_for_selector("div.calendar-lane", timeout=30000)
            await page.wait_for_timeout(5000)
            
            # Get first lesson link
            lesson_links = await page.evaluate("""
                () => Array.from(document.querySelectorAll(".calendar-lane .event a"))
                          .map(a => a.getAttribute("href"))
                          .filter(href => href && href.includes('/e/'))
                          .slice(0, 1)  // Just get first lesson
            """)
            
            if not lesson_links:
                print("❌ No lessons found")
                return
                
            lesson_url = f"https://westu-sor.pike13.com{lesson_links[0]}"
            print(f"Testing lesson: {lesson_url}")
            
            # Go to lesson page
            await page.goto(lesson_url)
            await page.wait_for_selector("span#title", timeout=15000)
            
            # Test attendance status detection
            print("\n=== Testing Attendance Status Detection ===")
            
            # Method 1: Look for status elements
            status_elements = await page.query_selector_all('.person-name, .attendance-status, .status, [class*="status"], [class*="attendance"]')
            print(f"Found {len(status_elements)} potential status elements")
            
            attendance_status = "unknown"
            for elem in status_elements:
                elem_text = await elem.text_content()
                if elem_text:
                    elem_text = elem_text.strip().lower()
                    status_words = ['confirmed', 'canceled', 'cancelled', 'complete', 'no show', 'pending', 'booked']
                    for word in status_words:
                        if word in elem_text:
                            attendance_status = word.replace('cancelled', 'canceled')
                            print(f"✅ Found status '{attendance_status}' in element: {elem_text[:50]}...")
                            break
                    if attendance_status != "unknown":
                        break
            
            # Method 2: Check page content
            if attendance_status == "unknown":
                print("Checking full page content...")
                page_content = await page.content()
                status_words = ['confirmed', 'canceled', 'cancelled', 'complete', 'no show', 'pending', 'booked']
                for word in status_words:
                    if word in page_content.lower():
                        attendance_status = word.replace('cancelled', 'canceled')
                        print(f"✅ Found status '{attendance_status}' in page content")
                        break
            
            print(f"\nFinal attendance status: {attendance_status}")
            
            # Save screenshot for inspection
            await page.screenshot(path="test_attendance_screenshot.png", full_page=True)
            print("📸 Screenshot saved as test_attendance_screenshot.png")

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_attendance_detection())