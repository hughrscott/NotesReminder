#!/usr/bin/env python3
"""Verify if 'empty' lessons actually have notes on Pike13.

Uses the shared pike13_urls helper for correct numeric-ID URLs.
"""
import asyncio, sys, json
from pathlib import Path
from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pike13_auto_auth
from notesreminder.lib.pike13_urls import pike13_note_url
from notesreminder.lib.note_page_probe import classify_note_page, strip_editor_chrome

URLS = [
    ("westu-sor", "westu-sor-295349613", "Matt Mireles — Drum Lessons"),
    ("theheights-sor", "theheights-sor-295389576", "Angel Garza — Drum Lessons"),
    ("westu-sor", "westu-sor-284949372", "Reef Stallworth — Drum Lessons"),
]


async def probe_note_page(page, school: str, lesson_id: str, desc: str):
    notes_url = pike13_note_url(school, lesson_id)
    print(f"\n{'='*60}")
    print(f"Checking: {desc}")
    print(f"  URL: {notes_url}")

    try:
        resp = await page.goto(notes_url, wait_until="networkidle", timeout=30000)
        http_status = resp.status if resp else 0
        await page.wait_for_timeout(2000)

        # Screenshot
        ss_path = f"/tmp/pike13_note_{lesson_id}.png"
        await page.screenshot(path=ss_path)

        # Detect signals
        page_text = await page.text_content("body") or ""
        has_delete = await page.query_selector(
            'a[href*="/notes/"][data-method="delete"], '
            'a:has-text("Delete"), '
            'button:has-text("Delete")'
        ) is not None
        has_no_notes = "No notes have been created" in page_text

        # Extract note text if Delete button exists
        extracted_text = None
        if has_delete:
            for sel in ["div.richtext_output p", "div.richtext_output div", "div.richtext_output"]:
                try:
                    els = await page.query_selector_all(sel)
                    texts = []
                    for el in els:
                        t = (await el.text_content() or "").strip()
                        if t and t.lower() != "no notes":
                            texts.append(t)
                    if texts:
                        extracted_text = ' '.join(texts)
                        break
                except Exception:
                    continue

        result = classify_note_page(
            http_status=http_status,
            page_text=page_text,
            has_delete_button=has_delete,
            has_no_notes_text=has_no_notes,
            extracted_text=extracted_text,
        )

        print(f"  Status: {result.status}")
        if result.notes_text:
            print(f"  Text: {result.notes_text[:150]}")
        if result.error_message:
            print(f"  Error: {result.error_message}")

    except Exception as e:
        print(f"  ERROR: {e}")


async def verify():
    async with async_playwright() as p:
        for school, lesson_id, desc in URLS:
            ctx = await pike13_auto_auth.authenticate_pike13(
                p, school_subdomain=school, headless=True, verbose=False
            )
            page = ctx.pages[0]
            await probe_note_page(page, school, lesson_id, desc)
            await ctx.close()


asyncio.run(verify())
