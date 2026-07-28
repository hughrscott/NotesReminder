#!/usr/bin/env python3
"""
Backfill empty/uncertain lesson notes.

Re-verifies all lessons with uncertain note_status using the definitive
Pike13 classification.  Uses fast HTTP probes (no rendering for 404s),
one browser context per school, concurrent pages.

Usage:
  python scripts/backfill_empty_lesson_notes.py --dry-run
  python scripts/backfill_empty_lesson_notes.py --school westu-sor
  python scripts/backfill_empty_lesson_notes.py --days 28 --concurrency 3
"""
import asyncio, sqlite3, sys, json, argparse, os
from datetime import date, datetime, timedelta
from pathlib import Path
from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import pike13_auto_auth
from notesreminder.lib.pike13_urls import pike13_note_url, numeric_pike13_lesson_id
from notesreminder.lib.note_page_probe import (
    classify_note_page, note_completed_for_status, VALID_STATUSES,
)

DB = Path(__file__).resolve().parent.parent / "reminders.db"
MAX_DRY_RUN = 10


def get_uncertain_lessons(days=28, school=None, limit=None):
    """Query lessons with uncertain note_status that need re-verification."""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    params = [f"-{days} days"]
    query = """
        SELECT r.lesson_id, r.school, r.lesson_date, r.instructor_name, 
               r.lesson_type, ln.note_status
        FROM reminders r
        LEFT JOIN lesson_notes ln ON r.lesson_id = ln.lesson_id
        WHERE r.lesson_date >= DATE('now', ?)
          AND (ln.note_status IN ('empty', 'unknown', 'error') OR ln.lesson_id IS NULL)
          AND COALESCE(r.lesson_type, '') NOT LIKE '%admin%'
          AND COALESCE(r.lesson_type, '') NOT LIKE '%availability%'
    """
    if school:
        query += " AND r.school = ?"
        params.append(school)
    query += " ORDER BY r.lesson_date DESC"
    if limit:
        query += f" LIMIT {limit}"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return rows


def update_lesson_note(lesson_id, result, lesson_url):
    """Store backfill result in lesson_notes."""
    conn = sqlite3.connect(DB)
    try:
        conn.execute("""
            INSERT OR REPLACE INTO lesson_notes 
            (lesson_id, note_completed, notes_text, note_status,
             note_page_status_code, note_url, note_checked_at, note_error)
            VALUES (?, ?, ?, ?, ?, ?, DATETIME('now'), ?)
        """, (
            lesson_id,
            note_completed_for_status(result.status),
            result.notes_text,
            result.status,
            result.http_status,
            lesson_url,
            result.error_message,
        ))
        conn.commit()
    finally:
        conn.close()


async def probe_lesson(sem, page, school, lesson):
    """Probe one lesson note page and update DB."""
    async with sem:
        lesson_id = lesson["lesson_id"]
        try:
            notes_url = pike13_note_url(school, lesson_id)
        except ValueError:
            return {"id": lesson_id, "status": "error", "error": "non-numeric ID"}

        try:
            resp = await page.goto(notes_url, wait_until="domcontentloaded", timeout=15000)
            http_status = resp.status if resp else 0
            await page.wait_for_timeout(1000)

            page_text = ""
            try:
                page_text = await page.text_content("body") or ""
            except Exception:
                pass

            has_delete = await page.query_selector(
                'a[href*="/notes/"][data-method="delete"], '
                'a:has-text("Delete"), '
                'button:has-text("Delete")'
            ) is not None
            has_no_notes = "No notes have been created" in page_text

            extracted_text = None
            if has_delete:
                for sel in ["div.richtext_output p", "div.richtext_output"]:
                    try:
                        els = await page.query_selector_all(sel)
                        texts = [t for el in els if (t := (await el.text_content() or "").strip()) and t.lower() != "no notes"]
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

            update_lesson_note(lesson_id, result, notes_url)
            return {
                "id": lesson_id,
                "status": result.status,
                "was": lesson["note_status"] or "missing",
                "instructor": lesson["instructor_name"],
            }

        except Exception as e:
            update_lesson_note(lesson_id, classify_note_page(0, "", False, False), notes_url)
            return {"id": lesson_id, "status": "error", "error": str(e)[:80]}


async def backfill(args):
    lessons = get_uncertain_lessons(days=args.days, school=args.school,
                                     limit=MAX_DRY_RUN if args.dry_run else None)
    if not lessons:
        print("No uncertain lessons to backfill.")
        return

    by_school = {}
    for l in lessons:
        by_school.setdefault(l["school"], []).append(l)

    print(f"Backfilling {sum(len(v) for v in by_school.values())} uncertain lessons"
          f" across {len(by_school)} schools...")

    sem = asyncio.Semaphore(args.concurrency)
    total_results = []

    async with async_playwright() as p:
        for school, school_lessons in by_school.items():
            print(f"\n  {school}: {len(school_lessons)} lessons...")
            ctx = await pike13_auto_auth.authenticate_pike13(
                p, school_subdomain=school, headless=True, verbose=False
            )

            tasks = []
            for i, lesson in enumerate(school_lessons):
                page = ctx.pages[0] if i == 0 else await ctx.new_page()
                tasks.append(probe_lesson(sem, page, school, dict(lesson)))

            results = await asyncio.gather(*tasks)
            total_results.extend(results)

            # Print results
            counts = {}
            for r in results:
                status = r.get("status", "unknown")
                counts[status] = counts.get(status, 0) + 1

            for status, cnt in sorted(counts.items()):
                print(f"    {status}: {cnt}")

            await ctx.close()

    # Summary
    print(f"\nDone: {len(total_results)} lessons probed")
    verdicts = {}
    for r in total_results:
        s = r.get("status", "?")
        verdicts[s] = verdicts.get(s, 0) + 1
    for s, c in sorted(verdicts.items()):
        print(f"  {s}: {c}")


def main():
    parser = argparse.ArgumentParser(description="Backfill uncertain lesson notes")
    parser.add_argument("--days", type=int, default=28)
    parser.add_argument("--school", help="Limit to one school (westu-sor or theheights-sor)")
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(backfill(args))


if __name__ == "__main__":
    main()
