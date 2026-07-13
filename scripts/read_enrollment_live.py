"""
Live enrollment reader for both SOR schools (CORRECTED rule).

CORRECTED RULE (locked 2026-07-11 / 2026-07-13):
  Enrollment = Pike13 people with has_membership:t
  Member = current pass OR available make-up lessons.
  Trials / camps / lesson-activity / parent orientation / immersion pass do NOT count.

Authoritative total is parsed from the report summary text:
  "1 - 50 of N results filtered by Has Membership?"
The distinct member-people link count is collected by clicking every
"Load 50 more results" button until it disappears. Both must agree.

Usage: python scripts/read_enrollment_live.py
"""
import asyncio
import re
import sys

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")

from pike13_auto_auth import authenticate_pike13
from playwright.async_api import async_playwright

SCHOOLS = {
    "West U": "westu-sor",
    "The Heights": "theheights-sor",
}

REPORT_URL = (
    "https://{sub}.pike13.com/desk/reports#/people/details"
    "?filters=(has_membership:!((eq:!(t))))"
    "&hide=1,4,5,6,7,8,10,13,16,17,18,19,24,25,26,28,29,30,31,32,34,36,37,38,39,57"
)


async def read_school(page, subdomain, school_name):
    url = REPORT_URL.format(sub=subdomain)
    print(f"\n=== {school_name} ({subdomain}) ===")
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)

    total = None
    for _ in range(20):
        body = await page.evaluate("document.body.innerText")
        m = re.search(r"of\s+(\d+)\s+results\s+filtered\s+by\s+Has\s+Membership", body)
        if m:
            total = int(m.group(1))
            break
        await page.wait_for_timeout(2000)
    print(f"  AUTHORITATIVE TOTAL (from report): {total}")

    seen = set()
    clicks = 0
    for _ in range(40):
        links = await page.evaluate(
            """() => Array.from(document.querySelectorAll('a[href*="/people/"]'))
                .filter(a => /\\/people\\/\\d+$/.test(a.href))
                .map(a => a.href.split('?')[0])"""
        )
        for h in links:
            seen.add(h)

        # Retry finding a visible load-more button across a few quick polls,
        # because it can be lazy-rendered just after a click.
        handle = None
        for _r in range(6):
            handle = await page.evaluate_handle(
                """() => Array.from(document.querySelectorAll('a,button'))
                    .find(e => /load\\s*\\d*\\s*more\\s*results/i.test(e.innerText || ''))"""
            )
            if handle and await handle.evaluate("(el) => !!el && el.offsetParent !== null"):
                break
            await page.wait_for_timeout(800)

        if handle and await handle.evaluate("(el) => !!el && el.offsetParent !== null"):
            clicks += 1
            print(f"    click {clicks}: {len(seen)} links so far")
            before = len(seen)
            try:
                await handle.scroll_into_view_if_needed()
            except Exception:
                pass
            await handle.click()
            # wait for the table to repaint (link count must grow OR reach total)
            for _w in range(20):
                await page.wait_for_timeout(700)
                links2 = await page.evaluate(
                    """() => Array.from(document.querySelectorAll('a[href*="/people/"]'))
                        .filter(a => /\\/people\\/\\d+$/.test(a.href))
                        .map(a => a.href.split('?')[0])"""
                )
                if len(links2) != before or (total is not None and len(links2) >= total):
                    break
        else:
            break

    distinct = len(seen)
    print(f"  pagination clicks: {clicks}")
    print(f"  DISTINCT MEMBER LINKS (cross-check): {distinct}")
    return {"total": total, "distinct": distinct}


async def main():
    async with async_playwright() as p:
        results = {}
        for school_name, sub in SCHOOLS.items():
            context = await authenticate_pike13(p, sub, "pike13_profile", True, True)
            page = context.pages[0] if context.pages else await context.new_page()
            try:
                results[school_name] = await read_school(page, sub, school_name)
            finally:
                await context.close()

    print("\n================ ENROLLMENT (corrected) ================")
    for name, d in results.items():
        ok = (d["total"] == d["distinct"]) if d["total"] is not None else False
        print(
            f"  {name}: report_total={d['total']}  distinct_links={d['distinct']}"
            + (f"  [MATCH]" if ok else "  [NOTE: pagination incomplete]")
        )
    print("========================================================")


asyncio.run(main())
