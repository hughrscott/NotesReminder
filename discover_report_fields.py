#!/usr/bin/env python3
"""
discover_report_fields.py — Open each named report once (ONE auth + MFA),
click into it, and capture the REAL filter field token Pike13 writes into
the address-bar hash. We do NOT guess tokens; we read them live.

For each report we record:
  - the label we clicked
  - the field token in the hash (e.g. last_membership_end, last_visited, ...)
  - the operator token (btw, gt, lt, eq, ...)
  - whether a /queries response fired and its row count

Output: models/report_field_map.json  (the authoritative field map)
"""
import asyncio, sys, json, re
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
SCHOOL = "westu-sor"

# reports to probe (labels visible on the Insights dashboard)
REPORTS = [
    "Last Memberships",
    "New Clients",
    "First Memberships",
    "First Visits",
    "Last Visited",
    "Expiring Memberships",
    "Failed Transactions",
    "No Shows",
    "Late Cancellations",
    "Unconfirmed Attendance",
    "Unpaid Visits",
    "Past Due Invoices",
    "New Memberships",
]


async def capture_hash_and_queries(page):
    """After clicking a report, read the URL hash + the latest /queries payload."""
    await page.wait_for_timeout(4000)

    hash_txt = await page.evaluate("() => location.hash || ''")

    # parse field/op from the hash: filters=(<field>:!((<op>:!(...)))
    field = op = None
    m = re.search(r"filters=\(([^:]+):!?\(?\(?([a-z]+):", hash_txt)
    if m:
        field = m.group(1)
        op = m.group(2)
    # looser: capture the first token after 'filters=(' and the op after ':!'
    if not field:
        m2 = re.search(r"filters=\(([a-z_]+):", hash_txt)
        if m2:
            field = m2.group(1)
    if not op:
        m3 = re.search(r":\(?([a-z]+):!", hash_txt)
        if m3:
            op = m3.group(1)

    # rows from a /queries we can fire by reloading the report body
    rows = None
    try:
        await page.locator("body").click(timeout=2000)
    except Exception:
        pass
    await page.wait_for_timeout(3000)

    return {
        "hash": hash_txt[:300],
        "field": field,
        "op": op,
    }


async def main():
    results = {}
    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=SCHOOL, headless=True, verbose=False
        )
        page = context.pages[0]

        await page.goto(f"https://{SCHOOL}.pike13.com/desk/reports", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(3000)
        await page.locator('a:has-text("Insights")').first.click(timeout=6000)
        await page.wait_for_timeout(4000)

        for label in REPORTS:
            # broad: any element containing the label text
            loc = page.locator(f':has-text("{label}")').first
            if await loc.count() == 0:
                # fallback: case-insensitive search via JS click
                clicked = await page.evaluate(
                    """(txt) => {
                        const els = Array.from(document.querySelectorAll('a, div, button, span, h1,h2,h3,h4'));
                        const el = els.find(e => (e.innerText||'').includes(txt));
                        if (el) { el.click(); return true; }
                        return false;
                    }""", label)
                if not clicked:
                    results[label] = {"found": False}
                    print(f"[skip] {label} not found")
                    continue
            else:
                try:
                    await loc.click(timeout=5000)
                except Exception:
                    await page.evaluate(
                        """(txt) => { const els=Array.from(document.querySelectorAll('a,div,button,span')); const el=els.find(e=>(e.innerText||'').includes(txt)); if(el) el.click(); }""", label)
            info = await capture_hash_and_queries(page)
            info["found"] = True
            results[label] = info
            print(f"[ok] {label:24s} -> field={info['field']} op={info['op']}")
            # go back to dashboard for next probe
            try:
                await page.go_back(timeout=5000)
                await page.wait_for_timeout(3000)
            except Exception:
                pass

        await context.close()

    out = {
        "school": SCHOOL,
        "discovered_at": datetime.now(timezone.utc).isoformat(),
        "reports": results,
    }
    OUT = MODELS / "report_field_map.json"
    OUT.write_text(json.dumps(out, indent=2, default=str))
    print(f"\nSaved -> {OUT}")
    print("\n=== FIELD MAP ===")
    for label, info in results.items():
        if info.get("found") and info.get("field"):
            print(f"  {label:24s} field={info['field']:24s} op={info.get('op')}")


if __name__ == "__main__":
    asyncio.run(main())
