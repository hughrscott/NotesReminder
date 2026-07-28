#!/usr/bin/env python3
"""Tight pass: open Last Memberships detail, click Export, capture CSV + the data API behind the list."""
import asyncio, sys, json, os
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_export_run.json"
CSV_OUT = MODELS / "pike13_last_memberships_export.csv"

API_CALLS = []

async def on_response(resp):
    url = resp.url
    if "/api/" in url and ("people" in url or "client" in url or "member" in url or "widget" in url or "report" in url):
        try:
            body = await resp.body()
            try:
                d = json.loads(body[:600000])
                rows = 0
                if isinstance(d, dict):
                    for k, v in d.items():
                        if isinstance(v, list):
                            rows = len(v)
                API_CALLS.append({"url": url[:120], "rows": rows, "keys": (list(d.keys())[:10] if isinstance(d, dict) else None)})
            except Exception:
                API_CALLS.append({"url": url[:120], "rows": -1})
        except Exception:
            pass

async def main():
    school = sys.argv[sys.argv.index("--school") + 1] if "--school" in sys.argv else "westu-sor"
    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=school, headless=True, verbose=False
        )
        page = context.pages[0]
        page.on("response", on_response)

        await page.goto(f"https://{school}.pike13.com/desk/reports", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2500)
        await page.locator('a:has-text("Insights")').first.click(timeout=6000)
        await page.wait_for_timeout(3500)

        # Click the Last Memberships card
        await page.locator('text=Last Memberships').first.click(timeout=6000)
        # wait for detail (the "All N results" pill or Filters button)
        try:
            await page.locator('button:has-text("Export")').wait_for(state="visible", timeout=10000)
            print("detail view open, Export visible")
        except Exception as e:
            print(f"Export not visible: {str(e)[:60]}")

        # Capture the visible table
        rows = await page.evaluate(
            """() => Array.from(document.querySelectorAll('table tbody tr')).map(r => Array.from(r.querySelectorAll('td')).map(td => td.innerText.trim())).filter(r => r.length)"""
        )
        print(f"\nTable: {len(rows)} rows, cols={len(rows[0]) if rows else 0}")
        for r in rows[:6]:
            print(f"  {r[:6]}")

        # Click Export -> download CSV
        export = page.locator('button:has-text("Export")').first
        exported = False
        if await export.count() > 0:
            try:
                async with page.expect_download(timeout=20000) as dl:
                    await export.click(timeout=5000)
                download = await dl.value
                await download.save_as(str(CSV_OUT))
                exported = True
                print(f"\nEXPORTED -> {CSV_OUT} (size {os.path.getsize(CSV_OUT)} bytes)")
            except Exception as e:
                print(f"export failed: {str(e)[:100]}")
        else:
            print("no Export button")

        OUT.write_text(json.dumps({
            "pulled_at": datetime.now(timezone.utc).isoformat(),
            "school": school,
            "exported_csv": exported,
            "csv_path": str(CSV_OUT) if exported else None,
            "table_rows": len(rows),
            "api_calls": API_CALLS[-25:],
        }, indent=2, default=str))
        print(f"\nSaved -> {OUT}")
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
