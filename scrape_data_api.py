#!/usr/bin/env python3
"""Capture the data API feeding the membership detail list + try Export via multiple selectors."""
import asyncio, sys, json, os
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_data_api.json"
CSV_OUT = MODELS / "pike13_last_memberships_export.csv"

API_LOG = []

async def on_response(resp):
    url = resp.url
    if any(k in url for k in ["/api/", "people", "client", "member", "widget", "report", "export", "csv"]):
        try:
            body = await resp.body()
            kind = "nonjson"
            rows = 0
            keys = None
            try:
                d = json.loads(body[:800000])
                kind = "json"
                if isinstance(d, dict):
                    for k, v in d.items():
                        if isinstance(v, list):
                            rows = len(v)
                    keys = list(d.keys())[:12]
            except Exception:
                pass
            API_LOG.append({"url": url[:160], "kind": kind, "rows": rows, "keys": keys, "bytes": len(body)})
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
        # wide viewport so toolbar isn't clipped
        await page.set_viewport_size({"width": 1920, "height": 1080})

        await page.goto(f"https://{school}.pike13.com/desk/reports", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2500)
        await page.locator('a:has-text("Insights")').first.click(timeout=6000)
        await page.wait_for_timeout(3500)
        # Clear existing API log before the click
        API_LOG.clear()
        await page.locator('text=Last Memberships').first.click(timeout=6000)
        await page.wait_for_timeout(4000)

        # Try Export with several selectors
        exported = False
        for sel in [
            'button:has-text("Export")',
            'a:has-text("Export")',
            '[class*=export]',
            'button[title*="Export" i]',
            'button:has-text("CSV")',
        ]:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                try:
                    async with page.expect_download(timeout=15000) as dl:
                        await loc.click(timeout=5000)
                    download = await dl.value
                    await download.save_as(str(CSV_OUT))
                    exported = True
                    print(f"EXPORTED via {sel} -> {CSV_OUT} ({os.path.getsize(CSV_OUT)} bytes)")
                    break
                except Exception as e:
                    print(f"  export {sel} failed: {str(e)[:70]}")
        if not exported:
            print("Export not captured by any selector")

        # Report the APIs that fired after the click
        print(f"\nAPI calls after click ({len(API_LOG)}):")
        for a in API_LOG:
            print(f"  [{a['kind']}] rows={a['rows']} {a['url']}")

        # Save the most promising data-bearing API sample
        data_apis = [a for a in API_LOG if a['rows'] and a['rows'] > 0]
        OUT.write_text(json.dumps({
            "pulled_at": datetime.now(timezone.utc).isoformat(),
            "school": school,
            "exported_csv": exported,
            "csv_path": str(CSV_OUT) if exported else None,
            "data_apis": data_apis,
            "all_apis": API_LOG,
        }, indent=2, default=str))
        print(f"\nSaved -> {OUT}")
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
