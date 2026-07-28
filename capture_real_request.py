#!/usr/bin/env python3
"""Capture the FULL queries request (URL incl. query params + method + body) when the membership widget loads."""
import asyncio, sys, json
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_real_request.json"

REQS = []

async def on_request(req):
    url = req.url
    if "reports/clients/queries" in url:
        try:
            body = req.body
        except Exception:
            body = None
        REQS.append({
            "method": req.method,
            "url": url,  # full, incl. query string
            "post_data": (body or "")[:4000] if body else None,
        })

async def main():
    school = sys.argv[sys.argv.index("--school") + 1] if "--school" in sys.argv else "westu-sor"
    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=school, headless=True, verbose=False
        )
        page = context.pages[0]
        page.on("request", on_request)
        await page.set_viewport_size({"width": 1920, "height": 1080})

        await page.goto(f"https://{school}.pike13.com/desk/reports", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)
        await page.locator('a:has-text("Insights")').first.click(timeout=6000)
        await page.wait_for_timeout(2500)
        REQS.clear()
        await page.locator('text=Last Memberships').first.click(timeout=6000)
        await page.wait_for_timeout(4000)

        print(f"Captured {len(REQS)} queries requests:")
        for i, r in enumerate(REQS):
            print(f"\n--- req {i+1} [{r['method']}] ---")
            print(f"  URL: {r['url'][:400]}")
            if r['post_data']:
                print(f"  BODY: {r['post_data'][:2000]}")

        OUT.write_text(json.dumps({
            "pulled_at": datetime.now(timezone.utc).isoformat(),
            "school": school,
            "requests": REQS,
        }, indent=2, default=str))
        print(f"\nSaved -> {OUT}")
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
