#!/usr/bin/env python3
"""Use page.route to intercept the queries POST and read its full body (route gives post_data reliably)."""
import asyncio, sys, json
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_real_body.json"

BODIES = []

async def on_route(route, request):
    url = request.url
    if "reports/clients/queries" in url:
        body = request.post_data
        BODIES.append({
            "method": request.method,
            "url": url,
            "post_data": body[:5000] if body else None,
            "headers": dict(request.headers),
        })
        # continue the request normally
        await route.continue_()
    else:
        await route.continue_()

async def main():
    school = sys.argv[sys.argv.index("--school") + 1] if "--school" in sys.argv else "westu-sor"
    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=school, headless=True, verbose=False
        )
        page = context.pages[0]
        await page.route("**/reports/clients/queries**", on_route)
        await page.set_viewport_size({"width": 1920, "height": 1080})

        await page.goto(f"https://{school}.pike13.com/desk/reports", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)
        await page.locator('a:has-text("Insights")').first.click(timeout=6000)
        await page.wait_for_timeout(2500)
        BODIES.clear()
        await page.locator('text=Last Memberships').first.click(timeout=6000)
        await page.wait_for_timeout(4000)

        print(f"Captured {len(BODIES)} queries POST bodies:")
        for i, b in enumerate(BODIES):
            print(f"\n--- body {i+1} [{b['method']}] ---")
            print(f"  CT: {b['headers'].get('content-type')}")
            print(f"  BODY: {b['post_data']}")

        OUT.write_text(json.dumps({
            "pulled_at": datetime.now(timezone.utc).isoformat(),
            "school": school,
            "bodies": BODIES,
        }, indent=2, default=str))
        print(f"\nSaved -> {OUT}")
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
