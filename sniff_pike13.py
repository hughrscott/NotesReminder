#!/usr/bin/env python3
"""Sniff Pike13's real pagination request."""
import asyncio, json, urllib.parse, sys
from pathlib import Path
from datetime import date
from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pike13_auto_auth

async def sniff():
    school = "westu-sor"
    frm, to = "2020-01-01", date.today().isoformat()
    filt = f"(last_membership_end:!((btw:!('{frm}','{to}'))))"
    sort = "(col:last_membership_end,order:d)"
    frag = f"/people/details?filters={urllib.parse.quote(filt, safe='(),:!')}&sort={urllib.parse.quote(sort, safe='(),:!')}&hide=1"
    url = f"https://{school}.pike13.com/desk/reports#{frag}"

    async with async_playwright() as p:
        ctx = await pike13_auto_auth.authenticate_pike13(p, school_subdomain=school, headless=True, verbose=False)
        pg = ctx.pages[0]

        # Log ALL queries requests to see what Pike13 sends for pagination
        async def log_request(req):
            if "/queries" in req.url and req.method == "POST":
                try:
                    body = req.post_data
                    if body:
                        d = json.loads(body)
                        page_info = d.get("data", {}).get("attributes", {}).get("page", {})
                        print(f"POST /queries page obj: {json.dumps(page_info)}")
                except:
                    print(f"POST /queries (raw): {req.post_data[:200] if req.post_data else 'none'}")

        pg.on("request", log_request)

        await pg.goto(url, wait_until="networkidle", timeout=30000)
        await pg.wait_for_timeout(5000)

        # Now try clicking a pagination button to trigger the next page request
        print("Looking for pagination controls...")
        pagination = pg.locator('[class*="pagination"], [class*="Pagination"], button[class*="next"], a[class*="next"]')
        count = await pagination.count()
        print(f"Pagination elements: {count}")

        # Click any visible element that might trigger next page
        next_btn = pg.locator('button:has-text("Next"), a:has-text("Next"), [aria-label="Next"], [title="Next"]')
        if await next_btn.count() > 0:
            print("Clicking Next...")
            await next_btn.first.click()
            await pg.wait_for_timeout(5000)
        else:
            print("No Next button found. Trying scroll...")
            await pg.keyboard.press("PageDown")
            await pg.wait_for_timeout(5000)

        await ctx.close()

asyncio.run(sniff())
