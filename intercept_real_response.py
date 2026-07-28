#!/usr/bin/env python3
"""Intercept the REAL working queries response (rows are in the response, not my replay).
Also replay verbatim using exact cookies from the browser context to confirm self-serve works.
"""
import asyncio, sys, json
from pathlib import Path
from datetime import datetime, timezone, date
import urllib.parse as up

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")

async def main():
    school = "westu-sor"
    args = sys.argv[1:]
    frm = "2026-07-15"; to = "2026-08-11"
    if "--from" in args: frm = args[args.index("--from")+1]
    if "--to" in args: to = args[args.index("--to")+1]
    if "--school" in args: school = args[args.index("--school")+1]

    REAL_RESP = {}
    TOKEN = {}

    async def on_response(resp):
        if "reports/clients/queries" in resp.url:
            try:
                body = await resp.text()
                REAL_RESP["url"] = resp.url
                REAL_RESP["status"] = resp.status
                REAL_RESP["body"] = body[:8000]
            except Exception as e:
                REAL_RESP["err"] = str(e)

    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=school, headless=True, verbose=False
        )
        page = context.pages[0]
        page.on("response", on_response)
        await page.set_viewport_size({"width": 1920, "height": 1080})

        await page.goto(f"https://{school}.pike13.com/desk/reports", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)
        # capture the auth_token from the real request
        async def cap(req):
            if "reports/clients/queries" in req.url:
                t = up.urlparse(req.url).query
                tk = up.parse_qs(t).get("auth_token", [None])[0]
                if tk: TOKEN["token"] = tk
        page.on("request", cap)
        await page.locator('a:has-text("Insights")').first.click(timeout=6000)
        await page.wait_for_timeout(2500)
        REAL_RESP.clear()
        await page.locator('text=Last Memberships').first.click(timeout=6000)
        await page.wait_for_timeout(4000)

        # The REAL response from Pike13's own request — this is the data we want
        print("=== REAL Pike13 response (widget click) ===")
        print("status:", REAL_RESP.get("status"))
        normalized = {}
        try:
            d = json.loads(REAL_RESP.get("body", "{}"))
            attrs = d.get("data", {}).get("attributes", {})
            rows = attrs.get("rows", [])
            fields = [f.get("name") for f in attrs.get("fields", [])]
            records = [dict(zip(fields, row)) for row in rows]
            print(f"ROWS in real response: {len(records)} (reported total: {attrs.get('total_count')})")
            for record in records[:10]:
                print("  ", record)
            normalized = {
                "status": REAL_RESP.get("status"),
                "range": [frm, to],
                "count": len(records),
                "reported_total_count": attrs.get("total_count"),
                "has_more": attrs.get("has_more"),
                "records": records,
            }
        except Exception as e:
            print("parse err:", e, REAL_RESP.get("body", "")[:400])
            normalized = {"status": REAL_RESP.get("status"), "range": [frm, to], "parse_error": str(e)}

        # Save normalized data only. Never persist the short-lived auth token.
        out = MODELS / f"pike13_real_response_{frm}_{to}.json"
        out.write_text(json.dumps(normalized, indent=2, default=str))
        print(f"\nSaved -> {out}")
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
