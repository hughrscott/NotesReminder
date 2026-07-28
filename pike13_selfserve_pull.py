#!/usr/bin/env python3
"""SELF-SERVE Pike13 clients pull via /desk/api/v3/reports/clients/queries.
Replicates the exact JSON:API wire format the UI sends. No clicking, no Export button.
Usage: python3 pike13_selfserve_pull.py --from 2026-07-15 --to 2026-08-11 [--school westu-sor]
"""
import asyncio, sys, json
from pathlib import Path
from datetime import datetime, timezone, date

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")

async def main():
    school = "westu-sor"
    args = sys.argv[1:]
    frm = date.today().isoformat()
    to = (date.today() + __import__("datetime").timedelta(days=28)).isoformat()
    if "--from" in args: frm = args[args.index("--from")+1]
    if "--to" in args: to = args[args.index("--to")+1]
    if "--school" in args: school = args[args.index("--school")+1]

    fields = ["full_name","email","phone","address","current_plans","person_id","status","last_membership_end_date","next_plan_end"]

    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=school, headless=True, verbose=False
        )
        page = context.pages[0]
        await page.goto(f"https://{school}.pike13.com/desk/reports", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

        # capture auth_token + csrf token by triggering one real widget click
        token_holder = {}
        csrf_holder = {}
        async def cap(req):
            if "reports/clients/queries" in req.url:
                import urllib.parse as up
                t = up.urlparse(req.url).query
                tk = up.parse_qs(t).get("auth_token", [None])[0]
                if tk: token_holder["token"] = tk
                ct = req.headers.get("x-csrf-token")
                if ct: csrf_holder["csrf"] = ct
        page.on("request", cap)
        await page.locator('a:has-text("Insights")').first.click(timeout=6000)
        await page.wait_for_timeout(2500)
        await page.locator('text=Last Memberships').first.click(timeout=6000)
        await page.wait_for_timeout(3000)
        token = token_holder.get("token")
        csrf = csrf_holder.get("csrf") or await page.evaluate("() => (document.querySelector('meta[name=csrf-token]')||{}).content || ''")
        if not token:
            print("NO auth_token"); await context.close(); return

        url = f"https://{school}.pike13.com/desk/api/v3/reports/clients/queries?auth_token={token}&subdomain={school}"
        body = {
            "data": {
                "type": "queries",
                "attributes": {
                    "page": {},
                    "fields": fields,
                    "total_count": "t",
                    "filter": [["btw", "last_membership_end_date", [frm, to]]],
                    "sort": ["last_membership_end_date-"],
                },
            }
        }
        # Use context.request so session cookies are sent automatically; add the required headers
        result = await context.request.post(
            url,
            headers={
                "Content-Type": "application/vnd.api+json",
                "Accept": "application/vnd.api+json",
                "X-Requested-With": "XMLHttpRequest",
                "X-CSRF-Token": csrf,
            },
            data=json.dumps(body),
        )
        raw = await result.text()
        print(f"status: {result.status}")
        try:
            data = json.loads(raw)
            rows = data.get("data", [])
            meta = data.get("meta", {})
            print(f"ROWS: {len(rows)}  meta: {meta}")
            for row in rows[:15]:
                print(f"  {row}")
            out = MODELS / f"pike13_pull_{frm}_{to}.json"
            out.write_text(json.dumps({"range":[frm,to],"count":len(rows),"rows":rows,"meta":meta}, indent=2, default=str))
            print(f"Saved -> {out}")
        except Exception as e:
            print(f"parse err {e}; raw: {result['body'][:800]}")
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
