#!/usr/bin/env python3
"""Fire a real clients/queries POST with a date filter and capture the JSON results.
Proves self-serve pull for any date range — no clicking, no Export button.
"""
import asyncio, sys, json, os
from pathlib import Path
from datetime import datetime, timezone, date

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_query_result.json"
CSV_OUT = MODELS / "pike13_query_export.csv"

async def main():
    school = sys.argv[sys.argv.index("--school") + 1] if "--school" in sys.argv else "westu-sor"
    # optional: --from 2026-07-01 --to 2026-07-31
    args = sys.argv[2:]
    frm = date.today().replace(day=1).isoformat()
    to = date.today().isoformat()
    if "--from" in args:
        frm = args[args.index("--from")+1]
    if "--to" in args:
        to = args[args.index("--to")+1]

    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=school, headless=True, verbose=False
        )
        page = context.pages[0]
        await page.goto(f"https://{school}.pike13.com/desk/reports", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

        # Get a fresh auth_token from the page (Pike13 stores it)
        auth_token = await page.evaluate(
            """() => { try { return JSON.parse(localStorage.getItem('auth_token')||localStorage.getItem('session')||'null'); } catch(e){ return null; } }"""
        )
        # Fallback: capture from any queries URL we can trigger
        # Easiest: replicate the queries endpoint the UI uses, with our filter
        # Build the query payload the UI sends
        query_payload = {
            "report": "people",
            "api_version": 3,
            "filters": [
                {"name": "last_membership_end", "operator": "btw", "value": [frm, to]}
            ],
            "columns": ["full_name", "status", "last_membership_end", "next_plan_end", "current_plans", "primary_staff_member_full_name"],
            "page": 1,
            "per_page": 200,
        }
        # We need the auth_token. Capture it from network by triggering a real queries call.
        token_holder = {}
        async def cap(req):
            u = req.url
            if "reports/clients/queries" in u:
                import urllib.parse as up
                q = up.urlparse(u).query
                t = up.parse_qs(q).get("auth_token", [None])[0]
                if t:
                    token_holder["token"] = t
        page.on("request", cap)
        # trigger a real one by clicking the widget (so we get a valid token + see the real payload shape)
        await page.locator('a:has-text("Insights")').first.click(timeout=6000)
        await page.wait_for_timeout(2500)
        await page.locator('text=Last Memberships').first.click(timeout=6000)
        await page.wait_for_timeout(3500)
        token = token_holder.get("token")
        print(f"captured auth_token: {bool(token)}")

        if not token:
            print("No auth_token captured; cannot call API directly")
            await context.close()
            return

        # Now POST our own query with the date range
        url = f"https://{school}.pike13.com/desk/api/v3/reports/clients/queries?auth_token={token}&subdomain={school}"
        # The UI likely sends the filter as a 'query' param or POST body. Try POST JSON.
        resp = await page.evaluate(
            """async (args) => {
                const r = await fetch(args.u, {method:'POST', headers:{'Content-Type':'application/json','Accept':'application/json'}, body: JSON.stringify(args.payload)});
                const t = await r.text();
                return {status: r.status, body: t.slice(0, 4000)};
            }""",
            {"u": url, "payload": query_payload}
        )
        print(f"\nAPI status: {resp['status']}")
        print(f"body preview: {resp['body'][:1500]}")
        try:
            data = json.loads(resp["body"])
            rows = data.get("data", [])
            print(f"\nROWS RETURNED: {len(rows)}")
            for row in rows[:10]:
                print(f"  {row}")
            OUT.write_text(json.dumps({"pulled_at": datetime.now(timezone.utc).isoformat(), "school": school, "range": [frm,to], "count": len(rows), "sample": rows[:50]}, indent=2, default=str))
            print(f"\nSaved -> {OUT}")
        except Exception as e:
            print(f"parse failed: {e}")
            OUT.write_text(json.dumps({"pulled_at": datetime.now(timezone.utc).isoformat(), "raw": resp["body"]}, indent=2))
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
