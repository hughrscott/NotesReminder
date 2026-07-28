#!/usr/bin/env python3
"""Intercept the clients/queries REQUEST (method+body) and parse configuration.json for the membership report schema."""
import asyncio, sys, json
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_query_request.json"
CFG = MODELS / "pike13_reports_config.json"

REQ_LOG = []
CONFIG = None

async def on_request(req):
    url = req.url
    if "reports/clients/queries" in url or "reports/" in url:
        try:
            body = req.body
            REQ_LOG.append({"method": req.method, "url": url[:160], "post_data": (body or "")[:2000]})
        except Exception:
            REQ_LOG.append({"method": req.method, "url": url[:160], "post_data": None})

async def on_response(resp):
    global CONFIG
    url = resp.url
    if "reports/people/configuration.json" in url:
        try:
            CONFIG = await resp.json()
            CFG.write_text(json.dumps(CONFIG, indent=2))
        except Exception:
            pass

async def main():
    school = sys.argv[sys.argv.index("--school") + 1] if "--school" in sys.argv else "westu-sor"
    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=school, headless=True, verbose=False
        )
        page = context.pages[0]
        page.on("request", on_request)
        page.on("response", on_response)
        await page.set_viewport_size({"width": 1920, "height": 1080})

        await page.goto(f"https://{school}.pike13.com/desk/reports", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2500)
        await page.locator('a:has-text("Insights")').first.click(timeout=6000)
        await page.wait_for_timeout(3500)
        REQ_LOG.clear()
        await page.locator('text=Last Memberships').first.click(timeout=6000)
        await page.wait_for_timeout(4000)

        # Find the membership-related report definition in config
        report_info = None
        if CONFIG:
            for rep in CONFIG.get("reports", []):
                name = str(rep.get("name", "")).lower()
                if "member" in name or "last" in name or "expir" in name:
                    report_info = {k: rep[k] for k in ("name", "api_endpoint", "display_name", "filters", "filter_types") if k in rep}
                    break
            # also surface filter_types list
            fts = CONFIG.get("filter_types", [])
            print(f"filter_types ({len(fts)}): {[f.get('name') if isinstance(f,dict) else f for f in fts][:30]}")

        print(f"\nRequests during membership click ({len(REQ_LOG)}):")
        for r in REQ_LOG:
            print(f"  [{r['method']}] {r['url']}")
            if r['post_data']:
                print(f"     body: {r['post_data'][:400]}")

        if report_info:
            print(f"\nMembership report def: {json.dumps(report_info, indent=2)[:800]}")

        OUT.write_text(json.dumps({
            "pulled_at": datetime.now(timezone.utc).isoformat(),
            "school": school,
            "requests": REQ_LOG,
            "membership_report_def": report_info,
            "config_saved": str(CFG),
        }, indent=2, default=str))
        print(f"\nSaved -> {OUT}")
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
