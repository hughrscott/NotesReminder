#!/usr/bin/env python3
"""Debug Pike13 API response structure."""
import asyncio, json, urllib.parse, sys
from pathlib import Path
from datetime import date
from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pike13_auto_auth

async def debug():
    school = "westu-sor"
    frm, to = "2020-01-01", date.today().isoformat()
    HIDE = "1,70,71,72,73,74,75,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,28,29,30,31,33,34,58,32,59,60,61,62,35,36,37,38,39,63,64,65,66,67,57"
    filt = f"(last_membership_end:!((btw:!('{frm}','{to}'))))"
    sort = "(col:last_membership_end,order:d)"
    frag = f"/people/details?filters={urllib.parse.quote(filt, safe='(),:!')}&sort={urllib.parse.quote(sort, safe='(),:!')}&hide={HIDE}"
    report_url = f"https://{school}.pike13.com/desk/reports#{frag}"

    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(p, school_subdomain=school, headless=True, verbose=False)
        page = context.pages[0]

        captured = {}
        async def on_response(resp):
            url = resp.url
            if "/queries" in url and "auth_token" in url:
                try:
                    body = await resp.body()
                    d = json.loads(body[:400000])
                    print(f"Response keys: {list(d.keys())}")
                    dd = d.get("data", {})
                    if isinstance(dd, dict):
                        attrs = dd.get("attributes", {})
                        print(f"  has_more: {attrs.get('has_more')} (type={type(attrs.get('has_more'))})")
                        print(f"  cursor: {str(attrs.get('cursor', 'NONE'))[:60]}")
                        print(f"  total_count: {attrs.get('total_count')}")
                        rows = attrs.get("rows") or []
                        print(f"  rows: {len(rows)}")
                        if rows:
                            print(f"  first row keys: {list(rows[0].keys())}")
                    else:
                        print(f"  data is list: {len(dd)} items")
                        captured["rows"] = dd
                        if dd:
                            print(f"  first item: {json.dumps(dd[0])[:200]}")
                except Exception as e:
                    print(f"  Error: {e}")

        page.on("response", on_response)
        await page.goto(report_url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(5000)
        await context.close()

asyncio.run(debug())
