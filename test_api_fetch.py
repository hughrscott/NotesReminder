#!/usr/bin/env python3
"""Read-only proof: from the AUTHENTICATED HubSpot page, call the HubSpot CRM
API via page.evaluate(fetch, credentials:'include'). No API token, no cookie
decryption needed if same-site CORS allows it. Confirms the refactor route.
Uses browser_profiles/hubspot (copy) -> does NOT touch sor_shared/Pike13."""
import asyncio, sys, time, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv(Path.home() / ".hermes" / "SOR" / ".sorenv")
from playwright.async_api import async_playwright
HUBSPOT_PROFILE = Path("browser_profiles/hubspot")

async def main():
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            str(HUBSPOT_PROFILE), headless=True, viewport={"width": 1440, "height": 1000})
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await page.goto("https://app.hubspot.com/", wait_until="domcontentloaded")
        await asyncio.sleep(8)
        print("AUTH URL:", page.url)
        # Try the CRM v3 deals endpoint via in-page fetch (same-site credentials)
        js = """async () => {
            const urls = [
                'https://api.hubspot.com/crm/v3/objects/deals?limit=3&properties=dealname,amount,dealstage,pipeline',
                'https://app.hubspot.com/api-crm/v3/objects/deals?limit=3&properties=dealname,dealstage',
            ];
            for (const u of urls) {
                try {
                    const r = await fetch(u, {credentials: 'include', headers: {'Content-Type':'application/json'}});
                    const txt = await r.text();
                    return {url: u, status: r.status, body: txt.slice(0, 800)};
                } catch (e) { return {url: u, error: String(e)}; }
            }
        }"""
        try:
            res = await page.evaluate(js)
            print("FETCH RESULT:")
            print(json.dumps(res, indent=2)[:1200])
        except Exception as e:
            print("evaluate err:", repr(e))
        await ctx.close()

asyncio.run(main())
