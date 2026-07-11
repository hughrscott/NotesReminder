#!/usr/bin/env python3
"""Find the deals board via PORTAL-SPECIFIC HubSpot URLs (scraping only, no API).
The generic /deals-board, /deals, /pipeline 404 because they lack the portal id.
Portal id is in the dashboard URL: app.hubspot.com/reports-dashboard/<PORTAL>/view/...
Tests candidate portal-specific paths via in-page nav (preserves warm session).
Uses browser_profiles/hubspot (copy) -> does NOT touch sor_shared/Pike13."""
import asyncio, sys, re, json
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
from dotenv import load_dotenv
load_dotenv(Path.home()/'.hermes'/'SOR'/'.sorenv')
from playwright.async_api import async_playwright
PROF = Path('browser_profiles/hubspot')

async def main():
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(str(PROF), headless=True, viewport={'width':1440,'height':1000})
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await page.goto('https://app.hubspot.com/', wait_until='domcontentloaded')
        await asyncio.sleep(8)
        m = re.search(r'app\.hubspot\.com/(?:reports-dashboard/)?(\d+)', page.url)
        pid = m.group(1) if m else None
        print('AUTH URL:', page.url, '| portal:', pid)
        if not pid:
            print('NO PORTAL ID'); await ctx.close(); return
        candidates = [
            f'https://app.hubspot.com/contacts/{pid}/deal',
            f'https://app.hubspot.com/deals/{pid}',
            f'https://app.hubspot.com/contacts/{pid}/deal/all',
            f'https://app.hubspot.com/sales/{pid}/deals',
            f'https://app.hubspot.com/pipeline/{pid}',
        ]
        js = """(u) => { window.location.href = u; return true; }"""
        for u in candidates:
            try:
                await page.evaluate(js, u)
                await asyncio.sleep(6)
                body = (await page.locator('body').inner_text()) or ''
                tokens = [t for t in ['deal','pipeline','stage','amount','Lead Pipeline','Deals'] if t.lower() in body.lower()]
                print(f"URL={u}\n  final={page.url}\n  len={len(body)} tokens={tokens[:6]}\n")
            except Exception as e:
                print(f"URL={u} ERR={e}\n")
        await ctx.close()

asyncio.run(main())
