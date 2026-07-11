#!/usr/bin/env python3
"""Find the correct HubSpot deals/pipeline view and capture its visible text.
The CRM home is a dashboard (no deal rows); individual deals live on a board view.
Tries candidate URLs with a proper SPA wait, dumps whatever renders."""
import asyncio, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv(Path.home() / ".hermes" / "SOR" / ".sorenv")
from okta_auth.scraper_session import launch_okta_context

CANDIDATES = [
    "/deals-board", "/pipeline", "/deals-board-view",
    "/contacts", "/sales/", "/reports-dashboard",
]

async def main():
    async with launch_okta_context() as ctx:
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await page.goto("https://app.hubspot.com/", wait_until="domcontentloaded")
        await asyncio.sleep(6)
        best = None
        for path in CANDIDATES:
            try:
                await page.goto("https://app.hubspot.com" + path, wait_until="domcontentloaded", timeout=20000)
                await asyncio.sleep(7)
                b = await page.locator("body").inner_text()
                fn = f"/tmp/hubspot_try{path.replace('/','_') or '_root'}.txt"
                Path(fn).write_text(f"URL={page.url}\n\n{b}", encoding="utf-8")
                has_rows = b.count("Lead Pipeline") + b.count("|") + b.count("View deal")
                print(f"{path} -> url={page.url} chars={len(b)} rowhits={has_rows} file={fn}")
                if has_rows > (best[1] if best else -1):
                    best = (path, has_rows)
            except Exception as e:
                print(f"{path} err: {e}")
        print("BEST:", best)

asyncio.run(main())
