#!/usr/bin/env python3
"""Capture the authenticated HubSpot CRM-home body text to inspect what deal
data is actually visible (for tomorrow's refactor). No scraping, no push."""
import asyncio, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv(Path.home() / ".hermes" / "SOR" / ".sorenv")
from okta_auth.scraper_session import launch_okta_context

async def main():
    async with launch_okta_context() as ctx:
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await page.goto("https://app.hubspot.com/", wait_until="domcontentloaded")
        await asyncio.sleep(8)
        print("URL:", page.url)
        body = await page.locator("body").inner_text()
        Path("/tmp/hubspot_crmhome_text.txt").write_text(body, encoding="utf-8")
        # also try the deals/board view
        for path in ["/deals", "/contacts", "/sales"]:
            try:
                await page.goto("https://app.hubspot.com" + path, wait_until="domcontentloaded", timeout=20000)
                await asyncio.sleep(6)
                b = await page.locator("body").inner_text()
                Path(f"/tmp/hubspot_{path.strip('/') or 'root'}_text.txt").write_text(b, encoding="utf-8")
                print(f"captured {path} ({len(b)} chars) -> url {page.url}")
            except Exception as e:
                print(f"{path} err: {e}")
        print("DONE; files in /tmp/hubspot_*_text.txt")

asyncio.run(main())
