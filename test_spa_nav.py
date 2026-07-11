#!/usr/bin/env python3
"""Test: from the authed dashboard SPA, do an IN-PAGE client-side nav to the
deals board (location.href assignment) to preserve the session (avoids the
raw-goto login redirect). Report whether deal rows appear."""
import asyncio, sys, time
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
        print("start url:", page.url)
        # candidate in-page SPA routes
        for route in ["/deals-board", "/deals", "/pipeline", "/sales", "/contacts"]:
            try:
                await page.evaluate("u => { window.location.href = 'https://app.hubspot.com' + u }", route)
                await asyncio.sleep(6)
                u = page.url.lower()
                body = await page.locator("body").inner_text()
                rows = body.count("Lead Pipeline") + body.count("|") + body.count("View deal")
                print(f"route {route} -> url={u[:70]} bodychars={len(body)} rowhits={rows}")
                if "login" not in u and rows > 0:
                    print("  *** DEALS VIEW FOUND ***")
                    Path("/tmp/hubspot_deals_found.txt").write_text(f"route={route}\nurl={u}\n\n{body[:4000]}", encoding="utf-8")
                    break
            except Exception as e:
                print(f"route {route} err: {e}")
        await ctx.close()

asyncio.run(main())
