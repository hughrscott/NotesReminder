#!/usr/bin/env python3
"""Debug a single HubSpot SSO handoff: click tile, approve, trace redirects."""
import asyncio, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from okta_auth.scraper_session import launch_okta_context
from okta_auth.config import get_config
from okta_auth.telegram_bot import _notify

CONFIG = get_config()

async def trace(page, label, url_substr):
    print(f"\n=== {label} ===")
    await page.goto("https://sor.okta.com/app/UserHome", wait_until="domcontentloaded")
    await asyncio.sleep(4)
    # click tile
    try:
        await page.locator(f"a:has-text('{label}')").first.click(timeout=15000)
        print(f"clicked {label}")
    except Exception as e:
        print(f"click fail {e}"); return
    await asyncio.sleep(3)
    # push controls
    for sel in ["button:has-text('Send push')","button:has-text('Verify')","button:has-text('Send Notification')"]:
        try:
            el=page.locator(sel).first
            if await el.count() and await el.is_visible():
                print(f"clicking push control {sel}"); await el.click(); break
        except: pass
    await _notify(f"📲 DEBUG push for {label} — approve to trace redirect")
    # trace
    t0=time.time()
    last=None
    while time.time()-t0 < 240:
        await asyncio.sleep(4)
        u=page.url
        if u!=last:
            print(f"  URL -> {u}")
            last=u
        # if we hit a login/401, report
        if "login" in u and ("401" in u or "authFailure" in u):
            print("  -> landed on LOGIN (SSO did not complete)")
            return
        if url_substr.lower() in u.lower() and "login" not in u.lower():
            print(f"  -> REACHED {label} app: {u}")
            return
    print("  -> timeout, final:", page.url)

async def main():
    async with launch_okta_context() as ctx:
        page=await ctx.new_page()
        await trace(page, "HubSpot", "hubspot.com")
        cookies=await ctx.cookies()
        hs=[c["name"] for c in cookies if "hubspot" in c.get("domain","")]
        print("hubspot cookies:", len(hs), hs[:10])

asyncio.run(main())
