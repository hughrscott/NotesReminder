#!/usr/bin/env python3
"""Confirm: after SSO handshake in one context, does direct nav to /contacts
stay authed WITHIN the same context? If yes, HubSpot extraction = SSO then scrape in-place."""
import asyncio, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from okta_auth.scraper_session import launch_okta_context

async def sso_handshake(page):
    await page.goto("https://app.hubspot.com/login", wait_until="domcontentloaded")
    await asyncio.sleep(4)
    try:
        ab = page.locator("button:has-text('Accept All')").first
        if await ab.count() and await ab.is_visible(): await ab.click()
    except: pass
    await asyncio.sleep(1)
    await page.locator("#username").fill("huscott@schoolofrock.com")
    await asyncio.sleep(1)
    await page.locator("button:has-text('Continue')").first.click()
    await asyncio.sleep(4)
    sso = page.locator("button:has-text('Sign in with SSO'), a:has-text('Sign in with SSO')").first
    if await sso.count(): await sso.click()
    # wait for dashboard
    for _ in range(15):
        await asyncio.sleep(3)
        if "hubspot.com" in page.url and "login" not in page.url.lower():
            return True
        if "okta" in page.url.lower():
            # okta auto-approve via warm session; wait
            continue
    return "hubspot.com" in page.url and "login" not in page.url.lower()

async def main():
    async with launch_okta_context() as ctx:
        p = await ctx.new_page()
        ok = await sso_handshake(p)
        print("handshake result:", ok, "url:", p.url)
        # NOW direct-nav to contacts in SAME context
        await p.goto("https://app.hubspot.com/contacts", wait_until="domcontentloaded")
        await asyncio.sleep(8)
        u = p.url
        print("same-context direct nav to /contacts:", u)
        print("AUTH" if ("contacts" in u and "login" not in u.lower()) else "NEEDS RE-AUTH")

asyncio.run(main())
