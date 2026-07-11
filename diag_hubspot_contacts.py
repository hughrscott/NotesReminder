#!/usr/bin/env python3
"""Diag 3: SSO -> CRM home -> click Contacts in-app -> confirm /contacts loads authed."""
import asyncio, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from okta_auth.scraper_session import launch_okta_context

async def main():
    async with launch_okta_context() as ctx:
        p = await ctx.new_page()
        await p.goto("https://app.hubspot.com/login", wait_until="domcontentloaded")
        await asyncio.sleep(5)
        try:
            ab = p.locator("button:has-text('Accept All')").first
            if await ab.count() and await ab.is_visible(): await ab.click()
        except: pass
        await asyncio.sleep(1)
        await p.locator("#username").fill("huscott@schoolofrock.com")
        await asyncio.sleep(1)
        await p.locator("button:has-text('Continue')").first.click()
        await asyncio.sleep(4)
        sso = p.locator("button:has-text('Sign in with SSO'), a:has-text('Sign in with SSO')").first
        if await sso.count(): await sso.click()
        for _ in range(15):
            await asyncio.sleep(3)
            if "hubspot.com" in p.url and "login" not in p.url.lower(): break
        print("landed:", p.url)
        # go to CRM home (not /contacts) to get full nav
        await p.goto("https://app.hubspot.com/", wait_until="domcontentloaded")
        await asyncio.sleep(8)
        print("after CRM home:", p.url, "| authed:", ("login" not in p.url.lower()))
        # find Contacts nav now
        clicked = False
        for sel in ["a:has-text('Contacts')", "button:has-text('Contacts')", "[data-test-id*='contacts' i]"]:
            el = p.locator(sel).first
            if await el.count() and await el.is_visible():
                print("clicking:", sel); await el.click(); clicked = True; break
        if not clicked: print("NO Contacts nav on CRM home")
        await asyncio.sleep(8)
        u = p.url
        print("FINAL URL:", u)
        print("ON CONTACTS:", "contacts" in u.lower() and "login" not in u.lower())
        txt = await p.evaluate("()=>document.body.innerText")
        # show a snippet that indicates a contacts/deals table
        for key in ["Lead Pipeline","All contacts","deals","Contacts","No contacts"]:
            if key in txt: print("  found UI token:", key)

asyncio.run(main())
