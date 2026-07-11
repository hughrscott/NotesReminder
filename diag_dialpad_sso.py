#!/usr/bin/env python3
"""Does Dialpad have an SSO/Okta button on dialpad.com/login that, when clicked,
SSO-redirects to Okta (warm sid auto-approves, NO push) and lands on the app page?"""
import asyncio, sys
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
from playwright.async_api import async_playwright
DIALPAD_PROFILE = Path("browser_profiles/dialpad")

async def main():
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(str(DIALPAD_PROFILE), headless=True, viewport={"width":1440,"height":1000})
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await page.goto("https://www.dialpad.com/login", wait_until="domcontentloaded")
        await asyncio.sleep(5)
        print("initial url:", page.url)
        # find SSO / Okta button
        selectors = [
            "button:has-text('Okta')", "a:has-text('Okta')",
            "button:has-text('Sign in with SSO')", "a:has-text('Sign in with SSO')",
            "button:has-text('Single Sign')", "button:has-text('SSO')",
            "button:has-text('Continue with Okta')",
        ]
        clicked = None
        for sel in selectors:
            el = page.locator(sel).first
            if await el.count() and await el.is_visible():
                print("clicking:", sel)
                await el.click(); clicked = sel; break
        if not clicked:
            print("NO SSO button found. Login page text sample:")
            print((await page.locator("body").inner_text())[:400])
            await ctx.close(); return
        # wait for SSO redirect (warm sid should auto-approve -> no push)
        for i in range(15):
            await asyncio.sleep(3)
            uu = page.url.lower()
            body = await page.locator("body").inner_text() if await page.locator("body").count() else ""
            if "dialpad.com/app/" in uu and any(t in body.lower() for t in ["messages","calls","voicemails"]):
                print(f"[{i*3}s] AUTO-AUTH OK -> {page.url}"); await ctx.close(); return
            if "okta.com" in uu:
                print(f"[{i*3}s] ON OKTA: {page.url} (warm sid should auto-approve...)")
            elif "dialpad.com/login" in uu:
                print(f"[{i*3}s] still on dialpad login")
        print(f"[45s] final: {page.url}")
        await ctx.close()
asyncio.run(main())
