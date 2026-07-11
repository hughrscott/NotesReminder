#!/usr/bin/env python3
"""Diagnose the Pike13 2FA page: is the OTP form in an iframe? What's the live cwr_u value?"""
import asyncio, sys
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
sys.path.insert(0, str(Path('.').resolve()/'scripts'))
from dotenv import load_dotenv
load_dotenv(Path.home()/'.hermes'/'SOR'/'.sorenv')
from playwright.async_api import async_playwright
import auto_auth_pike13 as A

PIKE13_USER="huscott@schoolofrock.com"
PIKE13_PASS=__import__('os').environ.get("PIKE13_PASSWORD","")

async def main():
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(str(Path("browser_profiles/sor_shared")), headless=True, viewport={"width":1920,"height":1080})
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await page.goto(f"https://westu-sor.pike13.com/accounts/sign_in", wait_until="domcontentloaded")
        await page.wait_for_selector('input[placeholder="Email address"]', timeout=15000)
        await page.fill('input[placeholder="Email address"]', PIKE13_USER)
        await page.fill('input[placeholder="Password"]', PIKE13_PASS)
        await page.click('button:has-text("Sign In")')
        await page.wait_for_timeout(5000)
        # count iframes
        frames = page.frames
        print("top-level frames:", [f.name or f.url for f in frames])
        for f in frames:
            try:
                n = await f.locator('input.otp-digit').count()
                vis = False
                if n:
                    vis = await f.locator('input.otp-digit').first.is_visible()
                print(f"  frame {f.name or f.url[:60]}: otp-digit count={n} visible={vis}")
            except Exception as e:
                print(f"  frame err: {e}")
        # live cookie value
        cookies = await ctx.cookies()
        for c in cookies:
            if c['name']=='cwr_u':
                print(f"live cwr_u value_len={len(c.get('value',''))}")
        await ctx.close()
asyncio.run(main())
