#!/usr/bin/env python3
"""Probe sor.okta.com for available MFA factors. READ-ONLY intent: submit username+password to reach the
factor-selection screen, read which factors are offered, then STOP. Never approves/selects a factor.
Side effect: Okta may auto-send one push to the enrolled device; just decline it. No approval happens here.
"""
import asyncio
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from playwright.async_api import async_playwright

OKTA_USER = os.getenv("OKTA_USER") or os.getenv("OKTA_USERNAME") or os.getenv("SOR_OKTA_USERNAME")
OKTA_PASS = os.getenv("OKTA_PASSWORD") or os.getenv("SOR_OKTA_PASSWORD")


def scan(text, label):
    lowered = (text or "").lower()
    hints = ["okta verify", "push", "sms", "text message", "phone", "call", "authenticator",
             "google authenticator", "totp", "security key", "security token", "email",
             "factor", "verification method", "different method", "other options",
             "duo", "yubikey", "webauthn", "biometric", "choose another"]
    hits = sorted({h for h in hints if h in lowered})
    print(f"\n===== {label} =====")
    print(f"[len={len(text)}] factor hints: {hits if hits else 'NONE'}")
    for line in (text or "").splitlines():
        ls = line.strip()
        if ls and any(h in ls.lower() for h in hints):
            print("   >", ls[:160])


async def main():
    if not (OKTA_USER and OKTA_PASS):
        print("MISSING OKTA_USER or OKTA_PASSWORD — cannot reach factor screen. Aborting.")
        return
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])
        context = await browser.new_context(viewport={"width": 1280, "height": 900})
        page = await context.new_page()
        page.set_default_timeout(15000)

        print(">>> Load login ...")
        await page.goto("https://sor.okta.com", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2500)

        uname = page.locator('input[name="username"], input#okta-signin-username, input[type="text"]').first
        await uname.wait_for(timeout=8000)
        await uname.fill(OKTA_USER)
        btn = page.get_by_role("button", name=re.compile(r"next|sign in|submit", re.I)).first
        await btn.click(timeout=8000)
        await page.wait_for_timeout(2500)

        pw = page.locator('input[name="password"], input#okta-signin-password, input[type="password"]').first
        await pw.wait_for(timeout=8000)
        await pw.fill(OKTA_PASS)
        print(">>> Submitted password (reaches factor screen; will NOT approve anything) ...")
        signin = page.get_by_role("button", name=re.compile(r"sign in|verify|continue", re.I)).first
        await signin.click(timeout=8000)
        await page.wait_for_timeout(4000)

        try:
            after_pw = await page.locator("body").inner_text(timeout=8000)
        except Exception:
            after_pw = ""
        scan(after_pw, "FACTOR SELECTION SCREEN (post-password)")
        print("URL:", page.url)

        # Reveal alternative methods if a chooser exists
        for sel in [
            'text=Sign in using a different method',
            'text=Other options',
            'text=Choose another method',
            'a:has-text("different method")',
            'button:has-text("different method")',
            'text=Another method',
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() and await el.is_visible(timeout=2000):
                    print(f"\n>>> Clicking factor chooser: {sel}")
                    await el.click(timeout=5000)
                    await page.wait_for_timeout(2500)
                    revealed = await page.locator("body").inner_text(timeout=8000)
                    scan(revealed, "REVEALED FACTOR LIST")
                    break
            except Exception:
                continue

        print("\n===== FINAL PAGE TEXT (clean) =====")
        try:
            final = await page.locator("body").inner_text(timeout=8000)
        except Exception:
            final = ""
        print("\n".join(l.strip() for l in final.splitlines() if l.strip())[:1800])
        print("URL:", page.url)
        await context.close()
    print("\n>>> PROBE COMPLETE — no factor selected/approved, session not authenticated.")


asyncio.run(main())
