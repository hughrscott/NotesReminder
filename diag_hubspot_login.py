#!/usr/bin/env python3
"""Diagnostic: after email + SSO, what exactly is the Okta page state?
Logs field values, buttons, and whether a push is pending or already authed.
No password forced; just observes."""
import asyncio, sys, time
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
        # observe okta page for up to 30s
        for i in range(10):
            await asyncio.sleep(3)
            u = p.url
            print(f"[{i}] URL: {u}")
            # dump key page info
            try:
                info = await p.evaluate("""() => {
                  const out = {};
                  const pw = document.querySelector('input[type=password]');
                  out.pw_filled = pw ? (pw.value.length>0) : null;
                  const user = document.querySelector('input[type=email], input[name=username], #okta-signin-username');
                  out.user_val = user ? user.value : null;
                  const btns = [...document.querySelectorAll('button')].map(b=>b.innerText.trim()).filter(Boolean);
                  out.buttons = btns.slice(0,12);
                  out.body_has = {
                    'push': /push/i.test(document.body.innerText),
                    'approve': /approv/i.test(document.body.innerText),
                    'verify': /verify/i.test(document.body.innerText),
                    'signed in': /signed in|success|redirect/i.test(document.body.innerText),
                    'hubspot': /hubspot/i.test(document.body.innerText),
                  };
                  return out;
                }""")
                print(f"    pw_filled={info['pw_filled']} user={info['user_val']}")
                print(f"    buttons={info['buttons']}")
                print(f"    flags={info['body_has']}")
            except Exception as e:
                print("   eval err", e)
            if "hubspot.com" in u and "login" not in u.lower():
                print(">>> LANDED ON HUBSPOT (auto-auth worked)")
                break
            if "okta.com" not in u.lower() and "hubspot" not in u.lower():
                print(">>> elsewhere:", u); break

asyncio.run(main())
