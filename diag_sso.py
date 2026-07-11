#!/usr/bin/env python3
"""Read-only diagnostic: for each service, navigate from the warm sor_shared
profile and report auth state. SENDS NO PUSHES — just observes."""
import asyncio, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from okta_auth.scraper_session import launch_okta_context

TARGETS = [
    ("Pike13", "https://westu-sor.pike13.com/schedule"),
    ("HubSpot", "https://app.hubspot.com/contacts"),
    ("Dialpad/Backstage", "https://dialpad.com"),
]

async def main():
    async with launch_okta_context() as ctx:
        p = await ctx.new_page()
        for name, url in TARGETS:
            await p.goto(url, wait_until="domcontentloaded")
            await asyncio.sleep(6)
            u = p.url
            lowered = u.lower()
            if "login" in lowered or "saml" in lowered or "okta" in lowered and "userhome" not in lowered:
                state = "NEEDS AUTH (redirected to login/Okta)"
            elif "401" in u or "authfailure" in lowered:
                state = "NEEDS AUTH (401)"
            else:
                state = "AUTHENTICATED (landed on app)"
            print(f"{name:20s} -> {state}\n    final URL: {u}")
            # if on Okta login, note it
            if "okta.com" in lowered and "userhome" not in lowered:
                print("    (on Okta login page — a real push would be needed to proceed)")

asyncio.run(main())
