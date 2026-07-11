#!/usr/bin/env python3
"""Bootstrap service sessions into the shared Okta profile.

For each source we open the (already-warm) Okta session, click that service's
SSO tile in Okta UserHome (which dispatches an Okta Verify push to Hugh's
iPhone), notify via Telegram, wait for approval, then verify the service
session cookie landed in browser_profiles/sor_shared.

After all sources bootstrap, the unattended backfill can run.

Run: python bootstrap_sessions.py
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from okta_auth.scraper_session import launch_okta_context
from okta_auth.config import get_config
from okta_auth.telegram_bot import _notify

CONFIG = get_config()

# (key, tile label keyword, expected service domain substring)
SOURCES = [
    ("pike13", "Pike13", "pike13.com"),
    ("hubspot", "HubSpot", "hubspot.com"),
    ("dialpad", "Backstage", "dialpad.com"),
    # school_email is IMAP/Himalaya, NOT Okta SSO — handled separately, no push.
]

PUSH_WAIT_S = int(CONFIG.get("BOOTSTRAP_PUSH_WAIT_S", "360"))


async def discover_tiles(page):
    """Return list of app tile labels visible on Okta UserHome."""
    labels = []
    try:
        locs = page.locator("a, div[role='link'], span.app-label, .app-title")
        n = min(await locs.count(), 60)
        for i in range(n):
            t = (await locs.nth(i).inner_text()).strip()
            if t:
                labels.append(t)
    except Exception as e:
        print(f"  tile discovery err: {e}")
    # de-dup preserve order
    seen, out = set(), []
    for l in labels:
        if l not in seen:
            seen.add(l); out.append(l)
    return out


async def bootstrap_one(page, key, keyword, domain):
    print(f"\n=== bootstrap: {key} (tile~'{keyword}', domain~'{domain}') ===")
    await page.goto("https://sor.okta.com/app/UserHome", wait_until="domcontentloaded")
    await asyncio.sleep(4)
    tiles = await discover_tiles(page)
    print("  available tiles:", tiles[:25])
    # find tile matching keyword
    target = None
    for t in tiles:
        if keyword.lower() in t.lower():
            target = t; break
    if not target:
        print(f"  !! no tile matching '{keyword}' — skipping {key}")
        return False, f"no Okta tile for {keyword}"
    print(f"  clicking tile: '{target}'")
    try:
        await page.locator(f"a:has-text('{target}')").first.click(timeout=15000)
    except Exception as e:
        # try a broader click
        try:
            await page.get_by_text(target, exact=False).first.click(timeout=15000)
        except Exception as e2:
            return False, f"click failed: {e2}"
    # After click, either SSO redirects (needs push approval) or a verify/push
    # screen appears. If a 'Send push'/'Verify' control shows, click it.
    await asyncio.sleep(3)
    for sel in ["button:has-text('Send push')", "button:has-text('Verify')",
                "button:has-text('Send Notification')", "input[value*='push' i]"]:
        try:
            el = page.locator(sel).first
            if await el.count() and await el.is_visible():
                print(f"  clicking push control: {sel}")
                await el.click()
                break
        except Exception:
            pass
    # Notify Hugh
    await _notify(f"📲 Okta push sent for **{key}** — approve on your iPhone to cache the session.")
    # Wait for the service domain to appear (approval completed SSO)
    print(f"  waiting up to {PUSH_WAIT_S}s for approval (service domain '{domain}')...")
    t0 = time.time()
    while time.time() - t0 < PUSH_WAIT_S:
        await asyncio.sleep(5)
        u = page.url
        if domain.lower() in u.lower():
            print(f"  ✅ redirected to service: {u}")
            # give SSO a moment to settle + write cookies
            await asyncio.sleep(4)
            return True, u
        # detect a fresh push prompt re-appearing (not approved)
    # timeout
    u = page.url
    return False, f"timeout waiting for {domain} (last url {u})"


async def main():
    results = {}
    async with launch_okta_context() as ctx:
        page = await ctx.new_page()
        for key, kw, dom in SOURCES:
            ok, detail = await bootstrap_one(page, key, kw, dom)
            results[key] = {"ok": ok, "detail": detail}
            print(f"  >>> {key}: {'OK' if ok else 'FAIL'} — {detail}")
            if not ok:
                await _notify(f"⚠️ Bootstrap FAILED for {key}: {detail}")
            # small gap between sources
            await asyncio.sleep(3)
        # verify cookies persisted for each
        cookies = await ctx.cookies()
        by_dom = {}
        for c in cookies:
            d = c.get("domain", "")
            by_dom.setdefault(d, []).append(c.get("name", ""))
        print("\n=== cookies in shared profile by domain ===")
        for d, names in sorted(by_dom.items()):
            print(f"  {d}: {len(names)} cookies")
    # summary
    okc = sum(1 for v in results.values() if v["ok"])
    print(f"\nBOOTSTRAP DONE: {okc}/{len(SOURCES)} sources cached")
    await _notify(f"🔐 Bootstrap complete: {okc}/{len(SOURCES)} service sessions cached. "
                  f"{'Launching backfill.' if okc == len(SOURCES) else 'Some failed — check logs.'}")
    # write result for the launcher to read
    Path("bootstrap_result.json").write_text(json.dumps(results, indent=2))
    return results


if __name__ == "__main__":
    asyncio.run(main())
