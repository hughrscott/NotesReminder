#!/usr/bin/env python3
"""Bootstrap Dialpad via Google Workspace SSO (same creds as Okta/HubSpot).
Clicks 'Log in with Google' on dialpad.com/login, fills Google email+password,
waits for redirect to dialpad.com/app, saves dialpad_storage.json."""
import asyncio, json, sys, time, os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

# ── credentials ────────────────────────────────────────────────────────────
from okta_auth.config import get_config
cfg = get_config()
GOOGLE_EMAIL = cfg.get("GOOGLE_EMAIL") or os.getenv("HUBSPOT_GOOGLE_EMAIL") or cfg.get("OKTA_USERNAME")
GOOGLE_PASSWORD = cfg.get("GOOGLE_PASSWORD") or os.getenv("HUBSPOT_GOOGLE_PASSWORD") or cfg.get("OKTA_PASSWORD")

if not GOOGLE_EMAIL or not GOOGLE_PASSWORD:
    # Fallback: read .sorenv directly
    sor_env = Path(os.path.expanduser("~/.hermes/SOR/.sorenv"))
    if sor_env.exists():
        for line in sor_env.read_text().splitlines():
            if line.startswith("OKTA_USERNAME="):
                GOOGLE_EMAIL = line.split("=", 1)[1].strip()
            elif line.startswith("OKTA_PASSWORD="):
                GOOGLE_PASSWORD = line.split("=", 1)[1].strip()
            elif line.startswith("HUBSPOT_GOOGLE_EMAIL="):
                if not GOOGLE_EMAIL:
                    GOOGLE_EMAIL = line.split("=", 1)[1].strip()
            elif line.startswith("HUBSPOT_GOOGLE_PASSWORD="):
                if not GOOGLE_PASSWORD:
                    GOOGLE_PASSWORD = line.split("=", 1)[1].strip()

print(f"  Google email: {GOOGLE_EMAIL}")
print(f"  Google password: {'***' if GOOGLE_PASSWORD else 'MISSING'}")

assert GOOGLE_EMAIL and GOOGLE_PASSWORD, "Missing Google credentials"

from playwright.async_api import async_playwright

PROFILE = str(Path(cfg.get("SHARED_PROFILE", "browser_profiles/sor_shared")).resolve())
STORAGE_JSON = ROOT / "browser_profiles" / "dialpad_storage.json"
FULL_STORAGE = ROOT / "browser_profiles" / "sor_shared_storage.json"

async def main():
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            PROFILE, headless=True,
            viewport={"width": 1920, "height": 1080},
            args=["--disable-dev-shm-usage"],
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        # 1. Go to Dialpad login page
        print("1) goto dialpad.com/app -> login page")
        await page.goto("https://dialpad.com/app", timeout=15000)
        await page.wait_for_timeout(4000)
        print(f"   URL: {page.url}")

        if "dialpad.com/app/" in page.url and "/login" not in page.url:
            print("   ALREADY AUTHENTICATED! (somehow)")
            state = await ctx.storage_state()
            STORAGE_JSON.parent.mkdir(parents=True, exist_ok=True)
            json.dump(state, open(STORAGE_JSON, "w"))
            json.dump(state, open(FULL_STORAGE, "w"))
            print("   dialpad_storage.json saved")
            return

        # 2. Click "Log in with Google"
        print("2) clicking 'Log in with Google'...")
        try:
            await page.click('text=Log in with Google', timeout=5000)
        except Exception as e:
            print(f"   click failed: {e}")
            # try button
            try:
                await page.locator('button:has-text("Log in with Google")').first.click(timeout=5000)
            except Exception as e2:
                print(f"   button click failed: {e2}")
        await page.wait_for_timeout(5000)
        print(f"   URL: {page.url[:120]}")

        # 3. Google sign-in: fill email
        if "accounts.google.com" in page.url:
            print("3) on Google sign-in page")
            # Try multiple selectors (Google v3 uses #identifierId)
            for sel in ('#identifierId', 'input[name="identifier"]', 'input[type="email"]'):
                try:
                    email_input = page.locator(sel)
                    if await email_input.count() > 0:
                        await email_input.first.wait_for(state="visible", timeout=5000)
                        await email_input.first.fill(GOOGLE_EMAIL)
                        print(f"   filled email via '{sel}': {GOOGLE_EMAIL}")
                        break
                except Exception:
                    continue
            else:
                # Dump body for debugging
                print(f"   ❌ couldn't find email input. Page body:")
                body_text = await page.locator("body").inner_text()
                print(body_text[:500])
                await ctx.close()
                return
            try:
                await page.click('text=Next', timeout=5000)
            except Exception:
                await page.locator('#identifierNext').first.click(timeout=5000)
            await page.wait_for_timeout(4000)
            print(f"   after email submit: {page.url[:120]}")

            # 4. Fill password
            try:
                pw_input = page.locator('input[type="password"]')
                await pw_input.wait_for(state="visible", timeout=10000)
                await pw_input.fill(GOOGLE_PASSWORD)
                print("   filled password")
                await page.click('text=Next', timeout=5000)
                await page.wait_for_timeout(5000)
                print(f"   after password: {page.url[:120]}")
            except Exception as e:
                print(f"   password fill error: {e}")
                # Maybe 2FA or Google consent screen
                body = await page.locator("body").inner_text()
                print(f"   page body: {body[:300]}")

        # 5. Wait for redirect to dialpad.com/app
        print("5) waiting for Dialpad app...")
        for _ in range(60):  # up to 120s
            await asyncio.sleep(2)
            all_urls = [p.url for p in ctx.pages]
            for url in all_urls:
                if "dialpad.com/app" in url and "/login" not in url:
                    print(f"   ✅ Dialpad reached: {url[:100]}")
                    state = await ctx.storage_state()
                    STORAGE_JSON.parent.mkdir(parents=True, exist_ok=True)
                    json.dump(state, open(STORAGE_JSON, "w"))
                    json.dump(state, open(FULL_STORAGE, "w"))
                    print(f"   dialpad_storage.json saved ({len(state.get('cookies',[]))} cookies)")
                    result = {"ok": True, "url": url}
                    Path("bootstrap_dialpad_result.json").write_text(json.dumps(result))
                    return
            # Also check current page
            if "dialpad.com/app" in page.url and "/login" not in page.url:
                print(f"   ✅ Dialpad reached (current page): {page.url[:100]}")
                state = await ctx.storage_state()
                STORAGE_JSON.parent.mkdir(parents=True, exist_ok=True)
                json.dump(state, open(STORAGE_JSON, "w"))
                json.dump(state, open(FULL_STORAGE, "w"))
                print(f"   dialpad_storage.json saved ({len(state.get('cookies',[]))} cookies)")
                result = {"ok": True, "url": page.url}
                Path("bootstrap_dialpad_result.json").write_text(json.dumps(result))
                return

        # Timeout
        print("   ❌ Dialpad NOT reached")
        result = {"ok": False, "urls": [p.url for p in ctx.pages]}
        Path("bootstrap_dialpad_result.json").write_text(json.dumps(result))
        for p in ctx.pages:
            print(f"   Page: {p.url[:150]}")

asyncio.run(main())
