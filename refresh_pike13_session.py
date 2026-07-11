#!/usr/bin/env python3
"""Full Pike13 re-auth into browser_profiles/sor_shared: email+password + email 2FA
(code from Himalaya 'sor' account), then save a POST-2FA storage_state JSON so the
backfill's add_cookies seed is a COMPLETE session. No iPhone push (email 2FA)."""
import asyncio, json, re, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path('.').resolve()))
sys.path.insert(0, str(Path('.').resolve()/'scripts'))
from dotenv import load_dotenv
load_dotenv(Path.home()/'.hermes'/'SOR'/'.sorenv')
import os
from playwright.async_api import async_playwright


SCHOOLS = ["westu-sor", "theheights-sor"]
PROFILE = Path("browser_profiles/sor_shared")
STORAGE_JSON = Path("browser_profiles/sor_shared_storage.json")
PIKE13_USER = os.environ.get("PIKE13_USER", "huscott@schoolofrock.com")
PIKE13_PASS = os.environ.get("PIKE13_PASSWORD", "")

def parse_fresh_verification_envelopes(raw, requested_at):
    """Return verification-message rows at/after the login request timestamp.

    Himalaya's table only exposes minute precision, so compare against the
    request minute rather than excluding previously-seen codes. Pike13 can
    legitimately reuse the same code for several fresh requests.
    """
    cutoff = requested_at.astimezone(timezone.utc).replace(second=0, microsecond=0)
    rows = []
    for line in raw.splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 5 or not cells[0].isdigit():
            continue
        if "verification code" not in cells[2].lower():
            continue
        try:
            received_at = datetime.fromisoformat(cells[4].replace("Z", "+00:00"))
        except ValueError:
            continue
        if received_at.astimezone(timezone.utc) >= cutoff:
            rows.append((cells[0], received_at))
    return sorted(rows, key=lambda row: row[1], reverse=True)


def extract_verification_code(body):
    match = re.search(r"Your code:\s*(\d{6})", body, re.I)
    return match.group(1) if match else None


def read_mfa_sor(requested_at, timeout_s=180):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            result = subprocess.run(
                ["himalaya", "envelope", "list", "-a", "sor", "--page-size", "8"],
                capture_output=True, text=True, timeout=30,
            )
            for message_id, _ in parse_fresh_verification_envelopes(result.stdout, requested_at):
                body = subprocess.run(
                    ["himalaya", "message", "read", "-a", "sor", message_id],
                    capture_output=True, text=True, timeout=30,
                ).stdout
                code = extract_verification_code(body)
                if code:
                    return code
        except Exception as exc:
            print(f"  MFA email read error: {exc}")
        time.sleep(5)
    return None

async def refresh_one(ctx, school):
    page = await ctx.new_page()
    url = f"https://{school}.pike13.com/accounts/sign_in"
    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_timeout(4000)
    # Pike13 shares ONE session across both SOR schools. If westu already
    # authenticated, this school redirects past sign_in -> already logged in.
    if "/accounts/sign_in" not in page.url and "two_factor" not in page.url:
        print(f"[{school}] already authenticated (shared session): {page.url}")
        await page.close()
        return True
    await page.wait_for_selector('input[placeholder="Email address"]', timeout=15000)
    await page.fill('input[placeholder="Email address"]', PIKE13_USER)
    await page.fill('input[placeholder="Password"]', PIKE13_PASS)
    requested_at = datetime.now(timezone.utc)
    await page.click('button:has-text("Sign In")')
    await page.wait_for_timeout(4000)
    if "/account/two_factor" in page.url or "two_factor" in page.url:
        print(f"[{school}] 2FA required -> read code requested by this login")
        code = read_mfa_sor(requested_at=requested_at, timeout_s=30)
        if not code:
            print(f"[{school}] login email absent -> request one resend")
            requested_at = datetime.now(timezone.utc)
            try:
                await page.click('button:has-text("Resend")', timeout=8000)
            except Exception as exc:
                print(f"[{school}] resend control unavailable: {exc}")
            code = read_mfa_sor(requested_at=requested_at, timeout_s=150)
        if not code:
            print(f"[{school}] NO 2FA CODE FOUND"); await page.close(); return False
        print(f"[{school}] got a fresh verification code")
        # React-aware OTP entry. Use the visible email OTP controls, dispatch
        # input/change events, then click the Angular submit button. Calling
        # form.submit()/requestSubmit() bypasses Pike13's handler.
        await page.evaluate("""(code) => {
            let inputs = document.querySelectorAll('input.otp-digit.email-otp-digit');
            if (inputs.length < 6) inputs = document.querySelectorAll('input.otp-digit:not([type="hidden"])');
            if (inputs.length >= 6) {
                for (let i=0;i<6;i++){
                    const el = inputs[i];
                    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype,'value').set;
                    setter.call(el, code[i]);
                    el.dispatchEvent(new Event('input', {bubbles:true}));
                    el.dispatchEvent(new Event('change', {bubbles:true}));
                }
            } else {
                const single = document.querySelector('input[name="code"], input[type="text"][inputmode="numeric"]');
                if (single){ const s=Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype,'value').set; s.call(single,code); single.dispatchEvent(new Event('input',{bubbles:true})); }
            }
        }""", code)
        await page.locator('button:has-text("Verify and Sign In"), button:has-text("Verify")').first.click(timeout=10000)
        await page.wait_for_timeout(6000)
    loggedin = "/account/two_factor" not in page.url and "sign_in" not in page.url and "two_factor" not in page.url
    print(f"[{school}] loggedin={loggedin} url={page.url}")
    await page.close()
    return loggedin

async def main():
    ok_all = True
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            str(PROFILE), headless=True, viewport={"width":1920,"height":1080},
            args=["--disable-dev-shm-usage"])
        for school in SCHOOLS:
            ok = await refresh_one(ctx, school)
            ok_all = ok_all and ok
            if not ok:
                # Pike13 rate-limits MFA sends. Avoid a second login attempt,
                # and preserve the last known-good storage state.
                break
        if ok_all:
            temp_state = STORAGE_JSON.with_suffix(".json.tmp")
            await ctx.storage_state(path=str(temp_state))
            temp_state.replace(STORAGE_JSON)
            print(f"storage_state saved -> {STORAGE_JSON}")
        else:
            print("storage_state NOT replaced; refresh failed")
        await ctx.close()
    print("REFRESH", "ALL OK" if ok_all else "HAD FAILURES")
    Path("refresh_pike13_result.json").write_text(json.dumps({"ok": ok_all}))
    return ok_all

if __name__ == "__main__":
    raise SystemExit(0 if asyncio.run(main()) else 1)
