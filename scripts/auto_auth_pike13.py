#!/usr/bin/env python3
"""Fully automated Pike13 authentication using email-based MFA."""
import asyncio
import os
import re
import subprocess
import time
from pathlib import Path
from playwright.async_api import async_playwright

PIKE13_USER = os.environ.get("PIKE13_USER", "")
PIKE13_PASS = os.environ.get("PIKE13_PASSWORD", "")


def read_mfa_code_from_himalaya(timeout_s=120, exclude_codes=None):
    """Read the latest verification code from SOR email. Optionally exclude stale codes."""
    print("  Reading MFA code from huscott@schoolofrock.com via Himalaya...")
    exclude = exclude_codes or set()
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            result = subprocess.run(
                ["himalaya", "envelope", "list", "-a", "sor", "--page-size", "5"],
                capture_output=True, text=True, timeout=30
            )
            for line in result.stdout.split("\n"):
                if "verification code" in line.lower() or "Your code" in line:
                    parts = line.split("|")
                    if len(parts) >= 3:
                        email_id = parts[1].strip()
                        if email_id and email_id.isdigit():
                            read_result = subprocess.run(
                                ["himalaya", "message", "read", "-a", "sor", email_id],
                                capture_output=True, text=True, timeout=30
                            )
                            match = re.search(r'Your code:\s*(\d{6})', read_result.stdout)
                            if match:
                                code = match.group(1)
                                if code not in exclude:
                                    print(f"  Found MFA code: {code}")
                                    return code
        except Exception as e:
            print(f"  Error: {e}")
        time.sleep(5)
    print("  No MFA code found in timeout")
    return None


def get_existing_codes():
    """Get all existing verification codes so we can exclude them."""
    codes = set()
    try:
        result = subprocess.run(
            ["himalaya", "envelope", "list", "-a", "sor", "--page-size", "5"],
            capture_output=True, text=True, timeout=30
        )
        for line in result.stdout.split("\n"):
            if "verification code" in line.lower():
                parts = line.split("|")
                if len(parts) >= 3:
                    email_id = parts[1].strip()
                    if email_id and email_id.isdigit():
                        read_result = subprocess.run(
                            ["himalaya", "message", "read", "-a", "sor", email_id],
                            capture_output=True, text=True, timeout=30
                        )
                        match = re.search(r'Your code:\s*(\d{6})', read_result.stdout)
                        if match:
                            codes.add(match.group(1))
    except:
        pass
    return codes


async def auto_authenticate(school="westu-sor", verbose=True):
    """Login to Pike13, read MFA from email, get authenticated session."""
    login_url = f"https://{school}.pike13.com/accounts/sign_in"
    os.makedirs("screenshots", exist_ok=True)

    # Get existing codes to exclude stale ones
    existing_codes = get_existing_codes()
    if verbose:
        print(f"Existing codes to exclude: {existing_codes}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        )
        page = await context.new_page()

        # Step 1: Login
        if verbose:
            print(f"Logging in to {school}.pike13.com...")
        await page.goto(login_url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_selector('input[placeholder="Email address"]', timeout=15000)
        await page.fill('input[placeholder="Email address"]', PIKE13_USER)
        await page.fill('input[placeholder="Password"]', PIKE13_PASS)
        await page.click('button:has-text("Sign In")')
        await page.wait_for_timeout(5000)
        if verbose:
            print(f"After login: {page.url}")

        # Step 2: Handle MFA
        if "/account/two_factor" in page.url:
            if verbose:
                print("MFA required")

            # Check if we need to resend code
            body_text = await page.locator("body").inner_text(timeout=5000)
            if "No code has been sent" in body_text:
                if verbose:
                    print("No code sent. Clicking Resend...")
                resend = page.locator('a:has-text("Resend"), button:has-text("Resend")')
                if await resend.count() > 0:
                    await resend.first.evaluate("el => el.click()")
                    await page.wait_for_timeout(5000)
            else:
                # Click Resend anyway to get a fresh code
                resend = page.locator('a:has-text("Resend"), button:has-text("Resend")')
                if await resend.count() > 0:
                    if verbose:
                        print("Clicking Resend for fresh code...")
                    await resend.first.evaluate("el => el.click()")
                    await page.wait_for_timeout(3000)

            # Wait for fresh email
            code = read_mfa_code_from_himalaya(120, exclude_codes=existing_codes)
            if not code:
                if verbose:
                    print("Failed to read fresh MFA code")
                await browser.close()
                return None

            # Enter code via JS
            if verbose:
                print(f"Entering code: {code}")
            success = await page.evaluate("""(code) => {
                const inputs = document.querySelectorAll('input.otp-digit');
                if (inputs.length >= 6) {
                    for (let i = 0; i < 6; i++) {
                        const input = inputs[i];
                        const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                        setter.call(input, code[i]);
                        input.dispatchEvent(new Event('input', {bubbles: true}));
                        input.dispatchEvent(new Event('change', {bubbles: true}));
                        input.dispatchEvent(new KeyboardEvent('keyup', {bubbles: true, key: code[i]}));
                    }
                    return true;
                }
                return false;
            }""", code)
            if verbose:
                print(f"OTP set: {success}")
            await page.wait_for_timeout(2000)

            # Submit form
            submit_result = await page.evaluate("""() => {
                const form = document.querySelector('form');
                if (form) {
                    if (form.requestSubmit) form.requestSubmit();
                    else form.submit();
                    return 'submitted';
                }
                const btn = document.querySelector('button[type="submit"]');
                if (btn) { btn.click(); return 'clicked'; }
                return 'none';
            }""")
            if verbose:
                print(f"Submit: {submit_result}")
            await page.wait_for_timeout(5000)
            if verbose:
                print(f"After submit: {page.url}")

        # Step 3: Check authentication
        current = page.url
        if "sign_in" not in current and "two_factor" not in current and "welcome" not in current:
            if verbose:
                print("✅ AUTHENTICATED!")

            # Test staff access
            await page.goto(f"https://{school}.pike13.com/e/295349611?return_to=/schedule",
                          wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)

            if "welcome" in page.url or "accounts/sign_in" in page.url:
                if verbose:
                    print("⚠️ Staff access not granted")
                await browser.close()
                return None

            if verbose:
                print(f"✅ Staff access confirmed! Lesson page: {page.url}")
                body = await page.locator("body").inner_text(timeout=5000)
                print(f"Page content (first 300): {body[:300]}")

            return context, page, browser

        if verbose:
            print(f"❌ Not authenticated. URL: {current}")
            await page.screenshot(path="screenshots/auth_failed.png")

        await browser.close()
        return None


if __name__ == "__main__":
    result = asyncio.run(auto_authenticate())
    if result:
        context, page, browser = result
        input("Press Enter to close...")
        browser.close()