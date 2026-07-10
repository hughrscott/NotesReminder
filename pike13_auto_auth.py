"""
Pike13 Auto-Auth: Login + MFA + Staff Session

Handles the full authentication flow:
1. Snapshot existing emails (to ignore stale codes)
2. Login with username/password
3. Detect MFA challenge (/account/two_factor)
4. Poll for NEW verification code from SOR email
5. Enter the 6-digit code into visible OTP fields
6. Return an authenticated browser context for scraping

Usage:
    from pike13_auto_auth import authenticate_pike13
    context = await authenticate_pike13(playwright, school_subdomain="westu-sor")
    page = context.pages[0]
    # Now authenticated as staff — scrape lesson notes
"""

import asyncio
import os
import re
import time
import imaplib
import email as email_lib
from datetime import datetime, timezone
from typing import Optional, Set

SOR_EMAIL = os.environ.get("SOR_EMAIL", "huscott@schoolofrock.com")
SOR_APP_PASSWORD = os.environ.get("SOR_APP_PASSWORD", "")
PIKE13_USER = os.environ.get("PIKE13_USER", "")
PIKE13_PASS = os.environ.get("PIKE13_PASSWORD", "")


def snapshot_inbox() -> Set[str]:
    """Return the set of existing message IDs so we can detect NEW emails."""
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
        mail.login(SOR_EMAIL, SOR_APP_PASSWORD)
        mail.select("INBOX")
        status, data = mail.search(None, "ALL")
        ids = set()
        if status == "OK" and data[0]:
            ids = {mid.decode() for mid in data[0].split()}
        mail.logout()
        print(f"  Inbox snapshot: {len(ids)} existing emails")
        return ids
    except Exception as e:
        print(f"  Inbox snapshot failed: {e}")
        return set()


async def read_mfa_code_via_imap(
    existing_ids: Set[str],
    timeout_s: int = 240,
    poll_interval: int = 5,
) -> Optional[str]:
    """
    Poll the SOR Gmail inbox for a NEW Pike13 verification code.
    Only looks at emails NOT in existing_ids (i.e. arrived after snapshot).
    Only matches emails whose subject indicates a verification code.
    """
    print(f"  Polling {SOR_EMAIL} for NEW MFA code (timeout {timeout_s}s)...")
    deadline = time.time() + timeout_s

    while time.time() < deadline:
        try:
            mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
            mail.login(SOR_EMAIL, SOR_APP_PASSWORD)
            mail.select("INBOX")

            status, data = mail.search(None, "ALL")
            if status != "OK" or not data[0]:
                mail.logout()
                await asyncio.sleep(poll_interval)
                continue

            all_ids = data[0].split()
            for msg_id in reversed(all_ids[-15:]):
                msg_id_str = msg_id.decode()
                if msg_id_str in existing_ids:
                    continue  # Skip pre-existing emails

                status, msg_data = mail.fetch(msg_id, "(RFC822)")
                if status != "OK" or not msg_data or not msg_data[0]:
                    continue

                raw = msg_data[0]
                if isinstance(raw, tuple) and len(raw) >= 2:
                    msg_bytes = raw[1]
                else:
                    continue

                msg = email_lib.message_from_bytes(msg_bytes)
                subject = msg.get("Subject", "")
                sender = str(msg.get("From", ""))

                # Must be from Pike13
                if "pike13" not in sender.lower():
                    continue

                # Must be specifically a verification code email
                subj_lower = subject.lower()
                is_code_email = (
                    "verification" in subj_lower
                    or "your code" in subj_lower
                    or "verify" in subj_lower
                )
                if not is_code_email:
                    continue

                # Extract body
                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        ct = part.get_content_type()
                        if ct == "text/plain":
                            payload = part.get_payload(decode=True)
                            if payload:
                                body += payload.decode("utf-8", errors="ignore")
                        elif ct == "text/html" and not body:
                            payload = part.get_payload(decode=True)
                            if payload:
                                body += payload.decode("utf-8", errors="ignore")
                else:
                    payload = msg.get_payload(decode=True)
                    if payload:
                        body = payload.decode("utf-8", errors="ignore")

                # Extract 6-digit code
                patterns = [
                    r"Your code:\s*(\d{6})",
                    r"verification code[:\s]*(\d{6})",
                    r"code[:\s]+(\d{6})",
                    r"^\s*(\d{6})\s*$",
                ]
                for pattern in patterns:
                    match = re.search(pattern, body, re.IGNORECASE | re.MULTILINE)
                    if match:
                        code = match.group(1)
                        print(f"  Found MFA code: {code} (subject: {subject[:60]})")
                        mail.logout()
                        return code

            mail.logout()
        except Exception as e:
            print(f"  IMAP error: {e}")

        await asyncio.sleep(poll_interval)

    print("  No NEW MFA code found within timeout")
    return None


async def enter_mfa_code(page, code: str) -> bool:
    """Enter the 6-digit MFA code into Pike13's OTP input fields."""
    print(f"  Entering MFA code: {code}")

    # Pike13 has two sets of OTP inputs:
    #   1. Hidden inputs with class="otp-digit" (0x0, not visible)
    #   2. Visible inputs with class="otp-digit email-otp-digit"
    # Target the VISIBLE ones. Button text is "Verify and Sign In".

    otp_inputs = page.locator('input.otp-digit.email-otp-digit')
    otp_count = await otp_inputs.count()
    print(f"  Visible OTP inputs found: {otp_count}")

    if otp_count >= 6:
        for i, digit in enumerate(code):
            await otp_inputs.nth(i).fill(digit, timeout=10000)
            await page.wait_for_timeout(100)

        await page.wait_for_timeout(1000)

        verify_btn = page.locator(
            'button:has-text("Verify"), input[type="submit"]:has-text("Verify")'
        )
        if await verify_btn.count() > 0:
            await verify_btn.first.click()
            print("  Clicked Verify and Sign In")
            await page.wait_for_timeout(5000)
            return True
        else:
            print("  No Verify button found")
            await page.wait_for_timeout(5000)
            return True

    print(f"  Not enough visible OTP inputs ({otp_count})")
    return False


async def authenticate_pike13(
    playwright,
    school_subdomain: str = "westu-sor",
    profile_dir: str = None,
    headless: bool = True,
    verbose: bool = True,
):
    """
    Full auto-auth flow: snapshot inbox -> login -> MFA -> staff session.
    Returns an authenticated browser context.
    Raises RuntimeError if authentication fails.
    """
    print(f"  Starting Pike13 auto-auth for {school_subdomain}.pike13.com...")

    # Step 0: Snapshot existing emails so we only pick up NEW MFA codes
    print("Step 0: Snapshotting inbox...")
    existing_email_ids = snapshot_inbox()

    context_options = {
        "viewport": {"width": 1920, "height": 1080},
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0 Safari/537.36",
    }

    if profile_dir:
        context = await playwright.chromium.launch_persistent_context(
            profile_dir,
            headless=headless,
            args=["--disable-dev-shm-usage"],
            **context_options,
        )
    else:
        browser = await playwright.chromium.launch(
            headless=headless,
            args=["--disable-dev-shm-usage"],
        )
        context = await browser.new_context(**context_options)

    page = context.pages[0] if context.pages else await context.new_page()

    login_url = f"https://{school_subdomain}.pike13.com/accounts/sign_in"

    # Step 1: Navigate to login page
    print("Step 1: Loading login page...")
    await page.goto(login_url, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_selector('input[placeholder="Email address"]', timeout=15000)

    # Step 2: Fill and submit login form
    print("Step 2: Filling login form...")
    await page.fill('input[placeholder="Email address"]', PIKE13_USER)
    await page.fill('input[placeholder="Password"]', PIKE13_PASS)
    await page.click('button:has-text("Sign In")')
    await page.wait_for_timeout(3000)

    current_url = page.url
    print(f"  After login: {current_url}")

    # Step 3: Handle MFA if required
    if "/account/two_factor" in current_url:
        print("Step 3: MFA required — waiting for new verification code email...")

        # Click "Resend code" to ensure a fresh email is sent
        resend_btn = page.locator('button:has-text("Resend")')
        if await resend_btn.count() > 0:
            print("  Clicking Resend code to ensure fresh email...")
            await resend_btn.first.click()
            await page.wait_for_timeout(2000)

        code = await read_mfa_code_via_imap(
            existing_ids=existing_email_ids,
            timeout_s=240,
            poll_interval=5,
        )
        if not code:
            raise RuntimeError("Could not read MFA code from email within timeout")

        success = await enter_mfa_code(page, code)
        if not success:
            raise RuntimeError("Failed to enter MFA code into the form")

        await page.wait_for_timeout(3000)
        current_url = page.url
        print(f"  After MFA: {current_url}")
    else:
        print("Step 3: No MFA required — proceeding")
        try:
            skip_btn = page.get_by_role("button", name=re.compile(r"skip for now", re.I))
            if await skip_btn.count() > 0 and await skip_btn.first.is_visible():
                await skip_btn.first.click()
                await page.wait_for_timeout(1500)
        except Exception:
            pass

    # Step 4: Verify authentication
    print("Step 4: Verifying authentication...")
    await page.wait_for_timeout(2000)
    current_url = page.url

    authenticated = (
        "sign_in" not in current_url
        and "two_factor" not in current_url
        and "login" not in current_url
    )

    if authenticated:
        print("  Authenticated successfully!")

        # Navigate to the staff schedule view
        schedule_staff_url = (
            f"https://{school_subdomain}.pike13.com/schedule#/list"
            f"?dt={datetime.now().strftime('%Y-%m-%d')}&lt=staff&el=1"
        )
        print(f"  Loading staff schedule...")
        await page.goto(schedule_staff_url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(5000)

        # Check for /e/ links (individual lessons = staff view)
        lesson_links = await page.evaluate(
            "() => Array.from(document.querySelectorAll(\"a[href*='/e/']\")).map(a => a.getAttribute('href')).filter(Boolean)"
        )

        if lesson_links:
            print(f"  Staff view confirmed — {len(lesson_links)} lesson links found")
        else:
            print(f"  No /e/ links found — may be client view. URL: {page.url}")
            events = page.locator(".calendar-lane .event, .list-event, [event_popup]")
            event_count = await events.count()
            print(f"  Found {event_count} event blocks")

        return context
    else:
        print(f"  Authentication failed. Current URL: {current_url}")
        raise RuntimeError(f"Pike13 authentication failed — stuck at {current_url}")


async def test_auth():
    """Test the full auto-auth flow end-to-end."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        try:
            context = await authenticate_pike13(
                p, school_subdomain="westu-sor", verbose=True
            )

            # Test: navigate to a specific lesson's notes page
            page = context.pages[0]
            test_lesson_url = (
                "https://westu-sor.pike13.com/desk/e/295349611/notes"
                "?return_to=%2Fschedule"
            )
            print(f"\nTesting lesson notes access: {test_lesson_url}")
            await page.goto(test_lesson_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)

            print(f"  URL: {page.url}")
            print(f"  Title: {await page.title()}")

            body = await page.locator("body").inner_text(timeout=5000)
            print(f"  Body (first 500 chars): {body[:500]}")

            print("\n  Full auth + lesson notes access confirmed!")
            await context.close()
        except Exception as e:
            print(f"\n  Test failed: {e}")
            raise


if __name__ == "__main__":
    asyncio.run(test_auth())