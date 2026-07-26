#!/usr/bin/env python3
"""
Shared Okta SSO authentication for NotesReminder source extractors.

Consolidates the Okta login / MFA-push logic that was previously
duplicated across extract_school_emails.py, extract_dialpad_voice.py,
and extract_dialpad_sms.py.

Design:
- The Okta Verify push is ONLY triggered after Hugh has explicitly
  approved via Telegram. This avoids surprise pushes at 2am that time out.
- `run_okta_mfa_with_gate()` is the single entry point: it sends a
  Telegram message, waits for Hugh's "go"/"ready"/"start" reply, then
  fills credentials and waits for the push approval.
- Headless cron runs and interactive runs both go through the same gate.

The Telegram gate utility (`wait_for_telegram_approval`) lives in the
project-root pike13_auto_auth.py so it can be shared with the payroll
automation project too.
"""
import os
import re
import sys
import time
import asyncio

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

# Telegram gate lives in the project root module (shared with sor-payroll-automation)
try:
    from pike13_auto_auth import wait_for_telegram_approval
except ImportError:
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from pike13_auto_auth import wait_for_telegram_approval


# ── Credential helpers ──────────────────────────────────────────────────────

def okta_username():
    return os.getenv("OKTA_USERNAME") or os.getenv("SOR_OKTA_USERNAME") or os.getenv("OKTA_USER")


def okta_password():
    return os.getenv("OKTA_PASSWORD") or os.getenv("SOR_OKTA_PASSWORD")


def okta_credentials_available():
    return bool(okta_username() and okta_password())


def is_okta_login_url(url):
    lowered = (url or "").lower()
    return "sor.okta.com" in lowered and ("login" in lowered or "signin" in lowered)


# ── Login form fill ──────────────────────────────────────────────────────────

def fill_okta_login(page):
    """Fill the Okta username/password form and click Sign In.

    Returns True if the form was filled, False if credentials were missing.
    Clicking Sign In is what triggers the Okta Verify push.
    """
    username = okta_username()
    password = okta_password()
    if not username or not password:
        return False
    username_input = page.locator('input[name="username"], input#okta-signin-username, input[type="text"]').first
    password_input = page.locator('input[name="password"], input#okta-signin-password, input[type="password"]').first
    username_input.wait_for(timeout=15000)
    username_input.fill(username)
    password_input.fill(password)
    remember_me = page.locator('input[type="checkbox"][name="remember"], input[type="checkbox"]')
    if remember_me.count():
        try:
            if not remember_me.first.is_checked():
                remember_me.first.check(timeout=3000)
        except Exception:
            pass
    page.get_by_role("button", name=re.compile(r"sign in", re.IGNORECASE)).click(timeout=10000)
    return True


# ── Wait for push approval ───────────────────────────────────────────────────

def wait_for_okta_push(page, timeout_seconds):
    """Poll for the Okta Verify push to be approved.

    Assumes credentials were already submitted (which fired the push).
    """
    deadline = time.time() + timeout_seconds
    notified = False
    while time.time() < deadline:
        lowered_url = page.url.lower()
        try:
            body = page.locator("body").inner_text(timeout=5000).lower()
        except PlaywrightTimeoutError:
            body = ""
        if "mail.google.com" in lowered_url and "signin" not in lowered_url:
            return
        if not notified and ("push sent" in body or "okta verify" in body):
            print("Okta Verify push sent by NotesReminder. Please approve it on your phone.", flush=True)
            notified = True
        time.sleep(2)
    raise RuntimeError("Timed out waiting for Okta Verify approval.")


# ── Telegram-gated MFA entry point ───────────────────────────────────────────

def request_okta_mfa_approval(service_name):
    """Send Hugh a Telegram asking for approval to run Okta MFA.

    Blocks until Hugh replies "go"/"ready"/"start". No timeout — if he
    misses the message, the process waits until he sees it.
    """
    message = (
        f"\U0001f6e1\ufe0f <b>Okta MFA required: {service_name}</b>\n\n"
        f"NotesReminder needs to authenticate with Okta SSO for <b>{service_name}</b>.\n"
        f"Reply <b>go</b>, <b>ready</b>, or <b>start</b> to authorize the MFA push."
    )
    # wait_for_telegram_approval is async; run it from sync Playwright code
    asyncio.run(wait_for_telegram_approval(service_name, None, message))


def run_okta_mfa_with_gate(page, service_name, login_timeout=300):
    """Full Okta MFA flow behind a Telegram gate.

    1. Send Telegram message asking for approval.
    2. Wait for Hugh's "go" reply.
    3. Submit credentials (fires the Okta Verify push).
    4. Wait for Hugh to approve the push on his phone.

    Returns when the SSO redirect has completed (page is no longer on the
    Okta login URL).
    """
    request_okta_mfa_approval(service_name)
    print(f"Okta MFA approved via Telegram for {service_name}. Submitting credentials...", flush=True)
    filled = fill_okta_login(page)
    if not filled:
        raise RuntimeError(
            f"Okta credentials not available; cannot complete MFA for {service_name}."
        )
    wait_for_okta_push(page, login_timeout)
