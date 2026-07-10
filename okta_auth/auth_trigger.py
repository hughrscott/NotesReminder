"""Triggers an Okta MFA push on demand and waits OPEN-ENDED for approval.

Design (the "inverted auth" flow):
  - A Telegram button calls trigger_okta_auth().
  - We fill the Okta login from env, submit, and Okta pushes to Hugh's iPhone.
  - We notify Hugh ("push sent, approve whenever") and then poll with NO TIMEOUT
    until the session URL leaves the Okta verify page (i.e. he approved).
  - On success we set the warm-session flag; the persistent profile is saved
    automatically by Playwright when the context closes.

There is deliberately NO timeout: Hugh approves on his own schedule,
which may be hours later. Okta's own session expiry is the only clock.
"""
import asyncio
import logging
import re

from playwright.async_api import async_playwright

from okta_auth.config import get_config
from okta_auth.session_state import acquire_lock, release_lock, set_session_ready

log = logging.getLogger("okta_auth.trigger")

CONFIG = get_config()

OKTA_USERNAME_SELECTOR = (
    'input[name="username"], input#okta-signin-username, input[type="text"]'
)
OKTA_PASSWORD_SELECTOR = (
    'input[name="password"], input#okta-signin-password, input[type="password"]'
)
SIGNIN_BUTTON = "Sign in"


def classify_url(url: str, body_text: str = "") -> str:
    """Classify an Okta/SOR page as 'authenticated', 'needs_login', or 'unknown'.

    STRICTER than the probe's rule: a bare okta.com landing page is NOT
    evidence of being logged in (Okta shows pre-auth dashboards there too).
    We only call Okta 'authenticated' on a known post-login path, and we
    treat the presence of the login form as the definitive 'needs_login'.
    """
    lowered = (url or "").lower()
    body = (body_text or "").lower()

    # Definitive "needs login": the Okta sign-in form is on the page.
    if "okta-signin-username" in body or 'id="okta-signin-username"' in body:
        return "needs_login"
    if any(
        marker in lowered
        for marker in ("/login", "/signin", "/sign_in", "accounts.google.com", "okta.com/signin")
    ):
        return "needs_login"

    if "access denied" in body or "not authorized" in body:
        return "blocked"

    # Known post-login destinations only.
    if "mail.google.com" in lowered and ("inbox" in body or "compose" in body or "gmail" in body):
        return "authenticated"
    if "pike13.com" in lowered and "accounts/sign_in" not in lowered:
        return "authenticated"
    if "hubspot.com" in lowered and "login" not in lowered:
        return "authenticated"
    if "dialpad.com" in lowered and "login" not in lowered:
        return "authenticated"
    # Okta authenticated surfaces (SSO dashboard / enduser / oauth callback).
    if "okta.com" in lowered and any(
        p in lowered for p in ("/app/", "/user/", "/oauth2/", "/enduser/", "/admin/", "okta.com/app/", "okta.com/user/")
    ):
        return "authenticated"

    # Bare okta.com or unknown page -> don't lie about being authed.
    return "unknown"


async def trigger_okta_auth(notify=None) -> dict:
    """Run the Okta login + push, then wait open-ended for approval.

    notify: optional async callable(str) used to message Hugh (e.g. via the
    Telegram bot). Kept decoupled so this module has no Telegram import.

    Returns {"ok": bool, "detail": str}.
    """
    if not acquire_lock():
        return {"ok": False, "detail": "auth already in progress"}

    async def send(message: str):
        if notify:
            try:
                await notify(message)
            except Exception:
                pass

    try:
        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                str(CONFIG["SHARED_PROFILE"]),
                headless=True,
                viewport={"width": 1440, "height": 1000},
            )
            try:
                page = await context.new_page()
                await page.goto("https://sor.okta.com", wait_until="domcontentloaded", timeout=60000)

                # Give Okta a moment to finish any redirect, then classify.
                try:
                    await page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass
                body = await page.locator("body").inner_text(timeout=5000)
                status = classify_url(page.url, body)
                if status == "authenticated":
                    set_session_ready(True)
                    return {"ok": True, "detail": "already authenticated"}
                if status == "blocked":
                    return {"ok": False, "detail": "access blocked on Okta landing page"}

                # Fill login from env.
                username_input = page.locator(OKTA_USERNAME_SELECTOR).first
                password_input = page.locator(OKTA_PASSWORD_SELECTOR).first
                await username_input.wait_for(timeout=15000)
                await username_input.fill(CONFIG["OKTA_USER"])
                await password_input.fill(CONFIG["OKTA_PASSWORD"])
                remember = page.locator('input[type="checkbox"][name="remember"], input[type="checkbox"]')
                if await remember.count():
                    try:
                        if not await remember.first.is_checked():
                            await remember.first.check(timeout=3000)
                    except Exception:
                        pass
                await page.get_by_role("button", name=re.compile(r"sign in", re.IGNORECASE)).click(timeout=10000)

                # After submit we land on the Okta verify/push factor screen.
                # Okta waits for an explicit "send push" action — we must
                # trigger it (click the factor / "Send push automatically")
                # BEFORE the push is actually dispatched to the iPhone.
                notified = False
                iteration = 0
                while True:
                    iteration += 1
                    try:
                        body = await page.locator("body").inner_text(timeout=5000)
                    except Exception:
                        body = ""
                    lowered = body.lower()
                    on_verify = (
                        "okta verify" in lowered
                        or "select authentication factor" in lowered
                        or "/signin/verify" in page.url.lower()
                    )
                    log.info(
                        "wait-loop #%d url=%s on_verify=%s body_snip=%s",
                        iteration, page.url, on_verify, lowered[:120].replace("\n", " "),
                    )
                    if on_verify and not notified:
                        # Fire the push. The real dispatch control on Okta's
                        # verify page is the primary SUBMIT button (empty-text
                        # submit), or the "Send push automatically" checkbox
                        # (name=autoPush). The factor row itself only SELECTS
                        # the factor — it does NOT send the push.
                        clicked = False
                        # Preferred: check "Send push automatically" so Okta
                        # dispatches immediately, then also click submit.
                        try:
                            auto = page.locator('input[name="autoPush"]').first
                            if await auto.count() and not await auto.is_checked():
                                await auto.check(timeout=5000)
                                log.info("checked autoPush (send push automatically)")
                        except Exception as e:
                            log.warning("autoPush check failed: %s", e)
                        for sel in (
                            'button[type="submit"]',
                            'button:has-text("Send push")',
                            'input[type="submit"]',
                        ):
                            try:
                                loc = page.locator(sel).first
                                if await loc.count():
                                    await loc.click(timeout=5000)
                                    clicked = True
                                    log.info("clicked push trigger: %s", sel)
                                    break
                            except Exception as e:
                                log.warning("click failed for %s: %s", sel, e)
                                continue
                        if not clicked:
                            log.warning("on_verify but NO push trigger element found")
                        await send(
                            "Okta Verify push sent — approve it on your phone "
                            "whenever you're ready. No rush."
                        )
                        log.info("notify sent: push dispatched")
                        notified = True
                        break
                    if "push sent" in lowered:
                        # Already dispatched (e.g. auto-send kicked in).
                        if not notified:
                            await send(
                                "Okta Verify push sent — approve it on your phone "
                                "whenever you're ready. No rush."
                            )
                            log.info("notify sent: push already dispatched")
                            notified = True
                        break
                    await asyncio.sleep(2)

                # Open-ended poll for successful auth (URL leaves the verify page).
                while True:
                    try:
                        body = await page.locator("body").inner_text(timeout=5000)
                    except Exception:
                        body = ""
                    if classify_url(page.url, body) == "authenticated":
                        break
                    await asyncio.sleep(3)

                # Persisted profile is saved automatically on context.close().
                set_session_ready(True)
                return {"ok": True, "detail": "Okta session established"}
            finally:
                await context.close()
    except Exception as exc:
        set_session_ready(False)
        return {"ok": False, "detail": str(exc)}
    finally:
        release_lock()
