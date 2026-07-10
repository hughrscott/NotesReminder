"""Scraper integration: launch a Playwright context on the warm Okta session.

Scrapers that SSO through Okta (Pike13, HubSpot, Dialpad, Gmail) call
launch_okta_context() to inherit the authenticated browser session that the
Telegram bot established. This skips MFA entirely -- no push, no waiting.

If no warm session exists, launch_okta_context() raises SessionNotReady so the
scraper can tell the user to press the Telegram button first.
"""
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, BrowserContext

from okta_auth.session_state import consume_session


class SessionNotReady(Exception):
    """Raised when no warm Okta session is cached in the shared profile."""


@asynccontextmanager
async def launch_okta_context(
    headless: bool = True,
    viewport: Optional[dict] = None,
):
    """Yield a persistent Playwright context on the warm Okta profile.

    The context is opened on browser_profiles/sor_shared, which holds the
    authenticated Okta session. Any SOR/Okta SSO site visited will already be
    logged in.

    Usage:
        async with launch_okta_context() as context:
            page = await context.new_page()
            await page.goto("https://westu-sor.pike13.com/...")
            # already authenticated via the warm Okta session
    """
    info = consume_session()
    if not info["ready"]:
        raise SessionNotReady(
            "No warm Okta session. Press the Telegram 'Trigger Okta MFA' button "
            "to authenticate first."
        )
    profile = info["profile"]
    Path(profile).mkdir(parents=True, exist_ok=True)
    pw = await async_playwright().start()
    try:
        context: BrowserContext = await pw.chromium.launch_persistent_context(
            profile,
            headless=headless,
            viewport=viewport or {"width": 1920, "height": 1080},
            args=["--disable-dev-shm-usage"],
        )
        yield context
    finally:
        await pw.stop()
