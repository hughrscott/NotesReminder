#!/usr/bin/env python3
"""Daily Okta MFA auth dispatcher for cron (runs ~11am CT).

Sends the Okta Verify push to Hugh's iPhone and waits (bounded) for approval,
so the warm session is fresh before the 10pm notes pipeline runs. Launched by
cron; loads its own env so the crontab line stays simple.

The wait is bounded (default 10 min) so the cron job always terminates — if
Hugh doesn't approve in the window, it notifies him to press the @sorauthbot
button manually later. The underlying trigger_okta_auth releases its lock on
cancellation, so no orphaned lock is left behind.
"""
import asyncio
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("daily_auth")

NOTES = Path(__file__).resolve().parent
sys.path.insert(0, str(NOTES))

# Load env (token + Okta creds) the same way the systemd service does.
_ENV_FILE = Path("/home/ubuntu/.hermes/SOR/okta_bot.env")
if _ENV_FILE.exists():
    for line in _ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

from okta_auth.auth_trigger import trigger_okta_auth  # noqa: E402
from okta_auth.telegram_bot import _notify  # noqa: E402

AUTH_TIMEOUT_S = int(os.environ.get("DAILY_AUTH_TIMEOUT_S", "600"))


async def main():
    log.info("Daily Okta auth dispatch starting (wait window=%ss)", AUTH_TIMEOUT_S)
    try:
        result = await asyncio.wait_for(
            trigger_okta_auth(notify=_notify),
            timeout=AUTH_TIMEOUT_S,
        )
        log.info("Daily auth result: %s", result)
        if result.get("ok"):
            await _notify(
                "✅ Daily Okta auth succeeded — warm session ready for tonight's "
                "notes run."
            )
        else:
            await _notify(f"⚠️ Daily Okta auth did not complete: {result.get('detail')}")
    except asyncio.TimeoutError:
        log.warning("Daily auth timed out waiting for iPhone approval (%ss)", AUTH_TIMEOUT_S)
        await _notify(
            "⏰ Daily Okta push sent but not approved within 10 min. Press the "
            "@sorauthbot 'Trigger Okta MFA' button when you can to refresh the session."
        )
    except Exception as e:  # noqa: BLE001
        log.exception("Daily auth crashed")
        await _notify(f"❌ Daily Okta auth error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
