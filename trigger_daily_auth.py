#!/usr/bin/env python3
"""Daily Okta MFA auth dispatcher for cron (runs ~7:16pm CT).

Sends Hugh a Telegram approval request FIRST, waits for his "go"/"ready"/
"start" reply, and ONLY THEN triggers the Okta Verify push. This prevents a
surprise push landing on his iPhone before he's paying attention.

The wait is bounded (default 10 min) so the cron job always terminates — if
Hugh doesn't reply in the window, it notifies him the daily auth was skipped
and he can press the @sorauthbot 'Trigger Okta MFA' button manually later.

The underlying trigger_okta_auth releases its lock on cancellation, so no
orphaned lock is left behind.
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
from pike13_auto_auth import wait_for_telegram_approval  # noqa: E402

AUTH_TIMEOUT_S = int(os.environ.get("DAILY_AUTH_TIMEOUT_S", "600"))
# How long to wait for Hugh's "go" reply before skipping the daily push.
GATE_TIMEOUT_S = int(os.environ.get("DAILY_AUTH_GATE_TIMEOUT_S", "600"))


async def main():
    log.info("Daily Okta auth dispatch starting (gate window=%ss, auth window=%ss)",
             GATE_TIMEOUT_S, AUTH_TIMEOUT_S)

    # ── Telegram gate: ask first, push only after approval ──────────────────
    gate_message = (
        "\U0001f6e1\ufe0f <b>Daily Okta MFA — approve to start?</b>\n\n"
        "The nightly School of Rock auth warm-up is ready to run. "
        "Reply <b>go</b>, <b>ready</b>, or <b>start</b> and I'll send the "
        "Okta Verify push to your phone. If you don't reply within 10 min, "
        "the push is skipped (no surprise prompt)."
    )
    try:
        log.info("Sending Telegram approval gate; waiting for 'go' reply...")
        await wait_for_telegram_approval("Daily Okta Warm-up", GATE_TIMEOUT_S, gate_message)
    except asyncio.TimeoutError:
        log.warning("Timed out waiting for Telegram approval (gate %ss)", GATE_TIMEOUT_S)
        await _notify(
            "\u23f0 Daily Okta warm-up skipped — no approval reply within 10 min. "
            "Press the @sorauthbot 'Trigger Okta MFA' button when you're ready "
            "to refresh the session."
        )
        return
    except Exception as e:  # noqa: BLE001
        log.exception("Telegram gate failed")
        await _notify(f"\u274c Daily Okta gate error: {e}")
        return

    log.info("Telegram approval received — triggering Okta push now.")
    await _notify("✅ Approved — sending the Okta Verify push to your phone now.")

    # ── Now (and only now) fire the push ────────────────────────────────────
    try:
        result = await asyncio.wait_for(
            trigger_okta_auth(notify=_notify),
            timeout=AUTH_TIMEOUT_S,
        )
        log.info("Daily auth result: %s", result)
        if result.get("ok"):
            await _notify(
                "\u2705 Daily Okta auth succeeded — warm session ready for tonight's "
                "notes run."
            )
        else:
            await _notify(f"\u26a0\ufe0f Daily Okta auth did not complete: {result.get('detail')}")
    except asyncio.TimeoutError:
        log.warning("Daily auth timed out waiting for iPhone approval (%ss)", AUTH_TIMEOUT_S)
        await _notify(
            "\u23f0 Daily Okta push sent but not approved within 10 min. Press the "
            "@sorauthbot 'Trigger Okta MFA' button when you can to refresh the session."
        )
    except Exception as e:  # noqa: BLE001
        log.exception("Daily auth crashed")
        await _notify(f"\u274c Daily Okta auth error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
