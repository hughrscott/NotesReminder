"""Telegram bot for the Inverted Okta MFA Auth system (python-telegram-bot v22).

Security model: chat-ID gate. The first /start from any chat auto-enrolls that
chat as authorized (persisted to the gitignored authorized_chats.json) when no
explicit TELEGRAM_CHAT_ID is configured. No custom auth scheme — the bot is
only reachable by your Telegram account.
"""
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    Defaults,
)

import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("okta_auth.bot")

import okta_auth.session_state
from okta_auth.auth_trigger import trigger_okta_auth
from okta_auth.config import get_config

CONFIG = get_config()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    log.info("[/start] from chat %s", chat_id)
    if CONFIG["TELEGRAM_CHAT_ID"] is None:
        okta_auth.session_state.enroll_chat(chat_id)
        text = (
            f"Authorized chat {chat_id}. Your chat ID is {chat_id}. "
            "Press the button to trigger Okta MFA."
        )
    else:
        text = "Press the button to trigger Okta MFA."

    button = [[InlineKeyboardButton("Trigger Okta MFA", callback_data="trigger_mfa")]]
    keyboard = InlineKeyboardMarkup(button)
    await update.message.reply_text(text=text, reply_markup=keyboard)


async def _notify(message: str):
    """Send a plain-text message to the authorized chat (used by auth_trigger)."""
    chat_id = CONFIG["TELEGRAM_CHAT_ID"]
    if chat_id is None:
        # Use the most-recently enrolled chat (last in the list), not a stale test id.
        try:
            ids = okta_auth.session_state.list_authorized_chats()
            chat_id = ids[-1] if ids else None
        except Exception:
            chat_id = None
    if chat_id is not None:
        from telegram import Bot
        Bot(CONFIG["TELEGRAM_BOT_TOKEN"]).send_message(chat_id=chat_id, text=message)


async def trigger_mfa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    log.info("[trigger_mfa] callback from chat %s", chat_id)

    if not okta_auth.session_state.is_chat_authorized(chat_id):
        log.warning("[trigger_mfa] chat %s NOT authorized", chat_id)
        await query.edit_message_text(text="Unauthorized.")
        return

    await query.edit_message_text(text="Triggering Okta MFA push...")
    log.info("[trigger_mfa] calling trigger_okta_auth()")
    result = await trigger_okta_auth(notify=_notify)
    log.info("[trigger_mfa] result: %s", result)
    await query.edit_message_text(
        text=f"Result: ok={result['ok']} — {result['detail']}"
    )


def run_bot():
    app = (
        Application.builder()
        .token(CONFIG["TELEGRAM_BOT_TOKEN"])
        .defaults(Defaults(parse_mode=None))
        .build()
    )
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(trigger_mfa, pattern="^trigger_mfa$"))
    app.run_polling()
