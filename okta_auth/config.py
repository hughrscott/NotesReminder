"""Configuration loader for the Inverted Okta MFA Auth system.

All secrets come from the environment. We load NotesReminder/.env (gitignored)
so the Telegram bot token and any local overrides are picked up at launch.
Nothing sensitive is hardcoded here.
"""
import os

try:
    from dotenv import load_dotenv
    load_dotenv()  # loads ./NotesReminder/.env into os.environ
except Exception:
    pass


def get_config():
    """Load Okta auth + Telegram bot configuration from the environment.

    Returns a plain dict. All values are read from os.getenv so no secrets
    live in the source tree.
    """
    config = {
        "OKTA_USER": (
            os.getenv("OKTA_USER")
            or os.getenv("OKTA_USERNAME")
            or os.getenv("SOR_OKTA_USERNAME")
        ),
        "OKTA_PASSWORD": (
            os.getenv("OKTA_PASSWORD")
            or os.getenv("SOR_OKTA_PASSWORD")
        ),
        "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN"),
        "TELEGRAM_CHAT_ID": os.getenv("TELEGRAM_CHAT_ID"),
        "SHARED_PROFILE": os.getenv("OKTA_SHARED_PROFILE", "browser_profiles/sor_shared"),
        "LOCK_PATH": os.getenv("OKTA_AUTH_LOCK", "/tmp/okta_auth.lock"),
        "AUTH_STATE": os.getenv(
            "OKTA_AUTH_STATE", "~/.hermes/SOR/authorized_chats.json"
        ),
    }
    return config
