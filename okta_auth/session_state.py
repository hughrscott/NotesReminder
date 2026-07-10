"""Session + lock state for the Inverted Okta MFA Auth system.

State files (all outside the repo / gitignored):
  - <SHARED_PROFILE>/.session_ready : exists => a warm Okta session is saved
  - /tmp/okta_auth.lock            : exists => an auth run is in progress
  - ~/.hermes/SOR/authorized_chats.json : list of Telegram chat IDs allowed to trigger
"""
import json
import os
from pathlib import Path

from okta_auth.config import get_config

CONFIG = get_config()


def _shared_profile() -> Path:
    return Path(CONFIG["SHARED_PROFILE"]).resolve()


def is_session_ready() -> bool:
    """Return True if a warm Okta session flag file exists.

    Okta enforces its own absolute session timeout (admin-set, 8-24h); we do
    NOT add our own expiry. The flag simply records that a session was saved.
    """
    return (_shared_profile() / ".session_ready").exists()


def set_session_ready(val: bool) -> None:
    """Write or remove the warm-session flag file."""
    flag = _shared_profile() / ".session_ready"
    if val:
        flag.touch()
    else:
        flag.unlink(missing_ok=True)


def _pid_alive(pid: int) -> bool:
    """Best-effort check whether a process with this PID still exists."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def acquire_lock() -> bool:
    """Non-blocking, PID-aware lock.

    Returns True if we created it (we own the run). If a stale lock exists
    whose owning process is no longer alive (e.g. the bot was killed mid-run,
    OOM, or rebooted), we reclaim it instead of deadlocking forever.
    """
    path = CONFIG["LOCK_PATH"]
    if os.path.exists(path):
        try:
            stale_pid = int(open(path).read().strip() or "0")
        except (OSError, ValueError):
            stale_pid = 0
        if stale_pid == 0 or not _pid_alive(stale_pid):
            # Orphaned lock (dead process or old format) — reclaim it.
            try:
                os.remove(path)
            except OSError:
                pass
    # Atomic non-blocking create (O_EXCL fails if the file already exists).
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False
    try:
        os.write(fd, str(os.getpid()).encode())
    finally:
        os.close(fd)
    return True


def release_lock() -> None:
    """Remove the lock only if we still own it (matches our PID)."""
    path = CONFIG["LOCK_PATH"]
    try:
        if os.path.exists(path):
            try:
                owned = int(open(path).read().strip() or "0") == os.getpid()
            except (OSError, ValueError):
                owned = True  # can't read it; assume we own it
            if owned:
                os.remove(path)
    except FileNotFoundError:
        pass


def _auth_state_file() -> Path:
    return Path(CONFIG["AUTH_STATE"]).expanduser()


def is_chat_authorized(chat_id) -> bool:
    auth_file = _auth_state_file()
    if not auth_file.exists():
        # First run: if an explicit chat id is configured, seed it.
        if CONFIG["TELEGRAM_CHAT_ID"]:
            try:
                with open(auth_file, "w") as f:
                    json.dump([str(CONFIG["TELEGRAM_CHAT_ID"])], f)
            except OSError:
                return False
            authorized = [str(CONFIG["TELEGRAM_CHAT_ID"])]
        else:
            return False
    else:
        try:
            authorized = json.load(open(auth_file))
        except (OSError, json.JSONDecodeError):
            return False
    return str(chat_id) in authorized


def list_authorized_chats() -> list:
    """Return the list of authorized Telegram chat IDs (empty if none)."""
    auth_file = _auth_state_file()
    if not auth_file.exists():
        return []
    try:
        return json.load(open(auth_file))
    except (OSError, json.JSONDecodeError):
        return []


def shared_profile_path() -> Path:
    """Path to the shared persistent Okta browser profile."""
    return _shared_profile()


def consume_session() -> dict:
    """Scraper integration point.

    Returns a dict scrapers use to inherit the warm Okta session:
      - ready:   bool — whether a warm session exists
      - profile: str — absolute path to browser_profiles/sor_shared
    Scrapers that SSO through Okta should launch Playwright on `profile`
    (launch_persistent_context) so they inherit the authenticated session and
    skip MFA entirely. If not ready, the scraper should ask the user to press
    the Telegram button first (or fall back to its own auth path).
    """
    return {
        "ready": is_session_ready(),
        "profile": str(_shared_profile()),
    }


def enroll_chat(chat_id) -> None:
    """Append a chat ID to the authorized list (create file if missing)."""
    auth_file = _auth_state_file()
    authorized = []
    if auth_file.exists():
        try:
            authorized = json.load(open(auth_file))
        except (OSError, json.JSONDecodeError):
            authorized = []
    if str(chat_id) not in authorized:
        authorized.append(str(chat_id))
        auth_file.parent.mkdir(parents=True, exist_ok=True)
        with open(auth_file, "w") as f:
            json.dump(authorized, f)
