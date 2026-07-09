"""Cookie-based authentication helpers for Pike13 (Okta SSO)."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


DEFAULT_COOKIES_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "pike13_cookies.json")
COOKIES_PATH = os.getenv("PIKE13_COOKIES_PATH", DEFAULT_COOKIES_PATH)
MAX_AGE_DAYS = int(os.getenv("PIKE13_COOKIE_MAX_AGE_DAYS", "14"))


class CookieAuthError(Exception):
    """Raised when cookies are missing, expired, or invalid."""


class CookieExpiredError(CookieAuthError):
    """Raised when cookies have passed their expiration threshold."""


def load_cookies(cookies_path: Optional[str] = None) -> dict:
    """Load cookie payload from JSON file. Raises CookieAuthError on failure."""
    path = Path(cookies_path or COOKIES_PATH)
    if not path.exists():
        raise CookieAuthError(
            f"Cookie file not found: {path}. "
            f"Run scripts/extract_pike13_cookies.py on your Mac first, then copy pike13_cookies.json here."
        )
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise CookieAuthError(f"Invalid cookie JSON in {path}: {e}")
    return payload


def check_cookie_freshness(payload: Optional[dict] = None, max_age_days: Optional[int] = None) -> dict:
    """Check if stored cookies are still acceptably fresh. Returns a status dict."""
    if payload is None:
        try:
            payload = load_cookies()
        except CookieAuthError:
            return {"status": "missing", "error": "Cookie file not found"}

    max_age = max_age_days or MAX_AGE_DAYS

    # Check extraction age
    extracted_at = payload.get("extracted_at")
    if extracted_at:
        try:
            extracted_dt = datetime.fromisoformat(extracted_at)
            age_days = (datetime.now(timezone.utc) - extracted_dt).days
            if age_days > max_age:
                return {
                    "status": "expired",
                    "error": f"Cookies extracted {age_days} days ago (max allowed: {max_age})",
                    "extracted_at": extracted_at,
                    "age_days": age_days,
                    "max_age_days": max_age,
                }
        except (ValueError, TypeError):
            pass

    # Check individual cookie expiries
    soonest = None
    for cookie in payload.get("cookies", []):
        expires = cookie.get("expires")
        if expires:
            try:
                expiry_dt = datetime.fromisoformat(expires)
                now = datetime.now(timezone.utc)
                if expiry_dt < now:
                    return {
                        "status": "expired",
                        "error": f"Cookie '{cookie['name']}' expired at {expires}",
                        "expired_cookie": cookie["name"],
                    }
                days_left = (expiry_dt - now).days
                if soonest is None or expiry_dt < soonest[0]:
                    soonest = (expiry_dt, cookie["name"], days_left)
            except (ValueError, TypeError):
                pass

    result = {
        "status": "fresh",
        "extracted_at": extracted_at,
        "cookie_count": payload.get("cookie_count", 0),
    }
    if soonest:
        result["soonest_expiry_days"] = soonest[2]
        result["soonest_expiry_cookie"] = soonest[1]
    return result


def inject_cookies_into_context(context, payload: Optional[dict] = None):
    """Inject saved cookies into a Playwright browser context. Must be called BEFORE navigating."""
    if payload is None:
        payload = load_cookies()

    cookies = payload.get("cookies", [])
    if not cookies:
        raise CookieAuthError("Cookie payload is empty — no cookies to inject.")

    # Filter out expired cookies
    now_ts = datetime.now(timezone.utc).timestamp()
    valid_cookies = []
    for c in cookies:
        expires = c.get("expires")
        if expires:
            try:
                expiry_dt = datetime.fromisoformat(expires)
                if expiry_dt.timestamp() < now_ts:
                    continue  # skip expired
            except (ValueError, TypeError):
                pass
        valid_cookies.append({
            "name": c["name"],
            "value": c["value"],
            "domain": c.get("domain", ""),
            "path": c.get("path", "/"),
            "httpOnly": c.get("httpOnly", False),
            "secure": c.get("secure", True),
            "sameSite": c.get("sameSite", "Lax"),
        })

    context.add_cookies(valid_cookies)
    return len(valid_cookies)


def inject_storage_into_page(page, payload: Optional[dict] = None, school: str = "westu-sor"):
    """Inject localStorage items into a page that's already on the Pike13 domain."""
    if payload is None:
        payload = load_cookies()

    storage = payload.get("storage", {}).get(school, {})
    if storage:
        page.evaluate(
            """([items]) => {
                for (const [key, value] of Object.entries(items)) {
                    localStorage.setItem(key, value);
                }
            }""",
            [storage],
        )
