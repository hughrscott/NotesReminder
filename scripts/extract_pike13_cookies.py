#!/usr/bin/env python3
"""Extract Pike13 auth cookies from an existing browser profile for injection into headless scrapers."""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright


PIKE13_DOMAINS = [".pike13.com", "westu-sor.pike13.com", "theheights-sor.pike13.com"]
OKTA_DOMAINS = [".okta.com", "sor.okta.com"]


def extract_cookies(profile_dir: str, output_path: str, chrome_channel: bool = False):
    """Open browser, navigate to Pike13, dump cookies + storage."""
    launch_kwargs = {"headless": False, "viewport": {"width": 1440, "height": 900}}

    # Create a fresh profile directory if none provided
    if not profile_dir:
        profile_dir = str(Path.home() / ".pike13_profile")
        Path(profile_dir).mkdir(parents=True, exist_ok=True)
        print(f"No profile dir specified — using {profile_dir}")

    profile_path = Path(profile_dir)
    if not profile_path.exists():
        profile_path.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(str(profile_path), **launch_kwargs)
        page = context.pages[0] if context.pages else context.new_page()

        # Navigate to login page — user will complete Okta MFA manually
        print("Opening Pike13 login page...")
        print("Please complete Okta login/MFA in the browser window.")
        print("The script will wait up to 5 minutes for you to authenticate as STAFF.")
        login_url = "https://westu-sor.pike13.com/accounts/sign_in"
        page.goto(login_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)

        # Wait for user to complete login (up to 5 min)
        deadline = time.time() + 300
        authenticated = False
        while time.time() < deadline:
            page.wait_for_timeout(3000)
            current_url = page.url.lower()
            if "/schedule" in current_url and "sign_in" not in current_url:
                print("  Authenticated! Detected schedule page.")
                authenticated = True
                page.wait_for_timeout(3000)
                break

        if not authenticated:
            print("  WARNING: Login not detected within 5 minutes. Extracting whatever cookies we have.")

        # Extract cookies for Pike13 + Okta domains
        all_cookies = []
        for domain in PIKE13_DOMAINS + OKTA_DOMAINS:
            try:
                cookies = context.cookies(domain)
                all_cookies.extend(cookies)
            except Exception:
                pass

        # Also extract from unrestricted cookie jar
        try:
            all_cookies_raw = context.cookies()
            existing_names = {c["name"] for c in all_cookies}
            for c in all_cookies_raw:
                if c["name"] not in existing_names:
                    all_cookies.append(c)
        except Exception:
            pass

        # Extract storage
        storage = {}
        try:
            for school in ["westu-sor", "theheights-sor"]:
                page.goto(f"https://{school}.pike13.com/schedule", wait_until="domcontentloaded")
                page.wait_for_timeout(1000)
                storage[school] = page.evaluate("""() => {
                    const items = {};
                    for (let i = 0; i < localStorage.length; i++) {
                        const key = localStorage.key(i);
                        items[key] = localStorage.getItem(key);
                    }
                    return items;
                }""")
        except Exception:
            pass

        context.close()

    # Build output
    now_utc = datetime.now(timezone.utc)
    cookie_list = []
    soonest_expiry = None
    for c in all_cookies:
        expires_raw = c.get("expires")
        expires_ts = None
        if expires_raw and expires_raw > 0:
            expires_ts = datetime.fromtimestamp(expires_raw, tz=timezone.utc).isoformat()
        cookie_list.append({
            "name": c["name"],
            "domain": c["domain"],
            "path": c.get("path", "/"),
            "value": c["value"],
            "expires": expires_ts,
            "httpOnly": c.get("httpOnly", False),
            "secure": c.get("secure", False),
            "sameSite": c.get("sameSite", "Lax"),
        })
        if expires_raw and expires_raw > 0:
            expires_dt = datetime.fromtimestamp(expires_raw, tz=timezone.utc)
            if soonest_expiry is None or expires_dt < soonest_expiry:
                soonest_expiry = expires_dt

    payload = {
        "extracted_at": now_utc.isoformat(),
        "source": "manual_extraction_from_mac",
        "cookies": cookie_list,
        "storage": storage,
        "cookie_count": len(cookie_list),
        "soonest_expiry": soonest_expiry.isoformat() if soonest_expiry else None,
        "recommended_refresh_days": 7,
    }

    output = Path(output_path)
    output.write_text(json.dumps(payload, indent=2))
    os.chmod(output_path, 0o600)
    print(f"\nExtracted {len(cookie_list)} cookies to {output_path}")
    if soonest_expiry:
        days_left = (soonest_expiry - now_utc).days
        print(f"Soonest cookie expires in ~{days_left} days ({soonest_expiry.date()})")
        print(f"Recommended refresh: every {min(7, max(1, days_left - 2))} days")
    else:
        print("No expiry dates found — cookies may be session cookies. Recommend refreshing daily.")
    print("\nNext step: Securely copy this file to the Oracle Cloud server.")

    return payload


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract Pike13 auth cookies from browser profile")
    parser.add_argument("--profile-dir", default="", help="Path to browser profile (optional — if not found, launches fresh browser)")
    parser.add_argument("--output", default="pike13_cookies.json", help="Output JSON file for cookies")
    parser.add_argument("--chrome-channel", action="store_true", help="Use system Chrome instead of Playwright's Chromium")
    args = parser.parse_args()
    extract_cookies(args.profile_dir, args.output, args.chrome_channel)
