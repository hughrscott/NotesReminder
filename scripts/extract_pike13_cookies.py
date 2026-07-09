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
    """Open existing browser profile, navigate to Pike13, dump cookies + storage."""
    profile = Path(profile_dir)
    if not profile.exists():
        raise FileNotFoundError(f"Profile directory not found: {profile_dir}")

    launch_kwargs = {"headless": False, "viewport": {"width": 1440, "height": 900}}
    if chrome_channel:
        launch_kwargs["channel"] = "chrome"

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(str(profile), **launch_kwargs)
        page = context.pages[0] if context.pages else context.new_page()

        # Try both schools
        schools = ["westu-sor", "theheights-sor"]
        authenticated = False
        for school in schools:
            url = f"https://{school}.pike13.com/schedule"
            print(f"Navigating to {url}...")
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(2000)

            # Check if we landed on schedule (authenticated) or got redirected to sign_in
            if "/accounts/sign_in" in page.url.lower() or "/login" in page.url.lower():
                print(f"  Not authenticated for {school}. Opening Okta login page...")
                # Navigate to Okta to trigger login
                page.goto(f"https://{school}.pike13.com/accounts/sign_in", wait_until="domcontentloaded")
                print(f"  Please complete Okta login/MFA in the opened browser.")
                print(f"  Waiting for authentication (checking every 3s, timeout 5 min)...")
                deadline = time.time() + 300
                while time.time() < deadline:
                    page.wait_for_timeout(3000)
                    if "/schedule" in page.url.lower() and "sign_in" not in page.url.lower():
                        # Try navigating directly to schedule
                        page.goto(url, wait_until="domcontentloaded")
                        page.wait_for_timeout(2000)
                    if "/schedule" in page.url.lower() and "sign_in" not in page.url.lower():
                        authenticated = True
                        print(f"  Authenticated for {school}!")
                        break
            else:
                print(f"  Already authenticated for {school}!")
                authenticated = True
            if authenticated:
                break

        if not authenticated:
            print("ERROR: Could not authenticate to Pike13. Please log in manually and retry.")
            # Dump whatever cookies we have anyway

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
            for school in schools:
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
    parser.add_argument("--profile-dir", required=True, help="Path to browser profile (e.g., 'browser_profiles/pike13')")
    parser.add_argument("--output", default="pike13_cookies.json", help="Output JSON file for cookies")
    parser.add_argument("--chrome-channel", action="store_true", help="Use system Chrome instead of Playwright's Chromium")
    args = parser.parse_args()
    extract_cookies(args.profile_dir, args.output, args.chrome_channel)
