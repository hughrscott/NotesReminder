#!/usr/bin/env python3
"""Extract Pike13 auth cookies via sor.okta.com SSO authentication.

Simple flow:
1. Opens sor.okta.com in a browser
2. Waits for you to complete Okta login/MFA manually (press Enter when done)
3. Navigates to Pike13 to establish staff session
4. Extracts all cookies (Okta + Pike13) and localStorage
5. Saves to pike13_cookies.json with secure permissions
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright


PIKE13_DOMAINS = [".pike13.com", "westu-sor.pike13.com", "theheights-sor.pike13.com"]
OKTA_DOMAINS = [".okta.com", "sor.okta.com"]


def extract_cookies(profile_dir: str = "", output_path: str = "pike13_cookies.json"):
    """Open browser, authenticate via sor.okta.com, then extract Pike13 cookies."""

    if not profile_dir:
        profile_dir = str(Path.home() / ".pike13_profile")
    Path(profile_dir).mkdir(parents=True, exist_ok=True)
    print(f"Using profile: {profile_dir}")

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            profile_dir, headless=False, viewport={"width": 1440, "height": 900}
        )
        page = context.pages[0] if context.pages else context.new_page()

        # Step 1: sor.okta.com — YOU log in, script waits
        print("\n" + "=" * 60)
        print("Opening sor.okta.com — please log in with your STAFF credentials.")
        print("Complete the Okta login and MFA in the browser window.")
        print("=" * 60)
        page.goto("https://sor.okta.com", wait_until="domcontentloaded", timeout=30000)

        # Block until user presses Enter — no auto-detection, no timeouts
        input("\n>>> Press ENTER when you are logged in to Okta. <<<\n")

        # Step 2: Navigate to Pike13 (SSO redirect establishes staff session)
        print("Navigating to Pike13 West U...")
        page.goto("https://westu-sor.pike13.com/schedule", wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(5000)
        print(f"  URL: {page.url}")

        print("Navigating to Pike13 The Heights...")
        page.goto("https://theheights-sor.pike13.com/schedule", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)
        print(f"  URL: {page.url}")

        # Step 3: Extract everything
        print("\nExtracting cookies and storage...")

        all_cookies = []
        for domain in PIKE13_DOMAINS + OKTA_DOMAINS:
            try:
                all_cookies.extend(context.cookies(domain))
            except Exception:
                pass

        # Also grab everything from the cookie jar
        try:
            raw = context.cookies()
            seen = {f"{c['name']}:{c.get('domain', '')}" for c in all_cookies}
            for c in raw:
                key = f"{c['name']}:{c.get('domain', '')}"
                if key not in seen:
                    all_cookies.append(c)
        except Exception:
            pass

        # Extract localStorage
        storage = {}
        for school in ["westu-sor", "theheights-sor"]:
            try:
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
    now = datetime.now(timezone.utc)
    cookie_list = []
    soonest_expiry = None
    for c in all_cookies:
        expires_raw = c.get("expires")
        expires_ts = None
        if expires_raw and expires_raw > 0:
            expires_ts = datetime.fromtimestamp(expires_raw, tz=timezone.utc).isoformat()
            exp_dt = datetime.fromtimestamp(expires_raw, tz=timezone.utc)
            if soonest_expiry is None or exp_dt < soonest_expiry:
                soonest_expiry = exp_dt
        cookie_list.append({
            "name": c.get("name", ""),
            "domain": c.get("domain", ""),
            "path": c.get("path", "/"),
            "value": c.get("value", ""),
            "expires": expires_ts,
            "httpOnly": c.get("httpOnly", False),
            "secure": c.get("secure", False),
            "sameSite": c.get("sameSite", "Lax"),
        })

    payload = {
        "extracted_at": now.isoformat(),
        "source": "okta_sso_extraction_from_mac",
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
    print(f"\nCookie domains:")
    for c in cookie_list:
        print(f"  {c['name']:30s} | {c['domain']}")
    if soonest_expiry:
        days_left = (soonest_expiry - now).days
        print(f"\nSoonest cookie expires in ~{days_left} days ({soonest_expiry.date()})")
    else:
        print("\nNo expiry dates found — cookies may be session cookies.")

    print(f"\nNext: copy the profile to the server:")
    print(f"  scp -r ~/.pike13_profile ubuntu@<server>:~/projects/hughrscott/NotesReminder/pike13_profile")
    return payload


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract Pike13 auth cookies via sor.okta.com SSO")
    parser.add_argument("--profile-dir", default="", help="Browser profile path (optional)")
    parser.add_argument("--output", default="pike13_cookies.json", help="Output JSON file")
    args = parser.parse_args()
    extract_cookies(args.profile_dir, args.output)