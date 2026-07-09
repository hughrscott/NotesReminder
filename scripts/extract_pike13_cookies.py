#!/usr/bin/env python3
"""Extract Pike13 auth cookies via sor.okta.com SSO authentication.

The script:
1. Opens a browser with a persistent profile
2. Navigates to sor.okta.com (the SSO gateway for all SOR services)
3. Waits for you to complete Okta login/MFA manually
4. Once authenticated, navigates to Pike13 to confirm staff access
5. Extracts all cookies (Okta + Pike13) and localStorage
6. Saves to pike13_cookies.json with secure permissions
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


def extract_cookies(profile_dir: str, output_path: str, chrome_channel: bool = False):
    """Open browser, authenticate via sor.okta.com, then extract Pike13 cookies."""

    # Create a fresh profile directory if none provided
    if not profile_dir:
        profile_dir = str(Path.home() / ".pike13_profile")
        Path(profile_dir).mkdir(parents=True, exist_ok=True)
        print(f"No profile dir specified — using {profile_dir}")

    profile_path = Path(profile_dir)
    if not profile_path.exists():
        profile_path.mkdir(parents=True, exist_ok=True)

    launch_kwargs = {"headless": False, "viewport": {"width": 1440, "height": 900}}

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(str(profile_path), **launch_kwargs)
        page = context.pages[0] if context.pages else context.new_page()

        # Step 1: Authenticate via sor.okta.com (the SSO gateway)
        print("=" * 60)
        print("STEP 1: Authenticating via sor.okta.com")
        print("=" * 60)
        print("Opening sor.okta.com in the browser...")
        print("Please complete Okta login/MFA as a STAFF member.")
        print("The script will wait up to 5 minutes for you to authenticate.")
        print()

        okta_url = "https://sor.okta.com"
        page.goto(okta_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)

        # Wait for Okta authentication (up to 5 min)
        # After Okta login, the user will be on the Okta dashboard
        # which shows app tiles or a welcome page
        deadline = time.time() + 300
        okta_authenticated = False
        while time.time() < deadline:
            page.wait_for_timeout(3000)
            current_url = page.url.lower()

            # Okta dashboard typically shows after login
            # Could be at sor.okta.com/app/UserHome or similar
            if "okta" in current_url:
                # Check if we're past the login form
                try:
                    body_text = page.locator("body").inner_text(timeout=3000)
                    lowered = body_text.lower()
                    # Okta dashboard has "My Apps" or user name or "Sign Out"
                    if any(marker in lowered for marker in ("my apps", "sign out", "logout", "dashboard")):
                        print("  Okta authentication detected!")
                        okta_authenticated = True
                        break
                except Exception:
                    pass

            # Or check if redirected away from okta login
            if not any(x in current_url for x in ("login", "signin", "sign_in", "authenticate")):
                if "okta" in current_url:
                    # Still on okta but past login
                    print(f"  On Okta page: {page.url}")
                    okta_authenticated = True
                    break

        if not okta_authenticated:
            print("  WARNING: Okta login not detected within 5 minutes.")
            print("  Continuing anyway — will try Pike13 directly.")

        # Step 2: Navigate to Pike13 to establish staff session
        print()
        print("=" * 60)
        print("STEP 2: Navigating to Pike13 (westu-sor) to confirm staff access")
        print("=" * 60)

        # The Okta SSO should redirect us through to Pike13 with staff auth
        pike13_url = "https://westu-sor.pike13.com/schedule"
        print(f"Opening {pike13_url}...")
        page.goto(pike13_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(5000)

        # Check if we need to complete an Okta→Pike13 redirect
        # Sometimes Okta SSO redirects through an intermediate page
        if "/accounts/sign_in" in page.url.lower():
            print("  Redirected to Pike13 sign-in. Waiting for SSO redirect...")
            page.wait_for_timeout(5000)
            # Try clicking sign in if there's a button (Okta SSO button)
            try:
                sign_in = page.locator('a:has-text("Sign In"), button:has-text("Sign In")')
                if sign_in.count() > 0:
                    print("  Clicking Sign In to trigger Okta SSO redirect...")
                    sign_in.first.click()
                    page.wait_for_timeout(10000)
            except Exception:
                pass

        current_url = page.url.lower()
        print(f"  Current URL: {page.url}")

        if "/schedule" in current_url and "sign_in" not in current_url:
            print("  Reached Pike13 schedule — authenticated!")
        else:
            print("  Not yet on schedule. Waiting up to 2 more minutes for SSO redirect...")
            deadline2 = time.time() + 120
            while time.time() < deadline2:
                page.wait_for_timeout(3000)
                if "/schedule" in page.url.lower() and "sign_in" not in page.url.lower():
                    print("  Reached Pike13 schedule — authenticated!")
                    break
            page.wait_for_timeout(3000)

        # Step 3: Also visit The Heights to capture that session
        print()
        print("Visiting The Heights (theheights-sor) to capture that session too...")
        page.goto("https://theheights-sor.pike13.com/schedule", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)
        print(f"  The Heights URL: {page.url}")

        # Step 4: Extract all cookies and storage
        print()
        print("=" * 60)
        print("Extracting cookies and storage...")
        print("=" * 60)

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
            existing_names = {f"{c['name']}:{c.get('domain','')}" for c in all_cookies}
            for c in all_cookies_raw:
                key = f"{c['name']}:{c.get('domain','')}"
                if key not in existing_names:
                    all_cookies.append(c)
        except Exception:
            pass

        # Extract storage
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
    now_utc = datetime.now(timezone.utc)
    cookie_list = []
    soonest_expiry = None
    for c in all_cookies:
        expires_raw = c.get("expires")
        if expires_raw and expires_raw > 0:
            expires_ts = datetime.fromtimestamp(expires_raw, tz=timezone.utc).isoformat()
        else:
            expires_ts = None
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
    if soonest_expiry:
        days_left = (soonest_expiry - now_utc).days
        print(f"Soonest cookie expires in ~{days_left} days ({soonest_expiry.date()})")
    else:
        print("No expiry dates found — cookies may be session cookies.")
    print(f"\nCookie domains:")
    for c in cookie_list:
        print(f"  {c['name']:30s} | {c['domain']}")
    print(f"\nNext step: Copy the profile to the Oracle Cloud server:")
    print(f"  scp -r ~/.pike13_profile ubuntu@<server>:~/projects/hughrscott/NotesReminder/pike13_profile")
    print(f"\nOr copy just the cookies JSON:")
    print(f"  scp {output_path} ubuntu@<server>:~/projects/hughrscott/NotesReminder/")

    return payload


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract Pike13 auth cookies via sor.okta.com SSO")
    parser.add_argument("--profile-dir", default="", help="Path to browser profile (optional)")
    parser.add_argument("--output", default="pike13_cookies.json", help="Output JSON file for cookies")
    parser.add_argument("--chrome-channel", action="store_true", help="Use system Chrome")
    args = parser.parse_args()
    extract_cookies(args.profile_dir, args.output, args.chrome_channel)