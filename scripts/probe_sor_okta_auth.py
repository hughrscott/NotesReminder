#!/usr/bin/env python3
import argparse
import json
import sqlite3
import sys
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import ensure_lead_followup_schema, finish_import_run, start_import_run  # noqa: E402


DEFAULT_PROFILE = "browser_profiles/sor_okta"
PROBES = {
    "okta": "https://sor.okta.com",
    "pike13_westu": "https://westu-sor.pike13.com",
    "hubspot": "https://app.hubspot.com/contacts",
    "dialpad": "https://dialpad.com/conversationhistory",
    "gmail": "https://mail.google.com/mail/u/0/#inbox",
}


def classify_url(url, body_text=""):
    lowered = (url or "").lower()
    body = (body_text or "").lower()
    if any(marker in lowered for marker in ("/login", "/signin", "/sign_in", "accounts.google.com", "okta.com/signin")):
        return "needs_login"
    if "access denied" in body or "not authorized" in body or "403" in body:
        return "blocked"
    if "mail.google.com" in lowered and ("inbox" in body or "compose" in body or "gmail" in body):
        return "authenticated"
    if "pike13.com" in lowered and "accounts/sign_in" not in lowered:
        return "authenticated"
    if "hubspot.com" in lowered and "login" not in lowered:
        return "authenticated"
    if "dialpad.com" in lowered and "login" not in lowered:
        return "authenticated"
    if "okta.com" in lowered and "signin" not in lowered:
        return "authenticated"
    return "unknown"


def run_probe(profile_dir, headless=True, interactive_login=False, login_timeout=300, chrome_channel=False):
    launch_kwargs = {
        "headless": headless and not interactive_login,
        "viewport": {"width": 1440, "height": 1000},
    }
    if chrome_channel:
        launch_kwargs["channel"] = "chrome"
    results = {}
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(str(profile_dir), **launch_kwargs)
        try:
            if interactive_login:
                page = context.pages[0] if context.pages else context.new_page()
                page.goto(PROBES["okta"], wait_until="domcontentloaded", timeout=60000)
                print("Complete Okta login/MFA in the opened browser, then press Enter here.")
                input()
                try:
                    page.wait_for_load_state("networkidle", timeout=login_timeout * 1000)
                except PlaywrightTimeoutError:
                    pass
            for name, url in PROBES.items():
                page = context.new_page()
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    try:
                        page.wait_for_load_state("networkidle", timeout=15000)
                    except PlaywrightTimeoutError:
                        pass
                    text = page.locator("body").inner_text(timeout=10000)
                    status = classify_url(page.url, text[:4000])
                    results[name] = {
                        "status": status,
                        "final_url": page.url,
                    }
                except Exception as exc:
                    results[name] = {"status": "error", "error": str(exc)[:240], "final_url": page.url if page else url}
                finally:
                    page.close()
        finally:
            context.close()
    return results


def main():
    parser = argparse.ArgumentParser(description="Probe shared SOR Okta SSO profile access across lead-intelligence tools.")
    parser.add_argument("--profile-dir", default=DEFAULT_PROFILE)
    parser.add_argument("--db", help="Optional working DB path to record source_import_runs metadata.")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--interactive-login", action="store_true")
    parser.add_argument("--login-timeout", type=int, default=300)
    parser.add_argument("--chrome-channel", action="store_true")
    args = parser.parse_args()

    run_id = None
    conn = None
    if args.db:
        conn = sqlite3.connect(args.db)
        ensure_lead_followup_schema(conn)
        run_id = start_import_run(conn, "sor_okta_auth_probe", Path(__file__).name, metadata={"profile_dir": args.profile_dir})
        conn.commit()
    try:
        results = run_probe(
            Path(args.profile_dir),
            headless=args.headless,
            interactive_login=args.interactive_login,
            login_timeout=args.login_timeout,
            chrome_channel=args.chrome_channel,
        )
        status = "success" if all(item["status"] == "authenticated" for item in results.values()) else "partial"
        if conn:
            finish_import_run(conn, run_id, status, rows_seen=len(results), metadata={"results": results})
            conn.commit()
        print(json.dumps({"status": status, "results": results}, indent=2, sort_keys=True))
    except Exception as exc:
        if conn:
            finish_import_run(conn, run_id, "error", error=str(exc)[:240])
            conn.commit()
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
