#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright


SERVICES = {
    "hubspot": "https://app.hubspot.com",
    "dialpad": "https://dialpad.com/app/history/messages",
    "pike13-westu": "https://westu-sor.pike13.com",
    "pike13-heights": "https://theheights-sor.pike13.com",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Open a persistent browser profile for read-only authenticated discovery."
    )
    parser.add_argument("service", choices=sorted(SERVICES))
    parser.add_argument("--url", help="Override the default URL for the service.")
    parser.add_argument(
        "--profile-root",
        default="browser_profiles",
        help="Directory for persistent browser profiles.",
    )
    parser.add_argument(
        "--evidence-dir",
        default="docs/discovery/evidence",
        help="Directory for session metadata and screenshots.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run headless. Use headed mode for first Okta/MFA login.",
    )
    parser.add_argument(
        "--no-screenshot",
        action="store_true",
        help="Do not save a screenshot when the session pauses.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    url = args.url or SERVICES[args.service]
    profile_dir = Path(args.profile_root) / args.service
    evidence_dir = Path(args.evidence_dir)
    evidence_dir.mkdir(parents=True, exist_ok=True)
    profile_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    metadata_path = evidence_dir / f"{args.service}_{stamp}.json"
    screenshot_path = evidence_dir / f"{args.service}_{stamp}.png"

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            str(profile_dir),
            headless=args.headless,
            viewport={"width": 1440, "height": 1000},
            accept_downloads=True,
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(url, wait_until="domcontentloaded")

        print(f"service={args.service}")
        print(f"url={url}")
        print(f"profile_dir={profile_dir.resolve()}")
        print("Complete login/navigation in the browser, then press Enter here.")
        input()

        final_url = page.url
        title = page.title()
        metadata = {
            "service": args.service,
            "started_at_utc": stamp,
            "initial_url": url,
            "final_url": final_url,
            "title": title,
            "profile_dir": str(profile_dir.resolve()),
            "screenshot": None,
            "screenshot_error": None,
            "safety_boundary": "view/export only; no creates, edits, deletes, sends, or status updates",
        }

        if not args.no_screenshot:
            try:
                page.screenshot(
                    path=str(screenshot_path),
                    full_page=False,
                    timeout=10000,
                    animations="disabled",
                )
                metadata["screenshot"] = str(screenshot_path.resolve())
            except Exception as exc:
                metadata["screenshot_error"] = str(exc)

        metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        print(f"metadata={metadata_path.resolve()}")
        if not args.no_screenshot:
            print(f"screenshot={screenshot_path.resolve()}")
        context.close()


if __name__ == "__main__":
    main()
