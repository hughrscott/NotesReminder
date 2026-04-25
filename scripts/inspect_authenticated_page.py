#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright


SERVICES = {
    "hubspot": "https://app.hubspot.com/home-beta",
    "dialpad": "https://dialpad.com/app/history/messages",
    "pike13-westu": "https://westu-sor.pike13.com",
    "pike13-heights": "https://theheights-sor.pike13.com",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Capture read-only page text and links from an authenticated profile."
    )
    parser.add_argument("service", choices=sorted(SERVICES))
    parser.add_argument("--url", help="Override the default URL.")
    parser.add_argument("--profile-root", default="browser_profiles")
    parser.add_argument("--evidence-dir", default="docs/discovery/evidence")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--wait-ms", type=int, default=5000)
    parser.add_argument("--text-limit", type=int, default=20000)
    parser.add_argument("--link-limit", type=int, default=300)
    return parser.parse_args()


def main():
    args = parse_args()
    url = args.url or SERVICES[args.service]
    profile_dir = Path(args.profile_root) / args.service
    evidence_dir = Path(args.evidence_dir)
    evidence_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = evidence_dir / f"{args.service}_inspect_{stamp}.json"

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            str(profile_dir),
            headless=args.headless,
            viewport={"width": 1440, "height": 1000},
            accept_downloads=True,
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(args.wait_ms)

        visible_text = page.locator("body").inner_text(timeout=10000)
        links = page.evaluate(
            """
            (limit) => Array.from(document.querySelectorAll('a[href]'))
              .slice(0, limit)
              .map((a) => ({
                text: (a.innerText || a.getAttribute('aria-label') || '').trim(),
                href: a.href
              }))
              .filter((row) => row.text || row.href)
            """,
            args.link_limit,
        )
        buttons = page.evaluate(
            """
            (limit) => Array.from(document.querySelectorAll('button,[role="button"]'))
              .slice(0, limit)
              .map((b) => ({
                text: (b.innerText || b.getAttribute('aria-label') || '').trim(),
                title: b.getAttribute('title') || ''
              }))
              .filter((row) => row.text || row.title)
            """,
            args.link_limit,
        )

        payload = {
            "service": args.service,
            "captured_at_utc": stamp,
            "url": page.url,
            "title": page.title(),
            "profile_dir": str(profile_dir.resolve()),
            "text": visible_text[: args.text_limit],
            "text_truncated": len(visible_text) > args.text_limit,
            "links": links,
            "buttons": buttons,
            "safety_boundary": "view/export only; no creates, edits, deletes, sends, or status updates",
        }
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"evidence={out_path.resolve()}")
        print(f"url={payload['url']}")
        print(f"title={payload['title']}")
        context.close()


if __name__ == "__main__":
    main()
