#!/usr/bin/env python3
import argparse
import json
import shlex
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright


SERVICES = {
    "hubspot": "https://app.hubspot.com/home-beta",
    "dialpad": "https://dialpad.com/app/history/messages",
    "pike13-westu": "https://westu-sor.pike13.com",
    "pike13-heights": "https://theheights-sor.pike13.com",
}


HELP = """Commands:
  capture [label]        Save screenshot plus text/link/button JSON evidence.
  goto <url>             Navigate to a URL in the same authenticated session.
  click <text>           Click the first visible element matching text.
  wait [milliseconds]    Wait for the current page to finish rendering.
  url                    Print current URL and page title.
  help                   Show this help text.
  quit                   Close browser and exit.
"""


def parse_args():
    parser = argparse.ArgumentParser(
        description="Keep an authenticated browser open and accept read-only discovery commands."
    )
    parser.add_argument("service", choices=sorted(SERVICES))
    parser.add_argument("--url", help="Override the default URL.")
    parser.add_argument("--profile-root", default="browser_profiles")
    parser.add_argument("--evidence-dir", default="docs/discovery/evidence")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--wait-ms", type=int, default=5000)
    parser.add_argument("--text-limit", type=int, default=30000)
    parser.add_argument("--link-limit", type=int, default=500)
    return parser.parse_args()


def safe_label(value):
    cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in value)
    return cleaned.strip("_")[:80] or "capture"


def capture(page, args, evidence_dir, label):
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stem = f"{args.service}_{safe_label(label)}_{stamp}"
    json_path = evidence_dir / f"{stem}.json"
    screenshot_path = evidence_dir / f"{stem}.png"

    page.wait_for_timeout(args.wait_ms)
    screenshot_error = None
    try:
        page.screenshot(
            path=str(screenshot_path),
            full_page=False,
            timeout=10000,
            animations="disabled",
        )
        screenshot_value = str(screenshot_path.resolve())
    except Exception as exc:
        screenshot_error = str(exc)
        screenshot_value = None

    try:
        visible_text = page.locator("body").inner_text(timeout=10000)
    except Exception as exc:
        visible_text = f"<text capture failed: {exc}>"

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
        "label": label,
        "captured_at_utc": stamp,
        "url": page.url,
        "title": page.title(),
        "screenshot": screenshot_value,
        "screenshot_error": screenshot_error,
        "text": visible_text[: args.text_limit],
        "text_truncated": len(visible_text) > args.text_limit,
        "links": links,
        "buttons": buttons,
        "safety_boundary": "view/export only; no creates, edits, deletes, sends, or status updates",
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"captured_json={json_path.resolve()}", flush=True)
    if screenshot_value:
        print(f"captured_screenshot={screenshot_value}", flush=True)
    if screenshot_error:
        print(f"screenshot_error={screenshot_error}", flush=True)


def main():
    args = parse_args()
    url = args.url or SERVICES[args.service]
    profile_dir = Path(args.profile_root) / args.service
    evidence_dir = Path(args.evidence_dir)
    profile_dir.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            str(profile_dir),
            headless=args.headless,
            viewport={"width": 1440, "height": 1000},
            accept_downloads=True,
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(url, wait_until="domcontentloaded")

        print(f"service={args.service}", flush=True)
        print(f"profile_dir={profile_dir.resolve()}", flush=True)
        print(f"current_url={page.url}", flush=True)
        print(HELP, flush=True)

        while True:
            try:
                raw = input("discovery> ").strip()
            except EOFError:
                break
            if not raw:
                continue
            parts = shlex.split(raw)
            command = parts[0].lower()
            rest = parts[1:]
            if command == "quit":
                break
            if command == "help":
                print(HELP, flush=True)
            elif command == "url":
                print(f"url={page.url}", flush=True)
                print(f"title={page.title()}", flush=True)
            elif command == "wait":
                wait_ms = int(rest[0]) if rest else args.wait_ms
                page.wait_for_timeout(wait_ms)
                print(f"waited_ms={wait_ms}", flush=True)
            elif command == "goto":
                if not rest:
                    print("error=goto requires a URL", flush=True)
                    continue
                page.goto(rest[0], wait_until="domcontentloaded")
                print(f"url={page.url}", flush=True)
                print(f"title={page.title()}", flush=True)
            elif command == "click":
                if not rest:
                    print("error=click requires visible text", flush=True)
                    continue
                target = " ".join(rest)
                try:
                    page.get_by_text(target, exact=False).first.click(timeout=10000)
                    page.wait_for_timeout(args.wait_ms)
                    print(f"url={page.url}", flush=True)
                    print(f"title={page.title()}", flush=True)
                except Exception as exc:
                    print(f"error=click failed for {target}: {exc}", flush=True)
            elif command == "capture":
                capture(page, args, evidence_dir, " ".join(rest) if rest else "capture")
            else:
                print(f"error=unknown command {command}", flush=True)

        context.close()


if __name__ == "__main__":
    main()
