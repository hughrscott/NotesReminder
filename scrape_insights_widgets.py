#!/usr/bin/env python3
"""
scrape_insights_widgets.py — Click the graph icon (upper right) in Pike13 and
pull the insights widgets (new memberships this month, expiring memberships, etc.)
as a logged-in user. Captures BOTH the rendered data AND the underlying
/api/insights/widgets/* request + response so we can self-serve next time.

This is Hugh's own account, authenticated session — same as clicking through the UX.

Run: python3 scrape_insights_widgets.py [--school westu-sor]
"""
import asyncio
import sys
import json
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_insights_pull.json"

WIDGET_CAPTURE = {}  # name -> {request_url, response_keys, sample_rows}
API_CALLS = []


async def on_response(resp):
    url = resp.url
    if "/api/insights/widgets/" in url:
        try:
            body = await resp.body()
            top = []
            try:
                d = json.loads(body[:200000])
                if isinstance(d, dict):
                    for k, v in d.items():
                        if isinstance(v, list):
                            top.append(f"{k}: list[{len(v)}]")
                            if v and isinstance(v[0], dict):
                                WIDGET_CAPTURE.setdefault(
                                    url.split("/api/insights/widgets/")[-1].split("?")[0],
                                    {"sample_keys": list(v[0].keys())[:20], "count": len(v)},
                                )
                        else:
                            top.append(f"{k}: {type(v).__name__}")
            except Exception:
                pass
            API_CALLS.append({"url": url, "structure": top})
        except Exception:
            pass


async def click_graph_and_read(page):
    """Click the graph/insights icon (upper right), then read widgets."""
    # The graph icon — upper right. Try common selectors.
    candidates = [
        'a[href*="insights"]',
        'button[aria-label*="insight" i]',
        '[class*="insights"]',
        'a[href*="dashboard"]',
        'header a[title*="report" i]',
        'header a[title*="insight" i]',
        'svg',
    ]
    clicked = False
    for sel in candidates:
        loc = page.locator(sel).first
        if await loc.count() > 0:
            try:
                await loc.click(timeout=4000)
                clicked = True
                print(f"  clicked selector: {sel}")
                break
            except Exception:
                continue
    if not clicked:
        print("  graph icon not auto-clicked; navigating to /desk/insights fallback")
        await page.goto(
            "https://westu-sor.pike13.com/desk/insights", wait_until="networkidle", timeout=30000
        )
    await page.wait_for_timeout(6000)
    await page.wait_for_load_state("networkidle", timeout=30000)
    # Capture visible widget text
    body = await page.locator("body").inner_text()
    return body


async def main():
    school = sys.argv[sys.argv.index("--school") + 1] if "--school" in sys.argv else "westu-sor"
    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=school, headless=True, verbose=False
        )
        page = context.pages[0]
        page.on("response", on_response)

        print("[1] Clicking graph/insights icon (upper right)...")
        body = await click_graph_and_read(page)
        print(f"  visible body chars: {len(body)}")
        print("  --- first 800 chars of insights view ---")
        print(body[:800])

        print("\n[2] Widgets captured from /api/insights/widgets/* :")
        for name, info in WIDGET_CAPTURE.items():
            print(f"  {name}: {info.get('count')} rows, keys={info.get('sample_keys')}")

        OUT.write_text(
            json.dumps(
                {
                    "pulled_at": datetime.now(timezone.utc).isoformat(),
                    "school": school,
                    "insights_body_preview": body[:2000],
                    "widgets": WIDGET_CAPTURE,
                    "api_calls": API_CALLS,
                },
                indent=2,
            )
        )
        print(f"\nSaved -> {OUT}")
        await context.close()


if __name__ == "__main__":
    asyncio.run(main())
