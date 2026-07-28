#!/usr/bin/env python3
"""
scrape_insights_widgets_v2.py — Go to the staff dashboard, find the graph/insights
icon, click it, capture the new/expiring membership widgets + underlying API calls.
Authenticated session = Hugh's own account.
"""
import asyncio
import sys
import json
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_insights_pull.json"
SHOT = MODELS / "pike13_dashboard.png"

WIDGETS = {}
API_CALLS = []


async def on_response(resp):
    url = resp.url
    if "/api/insights/widgets/" in url:
        try:
            body = await resp.body()
            d = json.loads(body[:300000])
            name = url.split("/api/insights/widgets/")[-1].split("?")[0]
            info = {"url": url, "rows": 0, "sample_keys": [], "sample": None}
            if isinstance(d, dict):
                for k, v in d.items():
                    if isinstance(v, list):
                        info["rows"] = len(v)
                        if v and isinstance(v[0], dict):
                            info["sample_keys"] = list(v[0].keys())[:25]
                            info["sample"] = v[0]
            WIDGETS[name] = info
            API_CALLS.append(url)
        except Exception:
            pass


async def main():
    school = sys.argv[sys.argv.index("--school") + 1] if "--school" in sys.argv else "westu-sor"
    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=school, headless=True, verbose=False
        )
        page = context.pages[0]
        page.on("response", on_response)

        # Go to the staff dashboard (where the graph icon lives, upper right)
        print("[1] Navigating to dashboard /today ...")
        await page.goto(f"https://{school}.pike13.com/today", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(4000)

        # Screenshot so we can SEE the graph icon
        await page.screenshot(path=str(SHOT), full_page=False)
        print(f"  screenshot -> {SHOT}")

        # Dump clickable header elements to find the icon
        header = await page.evaluate(
            """() => {
                const out = [];
                document.querySelectorAll('header a, header button, [class*=header] a, [class*=topbar] a, [class*=navbar] a').forEach(e => {
                    out.push({tag: e.tagName, text: (e.innerText||'').trim().slice(0,40), href: e.getAttribute('href')||'', cls: e.className.toString().slice(0,60)});
                });
                return out.slice(0, 40);
            }"""
        )
        print("  header elements:")
        for h in header:
            print(f"    {h}")

        # Try clicking anything that looks like a graph/insights/chart/report icon in header
        for sel in [
            'header a[href*="insights"]',
            'header a[href*="report"]',
            'header button[aria-label*="insight" i]',
            'header a[title*="insight" i]',
            'header svg',
            '[class*=chart]',
            '[class*=graph]',
            'a[href*="/desk/insights"]',
        ]:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                try:
                    await loc.click(timeout=4000)
                    print(f"  clicked: {sel}")
                    await page.wait_for_timeout(5000)
                    await page.wait_for_load_state("networkidle", timeout=20000)
                    break
                except Exception as e:
                    print(f"  click failed {sel}: {str(e)[:80]}")

        # Capture whatever widgets fired
        print(f"\n[2] Widgets captured ({len(WIDGETS)}):")
        for name, info in WIDGETS.items():
            print(f"  {name}: {info['rows']} rows | keys={info['sample_keys'][:8]}")

        OUT.write_text(
            json.dumps(
                {
                    "pulled_at": datetime.now(timezone.utc).isoformat(),
                    "school": school,
                    "widgets": WIDGETS,
                    "api_calls": API_CALLS,
                },
                indent=2,
                default=str,
            )
        )
        print(f"\nSaved -> {OUT}")
        await context.close()


if __name__ == "__main__":
    asyncio.run(main())
