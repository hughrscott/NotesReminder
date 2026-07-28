#!/usr/bin/env python3
"""Click the Insights panel (graph icon / Insights link) and capture new/expiring membership widgets."""
import asyncio, sys, json
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_insights_pull.json"
SHOT = MODELS / "pike13_insights_panel.png"

WIDGETS = {}
API_CALLS = []

async def on_response(resp):
    url = resp.url
    if "/api/insights/widgets/" in url:
        try:
            body = await resp.body()
            d = json.loads(body[:400000])
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

        print("[1] Going to /desk/reports ...")
        await page.goto(f"https://{school}.pike13.com/desk/reports", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(3000)

        # Try the sidebar Insights link (bar-chart icon) and any graph-icon element
        clicked = False
        for sel in [
            'a:has-text("Insights")',
            '[class*=sidebar] a:has-text("Insights")',
            'a[href=""]:has-text("Insights")',
            '[class*=chart]',
            '[class*=graph]',
            'button:has-text("Insights")',
        ]:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                try:
                    await loc.click(timeout=4000)
                    clicked = True
                    print(f"  clicked: {sel}")
                    break
                except Exception as e:
                    print(f"  fail {sel}: {str(e)[:60]}")
        if not clicked:
            print("  Insights not auto-clicked")

        await page.wait_for_timeout(5000)
        await page.wait_for_load_state("networkidle", timeout=20000)
        await page.screenshot(path=str(SHOT), full_page=True)
        print(f"  screenshot -> {SHOT}")

        body = await page.locator("body").inner_text()
        # Look for the specific widget names
        for kw in ["New Clients", "First Memberships", "Expiring", "New Memberships", "Last Visited"]:
            if kw.lower() in body.lower():
                print(f"  FOUND widget text: {kw}")

        print(f"\n[2] Widgets captured ({len(WIDGETS)}):")
        for name, info in WIDGETS.items():
            print(f"  {name}: {info['rows']} rows | keys={info['sample_keys'][:8]}")

        OUT.write_text(json.dumps({
            "pulled_at": datetime.now(timezone.utc).isoformat(),
            "school": school,
            "widgets": WIDGETS,
            "api_calls": API_CALLS,
            "body_preview": body[:1500],
        }, indent=2, default=str))
        print(f"\nSaved -> {OUT}")
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
