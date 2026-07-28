#!/usr/bin/env python3
"""Final step: open Insights -> click Expiring Memberships widget -> click Filters -> set date range -> capture API + data."""
import asyncio, sys, json
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_expiring_pull.json"
SHOT_FILTER = MODELS / "pike13_filters.png"
SHOT_RESULT = MODELS / "pike13_expiring_result.png"

WIDGET_DATA = {}
API_WITH_DATA = []

async def on_response(resp):
    url = resp.url
    if "/api/insights/widgets/" in url:
        try:
            body = await resp.body()
            try:
                d = json.loads(body[:500000])
                name = url.split("/api/insights/widgets/")[-1].split("?")[0]
                rows = 0
                if isinstance(d, dict):
                    for k, v in d.items():
                        if isinstance(v, list):
                            rows = len(v)
                if rows > 0:
                    WIDGET_DATA[name] = {"url": url, "rows": rows, "sample": d}
                    API_WITH_DATA.append({"name": name, "url": url, "rows": rows})
                    print(f"  DATA: {name} -> {rows} rows")
            except Exception:
                pass
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

        await page.goto(f"https://{school}.pike13.com/today", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2500)
        # Graph icon is on every page — click Insights link/icon
        await page.locator('a:has-text("Insights")').first.click(timeout=5000)
        await page.wait_for_timeout(3500)

        # Click the Expiring Memberships widget
        clicked_widget = False
        for label in ["Expiring Memberships", "Memberships", "Expiring"]:
            loc = page.locator(f"text={label}").first
            if await loc.count() > 0:
                await loc.click(timeout=4000)
                clicked_widget = True
                print(f"  opened widget: {label}")
                break
        if not clicked_widget:
            print("  could not open a membership widget")

        await page.wait_for_timeout(2500)
        # Click Filters
        filt = page.locator('button.fa-filter, button:has-text("Filters")').first
        if await filt.count() > 0:
            await filt.click(timeout=4000)
            print("  clicked Filters")
            await page.wait_for_timeout(2500)
            await page.screenshot(path=str(SHOT_FILTER), full_page=False)
            # Find date inputs / selects
            date_controls = await page.evaluate(
                """() => Array.from(document.querySelectorAll('input, select, [class*=date], [class*=period], button')).map(e => ({t:(e.innerText||'').trim().slice(0,30), ph:e.getAttribute('placeholder')||'', type:e.getAttribute('type')||'', cls:e.className.toString().slice(0,40), val:e.value||''})).filter(x => /date|from|to|range|period|month|year|start|end|between/i.test(x.t+' '+x.ph+' '+x.cls+' '+x.type))"""
            )
            print(f"  date controls ({len(date_controls)}):")
            for c in date_controls[:12]:
                print(f"    {c}")
            # Try to set a date range: look for date inputs and fill them
            date_inputs = page.locator('input[type="date"], input[placeholder*="date" i], input[class*=date]')
            n = await date_inputs.count()
            print(f"  date input elements: {n}")
            # Also try selects with period options
            selects = page.locator('select')
            ns = await selects.count()
            for i in range(ns):
                opts = await selects.nth(i).evaluate("el => Array.from(el.options).map(o=>o.text+o.value)")
                if any('month' in o.lower() or 'year' in o.lower() or 'week' in o.lower() or 'quarter' in o.lower() for o in opts):
                    print(f"  select[{i}] period options: {opts[:12]}")
        else:
            print("  no Filters button found")

        await page.wait_for_timeout(3000)
        await page.screenshot(path=str(SHOT_RESULT), full_page=False)

        OUT.write_text(json.dumps({
            "pulled_at": datetime.now(timezone.utc).isoformat(),
            "school": school,
            "widget_data": WIDGET_DATA,
            "api_with_data": API_WITH_DATA,
        }, indent=2, default=str))
        print(f"\nSaved -> {OUT}")
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
