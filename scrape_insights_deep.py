#!/usr/bin/env python3
"""Deep-dive: capture Insights KPI card data + click into Expiring/New Memberships widget to find date-filter API."""
import asyncio, sys, json
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_insights_deep.json"
SHOT1 = MODELS / "pike13_widget_click.png"
SHOT2 = MODELS / "pike13_filter_ui.png"

ALL_API = {}
CARD_NUMBERS = {}

async def on_response(resp):
    url = resp.url
    if "/api/" in url and ("insight" in url or "widget" in url or "report" in url or "dashboard" in url or "metric" in url):
        try:
            body = await resp.body()
            try:
                d = json.loads(body[:400000])
                ALL_API[url] = {"type": "json", "keys": (list(d.keys())[:15] if isinstance(d, dict) else f"list[{len(d)}]")}
            except Exception:
                ALL_API[url] = {"type": "nonjson", "len": len(body)}
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

        await page.goto(f"https://{school}.pike13.com/desk/reports", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(3000)
        await page.locator('a:has-text("Insights")').first.click(timeout=5000)
        await page.wait_for_timeout(4000)

        # Capture the rendered KPI card numbers
        cards = await page.evaluate(
            """() => Array.from(document.querySelectorAll('*')).filter(e => /New Clients|First Memberships|Last Memberships|Expiring|New Memberships/i.test(e.innerText||'')).slice(0,20).map(e => (e.innerText||'').trim().replace(/\\n/g,' ').slice(0,80))"""
        )
        CARD_NUMBERS["cards"] = cards
        print("Rendered cards:")
        for c in cards[:12]:
            print(f"  {c}")

        # Click into the "Memberships" or "Expiring" widget to open filters
        for label in ["Expiring Memberships", "New Memberships", "Last Memberships", "Memberships"]:
            loc = page.locator(f"text={label}").first
            if await loc.count() > 0:
                try:
                    await loc.click(timeout=4000)
                    print(f"\n  clicked widget: {label}")
                    await page.wait_for_timeout(3500)
                    await page.screenshot(path=str(SHOT1), full_page=False)
                    # Look for filter/date controls
                    filter_info = await page.evaluate(
                        """() => Array.from(document.querySelectorAll('button, a, input, [class*=filter], [class*=date]')).map(e => ({t:(e.innerText||'').trim().slice(0,40), ph:e.getAttribute('placeholder')||'', cls:e.className.toString().slice(0,50)})).filter(x => /filter|date|range|from|to|calendar|period|month/i.test(x.t+' '+x.ph+' '+x.cls))"""
                    )
                    CARD_NUMBERS[f"filters_after_{label}"] = filter_info
                    print(f"  filter controls found: {len(filter_info)}")
                    for f in filter_info[:10]:
                        print(f"    {f}")
                    break
                except Exception as e:
                    print(f"  click {label} failed: {str(e)[:60]}")

        OUT.write_text(json.dumps({
            "pulled_at": datetime.now(timezone.utc).isoformat(),
            "school": school,
            "cards": CARD_NUMBERS,
            "api_calls": list(ALL_API.keys())[:40],
            "api_detail": dict(list(ALL_API.items())[:15]),
        }, indent=2, default=str))
        print(f"\nSaved -> {OUT}")
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
