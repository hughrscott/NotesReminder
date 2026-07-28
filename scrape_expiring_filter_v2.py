#!/usr/bin/env python3
"""Locate the border graph icon on /today, click it, open Expiring widget, click Filters, capture date controls + data."""
import asyncio, sys, json
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_expiring_pull.json"
SHOT_TODAY = MODELS / "pike13_today_border.png"
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
        await page.screenshot(path=str(SHOT_TODAY), full_page=False)
        print(f"  screenshot /today -> {SHOT_TODAY}")

        # Find the border graph icon: look for an element near the right edge with a chart/graph/insight class or svg
        icon_box = await page.evaluate(
            """() => {
                const vw = window.innerWidth;
                const candidates = [];
                document.querySelectorAll('a, button, div, span, svg').forEach(e => {
                    const r = e.getBoundingClientRect();
                    if (r.width === 0 || r.height === 0) return;
                    const cls = (e.className || '').toString();
                    const txt = (e.getAttribute('title')||e.getAttribute('aria-label')||'').toLowerCase();
                    const nearRight = r.right > vw - 80;  // within 80px of right border
                    if (nearRight && /chart|graph|insight|bar|analytics|report|trend/i.test(cls + ' ' + txt)) {
                        candidates.push({cls: cls.slice(0,50), txt: txt, x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), h: Math.round(r.height), right: Math.round(r.right)});
                    }
                });
                return candidates.slice(0, 15);
            }"""
        )
        print(f"  border icon candidates: {len(icon_box)}")
        for c in icon_box:
            print(f"    {c}")

        # Click the rightmost candidate (the border icon)
        clicked_icon = False
        if icon_box:
            target = max(icon_box, key=lambda c: c['right'])
            await page.mouse.click(target['x'] + target['w']//2, target['y'] + target['h']//2)
            clicked_icon = True
            print(f"  clicked border icon at ({target['x']+target['w']//2}, {target['y']+target['h']//2})")
        else:
            # fallback: try the sidebar Insights link exists here?
            print("  no border icon found by class; trying text 'Insights'")
            loc = page.locator('a:has-text("Insights")').first
            if await loc.count() > 0:
                await loc.click(timeout=4000)
                clicked_icon = True

        await page.wait_for_timeout(3500)
        await page.screenshot(path=str(SHOT_RESULT), full_page=False)

        # Now try to find + click a membership widget, then Filters
        for label in ["Expiring Memberships", "Memberships", "Expiring", "New Memberships"]:
            loc = page.locator(f"text={label}").first
            if await loc.count() > 0:
                await loc.click(timeout=4000)
                print(f"  opened widget: {label}")
                break
        await page.wait_for_timeout(2500)

        filt = page.locator('button.fa-filter, button:has-text("Filters")').first
        if await filt.count() > 0:
            await filt.click(timeout=4000)
            print("  clicked Filters")
            await page.wait_for_timeout(2500)
            await page.screenshot(path=str(SHOT_FILTER), full_page=False)
            date_controls = await page.evaluate(
                """() => Array.from(document.querySelectorAll('input, select, [class*=date], [class*=period], button')).map(e => ({t:(e.innerText||'').trim().slice(0,30), ph:e.getAttribute('placeholder')||'', type:e.getAttribute('type')||'', cls:e.className.toString().slice(0,40), val:e.value||''})).filter(x => /date|from|to|range|period|month|year|start|end|between/i.test(x.t+' '+x.ph+' '+x.cls+' '+x.type))"""
            )
            print(f"  date controls ({len(date_controls)}):")
            for c in date_controls[:12]:
                print(f"    {c}")

        await page.wait_for_timeout(2000)
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
