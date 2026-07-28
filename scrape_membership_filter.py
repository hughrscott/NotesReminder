#!/usr/bin/env python3
"""Open Insights -> membership widget -> Filters -> set This Month -> capture data API + shape."""
import asyncio, sys, json
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_membership_pull.json"
SHOT_FILTER = MODELS / "pike13_filter_modal.png"
SHOT_RESULT = MODELS / "pike13_membership_result.png"

WIDGET_DATA = {}

async def on_response(resp):
    url = resp.url
    if "/api/" in url:
        try:
            body = await resp.body()
            try:
                d = json.loads(body[:600000])
                rows = 0
                name = url.split("?")[0].split("/")[-1]
                if isinstance(d, dict):
                    for k, v in d.items():
                        if isinstance(v, list):
                            rows = len(v)
                if rows > 0 and ("member" in name or "client" in name or "expir" in name or "widget" in url):
                    WIDGET_DATA[name] = {"url": url, "rows": rows, "sample": d if rows <= 3 else {"_truncated_rows": rows, "first": d}}
                    print(f"  DATA {name}: {rows} rows  ({url[:90]})")
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

        # Reliable Insights entry
        await page.goto(f"https://{school}.pike13.com/desk/reports", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2500)
        await page.locator('a:has-text("Insights")').first.click(timeout=6000)
        await page.wait_for_timeout(3500)
        print("Insights panel open")

        # Click a membership widget
        target = None
        for label in ["Last Memberships", "Expiring Memberships", "New Memberships", "Memberships"]:
            loc = page.locator(f"text={label}").first
            if await loc.count() > 0:
                await loc.click(timeout=4000)
                target = label
                print(f"opened widget: {label}")
                break
        await page.wait_for_timeout(2500)

        # Open Filters
        filt = page.locator('button.fa-filter, button:has-text("Filters")').first
        if await filt.count() == 0:
            print("NO Filters button")
        else:
            await filt.click(timeout=4000)
            await page.wait_for_timeout(2500)
            await page.screenshot(path=str(SHOT_FILTER), full_page=False)
            # Dump filter modal form controls
            controls = await page.evaluate(
                """() => {
                    const out = [];
                    document.querySelectorAll('.modal, [class*=filter], [class*=modal]').forEach(m => {
                        m.querySelectorAll('input, select, button, [role=button], label').forEach(e => {
                            const o = {tag:e.tagName, type:e.getAttribute('type')||'', ph:e.getAttribute('placeholder')||'', cls:e.className.toString().slice(0,50), text:(e.innerText||'').trim().slice(0,40), val:e.value||''};
                            if (e.tagName==='SELECT') o.options = Array.from(e.options).map(x=>x.text+'='+x.value);
                            out.push(o);
                        });
                    });
                    // also any visible date/period controls anywhere
                    document.querySelectorAll('input[type=date], input[placeholder*=date i], select').forEach(e=>{
                        const o={tag:e.tagName, type:e.getAttribute('type')||'', ph:e.getAttribute('placeholder')||'', cls:e.className.toString().slice(0,50)};
                        if(e.tagName==='SELECT') o.options=Array.from(e.options).map(x=>x.text+'='+x.value);
                        out.push(o);
                    });
                    return out.slice(0,60);
                }"""
            )
            print(f"\nFilter controls ({len(controls)}):")
            for c in controls:
                extra = f" opts={c.get('options')[:8]}" if c.get('options') else ""
                print(f"  {c['tag']} type={c['type']!r} ph={c['ph']!r} text={c['text']!r} val={c['val']!r}{extra}")

            # Try to set a period: look for a select with month/quarter options
            period_set = False
            for c in controls:
                if c.get('options') and any('month' in o.lower() or 'quarter' in o.lower() or 'year' in o.lower() or 'week' in o.lower() for o in c['options']):
                    sel = page.locator(f"select").filter(has_text=c['text']) if c['text'] else page.locator('select').first
                    # pick 'This Month' if present
                    opts = c['options']
                    pick = next((o for o in opts if 'this month' in o.lower()), opts[0])
                    val = pick.split('=')[-1]
                    try:
                        await sel.select_option(value=val, timeout=4000)
                        period_set = True
                        print(f"  set period -> {pick}")
                        break
                    except Exception as e:
                        print(f"  period select failed: {str(e)[:60]}")
            if not period_set:
                # try date inputs
                dates = page.locator('input[type=date]')
                nd = await dates.count()
                if nd >= 2:
                    from datetime import date
                    start = date(date.today().year, date.today().month, 1).isoformat()
                    end = date.today().isoformat()
                    await dates.nth(0).fill(start)
                    await dates.nth(1).fill(end)
                    print(f"  set date range {start} -> {end}")
                    period_set = True

            # Click Apply / Update / Run
            for btn in ["Apply", "Update", "Run Report", "Save", "Filter"]:
                b = page.locator(f"button:has-text('{btn}')").first
                if await b.count() > 0:
                    await b.click(timeout=4000)
                    print(f"  clicked {btn}")
                    break
            await page.wait_for_timeout(4000)
            await page.screenshot(path=str(SHOT_RESULT), full_page=True)

        OUT.write_text(json.dumps({
            "pulled_at": datetime.now(timezone.utc).isoformat(),
            "school": school,
            "widget": target,
            "widget_data": WIDGET_DATA,
        }, indent=2, default=str))
        print(f"\nSaved -> {OUT}")
        print(f"Captured {len(WIDGET_DATA)} data-bearing widget responses")
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
