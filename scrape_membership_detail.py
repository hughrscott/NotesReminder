#!/usr/bin/env python3
"""Robust: click membership KPI card -> detail list -> Filters -> set date -> capture table + API + Export CSV."""
import asyncio, sys, json
from pathlib import Path
from datetime import datetime, timezone, date

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_membership_detail.json"
CSV_OUT = MODELS / "pike13_last_memberships_export.csv"
SHOT_DETAIL = MODELS / "pike13_detail_view.png"
SHOT_FILTER = MODELS / "pike13_filter_modal2.png"
SHOT_RESULT = MODELS / "pike13_detail_result.png"

API_CALLS = []
TABLE_ROWS = {}

async def on_response(resp):
    url = resp.url
    if "/api/" in url:
        try:
            body = await resp.body()
            try:
                d = json.loads(body[:600000])
                name = url.split("?")[0].split("/")[-1]
                rows = 0
                if isinstance(d, dict):
                    for k, v in d.items():
                        if isinstance(v, list):
                            rows = len(v)
                if rows > 0 and ("people" in url or "client" in url or "member" in name or "widget" in url):
                    API_CALLS.append({"name": name, "url": url, "rows": rows})
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

        await page.goto(f"https://{school}.pike13.com/desk/reports", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2500)
        await page.locator('a:has-text("Insights")').first.click(timeout=6000)
        await page.wait_for_timeout(3500)

        # Robust card click: find the card containing the label, click its container
        label = "Last Memberships"
        card = page.locator(f"div:has-text('{label}')").filter(has_text=label).first
        # fallback: click the text element's nearest clickable card
        clicked = False
        for sel in [
            f"div.card:has-text('{label}')",
            f"[class*=card]:has-text('{label}')",
            f"text={label}",
        ]:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                try:
                    await loc.click(timeout=5000)
                    clicked = True
                    print(f"clicked card via: {sel}")
                    break
                except Exception:
                    continue
        if not clicked:
            print("FAILED to click card")

        # Wait for detail view (Filters button in toolbar)
        try:
            await page.locator('button:has-text("Filters")').wait_for(timeout=8000)
            print("detail view open (Filters present)")
        except Exception:
            print("detail view did NOT open")
        await page.wait_for_timeout(2000)
        await page.screenshot(path=str(SHOT_DETAIL), full_page=True)

        # Scrape the visible table rows (client + plan)
        rows = await page.evaluate(
            """() => Array.from(document.querySelectorAll('table tbody tr, .list-row, [class*=row]')).map(r => (r.innerText||'').replace(/\\n+/g,' | ').trim()).filter(t => t.length > 3).slice(0,40)"""
        )
        TABLE_ROWS["last_memberships"] = rows
        print(f"\nTable rows ({len(rows)}):")
        for r in rows[:10]:
            print(f"  {r[:100]}")

        # Open Filters
        filt = page.locator('button:has-text("Filters")').first
        if await filt.count() > 0:
            await filt.click(timeout=4000)
            await page.wait_for_timeout(2500)
            await page.screenshot(path=str(SHOT_FILTER), full_page=False)
            controls = await page.evaluate(
                """() => {
                    const out=[];
                    document.querySelectorAll('.modal, [class*=filter_modal], [class*=modal]').forEach(m=>{
                        m.querySelectorAll('input,select,button,label').forEach(e=>{
                            const o={tag:e.tagName,type:e.getAttribute('type')||'',ph:e.getAttribute('placeholder')||'',cls:e.className.toString().slice(0,50),text:(e.innerText||'').trim().slice(0,40),val:e.value||''};
                            if(e.tagName==='SELECT') o.options=Array.from(e.options).map(x=>x.text+'='+x.value);
                            out.push(o);
                        });
                    });
                    return out.slice(0,50);
                }"""
            )
            print(f"\nFilter modal controls ({len(controls)}):")
            for c in controls:
                extra = f" opts={c.get('options')[:10]}" if c.get('options') else ""
                print(f"  {c['tag']} type={c['type']!r} ph={c['ph']!r} text={c['text']!r}{extra}")
            # Try set This Month on a period select
            for c in controls:
                if c.get('options') and any('month' in o.lower() or 'quarter' in o.lower() or 'year' in o.lower() or 'week' in o.lower() or 'today' in o.lower() or 'range' in o.lower() for o in c['options']):
                    sel = page.locator('select').filter(has_text=c['text']) if c['text'] else page.locator('select').first
                    pick = next((o for o in c['options'] if 'this month' in o.lower() or 'month' in o.lower()), c['options'][0])
                    val = pick.split('=')[-1]
                    try:
                        await sel.select_option(value=val, timeout=4000)
                        print(f"  set period -> {pick}")
                        break
                    except Exception as e:
                        print(f"  period select failed: {str(e)[:50]}")
            # Apply
            for btn in ["Apply", "Update", "Run", "Filter", "Save"]:
                b = page.locator(f"button:has-text('{btn}')").first
                if await b.count() > 0:
                    await b.click(timeout=4000)
                    print(f"  clicked {btn}")
                    break
            await page.wait_for_timeout(3000)
            await page.screenshot(path=str(SHOT_RESULT), full_page=True)
            rows2 = await page.evaluate(
                """() => Array.from(document.querySelectorAll('table tbody tr, .list-row, [class*=row]')).map(r => (r.innerText||'').replace(/\\n+/g,' | ').trim()).filter(t => t.length>3).slice(0,60)"""
            )
            TABLE_ROWS["after_filter"] = rows2
            print(f"\nAfter filter: {len(rows2)} rows")

        # Try Export to CSV
        export = page.locator('button:has-text("Export")').first
        if await export.count() > 0:
            try:
                # Set download path
                import os
                os.makedirs(MODELS, exist_ok=True)
                async with page.expect_download(timeout=15000) as dl:
                    await export.click(timeout=4000)
                download = await dl.value
                await download.save_as(str(CSV_OUT))
                print(f"  EXPORTED CSV -> {CSV_OUT}")
            except Exception as e:
                print(f"  export failed: {str(e)[:80]}")

        OUT.write_text(json.dumps({
            "pulled_at": datetime.now(timezone.utc).isoformat(),
            "school": school,
            "tables": TABLE_ROWS,
            "api_calls": API_CALLS[-20:],
        }, indent=2, default=str))
        print(f"\nSaved -> {OUT}")
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
