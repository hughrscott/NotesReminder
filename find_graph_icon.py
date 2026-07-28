#!/usr/bin/env python3
"""Find the graph/insights icon by visiting likely pages and dumping report links."""
import asyncio, sys, json
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
OUT = MODELS / "pike13_report_links.json"
SHOTS = {}

async def main():
    school = sys.argv[sys.argv.index("--school") + 1] if "--school" in sys.argv else "westu-sor"
    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=school, headless=True, verbose=False
        )
        page = context.pages[0]
        report_links = {}

        candidates = [
            ("/desk/reports", "desk_reports"),
            ("/reports", "reports"),
            ("/desk/insights", "desk_insights"),
            ("/insights", "insights"),
            ("/people", "people"),
            ("/desk/clients", "desk_clients"),
        ]
        for url_path, label in candidates:
            try:
                await page.goto(f"https://{school}.pike13.com{url_path}", wait_until="networkidle", timeout=25000)
                await page.wait_for_timeout(3500)
                links = await page.evaluate(
                    """() => Array.from(document.querySelectorAll('a')).map(a => ({t:(a.innerText||'').trim().slice(0,40), h:a.getAttribute('href')||''})).filter(x => /report|insight|graph|analytic|metric|chart|member|expir/i.test(x.t+' '+x.h))"""
                )
                shots_path = MODELS / f"pike13_{label}.png"
                await page.screenshot(path=str(shots_path), full_page=False)
                SHOTS[label] = str(shots_path)
                report_links[label] = {"url": url_path, "links": links[:30], "matched": len(links)}
                print(f"[{label}] {url_path} -> {len(links)} report-ish links")
                for l in links[:8]:
                    print(f"    {l['t']!r} -> {l['h']}")
            except Exception as e:
                report_links[label] = {"url": url_path, "error": str(e)[:120]}
                print(f"[{label}] {url_path} -> ERR {str(e)[:80]}")

        # Also dump ALL header/sidebar nav links from the today page to find Reports
        await page.goto(f"https://{school}.pike13.com/today", wait_until="networkidle", timeout=25000)
        await page.wait_for_timeout(2000)
        nav = await page.evaluate(
            """() => Array.from(document.querySelectorAll('nav a, .nav a, header a, [class*=sidebar] a, [class*=menu] a, a')).map(a => ({t:(a.innerText||'').trim().slice(0,40), h:a.getAttribute('href')||''})).filter(x=>x.t && x.t.length>0)"""
        )
        report_links["ALL_NAV"] = nav[:60]

        OUT.write_text(json.dumps({"school": school, "shots": SHOTS, "pages": report_links}, indent=2, default=str))
        print(f"\nSaved -> {OUT}")
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
