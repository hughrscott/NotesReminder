#!/usr/bin/env python3
"""Find the Deals board entry point on the authenticated HubSpot dashboard.
Lists all clickable elements whose text/aria matches deals/pipeline/board.
Uses the warm hubspot profile copy (concurrent-safe)."""
import asyncio, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv(Path.home() / ".hermes" / "SOR" / ".sorenv")
from playwright.async_api import async_playwright

HUBSPOT_PROFILE = Path("browser_profiles/hubspot")

async def main():
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            str(HUBSPOT_PROFILE), headless=True, viewport={"width": 1440, "height": 1000})
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await page.goto("https://app.hubspot.com/", wait_until="domcontentloaded")
        await asyncio.sleep(8)
        # grab all <a> and role=link/button with href or text, dump those matching keywords
        els = await page.evaluate("""() => {
            const out = [];
            const nodes = document.querySelectorAll('a, button, [role=link], [role=button]');
            for (const n of nodes) {
                const t = (n.innerText||'').trim();
                const h = n.getAttribute('href')||'';
                if (/deal|pipeline|board|contact/i.test(t+h)) {
                    out.push({tag:n.tagName, text:t.slice(0,40), href:h.slice(0,60),
                              testid:n.getAttribute('data-test-id')||'',
                              cls:(n.className||'').toString().slice(0,50)});
                }
            }
            return out;
        }""")
        print("MATCHING NAV ELEMENTS:", len(els))
        for e in els[:40]:
            print(" ", e)
        # also dump the left rail nav container text
        nav_txt = await page.evaluate("""() => {
            const navs = document.querySelectorAll('nav, [role=navigation], aside, [data-test-id*=nav], header');
            let s='';
            for (const n of navs) s += n.innerText + '\\n---\\n';
            return s.slice(0, 1500);
        }""")
        Path("/tmp/hubspot_nav_dump.txt").write_text(nav_txt, encoding="utf-8")
        print("NAV DUMP written /tmp/hubspot_nav_dump.txt")
        await ctx.close()

asyncio.run(main())
