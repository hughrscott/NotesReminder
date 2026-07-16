#!/usr/bin/env python3
"""Debug scraper - print raw body text to see what's actually on the page."""
import asyncio, sys
from pathlib import Path
from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).parent))
import pike13_auto_auth

SCHOOLS = {"westu-sor": "West U"}

async def scrape_leavers(school_slug, school_name):
    """Scrape all leavers for one school using the People Details report."""
    async with async_playwright() as playwright:
        context = await pike13_auto_auth.authenticate_pike13(
            playwright, school_subdomain=school_slug, headless=True, verbose=False
        )
        page = await context.new_page()
        
        url = f"https://{school_slug}.pike13.com/desk/reports#/people/details?filters=(last_membership_end:!((empty:!(no))))&sort=(col:last_membership_end,order:d)&hide=1,70,71,72,73,74,75,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75"
        
        print(f"  Navigating to {school_name} leavers report...")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(8000)  # let Pike13 table render
        
        # Try to get the table rows directly
        print("  Trying to extract table rows...")
        rows = await page.evaluate("""
            () => {
                const table = document.querySelector('table.data_table');
                if (!table) return { error: 'No table found' };
                const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.innerText.trim());
                const data = [];
                table.querySelectorAll('tbody tr').forEach(tr => {
                    const tds = tr.querySelectorAll('td');
                    if (tds.length < 3) return;
                    const a = tr.querySelector('a[href*="/people/"]');
                    if (!a) return;
                    const m = a.href.match(/\/people\/(\d+)\/?$/);
                    if (!m) return;
                    const cells = {};
                    for (let i = 0; i < Math.min(tds.length, headers.length); i++) {
                        cells[headers[i]] = tds[i].innerText.trim();
                    }
                    if (!cells['Last Membership End Date']) return;
                    cells._pid = m[1];
                    cells._name = a.innerText.trim().split('\n')[0];
                    data.push(cells);
                });
                return { headers, data };
            }
        """)
        
        await context.browser.close()
        
        if 'error' in rows:
            print(f"  Error: {rows['error']}")
            return {}
        
        print(f"  Headers: {rows['headers']}")
        print(f"  Rows found: {len(rows['data'])}")
        for r in rows['data'][:5]:
            print(f"    {r.get('_name')}: {r.get('Last Membership End Date')}")
        
        leavers = {}
        for r in rows['data']:
            name = r.get('_name', '').strip()
            end_date = r.get('Last Membership End Date', '').strip()
            if name and end_date:
                leavers[name.lower()] = {
                    "school": school_name,
                    "end_date": end_date,
                    "name": name
                }
        return leavers


async def main():
    all_leavers = {}
    
    for slug, name in SCHOOLS.items():
        print(f"Scraping {name} ({slug})...")
        leavers = await scrape_leavers(slug, name)
        all_leavers.update(leavers)
    
    print(f"\nTotal leavers: {len(all_leavers)}")


if __name__ == "__main__":
    asyncio.run(main())