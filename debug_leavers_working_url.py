#!/usr/bin/env python3
"""Debug scraper - print actual visible table columns and data."""
import asyncio
import sys

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as playwright:
        context = await pike13_auto_auth.authenticate_pike13(
            playwright, school_subdomain="westu-sor", headless=True, verbose=False
        )
        page = await context.new_page()
        
        # Use the documented URL that works
        url = "https://westu-sor.pike13.com/desk/reports#/people/details?filters=(last_membership_end:!((btw:!('2026-01-01','2026-08-11'))))&sort=(col:last_membership_end,order:d)&hide=1,70,71,72,73,74,75,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75"
        
        print("Navigating to working URL...")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(10000)
        
        # Extract the actual table using a simpler approach
        result = await page.evaluate("""
            () => {
                const table = document.querySelector('table.data_table');
                if (!table) return {error: 'No table'};
                const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.innerText.trim());
                const data = [];
                table.querySelectorAll('tbody tr').forEach(tr => {
                    const tds = tr.querySelectorAll('td');
                    if (tds.length < 3) return;
                    const a = tr.querySelector('a[href*="/people/"]');
                    if (!a) return;
                    const m = a.href.match(/\\/people\\/(\\d+)\\/?$/);
                    if (!m) return;
                    const cells = {};
                    for (let i = 0; i < Math.min(tds.length, headers.length); i++) {
                        cells[headers[i]] = tds[i].innerText.trim();
                    }
                    if (!cells['Last Membership End Date']) return;
                    cells._pid = m[1];
                    cells._name = a.innerText.trim().split('\\n')[0];
                    data.push(cells);
                });
                return {headers, data: data.slice(0, 10)};
            }
        """)
        
        print(f"Headers: {result.get('headers')}")
        print(f"Rows: {len(result.get('data', []))}")
        for r in result.get('data', []):
            print(f"  {r.get('_name')}: end_date={r.get('Last Membership End Date')}")
        
        # Also try body text parsing
        body = await page.evaluate("() => document.body.innerText")
        print(f"\nBody length: {len(body)}")
        if 'Last Membership End Date' in body:
            idx = body.index('Last Membership End Date')
            print(f"Found at {idx}:")
            print(body[idx:idx+1000])
        elif 'Full Name' in body:
            idx = body.index('Full Name')
            print(f"Found 'Full Name' at {idx}:")
            print(body[idx:idx+1000])
        
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())