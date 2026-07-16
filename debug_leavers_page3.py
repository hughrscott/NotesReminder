#!/usr/bin/env python3
"""Debug scraper - wait longer for table to fully render."""
import asyncio, sys
from pathlib import Path

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as playwright:
        context = await pike13_auto_auth.authenticate_pike13(
            playwright, school_subdomain="westu-sor", headless=True, verbose=False
        )
        page = await context.new_page()
        
        # Use the documented URL from pike13-leaver-confirmation.md
        url = "https://westu-sor.pike13.com/desk/reports#/people/details?filters=(last_membership_end:!((btw:!('2026-01-01','2026-08-11'))))&sort=(col:last_membership_end,order:d)&hide=1,70,71,72,73,74,75,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75"
        
        print(f"Navigating to: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(15000)  # Wait much longer for virtualized table
        
        for check in range(1, 10):
            tables = await page.evaluate("""
                () => {
                    const tables = document.querySelectorAll('table');
                    return Array.from(tables).map((t, i) => ({
                        index: i,
                        class: t.className,
                        rows: t.querySelectorAll('tr').length,
                        headers: Array.from(t.querySelectorAll('thead th')).map(th => th.innerText.trim()),
                        first_rows: Array.from(t.querySelectorAll('tbody tr')).slice(0, 3).map(tr => 
                            Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim())
                        )
                    }));
                }
            """)
            
            print(f"\nCheck {check}:")
            for t in tables:
                if t['class'] == 'data_table ng-scope' or 'data_table' in t['class']:
                    print(f"  Table {t['index']}: class={t['class']}, rows={t['rows']}")
                    print(f"    Headers: {t['headers']}")
                    if t['first_rows']:
                        print(f"    First rows: {t['first_rows']}")
            
            await page.wait_for_timeout(5000)
        
        # Also get body text for TSV parsing
        body = await page.evaluate("() => document.body.innerText")
        print(f"\nBody text length: {len(body)}")
        if 'Last Membership End Date' in body:
            idx = body.index('Last Membership End Date')
            print(f"Found 'Last Membership End Date' at position {idx}")
            print(body[idx:idx+2000])
        elif 'Full Name' in body:
            idx = body.index('Full Name')
            print(f"Found 'Full Name' at position {idx}")
            print(body[idx:idx+2000])
        else:
            print("Neither 'Last Membership End Date' nor 'Full Name' found in body")

if __name__ == "__main__":
    asyncio.run(main())