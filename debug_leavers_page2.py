#!/usr/bin/env python3
"""Debug scraper - wait longer for table to render."""
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
        
        url = "https://westu-sor.pike13.com/desk/reports#/people/details?filters=(last_membership_end:!((btw:!('2026-01-01','2026-08-11'))))&sort=(col:last_membership_end,order:d)&hide=1,70,71,72,73,74,75,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75"
        
        print(f"Navigating to: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(15000)
        
        for i in range(5):
            tables = await page.evaluate("""() => {
                const tables = document.querySelectorAll('table');
                const results = [];
                tables.forEach((t, idx) => {
                    const firstRowCells = [];
                    const tbody = t.querySelector('tbody');
                    if (tbody) {
                        const firstTr = tbody.querySelector('tr');
                        if (firstTr) {
                            firstTr.querySelectorAll('td').forEach(td => {
                                firstRowCells.push(td.innerText.trim());
                            });
                        }
                    }
                    results.push({
                        index: idx,
                        class: t.className,
                        rows: t.querySelectorAll('tr').length,
                        headers: Array.from(t.querySelectorAll('th')).map(h => h.innerText.trim()),
                        first_row_cells: firstRowCells,
                    });
                });
                return results;
            }""")
            print(f"\nCheck {i+1}:")
            for t in tables:
                if t['rows'] > 0:
                    print(f"  Table {t['index']}: class='{t['class']}', rows={t['rows']}, headers={t['headers'][:5]}")
                    if t['first_row_cells']:
                        print(f"    First row: {t['first_row_cells']}")
            
            body = await page.evaluate("() => document.body.innerText")
            if "Loading" in body:
                print("  Still loading...")
            else:
                print("  Page fully loaded!")
            
            await page.wait_for_timeout(5000)
        
        await context.close()

asyncio.run(main())