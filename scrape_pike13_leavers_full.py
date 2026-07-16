#!/usr/bin/env python3
"""Scrape Pike13 People Details report for ALL leaver dates (full history).
Uses the documented pattern from pike13-leaver-confirmation.md"""
import asyncio, json, sys
from pathlib import Path
from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).parent))
import pike13_auto_auth

MODELS_DIR = Path(__file__).parent / "models"
SCHOOLS = {"westu-sor": "West U", "theheights-sor": "The Heights"}

async def scrape_leavers(school_slug, school_name):
    """Scrape all leavers for one school using the People Details report."""
    async with async_playwright() as playwright:
        context = await pike13_auto_auth.authenticate_pike13(
            playwright, school_subdomain=school_slug, headless=True, verbose=False
        )
        page = await context.new_page()
        
        # Use the documented URL pattern with empty:no filter for all dates
        url = f"https://{school_slug}.pike13.com/desk/reports#/people/details?filters=(last_membership_end:!((empty:!(no))))&sort=(col:last_membership_end,order:d)&hide=1,70,71,72,73,74,75,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75"
        
        print(f"  Navigating to {school_name} leavers report...")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(5000)  # let Pike13 table render
        
        # Extract using the documented pattern - get body text and parse
        body_text = await page.evaluate("() => document.body.innerText")
        
        await context.browser.close()
        
        # Parse the TSV-like output
        lines = body_text.strip().split('\n')
        leavers = {}
        
        # Find the header line "Full Name" and data rows
        in_data = False
        for line in lines:
            line = line.strip()
            if line == "Full Name":
                in_data = True
                continue
            if "results filtered" in line.lower():
                break
            if in_data and line:
                # TSV format: name \t date
                parts = line.split('\t')
                if len(parts) >= 2:
                    name = parts[0].strip()
                    end_date = parts[1].strip()
                    if name and end_date:
                        name_lower = name.lower()
                        leavers[name_lower] = {
                            "school": school_name,
                            "end_date": end_date,
                            "name": name
                        }
        
        print(f"  {school_name}: {len(leavers)} leavers found")
        return leavers


async def main():
    all_leavers = {}
    
    for slug, name in SCHOOLS.items():
        print(f"Scraping {name} ({slug})...")
        leavers = await scrape_leavers(slug, name)
        all_leavers.update(leavers)
    
    # Save combined
    out_path = MODELS_DIR / "pike13_leavers_full_history.json"
    json.dump(all_leavers, open(out_path, "w"), indent=2)
    print(f"\nTotal leavers: {len(all_leavers)} → {out_path}")
    
    # Also show date range
    from datetime import datetime
    dates = []
    for v in all_leavers.values():
        try:
            dates.append(datetime.strptime(v["end_date"], "%b %d, %Y"))
        except:
            pass
    if dates:
        print(f"Date range: {min(dates).date()} to {max(dates).date()}")


if __name__ == "__main__":
    asyncio.run(main())