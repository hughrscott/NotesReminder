#!/usr/bin/env python3
"""
Full-history Pike13 leaver scraper — background job.
Scrapes ALL students with last_membership_end dates using a wide date range filter.
Uses two-school session sharing to minimize MFA prompts.
Saves incrementally so it survives interruptions.
"""
import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS_DIR = Path(__file__).parent / "models"
OUTPUT_PATH = MODELS_DIR / "pike13_leavers_full_history.json"
PROGRESS_PATH = MODELS_DIR / "pike13_leavers_full_history_progress.json"

SCHOOLS = {"westu-sor": "West U", "theheights-sor": "The Heights"}

# Use a VERY wide date range (2010-2030) to capture all historical leavers
# This works because the btw filter was proven to work in the 8-month scrape
LEAVERS_URL_TEMPLATE = (
    "https://{school}.pike13.com/desk/reports#/people/details"
    "?filters=(last_membership_end:!((btw:!('2010-01-01','2030-12-31'))))"
    "&sort=(col:last_membership_end,order:d)"
    "&hide=1,70,71,72,73,74,75,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75"
)


async def scrape_school_leavers(playwright, school_slug, school_name, context):
    """Scrape all leavers for one school using authenticated context."""
    page = await context.new_page()
    url = LEAVERS_URL_TEMPLATE.format(school=school_slug)
    
    print(f"  [{school_name}] Navigating to leavers report...")
    await page.goto(url, wait_until="domcontentloaded", timeout=120000)
    await page.wait_for_timeout(15000)  # Let Pike13 virtualized table render
    
    all_leavers = {}
    page_num = 0
    consecutive_empty = 0
    
    while consecutive_empty < 3:
        page_num += 1
        print(f"  [{school_name}] Scraping page {page_num}...")
        
        # Get full body text and parse TSV format
        body_text = await page.evaluate("() => document.body.innerText")
        
        # Parse TSV: lines between "Full Name" header and "results filtered" footer
        lines = body_text.strip().split('\n')
        in_data = False
        page_leavers = 0
        
        for line in lines:
            line = line.strip()
            if line == "Full Name":
                in_data = True
                continue
            if "results filtered" in line.lower():
                if in_data:
                    break
                continue
            if in_data and line:
                parts = line.split('\t')
                if len(parts) >= 2:
                    name = parts[0].strip()
                    end_date = parts[1].strip()
                    if name and end_date:
                        all_leavers[name.lower()] = {
                            "school": school_name,
                            "end_date": end_date,
                            "name": name,
                        }
                        page_leavers += 1
        
        print(f"  [{school_name}] Page {page_num}: {page_leavers} leavers (total: {len(all_leavers)})")
        
        if page_leavers == 0:
            consecutive_empty += 1
        else:
            consecutive_empty = 0
        
        # Save progress incrementally
        with open(PROGRESS_PATH, 'w') as f:
            json.dump({
                "school": school_name,
                "page": page_num,
                "leavers_count": len(all_leavers),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "data": all_leavers,
            }, f, indent=2)
        
        # Try to click "more results"
        try:
            btn = page.locator("a:has-text('more results'), button:has-text('more results')").first
            if await btn.count() > 0:
                await btn.click()
                await page.wait_for_timeout(5000)
            else:
                print(f"  [{school_name}] No more results button - done")
                break
        except Exception as e:
            print(f"  [{school_name}] Pagination ended: {e}")
            break
    
    await page.close()
    return all_leavers


async def main():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  Pike13 Full-History Leaver Scraper                        ║")
    print("║  Two-school session sharing • Incremental saves • Resumable ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    
    # Load existing progress if any
    all_leavers = {}
    if PROGRESS_PATH.exists():
        with open(PROGRESS_PATH) as f:
            prog = json.load(f)
            all_leavers = prog.get('data', {})
        print(f"Resuming from progress: {len(all_leavers)} leavers already scraped")
    
    async with async_playwright() as playwright:
        # Authenticate ONCE, share session across both schools
        print("\n[1/3] Authenticating to West U (shared session)...")
        context = await pike13_auto_auth.authenticate_pike13(
            playwright, school_subdomain="westu-sor", headless=True, verbose=False
        )
        print("  ✓ Authenticated")
        
        # Scrape West U
        print("\n[2/3] Scraping West U leavers...")
        wu_leavers = await scrape_school_leavers(playwright, "westu-sor", "West U", context)
        all_leavers.update(wu_leavers)
        print(f"  West U: {len(wu_leavers)} leavers")
        
        # Scrape Heights using SAME context (shared Pike13.com cookies)
        print("\n[3/3] Scraping Heights leavers (same session)...")
        th_leavers = await scrape_school_leavers(playwright, "theheights-sor", "The Heights", context)
        all_leavers.update(th_leavers)
        print(f"  Heights: {len(th_leavers)} leavers")
        
        await context.close()
    
    # Final save
    MODELS_DIR.mkdir(exist_ok=True)
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(all_leavers, f, indent=2)
    
    # Clean up progress file
    if PROGRESS_PATH.exists():
        PROGRESS_PATH.unlink()
    
    # Date range summary
    dates = []
    for v in all_leavers.values():
        try:
            dates.append(datetime.strptime(v["end_date"], "%b %d, %Y"))
        except:
            pass
    
    print(f"\n{'='*60}")
    print(f"COMPLETE: {len(all_leavers)} total leavers saved to {OUTPUT_PATH}")
    if dates:
        print(f"Date range: {min(dates).date()} to {max(dates).date()}")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())