#!/usr/bin/env python3
"""Scrape Pike13 on-hold report and save structured data."""
import asyncio, json, os, sys
from pathlib import Path
from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).parent))
import pike13_auto_auth

MODELS_DIR = Path(__file__).parent / "models"
SCHOOLS = {"westu-sor": "West U", "theheights-sor": "The Heights"}

async def scrape_holds(school_slug):
    async with async_playwright() as playwright:
        context = await pike13_auto_auth.authenticate_pike13(
            playwright, school_subdomain=school_slug, headless=True, verbose=False
        )
        page = await context.new_page()
        
        url = f"https://{school_slug}.pike13.com/desk/reports#/person_plans/details?filters=(is_on_hold:!((eq:!(t))))"
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        # Wait on evidence, not a blind sleep: an authenticated report has a
        # table; an expired session redirects to sign-in / two-factor.
        await page.wait_for_function(
            """() => document.querySelector('table') !== null
                    || location.pathname.includes('sign_in')
                    || location.pathname.includes('two_factor')""",
            timeout=30000,
        )
        if "sign_in" in page.url or "two_factor" in page.url:
            raise RuntimeError(f"Pike13 session expired while loading hold report: {page.url}")
        await page.wait_for_timeout(2000)
        
        rows = await page.evaluate("""() => {
            const tables = document.querySelectorAll('table');
            if (tables.length === 0) return [];
            const headers = Array.from(tables[0].querySelectorAll('th')).map(h => h.innerText.trim());
            const dataRows = Array.from(tables[0].querySelectorAll('tbody tr'));
            const result = [];
            for (const row of dataRows) {
                const cells = row.querySelectorAll('td');
                if (cells.length < 3) continue;
                const rowData = {};
                for (let i = 0; i < Math.min(headers.length, cells.length); i++) {
                    rowData[headers[i]] = cells[i].innerText.trim();
                }
                result.push(rowData);
            }
            return result;
        }""")
        
        await context.close()

        if not rows:
            raise RuntimeError(f"Hold report rendered no rows for {school_slug}")
        
        # Filter to only those actively on hold and extract key fields
        holds = []
        for r in rows:
            if r.get("On Hold?", "").lower() != "yes":
                continue
            holds.append({
                "client": r.get("Client", ""),
                "first_name": r.get("First Name", ""),
                "last_name": r.get("Last Name", ""),
                "plan": r.get("Plan Name", ""),
                "on_hold": True,
                "hold_start": r.get("Last Hold Start Date", ""),
                "hold_end": r.get("Last Hold End Date", ""),
                "hold_indefinite": r.get("Last Hold Indefinite?", "") == "Yes",
                "hold_by": r.get("Last Hold By", ""),
                "account_managers": r.get("Account Managers", ""),
                "account_emails": r.get("Account Manager Emails", ""),
                "account_phones": r.get("Account Manager Phones", ""),
                "start_date": r.get("Start Date", ""),
                "ended": r.get("Ended?", "") == "Yes",
                "canceled": r.get("Canceled?", "") == "Yes",
                "base_price": r.get("Base Price", ""),
            })
        return holds


async def main():
    for slug, name in SCHOOLS.items():
        print(f"Scraping {name} ({slug})...")
        holds = await scrape_holds(slug)
        out_path = MODELS_DIR / f"pike13_holds_{slug}.json"
        json.dump(holds, open(out_path, "w"), indent=2)
        print(f"  {len(holds)} on-hold plans → {out_path}")

asyncio.run(main())
