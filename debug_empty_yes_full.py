#!/usr/bin/env python3
"""Debug - print the actual body text for the empty:yes URL."""
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
        
        # Use the empty:yes filter
        url = "https://westu-sor.pike13.com/desk/reports#/people/details?filters=(last_membership_end:!((empty:!(yes))))&sort=(col:last_membership_end,order:d)&hide=1,70,71,72,73,74,75,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75"
        
        print("Navigating...")
        await page.goto(url, wait_until="domcontentloaded", timeout=120000)
        print("Waiting for table to render...")
        await page.wait_for_timeout(45000)
        
        body = await page.evaluate("() => document.body.innerText")
        print(f"Body length: {len(body)}")
        print("Full body:")
        print("=" * 60)
        print(body)
        print("=" * 60)
        
        # Search for any student names we know
        known_names = ["Michael Branstetter", "Annabelle Ruan", "Will Bisson"]
        for name in known_names:
            if name in body:
                print(f"\nFOUND '{name}' in body!")
                idx = body.index(name)
                print(body[max(0,idx-200):idx+200])
            else:
                print(f"\nNOT found: '{name}'")
        
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())