#!/usr/bin/env python3
"""Debug - wait much longer for the empty:yes URL."""
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
        await page.wait_for_timeout(45000)  # 45 seconds!
        
        body = await page.evaluate("() => document.body.innerText")
        print(f"Body length: {len(body)}")
        print("First 5000 chars:")
        print(body[:5000])
        
        # Search for relevant text
        for term in ["Full Name", "Last Membership End Date", "results filtered", "showing", "Client"]:
            if term in body:
                idx = body.index(term)
                print(f"\nFound '{term}' at {idx}:")
                print(body[max(0,idx-100):idx+300])
            else:
                print(f"\nNOT found: '{term}'")
        
        await context.close()

if __name__ == "__main__":
    asyncio.run(main())