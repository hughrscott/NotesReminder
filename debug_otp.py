#!/usr/bin/env python3
"""Debug Pike13's OTP input structure to fix the MFA entry."""
import asyncio
import os
from playwright.async_api import async_playwright

PIKE13_USER = os.environ.get("PIKE13_USER", "")
PIKE13_PASS = os.environ.get("PIKE13_PASSWORD", "")

async def debug():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--disable-dev-shm-usage'])
        page = await browser.new_page(viewport={'width': 1920, 'height': 1080})
        
        # Login
        await page.goto('https://westu-sor.pike13.com/accounts/sign_in', wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_selector('input[placeholder="Email address"]', timeout=15000)
        await page.fill('input[placeholder="Email address"]', PIKE13_USER)
        await page.fill('input[placeholder="Password"]', PIKE13_PASS)
        await page.click('button:has-text("Sign In")')
        await page.wait_for_timeout(5000)
        
        print(f'URL: {page.url}')
        
        # Examine the OTP inputs in detail
        otp_info = await page.evaluate("""
            () => {
                const inputs = document.querySelectorAll('input.otp-digit');
                return Array.from(inputs).map((inp, i) => ({
                    index: i,
                    type: inp.type,
                    maxlength: inp.maxLength,
                    value: inp.value,
                    visible: inp.offsetParent !== null,
                    display: getComputedStyle(inp).display,
                    visibility: getComputedStyle(inp).visibility,
                    opacity: getComputedStyle(inp).opacity,
                    width: inp.offsetWidth,
                    height: inp.offsetHeight,
                    parentClass: inp.parentElement ? inp.parentElement.className : '',
                    parentId: inp.parentElement ? inp.parentElement.id : '',
                }));
            }
        """)
        
        print(f'OTP inputs: {len(otp_info)}')
        for inp in otp_info[:6]:
            print(f'  [{inp["index"]}] type={inp["type"]} max={inp["maxlength"]} vis={inp["visible"]} '
                  f'disp={inp["display"]} w={inp["width"]} h={inp["height"]} parent={inp["parentClass"][:50]}')
        
        # Check for the OTP container HTML
        otp_html = await page.evaluate("""
            () => {
                const otp = document.querySelector('.otp-digit');
                if (!otp) return 'no otp-digit found';
                let container = otp.closest('[class*="otp"], [class*="code"], [class*="verification"]');
                if (!container) container = otp.parentElement?.parentElement;
                return container ? container.outerHTML.substring(0, 3000) : 'no container';
            }
        """)
        print(f'\nOTP container HTML (first 3000):\n{otp_html}')
        
        # Check for ALL visible inputs on the page
        all_inputs = await page.evaluate("""
            () => Array.from(document.querySelectorAll('input')).filter(el => el.offsetParent !== null).map(el => ({
                type: el.type,
                name: el.name || '',
                id: el.id || '',
                class: el.className || '',
                placeholder: el.placeholder || '',
                maxlength: el.maxLength,
            }))
        """)
        print(f'\nVisible inputs: {len(all_inputs)}')
        for inp in all_inputs:
            print(f'  type={inp["type"]} name={inp["name"]} class={inp["class"][:40]} placeholder={inp["placeholder"]}')
        
        # Check buttons
        buttons = await page.evaluate("""
            () => Array.from(document.querySelectorAll('button')).map(b => ({
                text: b.textContent.trim().substring(0, 40),
                type: b.type,
                disabled: b.disabled,
                visible: b.offsetParent !== null,
            }))
        """)
        print(f'\nButtons: {len(buttons)}')
        for b in buttons:
            print(f'  [{b["type"]}] "{b["text"]}" disabled={b["disabled"]} visible={b["visible"]}')
        
        await browser.close()

asyncio.run(debug())