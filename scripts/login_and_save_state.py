import asyncio
from playwright.async_api import async_playwright
import os
import argparse

async def login_and_save_state(school_subdomain, output_file="state.json"):
    """
    Opens a visible browser for the user to log in manually,
    approve Okta Verify, and then saves the session state.
    """
    print(f"Launching browser to log into {school_subdomain}.pike13.com...")
    print("Please complete the login process, including Okta Verify.")
    print("The browser will automatically close and save your session once you reach the Schedule page.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        login_url = f"https://{school_subdomain}.pike13.com/accounts/sign_in"
        await page.goto(login_url)

        # Wait until the user successfully navigates to the schedule page
        try:
            # We wait for a long time to give the user time to approve the push on their phone
            await page.wait_for_url("**/schedule**", timeout=300000) # 5 minutes timeout
            print("Successfully reached the Schedule page!")

            # Additional wait to ensure all cookies are set
            await page.wait_for_timeout(3000)

            # Save the state
            await context.storage_state(path=output_file)
            print(f"Session state successfully saved to {output_file}")

        except Exception as e:
            print(f"Error during login: {e}")
            print("Did not reach the schedule page in time.")

        finally:
            await browser.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a Playwright state.json file by logging in manually.")
    parser.add_argument("--school", type=str, default="westu-sor", help="Pike13 school subdomain")
    parser.add_argument("--output", type=str, default="state.json", help="Output file path for the state")

    args = parser.parse_args()

    asyncio.run(login_and_save_state(args.school, args.output))
