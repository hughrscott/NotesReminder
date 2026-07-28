#!/usr/bin/env python3
"""
Pike13 Membership Data Extractor
Scrapes 4 reports via Pike13's internal API:
  1. Current Members (has_membership=true)
  2. Leavers (last_membership_end in range)
  3. New Members (first signup in range)
  4. Late Cancellations (enrollment cancellations)
Stores results in reminders.db under pike13_member_snapshots.
"""
import asyncio, os, sys, json, re, time, argparse
from datetime import datetime, timedelta
from pathlib import Path
from playwright.async_api import async_playwright

PROJECT_ROOT = Path.home() / "projects/hughrscott/NotesReminder"
DB_PATH = PROJECT_ROOT / "reminders.db"

# Load env
env_path = Path.home() / ".hermes" / ".env"
for line in open(env_path):
    if '=' in line and not line.startswith('#'):
        k, _, v = line.partition('=')
        if k.strip() in ('PIKE13_USER', 'PIKE13_PASSWORD'):
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

PIKE13_USER = os.environ.get('PIKE13_USER', '')
PIKE13_PASS = os.environ.get('PIKE13_PASSWORD', '')
SCHOOLS = ["westu-sor", "theheights-sor"]

def mfa_code(timeout_s=90):
    """Read fresh MFA code from Himalaya."""
    import subprocess
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = subprocess.run(["himalaya", "envelope", "list", "-a", "sor", "--page-size", "3"],
                capture_output=True, text=True, timeout=20)
            for line in r.stdout.split('\n'):
                if "verification code" in line.lower():
                    p = line.split("|")
                    if len(p) >= 3 and p[1].strip().isdigit():
                        r2 = subprocess.run(["himalaya", "message", "read", "-a", "sor", p[1].strip()],
                            capture_output=True, text=True, timeout=20)
                        m = re.search(r'Your code:\s*(\d{6})', r2.stdout)
                        if m:
                            return m.group(1)
        except:
            pass
        time.sleep(5)
    return None

async def auth_pike13(browser, page, school):
    """Auth to Pike13 with email+MFA. Returns True if successful."""
    await page.goto(f"https://{school}.pike13.com/accounts/sign_in",
                  wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_selector('input[placeholder="Email address"]', timeout=15000)
    await page.fill('input[placeholder="Email address"]', PIKE13_USER)
    await page.fill('input[placeholder="Password"]', PIKE13_PASS)
    await page.click('button:has-text("Sign In")')
    await page.wait_for_timeout(5000)
    
    if "/account/two_factor" in page.url:
        # Click Resend for fresh code
        resend = page.locator('a:has-text("Resend"), button:has-text("Resend")')
        if await resend.count() > 0:
            await resend.first.click()
            await page.wait_for_timeout(3000)
        code = mfa_code(90)
        if not code:
            print(f"  MFA failed for {school}")
            return False
        print(f"  MFA code: {code}")
        await page.evaluate("""(code) => {
            const inputs = document.querySelectorAll('input.otp-digit');
            for (let i = 0; i < 6; i++) {
                const setter = Object.getOwnPropertyDescriptor(
                    window.HTMLInputElement.prototype, 'value').set;
                setter.call(inputs[i], code[i]);
                inputs[i].dispatchEvent(new Event('input', {bubbles: true}));
            }
        }""", code)
        await page.wait_for_timeout(2000)
        await page.evaluate("""() => {
            const form = document.querySelector('form');
            if (form) form.requestSubmit ? form.requestSubmit() : form.submit();
        }""")
        await page.wait_for_timeout(5000)
    
    success = "sign_in" not in page.url and "two_factor" not in page.url
    if success:
        print(f"  ✓ Auth OK for {school}")
    return success

async def extract_current_members(page, school):
    """Extract current active members."""
    url = (f"https://{school}.pike13.com/desk/reports"
           "#/people/details?filters=(has_membership:!((eq:!(t))))")
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(10000)
    
    # Intercept API response
    token = None
    rows = []
    
    async def on_response(response):
        nonlocal token, rows
        if 'api/v3/reports/clients/queries' in response.url:
            if 'auth_token=' in response.url:
                m = re.search(r'auth_token=([a-f0-9-]+)', response.url)
                if m:
                    token = m.group(1)
            try:
                body = await response.json()
                attrs = body.get('data', {}).get('attributes', {})
                rows = attrs.get('rows', [])
            except:
                pass
    
    page.on("response", on_response)
    await page.reload(wait_until="domcontentloaded")
    await page.wait_for_timeout(5000)
    
    return {"token": token, "rows": rows, "report": "current_members"}

async def extract_leavers(page, school, start_date, end_date):
    """Extract students whose membership ended in a date range."""
    url = (f"https://{school}.pike13.com/desk/reports"
           f"#/people/details?filters=(last_membership_end:!((btw:!('{start_date}','{end_date}'))))"
           f"&sort=(col:last_membership_end,order:d)")
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(8000)
    
    rows = []
    async def on_response(response):
        nonlocal rows
        if 'api/v3/reports/clients/queries' in response.url:
            try:
                body = await response.json()
                attrs = body.get('data', {}).get('attributes', {})
                rows = attrs.get('rows', [])
            except:
                pass
    
    page.on("response", on_response)
    await page.reload(wait_until="domcontentloaded")
    await page.wait_for_timeout(5000)
    
    return {"rows": rows, "report": "leavers"}

async def extract_new_members(page, school, start_date, end_date):
    """Extract new membership signups in a date range."""
    url = (f"https://{school}.pike13.com/desk/reports"
           f"#/person_plans/details?filters=(is_first_membership:!((eq:!(t))),"
           f"start_date:!((btw:!('{start_date}','{end_date}'))))"
           f"&sort=(col:start_date,order:d)")
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(8000)
    
    rows = []
    async def on_response(response):
        nonlocal rows
        if 'api/v3/reports/' in response.url and 'queries' in response.url:
            try:
                body = await response.json()
                attrs = body.get('data', {}).get('attributes', {})
                rows = attrs.get('rows', [])
            except:
                pass
    
    page.on("response", on_response)
    await page.reload(wait_until="domcontentloaded")
    await page.wait_for_timeout(5000)
    
    return {"rows": rows, "report": "new_members"}

async def extract_late_cancels(page, school, start_date, end_date):
    """Extract late cancellations in a date range."""
    url = (f"https://{school}.pike13.com/desk/reports"
           f"#/enrollments/details?filters=(service_date:!((btw:!('{start_date}','{end_date}'))),"
           f"state:!((eq:!(late_canceled))))"
           f"&sort=(col:service_date,order:d)")
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(8000)
    
    rows = []
    async def on_response(response):
        nonlocal rows
        if 'api/v3/reports/' in response.url and 'queries' in response.url:
            try:
                body = await response.json()
                attrs = body.get('data', {}).get('attributes', {})
                rows = attrs.get('rows', [])
            except:
                pass
    
    page.on("response", on_response)
    await page.reload(wait_until="domcontentloaded")
    await page.wait_for_timeout(5000)
    
    return {"rows": rows, "report": "late_cancels"}

def store_snapshot(conn, school, data, report_type, scraped_at):
    """Store raw report rows in the database."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pike13_member_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            school TEXT NOT NULL,
            report_type TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            row_data JSON NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pms_report_type ON pike13_member_snapshots(report_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pms_scraped_at ON pike13_member_snapshots(scraped_at)")
    
    for row in data.get("rows", []):
        conn.execute("""
            INSERT INTO pike13_member_snapshots (school, report_type, scraped_at, row_data)
            VALUES (?, ?, ?, ?)
        """, (school, report_type, scraped_at, json.dumps(row)))
    conn.commit()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default=None, help="Start date YYYY-MM-DD (default: 7 days ago)")
    parser.add_argument("--end-date", default=None, help="End date YYYY-MM-DD (default: today)")
    args = parser.parse_args()
    
    today = datetime.now().strftime('%Y-%m-%d')
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    start = args.start_date or week_ago
    end = args.end_date or today
    
    print(f"=== Pike13 Membership Extractor ===\nRange: {start} → {end}\n")
    
    import sqlite3
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])
        
        for school in SCHOOLS:
            print(f"\n{'='*50}\nSCHOOL: {school}\n{'='*50}")
            context = await browser.new_context(viewport={"width": 1920, "height": 1080})
            page = await context.new_page()
            
            if not await auth_pike13(browser, page, school):
                await context.close()
                continue
            
            reports = []
            
            # 1. Current Members
            print("\n  [1] Current Members...")
            data = await extract_current_members(page, school)
            print(f"      {len(data['rows'])} members")
            store_snapshot(conn, school, data, "current_members", today)
            reports.append(data)
            
            # 2. Leavers
            print(f"\n  [2] Leavers ({start} → {end})...")
            data = await extract_leavers(page, school, start, end)
            print(f"      {len(data['rows'])} leavers")
            store_snapshot(conn, school, data, "leavers", today)
            reports.append(data)
            
            # 3. New Members
            print(f"\n  [3] New Members ({start} → {end})...")
            data = await extract_new_members(page, school, start, end)
            print(f"      {len(data['rows'])} new members")
            store_snapshot(conn, school, data, "new_members", today)
            reports.append(data)
            
            # 4. Late Cancellations
            print(f"\n  [4] Late Cancellations ({start} → {end})...")
            data = await extract_late_cancels(page, school, start, end)
            print(f"      {len(data['rows'])} late cancels")
            store_snapshot(conn, school, data, "late_cancels", today)
            reports.append(data)
            
            await context.close()
        
        await browser.close()
    
    # Summary
    for school in SCHOOLS:
        for report_type in ["current_members", "leavers", "new_members", "late_cancels"]:
            count = conn.execute("""
                SELECT COUNT(*) FROM pike13_member_snapshots
                WHERE school=? AND report_type=? AND scraped_at=?
            """, (school, report_type, today)).fetchone()[0]
            print(f"  {school}/{report_type}: {count}")
    
    conn.close()
    print("\n✅ Done")

if __name__ == "__main__":
    asyncio.run(main())
