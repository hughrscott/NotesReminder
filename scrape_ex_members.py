"""Quick one-shot: scrape ex-members (last_membership_end non-empty)
Stores in pike13_memberships table."""
import asyncio, sys, sqlite3, os
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
from pike13_auto_auth import authenticate_pike13
from playwright.async_api import async_playwright

DB = "/home/ubuntu/projects/hughrscott/NotesReminder/reminders.db"
SCHOOL = sys.argv[1] if len(sys.argv) > 1 else "westu-sor"

async def main():
    subdomain = f"{SCHOOL}.pike13.com"
    # Use People Details report with last_membership_end filter (all dates, not just date range)
    url = f"https://{subdomain}/desk/reports#/people/details?filters=(last_membership_end:!((empty:!(no))))&sort=(col:last_membership_end,order:d)&hide=1,4,5,6,7,8,10,13,16,17,18,19,24,25,26,28,29,30,31,32,34,36,37,38,39,57"

    conn = sqlite3.connect(DB)
    captured_at = datetime.now(timezone.utc).isoformat()

    async with async_playwright() as p:
        context = await authenticate_pike13(p, SCHOOL, "pike13_profile", True, True)
        page = context.pages[0] if context.pages else await context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(8000)

        # Collect rows — only those with Last Membership End Date filled
        for _ in range(40):  # 40 pages (2000 rows) — better coverage
            rows = await page.evaluate("""
                () => {
                    const table = document.querySelector('table.data_table');
                    if (!table) return [];
                    const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.innerText.trim());
                    const result = [];
                    table.querySelectorAll('tbody tr').forEach(tr => {
                        const tds = tr.querySelectorAll('td');
                        if (tds.length < 3) return;
                        const a = tr.querySelector('a[href*="/people/"]');
                        if (!a) return;
                        const m = a.href.match(/\/people\/(\d+)\/?$/);
                        if (!m) return;
                        const cells = {};
                        for (let i = 0; i < Math.min(tds.length, headers.length); i++) cells[headers[i]] = tds[i].innerText.trim();
                        if (!cells['Last Membership End Date']) return;  // Only past members
                        cells._pid = m[1];
                        cells._name = a.innerText.trim().split('\\n')[0];
                        result.push(cells);
                    });
                    return result;
                }
            """)
            print(f"Page: {len(rows)} past members")
            for r in rows:
                conn.execute("INSERT OR REPLACE INTO pike13_memberships (person_id, person_name, school, person_type, tenure_days, first_visit_date, last_completed_visit_date, last_membership_end_date, completed_visits, future_visits, plan_id, plan_type, plan_name, visits_remaining, ends_after, captured_at) VALUES (?,?,?,?,?,?,?,?,?,?,'','','','','',?)",
                    (r['_pid'], r['_name'], SCHOOL, 'ex_member',
                     r.get('Tenure',''), r.get('First Completed Visit Date',''),
                     r.get('Last Completed Visit Date',''), r.get('Last Membership End Date',''),
                     r.get('Completed Visits',''), r.get('Future Visits',''),
                     captured_at))
            conn.commit()

            # Load next page
            try:
                btn = page.locator("a:has-text('more results'), button:has-text('more results')")
                if await btn.count() > 0:
                    await btn.first.click()
                    await page.wait_for_timeout(5000)
                else:
                    break
            except:
                break
        await context.close()

    c = conn.execute("SELECT COUNT(*) FROM pike13_memberships WHERE school=? AND person_type='ex_member'", (SCHOOL,))
    print(f"Total ex-members stored: {c.fetchone()[0]}")
    conn.close()

asyncio.run(main())
