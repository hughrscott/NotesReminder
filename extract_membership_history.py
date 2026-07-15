"""extract_membership_history.py

Scrape the Pike13 people/details report for ALL people (not just current members)
to capture enrollment date (First Visit Date) and exit date (Last Membership End Date),
plus the current active plans/passes list per person.

Stores to pike13_memberships with additional person_type, tenure_days, date columns.

Usage:
  python extract_membership_history.py --school westu-sor --limit 20  # test
  python extract_membership_history.py --school westu-sor             # full
"""
import asyncio, sys, json, argparse, sqlite3, os, re
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pike13_auto_auth import authenticate_pike13
from playwright.async_api import async_playwright

DB = "/home/ubuntu/projects/hughrscott/NotesReminder/reminders.db"

# Report URLs — membership filter is parameterized
BASE_HIDE = "hide=1,4,5,6,7,8,10,13,16,17,18,19,24,25,26,28,29,30,31,32,34,36,37,38,39,57"

def report_url(school, has_membership="t"):
    subdomain = f"{school}.pike13.com"
    return f"https://{subdomain}/desk/reports#/people/details?filters=(has_membership:!((eq:!({has_membership}))))&{BASE_HIDE}"

def ensure_table(conn):
    conn.execute("DROP TABLE IF EXISTS pike13_memberships")
    conn.execute("""
        CREATE TABLE pike13_memberships (
            person_id TEXT,
            person_name TEXT,
            school TEXT,
            person_type TEXT,
            tenure_days TEXT,
            first_visit_date TEXT,
            last_completed_visit_date TEXT,
            last_membership_end_date TEXT,
            completed_visits TEXT,
            future_visits TEXT,
            plan_id TEXT,
            plan_type TEXT,
            plan_name TEXT,
            visits_remaining TEXT,
            ends_after TEXT,
            captured_at TEXT,
            PRIMARY KEY (person_id, plan_id, school)
        )
    """)
    conn.commit()
async def get_report_rows(page, url, max_clicks=5):
    """Load report, click Load-more until all rows visible, return list of row dicts."""
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    # Wait for table data cells to render (Angular slow — look for a td in a tbody row)
    try:
        await page.wait_for_function(
            "document.querySelector('table.data_table tbody tr td') !== null", timeout=25000
        )
    except Exception:
        await page.wait_for_timeout(8000)
    await page.wait_for_timeout(4000)

    for _ in range(max_clicks):
        try:
            btn = page.locator("a:has-text('more results'), button:has-text('more results')")
            if await btn.count() > 0:
                await btn.first.click()
                await page.wait_for_timeout(5000)
            else:
                break
        except Exception:
            break

    rows = await page.evaluate("""
        () => {
            const table = document.querySelector('table.data_table');
            if (!table) return [];
            const headers = [];
            table.querySelectorAll('thead th').forEach(th => headers.push(th.innerText.trim()));
            const result = [];
            table.querySelectorAll('tbody tr').forEach(tr => {
                const tds = tr.querySelectorAll('td');
                if (tds.length < 3) return;
                const a = tr.querySelector('a[href*="/people/"]');
                if (!a) return;
                const m = a.href.match(/\\/people\\/(\\d+)\\/?$/);
                if (!m) return;
                const cells = {};
                for (let i = 0; i < Math.min(tds.length, headers.length); i++) {
                    cells[headers[i]] = tds[i].innerText.trim();
                }
                cells._pid = m[1];
                cells._name = a.innerText.trim().split('\\n')[0];
                result.push(cells);
            });
            return result;
        }
    """)
    return rows

async def scrape_student_plans(page, href):
    await page.goto(href, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(3500)
    plans = await page.evaluate("""
        () => {
            const rows = [];
            const table = document.querySelector('#plans-and-passes-content');
            if (!table) return rows;
            table.querySelectorAll('tr.filterable').forEach(tr => {
                const nameEl = tr.querySelector('.plan-name');
                if (!nameEl) return;
                const planName = nameEl.innerText.trim();
                const rawHref = (tr.querySelector('a') || {}).href || '';
                const clean = decodeURIComponent(rawHref.split('?')[0]);
                const parts = clean.split('/').filter(Boolean);
                const idx = parts.findIndex(x => x === 'memberships' || x === 'packs');
                let ptype = 'pack', pid = '';
                if (idx >= 0 && idx + 1 < parts.length) { ptype = parts[idx]; pid = parts[idx + 1]; }
                const smalls = Array.from(tr.querySelectorAll('small')).map(s => s.innerText.trim());
                let visits = '', ends = '';
                for (const s of smalls) {
                    if (/visits remaining/.test(s)) visits = s.replace(/\\s+/g, ' ').trim();
                    if (/Ends after/.test(s)) ends = s.replace(/Ends after\\s*/,'').trim();
                }
                rows.push({plan_name: planName, plan_type: ptype, plan_id: pid, visits_remaining: visits, ends_after: ends});
            });
            return rows;
        }
    """)
    return plans

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--school", default="westu-sor", choices=["westu-sor", "theheights-sor"])
    ap.add_argument("--membership", default="t", choices=["t", "f"], help="Membership filter (t=active members, f=ex-members)")
    ap.add_argument("--limit", type=int, default=0, help="Limit to N people for testing")
    args = ap.parse_args()
    school = args.school

    conn = sqlite3.connect(DB)
    ensure_table(conn)
    # Don't DELETE — each run adds distinct person_ids (t vs f are disjoint sets)
    captured_at = datetime.now(timezone.utc).isoformat()

    async with async_playwright() as p:
        context = await authenticate_pike13(p, school, "pike13_profile", True, True)
        page = context.pages[0] if context.pages else await context.new_page()

        url = report_url(school, args.membership)
        print(f"Loading report: {url[:100]}...")
        # For ex-members (5k+ rows), only scrape 3 pages — past members cluster in first pages
        max_clicks = 5 if args.membership == "t" else 0  # f: first page only (50 rows)
        rows = await get_report_rows(page, url, max_clicks=max_clicks)
        print(f"Report rows (raw): {len(rows)} for {school} (has_membership={args.membership})")
        # For ex-members, filter to only people with Last Membership End Date (actual past members)
        if args.membership == "f":
            rows = [r for r in rows if r.get('Last Membership End Date', '').strip()]
            print(f"Report rows (past members only): {len(rows)}")

        if args.limit and args.limit < len(rows):
            rows = rows[:args.limit]

        for i, row in enumerate(rows):
            pid = row['_pid']
            name = row['_name']
            try:
                plans = await scrape_student_plans(
                    page, f"https://{school}.pike13.com/people/{pid}"
                )
            except Exception as e:
                print(f"  ERROR {name}: {e}")
                plans = []

            person_type = 'active_member' if plans else 'ex_member'

            for pl in plans:
                conn.execute("INSERT OR REPLACE INTO pike13_memberships (person_id, person_name, school, person_type, tenure_days, first_visit_date, last_completed_visit_date, last_membership_end_date, completed_visits, future_visits, plan_id, plan_type, plan_name, visits_remaining, ends_after, captured_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (pid, name, school, person_type,
                     row.get('Tenure', ''), row.get('First Completed Visit Date', ''),
                     row.get('Last Completed Visit Date', ''), row.get('Last Membership End Date', ''),
                     row.get('Completed Visits', ''), row.get('Future Visits', ''),
                     pl['plan_id'], pl['plan_type'], pl['plan_name'], pl['visits_remaining'], pl['ends_after'],
                     captured_at))

            # Ex-members get one row with no plan data
            if not plans:
                conn.execute("INSERT OR REPLACE INTO pike13_memberships (person_id, person_name, school, person_type, tenure_days, first_visit_date, last_completed_visit_date, last_membership_end_date, completed_visits, future_visits, plan_id, plan_type, plan_name, visits_remaining, ends_after, captured_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (pid, name, school, person_type,
                     row.get('Tenure', ''), row.get('First Completed Visit Date', ''),
                     row.get('Last Completed Visit Date', ''), row.get('Last Membership End Date', ''),
                     row.get('Completed Visits', ''), row.get('Future Visits', ''),
                     '', '', '', '', '',
                     captured_at))

            conn.commit()
            if (i+1) % 20 == 0:
                print(f"  processed {i+1}/{len(rows)}")

        await context.close()

    c = conn.execute("SELECT person_type, COUNT(DISTINCT person_id), COUNT(*) FROM pike13_memberships WHERE school=? GROUP BY person_type", (school,))
    print(f"\n{school} complete:")
    for pt, members, plan_rows in c:
        print(f"  {pt}: {members} people, {plan_rows} rows")

    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
