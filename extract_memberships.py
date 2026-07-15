"""
extract_memberships.py — Capture current active plans/passes per student from Pike13.

For each student in the West U `has_membership: t` report, opens their profile,
reads the "Plans & Passes" section, and stores each active plan/pass row.

Output table: pike13_memberships
  person_id        TEXT   (Pike13 person id)
  person_name      TEXT
  school           TEXT
  plan_id          TEXT   (membership id or pack id from href)
  plan_type        TEXT   ('membership' | 'pack')
  plan_name        TEXT
  visits_remaining TEXT   (e.g. "1 of 1")
  ends_after       TEXT   (date or NULL)
  captured_at      TEXT

Usage:
  python extract_memberships.py --school westu-sor
  python extract_memberships.py --school theheights-sor
"""
import asyncio, sys, json, argparse, sqlite3, os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pike13_auto_auth import authenticate_pike13
from playwright.async_api import async_playwright

DB = "/home/ubuntu/projects/hughrscott/NotesReminder/reminders.db"

REPORT_URLS = {
    "westu-sor": "https://westu-sor.pike13.com/desk/reports#/people/details?filters=(has_membership:!((eq:!(t))))&hide=1,4,5,6,7,8,10,13,16,17,18,19,24,25,26,28,29,30,31,32,34,36,37,38,39,57",
    "theheights-sor": "https://theheights-sor.pike13.com/desk/reports#/people/details?filters=(has_membership:!((eq:!(t))))&hide=1,4,5,6,7,8,10,13,16,17,18,19,24,25,26,28,29,30,31,32,34,36,37,38,39,57",
}

def ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pike13_memberships (
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
            PRIMARY KEY (person_id, school, person_type)
        )
    """)
    conn.commit()

async def get_member_links(page, url):
    """Load report, return list of dicts with person info + report column data."""
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    try:
        await page.wait_for_function(
            "document.body.innerText.includes('results filtered by')", timeout=20000
        )
    except Exception:
        pass
    await page.wait_for_timeout(4000)
    all_links = []
    seen = set()
    for _ in range(10):
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
                    const m = a.href.match(/\\/people\\/(\\d+)\\/?$/);
                    if (!m) return;
                    const out = {href: a.href, pid: m[1], name: a.innerText.trim().split('\\n')[0]};
                    // Capture key date columns: Tenure, First/Last Visit, Membership End
                    for (let i = 0; i < Math.min(tds.length, headers.length); i++) {
                        const h = headers[i];
                        if (['Tenure','First Completed Visit Date','Last Completed Visit Date',
                             'Last Membership End Date','Completed Visits','Future Visits'].includes(h)) {
                            out[h] = tds[i].innerText.trim();
                        }
                    }
                    result.push(out);
                });
                return result;
            }
        """)
        new = [l for l in rows if l['pid'] not in seen]
        for l in new:
            seen.add(l['pid'])
            all_links.append(l)
        try:
            btn = page.locator("a:has-text('more results'), button:has-text('more results')")
            if await btn.count() > 0:
                await btn.first.click()
                # Table is destroyed and rebuilt by Angular — wait for it to reappear
                try:
                    await page.wait_for_function("document.querySelector('table.data_table') !== null", timeout=30000)
                except Exception:
                    pass
                await page.wait_for_timeout(3000)
            else:
                break
        except Exception:
            break
    return all_links

async def scrape_student_plans(page, href, name):
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
                // Strip query string, decode, then split path (no regex -> can't fail)
                const clean = decodeURIComponent(rawHref.split('?')[0]);
                const parts = clean.split('/').filter(Boolean);  // [..., 'people', pid, 'memberships'|'packs', id]
                let ptype = 'pack';
                let pid = '';
                const idx = parts.findIndex(x => x === 'memberships' || x === 'packs');
                if (idx >= 0 && idx + 1 < parts.length) {
                    ptype = parts[idx];
                    pid = parts[idx + 1];
                }
                const smalls = Array.from(tr.querySelectorAll('small')).map(s => s.innerText.trim());
                let visits = '';
                let ends = '';
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
    ap.add_argument("--school", default="westu-sor", choices=list(REPORT_URLS.keys()))
    args = ap.parse_args()

    school = args.school
    report_url = REPORT_URLS[school]

    conn = sqlite3.connect(DB)
    ensure_table(conn)
    # Only delete active-member rows for this school (preserve ex-members)
    conn.execute("DELETE FROM pike13_memberships WHERE school = ? AND person_type != 'ex_member'", (school,))
    conn.commit()

    captured_at = datetime.now(timezone.utc).isoformat()

    async with async_playwright() as p:
        context = await authenticate_pike13(p, school, "pike13_profile", True, True)
        page = context.pages[0] if context.pages else await context.new_page()

        links = await get_member_links(page, report_url)
        print(f"Found {len(links)} members at {school}")

        for i, link in enumerate(links):
            # Store report column data directly — no per-student profile visit needed
            conn.execute(
                "INSERT OR REPLACE INTO pike13_memberships (person_id, person_name, school, person_type, tenure_days, first_visit_date, last_completed_visit_date, last_membership_end_date, completed_visits, future_visits, plan_id, plan_type, plan_name, visits_remaining, ends_after, captured_at) VALUES (?,?,?,?,?,?,?,?,?,?,'','','','','',?)",
                (link['pid'], link['name'], school, 'active_member',
                 link.get('Tenure', ''), link.get('First Completed Visit Date', ''),
                 link.get('Last Completed Visit Date', ''), link.get('Last Membership End Date', ''),
                 link.get('Completed Visits', ''), link.get('Future Visits', ''),
                 captured_at)
            )
            conn.commit()
            if (i+1) % 50 == 0:
                print(f"  stored {i+1}/{len(links)}")

        await context.close()

    c = conn.execute("SELECT person_type, COUNT(DISTINCT person_id), COUNT(*) FROM pike13_memberships WHERE school=? GROUP BY person_type", (school,))
    print(f"\n{school} complete:")
    for pt, members, plan_rows in c:
        print(f"  {pt}: {members} people, {plan_rows} rows")

    conn.close()

asyncio.run(main())

