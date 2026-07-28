#!/usr/bin/env python3
"""
pike13_pull_all.py — Pull ALL Pike13 people with current_plans.

Strategy: Split into monthly date ranges to avoid Pike13's opaque pagination.
Each range returns up to 964 rows; we aggregate and deduplicate.

Usage: python3 pike13_pull_all.py --school westu-sor
"""
import asyncio, sys, json, urllib.parse
from pathlib import Path
from datetime import date, datetime, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path(__file__).resolve().parent / "models"
HIDE = "1,70,71,72,73,74,75,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,28,29,30,31,33,34,58,32,59,60,61,62,35,36,37,38,39,63,64,65,66,67,57,56.163802,56.163803,56.163805,56.163806,56.163810,56.163812,56.163813,56.166237,56.163800,56.163804,56.163807,56.163808,56.163814"

FIELDS = ["full_name", "email", "phone", "address", "current_plans",
          "person_id", "status", "last_membership_end_date", "next_plan_end",
          "created_at", "first_visit_date", "last_visited"]


def month_ranges(start="2020-01-01", end=None, step_months=6):
    if end is None:
        end = date.today().isoformat()
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    current = start_date
    while current < end_date:
        next_date = min(current + timedelta(days=step_months * 30), end_date)
        yield (current.strftime("%Y-%m-%d"), next_date.strftime("%Y-%m-%d"))
        current = next_date


async def pull_range(school, frm, to, page):
    """Pull one date range via URL navigation, return (rows, total)."""
    filt = f"(last_membership_end:!((btw:!('{frm}','{to}'))))"
    sort = "(col:last_membership_end,order:d)"
    frag = f"/people/details?filters={urllib.parse.quote(filt, safe='(),:!')}&sort={urllib.parse.quote(sort, safe='(),:!')}&hide={HIDE}"
    report_url = f"https://{school}.pike13.com/desk/reports#{frag}"

    captured = {}
    async def on_response(resp):
        url = resp.url
        if "/queries" in url and "auth_token" in url:
            try:
                body = await resp.body()
                d = json.loads(body[:400000])
                attrs = d.get("data", {}).get("attributes", {})
                captured["rows"] = attrs.get("rows") or []
                captured["total"] = attrs.get("total_count", 0)
            except:
                pass

    page.on("response", on_response)
    await page.goto(report_url, wait_until="networkidle", timeout=30000)
    await page.wait_for_timeout(5000)

    rows = captured.get("rows", [])
    total = captured.get("total", 0)
    return rows, total


async def pull_all(school="westu-sor"):
    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=school, headless=True, verbose=False
        )
        page = context.pages[0]

        all_rows = []
        seen_ids = set()
        # Rows are lists: [full_name, email, phone, address, current_plans, person_id, ...]
        # person_id is at index 5

        ranges = list(month_ranges("2020-01-01", step_months=6))
        print(f"Pulling {len(ranges)} date ranges for {school}...")

        for i, (frm, to) in enumerate(ranges):
            rows, total = await pull_range(school, frm, to, page)
            new = 0
            for row in rows:
                pid = row[5] if len(row) > 5 else None  # person_id at index 5
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    all_rows.append(row)
                    new += 1
            print(f"  Range {i+1}/{len(ranges)}: {frm}→{to}: {len(rows)} rows ({new} new, total deduped: {len(all_rows)})")

        out = MODELS / f"pike13_people_plans_{school}.json"
        out.write_text(json.dumps({
            "school": school, "pulled_at": date.today().isoformat(),
            "total": len(all_rows), "fields": FIELDS, "rows": all_rows,
        }, indent=2, default=str))
        print(f"  Done: {len(all_rows)} unique people saved to {out}")
        await context.close()
        return all_rows


async def main():
    school = "westu-sor"
    if "--school" in sys.argv:
        school = sys.argv[sys.argv.index("--school") + 1]
    print(f"Pulling all Pike13 people for {school}...")
    await pull_all(school)


if __name__ == "__main__":
    asyncio.run(main())
