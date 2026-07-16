#!/usr/bin/env python3
"""Definitive verification of the date-bounded Pike13 report path.

The address bar gave us the real encoding (no guessing):
  /desk/reports#/people/details?filters=(last_membership_end:!((btw:!('YYYY-MM-DD','YYYY-MM-DD'))))&sort=(col:last_membership_end,order:d)&hide=...

This script:
  1. Auth
  2. Load the report with Pike13's DEFAULT window -> capture /queries rows (baseline)
  3. Build the SAME url with OUR custom range (from/to) -> load it
  4. Capture the /queries response rows for OUR range (proof the range drives data)
  5. Also read the visible report table row count as a cross-check
  6. Save evidence (URLs + row counts + screenshots)

If our-range row count != default-row count (or the response reflects our dates),
the date-bounded path is VERIFIED end-to-end.
"""
import asyncio, sys, json, urllib.parse
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

MODELS = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
SCHOOL = "westu-sor"

FRM = "2026-07-15"
TO = "2026-08-11"
args = sys.argv[1:]
if "--from" in args: FRM = args[args.index("--from") + 1]
if "--to" in args: TO = args[args.index("--to") + 1]
if "--school" in args: SCHOOL = args[args.index("--school") + 1]

QUERIES = {}
HIDE = ("1,70,71,72,73,74,75,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,"
         "25,26,28,29,30,31,33,34,58,32,59,60,61,62,35,36,37,38,39,63,64,65,66,67,57,"
         "56.163802,56.163803,56.163805,56.163806,56.163810,56.163812,56.163813,56.166237,"
         "56.163800,56.163804,56.163807,56.163808,56.163814")


def make_report_url(frm, to):
    """Build the date-bounded report URL from the encoding Pike13 itself emits
    in the address bar (captured live): #/people/details?filters=(...)&sort=...&hide=...
    We copy it verbatim except for the two date literals."""
    filt = f"(last_membership_end:!((btw:!('{frm}','{to}'))))"
    sort = "(col:last_membership_end,order:d)"
    frag = f"/people/details?filters={urllib.parse.quote(filt, safe='(),:!')}&sort={urllib.parse.quote(sort, safe='(),:!')}&hide={HIDE}"
    return f"https://{SCHOOL}.pike13.com/desk/reports#{frag}"


async def on_response(resp):
    url = resp.url
    if "/queries" in url and "auth_token" in url:
        try:
            body = await resp.body()
            d = json.loads(body[:400000])
            rows = None
            if isinstance(d, dict):
                if isinstance(d.get("data"), dict):
                    rows = len(d["data"].get("attributes", {}).get("rows", []) or [])
                elif isinstance(d.get("data"), list):
                    rows = len(d["data"])
            QUERIES[url.split("?")[0]] = {
                "url": url,
                "status": resp.status,
                "rows": rows,
                "query": url.split("?", 1)[1] if "?" in url else "",
            }
        except Exception as e:
            QUERIES.setdefault(url, {"parse_err": str(e)})


async def report_table_rows(page):
    return await page.evaluate(
        """() => {
            const tables = Array.from(document.querySelectorAll('table'));
            // pick the largest table (the report), ignore modal/small ones
            let best = null, bestN = -1;
            for (const t of tables) {
                const n = t.querySelectorAll('tbody tr').length;
                if (n > bestN) { bestN = n; best = t; }
            }
            return best ? best.querySelectorAll('tbody tr').length : 0;
        }"""
    )


async def main():
    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=SCHOOL, headless=True, verbose=False
        )
        page = context.pages[0]
        page.on("response", on_response)

        # --- Baseline: default window Pike13 uses (next 28 days) ---
        default_url = make_report_url("2026-07-16", "2026-08-12")
        print(f"[1] Baseline (default window):\n  {default_url[:120]}...")
        QUERIES.clear()
        await page.goto(default_url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(5000)
        base_rows = await report_table_rows(page)
        base_queries = dict(QUERIES)
        print(f"  baseline table rows: {base_rows}")
        for k, v in base_queries.items():
            print(f"    /queries rows={v.get('rows')} status={v.get('status')}")
        await page.screenshot(path=str(MODELS / "report_baseline.png"), full_page=False)

        # --- Custom range ---
        custom_url = make_report_url(FRM, TO)
        print(f"\n[2] Custom range {FRM} -> {TO}:\n  {custom_url[:120]}...")
        QUERIES.clear()
        await page.goto(custom_url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(5000)
        custom_rows = await report_table_rows(page)
        custom_queries = dict(QUERIES)
        print(f"  custom table rows: {custom_rows}")
        for k, v in custom_queries.items():
            print(f"    /queries rows={v.get('rows')} status={v.get('status')}")
        await page.screenshot(path=str(MODELS / "report_custom.png"), full_page=False)

        # --- A deliberately different (narrow) range to prove data moves ---
        narrow_url = make_report_url("2026-07-15", "2026-07-16")
        print(f"\n[3] Narrow range 2026-07-15 -> 2026-07-16 (should differ):")
        QUERIES.clear()
        await page.goto(narrow_url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(5000)
        narrow_rows = await report_table_rows(page)
        narrow_queries = dict(QUERIES)
        print(f"  narrow table rows: {narrow_rows}")
        for k, v in narrow_queries.items():
            print(f"    /queries rows={v.get('rows')} status={v.get('status')}")
        await page.screenshot(path=str(MODELS / "report_narrow.png"), full_page=False)

        verdict = "VERIFIED" if (custom_rows != base_rows or narrow_rows != base_rows) else "INCONCLUSIVE"
        print(f"\n[4] VERDICT: {verdict}")
        print(f"    baseline={base_rows}  custom={custom_rows}  narrow={narrow_rows}")

        out = {
            "school": SCHOOL,
            "range_tested": {"default": ["2026-07-16", "2026-08-12"], "custom": [FRM, TO], "narrow": ["2026-07-15", "2026-07-16"]},
            "pulled_at": datetime.now(timezone.utc).isoformat(),
            "baseline": {"url": default_url, "table_rows": base_rows, "queries": base_queries},
            "custom": {"url": custom_url, "table_rows": custom_rows, "queries": custom_queries},
            "narrow": {"url": narrow_url, "table_rows": narrow_rows, "queries": narrow_queries},
            "verdict": verdict,
        }
        OUT = MODELS / f"pike13_report_verify_{FRM}_{TO}.json"
        OUT.write_text(json.dumps(out, indent=2, default=str))
        print(f"\nSaved -> {OUT}")
        await context.close()


if __name__ == "__main__":
    asyncio.run(main())
