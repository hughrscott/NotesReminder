#!/usr/bin/env python3
"""Verify the date-bounded Pike13 report path by driving the REAL Filters modal.

Flow:
  auth -> /desk/reports -> Insights -> open 'Last Memberships' report
  -> baseline queries request fires (default 'next 28 days' window -> rows)
  -> click Filters -> dump EVERY visible input (learn real field structure)
  -> set the two date fields to our range (ISO -> MM/DD/YYYY)
  -> click 'Finish' -> capture the queries request with OUR range encoded
  -> compare baseline vs custom; save real request URLs + params + screenshots.

No param guessing: we read field attributes from the DOM, and we copy the
exact request URL Pike13 fires (URL-decode-able later).
"""
import asyncio, sys, json, re
from pathlib import Path
from datetime import datetime, timezone, date

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
REPORT_LABEL = "Last Memberships"
if "--report" in args: REPORT_LABEL = args[args.index("--report") + 1]


def iso_to_usd(iso: str) -> str:
    y, m, d = iso.split("-")
    return f"{int(m):02d}/{int(d):02d}/{y}"


QUERIES = {}          # report -> {url, status, rows, query}
ALL_REQ = []
PAGE_URLS = []
DIAG = {}


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
            name = url.split("/reports/")[-1].split("/queries")[0]
            QUERIES[name] = {
                "url": url, "status": resp.status, "rows": rows,
                "query": url.split("?", 1)[1] if "?" in url else "",
            }
        except Exception as e:
            QUERIES.setdefault(url, {"parse_err": str(e)})
    if any(k in url for k in ["/api/", "/reports/", "/queries", "/insights/"]):
        ALL_REQ.append(url)


async def dump_visible_inputs(page, include_hidden: bool = False):
    """Return inventory of inputs/selects. If include_hidden, also include
    display:none / visibility:hidden ones (so we can see fields that render later)."""
    JS = """(opts) => {
        const all = Array.from(document.querySelectorAll('input, select, textarea'))
            .filter(e => {
                const r = e.getBoundingClientRect();
                const style = getComputedStyle(e);
                const visible = style.display !== 'none' && style.visibility !== 'hidden'
                    && r.width > 0 && r.height > 0;
                return opts.includeHidden || visible;
            });
        return all.map(e => {
            const r = e.getBoundingClientRect();
            const style = getComputedStyle(e);
            const visible = style.display !== 'none' && style.visibility !== 'hidden'
                && r.width > 0 && r.height > 0;
            return {
                tag: e.tagName,
                type: e.getAttribute('type') || '',
                name: e.getAttribute('name') || '',
                id: e.id || '',
                ph: e.getAttribute('placeholder') || '',
                cls: (e.className || '').toString().slice(0, 70),
                value: (e.value || '').slice(0, 40),
                visible: visible,
            };
        });
    }"""
    return await page.evaluate(JS, {"includeHidden": include_hidden})


async def main():
    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=SCHOOL, headless=True, verbose=False
        )
        page = context.pages[0]
        page.on("response", on_response)

        print(f"[1] /desk/reports for {SCHOOL} ...")
        await page.goto(f"https://{SCHOOL}.pike13.com/desk/reports", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(3000)

        print(f"[2] Insights -> open report '{REPORT_LABEL}' ...")
        await page.locator('a:has-text("Insights")').first.click(timeout=6000)
        await page.wait_for_timeout(4000)
        opened = False
        for label in [REPORT_LABEL, "New Clients", "Expiring Memberships", "Last Visited"]:
            loc = page.locator(f'text={label}').first
            if await loc.count() > 0:
                try:
                    await loc.click(timeout=5000)
                    opened = True
                    print(f"  opened: {label}")
                    break
                except Exception as e:
                    print(f"  click {label} failed: {str(e)[:50]}")
        if not opened:
            print("  !! no report opened")
        await page.wait_for_timeout(4000)
        PAGE_URLS.append(("after-report-open", page.url))
        await page.screenshot(path=str(MODELS / "report_detail.png"), full_page=False)

        # BASELINE: capture the default (date-bounded) queries request
        await page.wait_for_timeout(2000)
        baseline = dict(QUERIES)
        print(f"[3] Baseline date-bounded queries (default window): {len(baseline)}")
        for name, info in baseline.items():
            print(f"    {name}: status={info.get('status')} rows={info.get('rows')} query={info.get('query','')[:90]}")

        # Open Filters (robust: try several, and dump toolbar buttons either way)
        print("[4] Click Filters ...")
        fb = page.locator('button:has-text("Filters")').first
        clicked_filters = False
        if await fb.count() > 0:
            await fb.click(timeout=5000)
            clicked_filters = True
            print("  clicked Filters (has-text)")
        else:
            # fallback: any button/element with Filters text
            for sel in ['[class*=filter]:has-text("Filters")', 'a:has-text("Filters")', 'div:has-text("Filters")']:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    try:
                        await loc.click(timeout=4000)
                        clicked_filters = True
                        print(f"  clicked Filters via {sel}")
                        break
                    except Exception as e:
                        print(f"  {sel} click failed: {str(e)[:40]}")
        if not clicked_filters:
            print("  !! Filters button not found -- dumping toolbar buttons")
            toolbar = await page.evaluate(
                """() => Array.from(document.querySelectorAll('button, a.btn, [role=button]'))
                    .slice(0,40).map(e => ({t:(e.innerText||'').trim().slice(0,30), cls:(e.className||'').toString().slice(0,50)}))"""
            )
            for b in toolbar:
                print(f"    toolbar: {b['t']!r} cls={b['cls']!r}")
        await page.wait_for_timeout(3000)
        await page.screenshot(path=str(MODELS / "report_filter_modal.png"), full_page=False)

        # The date inputs are <input name="inputDate" type="text"> (quickmoment picker),
        # currently hidden (vis=False) with the default window values.
        # Grab them by name; there should be exactly two (from/to).
        print("[5] Locating inputDate fields ...")
        date_loc = page.locator('input[name="inputDate"]')
        n_date = await date_loc.count()
        date_inputs = []
        for k in range(n_date):
            info = await date_loc.nth(k).evaluate(
                """(el) => ({name: el.getAttribute('name'), id: el.id,
                    cls:(el.className||'').toString().slice(0,60), value:(el.value||'').slice(0,40)})"""
            )
            date_inputs.append(info)
        DIAG["date_input_count"] = n_date
        print(f"  inputDate fields found: {n_date}")
        for di in date_inputs:
            print(f"    ** DATE FIELD: name={di['name']!r} id={di['id']!r} cls={di['cls']!r} value={di['value']!r}")

        # Snapshot the table row count BEFORE changing the range
        def count_rows():
            return page.evaluate(
                "() => document.querySelectorAll('table tbody tr, .report-table tbody tr, [class*=table] tbody tr').length"
            )
        rows_before = await count_rows()
        DIAG["rows_before_custom"] = rows_before

        # Set the two date fields to our range (MM/DD/YYYY)
        set_log = []
        if n_date >= 2:
            for idx, val_iso in [(0, FRM), (1, TO)]:
                loc = date_loc.nth(idx)
                val = iso_to_usd(val_iso)
                try:
                    await loc.fill(val, timeout=5000)
                    set_log.append(f"filled inputDate[{idx}] = {val}")
                except Exception as e:
                    try:
                        await loc.evaluate(
                            """(el, v) => { el.value=v; el.dispatchEvent(new Event('input',{bubbles:true})); el.dispatchEvent(new Event('change',{bubbles:true})); }""",
                            val,
                        )
                        set_log.append(f"js-set inputDate[{idx}] = {val}")
                    except Exception as e2:
                        set_log.append(f"FAIL inputDate[{idx}]: {str(e)[:40]} / js {str(e2)[:40]}")
        else:
            set_log.append(f"only {n_date} inputDate fields found")
        for l in set_log:
            print(f"  {l}")
        await page.wait_for_timeout(1500)
        await page.screenshot(path=str(MODELS / "report_filter_dateset.png"), full_page=False)

        # Apply via Finish
        print("[6] Click Finish ...")
        QUERIES.clear()
        ALL_REQ.clear()
        fin = page.locator('button:has-text("Finish")').first
        if await fin.count() > 0:
            await fin.click(timeout=5000)
            print("  clicked Finish")
        else:
            print("  !! Finish not found")
        await page.wait_for_timeout(6000)
        PAGE_URLS.append(("after-finish", page.url))
        await page.screenshot(path=str(MODELS / "report_after_apply.png"), full_page=False)

        rows_after = await count_rows()
        DIAG["rows_after_custom"] = rows_after
        print(f"  Table rows: before(custom-range applied? no)={rows_before}  after={rows_after}")

        print("[7] Custom-range queries captured:")
        nonzero = 0
        for name, info in QUERIES.items():
            rows = info.get("rows", "?")
            print(f"  {name}: status={info.get('status')} rows={rows}")
            print(f"    query={info.get('query','')[:140]}")
            if isinstance(rows, int) and rows > 0:
                nonzero += 1

        out = {
            "school": SCHOOL, "report_label": REPORT_LABEL,
            "range": {"from": FRM, "to": TO},
            "pulled_at": datetime.now(timezone.utc).isoformat(),
            "diag": DIAG,
            "baseline_queries": baseline,
            "custom_queries": QUERIES,
            "all_interesting_requests": list(dict.fromkeys(ALL_REQ)),
            "page_urls": PAGE_URLS,
            "modal_inputs": date_inputs,
            "date_inputs_found": date_inputs,
            "date_set_log": set_log,
            "table_rows": {"before": rows_before, "after": rows_after},
            "nonzero_custom_queries": nonzero,
        }
        OUT = MODELS / f"pike13_report_datefilter_{FRM}_{TO}.json"
        OUT.write_text(json.dumps(out, indent=2, default=str))
        print(f"\nSaved -> {OUT}")
        print(f"NONZERO custom-range queries: {nonzero}")
        await context.close()


if __name__ == "__main__":
    asyncio.run(main())
