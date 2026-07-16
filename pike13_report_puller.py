#!/usr/bin/env python3
"""
pike13_report_puller.py — Reusable date-bounded Pike13 report puller.

Verified path (2026-07-16):
  The Insights dashboard KPIs are hard-coded windows with NO date control.
  The real date-bounded data lives on the per-report detail view
  (/desk/reports#/people/details?filters=...), which fires:
      GET /desk/api/v3/reports/clients/queries?auth_token=<tok>&subdomain=<sub>
  Response shape (v3):
      {"data":{"type":"queries","attributes":{"rows":[[...]],
               "total_count":N,"has_more":bool,
               "fields":[{"name":...,"type":...}]}},
       "meta":{...}}
  The date range is encoded in the URL hash (client-side state), NOT as a
  query param. We replicate Pike13's own encoding (captured live):
      filters=(<field>:!((<op>:!('<YYYY-MM-DD>','<YYYY-MM-DD>'))))

Default field/op is last_membership_end / btW (between) — i.e. "Last
Memberships" report. Other reports use the same shape with a different
field (e.g. last_visited, first_membership_start, created_at).

Join key: the numeric `person_id` in the rows matches `pike13_people.person_id`
(8-digit Pike13 people ID) — NOT the 23-char Client hash on /api/v2 paths.

Usage (CLI):
  python3 pike13_report_puller.py --school westu-sor \
      --from 2026-07-15 --to 2026-08-11 [--field last_membership_end] [--op btW]

Programmatic:
  from pike13_report_puller import pull_report
  rows, meta = await pull_report("westu-sor", "2026-07-15", "2026-08-11")
"""
import asyncio
import sys
import json
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pike13_auto_auth
from playwright.async_api import async_playwright

SCHOOL_TO_NAME = {
    "westu-sor": "West U",
    "theheights-sor": "The Heights",
}

# Pike13's own filter encoding, captured from the live address bar.
HIDE = ("1,70,71,72,73,74,75,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,"
         "25,26,28,29,30,31,33,34,58,32,59,60,61,62,35,36,37,38,39,63,64,65,66,67,57,"
         "56.163802,56.163803,56.163805,56.163806,56.163810,56.163812,56.163813,56.166237,"
         "56.163800,56.163804,56.163807,56.163808,56.163814")


def build_report_url(subdomain: str, frm: str, to: str,
                    field: str = "last_membership_end", op: str = "btw") -> str:
    """Construct the date-bounded report URL from Pike13's hash encoding."""
    filt = f"({field}:!(({op}:!('{frm}','{to}'))))"
    sort = "(col:last_membership_end,order:d)"
    frag = (f"/people/details?filters={urllib.parse.quote(filt, safe='(),:!')}"
            f"&sort={urllib.parse.quote(sort, safe='(),:!')}&hide={HIDE}")
    return f"https://{subdomain}.pike13.com/desk/reports#{frag}"


async def _capture_queries(page):
    captured = {}

    async def on_response(resp):
        url = resp.url
        if "/queries" in url and "auth_token" in url:
            try:
                body = await resp.body()
                d = json.loads(body[:400000])
                rows, total, fields = None, None, None
                if isinstance(d, dict) and isinstance(d.get("data"), dict):
                    attrs = d["data"].get("attributes", {})
                    rows = attrs.get("rows") or []
                    total = attrs.get("total_count")
                    fields = attrs.get("fields")
                captured["payload"] = {
                    "url": url, "status": resp.status,
                    "rows": rows, "total_count": total, "fields": fields,
                }
            except Exception as e:
                captured["error"] = str(e)

    page.on("response", on_response)
    return captured


async def pull_reports_batch(subdomain: str, frm: str, to: str,
                             fields: list, op: str = "btw",
                             headless: bool = True, verbose: bool = False):
    """
    Authenticate ONCE, then pull every (field, op, from, to) in one session.
    Returns {field_token: (rows, meta)}. Tokens that return rows are real.
    """
    # Build all target URLs up front
    urls = {f: build_report_url(subdomain, frm, to, f, op) for f in fields}
    if verbose:
        for f, u in urls.items():
            print(f"[pull] {f}: {u[:90]}...")

    async with async_playwright() as p:
        context = await pike13_auto_auth.authenticate_pike13(
            p, school_subdomain=subdomain, headless=headless, verbose=verbose
        )
        page = context.pages[0]
        out = {}
        for f, url in urls.items():
            cap = await _capture_queries(page)   # re-arm listener per field
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(6000)
            if "payload" not in cap:
                try:
                    await page.locator("body").click(timeout=3000)
                    await page.wait_for_timeout(4000)
                except Exception:
                    pass
            rows = cap.get("payload", {}).get("rows") or []
            fields_seen = cap.get("payload", {}).get("fields") or []
            total = cap.get("payload", {}).get("total_count")
            status = cap.get("payload", {}).get("status")
            field_names = [x.get("name") for x in fields_seen] if fields_seen else None
            meta = {
                "subdomain": subdomain,
                "school": SCHOOL_TO_NAME.get(subdomain, subdomain),
                "field": f, "op": op, "from": frm, "to": to,
                "report_url": url,
                "http_status": status,
                "returned_rows": len(rows),
                "total_count": total,
                "field_names": field_names,
                "pulled_at": datetime.now(timezone.utc).isoformat(),
                "error": cap.get("error"),
            }
            out[f] = (rows, meta)
            if verbose:
                print(f"    {f}: HTTP {status} rows={len(rows)} total={total} names={field_names}")
        await context.close()
    return out


async def pull_report(subdomain: str, frm: str, to: str,
                     field: str = "last_membership_end", op: str = "btw",
                     headless: bool = True, verbose: bool = False):
    """
    Authenticate, open the date-bounded report, read the /queries payload.
    Returns (rows:list[list], meta:dict) where meta includes field names,
    total_count, auth_subdomain, and the exact report URL used.
    """
    batch = await pull_reports_batch(subdomain, frm, to, [field], op, headless, verbose)
    rows, meta = batch[field]
    return rows, meta


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--school", default="westu-sor")
    ap.add_argument("--from", dest="frm", required=True)
    ap.add_argument("--to", dest="to", required=True)
    ap.add_argument("--field", default="last_membership_end")
    ap.add_argument("--op", default="btw")
    ap.add_argument("--out", default=None, help="path to write rows JSON")
    ap.add_argument("--verbose", action="store_true")
    a = ap.parse_args()

    rows, meta = asyncio.run(pull_report(
        a.school, a.frm, a.to, a.field, a.op, verbose=a.verbose))

    print(f"HTTP {meta['http_status']} | rows returned: {len(rows)} "
          f"| total_count: {meta['total_count']}")
    print(f"field_names: {meta['field_names']}")
    if a.out:
        Path(a.out).write_text(json.dumps(
            {"meta": meta, "rows": rows}, indent=2, default=str))
        print(f"wrote -> {a.out}")
    else:
        print(json.dumps({"meta": meta, "rows": rows[:3]}, indent=2, default=str)[:800])


if __name__ == "__main__":
    main()
