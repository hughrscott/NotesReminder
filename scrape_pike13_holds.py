#!/usr/bin/env python3
"""Scrape active and recently ended Pike13 holds with complete API validation."""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from playwright.async_api import (
    BrowserContext,
    Page,
    Response,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

sys.path.insert(0, str(Path(__file__).parent))
import pike13_auto_auth
from scrape_pike13_current_members import full_roster_request, rows_to_records

MODELS_DIR = Path(__file__).parent / "models"
SCHOOLS = {"westu-sor": "West U", "theheights-sor": "The Heights"}
REPORT_ENDPOINT_PREFIX = "/api/v3/reports/"


def report_url(slug: str, filter_name: str, values: list[str] | None = None) -> str:
    if filter_name == "is_on_hold":
        filters = "(is_on_hold:!((eq:!(t))))"
    elif filter_name == "last_hold_end_date" and values and len(values) == 2:
        filters = (
            "(last_hold_end_date:!((btw:!("
            f"'{values[0]}','{values[1]}'"
            "))))"
        )
    else:
        raise ValueError(f"Unsupported Pike13 hold filter: {filter_name}")
    return f"https://{slug}.pike13.com/desk/reports#/person_plans/details?filters={filters}"


async def scrape_report(page: Page, url: str, label: str) -> tuple[list[dict[str, Any]], int]:
    rows: list[list[Any]] = []
    fields: list[str] = []
    total: int | None = None
    api_url: str | None = None
    request_body: dict[str, Any] | None = None
    errors: list[str] = []

    async def capture(response: Response) -> None:
        nonlocal rows, fields, total, api_url, request_body
        if REPORT_ENDPOINT_PREFIX not in response.url or response.request.method != "POST":
            return
        try:
            payload = await response.json()
            attrs = payload.get("data", {}).get("attributes", {})
            candidate_fields = attrs.get("fields") or []
            candidate_rows = attrs.get("rows") or []
            if not candidate_fields or not isinstance(candidate_rows, list):
                return
            names = [str(field.get("name", "")) for field in candidate_fields]
            if "Last Hold End Date" not in names or "Client" not in names:
                return
            fields = names
            rows = candidate_rows
            total = int(attrs.get("total_count") or len(candidate_rows))
            api_url = response.url
            request_body = json.loads(response.request.post_data or "{}")
        except Exception as exc:
            errors.append(str(exc))

    page.on("response", capture)
    try:
        for attempt in range(1, 4):
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            try:
                await page.wait_for_function(
                    """() => document.querySelector('table') !== null
                            || location.pathname.includes('sign_in')
                            || location.pathname.includes('two_factor')""",
                    timeout=30000,
                )
                break
            except PlaywrightTimeoutError:
                if attempt == 3:
                    raise RuntimeError(
                        f"Pike13 report SPA did not render {label} after 3 attempts"
                    )
                reports_home = url.split("/desk/", 1)[0] + "/desk/reports"
                await page.goto(
                    reports_home, wait_until="domcontentloaded", timeout=30000
                )
                await page.wait_for_timeout(2000 * attempt)
        if "sign_in" in page.url or "two_factor" in page.url:
            raise RuntimeError(f"Pike13 session expired while loading {label}: {page.url}")
        for _ in range(50):
            if total is not None:
                break
            await page.wait_for_timeout(200)
        if total is None:
            dom = await page.evaluate(
                """() => {
                    for (const table of document.querySelectorAll('table')) {
                        const fields = Array.from(table.querySelectorAll('th'))
                            .map(h => (h.innerText || '').trim());
                        if (!fields.includes('Client') || !fields.includes('Last Hold End Date')) continue;
                        const rows = Array.from(table.querySelectorAll('tbody tr')).map(tr =>
                            Array.from(tr.querySelectorAll('td')).map(td => (td.innerText || '').trim())
                        ).filter(row => row.length > 0);
                        return {fields, rows};
                    }
                    return null;
                }"""
            )
            if not dom:
                detail = "; ".join(errors) if errors else "no matching report table"
                raise RuntimeError(f"Could not parse {label}: {detail}")
            fields = dom["fields"]
            rows = dom["rows"]
            body_text = await page.locator("body").inner_text()
            total_match = re.search(
                r"(?:ALL\s+|OF\s+)([\d,]+)\s+RESULTS?", body_text, re.I
            )
            total = int(total_match.group(1).replace(",", "")) if total_match else len(rows)
        if len(rows) < total:
            if api_url is None or request_body is None:
                raise RuntimeError(
                    f"Partial rendered Pike13 {label}: {len(rows)} of {total}; "
                    "API replay request was not captured"
                )
            replay = await page.context.request.post(
                api_url,
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                data=json.dumps(full_roster_request(request_body, total)),
            )
            if not replay.ok:
                raise RuntimeError(f"Pike13 {label} replay failed: HTTP {replay.status}")
            attrs = (await replay.json()).get("data", {}).get("attributes", {})
            replay_fields = [str(field.get("name", "")) for field in attrs.get("fields") or []]
            if replay_fields != fields:
                raise RuntimeError(f"Pike13 schema changed while replaying {label}")
            rows = attrs.get("rows") or []
        records = rows_to_records(fields, rows)
        if len(records) != total:
            raise RuntimeError(f"Partial Pike13 {label}: {len(records)} of {total}")
        return records, total
    finally:
        page.remove_listener("response", capture)


def normalize_record(row: dict[str, Any], slug: str, scraped_at: str) -> dict[str, Any]:
    client = str(row.get("Client") or "").strip()
    if not client:
        raise ValueError("Pike13 hold row has no Client")
    return {
        "client": client,
        "first_name": str(row.get("First Name") or "").strip(),
        "last_name": str(row.get("Last Name") or "").strip(),
        "plan": str(row.get("Plan Name") or "").strip(),
        "on_hold": str(row.get("On Hold?") or "").strip().lower() == "yes",
        "hold_start": str(row.get("Last Hold Start Date") or "").strip(),
        "hold_end": str(row.get("Last Hold End Date") or "").strip(),
        "hold_indefinite": str(row.get("Last Hold Indefinite?") or "").strip().lower() == "yes",
        "hold_by": str(row.get("Last Hold By") or "").strip(),
        "account_managers": str(row.get("Account Managers") or "").strip(),
        "account_emails": str(row.get("Account Manager Emails") or "").strip(),
        "account_phones": str(row.get("Account Manager Phones") or "").strip(),
        "start_date": str(row.get("Start Date") or "").strip(),
        "ended": str(row.get("Ended?") or "").strip().lower() == "yes",
        "canceled": str(row.get("Canceled?") or "").strip().lower() == "yes",
        "base_price": str(row.get("Base Price") or "").strip(),
        "school_slug": slug,
        "scraped_at": scraped_at,
    }


async def scrape_holds(slug: str, as_of: date) -> list[dict[str, Any]]:
    async with async_playwright() as playwright:
        context: BrowserContext = await pike13_auto_auth.authenticate_pike13(
            playwright, school_subdomain=slug, headless=True, verbose=False
        )
        page = await context.new_page()
        try:
            await page.goto(
                f"https://{slug}.pike13.com/desk/reports",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await page.wait_for_timeout(3000)
            active_rows, active_total = await scrape_report(
                page, report_url(slug, "is_on_hold"), f"active holds for {slug}"
            )
            start = (as_of - timedelta(days=30)).isoformat()
            recent_rows, recent_total = await scrape_report(
                page,
                report_url(slug, "last_hold_end_date", [start, as_of.isoformat()]),
                f"recent holds for {slug}",
            )
        finally:
            await context.close()

    scraped_at = date.today().isoformat()
    combined: dict[tuple[str, str, str, bool], dict[str, Any]] = {}
    for row in active_rows + recent_rows:
        record = normalize_record(row, slug, scraped_at)
        key = (record["client"].lower(), record["plan"].lower(), record["hold_end"], record["on_hold"])
        combined[key] = record
    records = list(combined.values())
    active_count = sum(1 for row in records if row["on_hold"])
    recent_count = sum(1 for row in records if not row["on_hold"] and row["hold_end"])
    print(
        f"  Verified {active_count} active hold plans ({active_total} report rows) and "
        f"{recent_count} recently ended hold plans ({recent_total} report rows)"
    )
    return records


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--school", choices=[*SCHOOLS, "all"], default="all")
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today())
    args = parser.parse_args()
    MODELS_DIR.mkdir(exist_ok=True)
    schools = list(SCHOOLS) if args.school == "all" else [args.school]
    for slug in schools:
        print(f"Scraping holds: {SCHOOLS[slug]} ({slug})")
        records = await scrape_holds(slug, args.as_of)
        out_path = MODELS_DIR / f"pike13_holds_{slug}.json"
        out_path.write_text(json.dumps(records, indent=2, sort_keys=True) + "\n")
        print(f"  Wrote {len(records)} hold records to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
