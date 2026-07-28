#!/usr/bin/env python3
"""Extract complete current-member rosters from Pike13 for both schools.

The Pike13 report is paginated in 50-row chunks. This scraper captures the
report API responses while driving Pike13's own "Load 50 more results" control,
maps row values by returned field name (never by positional guesses), verifies
coverage against Pike13's total_count, and writes dated SQLite snapshots.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "reminders.db"
MODELS_DIR = ROOT / "models"
SCHOOLS = {"westu-sor": "West U", "theheights-sor": "The Heights"}
REPORT_PATH = "/desk/reports#/people/details?filters=(has_membership:!((eq:!(t))))"
FULL_ROSTER_TIMEOUT_MS = 120_000


def load_env() -> None:
    env_path = Path.home() / ".hermes" / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        if "=" not in line or line.lstrip().startswith("#"):
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env()
sys.path.insert(0, str(ROOT))
from playwright.async_api import async_playwright  # noqa: E402
import pike13_auto_auth  # noqa: E402


def rows_to_records(field_names: list[str], rows: list[list[Any]]) -> list[dict[str, Any]]:
    """Map Pike13 row arrays to dictionaries and validate their shape."""
    if not field_names:
        raise ValueError("Pike13 response did not include field names")
    records: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if len(row) != len(field_names):
            raise ValueError(
                f"Pike13 row {index} has {len(row)} values for {len(field_names)} fields"
            )
        records.append(dict(zip(field_names, row)))
    return records


def full_roster_request(base_request_body: dict[str, Any], total_count: int) -> dict[str, Any]:
    """Clone a Pike13 report request and expand its cumulative row limit."""
    if total_count <= 0:
        raise ValueError("Pike13 total_count must be positive")
    request_body = json.loads(json.dumps(base_request_body))
    try:
        request_body["data"]["attributes"]["page"] = {"limit": total_count}
    except (KeyError, TypeError) as exc:
        raise ValueError("Unexpected Pike13 report request shape") from exc
    return request_body


def coverage_request_bodies(
    base_request_body: dict[str, Any], total_count: int, page_limit: int = 100
) -> list[dict[str, Any]]:
    """Cover a roster from both ends without asking Pike13 for a 100+ row query.

    Pike13's report gateway can time out on a single cumulative request larger
    than 100 rows. For rosters up to twice the safe limit, ascending and
    descending person_id windows overlap and together cover every member.
    """
    if total_count <= 0:
        raise ValueError("Pike13 total_count must be positive")
    if total_count > page_limit * 2:
        raise ValueError(
            f"Pike13 roster of {total_count} exceeds two-window coverage limit {page_limit * 2}"
        )
    sorts = [["person_id"]]
    if total_count > page_limit:
        sorts.append(["person_id-"])
    requests = []
    for sort in sorts:
        request_body = json.loads(json.dumps(base_request_body))
        attrs = request_body["data"]["attributes"]
        attrs["page"] = {"limit": min(page_limit, total_count)}
        attrs["sort"] = sort
        requests.append(request_body)
    return requests


def store_snapshot(
    db_path: Path,
    school_slug: str,
    school_name: str,
    scraped_at: str,
    records: list[dict[str, Any]],
) -> None:
    """Atomically replace one school's snapshot for this scrape timestamp."""
    if not records:
        raise ValueError(f"Refusing to store empty roster for {school_slug}")
    person_ids = [str(r.get("person_id") or "").strip() for r in records]
    if any(not p for p in person_ids):
        raise ValueError(f"Roster for {school_slug} contains a blank person_id")
    if len(set(person_ids)) != len(person_ids):
        raise ValueError(f"Roster for {school_slug} contains duplicate person_id values")

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pike13_current_member_snapshots (
                school_slug TEXT NOT NULL,
                school_name TEXT NOT NULL,
                scraped_at TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                person_id TEXT NOT NULL,
                full_name TEXT NOT NULL,
                person_state TEXT,
                current_plans TEXT,
                current_plan_types TEXT,
                revenue_categories TEXT,
                client_since_date TEXT,
                last_visit_date TEXT,
                days_since_last_visit INTEGER,
                completed_visits INTEGER,
                future_visits INTEGER,
                has_membership INTEGER NOT NULL,
                has_plan_on_hold INTEGER,
                primary_staff_name TEXT,
                account_manager_names TEXT,
                account_manager_emails TEXT,
                account_manager_phones TEXT,
                guardian_name TEXT,
                guardian_email TEXT,
                raw_json TEXT NOT NULL,
                PRIMARY KEY (school_slug, scraped_at, person_id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pcm_latest "
            "ON pike13_current_member_snapshots(school_slug, scraped_at)"
        )
        snapshot_date = scraped_at[:10]
        with conn:
            for r in records:
                def as_int(value: Any) -> int | None:
                    if value in (None, ""):
                        return None
                    if value in (True, "t", "true", "True"):
                        return 1
                    if value in (False, "f", "false", "False"):
                        return 0
                    try:
                        return int(value)
                    except (TypeError, ValueError):
                        return None

                conn.execute(
                    """
                    INSERT OR REPLACE INTO pike13_current_member_snapshots (
                        school_slug, school_name, scraped_at, snapshot_date,
                        person_id, full_name, person_state, current_plans,
                        current_plan_types, revenue_categories, client_since_date,
                        last_visit_date, days_since_last_visit, completed_visits,
                        future_visits, has_membership, has_plan_on_hold,
                        primary_staff_name, account_manager_names,
                        account_manager_emails, account_manager_phones,
                        guardian_name, guardian_email, raw_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        school_slug,
                        school_name,
                        scraped_at,
                        snapshot_date,
                        str(r["person_id"]),
                        str(r.get("full_name") or "").strip(),
                        r.get("person_state"),
                        r.get("current_plans"),
                        r.get("current_plan_types"),
                        r.get("current_plan_revenue_category"),
                        r.get("client_since_date"),
                        r.get("last_visit_date"),
                        as_int(r.get("days_since_last_visit")),
                        as_int(r.get("completed_visits")),
                        as_int(r.get("future_visits")),
                        as_int(r.get("has_membership")) or 0,
                        as_int(r.get("has_plan_on_hold")),
                        r.get("primary_staff_name"),
                        r.get("account_manager_names"),
                        r.get("account_manager_emails"),
                        r.get("account_manager_phones"),
                        r.get("guardian_name"),
                        r.get("guardian_email"),
                        json.dumps(r, ensure_ascii=True, default=str),
                    ),
                )
    finally:
        conn.close()


async def scrape_school(slug: str, max_pages: int = 20) -> tuple[list[dict[str, Any]], int]:
    async with async_playwright() as playwright:
        context = await pike13_auto_auth.authenticate_pike13(
            playwright, school_subdomain=slug, headless=True, verbose=False
        )
        page = context.pages[0]
        captured_rows: list[list[Any]] = []
        field_names: list[str] = []
        total_count: int | None = None
        capture_errors: list[str] = []
        page_debug: list[dict[str, Any]] = []
        api_url: str | None = None
        base_request_body: dict[str, Any] | None = None

        async def on_response(response) -> None:
            nonlocal field_names, total_count, api_url, base_request_body
            if "/api/v3/reports/clients/queries" not in response.url:
                return
            try:
                payload = await response.json()
                attrs = payload.get("data", {}).get("attributes", {})
                api_url = response.url
                if response.request.post_data:
                    base_request_body = json.loads(response.request.post_data)
                rows = attrs.get("rows") or []
                fields = attrs.get("fields") or []
                if fields:
                    names = [f.get("name") for f in fields]
                    if any(not n for n in names):
                        raise ValueError("Pike13 returned an unnamed report field")
                    if field_names and names != field_names:
                        raise ValueError("Pike13 field order changed during pagination")
                    field_names = names
                captured_rows.extend(rows)
                page_debug.append(
                    {
                        "request_body": response.request.post_data,
                        "row_count": len(rows),
                        "first_row": rows[0] if rows else None,
                        "last_row": rows[-1] if rows else None,
                        "has_more": attrs.get("has_more"),
                        "total_count": attrs.get("total_count"),
                    }
                )
                if attrs.get("total_count") is not None:
                    total_count = int(attrs["total_count"])
            except Exception as exc:  # surfaced after navigation
                capture_errors.append(repr(exc))

        page.on("response", on_response)
        try:
            await page.goto(
                f"https://{slug}.pike13.com{REPORT_PATH}",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await page.wait_for_function(
                """() => document.querySelector('table') !== null
                        || location.pathname.includes('sign_in')
                        || location.pathname.includes('two_factor')""",
                timeout=30000,
            )
            if "sign_in" in page.url or "two_factor" in page.url:
                raise RuntimeError(f"Authentication expired on current-member report: {page.url}")
            await page.wait_for_timeout(2000)

            # Pike13's UI "Load more" does not use a cursor. It repeats the
            # query with page.limit=100, then 150, returning all rows from the
            # beginning each time. Replay that authenticated request directly
            # at total_count so viewport/Angular state cannot truncate data.
            if total_count is None or api_url is None or base_request_body is None:
                raise RuntimeError(f"Could not capture Pike13 report request for {slug}")
            if len(captured_rows) < total_count:
                coverage_rows: list[list[Any]] = []
                for request_body in coverage_request_bodies(base_request_body, total_count):
                    replay = await context.request.post(
                        api_url,
                        headers={
                            "Accept": "application/vnd.api+json",
                            "Content-Type": "application/vnd.api+json",
                        },
                        data=json.dumps(request_body),
                        timeout=FULL_ROSTER_TIMEOUT_MS,
                    )
                    if not replay.ok:
                        raise RuntimeError(
                            f"Pike13 roster coverage replay failed for {slug}: HTTP {replay.status}"
                        )
                    replay_payload = await replay.json()
                    replay_attrs = replay_payload.get("data", {}).get("attributes", {})
                    replay_fields = [f.get("name") for f in replay_attrs.get("fields", [])]
                    if replay_fields != field_names:
                        raise RuntimeError(
                            f"Pike13 field order changed on roster coverage replay for {slug}"
                        )
                    coverage_rows.extend(replay_attrs.get("rows") or [])
                captured_rows[:] = coverage_rows
        finally:
            await context.close()

        if capture_errors:
            raise RuntimeError(f"Pike13 response parsing failed for {slug}: {capture_errors}")
        if total_count is None:
            raise RuntimeError(f"Pike13 did not return total_count for {slug}")
        records = rows_to_records(field_names, captured_rows)
        deduped = {str(r.get("person_id")): r for r in records}
        if len(deduped) != total_count:
            Path("/tmp/pike13_pagination_debug.json").write_text(
                json.dumps(
                    {"field_names": field_names, "pages": page_debug},
                    indent=2,
                    default=str,
                )
            )
            raise RuntimeError(
                f"Partial Pike13 roster for {slug}: captured {len(deduped)} unique "
                f"members, report says {total_count}"
            )
        return list(deduped.values()), total_count


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--school", choices=[*SCHOOLS, "all"], default="all")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--no-json", action="store_true")
    args = parser.parse_args()
    selected = SCHOOLS if args.school == "all" else {args.school: SCHOOLS[args.school]}
    scraped_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for slug, name in selected.items():
        print(f"Scraping current members: {name} ({slug})")
        records, total = await scrape_school(slug)
        store_snapshot(args.db, slug, name, scraped_at, records)
        if not args.no_json:
            out = MODELS_DIR / f"pike13_current_members_{slug}.json"
            out.write_text(
                json.dumps(
                    {"school": name, "school_slug": slug, "scraped_at": scraped_at,
                     "total_count": total, "records": records},
                    indent=2,
                    ensure_ascii=True,
                    default=str,
                )
            )
        print(f"  Verified {total} of {total} current members")


if __name__ == "__main__":
    asyncio.run(main())
