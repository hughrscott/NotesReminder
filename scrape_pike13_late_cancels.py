#!/usr/bin/env python3
"""Extract Pike13 late cancellations for shadow-mode retention validation."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "reminders.db"
SCHOOLS = ("westu-sor", "theheights-sor")


def load_env() -> None:
    path = Path.home() / ".hermes" / ".env"
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        if "=" not in line or line.lstrip().startswith("#"):
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env()
sys.path.insert(0, str(ROOT))
from playwright.async_api import async_playwright  # noqa: E402
import pike13_auto_auth  # noqa: E402
from late_cancel_shadow import ensure_schema, ingest_late_cancel_records  # noqa: E402
from scrape_pike13_current_members import full_roster_request, rows_to_records  # noqa: E402


def build_report_path(start_date: str, end_date: str) -> str:
    filters = (
        f"(service_date:!((btw:!('{start_date}','{end_date}'))),"
        "state:!((eq:!(late_canceled))))"
    )
    sort = "(col:service_date,order:d)"
    return (
        "/desk/reports#/enrollments/details?filters="
        f"{urllib.parse.quote(filters, safe='(),:!')}"
        f"&sort={urllib.parse.quote(sort, safe='(),:!')}"
    )


async def scrape_school(
    school_slug: str, start_date: str, end_date: str
) -> tuple[list[dict[str, Any]], int]:
    async with async_playwright() as playwright:
        context = await pike13_auto_auth.authenticate_pike13(
            playwright, school_subdomain=school_slug, headless=True, verbose=False
        )
        page = context.pages[0]
        rows: list[list[Any]] = []
        fields: list[str] = []
        total_count: int | None = None
        api_url: str | None = None
        request_body: dict[str, Any] | None = None
        errors: list[str] = []

        async def on_response(response) -> None:
            nonlocal fields, total_count, api_url, request_body
            if "/api/v3/reports/" not in response.url or "/queries" not in response.url:
                return
            try:
                payload = await response.json()
                attrs = payload.get("data", {}).get("attributes", {})
                response_fields = attrs.get("fields") or []
                response_rows = attrs.get("rows") or []
                if response_fields:
                    names = [field.get("name") for field in response_fields]
                    if any(not name for name in names):
                        raise ValueError("Pike13 returned an unnamed report field")
                    if fields and names != fields:
                        raise ValueError("Pike13 field order changed during extraction")
                    fields = names
                rows.extend(response_rows)
                if attrs.get("total_count") is not None:
                    total_count = int(attrs["total_count"])
                api_url = response.url
                if response.request.post_data:
                    request_body = json.loads(response.request.post_data)
            except Exception as exc:
                errors.append(repr(exc))

        page.on("response", on_response)
        try:
            await page.goto(
                f"https://{school_slug}.pike13.com{build_report_path(start_date, end_date)}",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await page.wait_for_timeout(8000)
            if "sign_in" in page.url or "two_factor" in page.url:
                raise RuntimeError(f"Authentication expired on late-cancel report: {page.url}")
            if errors:
                raise RuntimeError(f"Pike13 response parsing failed: {errors}")
            if total_count is None:
                raise RuntimeError(f"Could not capture Pike13 late-cancel report for {school_slug}")
            if total_count == 0:
                return [], 0
            if not fields or api_url is None or request_body is None:
                raise RuntimeError(f"Incomplete Pike13 report metadata for {school_slug}")
            if len(rows) < total_count:
                replay = await context.request.post(
                    api_url,
                    data=full_roster_request(request_body, total_count),
                )
                if not replay.ok:
                    raise RuntimeError(f"Expanded Pike13 report request failed: HTTP {replay.status}")
                attrs = (await replay.json()).get("data", {}).get("attributes", {})
                replay_fields = [field.get("name") for field in attrs.get("fields") or []]
                if replay_fields != fields:
                    raise RuntimeError("Pike13 field order changed in expanded response")
                rows = attrs.get("rows") or []
            if len(rows) != total_count:
                raise RuntimeError(
                    f"Incomplete late-cancel report for {school_slug}: "
                    f"captured {len(rows)} of {total_count}"
                )
            return rows_to_records(fields, rows), total_count
        finally:
            await context.close()


def record_run(
    conn: sqlite3.Connection,
    school_slug: str,
    start_date: str,
    end_date: str,
    scraped_at: str,
    row_count: int,
) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS pike13_late_cancel_extract_runs (
            school_slug TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            PRIMARY KEY (school_slug, scraped_at)
        )"""
    )
    conn.execute(
        "INSERT INTO pike13_late_cancel_extract_runs VALUES (?,?,?,?,?)",
        (school_slug, start_date, end_date, scraped_at, row_count),
    )


async def run(args: argparse.Namespace) -> None:
    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    ensure_schema(conn)
    try:
        for school in args.school:
            records, total = await scrape_school(school, args.start_date, args.end_date)
            scraped_at = datetime.now(timezone.utc).isoformat()
            stored = ingest_late_cancel_records(conn, school, scraped_at, records)
            with conn:
                record_run(
                    conn, school, args.start_date, args.end_date, scraped_at, total
                )
            print(f"{school}: CAPTURED {total}; STORED {stored} UNIQUE EVENTS")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--school", action="append", choices=SCHOOLS)
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--start-date")
    args = parser.parse_args()
    args.school = args.school or list(SCHOOLS)
    if args.start_date is None:
        args.start_date = (
            date.fromisoformat(args.end_date) - timedelta(days=59)
        ).isoformat()
    if date.fromisoformat(args.start_date) > date.fromisoformat(args.end_date):
        parser.error("--start-date must be on or before --end-date")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
