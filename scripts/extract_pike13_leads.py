#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import sqlite3
import sys
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import (  # noqa: E402
    DEFAULT_INITIAL_LOAD_START,
    ensure_lead_followup_schema,
    finish_import_run,
    normalize_email,
    normalize_phone,
    start_import_run,
    utc_now_iso,
)


DEFAULT_URL = "https://westu-sor.pike13.com"
PERSON_RE = re.compile(r"/people/(\d+)")
EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}")
VISIT_RE = re.compile(r"/visits/(\d+)")
EVENT_RE = re.compile(r"/events/(\d+)")


def stable_id(prefix, *parts):
    digest = hashlib.sha256("|".join(str(p or "") for p in parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:24]}"


def text_after(label, text):
    pattern = re.compile(rf"{re.escape(label)}\s+([^\n]+)", re.IGNORECASE)
    match = pattern.search(text)
    return match.group(1).strip() if match else None


def person_id_from_url(url):
    match = PERSON_RE.search(url)
    return match.group(1) if match else None


def upsert_person(conn, row):
    conn.execute(
        """
        INSERT INTO pike13_people (
            person_id, full_name, first_name, last_name, email, email_normalized,
            phone, phone_normalized, membership_state, school, source_url,
            raw_text, raw_json, updated_at
        )
        VALUES (
            :person_id, :full_name, :first_name, :last_name, :email, :email_normalized,
            :phone, :phone_normalized, :membership_state, :school, :source_url,
            :raw_text, :raw_json, :updated_at
        )
        ON CONFLICT(person_id) DO UPDATE SET
            full_name = COALESCE(excluded.full_name, pike13_people.full_name),
            email = COALESCE(excluded.email, pike13_people.email),
            email_normalized = COALESCE(excluded.email_normalized, pike13_people.email_normalized),
            phone = COALESCE(excluded.phone, pike13_people.phone),
            phone_normalized = COALESCE(excluded.phone_normalized, pike13_people.phone_normalized),
            membership_state = COALESCE(excluded.membership_state, pike13_people.membership_state),
            school = COALESCE(excluded.school, pike13_people.school),
            source_url = COALESCE(excluded.source_url, pike13_people.source_url),
            raw_text = excluded.raw_text,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )


def upsert_visit(conn, row):
    conn.execute(
        """
        INSERT INTO pike13_visits (
            visit_id, person_id, event_id, service, starts_at, status,
            no_show_flag, unpaid_flag, waiver_flag, school, source_url,
            raw_text, raw_json, updated_at
        )
        VALUES (
            :visit_id, :person_id, :event_id, :service, :starts_at, :status,
            :no_show_flag, :unpaid_flag, :waiver_flag, :school, :source_url,
            :raw_text, :raw_json, :updated_at
        )
        ON CONFLICT(visit_id) DO UPDATE SET
            person_id = COALESCE(excluded.person_id, pike13_visits.person_id),
            event_id = COALESCE(excluded.event_id, pike13_visits.event_id),
            service = COALESCE(excluded.service, pike13_visits.service),
            starts_at = COALESCE(excluded.starts_at, pike13_visits.starts_at),
            status = COALESCE(excluded.status, pike13_visits.status),
            no_show_flag = excluded.no_show_flag,
            unpaid_flag = excluded.unpaid_flag,
            waiver_flag = excluded.waiver_flag,
            source_url = COALESCE(excluded.source_url, pike13_visits.source_url),
            raw_text = excluded.raw_text,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )


def upsert_plan_pass(conn, row):
    conn.execute(
        """
        INSERT INTO pike13_plans_passes (
            plan_pass_id, person_id, name, status, starts_at, ends_at, school,
            source_url, raw_text, raw_json, updated_at
        )
        VALUES (
            :plan_pass_id, :person_id, :name, :status, :starts_at, :ends_at, :school,
            :source_url, :raw_text, :raw_json, :updated_at
        )
        ON CONFLICT(plan_pass_id) DO UPDATE SET
            name = COALESCE(excluded.name, pike13_plans_passes.name),
            status = COALESCE(excluded.status, pike13_plans_passes.status),
            starts_at = COALESCE(excluded.starts_at, pike13_plans_passes.starts_at),
            ends_at = COALESCE(excluded.ends_at, pike13_plans_passes.ends_at),
            source_url = COALESCE(excluded.source_url, pike13_plans_passes.source_url),
            raw_text = excluded.raw_text,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )


def parse_person(page, school):
    text = page.locator("body").inner_text(timeout=30000)
    person_id = person_id_from_url(page.url)
    if not person_id:
        raise ValueError(f"Could not find Pike13 person ID in URL: {page.url}")
    email_match = EMAIL_RE.search(text)
    phone_match = PHONE_RE.search(text)
    first_line = next((line.strip() for line in text.splitlines() if line.strip()), None)
    person = {
        "person_id": person_id,
        "full_name": text_after("Name", text) or first_line,
        "first_name": None,
        "last_name": None,
        "email": email_match.group(0) if email_match else None,
        "email_normalized": normalize_email(email_match.group(0)) if email_match else None,
        "phone": phone_match.group(0) if phone_match else None,
        "phone_normalized": normalize_phone(phone_match.group(0)) if phone_match else None,
        "membership_state": text_after("Membership", text) or text_after("Status", text),
        "school": school,
        "source_url": page.url,
        "raw_text": text,
        "raw_json": json.dumps({"extraction": "person_text"}, sort_keys=True),
        "updated_at": utc_now_iso(),
    }
    return person, text


def capture_related_rows(person_id, url, text, school):
    visits = []
    plans = []
    event_match = EVENT_RE.search(text)
    for visit_id in sorted(set(VISIT_RE.findall(text))):
        visits.append(
            {
                "visit_id": visit_id,
                "person_id": person_id,
                "event_id": event_match.group(1) if event_match else None,
                "service": text_after("Service", text) or text_after("Event", text),
                "starts_at": text_after("Date", text) or text_after("Starts", text),
                "status": text_after("Status", text),
                "no_show_flag": 1 if re.search(r"no[\s-]?show", text, re.IGNORECASE) else 0,
                "unpaid_flag": 1 if re.search(r"\bunpaid\b", text, re.IGNORECASE) else 0,
                "waiver_flag": 1 if re.search(r"\bwaiver\b", text, re.IGNORECASE) else 0,
                "school": school,
                "source_url": url,
                "raw_text": text,
                "raw_json": json.dumps({"extraction": "person_related_text"}, sort_keys=True),
                "updated_at": utc_now_iso(),
            }
        )
    if re.search(r"\b(pass|plan|membership)\b", text, re.IGNORECASE):
        plans.append(
            {
                "plan_pass_id": stable_id("pike13_plan_pass", person_id, text_after("Plan", text), text_after("Pass", text), url),
                "person_id": person_id,
                "name": text_after("Plan", text) or text_after("Pass", text) or text_after("Membership", text),
                "status": text_after("Status", text),
                "starts_at": text_after("Start", text),
                "ends_at": text_after("End", text),
                "school": school,
                "source_url": url,
                "raw_text": text,
                "raw_json": json.dumps({"extraction": "person_plan_text"}, sort_keys=True),
                "updated_at": utc_now_iso(),
            }
        )
    return visits, plans


def person_urls_from_db(conn, base_url, limit):
    rows = conn.execute(
        """
        SELECT DISTINCT pike13_person_id
        FROM hubspot_deals
        WHERE pike13_person_id IS NOT NULL AND pike13_person_id != ''
        ORDER BY pike13_person_id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [f"{base_url.rstrip('/')}/people/{row[0]}" for row in rows]


def wait_until_ready(page, timeout=30000):
    page.wait_for_load_state("load", timeout=timeout)
    try:
        page.wait_for_load_state("networkidle", timeout=5000)
    except PlaywrightTimeoutError:
        pass


def main():
    parser = argparse.ArgumentParser(description="Extract Pike13 lead outcome details into SQLite.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--profile-dir", default="browser_profiles/pike13-westu")
    parser.add_argument("--base-url", default=DEFAULT_URL)
    parser.add_argument("--person-url", action="append", default=[])
    parser.add_argument("--headless", action="store_true")
    parser.add_argument(
        "--interactive-login",
        action="store_true",
        help="Open the base URL and wait for login before extracting in the same browser session.",
    )
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--school", default="West U")
    parser.add_argument("--start-date", default=DEFAULT_INITIAL_LOAD_START)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    ensure_lead_followup_schema(conn)
    urls = args.person_url or person_urls_from_db(conn, args.base_url, args.limit)
    run_id = start_import_run(conn, "pike13", Path(__file__).name, args.start_date, None, {"base_url": args.base_url, "url_count": len(urls)})
    conn.commit()
    rows_seen = rows_written = 0
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                args.profile_dir,
                headless=args.headless,
                viewport={"width": 1440, "height": 1000},
            )
            page = context.pages[0] if context.pages else context.new_page()
            if args.interactive_login:
                page.goto(args.base_url, wait_until="domcontentloaded", timeout=60000)
                wait_until_ready(page)
                print("Complete Pike13 login/navigation in the browser, then press Enter here.")
                input()
            for url in urls[: args.limit]:
                rows_seen += 1
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                wait_until_ready(page)
                person, text = parse_person(page, args.school)
                upsert_person(conn, person)
                rows_written += 1
                visits, plans = capture_related_rows(person["person_id"], page.url, text, args.school)
                for visit in visits:
                    upsert_visit(conn, visit)
                    rows_written += 1
                for plan in plans:
                    upsert_plan_pass(conn, plan)
                    rows_written += 1
            context.close()
        finish_import_run(conn, run_id, "success", rows_seen, rows_written, 0)
        conn.commit()
    except Exception as exc:
        finish_import_run(conn, run_id, "error", rows_seen, rows_written, 0, str(exc))
        conn.commit()
        raise
    finally:
        conn.close()

    print(f"Pike13 extraction complete: rows_seen={rows_seen} rows_written={rows_written}")


if __name__ == "__main__":
    main()
