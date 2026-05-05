#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import sqlite3
import sys
from pathlib import Path
from urllib.parse import urljoin

from dateutil import parser as date_parser
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
DEFAULT_PROFILE_DIR = "browser_profiles/pike13"
PERSON_RE = re.compile(r"/people/(\d+)")
EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}")
VISIT_RE = re.compile(r"/visits/(\d+)")
EVENT_RE = re.compile(r"/events/(\d+)")
DATE_RE = re.compile(
    r"\b(?:Mon|Tue|Tues|Wed|Thu|Thurs|Fri|Sat|Sun)?(?:day)?[,]?\s*"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}"
    r"(?:\s+(?:at\s+)?\d{1,2}:\d{2}\s*(?:AM|PM)?)?",
    re.IGNORECASE,
)
NUMERIC_DATE_RE = re.compile(r"\b\d{1,2}/\d{1,2}/\d{2,4}(?:\s+\d{1,2}:\d{2}\s*(?:AM|PM)?)?\b", re.IGNORECASE)
ISO_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b")
SERVICE_LABELS = ("Service", "Event", "Offering", "Appointment", "Visit", "Class")


def stable_id(prefix, *parts):
    digest = hashlib.sha256("|".join(str(p or "") for p in parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:24]}"


def text_after(label, text):
    pattern = re.compile(rf"{re.escape(label)}\s*:?\s+([^\n]+)", re.IGNORECASE)
    match = pattern.search(text)
    return match.group(1).strip() if match else None


def normalize_date_like(value):
    if not value:
        return None
    clean = re.sub(r"\s+", " ", str(value).replace(" at ", " ")).strip(" ,")
    if not clean:
        return None
    try:
        parsed = date_parser.parse(clean, fuzzy=True, default=date_parser.parse("1900-01-01"))
    except (ValueError, OverflowError, TypeError):
        return value.strip() if isinstance(value, str) else None
    if parsed.year == 1900:
        return value.strip() if isinstance(value, str) else None
    if parsed.hour or parsed.minute or parsed.second:
        return parsed.replace(second=0, microsecond=0).isoformat()
    return parsed.date().isoformat()


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
            no_show_flag, canceled_flag, unpaid_flag, waiver_flag, school, source_url,
            raw_text, raw_json, updated_at
        )
        VALUES (
            :visit_id, :person_id, :event_id, :service, :starts_at, :status,
            :no_show_flag, :canceled_flag, :unpaid_flag, :waiver_flag, :school, :source_url,
            :raw_text, :raw_json, :updated_at
        )
        ON CONFLICT(visit_id) DO UPDATE SET
            person_id = COALESCE(excluded.person_id, pike13_visits.person_id),
            event_id = COALESCE(excluded.event_id, pike13_visits.event_id),
            service = COALESCE(excluded.service, pike13_visits.service),
            starts_at = COALESCE(excluded.starts_at, pike13_visits.starts_at),
            status = COALESCE(excluded.status, pike13_visits.status),
            no_show_flag = excluded.no_show_flag,
            canceled_flag = excluded.canceled_flag,
            unpaid_flag = excluded.unpaid_flag,
            waiver_flag = excluded.waiver_flag,
            school = COALESCE(excluded.school, pike13_visits.school),
            source_url = COALESCE(excluded.source_url, pike13_visits.source_url),
            raw_text = excluded.raw_text,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )


def first_date_like(text):
    for label in ("Date & Time", "Date", "Starts", "Start", "When"):
        value = text_after(label, text)
        if value:
            return normalize_date_like(value)
    match = DATE_RE.search(text) or NUMERIC_DATE_RE.search(text) or ISO_DATE_RE.search(text)
    return normalize_date_like(match.group(0)) if match else None


def pike13_status(text):
    value = text_after("Status", text)
    lower = text.lower()
    if re.search(r"no[\s-]?show", lower):
        return "No Show"
    if "late cancel" in lower:
        return "Late Cancel"
    if "cancel" in lower:
        return "Canceled"
    if re.search(r"\bcomplete(?:d)?\b", lower):
        return "Complete"
    if "incomplete" in lower:
        return "Incomplete"
    return value


def pike13_service(text):
    for label in SERVICE_LABELS:
        value = text_after(label, text)
        if value:
            return value
    for line in (text or "").splitlines():
        clean = line.strip()
        if not clean:
            continue
        lower = clean.lower()
        if any(token in lower for token in ("trial", "lesson", "class", "band", "rookies", "rock 101")):
            return clean[:240]
    return None


def outcome_label(text):
    status = pike13_status(text)
    if status:
        return status
    lower = (text or "").lower()
    if "first visit" in lower:
        return "First Visit"
    if "converted" in lower or "membership" in lower or "plan" in lower or "pass" in lower:
        return "Enrollment Signal"
    return None


def visit_segments(text):
    text = text or ""
    matches = list(VISIT_RE.finditer(text))
    if not matches:
        return []
    segments = []
    for index, match in enumerate(matches):
        start = max(0, match.start() - 800)
        end = matches[index + 1].start() if index + 1 < len(matches) else min(len(text), match.end() + 1200)
        segments.append((match.group(1), text[start:end]))
    return segments


def visit_id_from_record(record):
    href = record.get("href") or record.get("url") or ""
    text = record.get("text") or ""
    match = VISIT_RE.search(href) or VISIT_RE.search(text)
    return match.group(1) if match else None


def event_id_from_record(record):
    href = record.get("href") or record.get("url") or ""
    text = record.get("text") or ""
    match = EVENT_RE.search(href) or EVENT_RE.search(text)
    return match.group(1) if match else None


def pike13_flags(text):
    lower = text.lower()
    return {
        "no_show_flag": 1 if re.search(r"no[\s-]?show", lower) else 0,
        "canceled_flag": 1 if "cancel" in lower else 0,
        "unpaid_flag": 1 if re.search(r"\bunpaid\b", lower) else 0,
        "waiver_flag": 1 if re.search(r"\bwaiver\b", lower) else 0,
    }


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


def parse_person_text(url, text, school):
    person_id = person_id_from_url(url)
    if not person_id:
        raise ValueError(f"Could not find Pike13 person ID in URL: {url}")
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
        "source_url": url,
        "raw_text": text,
        "raw_json": json.dumps({"extraction": "person_text"}, sort_keys=True),
        "updated_at": utc_now_iso(),
    }
    return person, text


def parse_person(page, school):
    text = page.locator("body").inner_text(timeout=30000)
    return parse_person_text(page.url, text, school)


def extract_page_links(page, base_url):
    hrefs = page.locator("a").evaluate_all(
        """links => links.map(link => link.getAttribute('href')).filter(Boolean)"""
    )
    return sorted({urljoin(base_url, href) for href in hrefs})


def extract_visit_link_records(page, base_url):
    records = page.locator("a").evaluate_all(
        """
        links => links
          .map(link => {
            const href = link.getAttribute('href') || '';
            if (!href.match(/\\/(visits|events)\\/\\d+/)) return null;
            let node = link;
            let text = link.innerText || link.textContent || '';
            for (let depth = 0; node && depth < 6; depth += 1) {
              const candidate = (node.innerText || node.textContent || '').trim();
              if (candidate.length > text.length && candidate.length <= 5000) {
                text = candidate;
              }
              const tag = (node.tagName || '').toLowerCase();
              if (['tr', 'li', 'article', 'section'].includes(tag)) break;
              node = node.parentElement;
            }
            return {href, text};
          })
          .filter(Boolean)
        """
    )
    rows = []
    seen = set()
    for record in records:
        href = urljoin(base_url, record.get("href") or "")
        key = (href, record.get("text") or "")
        if href and key not in seen:
            rows.append({"href": href, "text": record.get("text") or ""})
            seen.add(key)
    return rows


def related_urls(person_url, links):
    person_url = person_url.rstrip("/")
    urls = {f"{person_url}/visits", f"{person_url}/balances"}
    for href in links:
        if "/events/" in href or "/visits/" in href:
            urls.add(href.split("#", 1)[0])
    return sorted(urls)


def visit_row(person_id, visit_id, event_id, url, text, school, extraction):
    flags = pike13_flags(text)
    return {
        "visit_id": visit_id,
        "person_id": person_id,
        "event_id": event_id,
        "service": pike13_service(text),
        "starts_at": first_date_like(text),
        "status": outcome_label(text),
        **flags,
        "school": school,
        "source_url": url,
        "raw_text": text,
        "raw_json": json.dumps(
            {
                "extraction": extraction,
                "canceled_flag": flags["canceled_flag"],
                "source_timestamp_field": "starts_at",
                "trial_signal_visible": int("trial" in (text or "").lower()),
                "outcome_signal_visible": int(bool(outcome_label(text))),
            },
            sort_keys=True,
        ),
        "updated_at": utc_now_iso(),
    }


def capture_related_rows(person_id, url, text, school, extraction="related_text"):
    visits = []
    plans = []
    event_match = EVENT_RE.search(url) or EVENT_RE.search(text)
    segmented_visit_ids = set()
    for visit_id, segment in visit_segments(text):
        segmented_visit_ids.add(visit_id)
        visits.append(
            visit_row(
                person_id,
                visit_id,
                event_match.group(1) if event_match else None,
                url,
                segment,
                school,
                f"{extraction}:visit_segment",
            )
        )
    visit_ids = set(VISIT_RE.findall(text)) - segmented_visit_ids
    url_visit = VISIT_RE.search(url)
    if url_visit and url_visit.group(1) not in segmented_visit_ids:
        visit_ids.add(url_visit.group(1))
    if event_match and not visit_ids and not segmented_visit_ids:
        visit_ids.add(stable_id("pike13_visit", person_id, event_match.group(1), url))
    for visit_id in sorted(visit_ids):
        visits.append(visit_row(person_id, visit_id, event_match.group(1) if event_match else None, url, text, school, extraction))
    if re.search(r"\b(pass|plan|membership)\b", text, re.IGNORECASE) and not re.search(
        r"\bno\s+(active|upcoming|inactive)?\s*(plans?|passes?|memberships?)\b",
        text,
        re.IGNORECASE,
    ):
        plans.append(
            {
                "plan_pass_id": stable_id("pike13_plan_pass", person_id, text_after("Plan", text), text_after("Pass", text), url),
                "person_id": person_id,
                "name": text_after("Plan", text) or text_after("Pass", text) or text_after("Membership", text),
                "status": text_after("Status", text) or ("Active" if re.search(r"\bactive\b", text, re.IGNORECASE) else None),
                "starts_at": normalize_date_like(text_after("Start", text)) or first_date_like(text),
                "ends_at": normalize_date_like(text_after("End", text)),
                "school": school,
                "source_url": url,
                "raw_text": text,
                "raw_json": json.dumps({"extraction": extraction, "source_timestamp_field": "starts_at"}, sort_keys=True),
                "updated_at": utc_now_iso(),
            }
        )
    return visits, plans


def capture_visit_link_rows(person_id, records, school, extraction="visit_link_record"):
    visits = []
    for record in records:
        visit_id = visit_id_from_record(record)
        event_id = event_id_from_record(record)
        if not visit_id and event_id:
            visit_id = stable_id("pike13_visit", person_id, event_id, record.get("href"))
        if not visit_id:
            continue
        visits.append(
            visit_row(
                person_id,
                visit_id,
                event_id,
                record.get("href"),
                record.get("text") or record.get("href") or "",
                school,
                extraction,
            )
        )
    return visits


def person_urls_from_db(conn, base_url, limit, school=None):
    params = {"limit": limit}
    school_filter = ""
    if school:
        params["school"] = f"%{school.lower()}%"
        school_filter = "AND LOWER(COALESCE(school, '')) LIKE :school"
    rows = conn.execute(
        f"""
        SELECT DISTINCT pike13_person_id
        FROM hubspot_deals
        WHERE pike13_person_id IS NOT NULL AND pike13_person_id != ''
          {school_filter}
        ORDER BY pike13_person_id
        LIMIT :limit
        """,
        params,
    ).fetchall()
    person_ids = [row[0] for row in rows]
    if person_ids:
        return [f"{base_url.rstrip('/')}/people/{person_id}" for person_id in person_ids]

    fallback_school_filter = ""
    fallback_params = {"limit": limit}
    if school:
        fallback_params["school"] = f"%{school.lower()}%"
        fallback_school_filter = "AND LOWER(COALESCE(pp.school, '')) LIKE :school"
    fallback_rows = conn.execute(
        f"""
        SELECT DISTINCT pp.person_id
        FROM pike13_people pp
        WHERE pp.person_id IS NOT NULL
          AND pp.person_id != ''
          {fallback_school_filter}
          AND (
            EXISTS (
                SELECT 1
                FROM hubspot_contacts hc
                WHERE COALESCE(hc.phone_normalized, '') != ''
                  AND hc.phone_normalized = pp.phone_normalized
            )
            OR EXISTS (
                SELECT 1
                FROM hubspot_contacts hc
                WHERE COALESCE(hc.email_normalized, '') != ''
                  AND hc.email_normalized = pp.email_normalized
            )
          )
        ORDER BY pp.person_id
        LIMIT :limit
        """,
        fallback_params,
    ).fetchall()
    return [f"{base_url.rstrip('/')}/people/{row[0]}" for row in fallback_rows]


def wait_until_ready(page, timeout=30000):
    page.wait_for_load_state("load", timeout=timeout)
    try:
        page.wait_for_load_state("networkidle", timeout=5000)
    except PlaywrightTimeoutError:
        pass


def is_auth_redirect(url):
    lowered = (url or "").lower()
    return "/accounts/sign_in" in lowered or "/sign_in" in lowered or "/login" in lowered


def launch_browser_context(playwright, profile_dir, headless=False, chrome_channel=False):
    launch_kwargs = {
        "headless": headless,
        "viewport": {"width": 1440, "height": 1000},
    }
    if chrome_channel:
        launch_kwargs["channel"] = "chrome"
    try:
        return playwright.chromium.launch_persistent_context(str(profile_dir), **launch_kwargs)
    except Exception:
        if not chrome_channel:
            raise
        fallback_kwargs = {
            "headless": headless,
            "viewport": {"width": 1440, "height": 1000},
        }
        return playwright.chromium.launch_persistent_context(str(profile_dir), **fallback_kwargs)


def main():
    parser = argparse.ArgumentParser(description="Extract Pike13 lead outcome details into SQLite.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--profile-dir", default=DEFAULT_PROFILE_DIR)
    parser.add_argument("--base-url", default=DEFAULT_URL)
    parser.add_argument("--person-url", action="append", default=[])
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--chrome-channel", action="store_true", help="Prefer the installed Chrome channel before falling back to bundled Chromium.")
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
    urls = args.person_url or person_urls_from_db(conn, args.base_url, args.limit, args.school)
    run_id = start_import_run(
        conn,
        "pike13",
        Path(__file__).name,
        args.start_date,
        None,
        {"base_url": args.base_url, "url_count": len(urls), "chrome_channel": args.chrome_channel},
    )
    conn.commit()
    rows_seen = rows_written = auth_blocked_rows = 0
    try:
        with sync_playwright() as p:
            context = launch_browser_context(
                p,
                args.profile_dir,
                headless=args.headless,
                chrome_channel=args.chrome_channel,
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
                if is_auth_redirect(page.url):
                    auth_blocked_rows += 1
                    print(f"Skipping Pike13 URL that redirected to login: {url}")
                    continue
                person, text = parse_person(page, args.school)
                upsert_person(conn, person)
                rows_written += 1
                person_url = page.url
                queue = [person_url] + related_urls(person_url, extract_page_links(page, args.base_url))
                seen_related_urls = set()
                while queue and len(seen_related_urls) < 25:
                    related_url = queue.pop(0)
                    if related_url in seen_related_urls:
                        continue
                    seen_related_urls.add(related_url)
                    if related_url != page.url:
                        try:
                            page.goto(related_url, wait_until="domcontentloaded", timeout=60000)
                            wait_until_ready(page)
                            text = page.locator("body").inner_text(timeout=30000)
                            link_records = extract_visit_link_records(page, args.base_url)
                            for discovered_url in related_urls(person_url, extract_page_links(page, args.base_url)):
                                if discovered_url not in seen_related_urls and discovered_url not in queue:
                                    queue.append(discovered_url)
                        except Exception as exc:
                            print(f"Could not extract Pike13 related page {related_url}: {exc}")
                            continue
                    else:
                        link_records = extract_visit_link_records(page, args.base_url)
                    extraction = "person_text" if related_url == person_url else "related_page_text"
                    visits, plans = capture_related_rows(person["person_id"], related_url, text, args.school, extraction)
                    visits.extend(capture_visit_link_rows(person["person_id"], link_records, args.school))
                    for visit in visits:
                        upsert_visit(conn, visit)
                        rows_written += 1
                    for plan in plans:
                        upsert_plan_pass(conn, plan)
                        rows_written += 1
            context.close()
        status = "success" if rows_written else "blocked"
        error = None if rows_written else "Pike13 authentication blocked all requested person URLs."
        finish_import_run(
            conn,
            run_id,
            status,
            rows_seen,
            rows_written,
            0,
            error,
            metadata={"auth_blocked_rows": auth_blocked_rows},
        )
        conn.commit()
    except KeyboardInterrupt:
        finish_import_run(conn, run_id, "error", rows_seen, rows_written, 0, "interrupted before completion")
        conn.commit()
        raise
    except Exception as exc:
        finish_import_run(conn, run_id, "error", rows_seen, rows_written, 0, str(exc))
        conn.commit()
        raise
    finally:
        conn.close()

    print(f"Pike13 extraction complete: rows_seen={rows_seen} rows_written={rows_written}")


if __name__ == "__main__":
    main()
