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
EVENT_RE = re.compile(r"(?:/events/|/e/)(\d+)")
DATE_RE = re.compile(
    r"\b(?:Mon|Tue|Tues|Wed|Thu|Thurs|Fri|Sat|Sun)?(?:day)?[,]?\s*"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}"
    r"(?:\s+(?:at\s+)?\d{1,2}:\d{2}\s*(?:AM|PM)?)?",
    re.IGNORECASE,
)
NUMERIC_DATE_RE = re.compile(r"\b\d{1,2}/\d{1,2}/\d{2,4}(?:\s+\d{1,2}:\d{2}\s*(?:AM|PM)?)?\b", re.IGNORECASE)
ISO_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b")
SERVICE_LABELS = ("Service", "Event", "Offering", "Appointment", "Visit", "Class")
FIRST_VISITS_FIELDS = [
    "full_name",
    "email",
    "phone",
    "service_name",
    "instructor_names",
    "service_date",
    "service_day",
    "service_time",
    "first_visit",
    "consider_member",
    "is_paid",
    "paid_with",
    "state",
    "is_waitlist",
    "available_plans",
    "client_booked",
    "is_rollover",
    "account_manager_names",
    "account_manager_emails",
    "account_manager_phones",
    "service_location_name",
    "home_location_name",
    "service_category",
    "service_state",
    "service_type",
    "event_name",
    "paid_with_complimentary_pass",
    "paid_with_type",
    "registered_at",
    "visit_id",
    "waitlist_id",
    "person_id",
    "event_occurrence_id",
    "estimated_amount",
    "cancelled_to_start",
    "make_up_issued",
    "bulk_enrolled",
    "plan_id",
    "currency_code",
]
PIKE13_STATE_LABELS = {
    "noshowed": "No Show",
    "completed": "Complete",
    "late_canceled": "Late Cancel",
    "registered": "Enrolled",
    "reserved": "Reserved",
    "expired": "Waitlist Expired",
    "removed": "Canceled Waitlist",
    "waiting": "Currently on Waitlist",
}


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
    if not (DATE_RE.search(clean) or NUMERIC_DATE_RE.search(clean) or ISO_DATE_RE.search(clean)):
        return None
    try:
        parsed = date_parser.parse(clean, fuzzy=True, default=date_parser.parse("1900-01-01"))
    except (ValueError, OverflowError, TypeError):
        return None
    if parsed.year == 1900:
        return None
    if parsed.hour or parsed.minute or parsed.second:
        return parsed.replace(second=0, microsecond=0).isoformat()
    return parsed.date().isoformat()


def boolish(value):
    if value is None:
        return 0
    return 1 if str(value).strip().lower() in {"1", "t", "true", "yes", "y", "complete", "completed"} else 0


def combine_date_time(date_value, time_value):
    date_part = normalize_date_like(date_value)
    if not date_part:
        return None
    if "T" in date_part or not time_value:
        return date_part
    parsed_time = normalize_date_like(f"{date_part} {time_value}")
    return parsed_time or date_part


def pike13_state_label(value):
    if not value:
        return None
    return PIKE13_STATE_LABELS.get(str(value).strip().lower(), str(value).strip().replace("_", " ").title())


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
    row = {
        "instructor": None,
        "first_visit_flag": 0,
        "attendance_confirmed_flag": 0,
        "checked_in_flag": 0,
        "enrolled_flag": 0,
        "terms_accepted_flag": None,
        **row,
    }
    conn.execute(
        """
        INSERT INTO pike13_visits (
            visit_id, person_id, event_id, service, starts_at, status,
            no_show_flag, canceled_flag, unpaid_flag, waiver_flag, instructor,
            first_visit_flag, attendance_confirmed_flag, checked_in_flag,
            enrolled_flag, terms_accepted_flag, school, source_url,
            raw_text, raw_json, updated_at
        )
        VALUES (
            :visit_id, :person_id, :event_id, :service, :starts_at, :status,
            :no_show_flag, :canceled_flag, :unpaid_flag, :waiver_flag, :instructor,
            :first_visit_flag, :attendance_confirmed_flag, :checked_in_flag,
            :enrolled_flag, :terms_accepted_flag, :school, :source_url,
            :raw_text, :raw_json, :updated_at
        )
        ON CONFLICT(visit_id) DO UPDATE SET
            person_id = COALESCE(excluded.person_id, pike13_visits.person_id),
            event_id = COALESCE(excluded.event_id, pike13_visits.event_id),
            service = COALESCE(excluded.service, pike13_visits.service),
            starts_at = CASE
                WHEN excluded.starts_at IS NOT NULL THEN excluded.starts_at
                WHEN date(pike13_visits.starts_at) IS NULL THEN NULL
                ELSE pike13_visits.starts_at
            END,
            status = COALESCE(excluded.status, pike13_visits.status),
            no_show_flag = excluded.no_show_flag,
            canceled_flag = excluded.canceled_flag,
            unpaid_flag = excluded.unpaid_flag,
            waiver_flag = excluded.waiver_flag,
            instructor = COALESCE(excluded.instructor, pike13_visits.instructor),
            first_visit_flag = MAX(COALESCE(excluded.first_visit_flag, 0), COALESCE(pike13_visits.first_visit_flag, 0)),
            attendance_confirmed_flag = MAX(COALESCE(excluded.attendance_confirmed_flag, 0), COALESCE(pike13_visits.attendance_confirmed_flag, 0)),
            checked_in_flag = MAX(COALESCE(excluded.checked_in_flag, 0), COALESCE(pike13_visits.checked_in_flag, 0)),
            enrolled_flag = MAX(COALESCE(excluded.enrolled_flag, 0), COALESCE(pike13_visits.enrolled_flag, 0)),
            terms_accepted_flag = COALESCE(excluded.terms_accepted_flag, pike13_visits.terms_accepted_flag),
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


def event_enrichment_flags(text):
    lower = (text or "").lower()
    return {
        "attendance_confirmed_flag": 1 if "attendance confirmed" in lower else 0,
        "checked_in_flag": 1 if "checked in" in lower else 0,
        "enrolled_flag": 1 if re.search(r"\benrolled\b", lower) else 0,
        "terms_accepted_flag": 0 if "terms and conditions have not been accepted" in lower else None,
    }


def upsert_plan_pass(conn, row):
    row = {"payer_name": None, "next_invoice_at": None, "terms_accepted_flag": None, **row}
    conn.execute(
        """
        INSERT INTO pike13_plans_passes (
            plan_pass_id, person_id, name, status, starts_at, ends_at, school,
            source_url, payer_name, next_invoice_at, terms_accepted_flag,
            raw_text, raw_json, updated_at
        )
        VALUES (
            :plan_pass_id, :person_id, :name, :status, :starts_at, :ends_at, :school,
            :source_url, :payer_name, :next_invoice_at, :terms_accepted_flag,
            :raw_text, :raw_json, :updated_at
        )
        ON CONFLICT(plan_pass_id) DO UPDATE SET
            name = COALESCE(excluded.name, pike13_plans_passes.name),
            status = COALESCE(excluded.status, pike13_plans_passes.status),
            starts_at = COALESCE(excluded.starts_at, pike13_plans_passes.starts_at),
            ends_at = COALESCE(excluded.ends_at, pike13_plans_passes.ends_at),
            source_url = COALESCE(excluded.source_url, pike13_plans_passes.source_url),
            payer_name = COALESCE(excluded.payer_name, pike13_plans_passes.payer_name),
            next_invoice_at = COALESCE(excluded.next_invoice_at, pike13_plans_passes.next_invoice_at),
            terms_accepted_flag = COALESCE(excluded.terms_accepted_flag, pike13_plans_passes.terms_accepted_flag),
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
            if (!href.match(/\\/(visits|events)\\/\\d+/) && !href.match(/\\/e\\/\\d+/)) return null;
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
        if "/events/" in href or "/visits/" in href or re.search(r"/e/\d+", href):
            urls.add(href.split("#", 1)[0])
    return sorted(urls)


def visit_row(person_id, visit_id, event_id, url, text, school, extraction):
    flags = pike13_flags(text)
    enrichment = event_enrichment_flags(text)
    return {
        "visit_id": visit_id,
        "person_id": person_id,
        "event_id": event_id,
        "service": pike13_service(text),
        "starts_at": first_date_like(text),
        "status": outcome_label(text),
        **flags,
        **enrichment,
        "instructor": text_after("Instructed by", text),
        "first_visit_flag": 1 if "first visit" in (text or "").lower() else 0,
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


def row_dict(fields, row):
    return {field: row[index] if index < len(row) else None for index, field in enumerate(fields)}


def first_visit_report_url(base_url):
    return f"{base_url.rstrip('/')}/desk/reports#/enrollments/details"


def first_visits_filter(start_date, end_date):
    return [
        "and",
        [
            ["btw", "service_date", [start_date, end_date]],
            ["eq", "first_visit", ["t"]],
        ],
    ]


def find_enrollments_query_url(page, base_url, report_url=None):
    query_urls = []
    page.on(
        "request",
        lambda req: query_urls.append(req.url)
        if "/desk/api/v3/reports/enrollments/queries" in req.url
        else None,
    )
    page.goto(report_url or first_visit_report_url(base_url), wait_until="domcontentloaded", timeout=60000)
    wait_until_ready(page)
    page.wait_for_timeout(5000)
    if is_auth_redirect(page.url):
        return None
    if query_urls:
        return query_urls[-1]
    raise RuntimeError("Could not discover Pike13 Enrollments report query URL.")


def fetch_first_visits_report_rows(page, query_url, start_date, end_date, limit=500):
    payload = {
        "data": {
            "type": "queries",
            "attributes": {
                "page": {"limit": limit},
                "fields": FIRST_VISITS_FIELDS,
                "total_count": "t",
                "filter": first_visits_filter(start_date, end_date),
                "sort": ["service_date+"],
            },
        }
    }
    response = page.request.post(
        query_url,
        data=json.dumps(payload),
        headers={
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json",
        },
        timeout=60000,
    )
    if response.status >= 400:
        raise RuntimeError(f"Pike13 First Visits report query failed with HTTP {response.status}: {response.text()[:500]}")
    data = response.json()
    rows = data.get("data", {}).get("attributes", {}).get("rows") or []
    return [row_dict(FIRST_VISITS_FIELDS, row) for row in rows]


def report_source_text(row):
    labels = [
        ("Client", row.get("full_name")),
        ("Email", row.get("email")),
        ("Phone", row.get("phone")),
        ("Service", row.get("service_name")),
        ("Staff", row.get("instructor_names")),
        ("Service Date", row.get("service_date")),
        ("Service Time", row.get("service_time")),
        ("First Visit", row.get("first_visit")),
        ("Status", row.get("state")),
        ("Paid With", row.get("paid_with")),
        ("Registered At", row.get("registered_at")),
    ]
    return "\n".join(f"{label}: {value}" for label, value in labels if value not in (None, ""))


def report_person_row(row, school, base_url):
    person_id = str(row.get("person_id") or "")
    if not person_id:
        return None
    email = row.get("email")
    phone = row.get("phone")
    return {
        "person_id": person_id,
        "full_name": row.get("full_name"),
        "first_name": None,
        "last_name": None,
        "email": email,
        "email_normalized": normalize_email(email),
        "phone": phone,
        "phone_normalized": normalize_phone(phone),
        "membership_state": row.get("paid_with") or row.get("available_plans"),
        "school": school,
        "source_url": f"{base_url.rstrip('/')}/people/{person_id}",
        "raw_text": report_source_text(row),
        "raw_json": json.dumps({"extraction": "first_visits_report", "row": row}, sort_keys=True),
        "updated_at": utc_now_iso(),
    }


def report_visit_id(row, school):
    event_id = row.get("event_occurrence_id")
    person_id = row.get("person_id")
    if event_id:
        return stable_id("pike13_event_visit", school, person_id, event_id)
    if row.get("visit_id"):
        return str(row.get("visit_id"))
    return stable_id(
        "pike13_visit",
        school,
        person_id,
        row.get("service_name"),
        row.get("service_date"),
        row.get("service_time"),
        row.get("instructor_names"),
    )


def report_visit_row(row, school, base_url):
    state = pike13_state_label(row.get("state"))
    starts_at = combine_date_time(row.get("service_date"), row.get("service_time"))
    event_id = str(row.get("event_occurrence_id") or "") or None
    source_url = f"{base_url.rstrip('/')}/e/{event_id}" if event_id else first_visit_report_url(base_url)
    flags = pike13_flags(" ".join(str(value or "") for value in (state, row.get("paid_with"), row.get("available_plans"))))
    enrolled = boolish(row.get("first_visit")) if state == "Enrolled" else 0
    raw_json = {
        "extraction": "first_visits_report",
        "row": row,
        "report_authoritative_fields": ["starts_at", "service", "instructor", "first_visit_flag", "status"],
        "source_timestamp_field": "service_date",
    }
    return {
        "visit_id": report_visit_id(row, school),
        "person_id": str(row.get("person_id") or "") or None,
        "event_id": event_id,
        "service": row.get("service_name"),
        "starts_at": starts_at,
        "status": state,
        "no_show_flag": flags["no_show_flag"],
        "canceled_flag": flags["canceled_flag"],
        "unpaid_flag": 1 if str(row.get("paid_with_type") or "").lower() == "unpaid" else flags["unpaid_flag"],
        "waiver_flag": 0,
        "instructor": row.get("instructor_names"),
        "first_visit_flag": boolish(row.get("first_visit")),
        "attendance_confirmed_flag": 1 if state == "Complete" else 0,
        "checked_in_flag": 1 if state == "Complete" else 0,
        "enrolled_flag": 1 if enrolled or state in {"Enrolled", "Reserved", "Complete"} else 0,
        "terms_accepted_flag": None,
        "school": school,
        "source_url": source_url,
        "raw_text": report_source_text(row),
        "raw_json": json.dumps(raw_json, sort_keys=True),
        "updated_at": utc_now_iso(),
    }


def report_plan_row(row, school, base_url):
    plan_id = row.get("plan_id")
    paid_with = row.get("paid_with")
    if not plan_id and not paid_with:
        return None
    person_id = str(row.get("person_id") or "") or None
    plan_pass_id = str(plan_id) if plan_id else stable_id("pike13_plan_pass", person_id, paid_with, row.get("service_date"))
    event_id = row.get("event_occurrence_id")
    return {
        "plan_pass_id": plan_pass_id,
        "person_id": person_id,
        "name": paid_with or row.get("available_plans"),
        "status": "Active" if row.get("paid_with") else None,
        "starts_at": normalize_date_like(row.get("registered_at")) or normalize_date_like(row.get("service_date")),
        "ends_at": None,
        "school": school,
        "source_url": f"{base_url.rstrip('/')}/e/{event_id}" if event_id else first_visit_report_url(base_url),
        "payer_name": row.get("account_manager_names"),
        "next_invoice_at": None,
        "terms_accepted_flag": None,
        "raw_text": report_source_text(row),
        "raw_json": json.dumps({"extraction": "first_visits_report", "row": row}, sort_keys=True),
        "updated_at": utc_now_iso(),
    }


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


def enrich_report_visit_from_event(page, visit, timeout=60000):
    if not visit.get("source_url") or "/e/" not in visit["source_url"]:
        return visit
    page.goto(visit["source_url"], wait_until="domcontentloaded", timeout=timeout)
    wait_until_ready(page)
    text = page.locator("body").inner_text(timeout=30000)
    event_flags = event_enrichment_flags(text)
    pike_flags = pike13_flags(text)
    status = outcome_label(text)
    enriched = {
        **visit,
        "status": status or visit.get("status"),
        "no_show_flag": max(visit.get("no_show_flag") or 0, pike_flags["no_show_flag"]),
        "canceled_flag": max(visit.get("canceled_flag") or 0, pike_flags["canceled_flag"]),
        "unpaid_flag": max(visit.get("unpaid_flag") or 0, pike_flags["unpaid_flag"]),
        "waiver_flag": max(visit.get("waiver_flag") or 0, pike_flags["waiver_flag"]),
        "attendance_confirmed_flag": max(visit.get("attendance_confirmed_flag") or 0, event_flags["attendance_confirmed_flag"]),
        "checked_in_flag": max(visit.get("checked_in_flag") or 0, event_flags["checked_in_flag"]),
        "enrolled_flag": max(visit.get("enrolled_flag") or 0, event_flags["enrolled_flag"]),
        "terms_accepted_flag": event_flags["terms_accepted_flag"]
        if event_flags["terms_accepted_flag"] is not None
        else visit.get("terms_accepted_flag"),
        "raw_text": text,
        "raw_json": json.dumps(
            {
                "extraction": "first_visits_report:event_enriched",
                "report_row_json": visit.get("raw_json"),
                "event_enrichment_flags": event_flags,
                "source_timestamp_field": "service_date",
            },
            sort_keys=True,
        ),
        "updated_at": utc_now_iso(),
    }
    return enriched


def extract_first_visits_report(conn, page, args):
    start_date = args.first_visits_start_date
    end_date = args.first_visits_end_date
    if not start_date or not end_date:
        raise ValueError("--first-visits-start-date and --first-visits-end-date are required for first-visits mode.")
    query_url = find_enrollments_query_url(page, args.base_url, args.first_visits_report_url)
    if not query_url:
        raise RuntimeError("Pike13 authentication blocked the First Visits report.")
    rows = fetch_first_visits_report_rows(page, query_url, start_date, end_date, args.first_visits_limit)
    rows_written = 0
    for row in rows:
        person = report_person_row(row, args.school, args.base_url)
        if person:
            upsert_person(conn, person)
            rows_written += 1
        visit = report_visit_row(row, args.school, args.base_url)
        if not args.skip_first_visits_enrichment:
            try:
                visit = enrich_report_visit_from_event(page, visit)
            except Exception as exc:
                payload = json.loads(visit.get("raw_json") or "{}")
                payload["event_enrichment_error"] = str(exc)[:240]
                visit["raw_json"] = json.dumps(payload, sort_keys=True)
        upsert_visit(conn, visit)
        rows_written += 1
        plan = report_plan_row(row, args.school, args.base_url)
        if plan:
            upsert_plan_pass(conn, plan)
            rows_written += 1
    return len(rows), rows_written


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
    parser.add_argument(
        "--first-visits-report-url",
        help="Use Pike13 Enrollments Details / First Visits report mode. If omitted, the URL is derived from --base-url.",
    )
    parser.add_argument("--first-visits-start-date")
    parser.add_argument("--first-visits-end-date")
    parser.add_argument("--first-visits-limit", type=int, default=500)
    parser.add_argument("--skip-first-visits-enrichment", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    ensure_lead_followup_schema(conn)
    first_visits_mode = bool(args.first_visits_report_url or args.first_visits_start_date or args.first_visits_end_date)
    urls = [] if first_visits_mode else args.person_url or person_urls_from_db(conn, args.base_url, args.limit, args.school)
    run_id = start_import_run(
        conn,
        "pike13",
        Path(__file__).name,
        args.start_date,
        None,
        {
            "base_url": args.base_url,
            "url_count": len(urls),
            "chrome_channel": args.chrome_channel,
            "mode": "first_visits_report" if first_visits_mode else "person_urls",
            "first_visits_report_url": args.first_visits_report_url or first_visit_report_url(args.base_url) if first_visits_mode else None,
            "first_visits_start_date": args.first_visits_start_date,
            "first_visits_end_date": args.first_visits_end_date,
        },
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
            if first_visits_mode:
                rows_seen, rows_written = extract_first_visits_report(conn, page, args)
                auth_blocked_rows = 0
                context.close()
                status = "success" if rows_written else "blocked"
                finish_import_run(
                    conn,
                    run_id,
                    status,
                    rows_seen,
                    rows_written,
                    0,
                    None if rows_written else "Pike13 First Visits report returned no rows.",
                    metadata={"auth_blocked_rows": auth_blocked_rows, "mode": "first_visits_report"},
                )
                conn.commit()
                print(f"Pike13 extraction complete: rows_seen={rows_seen} rows_written={rows_written}")
                return
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
