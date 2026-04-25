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


DEFAULT_URL = "https://app.hubspot.com/contacts"
DEAL_RE = re.compile(r"/record/0-3/(\d+)")
CONTACT_RE = re.compile(r"/record/0-1/(\d+)")
PIKE13_PERSON_RE = re.compile(r"/people/(\d+)")
NO_VALUE_MARKERS = {"", "--", "details", "actions"}
FIELD_LABELS = {
    "actions",
    "area of interest",
    "close date",
    "create date",
    "date entered scheduled trial stage",
    "deal activity",
    "deal name",
    "deal owner",
    "deal stage",
    "follow up needed",
    "instrument type",
    "last activity date",
    "last contacted",
    "lead source",
    "lead source - deal",
    "marketing source",
    "marketing source - deal",
    "pipeline",
    "school",
    "school name - deal",
    "stage",
    "student age",
    "trial date",
    "trial date (deal)",
    "trial date - display deal",
    "trial no show",
    "trial time - display deal",
}
SCHOOL_FROM_OWNER = {
    "westu": "West University Place",
    "west u": "West University Place",
    "theheights": "The Heights",
    "the heights": "The Heights",
}


def stable_id(prefix, *parts):
    digest = hashlib.sha256("|".join(str(p or "") for p in parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:24]}"


def text_after(label, text):
    lines = visible_lines(text)
    value = value_after_label(lines, label)
    if value:
        return value
    pattern = re.compile(rf"(?:^|\n)\s*{re.escape(label)}:?\s+([^\n]+)", re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        return None
    value = clean_value(match.group(1))
    return value if value and value.lower() not in NO_VALUE_MARKERS else None


def clean_value(value):
    if value is None:
        return None
    value = str(value).replace("\xa0", " ").strip()
    value = re.sub(r"\s+", " ", value)
    return value or None


def visible_lines(text):
    return [clean_value(line) for line in text.splitlines() if clean_value(line)]


def value_after_label(lines, label):
    label_l = label.lower()
    for index, line in enumerate(lines):
        if line.lower() != label_l:
            continue
        for candidate in lines[index + 1 : index + 5]:
            candidate_l = candidate.lower()
            if candidate_l in NO_VALUE_MARKERS:
                continue
            if candidate_l.endswith(":"):
                continue
            if candidate_l in FIELD_LABELS:
                break
            return candidate
    return None


def first_valid(values):
    for value in values:
        value = clean_value(value)
        if value and value.lower() not in NO_VALUE_MARKERS:
            return value
    return None


def looks_like_date(value):
    value = clean_value(value)
    if not value:
        return False
    return bool(
        re.search(r"\d{4}-\d{2}-\d{2}", value)
        or re.search(r"\d{1,2}/\d{1,2}/\d{4}", value)
        or re.search(r"[A-Z][a-z]{2,8}\s+\d{1,2},\s+\d{4}", value)
    )


def normalize_stage(value):
    value = clean_value(value)
    if not value:
        return None
    return re.sub(r"\s*\(Lead Pipeline\)\s*$", "", value).strip()


def school_from_deal_name(deal_name):
    if not deal_name or "|" not in deal_name:
        return None
    return clean_value(deal_name.split("|", 1)[1])


def school_from_owner(owner):
    owner_l = (owner or "").lower()
    for marker, school in SCHOOL_FROM_OWNER.items():
        if marker in owner_l:
            return school
    return None


def date_from_created_activity(lines):
    for index, line in enumerate(lines):
        if line.lower() == "created" and index + 1 < len(lines):
            candidate = lines[index + 1]
            if looks_like_date(candidate):
                return candidate
        if looks_like_date(line) and index + 1 < len(lines) and lines[index + 1].lower() == "created":
            return line
    return None


def date_from_activity_header(lines):
    for line in lines:
        if looks_like_date(line) and " at " in line:
            return line
    return None


def build_deal_raw_json(extraction, row, required, **extra):
    found = [field for field in required if row.get(field)]
    missing = [field for field in required if not row.get(field)]
    payload = {
        "extraction": extraction,
        "fields_found": found,
        "fields_missing": missing,
    }
    payload.update(extra)
    return json.dumps(payload, sort_keys=True)


def parse_deal_text(deal_id, url, text):
    lines = visible_lines(text)
    pike13_match = PIKE13_PERSON_RE.search(text)
    deal_name = text_after("Deal name", text) or text_after("Name", text)
    if not deal_name:
        deal_name = next((line for line in lines if " | " in line), None)
    stage = normalize_stage(text_after("Deal Stage", text) or text_after("Stage", text))
    create_date = first_valid(
        [
            text_after("Create Date", text),
            text_after("Create date", text),
            date_from_created_activity(lines),
        ]
    )
    if create_date and not looks_like_date(create_date):
        create_date = None
    if not create_date:
        create_date = date_from_created_activity(lines)
    last_activity_date = text_after("Last Activity Date", text)
    if last_activity_date and not looks_like_date(last_activity_date):
        last_activity_date = None
    if not last_activity_date:
        last_activity_date = date_from_activity_header(lines)
    school = text_after("School Name - Deal", text)
    if not school:
        school = school_from_deal_name(deal_name)
    if not school:
        school = text_after("School", text)
    row = {
        "deal_id": deal_id,
        "deal_name": deal_name,
        "stage": stage,
        "pipeline": text_after("Pipeline", text),
        "owner": text_after("Deal owner", text) or text_after("Owner", text),
        "school": school,
        "create_date": create_date,
        "last_activity_date": last_activity_date,
        "last_contacted": first_valid([text_after("Last Contacted", text)]),
        "follow_up_needed": text_after("Follow Up Needed", text),
        "trial_date": first_valid(
            [
                text_after("Trial Date", text),
                text_after("Trial Date (Deal)", text),
                text_after("Trial Date - Display Deal", text),
            ]
        ),
        "trial_no_show": text_after("Trial No Show", text),
        "date_entered_scheduled_trial_stage": text_after("Date Entered Scheduled Trial Stage", text),
        "area_of_interest": text_after("Area of Interest", text),
        "instrument_type": text_after("Instrument Type", text),
        "lead_source": text_after("Lead Source - Deal", text) or text_after("Lead Source", text),
        "marketing_source": text_after("Marketing Source - Deal", text) or text_after("Marketing Source", text),
        "pike13_person_id": pike13_match.group(1) if pike13_match else None,
        "source_url": url,
        "raw_text": text,
        "raw_json": None,
        "updated_at": utc_now_iso(),
    }
    required = [
        "deal_name",
        "stage",
        "owner",
        "school",
        "create_date",
        "last_activity_date",
        "last_contacted",
        "follow_up_needed",
        "trial_date",
        "pike13_person_id",
    ]
    row["raw_json"] = build_deal_raw_json(
        "deal_detail_text",
        row,
        required,
        source_url=url,
    )
    return row


def parse_hubspot_table_rows(text):
    lines = visible_lines(text)
    rows = []
    for index, line in enumerate(lines):
        if " | " not in line:
            continue
        if index + 1 >= len(lines) or "Lead Pipeline" not in lines[index + 1]:
            continue
        stage = normalize_stage(lines[index + 1])
        close_date = None if index + 2 >= len(lines) or lines[index + 2] == "--" else lines[index + 2]
        owner_parts = []
        for candidate in lines[index + 3 : index + 5]:
            if candidate != "--":
                owner_parts.append(candidate)
        owner = clean_value(" ".join(owner_parts))
        deal_name = line
        school = school_from_deal_name(deal_name) or school_from_owner(owner)
        rows.append(
            {
                "deal_name": deal_name,
                "stage": stage,
                "pipeline": "Lead Pipeline",
                "owner": owner,
                "school": school,
                "close_date": close_date,
                "raw_text": "\n".join(lines[index : index + 6]),
            }
        )
    return rows


def parse_hubspot_board_cards(text):
    lines = visible_lines(text)
    rows = []
    known_stages = {
        "New Leads",
        "Contacted",
        "Waiting On Us",
        "Scheduled Trial/Tour",
        "Trial/Tour Completed & Unconverted",
        "Immersion Pass",
        "Campers",
        "Enrolled",
        "Closed Lost",
        "Not a Lead",
    }
    current_stage = None
    index = 0
    while index < len(lines):
        line = lines[index]
        if line in known_stages:
            current_stage = line
            index += 1
            continue
        if " | " not in line:
            index += 1
            continue
        chunk = lines[index : index + 14]
        row = {
            "deal_name": line,
            "stage": current_stage,
            "pipeline": "Lead Pipeline",
            "owner": None,
            "school": school_from_deal_name(line),
            "create_date": None,
            "last_contacted": None,
            "follow_up_needed": None,
            "trial_date": None,
            "raw_text": "\n".join(chunk),
        }
        for offset, item in enumerate(chunk):
            if item.startswith("Create date:"):
                row["create_date"] = clean_value(item.split(":", 1)[1])
            elif item.startswith("Last contacted:"):
                row["last_contacted"] = clean_value(item.split(":", 1)[1])
            elif item.startswith("Trial Date (Deal):"):
                row["trial_date"] = clean_value(item.split(":", 1)[1])
            elif item.startswith("Follow Up Needed:"):
                row["follow_up_needed"] = chunk[offset + 1] if offset + 1 < len(chunk) else None
        rows.append(row)
        index += 1
    return rows


def row_to_deal(deal_id, url, parsed_row, extraction):
    required = ["deal_name", "stage", "school", "create_date", "source_url", "raw_text"]
    row = {
        "deal_id": deal_id,
        "deal_name": parsed_row.get("deal_name"),
        "stage": parsed_row.get("stage"),
        "pipeline": parsed_row.get("pipeline"),
        "owner": parsed_row.get("owner"),
        "school": parsed_row.get("school"),
        "create_date": parsed_row.get("create_date"),
        "last_activity_date": parsed_row.get("last_activity_date"),
        "last_contacted": parsed_row.get("last_contacted"),
        "follow_up_needed": parsed_row.get("follow_up_needed"),
        "trial_date": parsed_row.get("trial_date"),
        "trial_no_show": parsed_row.get("trial_no_show"),
        "date_entered_scheduled_trial_stage": parsed_row.get("date_entered_scheduled_trial_stage"),
        "area_of_interest": parsed_row.get("area_of_interest"),
        "instrument_type": parsed_row.get("instrument_type"),
        "lead_source": parsed_row.get("lead_source"),
        "marketing_source": parsed_row.get("marketing_source"),
        "pike13_person_id": parsed_row.get("pike13_person_id"),
        "source_url": url,
        "raw_text": parsed_row.get("raw_text") or parsed_row.get("deal_name") or "",
        "raw_json": None,
        "updated_at": utc_now_iso(),
    }
    row["raw_json"] = build_deal_raw_json(
        extraction,
        row,
        required,
        source_url=url,
    )
    return row


def merge_deal_rows(spine_row, detail_row):
    """Keep list/board spine fields while adding richer detail-page fields."""
    if not spine_row:
        return detail_row
    if not detail_row:
        return spine_row

    merged = dict(detail_row)
    for field in ("deal_name", "stage", "pipeline", "owner", "school", "create_date"):
        spine_value = clean_value(spine_row.get(field))
        if spine_value and spine_value.lower() not in NO_VALUE_MARKERS:
            merged[field] = spine_value

    spine_text = spine_row.get("raw_text") or ""
    detail_text = detail_row.get("raw_text") or ""
    if spine_text and detail_text and spine_text not in detail_text:
        merged["raw_text"] = f"{spine_text}\n\n--- HubSpot detail page ---\n\n{detail_text}"

    try:
        spine_meta = json.loads(spine_row.get("raw_json") or "{}")
        detail_meta = json.loads(detail_row.get("raw_json") or "{}")
    except json.JSONDecodeError:
        spine_meta = {"raw_json": spine_row.get("raw_json")}
        detail_meta = {"raw_json": detail_row.get("raw_json")}
    merged["raw_json"] = json.dumps(
        {
            "extraction": "deal_spine_plus_detail",
            "spine": spine_meta,
            "detail": detail_meta,
        },
        sort_keys=True,
    )
    return merged


def upsert_deal(conn, row):
    conn.execute(
        """
        INSERT INTO hubspot_deals (
            deal_id, deal_name, stage, pipeline, owner, school, create_date,
            last_activity_date, last_contacted, follow_up_needed, trial_date,
            trial_no_show, date_entered_scheduled_trial_stage, area_of_interest,
            instrument_type, lead_source, marketing_source, pike13_person_id,
            source_url, raw_text, raw_json, updated_at
        )
        VALUES (
            :deal_id, :deal_name, :stage, :pipeline, :owner, :school, :create_date,
            :last_activity_date, :last_contacted, :follow_up_needed, :trial_date,
            :trial_no_show, :date_entered_scheduled_trial_stage, :area_of_interest,
            :instrument_type, :lead_source, :marketing_source, :pike13_person_id,
            :source_url, :raw_text, :raw_json, :updated_at
        )
        ON CONFLICT(deal_id) DO UPDATE SET
            deal_name = COALESCE(excluded.deal_name, hubspot_deals.deal_name),
            stage = COALESCE(excluded.stage, hubspot_deals.stage),
            pipeline = COALESCE(excluded.pipeline, hubspot_deals.pipeline),
            owner = COALESCE(excluded.owner, hubspot_deals.owner),
            school = COALESCE(excluded.school, hubspot_deals.school),
            create_date = COALESCE(excluded.create_date, hubspot_deals.create_date),
            last_activity_date = COALESCE(excluded.last_activity_date, hubspot_deals.last_activity_date),
            last_contacted = COALESCE(excluded.last_contacted, hubspot_deals.last_contacted),
            follow_up_needed = COALESCE(excluded.follow_up_needed, hubspot_deals.follow_up_needed),
            trial_date = COALESCE(excluded.trial_date, hubspot_deals.trial_date),
            trial_no_show = COALESCE(excluded.trial_no_show, hubspot_deals.trial_no_show),
            date_entered_scheduled_trial_stage = COALESCE(excluded.date_entered_scheduled_trial_stage, hubspot_deals.date_entered_scheduled_trial_stage),
            area_of_interest = COALESCE(excluded.area_of_interest, hubspot_deals.area_of_interest),
            instrument_type = COALESCE(excluded.instrument_type, hubspot_deals.instrument_type),
            lead_source = COALESCE(excluded.lead_source, hubspot_deals.lead_source),
            marketing_source = COALESCE(excluded.marketing_source, hubspot_deals.marketing_source),
            pike13_person_id = COALESCE(excluded.pike13_person_id, hubspot_deals.pike13_person_id),
            source_url = COALESCE(excluded.source_url, hubspot_deals.source_url),
            raw_text = excluded.raw_text,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )


def upsert_contact_from_text(conn, deal_id, url, text):
    contact_match = CONTACT_RE.search(url + "\n" + text)
    email_match = re.search(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", text)
    phone_match = re.search(r"(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}", text)
    if not contact_match and not email_match and not phone_match:
        return 0
    contact_id = contact_match.group(1) if contact_match else stable_id("hubspot_contact", email_match.group(0) if email_match else phone_match.group(0))
    row = {
        "contact_id": contact_id,
        "full_name": text_after("Contact", text) or text_after("Name", text),
        "email": email_match.group(0) if email_match else None,
        "email_normalized": normalize_email(email_match.group(0)) if email_match else None,
        "phone": phone_match.group(0) if phone_match else None,
        "phone_normalized": normalize_phone(phone_match.group(0)) if phone_match else None,
        "sms_opt_in": text_after("SMS Opt In", text) or text_after("SMS opt-in", text),
        "owner": text_after("Contact owner", text),
        "school": text_after("School Lead Status", text) or text_after("School", text),
        "school_lead_status": text_after("School Lead Status", text),
        "associated_deal_ids": deal_id,
        "source_url": url,
        "raw_text": text,
        "raw_json": json.dumps({"extraction": "deal_contact_text"}, sort_keys=True),
        "updated_at": utc_now_iso(),
    }
    conn.execute(
        """
        INSERT INTO hubspot_contacts (
            contact_id, full_name, email, email_normalized, phone, phone_normalized,
            sms_opt_in, owner, school, school_lead_status, associated_deal_ids,
            source_url, raw_text, raw_json, updated_at
        )
        VALUES (
            :contact_id, :full_name, :email, :email_normalized, :phone, :phone_normalized,
            :sms_opt_in, :owner, :school, :school_lead_status, :associated_deal_ids,
            :source_url, :raw_text, :raw_json, :updated_at
        )
        ON CONFLICT(contact_id) DO UPDATE SET
            full_name = COALESCE(excluded.full_name, hubspot_contacts.full_name),
            email = COALESCE(excluded.email, hubspot_contacts.email),
            email_normalized = COALESCE(excluded.email_normalized, hubspot_contacts.email_normalized),
            phone = COALESCE(excluded.phone, hubspot_contacts.phone),
            phone_normalized = COALESCE(excluded.phone_normalized, hubspot_contacts.phone_normalized),
            associated_deal_ids = excluded.associated_deal_ids,
            raw_text = excluded.raw_text,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )
    return 1


def capture_visible_deal_links(page, limit):
    links = page.locator("a").evaluate_all(
        """
        links => links.map(a => ({href: a.href, text: a.innerText || a.textContent || ''}))
                      .filter(a => /\\/record\\/0-3\\/\\d+/.test(a.href))
        """
    )
    seen = {}
    for link in links:
        match = DEAL_RE.search(link["href"])
        if match:
            seen.setdefault(match.group(1), link)
    return list(seen.items())[:limit]


def capture_visible_deal_rows(page, limit):
    body_text = page.locator("body").inner_text(timeout=30000)
    parsed_rows = parse_hubspot_table_rows(body_text)
    extraction = "deal_table_row"
    if not parsed_rows:
        parsed_rows = parse_hubspot_board_cards(body_text)
        extraction = "deal_board_card"

    links = capture_visible_deal_links(page, limit)
    deal_rows = []
    for index, (deal_id, link) in enumerate(links):
        parsed_row = parsed_rows[index] if index < len(parsed_rows) else {"deal_name": link.get("text") or None}
        source_url = link["href"]
        row = row_to_deal(deal_id, source_url, parsed_row, extraction if index < len(parsed_rows) else "visible_link")
        deal_rows.append((deal_id, link, row))
    return deal_rows


def filter_deal_rows_by_school(deal_rows, school):
    school = clean_value(school)
    if not school:
        return deal_rows
    school_l = school.lower()
    return [
        deal_row
        for deal_row in deal_rows
        if school_l in (deal_row[2].get("school") or "").lower()
        or school_l in (deal_row[2].get("deal_name") or "").lower()
        or school_l in (deal_row[2].get("owner") or "").lower()
    ]


def wait_until_ready(page, timeout=30000):
    page.wait_for_load_state("load", timeout=timeout)
    try:
        page.wait_for_load_state("networkidle", timeout=5000)
    except PlaywrightTimeoutError:
        pass


def main():
    parser = argparse.ArgumentParser(description="Extract visible HubSpot lead/deal data into SQLite.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--profile-dir", default="browser_profiles/hubspot")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--detail-limit", type=int, default=10)
    parser.add_argument("--start-date", default=DEFAULT_INITIAL_LOAD_START)
    parser.add_argument("--school", help="Optional school filter applied after visible deal rows are parsed.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    ensure_lead_followup_schema(conn)
    run_id = start_import_run(conn, "hubspot", Path(__file__).name, args.start_date, None, {"url": args.url})
    conn.commit()
    rows_seen = rows_written = 0
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                args.profile_dir,
                headless=args.headless,
                viewport={"width": 1440, "height": 1000},
                accept_downloads=True,
            )
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(args.url, wait_until="domcontentloaded", timeout=60000)
            wait_until_ready(page)
            deal_rows = filter_deal_rows_by_school(capture_visible_deal_rows(page, args.limit), args.school)
            for index, (deal_id, link, spine_row) in enumerate(deal_rows):
                rows_seen += 1
                upsert_deal(conn, spine_row)
                rows_written += 1
                if index < args.detail_limit:
                    detail_page = context.new_page()
                    detail_page.goto(link["href"], wait_until="domcontentloaded", timeout=60000)
                    wait_until_ready(detail_page)
                    text = detail_page.locator("body").inner_text(timeout=30000)
                    detail_row = parse_deal_text(deal_id, detail_page.url, text)
                    row = merge_deal_rows(spine_row, detail_row)
                    upsert_deal(conn, row)
                    rows_written += 1
                    rows_written += upsert_contact_from_text(conn, deal_id, detail_page.url, text)
                    detail_page.close()
            context.close()
        finish_import_run(conn, run_id, "success", rows_seen, rows_written, 0)
        conn.commit()
    except Exception as exc:
        finish_import_run(conn, run_id, "error", rows_seen, rows_written, 0, str(exc))
        conn.commit()
        raise
    finally:
        conn.close()

    print(f"HubSpot extraction complete: rows_seen={rows_seen} rows_written={rows_written}")


if __name__ == "__main__":
    main()
