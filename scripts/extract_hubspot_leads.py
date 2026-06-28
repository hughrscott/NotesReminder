#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin

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
from notesreminder.lib.raw_capture import write_raw_capture  # noqa: E402


DEFAULT_CONTACT_REPORT_URL = "https://app.hubspot.com/reports-dashboard/6841203/view/18996986"
DEFAULT_HUBSPOT_AUTH_LAUNCH_URL = "https://sor.okta.com/home/hubspotsaml/0oa1ljaq3melIw1DD1d8/aln1emgmc7tbRiire1d8"
DEFAULT_DEAL_URL = "https://app.hubspot.com/contacts/6841203/objects/0-3/views/all/list?prefetch="
DEFAULT_URL = DEFAULT_CONTACT_REPORT_URL
HUBSPOT_PORTAL_ID = "6841203"
HUBSPOT_REPORTING_ASYNC_ROOT = "https://app.hubspot.com/api/reporting/v3/dataset/resolve/async"
HUBSPOT_REPORTING_QUERY = (
    "portalId=6841203&clienttimeout=30000&hs_static_app=DashboardUI&hs_static_app_version=4.86819"
)
DEAL_RE = re.compile(r"/record/0-3/(\d+)")
CONTACT_RE = re.compile(r"/(?:record/0-1|contact)/(\d+)")
PIKE13_PERSON_RE = re.compile(r"/people/(\d+)")
EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}")
NO_VALUE_MARKERS = {"", "--", "details", "actions", "- deal", "- display deal"}
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
    "record source detail 3",
    "registration method",
    "registration type",
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
LABEL_NOISE_MARKERS = {
    "ga utm term - deal",
    "marketing source category",
    "student for deal",
}
SCHOOL_FROM_OWNER = {
    "westu": "West University Place",
    "west u": "West University Place",
    "theheights": "The Heights",
    "the heights": "The Heights",
}
SCHOOL_CANONICAL_MARKERS = {
    "west university": "west_u",
    "west u": "west_u",
    "westu": "west_u",
    "the heights": "heights",
    "heights": "heights",
}
SCHOOL_CANONICAL_LABELS = {
    "west_u": "West University Place",
    "heights": "The Heights",
}


def stable_id(prefix, *parts):
    digest = hashlib.sha256("|".join(str(p or "") for p in parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:24]}"


def text_after(label, text):
    lines = visible_lines(text)
    value = value_after_label(lines, label)
    if value:
        return value
    pattern = re.compile(rf"(?:^|\n)\s*{re.escape(label)}\s*:\s*([^\n]+)", re.IGNORECASE)
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


def is_internal_email(email):
    normalized = normalize_email(email)
    return bool(normalized and normalized.endswith("@schoolofrock.com"))


def is_label_or_placeholder(value):
    value = clean_value(value)
    if not value:
        return True
    value_l = value.lower()
    return (
        value_l in NO_VALUE_MARKERS
        or value_l in FIELD_LABELS
        or value_l in LABEL_NOISE_MARKERS
        or value_l.endswith(" - deal")
        or value_l.endswith(" - display deal")
    )


def sanitized_value(value):
    value = clean_value(value)
    if is_label_or_placeholder(value):
        return None
    return value


def sanitized_date(value):
    value = sanitized_value(value)
    return value if looks_like_date(value) else None


def sanitized_yes_no(value):
    value = sanitized_value(value)
    if not value:
        return None
    value_l = value.lower()
    if value_l in {"yes", "y", "true"}:
        return "Yes"
    if value_l in {"no", "n", "false"}:
        return "No"
    return None


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
        value = sanitized_value(value)
        if value:
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


def person_name_from_deal_name(deal_name):
    if not deal_name:
        return None
    name = clean_value(deal_name.split("|", 1)[0])
    if not name:
        return None
    return re.sub(r"\s*\([^)]*\)\s*$", "", name).strip() or None


def school_from_owner(owner):
    owner_l = (owner or "").lower()
    for marker, school in SCHOOL_FROM_OWNER.items():
        if marker in owner_l:
            return school
    return None


def normalized_name(value):
    value = sanitized_value(value)
    if not value:
        return None
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def collapsed_duplicate_name(value):
    name = normalized_name(value)
    if not name:
        return None
    tokens = []
    for token in name.split():
        if not tokens or tokens[-1] != token:
            tokens.append(token)
    return " ".join(tokens)


def normalized_name_keys(value):
    return {key for key in (normalized_name(value), collapsed_duplicate_name(value)) if key}


def canonical_school(value):
    value_l = (value or "").lower()
    for marker, canonical in SCHOOL_CANONICAL_MARKERS.items():
        if marker in value_l:
            return canonical
    return None


def normalized_school_value(value):
    return SCHOOL_CANONICAL_LABELS.get(canonical_school(value))


def first_school_value(*values):
    for value in values:
        school = normalized_school_value(value)
        if school:
            return school
    return None


def schools_compatible(left, right):
    left_key = canonical_school(left)
    right_key = canonical_school(right)
    return bool(left_key and right_key and left_key == right_key)


def parse_date_value(value):
    if not value:
        return None
    value = str(value).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(value.split("T", 1)[0], fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def normalized_date_value(value):
    parsed = parse_date_value(value)
    return parsed.isoformat() if parsed else None


def contact_school_context(row):
    candidates = (
        row.get("school"),
        school_from_deal_name(row.get("hubspot_deal_name")),
        row.get("school_lead_status"),
        school_from_owner(row.get("owner")),
    )
    for candidate in candidates:
        if canonical_school(candidate):
            return candidate
    return None


def pike13_person_trial_dates(conn, person_id):
    rows = conn.execute(
        """
        SELECT starts_at
        FROM pike13_visits
        WHERE person_id = ?
          AND (
              COALESCE(first_visit_flag, 0) = 1
              OR LOWER(COALESCE(service, '')) LIKE '%trial%'
              OR LOWER(COALESCE(service, '')) LIKE '%free%'
          )
        """,
        (person_id,),
    ).fetchall()
    return [parsed for row in rows if (parsed := parse_date_value(row[0]))]


def choose_name_match_by_trial_date(conn, candidates, created_at):
    created = parse_date_value(created_at)
    if not created:
        return None
    lower_bound = created - timedelta(days=14)
    scored = []
    for candidate in candidates:
        dates = [trial_date for trial_date in pike13_person_trial_dates(conn, candidate[0]) if trial_date >= lower_bound]
        if dates:
            scored.append((min(dates), candidate))
    if len(scored) == 1:
        return scored[0][1]
    return None


def pike13_person_by_normalized_name(conn, name, school, created_at=None):
    name_keys = normalized_name_keys(name)
    if not name_keys:
        return None
    candidates = conn.execute(
        """
        SELECT person_id, full_name, school
        FROM pike13_people
        WHERE COALESCE(full_name, '') != ''
        """
    ).fetchall()
    name_matches = [candidate for candidate in candidates if name_keys & normalized_name_keys(candidate[1])]
    school_matches = [candidate for candidate in name_matches if schools_compatible(school, candidate[2])]
    if len(school_matches) == 1:
        return school_matches[0]
    if len(school_matches) > 1:
        return choose_name_match_by_trial_date(conn, school_matches, created_at)
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
    create_date = sanitized_date(first_valid(
        [
            text_after("Create Date", text),
            text_after("Create date", text),
            date_from_created_activity(lines),
        ]
    ))
    if not create_date:
        create_date = date_from_created_activity(lines)
    last_activity_date = sanitized_date(text_after("Last Activity Date", text))
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
        "last_contacted": sanitized_date(text_after("Last Contacted", text)),
        "follow_up_needed": sanitized_yes_no(text_after("Follow Up Needed", text)),
        "trial_date": sanitized_date(first_valid(
            [
                text_after("Trial Date", text),
                text_after("Trial Date (Deal)", text),
                text_after("Trial Date - Display Deal", text),
            ]
        )),
        "trial_no_show": sanitized_yes_no(text_after("Trial No Show", text)),
        "date_entered_scheduled_trial_stage": sanitized_date(text_after("Date Entered Scheduled Trial Stage", text)),
        "area_of_interest": sanitized_value(text_after("Area of Interest", text)),
        "instrument_type": sanitized_value(text_after("Instrument Type", text)),
        "lead_source": sanitized_value(text_after("Lead Source - Deal", text) or text_after("Lead Source", text)),
        "marketing_source": sanitized_value(text_after("Marketing Source - Deal", text) or text_after("Marketing Source", text)),
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
                row["last_contacted"] = sanitized_date(item.split(":", 1)[1])
            elif item.startswith("Trial Date (Deal):"):
                row["trial_date"] = sanitized_date(item.split(":", 1)[1])
            elif item.startswith("Follow Up Needed:"):
                row["follow_up_needed"] = sanitized_yes_no(chunk[offset + 1] if offset + 1 < len(chunk) else None)
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
            follow_up_needed = excluded.follow_up_needed,
            trial_date = excluded.trial_date,
            trial_no_show = excluded.trial_no_show,
            date_entered_scheduled_trial_stage = excluded.date_entered_scheduled_trial_stage,
            area_of_interest = excluded.area_of_interest,
            instrument_type = excluded.instrument_type,
            lead_source = excluded.lead_source,
            marketing_source = excluded.marketing_source,
            pike13_person_id = COALESCE(excluded.pike13_person_id, hubspot_deals.pike13_person_id),
            source_url = COALESCE(excluded.source_url, hubspot_deals.source_url),
            raw_text = excluded.raw_text,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )


def parse_contact_from_text(deal_id, url, text):
    lines = visible_lines(text)
    contact_ids = CONTACT_RE.findall(url + "\n" + text)
    pike13_match = PIKE13_PERSON_RE.search(url + "\n" + text)
    emails = EMAIL_RE.findall(text)
    accepted_email = None
    rejected_emails = []
    for email in emails:
        normalized = normalize_email(email)
        if not normalized:
            continue
        if is_internal_email(normalized):
            rejected_emails.append({"email": normalized, "reason": "internal_schoolofrock_email"})
            continue
        if not accepted_email:
            accepted_email = normalized

    accepted_phone = None
    accepted_phone_raw = None
    full_name = None
    if accepted_email:
        email_index = next((idx for idx, line in enumerate(lines) if accepted_email in line.lower()), None)
        if email_index is not None:
            for candidate in reversed(lines[max(0, email_index - 4) : email_index]):
                if not is_label_or_placeholder(candidate) and "school of rock" not in candidate.lower():
                    full_name = candidate
                    break
            phone_window = "\n".join(lines[email_index : email_index + 8])
            phone_match = PHONE_RE.search(phone_window)
            if phone_match:
                accepted_phone_raw = phone_match.group(0)
                accepted_phone = normalize_phone(accepted_phone_raw)
    if not full_name:
        deal_name = next((line for line in lines if " | " in line), None)
        full_name = clean_value(deal_name.split("|", 1)[0]) if deal_name else None

    trusted = bool(accepted_email or accepted_phone or contact_ids)
    diagnostics = {
        "extraction": "deal_contact_context",
        "trusted": trusted,
        "confidence": 0.9 if accepted_email and accepted_phone else 0.75 if accepted_email or accepted_phone else 0.55 if contact_ids else 0.0,
        "accepted_email": accepted_email,
        "accepted_phone": accepted_phone,
        "rejected_emails": rejected_emails,
        "contact_ids": contact_ids,
        "evidence": "customer email/phone found in HubSpot detail text near contact context" if trusted else "no trusted contact context found",
    }
    if not trusted:
        return None
    contact_id = contact_ids[0] if contact_ids else stable_id("hubspot_contact", accepted_email or accepted_phone or deal_id)
    return {
        "contact_id": contact_id,
        "full_name": sanitized_value(full_name),
        "create_date": sanitized_date(text_after("Create Date", text) or text_after("Create date", text)),
        "email": accepted_email,
        "email_normalized": accepted_email,
        "phone": accepted_phone_raw,
        "phone_normalized": accepted_phone,
        "sms_opt_in": sanitized_yes_no(text_after("SMS Opt In", text) or text_after("SMS opt-in", text)),
        "owner": sanitized_value(text_after("Contact owner", text)),
        "school": sanitized_value(text_after("School Lead Status", text) or text_after("School", text)),
        "school_lead_status": sanitized_value(text_after("School Lead Status", text)),
        "lead_source": sanitized_value(text_after("Lead Source", text) or text_after("Original Source", text)),
        "marketing_source": sanitized_value(text_after("Marketing Source", text)),
        "record_source_detail": sanitized_value(text_after("Record source detail 3", text)),
        "registration_method": sanitized_value(text_after("Registration Method", text)),
        "registration_type": sanitized_value(text_after("Registration Type", text)),
        "pike13_person_id": pike13_match.group(1) if pike13_match else None,
        "pike13_loaded_flag": 1 if pike13_match else 0,
        "pike13_match_method": "hubspot_detail_pike13_link" if pike13_match else None,
        "associated_deal_ids": deal_id,
        "source_url": url,
        "raw_text": text,
        "raw_json": json.dumps(diagnostics, sort_keys=True),
        "updated_at": utc_now_iso(),
    }


def split_name(full_name):
    full_name = sanitized_value(full_name)
    if not full_name:
        return None, None
    parts = full_name.split()
    if len(parts) == 1:
        return parts[0], None
    return parts[0], " ".join(parts[1:])


def parse_contact_report_rows(text):
    """Parse HubSpot report-details rows after clicking Contact created totals bars."""
    lines = visible_lines(text)
    header_index = None
    for index in range(len(lines) - 2):
        if lines[index].upper() == "CONTACT" and lines[index + 1].upper() == "EMAIL" and lines[index + 2].upper() == "CREATE DATE":
            header_index = index + 3
            break
    if header_index is None:
        return []
    lines = lines[header_index:]
    rows = []
    date_re = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")
    index = 0
    while index < len(lines):
        line = lines[index]
        if line.upper() in {"CONTACT", "EMAIL", "CREATE DATE"} or line.lower().endswith("contacts"):
            index += 1
            continue
        if index + 2 < len(lines) and (lines[index + 1] == "-" or EMAIL_RE.fullmatch(lines[index + 1])) and date_re.match(lines[index + 2]):
            email = None if lines[index + 1] == "-" else normalize_email(lines[index + 1])
            first_name, last_name = split_name(line)
            rows.append(
                {
                    "full_name": line,
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email,
                    "email_normalized": email,
                    "create_date": lines[index + 2],
                    "raw_text": "\n".join(lines[index : index + 3]),
                }
            )
            index += 3
            continue
        if index + 1 < len(lines) and date_re.match(lines[index + 1]):
            first_name, last_name = split_name(line)
            rows.append(
                {
                    "full_name": line,
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": None,
                    "email_normalized": None,
                    "create_date": lines[index + 1],
                    "raw_text": "\n".join(lines[index : index + 2]),
                }
            )
            index += 2
            continue
        index += 1
    return rows


def text_before_contact_associations(text):
    markers = ["\nOverview\n", "\nAssociated deals\n", "\nParent/Guardian", "\nDeals ("]
    end = len(text)
    for marker in markers:
        marker_index = text.find(marker)
        if marker_index >= 0:
            end = min(end, marker_index)
    return text[:end]


def parent_guardian_identity(text):
    lines = visible_lines(text)
    start = next((index for index, line in enumerate(lines) if line.lower().startswith("parent/guardian")), None)
    if start is None:
        return {}
    end = len(lines)
    for index in range(start + 1, len(lines)):
        line_l = lines[index].lower()
        if (
            line_l.startswith("integrations")
            or line_l.startswith("deals (")
            or line_l.startswith("schools (")
            or line_l == "view all associated deals"
            or line_l == "view all associated schools"
        ):
            end = index
            break
    section = "\n".join(lines[start:end])
    accepted_email = None
    for email in EMAIL_RE.findall(section):
        normalized = normalize_email(email)
        if normalized and not is_internal_email(normalized):
            accepted_email = normalized
            break
    phone_match = PHONE_RE.search(section)
    phone_raw = phone_match.group(0) if phone_match else None
    return {
        "email": accepted_email,
        "phone": phone_raw,
        "phone_normalized": normalize_phone(phone_raw),
    }


def contact_detail_header_school(text):
    for line in visible_lines(text)[:20]:
        if "school of rock" in line.lower():
            school = first_school_value(line)
            if school:
                return school
    return None


def contact_created_source(text):
    match = re.search(r"This contact was created from\s+(.+?)\s+from", text, re.IGNORECASE)
    if match:
        return sanitized_value(match.group(1))
    match = re.search(r"This contact was created from\s+([^\n]+)", text, re.IGNORECASE)
    return sanitized_value(match.group(1)) if match else None


def value_after_label_in_lines(lines, index):
    line = lines[index]
    if ":" in line:
        value = clean_value(line.split(":", 1)[1])
        if value and not is_label_or_placeholder(value):
            return value
    for candidate in lines[index + 1 : index + 5]:
        if is_label_or_placeholder(candidate):
            continue
        return candidate
    return None


def associated_deal_summaries(text):
    lines = visible_lines(text)
    deals = []
    seen = set()
    for index, line in enumerate(lines):
        if " | " not in line or "school of rock" in line.lower():
            continue
        window = lines[index : index + 24]
        stage = None
        trial_date = None
        create_date = None
        for offset, item in enumerate(window):
            item_l = item.lower()
            if item_l.endswith("(lead pipeline)"):
                stage = normalize_stage(item)
            elif item_l.startswith("deal stage"):
                stage = normalize_stage(value_after_label_in_lines(window, offset))
            elif item_l.startswith("trial date"):
                trial_date = sanitized_date(value_after_label_in_lines(window, offset))
            elif item_l.startswith("create date"):
                create_date = sanitized_date(value_after_label_in_lines(window, offset))
        if not stage and not trial_date and not create_date:
            continue
        key = (line, stage, trial_date, create_date)
        if key in seen:
            continue
        seen.add(key)
        deals.append(
            {
                "deal_name": line,
                "stage": stage,
                "trial_date": trial_date,
                "create_date": create_date,
            }
        )
    return deals


def hubspot_trial_scheduled_flag(deals):
    for deal in deals:
        stage = (deal.get("stage") or "").lower()
        if deal.get("trial_date") or "scheduled trial" in stage or "trial/tour" in stage:
            return 1
    return 0


def parse_contact_detail_text(contact_id, url, text, report_row=None):
    report_row = report_row or {}
    lines = visible_lines(text)
    primary_text = text_before_contact_associations(text)
    emails = [normalize_email(email) for email in EMAIL_RE.findall(primary_text)]
    accepted_email = next((email for email in emails if email and not is_internal_email(email)), None)
    phone_match = PHONE_RE.search(primary_text)
    phone_raw = phone_match.group(0) if phone_match else None
    guardian_identity = parent_guardian_identity(text)
    pike13_match = PIKE13_PERSON_RE.search(url + "\n" + text)
    full_name = (
        sanitized_value(report_row.get("full_name"))
        or sanitized_value(text_after("Name", primary_text))
        or next((line for line in lines if not is_label_or_placeholder(line)), None)
    )
    first_name, last_name = split_name(full_name)
    deals = associated_deal_summaries(text)
    primary_deal = deals[0] if deals else {}
    row = {
        "contact_id": contact_id,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "create_date": sanitized_date(text_after("Create Date", text) or text_after("Create date", text))
        or report_row.get("create_date"),
        "email": accepted_email or guardian_identity.get("email") or report_row.get("email"),
        "email_normalized": normalize_email(
            accepted_email
            or guardian_identity.get("email")
            or report_row.get("email_normalized")
            or report_row.get("email")
        ),
        "phone": phone_raw or guardian_identity.get("phone"),
        "phone_normalized": normalize_phone(phone_raw) or guardian_identity.get("phone_normalized"),
        "sms_opt_in": sanitized_yes_no(text_after("SMS Opt In", text) or text_after("SMS opt-in", text)),
        "owner": sanitized_value(text_after("Contact owner", text) or text_after("Owner", text)),
        "school": first_school_value(
            text_after("School", text),
            text_after("School Name", text),
            text_after("School Lead Status", text),
            contact_detail_header_school(text),
            report_row.get("school"),
            school_from_deal_name(primary_deal.get("deal_name")),
        ),
        "school_lead_status": sanitized_value(text_after("School Lead Status", text)),
        "lead_source": sanitized_value(text_after("Lead Source", text) or text_after("Original Source", text) or contact_created_source(text)),
        "marketing_source": sanitized_value(text_after("Marketing Source", text)),
        "record_source_detail": sanitized_value(text_after("Record source detail 3", text)),
        "registration_method": sanitized_value(text_after("Registration Method", text)),
        "registration_type": sanitized_value(text_after("Registration Type", text)),
        "hubspot_deal_name": primary_deal.get("deal_name"),
        "hubspot_deal_stage": primary_deal.get("stage"),
        "hubspot_trial_date": primary_deal.get("trial_date"),
        "hubspot_trial_scheduled_flag": hubspot_trial_scheduled_flag(deals),
        "hubspot_associated_deals_json": json.dumps(deals, sort_keys=True) if deals else None,
        "pike13_person_id": pike13_match.group(1) if pike13_match else None,
        "pike13_loaded_flag": 1 if pike13_match else 0,
        "pike13_match_method": "hubspot_detail_pike13_link" if pike13_match else None,
        "associated_deal_ids": report_row.get("associated_deal_ids"),
        "source_url": url,
        "raw_text": text,
        "raw_json": None,
        "updated_at": utc_now_iso(),
    }
    row["raw_json"] = json.dumps(
        {
            "extraction": "contact_detail_text",
            "fields_found": [key for key, value in row.items() if value and key not in {"raw_text", "raw_json"}],
            "pike13_loaded_flag": row["pike13_loaded_flag"],
            "pike13_match_method": row["pike13_match_method"],
            "associated_deals": deals,
            "parent_guardian_identity_found": bool(
                guardian_identity.get("email") or guardian_identity.get("phone_normalized")
            ),
            "report_row": report_row,
        },
        sort_keys=True,
    )
    return row


def apply_pike13_match_from_db(conn, row):
    if row.get("pike13_loaded_flag"):
        return row
    match = None
    match_school = None
    method = None
    school = contact_school_context(row)
    if row.get("pike13_person_id"):
        match = conn.execute(
            "SELECT person_id, school FROM pike13_people WHERE person_id = ?",
            (row["pike13_person_id"],),
        ).fetchone()
        method = "pike13_person_id"
    if not match and row.get("email_normalized"):
        matches = conn.execute(
            "SELECT person_id, school FROM pike13_people WHERE email_normalized = ?",
            (row["email_normalized"],),
        ).fetchall()
        match = matches[0] if len(matches) == 1 else None
        method = "email"
    if not match and row.get("phone_normalized"):
        matches = conn.execute(
            "SELECT person_id, school FROM pike13_people WHERE phone_normalized = ?",
            (row["phone_normalized"],),
        ).fetchall()
        match = matches[0] if len(matches) == 1 else None
        method = "phone"
    if not match and row.get("full_name"):
        candidate = pike13_person_by_normalized_name(conn, row.get("full_name"), school, row.get("create_date"))
        if candidate:
            match = (candidate[0], candidate[2])
            method = "name_school"
    deal_person_name = person_name_from_deal_name(row.get("hubspot_deal_name"))
    if not match and deal_person_name:
        candidate = pike13_person_by_normalized_name(conn, deal_person_name, school, row.get("create_date"))
        if candidate:
            match = (candidate[0], candidate[2])
            method = "deal_name_school"
    if match:
        match_school = match[1] if len(match) > 1 else None
        row["pike13_person_id"] = row.get("pike13_person_id") or match[0]
        row["pike13_loaded_flag"] = 1
        row["pike13_match_method"] = f"existing_pike13_{method}"
        if match_school and not contact_school_context(row):
            row["school"] = match_school
    return row


def merge_contact_rows(spine_row, detail_row):
    if not spine_row:
        return detail_row
    if not detail_row:
        return spine_row
    merged = dict(detail_row)
    if spine_row.get("create_date"):
        merged["create_date"] = spine_row["create_date"]
    if not merged.get("school"):
        merged["school"] = first_school_value(
            spine_row.get("school"),
            spine_row.get("school_lead_status"),
            school_from_deal_name(spine_row.get("hubspot_deal_name")),
        )
    for field in ("full_name", "first_name", "last_name", "email", "email_normalized"):
        if spine_row.get(field) and not merged.get(field):
            merged[field] = spine_row[field]
    spine_text = spine_row.get("raw_text") or ""
    detail_text = detail_row.get("raw_text") or ""
    if spine_text and detail_text and spine_text not in detail_text:
        merged["raw_text"] = f"{spine_text}\n\n--- HubSpot contact detail page ---\n\n{detail_text}"
    return merged


def compact_row(row):
    """Return fields useful for smoke-test review without raw page text."""
    if not row:
        return None
    return {key: value for key, value in row.items() if key not in {"raw_text", "raw_json", "updated_at"}}


def write_json_output(payload, output_path):
    content = json.dumps(payload, indent=2, sort_keys=True)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content + "\n", encoding="utf-8")
        return str(path)
    print(content)
    return None


def parse_iso_date(value):
    return datetime.strptime(value, "%Y-%m-%d").date()


def date_range(start_date, end_date):
    current = parse_iso_date(start_date)
    end = parse_iso_date(end_date)
    while current <= end:
        yield current.isoformat()
        current += timedelta(days=1)


def epoch_ms_to_date(value):
    if value in (None, "", "@@MISSING@@"):
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1000, timezone.utc).date().isoformat()
    except (TypeError, ValueError, OSError):
        return None


def upsert_contact(conn, row):
    if not row:
        return 0
    for key in (
        "first_name",
        "last_name",
        "full_name",
        "create_date",
        "email",
        "email_normalized",
        "phone",
        "phone_normalized",
        "sms_opt_in",
        "owner",
        "school",
        "school_lead_status",
        "lead_source",
        "marketing_source",
        "record_source_detail",
        "registration_method",
        "registration_type",
        "hubspot_deal_name",
        "hubspot_deal_stage",
        "hubspot_trial_date",
        "hubspot_trial_scheduled_flag",
        "hubspot_associated_deals_json",
        "pike13_person_id",
        "pike13_loaded_flag",
        "pike13_match_method",
        "associated_deal_ids",
        "source_url",
        "raw_text",
        "raw_json",
        "updated_at",
    ):
        row.setdefault(key, None)
    row["pike13_loaded_flag"] = int(row.get("pike13_loaded_flag") or 0)
    row["hubspot_trial_scheduled_flag"] = int(row.get("hubspot_trial_scheduled_flag") or 0)
    row["create_date"] = normalized_date_value(row.get("create_date"))
    row["hubspot_trial_date"] = normalized_date_value(row.get("hubspot_trial_date")) or row.get("hubspot_trial_date")
    row["school"] = first_school_value(
        row.get("school"),
        row.get("school_lead_status"),
        school_from_deal_name(row.get("hubspot_deal_name")),
        school_from_owner(row.get("owner")),
    )
    row = apply_pike13_match_from_db(conn, row)
    conn.execute(
        """
        INSERT INTO hubspot_contacts (
            contact_id, first_name, last_name, full_name, create_date, email,
            email_normalized, phone, phone_normalized, sms_opt_in, owner, school,
            school_lead_status, lead_source, marketing_source, record_source_detail,
            registration_method, registration_type, hubspot_deal_name,
            hubspot_deal_stage, hubspot_trial_date, hubspot_trial_scheduled_flag,
            hubspot_associated_deals_json, pike13_person_id,
            pike13_loaded_flag, pike13_match_method, associated_deal_ids,
            source_url, raw_text, raw_json, updated_at
        )
        VALUES (
            :contact_id, :first_name, :last_name, :full_name, :create_date, :email,
            :email_normalized, :phone, :phone_normalized, :sms_opt_in, :owner, :school,
            :school_lead_status, :lead_source, :marketing_source, :record_source_detail,
            :registration_method, :registration_type, :hubspot_deal_name,
            :hubspot_deal_stage, :hubspot_trial_date, :hubspot_trial_scheduled_flag,
            :hubspot_associated_deals_json, :pike13_person_id,
            :pike13_loaded_flag, :pike13_match_method, :associated_deal_ids,
            :source_url, :raw_text, :raw_json, :updated_at
        )
        ON CONFLICT(contact_id) DO UPDATE SET
            first_name = COALESCE(excluded.first_name, hubspot_contacts.first_name),
            last_name = COALESCE(excluded.last_name, hubspot_contacts.last_name),
            full_name = COALESCE(excluded.full_name, hubspot_contacts.full_name),
            create_date = CASE
                WHEN excluded.create_date IS NOT NULL THEN excluded.create_date
                ELSE hubspot_contacts.create_date
            END,
            email = COALESCE(excluded.email, hubspot_contacts.email),
            email_normalized = COALESCE(excluded.email_normalized, hubspot_contacts.email_normalized),
            phone = COALESCE(excluded.phone, hubspot_contacts.phone),
            phone_normalized = COALESCE(excluded.phone_normalized, hubspot_contacts.phone_normalized),
            sms_opt_in = COALESCE(excluded.sms_opt_in, hubspot_contacts.sms_opt_in),
            owner = COALESCE(excluded.owner, hubspot_contacts.owner),
            school = CASE
                WHEN LOWER(COALESCE(excluded.school, '')) LIKE '%west university%'
                  OR LOWER(COALESCE(excluded.school, '')) LIKE '%west u%'
                  OR LOWER(COALESCE(excluded.school, '')) LIKE '%westu%'
                  THEN 'West University Place'
                WHEN LOWER(COALESCE(excluded.school, '')) LIKE '%height%'
                  THEN 'The Heights'
                WHEN LOWER(COALESCE(hubspot_contacts.school, '')) LIKE '%west university%'
                  OR LOWER(COALESCE(hubspot_contacts.school, '')) LIKE '%west u%'
                  OR LOWER(COALESCE(hubspot_contacts.school, '')) LIKE '%westu%'
                  THEN 'West University Place'
                WHEN LOWER(COALESCE(hubspot_contacts.school, '')) LIKE '%height%'
                  THEN 'The Heights'
                ELSE NULL
            END,
            school_lead_status = COALESCE(excluded.school_lead_status, hubspot_contacts.school_lead_status),
            lead_source = COALESCE(excluded.lead_source, hubspot_contacts.lead_source),
            marketing_source = COALESCE(excluded.marketing_source, hubspot_contacts.marketing_source),
            record_source_detail = COALESCE(excluded.record_source_detail, hubspot_contacts.record_source_detail),
            registration_method = COALESCE(excluded.registration_method, hubspot_contacts.registration_method),
            registration_type = COALESCE(excluded.registration_type, hubspot_contacts.registration_type),
            hubspot_deal_name = COALESCE(excluded.hubspot_deal_name, hubspot_contacts.hubspot_deal_name),
            hubspot_deal_stage = COALESCE(excluded.hubspot_deal_stage, hubspot_contacts.hubspot_deal_stage),
            hubspot_trial_date = COALESCE(excluded.hubspot_trial_date, hubspot_contacts.hubspot_trial_date),
            hubspot_trial_scheduled_flag = MAX(COALESCE(excluded.hubspot_trial_scheduled_flag, 0), COALESCE(hubspot_contacts.hubspot_trial_scheduled_flag, 0)),
            hubspot_associated_deals_json = COALESCE(excluded.hubspot_associated_deals_json, hubspot_contacts.hubspot_associated_deals_json),
            pike13_person_id = COALESCE(excluded.pike13_person_id, hubspot_contacts.pike13_person_id),
            pike13_loaded_flag = MAX(COALESCE(excluded.pike13_loaded_flag, 0), COALESCE(hubspot_contacts.pike13_loaded_flag, 0)),
            pike13_match_method = COALESCE(excluded.pike13_match_method, hubspot_contacts.pike13_match_method),
            associated_deal_ids = COALESCE(excluded.associated_deal_ids, hubspot_contacts.associated_deal_ids),
            source_url = COALESCE(excluded.source_url, hubspot_contacts.source_url),
            raw_text = excluded.raw_text,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )
    return 1


def upsert_contact_from_text(conn, deal_id, url, text):
    return upsert_contact(conn, parse_contact_from_text(deal_id, url, text))


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


def click_next_page(page):
    next_control = page.get_by_text("Next", exact=True).last
    try:
        if next_control.count() == 0:
            return False
        if next_control.is_disabled(timeout=1000):
            return False
        next_control.click(timeout=5000)
        wait_until_ready(page, timeout=15000)
        return True
    except PlaywrightTimeoutError:
        return False


def capture_visible_deal_rows_across_pages(page, limit, max_pages):
    seen = {}
    pages_processed = 0
    for _ in range(max(1, max_pages)):
        page_rows = capture_visible_deal_rows(page, max(1, limit - len(seen)))
        pages_processed += 1
        for deal_id, link, row in page_rows:
            seen.setdefault(deal_id, (deal_id, link, row))
            if len(seen) >= limit:
                return list(seen.values()), pages_processed
        if not page_rows or not click_next_page(page):
            break
    return list(seen.values())[:limit], pages_processed


def capture_visible_contact_links(page, limit):
    links = page.locator("a").evaluate_all(
        """
        links => links.map(a => ({href: a.href, text: a.innerText || a.textContent || ''}))
                      .filter(a => /\\/(?:record\\/0-1|contact)\\/\\d+/.test(a.href))
        """
    )
    seen = {}
    for link in links:
        match = CONTACT_RE.search(link["href"])
        if match:
            seen.setdefault(match.group(1), link)
    return list(seen.items())[:limit]


def contact_row_from_report_link(contact_id, link, parsed_row=None):
    parsed_row = dict(parsed_row or {})
    full_name = sanitized_value(parsed_row.get("full_name") or link.get("text"))
    first_name, last_name = split_name(full_name)
    return {
        "contact_id": contact_id,
        "first_name": parsed_row.get("first_name") or first_name,
        "last_name": parsed_row.get("last_name") or last_name,
        "full_name": full_name,
        "create_date": parsed_row.get("create_date"),
        "email": parsed_row.get("email"),
        "email_normalized": parsed_row.get("email_normalized"),
        "phone": None,
        "phone_normalized": None,
        "sms_opt_in": None,
        "owner": None,
        "school": parsed_row.get("school"),
        "school_lead_status": None,
        "lead_source": None,
        "marketing_source": None,
        "record_source_detail": None,
        "registration_method": None,
        "registration_type": None,
        "pike13_person_id": None,
        "pike13_loaded_flag": 0,
        "pike13_match_method": None,
        "associated_deal_ids": None,
        "source_url": link["href"],
        "raw_text": parsed_row.get("raw_text") or full_name or "",
        "raw_json": json.dumps({"extraction": "contact_report_row"}, sort_keys=True),
        "updated_at": utc_now_iso(),
    }


def capture_visible_contact_rows(page, limit):
    body_text = page.locator("body").inner_text(timeout=30000)
    parsed_rows = parse_contact_report_rows(body_text)
    links = capture_visible_contact_links(page, limit)
    contact_rows = []
    for index, (contact_id, link) in enumerate(links):
        parsed_row = parsed_rows[index] if index < len(parsed_rows) else {"full_name": link.get("text") or None}
        row = contact_row_from_report_link(contact_id, link, parsed_row)
        contact_rows.append((contact_id, link, row))
    return contact_rows


def click_report_details_close(page):
    for locator in (
        page.get_by_role("button", name=re.compile(r"close", re.I)).last,
        page.locator('[aria-label="Close"]').last,
        page.locator("text=Close").last,
    ):
        try:
            if locator.count() > 0 and locator.is_visible(timeout=1000):
                locator.click(timeout=5000)
                page.wait_for_timeout(1000)
                return True
        except PlaywrightTimeoutError:
            continue
    return False


def contact_report_bar_candidates(page):
    return page.locator("rect.highcharts-point[aria-label]").evaluate_all(
        """
        elements => elements.map((element, index) => {
            const box = element.getBoundingClientRect();
            const label = element.getAttribute('aria-label') || '';
            const count = Number((label.match(/,\\s*(\\d+)\\s*$/) || [])[1] || 0);
            return {
                index,
                label,
                count,
                x: box.x,
                y: box.y,
                width: box.width,
                height: box.height,
            };
        }).filter(item =>
            item.count > 0
            && item.width >= 2
            && item.height >= 2
            && item.y < window.innerHeight
        ).sort((a, b) => a.x - b.x || a.y - b.y)
        """
    )


def capture_visible_contact_rows_from_report(page, limit, max_bars):
    rows_by_id = {}
    rows = capture_visible_contact_rows(page, limit)
    for contact_id, link, row in rows:
        rows_by_id.setdefault(contact_id, (contact_id, link, row))
    if len(rows_by_id) >= limit:
        return list(rows_by_id.values())[:limit], 0

    clicked_bars = 0
    for candidate in contact_report_bar_candidates(page):
        if clicked_bars >= max_bars or len(rows_by_id) >= limit:
            break
        try:
            page.mouse.click(
                candidate["x"] + candidate["width"] / 2,
                candidate["y"] + candidate["height"] / 2,
            )
            page.wait_for_timeout(1500)
            if page.get_by_text("Report details").count() == 0:
                continue
            clicked_bars += 1
            rows = capture_visible_contact_rows(page, max(1, limit - len(rows_by_id)))
            for contact_id, link, row in rows:
                rows_by_id.setdefault(contact_id, (contact_id, link, row))
            click_report_details_close(page)
            page.wait_for_timeout(500)
        except Exception:
            click_report_details_close(page)
            continue
    return list(rows_by_id.values())[:limit], clicked_bars


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


def wait_until_ready(page, timeout=30000, networkidle_timeout=5000, tolerate_load_timeout=False):
    try:
        page.wait_for_load_state("load", timeout=timeout)
    except PlaywrightTimeoutError:
        if not tolerate_load_timeout:
            raise
    if not networkidle_timeout:
        return
    try:
        page.wait_for_load_state("networkidle", timeout=networkidle_timeout)
    except PlaywrightTimeoutError:
        pass


def goto_hubspot_url(page, target_url, auth_launch_url=""):
    if auth_launch_url:
        page.goto(auth_launch_url, wait_until="domcontentloaded", timeout=60000)
        wait_until_ready(page, tolerate_load_timeout=True)
    if page.url != target_url:
        page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
        wait_until_ready(page)


def is_hubspot_auth_page(url, text):
    url_l = (url or "").lower()
    text_l = (text or "").lower()
    return (
        "app.hubspot.com/login" in url_l
        or "loginredirecturl=" in url_l
        or "authfailurereason" in url_l
        or "sign in with your account to access hubspot" in text_l
        or "powered by okta" in text_l and "hubspot" in text_l
    )


def dry_run_db_match(args, row):
    if not getattr(args, "dry_run_match_db", False) or not row:
        return row
    db_path = Path(args.db)
    if not db_path.exists():
        return row
    readonly_uri = f"file:{db_path.resolve()}?mode=ro"
    try:
        with sqlite3.connect(readonly_uri, uri=True) as conn:
            return apply_pike13_match_from_db(conn, row)
    except sqlite3.Error:
        return row


def pike13_trial_reconciliation(conn, row):
    if not row:
        return None
    matched = apply_pike13_match_from_db(conn, dict(row))
    person_id = matched.get("pike13_person_id")
    match_method = matched.get("pike13_match_method")
    if not person_id:
        return {
            "pike13_person_id": None,
            "pike13_match_method": None,
            "trial_found": False,
            "conversion_found": False,
        }

    trial = conn.execute(
        """
        SELECT visit_id, service, starts_at, status, school,
               COALESCE(no_show_flag, 0) AS no_show_flag,
               COALESCE(canceled_flag, 0) AS canceled_flag,
               COALESCE(attendance_confirmed_flag, 0) AS attendance_confirmed_flag,
               COALESCE(checked_in_flag, 0) AS checked_in_flag,
               COALESCE(first_visit_flag, 0) AS first_visit_flag,
               COALESCE(enrolled_flag, 0) AS enrolled_flag
        FROM pike13_visits
        WHERE person_id = ?
          AND (
            COALESCE(first_visit_flag, 0) = 1
            OR LOWER(COALESCE(service, '')) LIKE '%trial%'
          )
        ORDER BY COALESCE(starts_at, ''), visit_id
        LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    conversion = conn.execute(
        """
        SELECT plan_pass_id, name, status, starts_at, ends_at, school
        FROM pike13_plans_passes
        WHERE person_id = ?
          AND LOWER(COALESCE(name, '')) NOT LIKE '%trial%'
          AND LOWER(COALESCE(name, '')) NOT LIKE '%free%'
          AND (
            COALESCE(starts_at, '') != ''
            OR COALESCE(next_invoice_at, '') != ''
            OR COALESCE(payer_name, '') != ''
          )
        ORDER BY COALESCE(starts_at, ''), plan_pass_id
        LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    trial_payload = None
    if trial:
        trial_payload = {
            "visit_id": trial[0],
            "service": trial[1],
            "starts_at": trial[2],
            "status": trial[3],
            "school": trial[4],
            "no_show_flag": int(trial[5] or 0),
            "canceled_flag": int(trial[6] or 0),
            "attendance_confirmed_flag": int(trial[7] or 0),
            "checked_in_flag": int(trial[8] or 0),
            "first_visit_flag": int(trial[9] or 0),
            "enrolled_flag": int(trial[10] or 0),
            "happened_flag": 1 if (trial[3] or "").lower() in {"complete", "completed"} or int(trial[7] or 0) or int(trial[8] or 0) else 0,
        }
    conversion_payload = None
    if conversion:
        conversion_payload = {
            "plan_pass_id": conversion[0],
            "name": conversion[1],
            "status": conversion[2],
            "starts_at": conversion[3],
            "ends_at": conversion[4],
            "school": conversion[5],
        }
    return {
        "pike13_person_id": person_id,
        "pike13_match_method": match_method,
        "trial_found": bool(trial_payload),
        "trial": trial_payload,
        "conversion_found": bool(conversion_payload),
        "conversion": conversion_payload,
    }


def dry_run_reconcile_pike13(args, row):
    if not getattr(args, "dry_run_match_db", False) or not row:
        return None
    db_path = Path(args.db)
    if not db_path.exists():
        return None
    readonly_uri = f"file:{db_path.resolve()}?mode=ro"
    try:
        with sqlite3.connect(readonly_uri, uri=True) as conn:
            return pike13_trial_reconciliation(conn, row)
    except sqlite3.Error as exc:
        return {"error": str(exc)}


def maybe_wait_for_hubspot_auth(page, args, body_text):
    if not is_hubspot_auth_page(page.url, body_text):
        return body_text, False
    if not getattr(args, "pause_on_auth", False) or args.headless:
        return body_text, True
    print("HubSpot authentication required. Complete login in the opened browser, then press Enter here to continue.")
    input()
    wait_until_ready(page)
    refreshed_text = page.locator("body").inner_text(timeout=30000)
    return refreshed_text, is_hubspot_auth_page(page.url, refreshed_text)


def hubspot_request_headers(context):
    csrf = next(
        (cookie["value"] for cookie in context.cookies("https://app.hubspot.com") if cookie["name"] == "hubspotapi-csrf"),
        None,
    )
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }
    if csrf:
        headers["x-hubspot-csrf-hubspotapi"] = csrf
    return headers


def hubspot_reporting_payload(day, limit, offset=0):
    return {
        "chartType": "TABLE",
        "config": {
            "customized": False,
            "processors": [],
            "metrics": [
                {"property": "vid", "metricTypes": ["COUNT"]},
                {"property": "email", "metricTypes": ["COUNT"]},
                {"property": "createdate", "metricTypes": ["COUNT"]},
            ],
            "dataType": "CRM_OBJECT",
            "dimensions": [],
            "offset": offset,
            "properties": [],
            "objectTypeId": "0-1",
            "limit": limit,
            "filters": {
                "dateRange": {
                    "property": "createdate",
                    "value": {
                        "rangeType": "CUSTOM",
                        "startDate": day,
                        "endDate": day,
                    },
                },
                "custom": [],
            },
            "configType": "SEARCH",
            "v2": False,
            "sort": [],
        },
        "offset": offset,
        "displayParams": {
            "allowDrilldown": True,
            "drilldown": True,
            "hideKeyColumn": True,
        },
    }


def resolve_hubspot_dataset(page, payload, headers, poll_interval=0.5, attempts=60):
    response = page.request.post(
        f"{HUBSPOT_REPORTING_ASYNC_ROOT}?{HUBSPOT_REPORTING_QUERY}",
        data=json.dumps(payload),
        headers=headers,
        timeout=60000,
    )
    if response.status >= 400:
        raise RuntimeError(f"HubSpot reporting request failed with HTTP {response.status}: {response.text()[:500]}")
    task_id = response.json().get("id")
    if not task_id:
        raise RuntimeError(f"HubSpot reporting request did not return a task id: {response.text()[:500]}")
    poll_url = f"{HUBSPOT_REPORTING_ASYNC_ROOT}/{task_id}?{HUBSPOT_REPORTING_QUERY}"
    for _ in range(attempts):
        poll = page.request.get(poll_url, headers=headers, timeout=60000)
        if poll.status >= 400:
            raise RuntimeError(f"HubSpot reporting poll failed with HTTP {poll.status}: {poll.text()[:500]}")
        payload = poll.json()
        if payload.get("taskStatus") == "COMPLETED":
            return payload
        if payload.get("taskStatus") in {"FAILED", "CANCELED"}:
            raise RuntimeError(f"HubSpot reporting task ended with status={payload.get('taskStatus')}: {json.dumps(payload)[:500]}")
        time.sleep(poll_interval)
    raise RuntimeError(f"HubSpot reporting task timed out: {task_id}")


def contact_rows_from_dataset(day, dataset):
    primary = (((dataset or {}).get("result") or {}).get("primaryDataSet") or {})
    identifiers = primary.get("identifiers") or {}
    labels = (identifiers.get("vid") or identifiers.get("hs_object_id") or {})
    links = (primary.get("links") or {}).get("vid") or {}
    rows = []
    for item in primary.get("data") or []:
        contact_id = str(item.get("vid") or item.get("hs_object_id") or "").strip()
        if not contact_id:
            continue
        label = (((labels.get(contact_id) or {}).get("references") or {}).get("label"))
        contact_links = links.get(contact_id) or {}
        href = next(iter(contact_links.values()), None) or f"/contacts/{HUBSPOT_PORTAL_ID}/contact/{contact_id}"
        email = item.get("email")
        if email == "@@MISSING@@":
            email = None
        parsed_row = {
            "full_name": sanitized_value(label),
            "email": normalize_email(email),
            "email_normalized": normalize_email(email),
            "create_date": day,
            "raw_text": json.dumps(item, sort_keys=True),
        }
        link = {"href": urljoin("https://app.hubspot.com", href), "text": label or ""}
        rows.append((contact_id, link, contact_row_from_report_link(contact_id, link, parsed_row)))
    pagination = primary.get("pagination") or {}
    return rows, int(pagination.get("total") or len(rows)), int(pagination.get("offset") or len(rows))


def fetch_contact_rows_for_day(page, headers, day, page_size):
    rows_by_id = {}
    offset = 0
    total = None
    while total is None or offset < total:
        dataset = resolve_hubspot_dataset(page, hubspot_reporting_payload(day, page_size, offset), headers)
        rows, total, next_offset = contact_rows_from_dataset(day, dataset)
        for contact_id, link, row in rows:
            rows_by_id.setdefault(contact_id, (contact_id, link, row))
        if not rows or next_offset <= offset:
            break
        offset = next_offset
    return list(rows_by_id.values()), total or len(rows_by_id)


def extract_contacts_api(conn, context, page, args, run_id):
    goto_hubspot_url(page, args.url, args.auth_launch_url)
    body_text = page.locator("body").inner_text(timeout=30000)
    if is_hubspot_auth_page(page.url, body_text):
        raise RuntimeError("HubSpot authentication required; extractor reached the login page.")
    headers = hubspot_request_headers(context)
    detail_page = context.new_page() if args.detail_limit != 0 else None
    rows_seen = rows_written = 0
    detail_count = 0
    for day in date_range(args.start_date, args.end_date):
        day_rows, day_total = fetch_contact_rows_for_day(page, headers, day, args.api_page_size)
        write_raw_capture(
            conn,
            source="hubspot",
            capture_type="hubspot_api_contact_rows",
            content=[row for _, _, row in day_rows],
            source_url=page.url,
            metadata={"date": day, "total": day_total, "mode": "contacts-api"},
            import_run_id=run_id,
            extension="json",
            label=f"api-contact-rows-{day}",
        )
        day_errors = []
        for contact_id, link, spine_row in day_rows:
            rows_seen += 1
            rows_written += upsert_contact(conn, spine_row)
            should_detail = detail_page and (args.detail_limit < 0 or detail_count < args.detail_limit)
            if should_detail:
                try:
                    detail_page.goto(link["href"], wait_until="domcontentloaded", timeout=60000)
                    # HubSpot contact detail pages can keep loading background app bundles indefinitely.
                    # DOMContentLoaded from goto() is enough to parse the visible CRM fields.
                    text = detail_page.locator("body").inner_text(timeout=30000)
                    write_raw_capture(
                        conn,
                        source="hubspot",
                        capture_type="hubspot_contact_text",
                        content=text,
                        source_url=detail_page.url,
                        metadata={"contact_id": contact_id, "mode": "contacts-api"},
                        import_run_id=run_id,
                        extension="txt",
                        label=f"contact-{contact_id}",
                    )
                    detail_row = parse_contact_detail_text(contact_id, detail_page.url, text, spine_row)
                    rows_written += upsert_contact(conn, merge_contact_rows(spine_row, detail_row))
                    detail_count += 1
                except Exception as exc:
                    day_errors.append({"contact_id": contact_id, "error": str(exc)[:500]})
                    try:
                        detail_page.close()
                    except Exception:
                        pass
                    detail_page = context.new_page() if args.detail_limit != 0 else None
        conn.commit()
        print(
            f"HubSpot contacts-api {day}: rows_seen={rows_seen} rows_written={rows_written} detail_errors={len(day_errors)}",
            flush=True,
        )
        if day_errors:
            write_raw_capture(
                conn,
                source="hubspot",
                capture_type="hubspot_contact_detail_errors",
                content=day_errors,
                source_url=page.url,
                metadata={"date": day, "mode": "contacts-api"},
                import_run_id=run_id,
                extension="json",
                label=f"contact-detail-errors-{day}",
            )
            conn.commit()
    if detail_page:
        detail_page.close()
    return rows_seen, rows_written


def run_contacts_report_dry_run(context, page, args):
    goto_hubspot_url(page, args.url, args.auth_launch_url)
    body_text = page.locator("body").inner_text(timeout=30000)
    body_text, auth_required = maybe_wait_for_hubspot_auth(page, args, body_text)
    payload = {
        "dry_run": True,
        "mode": args.mode,
        "source_url": page.url,
        "limit": args.limit,
        "detail_limit": args.detail_limit,
        "max_bars": args.max_bars,
        "school": args.school,
        "db_match_enabled": args.dry_run_match_db,
        "auth_required": auth_required,
        "bars_processed": 0,
        "rows_seen": 0,
        "contacts": [],
        "errors": [],
    }
    if auth_required:
        payload["errors"].append("HubSpot authentication required; extractor reached the login page.")
        return payload

    visible_contact_rows, bars_processed = capture_visible_contact_rows_from_report(page, args.limit, args.max_bars)
    payload["bars_processed"] = bars_processed
    payload["rows_seen"] = len(visible_contact_rows)
    if not visible_contact_rows:
        payload["errors"].append("No HubSpot contact rows were parsed from the loaded report.")
        return payload

    detail_page = context.new_page() if args.detail_limit > 0 else None
    for index, (contact_id, link, spine_row) in enumerate(visible_contact_rows):
        entry = {
            "contact_id": contact_id,
            "contact_url": link["href"],
            "report_row": compact_row(spine_row),
            "detail_row": None,
            "merged_row": compact_row(dry_run_db_match(args, dict(spine_row))),
            "pike13_reconciliation": dry_run_reconcile_pike13(args, dict(spine_row)),
            "error": None,
        }
        if detail_page and index < args.detail_limit:
            try:
                detail_page.goto(link["href"], wait_until="domcontentloaded", timeout=60000)
                wait_until_ready(detail_page, networkidle_timeout=0)
                text = detail_page.locator("body").inner_text(timeout=30000)
                detail_row = parse_contact_detail_text(contact_id, detail_page.url, text, spine_row)
                merged_row = dry_run_db_match(args, merge_contact_rows(spine_row, detail_row))
                entry["contact_url"] = detail_page.url
                entry["detail_row"] = compact_row(detail_row)
                entry["merged_row"] = compact_row(merged_row)
                entry["pike13_reconciliation"] = dry_run_reconcile_pike13(args, merged_row)
            except Exception as exc:
                entry["error"] = str(exc)
                payload["errors"].append(f"{contact_id}: {exc}")
        payload["contacts"].append(entry)
    if detail_page:
        detail_page.close()
    return payload


def run_deals_dry_run(context, page, args):
    goto_hubspot_url(page, args.url, args.auth_launch_url)
    body_text = page.locator("body").inner_text(timeout=30000)
    body_text, auth_required = maybe_wait_for_hubspot_auth(page, args, body_text)
    payload = {
        "dry_run": True,
        "mode": args.mode,
        "source_url": page.url,
        "limit": args.limit,
        "detail_limit": args.detail_limit,
        "max_pages": args.max_pages,
        "school": args.school,
        "auth_required": auth_required,
        "pages_processed": 0,
        "rows_seen": 0,
        "deals": [],
        "errors": [],
    }
    if auth_required:
        payload["errors"].append("HubSpot authentication required; extractor reached the login page.")
        return payload

    visible_deal_rows, pages_processed = capture_visible_deal_rows_across_pages(page, args.limit, args.max_pages)
    payload["pages_processed"] = pages_processed
    deal_rows = filter_deal_rows_by_school(visible_deal_rows, args.school)
    payload["rows_seen"] = len(deal_rows)
    if not deal_rows:
        payload["errors"].append("No HubSpot deal rows were parsed from the loaded page.")
        return payload

    for index, (deal_id, link, spine_row) in enumerate(deal_rows):
        entry = {
            "deal_id": deal_id,
            "deal_url": link["href"],
            "list_row": compact_row(spine_row),
            "detail_row": None,
            "merged_row": compact_row(spine_row),
            "contact_row": None,
            "error": None,
        }
        if index < args.detail_limit:
            detail_page = context.new_page()
            try:
                detail_page.goto(link["href"], wait_until="domcontentloaded", timeout=60000)
                wait_until_ready(detail_page)
                text = detail_page.locator("body").inner_text(timeout=30000)
                detail_row = parse_deal_text(deal_id, detail_page.url, text)
                entry["deal_url"] = detail_page.url
                entry["detail_row"] = compact_row(detail_row)
                entry["merged_row"] = compact_row(merge_deal_rows(spine_row, detail_row))
                entry["contact_row"] = compact_row(parse_contact_from_text(deal_id, detail_page.url, text))
            except Exception as exc:
                entry["error"] = str(exc)
                payload["errors"].append(f"{deal_id}: {exc}")
            finally:
                detail_page.close()
        payload["deals"].append(entry)
    return payload


def run_dry_run(args):
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            args.profile_dir,
            headless=args.headless,
            viewport={"width": 1440, "height": 1000},
            accept_downloads=True,
        )
        try:
            page = context.pages[0] if context.pages else context.new_page()
            if args.mode == "contacts-report":
                payload = run_contacts_report_dry_run(context, page, args)
            else:
                payload = run_deals_dry_run(context, page, args)
        finally:
            context.close()
    output = write_json_output(payload, args.out)
    if output:
        print(f"HubSpot dry run wrote {output}")
    if payload["errors"]:
        raise RuntimeError("; ".join(payload["errors"]))
    return payload


def main():
    parser = argparse.ArgumentParser(description="Extract visible HubSpot lead/deal data into SQLite.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--profile-dir", default="browser_profiles/hubspot")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument(
        "--auth-launch-url",
        default=DEFAULT_HUBSPOT_AUTH_LAUNCH_URL,
        help="Optional SSO launch URL to visit before the requested HubSpot URL. Pass an empty string to disable.",
    )
    parser.add_argument(
        "--mode",
        choices=["contacts-report", "contacts-api", "deals"],
        default="contacts-report",
        help="Use the Contacts dashboard drilldown, the report API backfill path, or the legacy deals board extractor.",
    )
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--detail-limit", type=int, default=10)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--max-bars", type=int, default=35)
    parser.add_argument("--start-date", default=DEFAULT_INITIAL_LOAD_START)
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--api-page-size", type=int, default=100)
    parser.add_argument("--school", help="Optional school filter applied after visible rows are parsed.")
    parser.add_argument("--dry-run", action="store_true", help="Capture and parse rows without writing to the database.")
    parser.add_argument("--out", help="Write dry-run JSON to this path. Defaults to stdout.")
    parser.add_argument(
        "--dry-run-match-db",
        action="store_true",
        help="In dry-run mode, read the local DB to mark contacts that already match Pike13 people. No DB writes are made.",
    )
    parser.add_argument(
        "--pause-on-auth",
        action="store_true",
        help="In headed dry-run mode, pause on HubSpot login so the browser profile can be authenticated before parsing.",
    )
    args = parser.parse_args()
    if args.mode == "deals" and args.url == DEFAULT_CONTACT_REPORT_URL:
        args.url = DEFAULT_DEAL_URL
    if args.dry_run:
        run_dry_run(args)
        return

    conn = sqlite3.connect(args.db)
    ensure_lead_followup_schema(conn)
    run_id = start_import_run(conn, "hubspot", Path(__file__).name, args.start_date, None, {"url": args.url, "mode": args.mode})
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
            goto_hubspot_url(page, args.url, args.auth_launch_url)
            body_text = page.locator("body").inner_text(timeout=30000)
            auth_required = is_hubspot_auth_page(page.url, body_text)
            if args.mode == "contacts-api":
                if auth_required:
                    raise RuntimeError("HubSpot authentication required; extractor reached the login page.")
                rows_seen, rows_written = extract_contacts_api(conn, context, page, args, run_id)
            elif args.mode == "contacts-report":
                if auth_required:
                    visible_contact_rows = []
                    bars_processed = 0
                else:
                    visible_contact_rows, bars_processed = capture_visible_contact_rows_from_report(
                        page,
                        args.limit,
                        args.max_bars,
                    )
                write_raw_capture(
                    conn,
                    source="hubspot",
                    capture_type="hubspot_visible_contact_rows",
                    content=visible_contact_rows,
                    source_url=page.url,
                    metadata={"limit": args.limit, "school": args.school, "bars_processed": bars_processed},
                    import_run_id=run_id,
                    extension="json",
                    label="visible-contact-rows",
                )
                if auth_required:
                    raise RuntimeError("HubSpot authentication required; extractor reached the login page.")
                if not visible_contact_rows:
                    raise RuntimeError("No HubSpot contact rows were parsed from the loaded report.")
                detail_page = context.new_page() if args.detail_limit > 0 else None
                for index, (contact_id, link, spine_row) in enumerate(visible_contact_rows):
                    rows_seen += 1
                    rows_written += upsert_contact(conn, spine_row)
                    if detail_page and index < args.detail_limit:
                        detail_page.goto(link["href"], wait_until="domcontentloaded", timeout=60000)
                        wait_until_ready(detail_page, networkidle_timeout=0)
                        text = detail_page.locator("body").inner_text(timeout=30000)
                        write_raw_capture(
                            conn,
                            source="hubspot",
                            capture_type="hubspot_contact_text",
                            content=text,
                            source_url=detail_page.url,
                            metadata={"contact_id": contact_id, "school": args.school},
                            import_run_id=run_id,
                            extension="txt",
                            label=f"contact-{contact_id}",
                        )
                        detail_row = parse_contact_detail_text(contact_id, detail_page.url, text, spine_row)
                        rows_written += upsert_contact(conn, merge_contact_rows(spine_row, detail_row))
                if detail_page:
                    detail_page.close()
            else:
                if auth_required:
                    visible_deal_rows = []
                    pages_processed = 0
                else:
                    visible_deal_rows, pages_processed = capture_visible_deal_rows_across_pages(
                        page,
                        args.limit,
                        args.max_pages,
                    )
                write_raw_capture(
                    conn,
                    source="hubspot",
                    capture_type="hubspot_visible_deal_rows",
                    content=visible_deal_rows,
                    source_url=page.url,
                    metadata={"limit": args.limit, "school": args.school, "pages_processed": pages_processed},
                    import_run_id=run_id,
                    extension="json",
                    label="visible-deal-rows",
                )
                if auth_required:
                    raise RuntimeError("HubSpot authentication required; extractor reached the login page.")
                if not visible_deal_rows:
                    raise RuntimeError("No HubSpot deal rows were parsed from the loaded page.")
                deal_rows = filter_deal_rows_by_school(visible_deal_rows, args.school)
                for index, (deal_id, link, spine_row) in enumerate(deal_rows):
                    rows_seen += 1
                    upsert_deal(conn, spine_row)
                    rows_written += 1
                    if index < args.detail_limit:
                        detail_page = context.new_page()
                        detail_page.goto(link["href"], wait_until="domcontentloaded", timeout=60000)
                        wait_until_ready(detail_page)
                        text = detail_page.locator("body").inner_text(timeout=30000)
                        write_raw_capture(
                            conn,
                            source="hubspot",
                            capture_type="hubspot_deal_text",
                            content=text,
                            source_url=detail_page.url,
                            metadata={"deal_id": deal_id, "school": args.school},
                            import_run_id=run_id,
                            extension="txt",
                            label=f"deal-{deal_id}",
                        )
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
