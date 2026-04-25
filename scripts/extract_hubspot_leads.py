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


def stable_id(prefix, *parts):
    digest = hashlib.sha256("|".join(str(p or "") for p in parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:24]}"


def text_after(label, text):
    pattern = re.compile(rf"{re.escape(label)}\s+([^\n]+)", re.IGNORECASE)
    match = pattern.search(text)
    return match.group(1).strip() if match else None


def parse_deal_text(deal_id, url, text):
    pike13_match = PIKE13_PERSON_RE.search(text)
    row = {
        "deal_id": deal_id,
        "deal_name": text_after("Deal name", text) or text_after("Name", text),
        "stage": text_after("Deal Stage", text) or text_after("Stage", text),
        "pipeline": text_after("Pipeline", text),
        "owner": text_after("Deal owner", text) or text_after("Owner", text),
        "school": text_after("School Name - Deal", text) or text_after("School", text),
        "create_date": text_after("Create Date", text) or text_after("Create date", text),
        "last_activity_date": text_after("Last Activity Date", text),
        "last_contacted": text_after("Last Contacted", text),
        "follow_up_needed": text_after("Follow Up Needed", text),
        "trial_date": text_after("Trial Date", text) or text_after("Trial Date (Deal)", text),
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
    found = [field for field in required if row.get(field)]
    missing = [field for field in required if not row.get(field)]
    row["raw_json"] = json.dumps(
        {
            "extraction": "deal_detail_text",
            "fields_found": found,
            "fields_missing": missing,
            "source_url": url,
        },
        sort_keys=True,
    )
    return row


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
            deal_links = capture_visible_deal_links(page, args.limit)
            for index, (deal_id, link) in enumerate(deal_links):
                rows_seen += 1
                if index < args.detail_limit:
                    detail_page = context.new_page()
                    detail_page.goto(link["href"], wait_until="domcontentloaded", timeout=60000)
                    wait_until_ready(detail_page)
                    text = detail_page.locator("body").inner_text(timeout=30000)
                    row = parse_deal_text(deal_id, detail_page.url, text)
                    upsert_deal(conn, row)
                    rows_written += 1
                    rows_written += upsert_contact_from_text(conn, deal_id, detail_page.url, text)
                    detail_page.close()
                else:
                    upsert_deal(
                        conn,
                        {
                            "deal_id": deal_id,
                            "deal_name": link.get("text") or None,
                            "stage": None,
                            "pipeline": None,
                            "owner": None,
                            "school": None,
                            "create_date": None,
                            "last_activity_date": None,
                            "last_contacted": None,
                            "follow_up_needed": None,
                            "trial_date": None,
                            "trial_no_show": None,
                            "date_entered_scheduled_trial_stage": None,
                            "area_of_interest": None,
                            "instrument_type": None,
                            "lead_source": None,
                            "marketing_source": None,
                            "pike13_person_id": None,
                            "source_url": link["href"],
                            "raw_text": link.get("text") or "",
                            "raw_json": json.dumps({"extraction": "visible_link"}, sort_keys=True),
                            "updated_at": utc_now_iso(),
                        },
                    )
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

    print(f"HubSpot extraction complete: rows_seen={rows_seen} rows_written={rows_written}")


if __name__ == "__main__":
    main()
