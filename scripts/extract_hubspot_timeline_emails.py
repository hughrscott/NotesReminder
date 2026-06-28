#!/usr/bin/env python3
import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import (  # noqa: E402
    ensure_lead_followup_schema,
    finish_import_run,
    normalize_email,
    start_import_run,
    upsert_school_email_message,
    utc_now_iso,
)
from scripts.extract_hubspot_leads import (  # noqa: E402
    DEFAULT_HUBSPOT_AUTH_LAUNCH_URL,
    HUBSPOT_PORTAL_ID,
    hubspot_request_headers,
)


DEFAULT_PROFILE = "browser_profiles/sor_okta"
DEFAULT_SCHOOL_MAILBOX = "hubspot-timeline@schoolofrock.com"


def millis_to_iso(value):
    try:
        millis = int(value)
    except (TypeError, ValueError):
        return None
    if millis <= 0:
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).isoformat()


def school_alias_sql(school):
    value = (school or "").strip().lower()
    if not value:
        return "1=1", {}
    if "height" in value:
        aliases = ["%height%"]
    elif "west" in value:
        aliases = ["%west%", "%west university%"]
    else:
        aliases = [f"%{value}%"]
    params = {f"school_{index}": alias for index, alias in enumerate(aliases)}
    predicate = " OR ".join(f"LOWER(COALESCE(school, '')) LIKE :{key}" for key in params)
    return f"({predicate})", params


def contact_rows(conn, start_date, end_date, school=None, limit=0):
    school_sql, school_params = school_alias_sql(school)
    limit_sql = "LIMIT :limit" if limit else ""
    params = {"start": start_date, "end": end_date, **school_params}
    if limit:
        params["limit"] = limit
    return conn.execute(
        f"""
        SELECT contact_id, email_normalized, school
        FROM hubspot_contacts
        WHERE COALESCE(contact_id, '') != ''
          AND COALESCE(email_normalized, '') != ''
          AND date(substr(create_date, 1, 10)) BETWEEN date(:start) AND date(:end)
          AND {school_sql}
        ORDER BY date(substr(create_date, 1, 10)), contact_id
        {limit_sql}
        """,
        params,
    ).fetchall()


def fetch_timeline(page, headers, contact_id, limit):
    url = (
        f"https://app.hubspot.com/api/timeline/v2/object/0-1/{contact_id}"
        f"?limit={limit}&renderingRequested=true&hs_static_app=crm-records-ui"
        f"&hs_static_app_version=1.83273&portalId={HUBSPOT_PORTAL_ID}&clienttimeout=10000"
    )
    response = page.request.get(url, headers=headers, timeout=60000)
    if response.status >= 400:
        return response.status, {}
    return response.status, response.json()


def hubspot_email_events(payload, contact, start_date, end_date):
    rows = []
    contact_email = normalize_email(contact["email_normalized"])
    if not contact_email:
        return rows
    start = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()
    for event in (payload or {}).get("events") or []:
        if event.get("etype") != "eventEmailSend":
            continue
        event_data = event.get("eventData") or {}
        message = event_data.get("messageId") or {}
        recipient = normalize_email(event_data.get("recipient") or message.get("to"))
        if not recipient:
            continue
        event_at = millis_to_iso(event.get("timestamp") or event_data.get("created") or message.get("created"))
        if not event_at:
            continue
        event_day = datetime.fromisoformat(event_at).date()
        if not (start <= event_day <= end):
            continue
        # Keep this source tied to the lead spine. Other recipients can be explored later
        # but should not inflate lead-email coverage.
        if recipient != contact_email:
            continue
        event_id = str(event_data.get("id") or event.get("id") or message.get("id") or "").strip()
        if not event_id:
            event_id = f"{contact['contact_id']}:{event.get('timestamp')}"
        row_id = f"hubspot_timeline_email_{contact['contact_id']}_{event_id}"
        rows.append(
            {
                "message_id": row_id,
                "thread_id": None,
                "school_mailbox": DEFAULT_SCHOOL_MAILBOX,
                "school": contact["school"],
                "direction": "outbound",
                "message_at": event_at,
                "from_email": DEFAULT_SCHOOL_MAILBOX,
                "from_email_normalized": DEFAULT_SCHOOL_MAILBOX,
                "to_emails": json.dumps([recipient], sort_keys=True),
                "to_emails_normalized": json.dumps([recipient], sort_keys=True),
                "cc_emails": json.dumps([], sort_keys=True),
                "cc_emails_normalized": json.dumps([], sort_keys=True),
                "external_email_normalized": recipient,
                "subject": "[redacted HubSpot timeline email subject]",
                "snippet": "[redacted HubSpot timeline email snippet]",
                "body": "[redacted HubSpot timeline email body]",
                "source_url": f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/record/0-1/{contact['contact_id']}",
                "raw_text": "",
                "raw_json": json.dumps(
                    {
                        "extraction": "hubspot_timeline_email",
                        "event_type": event.get("etype"),
                        "raw_subject_redacted": True,
                        "raw_body_redacted": True,
                    },
                    sort_keys=True,
                ),
                "updated_at": utc_now_iso(),
            }
        )
    return rows


def run(args):
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    ensure_lead_followup_schema(conn)
    run_id = start_import_run(
        conn,
        "hubspot_timeline_email",
        Path(__file__).name,
        args.start_date,
        args.end_date,
        {"school": args.school, "profile_dir": args.profile_dir, "timeline_limit": args.timeline_limit},
    )
    conn.commit()
    rows_seen = rows_written = rows_updated = 0
    metadata = {"status_counts": {}, "contacts_with_email_events": 0}
    try:
        contacts = contact_rows(conn, args.start_date, args.end_date, args.school, args.limit)
        with sync_playwright() as playwright:
            context = playwright.chromium.launch_persistent_context(
                str(Path(args.profile_dir)),
                headless=args.headless,
                viewport={"width": 1440, "height": 1100},
            )
            try:
                page = context.pages[0] if context.pages else context.new_page()
                page.goto(args.auth_launch_url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(4000)
                headers = hubspot_request_headers(context)
                for index, contact in enumerate(contacts, start=1):
                    status, payload = fetch_timeline(page, headers, contact["contact_id"], args.timeline_limit)
                    metadata["status_counts"][str(status)] = metadata["status_counts"].get(str(status), 0) + 1
                    if status != 200:
                        continue
                    rows = hubspot_email_events(payload, contact, args.start_date, args.end_date)
                    if rows:
                        metadata["contacts_with_email_events"] += 1
                    for row in rows:
                        before = conn.total_changes
                        upsert_school_email_message(conn, row)
                        changed = conn.total_changes - before
                        rows_seen += 1
                        if changed:
                            rows_written += 1
                    if index % 25 == 0:
                        conn.commit()
                        print(
                            f"HubSpot timeline email contacts={index}/{len(contacts)} "
                            f"events_seen={rows_seen} rows_written={rows_written}",
                            flush=True,
                        )
                    time.sleep(args.sleep_seconds)
            finally:
                context.close()
        conn.commit()
        finish_import_run(conn, run_id, "success", len(contacts), rows_written, rows_updated, metadata=metadata)
        conn.commit()
        return {"contacts": len(contacts), "events_seen": rows_seen, "rows_written": rows_written, "metadata": metadata}
    except Exception as exc:
        finish_import_run(conn, run_id, "error", rows_seen, rows_written, rows_updated, str(exc)[:240], metadata=metadata)
        conn.commit()
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Backfill sanitized HubSpot timeline email events into communication evidence.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--profile-dir", default=DEFAULT_PROFILE)
    parser.add_argument("--auth-launch-url", default=DEFAULT_HUBSPOT_AUTH_LAUNCH_URL)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--school")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--timeline-limit", type=int, default=100)
    parser.add_argument("--sleep-seconds", type=float, default=0.05)
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
