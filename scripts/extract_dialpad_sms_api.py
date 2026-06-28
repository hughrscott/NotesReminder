#!/usr/bin/env python3
import argparse
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit
from urllib.parse import urlencode

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import ensure_lead_followup_schema, finish_import_run, normalize_phone, start_import_run, utc_now_iso  # noqa: E402
from scripts.extract_dialpad_sms import normalize_department, select_department_messages, upsert_message, upsert_thread  # noqa: E402
from scripts.extract_dialpad_voice import is_dialpad_app_page, wait_for_authenticated_page, wait_until_ready  # noqa: E402


DEFAULT_URL = "https://dialpad.com/app/history/new"
CONTACT_FILTERS = ("recent", "unread", "all")


def stable_id(prefix, *parts):
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:24]}"


def millis_to_iso(value):
    if value is None:
        return None
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    return datetime.fromtimestamp(numeric / 1000, tz=timezone.utc).isoformat()


def first_phone(contact, row):
    candidates = [
        contact.get("primary_phone"),
        contact.get("display_primary_phone"),
        row.get("from_phone"),
        row.get("to_phone"),
    ]
    for value in candidates:
        normalized = normalize_phone(value)
        if normalized:
            return value, normalized
    for item in contact.get("phones") or []:
        if isinstance(item, dict):
            value = item.get("phone") or item.get("number") or item.get("value")
        else:
            value = item
        normalized = normalize_phone(value)
        if normalized:
            return value, normalized
    return None, None


def sms_direction(row, target_key):
    value = (row.get("direction") or "").lower()
    if value in {"inbound", "outbound"}:
        return value
    orientation = (row.get("orientation") or "").lower()
    if orientation in {"external", "inbound"}:
        return "inbound"
    if row.get("delivery_result") or row.get("delivery_result_code") or orientation in {"internal", "outbound"}:
        return "outbound"
    if row.get("sender_key") and target_key and row.get("sender_key") == target_key:
        return "outbound"
    return "unknown"


def contact_thread_id(target_key, contact_key):
    return stable_id("dialpad_sms_thread", target_key, contact_key)


def feed_row_to_sms_rows(row, contact, target_key, school, department):
    if row.get("feed_type") != "TextMessage":
        return None, None
    contact_key = row.get("contact_key") or contact.get("contact_key") or contact.get("key")
    if not contact_key:
        return None, None
    message_at = millis_to_iso(row.get("date") or row.get("feed_date") or row.get("date_modified"))
    if not message_at:
        return None, None
    phone, phone_normalized = first_phone(contact, row)
    thread_id = contact_thread_id(target_key, contact_key)
    direction = sms_direction(row, target_key)
    now = utc_now_iso()
    thread = {
        "thread_id": thread_id,
        "feed_id": thread_id,
        "phone": phone,
        "phone_normalized": phone_normalized,
        "contact_name": None,
        "last_message_at": message_at,
        "unread_count": int(contact.get("unread") or 0) if str(contact.get("unread") or "0").isdigit() else 0,
        "school": school,
        "department": department,
        "source_url": "https://dialpad.com/api/feed",
        "raw_text": "",
        "raw_json": json.dumps(
            {
                "extraction": "dialpad_sms_api",
                "raw_customer_content_redacted": True,
                "target_hash": stable_id("target", target_key),
            },
            sort_keys=True,
        ),
        "updated_at": now,
    }
    message = {
        "message_id": stable_id("dialpad_sms_api", target_key, contact_key, row.get("message_id") or row.get("id") or row.get("feed_key"), message_at),
        "thread_id": thread_id,
        "message_at": message_at,
        "direction": direction,
        "sender": None,
        "recipient": None,
        "body": "[redacted Dialpad SMS API message]",
        "source_url": "https://dialpad.com/api/feed",
        "raw_text": "",
        "raw_json": json.dumps(
            {
                "delivery_method": row.get("delivery_method"),
                "delivery_result": row.get("delivery_result"),
                "extraction": "dialpad_sms_api",
                "feed_type": row.get("feed_type"),
                "orientation": row.get("orientation"),
                "raw_body_redacted": True,
            },
            sort_keys=True,
        ),
        "updated_at": now,
    }
    return thread, message


def api_get_json(context, url, auth_header, timeout_ms=60000):
    response = context.request.get(url, headers={"authorization": auth_header}, timeout=timeout_ms)
    if response.status >= 400:
        raise RuntimeError(f"Dialpad API request failed status={response.status} path={urlsplit(url).path}")
    return response.json()


def extract_target_key_from_url(url):
    query = dict(parse_qsl(urlsplit(url).query))
    return query.get("target_key")


def resolve_department_target_key(page, department):
    target_keys = []

    def on_request(request):
        if "dialpad.com/api/" not in request.url or "target_key=" not in request.url:
            return
        key = extract_target_key_from_url(request.url)
        if key and key not in target_keys:
            target_keys.append(key)

    page.on("request", on_request)
    try:
        select_department_messages(page, department)
        page.wait_for_timeout(3000)
    finally:
        page.remove_listener("request", on_request)
    if not target_keys:
        raise RuntimeError(f"Could not resolve Dialpad target_key for department {department}.")
    return target_keys[-1]


def collect_auth_header(page):
    headers = []

    def on_request(request):
        if "dialpad.com/api/" in request.url and request.headers.get("authorization"):
            headers.append(request.headers["authorization"])

    page.on("request", on_request)
    return headers, on_request


def fetch_contacts(context, target_key, auth_header, per_filter_limit, timeout_ms=60000):
    contacts = {}
    for filter_name in CONTACT_FILTERS:
        url = contact_list_url(target_key, filter_name, per_filter_limit)
        rows = api_get_json(context, url, auth_header, timeout_ms=timeout_ms)
        if not isinstance(rows, list):
            continue
        for row in rows:
            key = row.get("contact_key") or row.get("key")
            if key:
                contacts.setdefault(key, row)
    return list(contacts.values())


def contact_list_url(target_key, filter_name, limit):
    return "https://dialpad.com/api/contact/?" + urlencode(
        {"filter": filter_name, "target_key": target_key, "limit": limit}
    )


def contact_search_url(target_key, query, limit):
    return "https://dialpad.com/api/contact/?" + urlencode(
        {"filter": "all", "target_key": target_key, "limit": limit, "search": query}
    )


def fetch_contacts_by_search(context, target_key, auth_header, targets, limit, timeout_ms=60000):
    contacts = {}
    for target in targets:
        query = str(target or "").strip()
        if not query:
            continue
        rows = api_get_json(
            context,
            contact_search_url(target_key, query, limit),
            auth_header,
            timeout_ms=timeout_ms,
        )
        if not isinstance(rows, list):
            continue
        for row in rows:
            key = row.get("contact_key") or row.get("key")
            if key:
                contacts.setdefault(key, row)
    return list(contacts.values())


def fetch_feed_rows(context, target_key, contact_key, auth_header, limit, timeout_ms=60000):
    url = (
        "https://dialpad.com/api/feed/"
        f"?target_key={target_key}&contact_key={contact_key}&limit={limit}&support_link_media=true"
    )
    rows = api_get_json(context, url, auth_header, timeout_ms=timeout_ms)
    return rows if isinstance(rows, list) else []


def run(args):
    department = normalize_department(args.department) or normalize_department(args.school)
    if not department:
        raise RuntimeError("--department or --school is required for the Dialpad SMS API extractor.")
    school = args.school or ("The Heights" if department == "HEIGHTS" else "West U" if department == "WESTU" else department)
    conn = sqlite3.connect(args.db)
    ensure_lead_followup_schema(conn)
    run_id = start_import_run(
        conn,
        "dialpad_sms",
        Path(__file__).name,
        args.start_date,
        None,
        {
            "department": department,
            "school": school,
            "mode": "api_search" if args.target_phone else "api",
            "target_phone_count": len(args.target_phone or []),
        },
    )
    conn.commit()
    rows_seen = rows_written = 0
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                args.profile_dir,
                headless=args.headless and not args.interactive_login,
                viewport={"width": 1440, "height": 1000},
            )
            page = context.pages[0] if context.pages else context.new_page()
            auth_headers, auth_listener = collect_auth_header(page)
            page.goto(args.url, wait_until="domcontentloaded", timeout=60000)
            wait_until_ready(page)
            wait_for_authenticated_page(page, args.url, args.interactive_login, args.login_timeout)
            if not is_dialpad_app_page(page.url, page.locator("body").inner_text(timeout=10000)):
                raise RuntimeError(f"Dialpad profile is not authenticated; final_url={page.url}")
            target_key = resolve_department_target_key(page, department)
            if not auth_headers:
                page.wait_for_timeout(2000)
            if not auth_headers:
                raise RuntimeError("Could not capture Dialpad API authorization header.")
            auth_header = auth_headers[-1]
            page.remove_listener("request", auth_listener)
            request_timeout_ms = args.request_timeout * 1000
            if args.target_phone:
                contacts = fetch_contacts_by_search(
                    context,
                    target_key,
                    auth_header,
                    args.target_phone,
                    args.search_limit,
                    timeout_ms=request_timeout_ms,
                )
            else:
                contacts = fetch_contacts(context, target_key, auth_header, args.contact_limit, timeout_ms=request_timeout_ms)
            print(f"Resolved {len(contacts)} Dialpad contacts for department={department}", flush=True)
            for index, contact in enumerate(contacts, start=1):
                contact_key = contact.get("contact_key") or contact.get("key")
                if not contact_key:
                    continue
                if index == 1 or index % 50 == 0 or index == len(contacts):
                    print(
                        f"Fetching Dialpad SMS feed {index}/{len(contacts)} "
                        f"department={department} rows_seen={rows_seen}",
                        flush=True,
                    )
                feed_rows = fetch_feed_rows(
                    context,
                    target_key,
                    contact_key,
                    auth_header,
                    args.feed_limit,
                    timeout_ms=request_timeout_ms,
                )
                for row in feed_rows:
                    if row.get("feed_type") != "TextMessage":
                        continue
                    rows_seen += 1
                    thread, message = feed_row_to_sms_rows(row, contact, target_key, school, department)
                    if not thread or not message:
                        continue
                    before = conn.total_changes
                    upsert_thread(conn, thread)
                    upsert_message(conn, message)
                    rows_written += conn.total_changes - before
                conn.commit()
            context.close()
        finish_import_run(conn, run_id, "success", rows_seen, rows_written, 0)
        conn.commit()
    except KeyboardInterrupt:
        finish_import_run(conn, run_id, "error", rows_seen, rows_written, 0, "Interrupted Dialpad SMS API extraction.")
        conn.commit()
        raise
    except Exception as exc:
        finish_import_run(conn, run_id, "error", rows_seen, rows_written, 0, str(exc))
        conn.commit()
        raise
    finally:
        conn.close()
    print(f"Dialpad SMS API extraction complete: rows_seen={rows_seen} rows_written={rows_written}")


def main():
    parser = argparse.ArgumentParser(description="Extract Dialpad SMS from authenticated department feed APIs.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--profile-dir", default="browser_profiles/dialpad")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--school")
    parser.add_argument("--department")
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--contact-limit", type=int, default=100)
    parser.add_argument("--target-phone", action="append", default=[], help="Phone number to search directly in Dialpad contacts. Repeatable.")
    parser.add_argument("--search-limit", type=int, default=10, help="Max contacts returned for each --target-phone search.")
    parser.add_argument("--feed-limit", type=int, default=50)
    parser.add_argument("--request-timeout", type=int, default=60, help="Seconds to wait for each Dialpad API request.")
    parser.add_argument("--login-timeout", type=int, default=300)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--interactive-login", action="store_true")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
