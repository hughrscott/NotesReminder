#!/usr/bin/env python3
import argparse
import json
import os
import re
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from date_window_lead_load import validate_target_db, validate_window  # noqa: E402
from lead_followup_schema import (  # noqa: E402
    ensure_lead_followup_schema,
    finish_import_run,
    normalize_email,
    start_import_run,
    upsert_school_email_message,
    utc_now_iso,
)
from notesreminder.lib.raw_capture import write_raw_capture  # noqa: E402
from school_email import (  # noqa: E402
    DEFAULT_TIMEZONE,
    SCHOOL_MAILBOXES,
    classify_direction,
    external_email_for_message,
    gmail_query,
    is_school_email,
    normalize_email_list,
    parse_gmail_datetime,
    school_for_mailbox,
    stable_email_id,
)


DEFAULT_PROFILE = "browser_profiles/sor_okta"
DEFAULT_DB = "outputs/lead_intelligence/lead_intelligence_working.db"
SYSTEM_EMAIL_DOMAINS = {
    "dialpad.com",
    "email.amazonses.com",
    "hubspot.com",
    "jumbula.com",
    "paydici.com",
    "pciapply.com",
    "pike13.com",
    "schoolofrock.com",
}
NON_CUSTOMER_SMARTLABELS = {
    "^smartlabel_group",
    "^smartlabel_notification",
    "^smartlabel_promo",
    "^smartlabel_social",
}

load_dotenv(ROOT / ".env")


def gmail_search_url(query):
    return "https://mail.google.com/mail/u/0/#search/" + quote(query, safe="")


def okta_credentials_available():
    return bool(okta_username() and okta_password())


def okta_username():
    return os.getenv("OKTA_USERNAME") or os.getenv("SOR_OKTA_USERNAME") or os.getenv("OKTA_USER")


def okta_password():
    return os.getenv("OKTA_PASSWORD") or os.getenv("SOR_OKTA_PASSWORD")


def is_okta_login_url(url):
    lowered = (url or "").lower()
    return "sor.okta.com" in lowered and ("login" in lowered or "signin" in lowered)


def fill_okta_login(page):
    username = okta_username()
    password = okta_password()
    if not username or not password:
        return False
    username_input = page.locator('input[name="username"], input#okta-signin-username, input[type="text"]').first
    password_input = page.locator('input[name="password"], input#okta-signin-password, input[type="password"]').first
    username_input.wait_for(timeout=15000)
    username_input.fill(username)
    password_input.fill(password)
    remember_me = page.locator('input[type="checkbox"][name="remember"], input[type="checkbox"]')
    if remember_me.count():
        try:
            if not remember_me.first.is_checked():
                remember_me.first.check(timeout=3000)
        except Exception:
            pass
    page.get_by_role("button", name=re.compile(r"sign in", re.IGNORECASE)).click(timeout=10000)
    return True


def wait_for_okta_push(page, timeout_seconds):
    deadline = time.time() + timeout_seconds
    notified = False
    while time.time() < deadline:
        lowered_url = page.url.lower()
        try:
            body = page.locator("body").inner_text(timeout=5000).lower()
        except PlaywrightTimeoutError:
            body = ""
        if "mail.google.com" in lowered_url and "signin" not in lowered_url:
            return
        if not notified and ("push sent" in body or "okta verify" in body):
            print("Okta Verify push sent by NotesReminder. Please approve it on your phone.", flush=True)
            notified = True
        time.sleep(2)
    raise RuntimeError("Timed out waiting for Okta Verify approval.")


def wait_for_gmail(page, interactive_login=False, login_timeout=300):
    lowered_url = page.url.lower()
    if "accounts.google.com" in lowered_url or "signin" in lowered_url or is_okta_login_url(page.url):
        if not interactive_login:
            raise RuntimeError(f"Gmail profile is not authenticated; final_url={page.url}")
        if is_okta_login_url(page.url) and okta_credentials_available():
            print("Filling Okta username/password from environment. Approve only the Okta Verify push you expect from NotesReminder.", flush=True)
            fill_okta_login(page)
            wait_for_okta_push(page, login_timeout)
        else:
            print("Complete Google/Okta login in the opened browser, then press Enter here.")
            input()
        page.wait_for_load_state("domcontentloaded", timeout=login_timeout * 1000)


def visible_message_rows(page, limit):
    try:
        page.wait_for_selector("tr.zA", timeout=20000)
    except PlaywrightTimeoutError:
        return []
    rows = page.locator("tr.zA")
    count = min(rows.count(), limit)
    result = []
    for index in range(count):
        row = rows.nth(index)
        try:
            result.append(
                {
                    "index": index,
                    "text": row.inner_text(timeout=5000),
                    "legacy_message_id": row.get_attribute("data-legacy-message-id"),
                    "legacy_thread_id": row.get_attribute("data-legacy-thread-id"),
                }
            )
        except Exception:
            continue
    return result


def current_result_range(page):
    try:
        return page.evaluate(
            """
            () => {
              const candidates = Array.from(document.querySelectorAll('[role="button"], div, span'))
                .map(element => (element.innerText || element.textContent || '').trim().replace(/\\s+/g, ' '))
                .filter(text => /^\\d+[–-]\\d+ of /.test(text));
              return candidates[0] || '';
            }
            """
        )
    except Exception:
        return ""


def first_result_signature(page):
    try:
        return page.evaluate(
            """
            () => {
              const row = document.querySelector('tr.zA');
              if (!row) {
                return '';
              }
              return row.getAttribute('data-legacy-message-id') ||
                row.getAttribute('data-legacy-thread-id') ||
                (row.innerText || row.textContent || '').trim().replace(/\\s+/g, ' ').slice(0, 160);
            }
            """
        )
    except Exception:
        return ""


def go_to_next_results_page(page, timeout_ms):
    before_range = current_result_range(page)
    before_first = first_result_signature(page)
    next_button = page.locator('[role="button"][aria-label="Next results"]')
    if next_button.count() < 1:
        return False
    try:
        next_button.first.click(timeout=timeout_ms)
    except PlaywrightTimeoutError:
        return False
    deadline = time.time() + (timeout_ms / 1000)
    while time.time() < deadline:
        page.wait_for_timeout(500)
        after_range = current_result_range(page)
        after_first = first_result_signature(page)
        if after_range and (after_range != before_range or after_first != before_first):
            return True
    return False


def collect_gmail_sync_responses(page):
    responses = []

    def on_response(response):
        url = response.url
        if "mail.google.com/sync/u/0/i/" not in url:
            return
        if "/bv?" not in url and "/fd?" not in url:
            return
        try:
            body = response.text()
        except Exception as exc:
            responses.append({"url": url, "kind": "error", "error": type(exc).__name__})
            return
        responses.append(
            {
                "url": url,
                "kind": "bv" if "/bv?" in url else "fd",
                "body": body,
            }
        )

    page.on("response", on_response)
    return responses, on_response


def iter_gmail_message_lists(value):
    if isinstance(value, list):
        if value and isinstance(value[0], str) and value[0].startswith("msg-f:"):
            yield value
        for item in value:
            yield from iter_gmail_message_lists(item)


def participant_emails(value):
    emails = []
    if isinstance(value, list):
        if len(value) >= 2 and isinstance(value[0], int) and isinstance(value[1], str) and "@" in value[1]:
            normalized = normalize_email(value[1])
            if normalized and normalized not in emails:
                emails.append(normalized)
        for item in value:
            for email in participant_emails(item):
                if email not in emails:
                    emails.append(email)
    return emails


def is_system_email(email):
    normalized = normalize_email(email) or ""
    domain = normalized.rsplit("@", 1)[-1]
    return any(domain == item or domain.endswith(f".{item}") for item in SYSTEM_EMAIL_DOMAINS)


def external_lead_email(candidates):
    for email in normalize_email_list(candidates):
        if not is_school_email(email) and not is_system_email(email):
            return email
    return None


def iter_large_ints(value):
    if isinstance(value, int):
        if value > 1_000_000_000_000:
            yield value
    elif isinstance(value, list):
        for item in value:
            yield from iter_large_ints(item)


def collect_smartlabels(value):
    labels = set()
    if isinstance(value, str) and value.startswith("^smartlabel_"):
        labels.add(value)
    elif isinstance(value, list):
        for item in value:
            labels.update(collect_smartlabels(item))
    return labels


def millis_to_iso(value):
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    return datetime.fromtimestamp(numeric / 1000, tz=timezone.utc).astimezone(DEFAULT_TIMEZONE).isoformat()


def pick_message_timestamp(message_list, start_date, end_date):
    values = sorted(set(iter_large_ints(message_list)))
    if not values:
        return None
    start = datetime.combine(date.fromisoformat(start_date) - timedelta(days=1), datetime.min.time(), tzinfo=DEFAULT_TIMEZONE)
    end = datetime.combine(date.fromisoformat(end_date) + timedelta(days=2), datetime.min.time(), tzinfo=DEFAULT_TIMEZONE)
    for value in values:
        parsed = datetime.fromtimestamp(value / 1000, tz=timezone.utc).astimezone(DEFAULT_TIMEZONE)
        if start <= parsed < end:
            return parsed.isoformat()
    return millis_to_iso(values[0])


def parse_sync_message(message_list, school_mailbox, start_date, end_date, source_url):
    if len(message_list) < 2 or not isinstance(message_list[1], list):
        return None
    message_id = message_list[0]
    details = message_list[1]
    mailbox = normalize_email(school_mailbox)
    from_emails = participant_emails(details[0]) if len(details) > 0 else []
    to_emails = participant_emails(details[3]) if len(details) > 3 else []
    smartlabels = collect_smartlabels(message_list)
    if smartlabels & NON_CUSTOMER_SMARTLABELS:
        return None
    direction = classify_direction(from_emails, to_emails, mailbox)
    if direction == "outbound":
        external_email = external_lead_email(to_emails)
    elif direction == "inbound":
        external_email = external_lead_email(from_emails)
    else:
        return None
    if not external_email:
        return None
    message_at = pick_message_timestamp(message_list, start_date, end_date)
    return {
        "message_id": message_id,
        "thread_id": None,
        "school_mailbox": mailbox,
        "school": school_for_mailbox(mailbox),
        "direction": direction,
        "message_at": message_at,
        "from_email": from_emails[0] if from_emails else None,
        "from_email_normalized": from_emails[0] if from_emails else None,
        "to_emails": json.dumps(to_emails, sort_keys=True),
        "to_emails_normalized": json.dumps(to_emails, sort_keys=True),
        "cc_emails": json.dumps([], sort_keys=True),
        "cc_emails_normalized": json.dumps([], sort_keys=True),
        "external_email_normalized": external_email,
        "subject": "[redacted Gmail sync subject]",
        "snippet": "[redacted Gmail sync snippet]",
        "body": "[redacted Gmail sync body]",
        "source_url": source_url,
        "raw_text": "",
        "raw_json": json.dumps(
            {
                "extraction": "gmail_browser_sync",
                "raw_body_redacted": True,
                "raw_subject_redacted": True,
            },
            sort_keys=True,
        ),
        "updated_at": utc_now_iso(),
    }


def parse_gmail_sync_response(body, school_mailbox, start_date, end_date, source_url):
    try:
        payload = json.loads(body)
    except (TypeError, json.JSONDecodeError):
        return []
    rows = []
    seen = set()
    for message_list in iter_gmail_message_lists(payload):
        row = parse_sync_message(message_list, school_mailbox, start_date, end_date, source_url)
        if not row or row["message_id"] in seen:
            continue
        seen.add(row["message_id"])
        rows.append(row)
    return rows


def open_message_row(page, row_meta, timeout_ms):
    clicked = page.evaluate(
        """
        ({messageId, threadId, index}) => {
          const rows = Array.from(document.querySelectorAll("tr.zA"));
          const row =
            rows.find((candidate) => messageId && candidate.getAttribute("data-legacy-message-id") === messageId) ||
            rows.find((candidate) => threadId && candidate.getAttribute("data-legacy-thread-id") === threadId) ||
            rows[index];
          if (!row) {
            return false;
          }
          row.scrollIntoView({block: "center", inline: "nearest"});
          const target = row.querySelector('a[href], td[role="link"], div[role="link"]') || row;
          target.click();
          return true;
        }
        """,
        {
            "messageId": row_meta.get("legacy_message_id"),
            "threadId": row_meta.get("legacy_thread_id"),
            "index": row_meta.get("index"),
        },
    )
    if not clicked:
        page.locator("tr.zA").nth(row_meta["index"]).click(timeout=timeout_ms)
    try:
        page.wait_for_function(
            "() => !document.querySelector('tr.zA') || document.querySelector('h2')",
            timeout=timeout_ms,
        )
    except PlaywrightTimeoutError:
        pass


def parse_open_message(page, row_meta, school_mailbox, forced_direction, now_year=None):
    data = page.evaluate(
        """
        () => {
          const text = document.body ? document.body.innerText : "";
          const mailtos = Array.from(document.querySelectorAll('a[href^="mailto:"]')).map(a => a.href.replace(/^mailto:/, '').split('?')[0]);
          const attrEmails = Array.from(document.querySelectorAll('[email], [data-hovercard-id]')).flatMap(e => [e.getAttribute('email'), e.getAttribute('data-hovercard-id')]).filter(Boolean);
          const subject = document.querySelector('h2')?.innerText || document.querySelector('[data-thread-perm-id] h2')?.innerText || "";
          const dates = Array.from(document.querySelectorAll('span.g3, span[title]')).map(e => e.getAttribute('title') || e.innerText).filter(Boolean);
          const message = document.querySelector('[data-legacy-message-id]');
          const thread = document.querySelector('[data-legacy-thread-id]');
          return {
            text,
            mailtos,
            attrEmails,
            subject,
            dates,
            messageId: message ? message.getAttribute('data-legacy-message-id') : null,
            threadId: thread ? thread.getAttribute('data-legacy-thread-id') : null,
            url: location.href
          };
        }
        """
    )
    raw_text = data.get("text") or row_meta.get("text") or ""
    emails = normalize_email_list((data.get("attrEmails") or []) + (data.get("mailtos") or []) + [raw_text])
    mailbox = normalize_email(school_mailbox)
    from_email = mailbox if forced_direction == "outbound" else None
    to_emails = [mailbox] if forced_direction == "inbound" else []
    if forced_direction == "inbound":
        from_email = next((email for email in emails if email != mailbox), None)
    else:
        to_emails = [email for email in emails if email != mailbox]
    external_email = external_email_for_message(from_email, to_emails)
    direction = classify_direction(from_email, to_emails, mailbox) if from_email or to_emails else forced_direction
    message_at = None
    for date_text in reversed(data.get("dates") or []):
        message_at = parse_gmail_datetime(date_text, now_year=now_year)
        if message_at:
            break
    subject = clean_subject(extract_subject(raw_text) or data.get("subject") or row_meta.get("text") or "")
    message_id = data.get("messageId") or row_meta.get("legacy_message_id")
    thread_id = data.get("threadId") or row_meta.get("legacy_thread_id")
    if not message_id:
        message_id = stable_email_id(mailbox, direction, message_at, subject, external_email, data.get("url"), raw_text)
    return {
        "message_id": message_id,
        "thread_id": thread_id,
        "school_mailbox": mailbox,
        "school": school_for_mailbox(mailbox),
        "direction": direction,
        "message_at": message_at,
        "from_email": from_email,
        "from_email_normalized": normalize_email(from_email),
        "to_emails": json.dumps(to_emails, sort_keys=True),
        "to_emails_normalized": json.dumps(normalize_email_list(to_emails), sort_keys=True),
        "cc_emails": json.dumps([], sort_keys=True),
        "cc_emails_normalized": json.dumps([], sort_keys=True),
        "external_email_normalized": external_email,
        "subject": subject,
        "snippet": clean_snippet(row_meta.get("text") or raw_text),
        "body": raw_text,
        "source_url": data.get("url"),
        "raw_text": raw_text,
        "raw_json": json.dumps(
            {
                "extraction": "gmail_browser",
                "forced_direction": forced_direction,
                "mailto_count": len(emails),
                "row_index": row_meta.get("index"),
            },
            sort_keys=True,
        ),
        "updated_at": utc_now_iso(),
    }


def clean_subject(value):
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:240]


def extract_subject(raw_text):
    lines = [line.strip() for line in str(raw_text or "").splitlines() if line.strip()]
    for index, line in enumerate(lines):
        if line == "In new window" and index + 1 < len(lines):
            return lines[index + 1]
    for line in lines:
        if line.lower().startswith(("re:", "fwd:", "fw:")):
            return line
    return ""


def clean_snippet(value):
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:500]


def run_extraction(args):
    if args.mode == "sync":
        return run_sync_extraction(args)

    validate_window(args.start_date, args.end_date)
    db_path = validate_target_db(args.db, allow_production=args.allow_production_db)
    conn = sqlite3.connect(db_path)
    ensure_lead_followup_schema(conn)
    run_id = start_import_run(
        conn,
        "school_email",
        Path(__file__).name,
        args.start_date,
        args.end_date,
        {"mailboxes": args.mailbox, "profile_dir": args.profile_dir},
    )
    conn.commit()
    rows_seen = rows_written = 0
    metadata = {"queries": []}
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                str(Path(args.profile_dir)),
                headless=args.headless and not args.interactive_login,
                viewport={"width": 1440, "height": 1000},
            )
            try:
                page = context.pages[0] if context.pages else context.new_page()
                page.set_default_timeout(args.query_timeout * 1000)
                page.set_default_navigation_timeout(args.query_timeout * 1000)
                for mailbox in args.mailbox:
                    for direction in ("inbound", "outbound"):
                        query = gmail_query(mailbox, direction, args.start_date, args.end_date, args.query_term)
                        url = gmail_search_url(query)
                        print(f"Searching Gmail mailbox={mailbox} direction={direction}", flush=True)
                        page.goto(url, wait_until="domcontentloaded", timeout=args.query_timeout * 1000)
                        wait_for_gmail(page, args.interactive_login, args.login_timeout)
                        try:
                            page.wait_for_load_state("networkidle", timeout=min(args.query_timeout, 15) * 1000)
                        except PlaywrightTimeoutError:
                            pass
                        query_rows = 0
                        query_pages = 0
                        seen_message_ids = set()
                        while query_pages < args.max_pages:
                            query_pages += 1
                            rows = visible_message_rows(page, args.limit_per_query)
                            page_range = current_result_range(page)
                            print(
                                f"Found visible Gmail rows: {len(rows)} mailbox={mailbox} "
                                f"direction={direction} page={query_pages} range={page_range}",
                                flush=True,
                            )
                            for row_meta in rows:
                                message_key = row_meta.get("legacy_message_id") or row_meta.get("legacy_thread_id") or f"page{query_pages}:{row_meta.get('index')}"
                                if message_key in seen_message_ids:
                                    continue
                                seen_message_ids.add(message_key)
                                rows_seen += 1
                                query_rows += 1
                                print(
                                    f"Opening Gmail row {rows_seen} mailbox={mailbox} direction={direction} "
                                    f"page={query_pages} index={row_meta['index']}",
                                    flush=True,
                                )
                                open_message_row(page, row_meta, min(args.query_timeout, 10) * 1000)
                                try:
                                    page.wait_for_load_state("networkidle", timeout=10000)
                                except PlaywrightTimeoutError:
                                    pass
                                message_text = page.locator("body").inner_text(timeout=args.query_timeout * 1000)
                                write_raw_capture(
                                    conn,
                                    source="school_email",
                                    capture_type="school_email_message_text",
                                    content=message_text,
                                    source_url=page.url,
                                    metadata={
                                        "mailbox": mailbox,
                                        "direction": direction,
                                        "query": query,
                                        "row_index": row_meta["index"],
                                        "page": query_pages,
                                        "range": page_range,
                                    },
                                    import_run_id=run_id,
                                    extension="txt",
                                    label=f"{mailbox}-{direction}-{rows_seen}",
                                )
                                parsed = parse_open_message(page, row_meta, mailbox, direction)
                                upsert_school_email_message(conn, parsed)
                                rows_written += 1
                                conn.commit()
                                page.go_back(wait_until="domcontentloaded", timeout=30000)
                                try:
                                    page.wait_for_selector("tr.zA", timeout=10000)
                                except PlaywrightTimeoutError:
                                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                                    for _ in range(1, query_pages):
                                        if not go_to_next_results_page(page, min(args.query_timeout, 15) * 1000):
                                            break
                            if not rows:
                                break
                            if not go_to_next_results_page(page, min(args.query_timeout, 15) * 1000):
                                break
                        metadata["queries"].append(
                            {
                                "mailbox": mailbox,
                                "direction": direction,
                                "rows": query_rows,
                                "pages": query_pages,
                            }
                        )
            finally:
                context.close()
        finish_import_run(conn, run_id, "success", rows_seen, rows_written, 0, metadata=metadata)
        conn.commit()
    except Exception as exc:
        finish_import_run(conn, run_id, "error", rows_seen, rows_written, 0, str(exc)[:240], metadata=metadata)
        conn.commit()
        raise
    finally:
        conn.close()
    return rows_seen, rows_written


def run_sync_extraction(args):
    validate_window(args.start_date, args.end_date)
    db_path = validate_target_db(args.db, allow_production=args.allow_production_db)
    conn = sqlite3.connect(db_path)
    ensure_lead_followup_schema(conn)
    run_id = start_import_run(
        conn,
        "school_email",
        Path(__file__).name,
        args.start_date,
        args.end_date,
        {"mailboxes": args.mailbox, "profile_dir": args.profile_dir, "mode": "sync"},
    )
    conn.commit()
    rows_seen = rows_written = 0
    metadata = {"queries": []}
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                str(Path(args.profile_dir)),
                headless=args.headless and not args.interactive_login,
                viewport={"width": 1440, "height": 1000},
            )
            try:
                page = context.pages[0] if context.pages else context.new_page()
                page.set_default_timeout(args.query_timeout * 1000)
                page.set_default_navigation_timeout(args.query_timeout * 1000)
                sync_responses, sync_listener = collect_gmail_sync_responses(page)
                try:
                    for mailbox in args.mailbox:
                        for query_direction in ("inbound", "outbound"):
                            query = gmail_query(mailbox, query_direction, args.start_date, args.end_date, args.query_term)
                            url = gmail_search_url(query)
                            print(f"Searching Gmail sync mailbox={mailbox} query_direction={query_direction}", flush=True)
                            sync_responses.clear()
                            page.goto(url, wait_until="domcontentloaded", timeout=args.query_timeout * 1000)
                            wait_for_gmail(page, args.interactive_login, args.login_timeout)
                            try:
                                page.wait_for_selector("tr.zA", timeout=min(args.query_timeout, 20) * 1000)
                            except PlaywrightTimeoutError:
                                pass
                            page.wait_for_timeout(5000)
                            query_rows = 0
                            query_pages = 0
                            seen_message_ids = set()
                            while query_pages < args.max_pages:
                                query_pages += 1
                                page_range = current_result_range(page)
                                page_rows = 0
                                for response in list(sync_responses):
                                    if response.get("kind") != "fd":
                                        continue
                                    for parsed in parse_gmail_sync_response(
                                        response.get("body"),
                                        mailbox,
                                        args.start_date,
                                        args.end_date,
                                        page.url,
                                    ):
                                        message_id = parsed["message_id"]
                                        if message_id in seen_message_ids:
                                            continue
                                        seen_message_ids.add(message_id)
                                        rows_seen += 1
                                        query_rows += 1
                                        page_rows += 1
                                        upsert_school_email_message(conn, parsed)
                                        rows_written += 1
                                conn.commit()
                                print(
                                    f"Parsed Gmail sync rows: {page_rows} mailbox={mailbox} "
                                    f"query_direction={query_direction} page={query_pages} range={page_range}",
                                    flush=True,
                                )
                                sync_responses.clear()
                                if not go_to_next_results_page(page, min(args.query_timeout, 15) * 1000):
                                    break
                                page.wait_for_timeout(5000)
                            metadata["queries"].append(
                                {
                                    "mailbox": mailbox,
                                    "direction": query_direction,
                                    "rows": query_rows,
                                    "pages": query_pages,
                                }
                            )
                finally:
                    page.remove_listener("response", sync_listener)
            finally:
                context.close()
        finish_import_run(conn, run_id, "success", rows_seen, rows_written, 0, metadata=metadata)
        conn.commit()
    except Exception as exc:
        finish_import_run(conn, run_id, "error", rows_seen, rows_written, 0, str(exc)[:240], metadata=metadata)
        conn.commit()
        raise
    finally:
        conn.close()
    return rows_seen, rows_written


def main():
    parser = argparse.ArgumentParser(description="Extract school Gmail lead emails into the local lead working DB.")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--profile-dir", default=DEFAULT_PROFILE)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--mailbox", action="append", choices=sorted(SCHOOL_MAILBOXES), default=[])
    parser.add_argument("--limit-per-query", type=int, default=50)
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--mode", choices=["visible", "sync"], default="visible")
    parser.add_argument("--query-timeout", type=int, default=45)
    parser.add_argument("--query-term", default="")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--interactive-login", action="store_true")
    parser.add_argument("--login-timeout", type=int, default=300)
    parser.add_argument(
        "--allow-production-db",
        action="store_true",
        help="Allow this shadow-mode email refresh to target the canonical reminders.db after the Phase 7 single-DB promotion.",
    )
    args = parser.parse_args()
    if not args.mailbox:
        args.mailbox = sorted(SCHOOL_MAILBOXES)
    rows_seen, rows_written = run_extraction(args)
    print(f"School email extraction complete: rows_seen={rows_seen} rows_written={rows_written}")


if __name__ == "__main__":
    main()
