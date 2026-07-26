#!/usr/bin/env python3
import argparse
import json
import os
import re
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from date_window_lead_load import validate_target_db, validate_window  # noqa: E402
from scripts.okta_auth import (  # noqa: E402
    is_okta_login_url,
    okta_credentials_available,
    run_okta_mfa_with_gate,
)
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
    SCHOOL_MAILBOXES,
    classify_direction,
    external_email_for_message,
    gmail_query,
    normalize_email_list,
    parse_gmail_datetime,
    school_for_mailbox,
    stable_email_id,
)


DEFAULT_PROFILE = "browser_profiles/sor_okta"
DEFAULT_DB = "outputs/lead_intelligence/lead_intelligence_working.db"

load_dotenv(ROOT / ".env")


def gmail_search_url(query):
    return "https://mail.google.com/mail/u/0/#search/" + quote(query, safe="")


def wait_for_gmail(page, interactive_login=False, login_timeout=300):
    lowered_url = page.url.lower()
    if "accounts.google.com" in lowered_url or "signin" in lowered_url or is_okta_login_url(page.url):
        if not interactive_login:
            raise RuntimeError(f"Gmail profile is not authenticated; final_url={page.url}")
        if is_okta_login_url(page.url) and okta_credentials_available():
            # Telegram-gated Okta MFA: ask Hugh first, then submit + wait for push
            run_okta_mfa_with_gate(page, "School Email (Gmail)", login_timeout)
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
    validate_window(args.start_date, args.end_date)
    db_path = validate_target_db(args.db, allow_production=args.allow_production_db)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
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
                        rows = visible_message_rows(page, args.limit_per_query)
                        processed_ids = set()
                        total_this_query = 0
                        empty_pages = 0
                        max_pages = 50
                        page_num = 0
                        while rows and page_num < max_pages:
                            page_num += 1
                            new_rows = []
                            for r in rows:
                                key = r.get("legacy_message_id") or r.get("legacy_thread_id") or r.get("text", "")[:80]
                                if key not in processed_ids:
                                    processed_ids.add(key)
                                    new_rows.append(r)
                            if not new_rows:
                                empty_pages += 1
                                if empty_pages >= 2:
                                    print(f"No new rows for {empty_pages} consecutive pages; stopping pagination.", flush=True)
                                    break
                            else:
                                empty_pages = 0
                            print(f"Page {page_num}: {len(rows)} visible, {len(new_rows)} new (total so far: {total_this_query})", flush=True)
                            metadata["queries"].append({"mailbox": mailbox, "direction": direction, "rows": len(rows), "new_rows": len(new_rows), "page": page_num})
                            for row_meta in new_rows:
                                rows_seen += 1
                                total_this_query += 1
                                row = page.locator("tr.zA").nth(row_meta["index"])
                                try:
                                    row.click(timeout=15000)
                                    page.wait_for_load_state("domcontentloaded", timeout=15000)
                                    page.wait_for_timeout(1500)
                                except PlaywrightTimeoutError:
                                    print(f"[warn] mail view did not settle for row {row_meta['index']}; continuing", flush=True)
                                try:
                                    message_text = page.locator("body").inner_text(timeout=args.query_timeout * 1000)
                                except PlaywrightTimeoutError:
                                    message_text = ""
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
                                        "page": page_num,
                                    },
                                    import_run_id=run_id,
                                    extension="txt",
                                    label=f"{mailbox}-{direction}-{rows_seen}",
                                )
                                parsed = parse_open_message(page, row_meta, mailbox, direction)
                                if parsed.get("message_id"):
                                    upsert_school_email_message(conn, parsed)
                                    rows_written += 1
                                else:
                                    print(f"[warn] skipped row {row_meta['index']} — no message_id", flush=True)
                                # Commit every 10 messages so kills don't lose everything
                                if rows_written % 10 == 0:
                                    conn.commit()
                                try:
                                    page.goto(url, wait_until="domcontentloaded", timeout=args.query_timeout * 1000)
                                    page.wait_for_selector("tr.zA", timeout=args.query_timeout * 1000)
                                except Exception as nav_exc:
                                    print(f"[error] nav failed: {nav_exc!r}; retrying goto", flush=True)
                                    try:
                                        page.goto(url, wait_until="domcontentloaded", timeout=args.query_timeout * 1000)
                                    except Exception:
                                        pass
                            # Scroll to load more results
                            if page_num < max_pages and empty_pages < 2:
                                try:
                                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                    page.wait_for_timeout(3000)
                                    rows = visible_message_rows(page, args.limit_per_query)
                                except Exception:
                                    print("[warn] scroll failed; trying Older link", flush=True)
                                    try:
                                        page.locator("a:has-text('Older'), div[aria-label='Older']").first.click(timeout=5000)
                                        page.wait_for_load_state("domcontentloaded", timeout=15000)
                                        page.wait_for_selector("tr.zA", timeout=15000)
                                        rows = visible_message_rows(page, args.limit_per_query)
                                    except Exception:
                                        rows = []
                        print(f"Query complete for {mailbox}/{direction}: {total_this_query} new emails across {page_num} pages", flush=True)
                        metadata["queries"].append({"mailbox": mailbox, "direction": direction, "total_new": total_this_query, "pages": page_num})
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
