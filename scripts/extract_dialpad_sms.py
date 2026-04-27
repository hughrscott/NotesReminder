#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import (  # noqa: E402
    DEFAULT_INITIAL_LOAD_START,
    ensure_lead_followup_schema,
    finish_import_run,
    normalize_phone,
    start_import_run,
    utc_now_iso,
)


DEFAULT_URL = "https://dialpad.com/app/history/messages"
PHONE_RE = re.compile(r"(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}")
FEED_RE = re.compile(r"(?:feed|conversation|thread|contact)[=/](\d+)")
MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
NAV_LABELS = {
    "all channels",
    "all",
    "calls",
    "coaching teams",
    "contact centers",
    "contacts",
    "departments",
    "dnd on",
    "download",
    "inbox",
    "meetings",
    "messages",
    "missed",
    "monitor all coaching teams",
    "recordings",
    "recents",
    "search",
    "search dialpad",
    "scheduled",
    "spam",
    "starred",
    "the power of dialpad. on your desktop.",
    "threads",
    "unread",
    "unread messages",
    "voicemails",
}
NOISE_PREFIXES = (
    "dialpad supports only one active app tab",
    "multiple tabs detected",
)


def stable_id(prefix, *parts):
    digest = hashlib.sha256("|".join(str(p or "") for p in parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:24]}"


def thread_id_from_url(url, fallback_text):
    match = FEED_RE.search(url)
    if match:
        return match.group(1)
    return stable_id("dialpad_thread", url, fallback_text[:300])


def message_id(thread_id, message_at, body, direction):
    return stable_id("dialpad_sms", thread_id, message_at, direction, body)


def normalize_dialpad_date(value, now=None):
    value = (value or "").strip().strip(",")
    if not value:
        return None
    now = now or datetime.now()
    lowered = value.lower()
    if lowered == "today":
        return now.date().isoformat()
    if lowered == "yesterday":
        return (now.date() - timedelta(days=1)).isoformat()
    slash_match = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", value)
    if slash_match:
        month, day, year = slash_match.groups()
        year = int(year)
        if year < 100:
            year += 2000
        return f"{year:04d}-{int(month):02d}-{int(day):02d}"
    month_match = re.search(
        r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})(?:,\s*(\d{4}))?",
        value,
        re.IGNORECASE,
    )
    if month_match:
        month_name, day, year = month_match.groups()
        year = int(year) if year else now.year
        parsed = datetime(year, MONTHS[month_name[:3].lower()], int(day)).date()
        if not month_match.group(3) and parsed > now.date():
            parsed = datetime(year - 1, MONTHS[month_name[:3].lower()], int(day)).date()
        return parsed.isoformat()
    return value if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) else None


def looks_like_dialpad_date(line):
    return normalize_dialpad_date(line) is not None


def clean_message_body(line):
    return re.sub(r"^(you|me|sent|outbound|from|inbound|received)\s*:\s*", "", line, flags=re.IGNORECASE).strip()


def infer_sms_direction(line):
    lowered = line.lower().strip()
    if lowered.startswith(("you:", "me:", "sent:", "outbound:")):
        return "outbound"
    if lowered.startswith(("from:", "inbound:", "received:")):
        return "inbound"
    if "sorry, i can" in lowered and "talk right now" in lowered:
        return "inbound"
    return "unknown"


def is_noise_message_line(line):
    lowered = line.lower().strip()
    return (
        lowered in NAV_LABELS
        or any(lowered.startswith(prefix) for prefix in NOISE_PREFIXES)
        or re.fullmatch(r"\d+", line) is not None
        or re.fullmatch(r"[A-Z]{1,3}", line) is not None
    )


def looks_like_message_body(line, default_direction):
    stripped = line.strip()
    if infer_sms_direction(line) != "unknown":
        return True
    cleaned = stripped.strip('"')
    if len(cleaned) >= 20:
        return True
    if default_direction != "unknown" and stripped.startswith('"') and stripped.endswith('"'):
        return True
    if default_direction != "unknown" and re.search(r"[.!?]", cleaned) and len(cleaned) >= 20:
        return True
    return False


def extract_message_lines(text, now=None, default_direction="unknown"):
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    messages = []
    current_date = None
    for line in lines:
        normalized_date = normalize_dialpad_date(line, now=now)
        if normalized_date:
            if messages and not messages[-1]["message_at"]:
                messages[-1]["message_at"] = normalized_date
                current_date = None
            else:
                current_date = normalized_date
            continue
        if len(line) < 2 or is_noise_message_line(line) or PHONE_RE.fullmatch(line):
            continue
        if not looks_like_message_body(line, default_direction):
            continue
        direction = infer_sms_direction(line)
        if direction == "unknown":
            direction = default_direction
        body = clean_message_body(line)
        body = body.strip('"').strip()
        messages.append({"message_at": current_date, "body": body, "direction": direction})
    return messages


def upsert_thread(conn, row):
    conn.execute(
        """
        INSERT INTO dialpad_sms_threads (
            thread_id, feed_id, phone, phone_normalized, contact_name, last_message_at,
            unread_count, school, department, source_url, raw_text, raw_json, updated_at
        )
        VALUES (
            :thread_id, :feed_id, :phone, :phone_normalized, :contact_name, :last_message_at,
            :unread_count, :school, :department, :source_url, :raw_text, :raw_json, :updated_at
        )
        ON CONFLICT(thread_id) DO UPDATE SET
            phone = COALESCE(excluded.phone, dialpad_sms_threads.phone),
            phone_normalized = COALESCE(excluded.phone_normalized, dialpad_sms_threads.phone_normalized),
            contact_name = COALESCE(excluded.contact_name, dialpad_sms_threads.contact_name),
            last_message_at = COALESCE(excluded.last_message_at, dialpad_sms_threads.last_message_at),
            unread_count = COALESCE(excluded.unread_count, dialpad_sms_threads.unread_count),
            school = COALESCE(excluded.school, dialpad_sms_threads.school),
            department = COALESCE(excluded.department, dialpad_sms_threads.department),
            source_url = COALESCE(excluded.source_url, dialpad_sms_threads.source_url),
            raw_text = excluded.raw_text,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )


def upsert_message(conn, row):
    conn.execute(
        """
        INSERT INTO dialpad_sms_messages (
            message_id, thread_id, message_at, direction, sender, recipient, body,
            source_url, raw_text, raw_json, updated_at
        )
        VALUES (
            :message_id, :thread_id, :message_at, :direction, :sender, :recipient, :body,
            :source_url, :raw_text, :raw_json, :updated_at
        )
        ON CONFLICT(message_id) DO UPDATE SET
            message_at = COALESCE(excluded.message_at, dialpad_sms_messages.message_at),
            direction = COALESCE(excluded.direction, dialpad_sms_messages.direction),
            body = excluded.body,
            source_url = COALESCE(excluded.source_url, dialpad_sms_messages.source_url),
            raw_text = excluded.raw_text,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )


def capture_thread_links(page, limit):
    links = page.locator("a").evaluate_all(
        """
        links => links.map(a => ({href: a.href, text: a.innerText || a.textContent || ''}))
                      .filter(a => /dialpad\\.com\\/app/.test(a.href) && a.text.trim().length > 0)
        """
    )
    seen = {}
    for link in links:
        key = thread_id_from_url(link["href"], link["text"])
        seen.setdefault(key, link)
    return list(seen.items())[:limit]


def parse_thread(page):
    text = page.locator("body").inner_text(timeout=30000)
    phone_match = PHONE_RE.search(text)
    thread_id = thread_id_from_url(page.url, text)
    default_direction = "inbound" if "/history/messages" in page.url else "unknown"
    messages = extract_message_lines(text, default_direction=default_direction)
    return {
        "thread": {
            "thread_id": thread_id,
            "feed_id": thread_id,
            "phone": phone_match.group(0) if phone_match else None,
            "phone_normalized": normalize_phone(phone_match.group(0)) if phone_match else None,
            "contact_name": None,
            "last_message_at": messages[-1]["message_at"] if messages else None,
            "unread_count": 1 if re.search(r"\bunread\b", text, re.IGNORECASE) else 0,
            "school": None,
            "department": None,
            "source_url": page.url,
            "raw_text": text,
            "raw_json": json.dumps({"extraction": "thread_text"}, sort_keys=True),
            "updated_at": utc_now_iso(),
        },
        "messages": messages,
    }


def delete_known_sms_noise(conn):
    conn.execute(
        """
        DELETE FROM dialpad_sms_messages
        WHERE LOWER(body) IN (
            'the power of dialpad. on your desktop.',
            'download',
            'multiple tabs detected.',
            'dnd on',
            'inbox',
            'contact centers',
            'all channels',
            'threads',
            'scheduled',
            'coaching teams',
            'monitor all coaching teams',
            'unread messages',
            'unread',
            'all',
            'calls',
            'missed',
            'meetings',
            'voicemails',
            'recordings',
            'messages',
            'starred',
            'spam'
        )
           OR LOWER(body) LIKE 'dialpad supports only one active app tab%'
        """
    )
    conn.execute(
        """
        DELETE FROM dialpad_sms_messages
        WHERE date(message_at) > date('now')
          AND json_extract(raw_json, '$.extraction') = 'message_line'
        """
    )
    conn.execute(
        """
        DELETE FROM dialpad_sms_messages
        WHERE direction = 'unknown'
          AND EXISTS (
              SELECT 1
              FROM dialpad_sms_messages better
              WHERE better.thread_id = dialpad_sms_messages.thread_id
                AND better.body = dialpad_sms_messages.body
                AND COALESCE(better.message_at, '') = COALESCE(dialpad_sms_messages.message_at, '')
                AND better.direction IN ('inbound', 'outbound')
          )
        """
    )
    conn.execute(
        """
        DELETE FROM dialpad_sms_messages
        WHERE direction = 'unknown'
          AND source_url LIKE '%/history/messages%'
          AND json_extract(raw_json, '$.extraction') = 'message_line'
        """
    )


def wait_until_ready(page, timeout=30000):
    page.wait_for_load_state("load", timeout=timeout)
    try:
        page.wait_for_load_state("networkidle", timeout=5000)
    except PlaywrightTimeoutError:
        pass


def main():
    parser = argparse.ArgumentParser(description="Extract visible Dialpad SMS threads into SQLite.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--profile-dir", default="browser_profiles/dialpad")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--thread-limit", type=int, default=20)
    parser.add_argument("--start-date", default=DEFAULT_INITIAL_LOAD_START)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    ensure_lead_followup_schema(conn)
    delete_known_sms_noise(conn)
    run_id = start_import_run(conn, "dialpad_sms", Path(__file__).name, args.start_date, None, {"url": args.url})
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
            page.goto(args.url, wait_until="domcontentloaded", timeout=60000)
            wait_until_ready(page)
            thread_links = capture_thread_links(page, args.thread_limit)
            urls = [link["href"] for _, link in thread_links] or [page.url]
            for url in urls[: args.thread_limit]:
                rows_seen += 1
                thread_page = context.new_page()
                thread_page.goto(url, wait_until="domcontentloaded", timeout=60000)
                wait_until_ready(thread_page)
                parsed = parse_thread(thread_page)
                upsert_thread(conn, parsed["thread"])
                rows_written += 1
                for message in parsed["messages"]:
                    row = {
                        "message_id": message_id(parsed["thread"]["thread_id"], message["message_at"], message["body"], message["direction"]),
                        "thread_id": parsed["thread"]["thread_id"],
                        "message_at": message["message_at"],
                        "direction": message["direction"],
                        "sender": None,
                        "recipient": None,
                        "body": message["body"],
                        "source_url": thread_page.url,
                        "raw_text": message["body"],
                        "raw_json": json.dumps({"extraction": "message_line"}, sort_keys=True),
                        "updated_at": utc_now_iso(),
                    }
                    upsert_message(conn, row)
                    rows_written += 1
                thread_page.close()
            context.close()
        finish_import_run(conn, run_id, "success", rows_seen, rows_written, 0)
        conn.commit()
    except Exception as exc:
        finish_import_run(conn, run_id, "error", rows_seen, rows_written, 0, str(exc))
        conn.commit()
        raise
    finally:
        conn.close()

    print(f"Dialpad SMS extraction complete: rows_seen={rows_seen} rows_written={rows_written}")


if __name__ == "__main__":
    main()
