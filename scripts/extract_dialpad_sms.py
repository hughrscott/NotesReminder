#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import sqlite3
import sys
import time
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
WEEKDAYS = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
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
LOGIN_LABELS = {
    "log in",
    "log in with google",
    "log in with microsoft",
    "work email",
    "password",
    "not a customer? sign up here",
}
DEPARTMENT_LABELS = {
    "WESTU": ("West U", "WESTU"),
    "WEST U": ("West U", "WESTU"),
    "WEST UNIVERSITY": ("West University Place", "WESTU"),
    "HEIGHTS": ("The Heights", "HEIGHTS"),
}
SCHOOL_DEPARTMENTS = {
    "west u": "WESTU",
    "west university": "WESTU",
    "west university place": "WESTU",
    "westu": "WESTU",
    "westu-sor": "WESTU",
    "the heights": "HEIGHTS",
    "heights": "HEIGHTS",
    "theheights": "HEIGHTS",
    "theheights-sor": "HEIGHTS",
}


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
    if lowered in WEEKDAYS:
        days_back = (now.weekday() - WEEKDAYS[lowered]) % 7
        return (now.date() - timedelta(days=days_back)).isoformat()
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


def detect_department(text):
    upper = (text or "").upper()
    for label, value in DEPARTMENT_LABELS.items():
        if re.search(rf"\b{re.escape(label)}\b", upper):
            return value
    return (None, None)


def normalize_department(value):
    value = (value or "").strip()
    if not value:
        return None
    direct = DEPARTMENT_LABELS.get(value.upper())
    if direct:
        return direct[1]
    return SCHOOL_DEPARTMENTS.get(value.lower())


def department_school(department):
    if not department:
        return (None, None)
    return DEPARTMENT_LABELS.get(department.upper(), (None, None))


def sms_extraction_source(url):
    if "/history/messages" in url:
        return "message_list"
    if re.search(r"/(?:feed|conversation|thread|contact|profile|messages?)[=/]", url):
        return "thread_detail"
    return "fallback_visible_text"


def is_noise_message_line(line):
    lowered = line.lower().strip()
    return (
        lowered in NAV_LABELS
        or lowered in LOGIN_LABELS
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


def is_login_page(url, text):
    lowered = (text or "").lower()
    return "/login" in (url or "") or (
        "log in to dialpad" in lowered and "work email" in lowered and "password" in lowered
    )


def is_dialpad_app_page(url, text):
    lowered = (text or "").lower()
    return "dialpad.com/app/" in (url or "") and any(
        token in lowered for token in ("search dialpad", "departments", "messages", "calls", "voicemails")
    )


def extract_message_lines(text, now=None, default_direction="unknown", date_follows_message=True):
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    messages = []
    current_date = None
    for line in lines:
        normalized_date = normalize_dialpad_date(line, now=now)
        if normalized_date:
            if date_follows_message and messages and not messages[-1]["message_at"]:
                messages[-1]["message_at"] = normalized_date
                messages[-1]["timestamp_source"] = "visible_date"
                current_date = None
            else:
                current_date = normalized_date
            continue
        if len(line) < 2 or is_noise_message_line(line) or PHONE_RE.fullmatch(line):
            continue
        if not looks_like_message_body(line, default_direction):
            continue
        direction = infer_sms_direction(line)
        direction_source = "observed"
        if direction == "unknown":
            direction = default_direction
            direction_source = "inferred" if default_direction != "unknown" else "unknown"
        body = clean_message_body(line)
        body = body.strip('"').strip()
        messages.append(
            {
                "message_at": current_date,
                "body": body,
                "direction": direction,
                "direction_source": direction_source,
                "timestamp_source": "visible_date" if current_date else "missing",
            }
        )
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


def parse_thread(page, expected_department=None):
    text = page.locator("body").inner_text(timeout=30000)
    if is_login_page(page.url, text):
        raise RuntimeError("Dialpad profile is not authenticated; landed on login page.")
    phone_match = PHONE_RE.search(text)
    thread_id = thread_id_from_url(page.url, text)
    extraction_source = sms_extraction_source(page.url)
    default_direction = "inbound" if extraction_source == "message_list" else "unknown"
    messages = extract_message_lines(
        text,
        default_direction=default_direction,
        date_follows_message=extraction_source == "message_list",
    )
    forced_department = normalize_department(expected_department)
    school, department = department_school(forced_department) if forced_department else detect_department(text)
    return {
        "thread": {
            "thread_id": thread_id,
            "feed_id": thread_id,
            "phone": phone_match.group(0) if phone_match else None,
            "phone_normalized": normalize_phone(phone_match.group(0)) if phone_match else None,
            "contact_name": None,
            "last_message_at": messages[-1]["message_at"] if messages else None,
            "unread_count": 1 if re.search(r"\bunread\b", text, re.IGNORECASE) else 0,
            "school": school,
            "department": department,
            "source_url": page.url,
            "raw_text": text,
            "raw_json": json.dumps(
                {
                    "extraction": "thread_text",
                    "extraction_source": extraction_source,
                    "default_direction": default_direction,
                    "department_detected": department,
                    "department_forced": bool(forced_department),
                    "timestamp_policy": "source visible dates normalized to most recent non-future date",
                },
                sort_keys=True,
            ),
            "updated_at": utc_now_iso(),
        },
        "messages": messages,
        "extraction_source": extraction_source,
    }


def select_department_messages(page, department):
    department = normalize_department(department)
    if not department:
        return None
    department_link = page.locator(f"[data-qa='leftbar-links-cc-{department}']")
    if department_link.count() != 1:
        raise RuntimeError(f"Dialpad department selector not found or ambiguous for {department}.")
    department_link.click(timeout=10000)
    page.wait_for_timeout(1500)
    messages_tab = page.locator(".d-tab, button.d-tab, [role='tab']").filter(has_text="Messages")
    if messages_tab.count() == 0:
        raise RuntimeError(f"Dialpad Messages tab not found after selecting department {department}.")
    for index in range(messages_tab.count()):
        try:
            messages_tab.nth(index).click(timeout=10000)
            page.wait_for_timeout(2000)
            if visible_message_list_rows(page):
                break
            selected_text = page.locator(".d-tab--selected, [aria-selected='true']").inner_text(timeout=1000)
            if "messages" in selected_text.lower():
                break
        except PlaywrightTimeoutError:
            continue
    if not visible_message_list_rows(page):
        selected_tab = ""
        try:
            selected_tab = page.locator(".d-tab--selected, [aria-selected='true']").inner_text(timeout=1000)
        except Exception:
            pass
        raise RuntimeError(
            f"Dialpad Messages tab did not load message rows for {department}; selected_tab={selected_tab!r}."
        )
    selected = page.locator(f"[data-qa='leftbar-links-cc-{department}'][data-qa-selected='true']")
    if selected.count() != 1:
        raise RuntimeError(f"Dialpad department {department} was not selected after click.")
    try:
        page.locator("[data-qa='contact-row']").first.wait_for(state="visible", timeout=10000)
    except PlaywrightTimeoutError:
        # Some departments can legitimately have no visible rows in a narrow filter. Let the caller
        # decide whether zero rows is acceptable for that run.
        pass
    return department


def upsert_parsed_messages(conn, parsed, source_url, include_unknown_direction=False):
    rows_written = 1
    upsert_thread(conn, parsed["thread"])
    for message in parsed["messages"]:
        if not message["message_at"]:
            continue
        if message["direction"] == "unknown" and not include_unknown_direction:
            continue
        row = {
            "message_id": message_id(parsed["thread"]["thread_id"], message["message_at"], message["body"], message["direction"]),
            "thread_id": parsed["thread"]["thread_id"],
            "message_at": message["message_at"],
            "direction": message["direction"],
            "sender": None,
            "recipient": None,
            "body": message["body"],
            "source_url": source_url,
            "raw_text": message["body"],
            "raw_json": json.dumps(
                {
                    "extraction": "message_line",
                    "extraction_source": parsed["extraction_source"],
                    "direction_source": message["direction_source"],
                    "timestamp_source": message["timestamp_source"],
                    "source_timestamp_field": "message_at",
                    "import_timestamp_field": "updated_at",
                },
                sort_keys=True,
            ),
            "updated_at": utc_now_iso(),
        }
        upsert_message(conn, row)
        rows_written += 1
    return rows_written


def message_list_row_to_records(row, school, department, source_url, now=None):
    contact = (row.get("contact") or "").strip()
    snippet = clean_message_body(row.get("snippet") or "")
    date_text = (row.get("date_text") or "").strip()
    if not contact or not snippet or not date_text:
        return None, None
    message_at = normalize_dialpad_date(date_text, now=now)
    if not message_at:
        return None, None
    phone = None
    phone_normalized = None
    phone_match = PHONE_RE.search(contact)
    if phone_match:
        phone = phone_match.group(0)
        phone_normalized = normalize_phone(phone)
    thread_id = stable_id("dialpad_sms_web_thread", department, contact)
    raw_json = json.dumps(
        {
            "extraction": "message_list_row",
            "raw_body_redacted": True,
            "date_text": date_text,
            "operator_present": bool(row.get("operator")),
            "row_text_hash": stable_id("dialpad_sms_web_row", row.get("row_text")),
        },
        sort_keys=True,
    )
    thread = {
        "thread_id": thread_id,
        "feed_id": thread_id,
        "phone": phone,
        "phone_normalized": phone_normalized,
        "contact_name": None if phone_normalized else contact,
        "last_message_at": message_at,
        "unread_count": 1 if row.get("unread") else 0,
        "school": school,
        "department": department,
        "source_url": source_url,
        "raw_text": "",
        "raw_json": raw_json,
        "updated_at": utc_now_iso(),
    }
    message = {
        "message_id": stable_id("dialpad_sms_web_list", department, contact, message_at, snippet),
        "thread_id": thread_id,
        "message_at": message_at,
        "direction": "unknown",
        "sender": None,
        "recipient": None,
        "body": "[redacted Dialpad SMS web list snippet]",
        "source_url": source_url,
        "raw_text": "",
        "raw_json": raw_json,
        "updated_at": utc_now_iso(),
    }
    return thread, message


def visible_message_list_rows(page):
    return page.evaluate(
        """
        () => Array.from(document.querySelectorAll('tr.inbox-aff-row')).map((row) => {
          const text = (selector) => {
            const node = row.querySelector(selector);
            return node ? (node.innerText || node.textContent || '').trim() : '';
          };
          return {
            contact: text('.inbox-aff-name'),
            snippet: text('.inbox-aff-description'),
            operator: text('.inbox-aff-operator'),
            date_text: text('.inbox-aff-date'),
            unread: row.classList.contains('unread'),
            row_text: (row.innerText || row.textContent || '').trim(),
          };
        }).filter((row) => row.contact && row.snippet && row.date_text)
        """
    )


def scroll_message_list(page):
    return page.evaluate(
        """
        () => {
          const candidates = Array.from(document.querySelectorAll('*')).filter((el) => {
            const style = getComputedStyle(el);
            return style.overflowY === 'auto' && el.scrollHeight > el.clientHeight + 20;
          });
          const scroller = candidates.find((el) => el.querySelector && el.querySelector('tr.inbox-aff-row'));
          if (!scroller) {
            return {scrolled: false};
          }
          const before = scroller.scrollTop;
          scroller.scrollTop = Math.min(scroller.scrollTop + Math.max(scroller.clientHeight, 500), scroller.scrollHeight);
          return {
            scrolled: scroller.scrollTop !== before,
            scrollTop: scroller.scrollTop,
            scrollHeight: scroller.scrollHeight,
            clientHeight: scroller.clientHeight,
          };
        }
        """
    )


def extract_visible_message_list_rows(conn, page, thread_limit, school, department):
    rows_seen = rows_written = 0
    seen = set()
    stagnant_scrolls = 0
    while rows_seen < thread_limit and stagnant_scrolls < 3:
        batch = visible_message_list_rows(page)
        before_seen = len(seen)
        for row in batch:
            key = stable_id("dialpad_sms_web_seen", department, row.get("contact"), row.get("date_text"), row.get("snippet"))
            if key in seen:
                continue
            seen.add(key)
            thread, message = message_list_row_to_records(row, school, department, page.url)
            if not thread or not message:
                continue
            rows_seen += 1
            upsert_thread(conn, thread)
            upsert_message(conn, message)
            rows_written += 2
            if rows_seen >= thread_limit:
                break
        if rows_seen >= thread_limit:
            break
        scroll_state = scroll_message_list(page)
        page.wait_for_timeout(1000)
        if len(seen) == before_seen and not scroll_state.get("scrolled"):
            stagnant_scrolls += 1
        elif len(seen) == before_seen and scroll_state.get("scrollTop") + scroll_state.get("clientHeight", 0) >= scroll_state.get("scrollHeight", 0):
            stagnant_scrolls += 1
        else:
            stagnant_scrolls = 0
    return rows_seen, rows_written


def extract_selected_department_rows(conn, page, thread_limit, department, base_url=None):
    rows_seen = rows_written = 0
    department = normalize_department(department)
    school, _ = department_school(department)
    visible_rows = visible_message_list_rows(page)
    if visible_rows:
        return extract_visible_message_list_rows(conn, page, thread_limit, school, department)
    index = 0
    while rows_seen < thread_limit:
        contact_rows = page.locator("[data-qa='contact-row']")
        if index >= contact_rows.count():
            break
        try:
            row = contact_rows.nth(index)
            row.scroll_into_view_if_needed(timeout=5000)
            row.click(timeout=10000)
        except PlaywrightTimeoutError:
            index += 1
            continue
        rows_seen += 1
        page.wait_for_timeout(1500)
        parsed = parse_thread(page, expected_department=department)
        rows_written += upsert_parsed_messages(
            conn,
            parsed,
            page.url,
            include_unknown_direction=True,
        )
        index += 1
        if base_url and rows_seen < thread_limit:
            page.goto(base_url, wait_until="domcontentloaded", timeout=60000)
            wait_until_ready(page)
            select_department_messages(page, department)
    return rows_seen, rows_written


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
    conn.execute(
        """
        DELETE FROM dialpad_sms_messages
        WHERE source_url LIKE '%dialpad.com/login%'
           OR LOWER(body) IN (
                'talk, message & meet',
                'log in with google',
                'log in with microsoft',
                'log in with another provider',
                'or use your email address',
                'work email',
                'password',
                'log in to dialpad',
                'forgot password?',
                'not a customer? sign up here'
           )
        """
    )
    conn.execute(
        """
        DELETE FROM dialpad_sms_messages
        WHERE source_url LIKE '%/history/messages%'
          AND (message_at IS NULL OR message_at = '')
          AND json_extract(raw_json, '$.extraction') = 'message_line'
        """
    )


def wait_until_ready(page, timeout=30000):
    try:
        page.wait_for_load_state("load", timeout=timeout)
    except PlaywrightTimeoutError:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=5000)
    except PlaywrightTimeoutError:
        pass


def wait_for_authenticated_page(page, target_url, interactive_login=False, timeout_seconds=300):
    deadline = time.time() + timeout_seconds
    if interactive_login:
        print("Dialpad login required. Complete login in the opened browser window; extraction will continue automatically.")
    while time.time() < deadline:
        try:
            text = page.locator("body").inner_text(timeout=5000)
        except PlaywrightTimeoutError:
            text = ""
        if is_dialpad_app_page(page.url, text):
            return
        if is_login_page(page.url, text) and not interactive_login:
            raise RuntimeError(f"Dialpad profile is not authenticated; final_url={page.url}")
        if not interactive_login:
            time.sleep(2)
            continue
        time.sleep(2)
        try:
            text = page.locator("body").inner_text(timeout=5000)
        except PlaywrightTimeoutError:
            continue
        if is_dialpad_app_page(page.url, text):
            page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
            wait_until_ready(page)
            text = page.locator("body").inner_text(timeout=30000)
            if is_dialpad_app_page(page.url, text):
                return
        elif "dialpad.com/app/" not in page.url and "dialpad.com/login" not in page.url:
            continue
        else:
            try:
                page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
                wait_until_ready(page)
            except PlaywrightTimeoutError:
                pass
            text = page.locator("body").inner_text(timeout=30000)
            if is_dialpad_app_page(page.url, text):
                return
    raise RuntimeError(f"Timed out waiting for Dialpad app page; final_url={page.url}")


def main():
    parser = argparse.ArgumentParser(description="Extract visible Dialpad SMS threads into SQLite.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--profile-dir", default="browser_profiles/dialpad")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--interactive-login", action="store_true", help="Open a headed browser and wait for Dialpad login if the profile is expired.")
    parser.add_argument("--login-timeout", type=int, default=300, help="Seconds to wait for interactive Dialpad login.")
    parser.add_argument("--thread-limit", type=int, default=20)
    parser.add_argument("--start-date", default=DEFAULT_INITIAL_LOAD_START)
    parser.add_argument("--department", help="Dialpad department to select before extracting messages, e.g. HEIGHTS or WESTU.")
    parser.add_argument("--school", help="School label used to infer the Dialpad department, e.g. The Heights or West U.")
    args = parser.parse_args()
    department = normalize_department(args.department) or normalize_department(args.school)

    conn = sqlite3.connect(args.db)
    ensure_lead_followup_schema(conn)
    delete_known_sms_noise(conn)
    run_id = start_import_run(
        conn,
        "dialpad_sms",
        Path(__file__).name,
        args.start_date,
        None,
        {"url": args.url, "department": department},
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
            page.goto(args.url, wait_until="domcontentloaded", timeout=60000)
            wait_until_ready(page)
            wait_for_authenticated_page(page, args.url, args.interactive_login, args.login_timeout)
            if department:
                select_department_messages(page, department)
                rows_seen, rows_written = extract_selected_department_rows(
                    conn,
                    page,
                    args.thread_limit,
                    department,
                    args.url,
                )
                if rows_seen == 0:
                    raise RuntimeError(f"No Dialpad SMS/contact rows were visible for department {department}.")
            else:
                thread_links = capture_thread_links(page, args.thread_limit)
                urls = [link["href"] for _, link in thread_links] or [page.url]
                for url in urls[: args.thread_limit]:
                    rows_seen += 1
                    thread_page = context.new_page()
                    thread_page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    wait_until_ready(thread_page)
                    parsed = parse_thread(thread_page)
                    rows_written += upsert_parsed_messages(conn, parsed, thread_page.url)
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
