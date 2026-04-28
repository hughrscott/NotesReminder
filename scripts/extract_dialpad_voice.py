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


HISTORY_URLS = {
    "all": "https://dialpad.com/app/history/all",
    "calls": "https://dialpad.com/app/history/calls",
    "missed": "https://dialpad.com/app/history/missed",
    "voicemails": "https://dialpad.com/app/history/voicemails",
    "recordings": "https://dialpad.com/app/history/recordings",
}
PHONE_RE = re.compile(r"(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}")
DURATION_RE = re.compile(r"^\d+\s*(?:s|m|h)(?:\s+\d+\s*s)?$|^\d+:\d{2}$", re.IGNORECASE)
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
    "download",
    "search dialpad",
    "inbox",
    "contacts",
    "contact centers",
    "all channels",
    "threads",
    "scheduled",
    "coaching teams",
    "departments",
    "recents",
    "all",
    "calls",
    "missed",
    "meetings",
    "voicemails",
    "voicemail",
    "recordings",
    "recording",
    "messages",
    "starred",
    "spam",
    "unread",
}
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
    "HEIGHTS": ("Heights", "HEIGHTS"),
}
SOURCE_ID_RE = re.compile(
    r"(?:call|calls|recording|recordings|voicemail|voicemails|event)[=/]([A-Za-z0-9_-]{6,})",
    re.IGNORECASE,
)


def stable_id(prefix, *parts):
    digest = hashlib.sha256("|".join(str(p or "") for p in parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:24]}"


def wait_until_ready(page, timeout=30000):
    page.wait_for_load_state("load", timeout=timeout)
    try:
        page.wait_for_load_state("networkidle", timeout=5000)
    except PlaywrightTimeoutError:
        pass


def classify_event(source_view, text):
    lowered = text.lower()
    if "voicemail" in lowered:
        return "voicemail"
    if "missed call" in lowered or source_view == "missed":
        return "missed_call"
    if "recording" in lowered or source_view == "recordings":
        return "recording"
    return "call"


def infer_direction(text):
    lowered = text.lower()
    if "outbound" in lowered or "placed call" in lowered or "outgoing" in lowered:
        return "outbound"
    if "inbound" in lowered or "incoming" in lowered or "missed call" in lowered or "voicemail" in lowered:
        return "inbound"
    return "unknown"


def detect_department(text):
    upper = (text or "").upper()
    for label, value in DEPARTMENT_LABELS.items():
        if re.search(rf"\b{re.escape(label)}\b", upper):
            return value
    return (None, None)


def extract_source_id(*values):
    for value in values:
        match = SOURCE_ID_RE.search(value or "")
        if match:
            return match.group(1)
    return None


def extract_links(page):
    return page.locator("a").evaluate_all(
        """
        links => links.map(a => ({href: a.href, text: a.innerText || a.textContent || ''}))
                      .filter(a => a.href && /dialpad\\.com/.test(a.href))
        """
    )


def first_recording_or_transcript_url(links):
    for link in links or []:
        value = f"{link.get('href', '')} {link.get('text', '')}".lower()
        if "recording" in value or "transcript" in value or "download" in value:
            return link.get("href")
    return None


def link_availability(links):
    links = links or []
    values = [f"{link.get('href', '')} {link.get('text', '')}".lower() for link in links]
    return {
        "download_link_visible": any("download" in value for value in values),
        "recording_link_visible": any("recording" in value for value in values),
        "transcript_link_visible": any("transcript" in value for value in values),
    }


def summarize_view(source_view, url, rows, links):
    transcript_rows = sum(1 for row in rows if row.get("voicemail_transcript") or row.get("transcript_summary"))
    event_types = {}
    for row in rows:
        event_types[row["event_type"]] = event_types.get(row["event_type"], 0) + 1
    summary = {
        "url": url,
        "rows": len(rows),
        "event_types": event_types,
        "transcript_rows": transcript_rows,
        "voicemail_transcript_rows": sum(1 for row in rows if row.get("voicemail_transcript")),
        "recording_or_transcript_url_rows": sum(1 for row in rows if row.get("recording_url")),
        "availability": link_availability(links),
    }
    if source_view == "voicemails" and transcript_rows == 0:
        summary["blocker"] = "No visible voicemail transcripts captured from this view."
    if source_view == "recordings" and not summary["availability"]["recording_link_visible"]:
        summary["blocker"] = "No visible recording links captured from this view."
    if source_view == "recordings" and not summary["availability"]["transcript_link_visible"]:
        summary["transcript_blocker"] = "No visible call/recording transcript links captured from this view."
    return summary


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
        r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})(?:,\s*(\d{4}))?",
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


def is_noise_line(line):
    lowered = line.lower().strip()
    return (
        not lowered
        or lowered in NAV_LABELS
        or lowered in LOGIN_LABELS
        or lowered.startswith("call ")
        or lowered in {"the power of dialpad. on your desktop.", "dnd on"}
        or re.fullmatch(r"[A-Z]{1,3}", line.strip()) is not None
    )


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


def wait_for_authenticated_page(page, target_url, interactive_login=False, timeout_seconds=300):
    text = page.locator("body").inner_text(timeout=30000)
    if is_dialpad_app_page(page.url, text):
        return
    if not interactive_login:
        raise RuntimeError("Dialpad profile is not authenticated; landed on login page.")
    print("Dialpad login required. Complete login in the opened browser window; extraction will continue automatically.")
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
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
    raise RuntimeError("Timed out waiting for Dialpad interactive login.")


def is_outcome_line(line):
    lowered = line.lower()
    return any(
        token in lowered
        for token in (
            "voicemail",
            "missed call",
            "caller hung up",
            "recording",
            "answered",
            "declined",
            "incoming",
            "inbound",
            "outgoing",
            "outbound",
        )
    )


def parse_outcome_line(source_view, line):
    lowered = line.lower()
    if source_view == "missed" or "missed call" in lowered:
        event_type = "voicemail" if "voicemail" in lowered else "missed_call"
    elif source_view == "voicemails" or "voicemail" in lowered:
        event_type = "voicemail"
    elif source_view == "recordings" or "recording" in lowered:
        event_type = "recording"
    else:
        event_type = "call"
    return event_type, infer_direction(line)


def rows_from_visible_text(source_view, url, text, limit, now=None, links=None):
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    lines = [
        line
        for line in lines
        if not is_noise_line(line)
        or (source_view == "recordings" and line.strip().lower() == "recording")
    ]
    rows = []
    school, department = detect_department(text)
    source_id_from_url = extract_source_id(url)
    recording_or_transcript_url = first_recording_or_transcript_url(links)
    for index, line in enumerate(lines):
        if not is_outcome_line(line):
            continue
        event_type, direction = parse_outcome_line(source_view, line)
        context = lines[max(0, index - 4) : index + 5]
        phone_match = next((PHONE_RE.search(item) for item in context if PHONE_RE.search(item)), None)
        event_at = next((normalize_dialpad_date(item, now=now) for item in context if normalize_dialpad_date(item, now=now)), None)
        contact_name = None
        for candidate in reversed(lines[max(0, index - 4) : index]):
            if DURATION_RE.match(candidate) or normalize_dialpad_date(candidate, now=now):
                continue
            if is_outcome_line(candidate):
                continue
            contact_name = candidate
            break
        transcript = next((item.strip('"') for item in lines[index + 1 : index + 6] if len(item) > 30 and not is_outcome_line(item)), None)
        if event_type == "voicemail" and not transcript and len(line) > 40:
            transcript = line
        call_id = extract_source_id(url, line) or source_id_from_url
        source_id_status = "visible" if call_id else "generated_hash"
        transcript_field = "voicemail_transcript" if event_type == "voicemail" else "transcript_summary"
        transcript_status = "visible" if transcript else "not_visible"
        recording_url = recording_or_transcript_url if event_type == "recording" or recording_or_transcript_url else None
        rows.append(
            {
                "event_id": call_id or stable_id("dialpad_voice", source_view, url, index, line),
                "source_view": source_view,
                "event_type": event_type,
                "call_id": call_id,
                "phone": phone_match.group(0) if phone_match else None,
                "phone_normalized": normalize_phone(phone_match.group(0)) if phone_match else None,
                "contact_name": contact_name,
                "direction": direction,
                "event_at": event_at,
                "school": school,
                "department": department,
                "outcome": line[:240],
                "voicemail_transcript": transcript if transcript_field == "voicemail_transcript" else None,
                "recording_url": recording_url,
                "transcript_summary": transcript if transcript_field == "transcript_summary" else None,
                "source_url": url,
                "raw_text": line,
                "raw_json": json.dumps(
                    {
                        "extraction": "visible_history_text",
                        "source_view": source_view,
                        "line_index": index,
                        "context": context,
                        "source_id_status": source_id_status,
                        "transcript_status": transcript_status,
                        "transcript_field": transcript_field if transcript else None,
                        "recording_or_transcript_url_status": "visible" if recording_or_transcript_url else "not_visible",
                        "department_detected": department,
                        "source_timestamp_field": "event_at",
                        "import_timestamp_field": "updated_at",
                    },
                    sort_keys=True,
                ),
                "updated_at": utc_now_iso(),
            }
        )
        if len(rows) >= limit:
            break
    return rows


def upsert_voice_event(conn, row):
    conn.execute(
        """
        INSERT INTO dialpad_voice_events (
            event_id, source_view, event_type, call_id, phone, phone_normalized,
            contact_name, direction, event_at, school, department, outcome,
            voicemail_transcript, recording_url, transcript_summary, source_url,
            raw_text, raw_json, updated_at
        )
        VALUES (
            :event_id, :source_view, :event_type, :call_id, :phone, :phone_normalized,
            :contact_name, :direction, :event_at, :school, :department, :outcome,
            :voicemail_transcript, :recording_url, :transcript_summary, :source_url,
            :raw_text, :raw_json, :updated_at
        )
        ON CONFLICT(event_id) DO UPDATE SET
            source_view = excluded.source_view,
            event_type = excluded.event_type,
            phone = COALESCE(excluded.phone, dialpad_voice_events.phone),
            phone_normalized = COALESCE(excluded.phone_normalized, dialpad_voice_events.phone_normalized),
            contact_name = COALESCE(excluded.contact_name, dialpad_voice_events.contact_name),
            direction = excluded.direction,
            event_at = COALESCE(excluded.event_at, dialpad_voice_events.event_at),
            school = COALESCE(excluded.school, dialpad_voice_events.school),
            department = COALESCE(excluded.department, dialpad_voice_events.department),
            outcome = excluded.outcome,
            voicemail_transcript = COALESCE(excluded.voicemail_transcript, dialpad_voice_events.voicemail_transcript),
            recording_url = COALESCE(excluded.recording_url, dialpad_voice_events.recording_url),
            transcript_summary = COALESCE(excluded.transcript_summary, dialpad_voice_events.transcript_summary),
            source_url = excluded.source_url,
            raw_text = excluded.raw_text,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )


def main():
    parser = argparse.ArgumentParser(description="Extract visible Dialpad voice history rows into SQLite.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--profile-dir", default="browser_profiles/dialpad")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--interactive-login", action="store_true", help="Open a headed browser and wait for Dialpad login if the profile is expired.")
    parser.add_argument("--login-timeout", type=int, default=300, help="Seconds to wait for interactive Dialpad login.")
    parser.add_argument("--views", default="calls,missed,voicemails,recordings")
    parser.add_argument("--limit-per-view", type=int, default=25)
    parser.add_argument("--start-date", default=DEFAULT_INITIAL_LOAD_START)
    args = parser.parse_args()

    requested_views = [item.strip() for item in args.views.split(",") if item.strip()]
    unknown = sorted(set(requested_views) - set(HISTORY_URLS))
    if unknown:
        raise ValueError(f"Unknown Dialpad history views: {', '.join(unknown)}")

    conn = sqlite3.connect(args.db)
    ensure_lead_followup_schema(conn)
    run_id = start_import_run(
        conn,
        "dialpad_voice",
        Path(__file__).name,
        args.start_date,
        None,
        {"views": requested_views, "limit_per_view": args.limit_per_view},
    )
    conn.commit()
    rows_seen = rows_written = 0
    view_summaries = {}
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                args.profile_dir,
                headless=args.headless and not args.interactive_login,
                viewport={"width": 1440, "height": 1000},
            )
            page = context.pages[0] if context.pages else context.new_page()
            for source_view in requested_views:
                url = HISTORY_URLS[source_view]
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                wait_until_ready(page)
                wait_for_authenticated_page(page, url, args.interactive_login, args.login_timeout)
                text = page.locator("body").inner_text(timeout=30000)
                links = extract_links(page)
                rows = rows_from_visible_text(source_view, page.url, text, args.limit_per_view, links=links)
                view_summaries[source_view] = summarize_view(source_view, page.url, rows, links)
                rows_seen += len(rows)
                for row in rows:
                    upsert_voice_event(conn, row)
                    rows_written += 1
            context.close()
        metadata = {"views": requested_views, "limit_per_view": args.limit_per_view, "view_summaries": view_summaries}
        finish_import_run(conn, run_id, "success", rows_seen, rows_written, 0, metadata=metadata)
        conn.commit()
    except Exception as exc:
        metadata = {"views": requested_views, "limit_per_view": args.limit_per_view, "view_summaries": view_summaries}
        finish_import_run(conn, run_id, "error", rows_seen, rows_written, 0, str(exc), metadata=metadata)
        conn.commit()
        raise
    finally:
        conn.close()

    print(f"Dialpad voice extraction complete: rows_seen={rows_seen} rows_written={rows_written}")


if __name__ == "__main__":
    main()
