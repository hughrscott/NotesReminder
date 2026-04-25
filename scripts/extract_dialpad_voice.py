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
    if "outbound" in lowered or "placed call" in lowered:
        return "outbound"
    if "inbound" in lowered or "missed call" in lowered or "voicemail" in lowered:
        return "inbound"
    return "unknown"


def rows_from_visible_text(source_view, url, text, limit):
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    rows = []
    ignored = {
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
    }
    for index, line in enumerate(lines):
        lowered = line.lower()
        if lowered in ignored or lowered.startswith("call "):
            continue
        if not any(token in lowered for token in ("voicemail", "missed call", "caller hung up", "recording", "answered", "declined")):
            continue
        event_type = classify_event(source_view, line)
        phone_match = PHONE_RE.search(line)
        transcript = line if event_type == "voicemail" and len(line) > 40 else None
        rows.append(
            {
                "event_id": stable_id("dialpad_voice", source_view, url, index, line),
                "source_view": source_view,
                "event_type": event_type,
                "call_id": None,
                "phone": phone_match.group(0) if phone_match else None,
                "phone_normalized": normalize_phone(phone_match.group(0)) if phone_match else None,
                "contact_name": None,
                "direction": infer_direction(line),
                "event_at": None,
                "school": None,
                "department": None,
                "outcome": line[:240],
                "voicemail_transcript": transcript,
                "recording_url": None,
                "transcript_summary": None,
                "source_url": url,
                "raw_text": line,
                "raw_json": json.dumps({"source_view": source_view, "line_index": index}, sort_keys=True),
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
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                args.profile_dir,
                headless=args.headless,
                viewport={"width": 1440, "height": 1000},
            )
            page = context.pages[0] if context.pages else context.new_page()
            for source_view in requested_views:
                url = HISTORY_URLS[source_view]
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                wait_until_ready(page)
                text = page.locator("body").inner_text(timeout=30000)
                rows = rows_from_visible_text(source_view, page.url, text, args.limit_per_view)
                rows_seen += len(rows)
                for row in rows:
                    upsert_voice_event(conn, row)
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

    print(f"Dialpad voice extraction complete: rows_seen={rows_seen} rows_written={rows_written}")


if __name__ == "__main__":
    main()
