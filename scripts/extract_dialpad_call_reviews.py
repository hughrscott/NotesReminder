#!/usr/bin/env python3
import argparse
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
    ensure_lead_followup_schema,
    finish_import_run,
    start_import_run,
    utc_now_iso,
)
from notesreminder.lib.raw_capture import write_raw_capture  # noqa: E402
from scripts.extract_dialpad_voice import (  # noqa: E402
    extract_source_id,
    wait_for_authenticated_page,
    wait_until_ready,
)


CALL_REVIEW_URL_RE = re.compile(r"dialpad\.com/callhistory/callreview/([A-Za-z0-9_-]+)", re.IGNORECASE)


def short_error(value):
    return str(value or "").replace("\n", " ")[:300]


def call_review_id_from_url(url):
    match = CALL_REVIEW_URL_RE.search(url or "")
    return match.group(1) if match else extract_source_id(url)


def clean_lines(text):
    return [line.strip() for line in (text or "").splitlines() if line.strip()]


def parse_action_items(lines):
    items = []
    in_actions = False
    pending_item = None
    for line in lines:
        lowered = line.lower()
        if lowered == "action items":
            in_actions = True
            continue
        if in_actions and lowered in {"transcript", "excerpts", "comments", "recap", "comment"}:
            break
        if not in_actions:
            continue
        numbered = re.match(r"^\d+[\).]\s*(?P<text>.*)$", line)
        if numbered:
            text = numbered.group("text").strip()
            if text:
                items.append(text)
                pending_item = None
            else:
                pending_item = len(items)
                items.append("")
            continue
        if pending_item is not None:
            items[pending_item] = f"{items[pending_item]} {line}".strip()
    return [item for item in items if item]


def parse_transcript_turns(lines):
    turns = []
    inline_time_re = re.compile(r"^(?P<speaker>.+?)\s+(?P<time>\d{1,2}:\d{2}\s*[AP]M)\s+(?P<text>.+)$", re.IGNORECASE)
    speaker_time_re = re.compile(r"^(?P<speaker>.+?)\s+(?P<time>\d{1,2}:\d{2}\s*[AP]M)$", re.IGNORECASE)
    time_only_re = re.compile(r"^\d{1,2}:\d{2}\s*[AP]M$", re.IGNORECASE)
    stop_lines = {
        "transcript",
        "comments",
        "recap",
        "excerpts",
        "comment",
        "no comments",
        "transcript search by keyword",
        "powered by dialpad ai",
        "show callers",
        "save",
    }
    index = 0
    while index < len(lines):
        line = lines[index]
        lowered = line.lower()
        if (
            lowered in stop_lines
            or lowered.startswith("how accurate ")
            or "call audio seek slider" in lowered
            or re.search(r"^\d+:\d{2}/\d+:\d{2}$", line)
            or re.fullmatch(r"[A-Z]{1,3}", line)
        ):
            index += 1
            continue
        match = inline_time_re.match(line)
        if match:
            turns.append(
                {
                    "speaker": match.group("speaker").strip(),
                    "time": match.group("time").strip(),
                    "text": match.group("text").strip(),
                }
            )
            index += 1
            continue
        match = speaker_time_re.match(line)
        if match:
            turns.append(
                {
                    "speaker": match.group("speaker").strip(),
                    "time": match.group("time").strip(),
                    "text": "",
                }
            )
            index += 1
            continue
        if index + 1 < len(lines) and time_only_re.match(lines[index + 1]):
            turns.append({"speaker": line.strip(), "time": lines[index + 1].strip(), "text": ""})
            index += 2
            continue
        if turns:
            turns[-1]["text"] = f"{turns[-1]['text']} {line}".strip()
        index += 1
    return [turn for turn in turns if turn["text"]]


def parse_call_review_text(url, text):
    lines = clean_lines(text)
    call_review_id = call_review_id_from_url(url)
    recap_text = ""
    for index, line in enumerate(lines):
        if line.lower() == "recap" and index + 1 < len(lines):
            recap_text = lines[index + 1]
            break
    action_items = parse_action_items(lines)
    turns = parse_transcript_turns(lines)
    transcript_text = "\n".join(turn["text"] for turn in turns)
    audio_available = any("call audio seek slider" in line.lower() or re.search(r"\d+:\d{2}/\d+:\d{2}", line) for line in lines)
    return {
        "call_review_id": call_review_id,
        "call_id": call_review_id,
        "call_review_url": url,
        "transcript_text": transcript_text or None,
        "recap_text": recap_text or None,
        "action_items_json": json.dumps(action_items, sort_keys=True),
        "speaker_turns_json": json.dumps(turns, sort_keys=True),
        "transcript_available": 1 if turns or transcript_text else 0,
        "recap_available": 1 if recap_text else 0,
        "action_items_available": 1 if action_items else 0,
        "audio_available": 1 if audio_available else 0,
        "extraction_status": "success" if (turns or recap_text or action_items) else "partial",
        "raw_json": json.dumps(
            {
                "line_count": len(lines),
                "turn_count": len(turns),
                "action_item_count": len(action_items),
                "source": "dialpad_call_review",
            },
            sort_keys=True,
        ),
    }


def upsert_call_review(conn, row):
    conn.execute(
        """
        INSERT INTO dialpad_call_reviews (
            call_review_id, call_id, voice_event_id, call_review_url, event_at,
            transcript_text, recap_text, action_items_json, speaker_turns_json,
            transcript_available, recap_available, action_items_available, audio_available,
            extraction_status, raw_json, updated_at
        )
        VALUES (
            :call_review_id, :call_id, :voice_event_id, :call_review_url, :event_at,
            :transcript_text, :recap_text, :action_items_json, :speaker_turns_json,
            :transcript_available, :recap_available, :action_items_available, :audio_available,
            :extraction_status, :raw_json, :updated_at
        )
        ON CONFLICT(call_review_id) DO UPDATE SET
            call_id = excluded.call_id,
            voice_event_id = COALESCE(excluded.voice_event_id, dialpad_call_reviews.voice_event_id),
            call_review_url = excluded.call_review_url,
            event_at = COALESCE(excluded.event_at, dialpad_call_reviews.event_at),
            transcript_text = COALESCE(excluded.transcript_text, dialpad_call_reviews.transcript_text),
            recap_text = COALESCE(excluded.recap_text, dialpad_call_reviews.recap_text),
            action_items_json = excluded.action_items_json,
            speaker_turns_json = excluded.speaker_turns_json,
            transcript_available = excluded.transcript_available,
            recap_available = excluded.recap_available,
            action_items_available = excluded.action_items_available,
            audio_available = excluded.audio_available,
            extraction_status = excluded.extraction_status,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )


def call_review_targets(conn, limit):
    return conn.execute(
        """
        SELECT
            event_id AS voice_event_id,
            call_id,
            source_url AS call_review_url,
            event_at
        FROM dialpad_voice_events
        WHERE source_url LIKE '%dialpad.com/callhistory/callreview/%'
        ORDER BY event_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def goto_call_review_with_retry(page, url, attempts=3):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            return
        except Exception as exc:
            last_error = exc
            if attempt < attempts:
                page.wait_for_timeout(2000 * attempt)
    raise last_error


def extract_call_review_page(page, target, interactive_login=False, login_timeout=300):
    goto_call_review_with_retry(page, target["call_review_url"])
    wait_until_ready(page)
    wait_for_authenticated_page(page, target["call_review_url"], interactive_login, login_timeout)
    recap_text = page.locator("body").inner_text(timeout=30000)
    transcript_button = page.get_by_text("Transcript", exact=True)
    if transcript_button.count():
        try:
            transcript_button.first.click(timeout=10000)
            page.wait_for_timeout(1000)
        except PlaywrightTimeoutError:
            pass
    transcript_text = page.locator("body").inner_text(timeout=30000)
    text = f"{recap_text}\n{transcript_text}"
    parsed = parse_call_review_text(page.url, text)
    parsed.update(
        {
            "call_review_id": parsed["call_review_id"] or target["call_id"] or target["voice_event_id"],
            "call_id": parsed["call_id"] or target["call_id"],
            "voice_event_id": target["voice_event_id"],
            "call_review_url": page.url,
            "event_at": target["event_at"],
            "updated_at": utc_now_iso(),
            "_raw_capture_text": text,
        }
    )
    return parsed


def main():
    parser = argparse.ArgumentParser(description="Extract Dialpad call-review transcripts and recaps into SQLite.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--profile-dir", default="browser_profiles/dialpad")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--interactive-login", action="store_true", help="Open a headed browser and wait for Dialpad login if the profile is expired.")
    parser.add_argument("--login-timeout", type=int, default=300, help="Seconds to wait for interactive Dialpad login.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    ensure_lead_followup_schema(conn)
    run_id = start_import_run(
        conn,
        "dialpad_call_reviews",
        Path(__file__).name,
        metadata={"limit": args.limit},
    )
    conn.commit()
    targets = call_review_targets(conn, args.limit)
    rows_seen = rows_inserted = rows_updated = 0
    failures = []
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                args.profile_dir,
                headless=args.headless and not args.interactive_login,
                viewport={"width": 1440, "height": 1000},
            )
            page = context.pages[0] if context.pages else context.new_page()
            for target in targets:
                rows_seen += 1
                try:
                    row = extract_call_review_page(page, target, args.interactive_login, args.login_timeout)
                except Exception as exc:
                    failures.append(
                        {
                            "voice_event_id": target["voice_event_id"],
                            "call_review_id": call_review_id_from_url(target["call_review_url"]),
                            "error": short_error(exc),
                        }
                    )
                    continue
                write_raw_capture(
                    conn,
                    source="dialpad",
                    capture_type="dialpad_call_review_text",
                    content=row.pop("_raw_capture_text", ""),
                    source_url=row["call_review_url"],
                    metadata={
                        "call_review_id": row["call_review_id"],
                        "voice_event_id": row["voice_event_id"],
                    },
                    import_run_id=run_id,
                    extension="txt",
                    label=f"call-review-{row['call_review_id']}",
                )
                exists = conn.execute(
                    "SELECT 1 FROM dialpad_call_reviews WHERE call_review_id = ?",
                    (row["call_review_id"],),
                ).fetchone()
                upsert_call_review(conn, row)
                conn.commit()
                if exists:
                    rows_updated += 1
                else:
                    rows_inserted += 1
            context.close()
        finish_import_run(
            conn,
            run_id,
            "partial" if failures else "success",
            rows_seen=rows_seen,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            metadata={"limit": args.limit, "failure_count": len(failures), "failures": failures[:10]},
        )
        conn.commit()
    except Exception as exc:
        finish_import_run(
            conn,
            run_id,
            "error",
            rows_seen=rows_seen,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            error=str(exc),
            metadata={"limit": args.limit},
        )
        conn.commit()
        raise
    finally:
        conn.close()
    print(
        "Dialpad call-review extraction complete: "
        f"rows_seen={rows_seen} rows_inserted={rows_inserted} rows_updated={rows_updated}"
    )


if __name__ == "__main__":
    main()
