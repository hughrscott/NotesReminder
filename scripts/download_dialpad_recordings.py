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
    ensure_lead_followup_schema,
    finish_import_run,
    start_import_run,
    utc_now_iso,
)
from scripts.extract_dialpad_voice import (  # noqa: E402
    conversation_history_row_from_dom,
    upsert_voice_event,
    wait_for_authenticated_page,
    wait_until_ready,
)


DEFAULT_DB = "outputs/lead_intelligence/lead_intelligence_working.db"
DEFAULT_PROFILE_DIR = "browser_profiles/dialpad"
DEFAULT_OUTPUT_DIR = "outputs/lead_intelligence/dialpad_recordings"
CONVERSATION_HISTORY_URL = "https://dialpad.com/conversationhistory"


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_extension(filename):
    suffix = Path(filename or "").suffix.lower()
    if suffix and re.fullmatch(r"\.[a-z0-9]{2,5}", suffix):
        return suffix
    return ".mp3"


def row_duration(parsed):
    try:
        metadata = json.loads(parsed.get("raw_json") or "{}")
    except json.JSONDecodeError:
        return None
    return metadata.get("duration") or metadata.get("connected_duration")


def has_recording_action(parsed):
    try:
        metadata = json.loads(parsed.get("raw_json") or "{}")
    except json.JSONDecodeError:
        return False
    return bool(metadata.get("recording_action_visible") or metadata.get("call_review_url"))


def within_window(parsed, start_date=None, end_date=None):
    event_at = parsed.get("event_at") or ""
    event_date = event_at[:10]
    if start_date and event_date < start_date:
        return False
    if end_date and event_date > end_date:
        return False
    return True


def visible_conversation_rows(page):
    dom_rows = page.locator("table tr").evaluate_all(
        """
        rows => rows.slice(1).map((row, index) => {
          const cells = Array.from(row.querySelectorAll('td, [role="cell"]'));
          const actionCell = cells[cells.length - 1];
          return {
            index,
            text: row.innerText || row.textContent || '',
            cells: cells.map(cell => cell.innerText || cell.textContent || ''),
            links: Array.from(row.querySelectorAll('a')).map(a => ({
              href: a.href || '',
              text: a.innerText || a.textContent || '',
              label: a.getAttribute('aria-label') || a.getAttribute('title') || ''
            })),
            button_labels: Array.from(row.querySelectorAll('button')).map(b =>
              b.innerText || b.textContent || b.getAttribute('aria-label') || b.getAttribute('title') || ''
            ).filter(Boolean),
            action_button_count: actionCell ? actionCell.querySelectorAll('button').length : 0
          };
        })
        """
    )
    parsed_rows = []
    for row in dom_rows:
        parsed = conversation_history_row_from_dom(page.url, row, row["index"])
        if parsed:
            parsed_rows.append({"dom_index": row["index"], "dom": row, "parsed": parsed})
    return parsed_rows


def existing_success(conn, call_id):
    row = conn.execute(
        """
        SELECT file_path
        FROM recording_downloads
        WHERE call_id = ?
          AND status = 'success'
          AND COALESCE(file_path, '') != ''
        """,
        (call_id,),
    ).fetchone()
    return row and Path(row[0]).exists()


def upsert_recording_download(conn, row):
    conn.execute(
        """
        INSERT INTO recording_downloads (
            call_id, recording_url, file_path, status, error_message, downloaded_at,
            voice_event_id, source_url, event_at, phone_normalized, contact_name, school,
            duration, file_sha256, file_size_bytes, content_type, transcription_status,
            updated_at
        )
        VALUES (
            :call_id, :recording_url, :file_path, :status, :error_message, :downloaded_at,
            :voice_event_id, :source_url, :event_at, :phone_normalized, :contact_name, :school,
            :duration, :file_sha256, :file_size_bytes, :content_type, :transcription_status,
            :updated_at
        )
        ON CONFLICT(call_id) DO UPDATE SET
            recording_url = COALESCE(excluded.recording_url, recording_downloads.recording_url),
            file_path = COALESCE(excluded.file_path, recording_downloads.file_path),
            status = excluded.status,
            error_message = excluded.error_message,
            downloaded_at = COALESCE(excluded.downloaded_at, recording_downloads.downloaded_at),
            voice_event_id = COALESCE(excluded.voice_event_id, recording_downloads.voice_event_id),
            source_url = COALESCE(excluded.source_url, recording_downloads.source_url),
            event_at = COALESCE(excluded.event_at, recording_downloads.event_at),
            phone_normalized = COALESCE(excluded.phone_normalized, recording_downloads.phone_normalized),
            contact_name = COALESCE(excluded.contact_name, recording_downloads.contact_name),
            school = COALESCE(excluded.school, recording_downloads.school),
            duration = COALESCE(excluded.duration, recording_downloads.duration),
            file_sha256 = COALESCE(excluded.file_sha256, recording_downloads.file_sha256),
            file_size_bytes = COALESCE(excluded.file_size_bytes, recording_downloads.file_size_bytes),
            content_type = COALESCE(excluded.content_type, recording_downloads.content_type),
            transcription_status = COALESCE(excluded.transcription_status, recording_downloads.transcription_status),
            updated_at = excluded.updated_at
        """,
        row,
    )


def ensure_transcription_pending(conn, call_id, recording_url, duration):
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO recording_transcripts (
            call_id, recording_url, recording_duration, transcript_status, created_at
        )
        VALUES (?, ?, ?, 'pending', ?)
        ON CONFLICT(call_id) DO UPDATE SET
            recording_url = COALESCE(excluded.recording_url, recording_transcripts.recording_url),
            recording_duration = COALESCE(excluded.recording_duration, recording_transcripts.recording_duration),
            transcript_status = CASE
                WHEN COALESCE(recording_transcripts.transcript_status, '') IN ('', 'failed', 'error')
                THEN 'pending'
                ELSE recording_transcripts.transcript_status
            END
        """,
        (call_id, recording_url, duration, now),
    )


def click_download_for_row(page, dom_index):
    row_locator = page.locator("table tr").nth(dom_index + 1)
    buttons = row_locator.locator("button")
    if buttons.count() == 0:
        raise RuntimeError("No row action button found.")
    row_locator.scroll_into_view_if_needed(timeout=10000)
    buttons.last.click(timeout=10000, force=True)
    menu_item = page.get_by_text(re.compile(r"Download recordings?", re.IGNORECASE))
    if menu_item.count() == 0:
        raise RuntimeError("Download recordings menu item was not visible.")
    with page.expect_download(timeout=30000) as download_info:
        menu_item.first.click(timeout=10000)
    return download_info.value


def download_visible_recordings(args):
    db_path = Path(args.db).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_lead_followup_schema(conn)
    run_id = start_import_run(
        conn,
        "dialpad_recording_downloads",
        Path(__file__).name,
        args.start_date,
        args.end_date,
        {"limit": args.limit, "output_dir": str(output_dir), "dry_run": args.dry_run},
    )
    conn.commit()
    rows_seen = rows_inserted = rows_updated = 0
    failures = []
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                args.profile_dir,
                headless=args.headless and not args.interactive_login,
                accept_downloads=True,
                viewport={"width": 1440, "height": 1000},
            )
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(CONVERSATION_HISTORY_URL, wait_until="domcontentloaded", timeout=60000)
            wait_until_ready(page)
            wait_for_authenticated_page(page, CONVERSATION_HISTORY_URL, args.interactive_login, args.login_timeout)
            candidates = [
                row
                for row in visible_conversation_rows(page)
                if within_window(row["parsed"], args.start_date, args.end_date)
                and has_recording_action(row["parsed"])
            ]
            for candidate in candidates:
                if rows_seen >= args.limit:
                    break
                parsed = candidate["parsed"]
                call_id = parsed.get("call_id") or parsed["event_id"]
                rows_seen += 1
                upsert_voice_event(conn, parsed)
                if existing_success(conn, call_id) and not args.force:
                    upsert_recording_download(
                        conn,
                        recording_row(parsed, call_id, "skipped_existing", None, None, None, None, None),
                    )
                    rows_updated += 1
                    conn.commit()
                    continue
                if args.dry_run:
                    upsert_recording_download(
                        conn,
                        recording_row(parsed, call_id, "dry_run", None, None, None, None, None),
                    )
                    rows_inserted += 1
                    conn.commit()
                    continue
                try:
                    download = click_download_for_row(page, candidate["dom_index"])
                    extension = safe_extension(download.suggested_filename)
                    destination = output_dir / f"{call_id}{extension}"
                    download.save_as(str(destination))
                    file_hash = file_sha256(destination)
                    size = destination.stat().st_size
                    recording_url = download.url
                    upsert_recording_download(
                        conn,
                        recording_row(
                            parsed,
                            call_id,
                            "success",
                            recording_url,
                            str(destination),
                            file_hash,
                            size,
                            None,
                        ),
                    )
                    ensure_transcription_pending(conn, call_id, recording_url, row_duration(parsed))
                    rows_inserted += 1
                except Exception as exc:
                    failures.append({"call_id": call_id, "error": str(exc)[:300]})
                    upsert_recording_download(
                        conn,
                        recording_row(parsed, call_id, "error", None, None, None, None, str(exc)[:300]),
                    )
                conn.commit()
            context.close()
        finish_import_run(
            conn,
            run_id,
            "partial" if failures else "success",
            rows_seen,
            rows_inserted,
            rows_updated,
            metadata={"failure_count": len(failures), "failures": failures[:10]},
        )
        conn.commit()
    except Exception as exc:
        finish_import_run(conn, run_id, "error", rows_seen, rows_inserted, rows_updated, error=str(exc))
        conn.commit()
        raise
    finally:
        conn.close()
    return rows_seen, rows_inserted, rows_updated, failures


def recording_row(parsed, call_id, status, recording_url, file_path, file_hash, file_size, error):
    now = utc_now_iso()
    return {
        "call_id": call_id,
        "recording_url": recording_url,
        "file_path": file_path,
        "status": status,
        "error_message": error,
        "downloaded_at": now if status == "success" else None,
        "voice_event_id": parsed.get("event_id"),
        "source_url": parsed.get("source_url"),
        "event_at": parsed.get("event_at"),
        "phone_normalized": parsed.get("phone_normalized"),
        "contact_name": parsed.get("contact_name"),
        "school": parsed.get("school"),
        "duration": row_duration(parsed),
        "file_sha256": file_hash,
        "file_size_bytes": file_size,
        "content_type": None,
        "transcription_status": "pending" if status == "success" else None,
        "updated_at": now,
    }


def main():
    parser = argparse.ArgumentParser(description="Download visible Dialpad Conversation History recordings into local private storage.")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--profile-dir", default=DEFAULT_PROFILE_DIR)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--interactive-login", action="store_true")
    parser.add_argument("--login-timeout", type=int, default=300)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    rows_seen, rows_inserted, rows_updated, failures = download_visible_recordings(args)
    print(
        "Dialpad recording download complete: "
        f"rows_seen={rows_seen} rows_inserted={rows_inserted} rows_updated={rows_updated} failures={len(failures)}"
    )


if __name__ == "__main__":
    main()
