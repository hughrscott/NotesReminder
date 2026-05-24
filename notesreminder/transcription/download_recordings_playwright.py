import argparse
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright


CONTENT_TYPE_EXT = {
    "audio/mpeg": "mp3",
    "audio/mp3": "mp3",
    "audio/wav": "wav",
    "audio/x-wav": "wav",
    "audio/flac": "flac",
    "audio/ogg": "ogg",
    "audio/webm": "webm",
    "audio/mp4": "mp4",
    "video/mp4": "mp4",
}


def ensure_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recording_downloads (
            call_id TEXT PRIMARY KEY,
            recording_url TEXT,
            file_path TEXT,
            status TEXT,
            error_message TEXT,
            downloaded_at TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_recording_downloads_status "
        "ON recording_downloads(status)"
    )


def get_pending_recordings(conn, limit=None):
    sql = """
        SELECT r.call_id, r.recording_url
        FROM (
            SELECT
                call_id,
                recording_url,
                date,
                ROW_NUMBER() OVER (
                    PARTITION BY call_id
                    ORDER BY date DESC
                ) AS rn
            FROM dialpad_recordings
            WHERE recording_url IS NOT NULL
              AND recording_url != ''
        ) r
        LEFT JOIN recording_downloads d ON d.call_id = r.call_id
        WHERE (d.call_id IS NULL OR d.status != 'success')
          AND r.rn = 1
        ORDER BY r.call_id
    """
    if limit:
        sql += " LIMIT ?"
        return conn.execute(sql, (limit,)).fetchall()
    return conn.execute(sql).fetchall()


def ext_from_content_type(content_type):
    if not content_type:
        return None
    normalized = content_type.split(";")[0].strip().lower()
    return CONTENT_TYPE_EXT.get(normalized)


def download_recording(context, url, dest_base, default_ext=None):
    response = context.request.get(url, timeout=120000)
    status = response.status
    content_type = response.headers.get("content-type")
    if status != 200:
        raise RuntimeError(f"HTTP {status}")
    ext = ext_from_content_type(content_type) or default_ext
    if not ext:
        raise RuntimeError("Unknown content type; set --default-ext")
    dest_path = dest_base.with_suffix(f".{ext}")
    body = response.body()
    if body.startswith(b"<!DOCTYPE html") or body.startswith(b"<html"):
        raise RuntimeError("Received HTML instead of audio (auth required).")
    dest_path.write_bytes(body)
    return dest_path


def record_result(conn, call_id, recording_url, file_path, status, error_message):
    downloaded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT OR REPLACE INTO recording_downloads (
            call_id,
            recording_url,
            file_path,
            status,
            error_message,
            downloaded_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            call_id,
            recording_url,
            str(file_path) if file_path else None,
            status,
            error_message,
            downloaded_at,
        ),
    )
    conn.commit()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download Dialpad recordings with a logged-in browser session."
    )
    parser.add_argument(
        "--db",
        default="reminders.db",
        help="Path to the SQLite database (default: reminders.db)",
    )
    parser.add_argument(
        "--out-dir",
        default="recordings",
        help="Directory to save recordings (default: recordings)",
    )
    parser.add_argument(
        "--profile-dir",
        default="dialpad_profile",
        help="Playwright user data dir for persistent login (default: dialpad_profile)",
    )
    parser.add_argument(
        "--login-url",
        default="https://dialpad.com",
        help="URL to open for interactive login",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of recordings to download (0 = none; use --all for no limit)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Download all pending recordings (overrides --limit 0)",
    )
    parser.add_argument(
        "--default-ext",
        default="mp3",
        help="Fallback extension if content-type is unknown (default: mp3)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run headless (not recommended for first login)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    try:
        ensure_table(conn)
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                args.profile_dir,
                headless=args.headless,
            )
            page = context.new_page()
            page.goto(args.login_url)
            if not args.headless:
                input("Login in the browser window, then press Enter to continue...")

            if not args.all and args.limit == 0:
                print("Login complete. No downloads requested.")
                context.close()
                return

            limit = None if args.all else args.limit
            rows = get_pending_recordings(conn, limit=limit)
            if not rows:
                print("No recordings to download.")
                context.close()
                return

            for call_id, recording_url in rows:
                print(f"Downloading call {call_id}")
                dest_base = out_dir / f"{call_id}"
                try:
                    file_path = download_recording(
                        context,
                        recording_url,
                        dest_base,
                        default_ext=args.default_ext,
                    )
                    record_result(conn, call_id, recording_url, file_path, "success", None)
                    print(f"Saved {file_path}")
                except Exception as exc:
                    record_result(conn, call_id, recording_url, None, "failed", str(exc))
                    print(f"Failed {call_id}: {exc}")

            context.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
