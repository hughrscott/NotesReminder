import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


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


def get_recording_url(conn, call_id):
    row = conn.execute(
        """
        SELECT recording_url
        FROM dialpad_recordings
        WHERE call_id = ?
          AND recording_url IS NOT NULL
          AND recording_url != ''
        ORDER BY date DESC
        LIMIT 1
        """,
        (call_id,),
    ).fetchone()
    return row[0] if row else None


def parse_args():
    parser = argparse.ArgumentParser(
        description="Rebuild recording_downloads from local recordings directory."
    )
    parser.add_argument(
        "--db",
        default="reminders.db",
        help="Path to the SQLite database (default: reminders.db)",
    )
    parser.add_argument(
        "--recordings-dir",
        default="recordings",
        help="Directory containing downloaded recordings (default: recordings)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    recordings_dir = Path(args.recordings_dir)
    if not recordings_dir.exists():
        raise SystemExit(f"Recordings directory not found: {recordings_dir}")

    files = sorted(recordings_dir.glob("*.*"))
    if not files:
        print("No recordings found.")
        return

    conn = sqlite3.connect(args.db)
    try:
        ensure_table(conn)
        inserted = 0
        updated = 0
        for path in files:
            call_id = path.stem
            recording_url = get_recording_url(conn, call_id)
            downloaded_at = datetime.fromtimestamp(
                path.stat().st_mtime, tz=timezone.utc
            ).isoformat(timespec="seconds")
            cur = conn.execute(
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
                    str(path),
                    "success",
                    None,
                    downloaded_at,
                ),
            )
            if cur.rowcount == 1:
                inserted += 1
            else:
                updated += 1
        conn.commit()
        print(f"Rebuilt recording_downloads for {len(files)} files.")
        print(f"Inserted/updated rows: {inserted + updated}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
