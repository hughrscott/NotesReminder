import argparse
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI


load_dotenv()


def is_retryable_openai_error(exc):
    status_code = getattr(exc, "status_code", None)
    if status_code in {429, 500, 502, 503, 504}:
        return True
    message = str(exc).lower()
    return any(marker in message for marker in ("rate limit", "429", "500", "502", "503", "504", "temporarily"))


def call_with_backoff(fn, attempts=3, base_sleep=1.0):
    last_exc = None
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if not is_retryable_openai_error(exc) or attempt == attempts - 1:
                raise
            time.sleep(base_sleep * (2 ** attempt))
    raise last_exc


def ensure_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recording_transcripts (
            call_id TEXT PRIMARY KEY,
            recording_url TEXT,
            recording_duration TEXT,
            s3_bucket TEXT,
            s3_key TEXT,
            transcript_bucket TEXT,
            transcript_key TEXT,
            transcript_uri TEXT,
            transcript_text TEXT,
            transcript_provider TEXT,
            transcript_model TEXT,
            transcript_status TEXT,
            error_message TEXT,
            intent TEXT,
            sentiment TEXT,
            outcome TEXT,
            action_items TEXT,
            urgency TEXT,
            topic TEXT,
            summary TEXT,
            created_at TEXT,
            completed_at TEXT
        )
        """
    )
    columns = {
        row[1] for row in conn.execute("PRAGMA table_info(recording_transcripts)").fetchall()
    }
    for column, col_type in (
        ("transcript_model", "TEXT"),
        ("intent", "TEXT"),
        ("sentiment", "TEXT"),
        ("outcome", "TEXT"),
        ("action_items", "TEXT"),
        ("urgency", "TEXT"),
        ("topic", "TEXT"),
        ("summary", "TEXT"),
    ):
        if column not in columns:
            conn.execute(f"ALTER TABLE recording_transcripts ADD COLUMN {column} {col_type}")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_recording_transcripts_status "
        "ON recording_transcripts(transcript_status)"
    )


def claim_recording(conn, call_id, recording_url, model_name, force):
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    row = conn.execute(
        "SELECT transcript_status FROM recording_transcripts WHERE call_id = ?",
        (call_id,),
    ).fetchone()
    if row:
        status = row[0]
        if not force and status in ("completed", "in_progress", "failed_permanent"):
            return False
        conn.execute(
            """
            UPDATE recording_transcripts
            SET transcript_status = ?, transcript_provider = ?, transcript_model = ?, error_message = NULL, created_at = ?
            WHERE call_id = ?
            """,
            ("in_progress", "openai-whisper", model_name, created_at, call_id),
        )
        conn.commit()
        return True
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO recording_transcripts (
            call_id,
            recording_url,
            transcript_provider,
            transcript_model,
            transcript_status,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            call_id,
            recording_url,
            "openai-whisper",
            model_name,
            "in_progress",
            created_at,
        ),
    )
    conn.commit()
    return cur.rowcount == 1


def save_result(
    conn,
    call_id,
    recording_url,
    transcript_text,
    status,
    error_message,
    model_name,
):
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    completed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO recording_transcripts (
            call_id,
            recording_url,
            transcript_text,
            transcript_provider,
            transcript_model,
            transcript_status,
            error_message,
            created_at,
            completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(call_id) DO UPDATE SET
            recording_url = excluded.recording_url,
            transcript_text = excluded.transcript_text,
            transcript_provider = excluded.transcript_provider,
            transcript_model = excluded.transcript_model,
            transcript_status = excluded.transcript_status,
            error_message = excluded.error_message,
            completed_at = excluded.completed_at
        """,
        (
            call_id,
            recording_url,
            transcript_text,
            "openai-whisper",
            model_name,
            status,
            error_message,
            created_at,
            completed_at,
        ),
    )
    conn.commit()


def get_pending_downloads(conn, limit=None, force=False, call_ids=None):
    base_sql = """
        SELECT d.call_id, d.recording_url, d.file_path
        FROM recording_downloads d
        LEFT JOIN recording_transcripts t ON t.call_id = d.call_id
        WHERE d.status = 'success'
          AND d.file_path IS NOT NULL
    """
    params = []
    if call_ids:
        placeholders = ",".join("?" for _ in call_ids)
        base_sql += f" AND d.call_id IN ({placeholders})"
        params.extend(call_ids)
    if not force:
        base_sql += " AND (t.call_id IS NULL OR t.transcript_status != 'completed')"
    base_sql += " ORDER BY d.downloaded_at"
    if limit:
        base_sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(base_sql, params).fetchall()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transcribe downloaded recordings with OpenAI Whisper."
    )
    parser.add_argument(
        "--db",
        default="reminders.db",
        help="Path to the SQLite database (default: reminders.db)",
    )
    parser.add_argument(
        "--model",
        default="whisper-1",
        help="OpenAI Whisper model (default: whisper-1)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of recordings to process (0 = no limit)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-transcribe even if a transcript already exists",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Seconds to sleep between requests",
    )
    parser.add_argument(
        "--call-ids",
        default="",
        help="Comma-separated call_ids to process (overrides normal selection)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    client = OpenAI()
    call_ids = [item.strip() for item in args.call_ids.split(",") if item.strip()]

    conn = sqlite3.connect(args.db)
    try:
        ensure_table(conn)
        rows = get_pending_downloads(
            conn,
            limit=args.limit or None,
            force=args.force,
            call_ids=call_ids or None,
        )
        if not rows:
            print("No recordings to transcribe.")
            return
        for call_id, recording_url, file_path in rows:
            path = Path(file_path)
            if not path.exists():
                save_result(
                    conn,
                    call_id,
                    recording_url,
                    None,
                    "failed_permanent",
                    f"Missing file: {path}",
                    args.model,
                )
                continue
            if not claim_recording(conn, call_id, recording_url, args.model, args.force):
                continue
            print(f"Transcribing {call_id}")
            try:
                with path.open("rb") as f:
                    result = call_with_backoff(
                        lambda: client.audio.transcriptions.create(
                            model=args.model,
                            file=f,
                        )
                    )
                transcript_text = (result.text or "").strip()
                save_result(
                    conn,
                    call_id,
                    recording_url,
                    transcript_text,
                    "completed",
                    None,
                    args.model,
                )
            except Exception as exc:
                save_result(
                    conn,
                    call_id,
                    recording_url,
                    None,
                    "failed_permanent",
                    str(exc),
                    args.model,
                )
            if args.sleep:
                time.sleep(args.sleep)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
