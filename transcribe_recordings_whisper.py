import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


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


def get_recording_url(conn, call_id):
    row = conn.execute(
        "SELECT recording_url FROM recording_downloads WHERE call_id = ?",
        (call_id,),
    ).fetchone()
    return row[0] if row else None


def should_skip(conn, call_id):
    row = conn.execute(
        """
        SELECT transcript_status
        FROM recording_transcripts
        WHERE call_id = ?
        """,
        (call_id,),
    ).fetchone()
    if not row:
        return False
    return row[0] == "completed"


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
        INSERT OR REPLACE INTO recording_transcripts (
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
        """,
        (
            call_id,
            recording_url,
            transcript_text,
            "whisper-local",
            model_name,
            status,
            error_message,
            created_at,
            completed_at,
        ),
    )
    conn.commit()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transcribe local recordings with Whisper (CPU)."
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
    parser.add_argument(
        "--model",
        default="small",
        help="Whisper model size (default: small)",
    )
    parser.add_argument(
        "--language",
        default="en",
        help="Language hint for Whisper (default: en)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of recordings to process (0 = no limit)",
    )
    parser.add_argument(
        "--preview-chars",
        type=int,
        default=300,
        help="Preview length when --verbose is set",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print transcript previews as they complete",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        import whisper  # type: ignore
    except ImportError as exc:
        raise SystemExit(
            "Missing whisper package. Install with: pip install openai-whisper"
        ) from exc

    recordings_dir = Path(args.recordings_dir)
    if not recordings_dir.exists():
        raise SystemExit(f"Recordings directory not found: {recordings_dir}")

    conn = sqlite3.connect(args.db)
    try:
        ensure_table(conn)
        model = whisper.load_model(args.model)

        files = sorted(recordings_dir.glob("*.*"))
        processed = 0
        for path in files:
            call_id = path.stem
            if should_skip(conn, call_id):
                continue
            recording_url = get_recording_url(conn, call_id)
            print(f"Transcribing {call_id}")
            try:
                result = model.transcribe(
                    str(path),
                    language=args.language,
                    fp16=False,
                )
                transcript_text = (result.get("text") or "").strip()
                save_result(
                    conn,
                    call_id,
                    recording_url,
                    transcript_text,
                    "completed",
                    None,
                    args.model,
                )
                if args.verbose:
                    preview = transcript_text
                    if args.preview_chars and len(preview) > args.preview_chars:
                        preview = preview[: args.preview_chars].rstrip() + "..."
                    print("Transcript preview:")
                    print(preview)
            except Exception as exc:
                save_result(
                    conn,
                    call_id,
                    recording_url,
                    None,
                    "failed",
                    str(exc),
                    args.model,
                )

            processed += 1
            if args.limit and processed >= args.limit:
                break
    finally:
        conn.close()


if __name__ == "__main__":
    main()
