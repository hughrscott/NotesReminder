import argparse
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List


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


def claim_recording(conn, call_id, recording_url, model_name, force):
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    row = conn.execute(
        "SELECT transcript_status FROM recording_transcripts WHERE call_id = ?",
        (call_id,),
    ).fetchone()
    if row and not force:
        return False
    if row and force:
        conn.execute(
            """
            UPDATE recording_transcripts
            SET transcript_status = ?, transcript_provider = ?, transcript_model = ?, error_message = NULL, created_at = ?
            WHERE call_id = ?
            """,
            ("in_progress", "whisper-local", model_name, created_at, call_id),
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
            "whisper-local",
            model_name,
            "in_progress",
            created_at,
        ),
    )
    conn.commit()
    return cur.rowcount == 1


def chunk_list(items: List[str], chunks: int) -> List[List[str]]:
    if chunks <= 1:
        return [items]
    size = max(1, math.ceil(len(items) / chunks))
    return [items[i : i + size] for i in range(0, len(items), size)]


def transcribe_files(file_list: Iterable[str], args):
    try:
        import whisper  # type: ignore
    except ImportError as exc:
        raise SystemExit(
            "Missing whisper package. Install with: pip install openai-whisper"
        ) from exc

    conn = sqlite3.connect(args.db)
    try:
        ensure_table(conn)
        model = whisper.load_model(args.model, device=args.device)
        for file_path in file_list:
            path = Path(file_path)
            call_id = path.stem
            recording_url = get_recording_url(conn, call_id)
            if not claim_recording(
                conn,
                call_id,
                recording_url,
                args.model,
                args.force,
            ):
                continue
            print(f"Transcribing {call_id}")
            try:
                result = model.transcribe(
                    str(path),
                    language=args.language,
                    fp16=args.fp16,
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
    finally:
        conn.close()


def filter_pending_files(db_path: str, file_list: List[str], force: bool) -> List[str]:
    if force:
        return file_list
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT call_id FROM recording_transcripts WHERE transcript_status = 'completed'"
        ).fetchall()
    finally:
        conn.close()
    completed = {row[0] for row in rows}
    return [path for path in file_list if Path(path).stem not in completed]


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
        "--device",
        default="cpu",
        help="Device for Whisper (cpu or mps; default: cpu)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes (default: 1)",
    )
    parser.add_argument(
        "--language",
        default="en",
        help="Language hint for Whisper (default: en)",
    )
    parser.add_argument(
        "--fp16",
        action="store_true",
        help="Enable fp16 inference (default: off)",
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
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-transcribe even if a transcript already exists",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.device == "mps":
        try:
            import torch  # type: ignore
        except ImportError:
            print("torch not available; falling back to cpu.")
            args.device = "cpu"
        else:
            if not torch.backends.mps.is_available():
                print("MPS not available; falling back to cpu.")
                args.device = "cpu"

    recordings_dir = Path(args.recordings_dir)
    if not recordings_dir.exists():
        raise SystemExit(f"Recordings directory not found: {recordings_dir}")

    files = sorted(recordings_dir.glob("*.*"))
    file_list = [str(path) for path in files]
    file_list = filter_pending_files(args.db, file_list, args.force)
    if args.limit:
        file_list = file_list[: args.limit]
    if args.verbose:
        print(f"Device: {args.device}")
        print(f"Recordings dir: {recordings_dir}")
        print(f"Found {len(file_list)} file(s) to consider")
        if file_list:
            print(f"First file: {file_list[0]}")
    if not file_list:
        print("No recordings found to transcribe.")
        return

    if args.workers <= 1:
        transcribe_files(file_list, args)
        return

    from multiprocessing import get_context

    chunks = chunk_list(file_list, args.workers)
    ctx = get_context("spawn")
    with ctx.Pool(processes=args.workers) as pool:
        pool.starmap(transcribe_files, [(chunk, args) for chunk in chunks])


if __name__ == "__main__":
    main()
