import argparse
import json
import os
import sqlite3
import ssl
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
import certifi
from dotenv import load_dotenv


load_dotenv()


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


def get_pending_recordings(conn, limit=None):
    sql = """
        SELECT r.call_id, r.recording_url, r.duration
        FROM (
            SELECT
                call_id,
                recording_url,
                duration,
                ROW_NUMBER() OVER (
                    PARTITION BY call_id
                    ORDER BY date DESC
                ) AS rn
            FROM dialpad_recordings
            WHERE recording_url IS NOT NULL
              AND recording_url != ''
        ) r
        LEFT JOIN recording_transcripts t ON t.call_id = r.call_id
        WHERE t.call_id IS NULL
          AND r.rn = 1
        ORDER BY r.call_id
    """
    if limit:
        sql += " LIMIT ?"
        return conn.execute(sql, (limit,)).fetchall()
    return conn.execute(sql).fetchall()


def guess_media_format(url, headers, final_url=None):
    path = urllib.parse.urlparse(url).path
    ext = Path(path).suffix.lower().lstrip(".")
    if ext in {"mp3", "mp4", "wav", "flac", "ogg", "amr", "webm"}:
        return ext
    if final_url:
        final_path = urllib.parse.urlparse(final_url).path
        final_ext = Path(final_path).suffix.lower().lstrip(".")
        if final_ext in {"mp3", "mp4", "wav", "flac", "ogg", "amr", "webm"}:
            return final_ext
    content_type = (headers.get("Content-Type") or "").lower()
    if "audio/mpeg" in content_type:
        return "mp3"
    if "audio/wav" in content_type:
        return "wav"
    if "audio/flac" in content_type:
        return "flac"
    if "audio/ogg" in content_type:
        return "ogg"
    if "audio/webm" in content_type:
        return "webm"
    if "audio/amr" in content_type:
        return "amr"
    if "video/mp4" in content_type or "audio/mp4" in content_type:
        return "mp4"
    return None


def download_recording(url, dest_path, default_media_format=None):
    req = urllib.request.Request(url, headers={"User-Agent": "NotesReminder/1.0"})
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    with urllib.request.urlopen(req, context=ssl_context) as response:
        media_format = guess_media_format(url, response.headers, response.geturl())
        with open(dest_path, "wb") as f:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
    if media_format:
        return media_format
    return default_media_format


def start_transcription_job(client, job_name, media_uri, media_format, language_code, output_bucket, output_key):
    client.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode=language_code,
        MediaFormat=media_format,
        Media={"MediaFileUri": media_uri},
        OutputBucketName=output_bucket,
        OutputKey=output_key,
    )


def wait_for_job(client, job_name, poll_seconds):
    while True:
        response = client.get_transcription_job(TranscriptionJobName=job_name)
        job = response["TranscriptionJob"]
        status = job["TranscriptionJobStatus"]
        if status in ("COMPLETED", "FAILED"):
            return job
        time.sleep(poll_seconds)


def fetch_transcript_text(transcript_uri):
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    with urllib.request.urlopen(transcript_uri, context=ssl_context) as response:
        payload = json.loads(response.read().decode("utf-8"))
    transcripts = payload.get("results", {}).get("transcripts", [])
    if not transcripts:
        return ""
    return transcripts[0].get("transcript", "")


def delete_s3_object(s3, bucket, key):
    if bucket and key:
        s3.delete_object(Bucket=bucket, Key=key)


def process_recording(
    conn,
    s3,
    transcribe,
    bucket,
    call_id,
    recording_url,
    duration,
    language_code,
    poll_seconds,
    delete_after,
    verbose,
    preview_chars,
    default_media_format,
):
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    tmp_dir = Path("/tmp/notesreminder_recordings")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = tmp_dir / f"{call_id}"

    media_format = None
    s3_key = None
    transcript_key = None
    transcript_uri = None
    transcript_text = None
    status = "failed"
    error_message = None

    try:
        media_format = download_recording(
            recording_url,
            tmp_path,
            default_media_format=default_media_format,
        )
        if not media_format:
            raise RuntimeError("Could not determine media format for recording.")

        s3_key = f"recordings/{call_id}.{media_format}"
        s3.upload_file(str(tmp_path), bucket, s3_key)
        media_uri = f"s3://{bucket}/{s3_key}"

        job_name = f"call-{call_id}-{int(time.time())}"
        transcript_key = f"transcripts/{call_id}.json"
        start_transcription_job(
            transcribe,
            job_name,
            media_uri,
            media_format,
            language_code,
            bucket,
            transcript_key,
        )

        job = wait_for_job(transcribe, job_name, poll_seconds)
        status = job["TranscriptionJobStatus"].lower()
        if status == "failed":
            error_message = job.get("FailureReason")
        else:
            transcript_uri = job["Transcript"]["TranscriptFileUri"]
            transcript_text = fetch_transcript_text(transcript_uri)
            if verbose:
                preview = transcript_text
                if preview_chars and len(preview) > preview_chars:
                    preview = preview[:preview_chars].rstrip() + "..."
                print("Transcript preview:")
                print(preview)
    except Exception as exc:
        status = "failed"
        error_message = str(exc)
    finally:
        completed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        conn.execute(
            """
            INSERT OR REPLACE INTO recording_transcripts (
                call_id,
                recording_url,
                recording_duration,
                s3_bucket,
                s3_key,
                transcript_bucket,
                transcript_key,
                transcript_uri,
                transcript_text,
                transcript_provider,
                transcript_status,
                error_message,
                created_at,
                completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                call_id,
                recording_url,
                duration,
                bucket,
                s3_key,
                bucket,
                transcript_key,
                transcript_uri,
                transcript_text,
                "aws-transcribe",
                status,
                error_message,
                created_at,
                completed_at,
            ),
        )
        conn.commit()

        if delete_after:
            delete_s3_object(s3, bucket, s3_key)
            delete_s3_object(s3, bucket, transcript_key)
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
    return status, error_message, transcript_text


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transcribe Dialpad recordings via AWS Transcribe."
    )
    parser.add_argument(
        "--db",
        default="reminders.db",
        help="Path to the SQLite database (default: reminders.db)",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("TRANSCRIBE_BUCKET"),
        help="S3 bucket for recordings + transcripts (default: TRANSCRIBE_BUCKET)",
    )
    parser.add_argument(
        "--language-code",
        default="en-US",
        help="AWS Transcribe language code (default: en-US)",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=15,
        help="Polling interval for transcription job status",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of recordings to process (0 = no limit)",
    )
    parser.add_argument(
        "--delete-after",
        action="store_true",
        help="Delete audio + transcript objects from S3 after storing in DB",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print transcript text as each job completes",
    )
    parser.add_argument(
        "--preview-chars",
        type=int,
        default=500,
        help="Max characters to print per transcript when --verbose is set",
    )
    parser.add_argument(
        "--default-media-format",
        default=None,
        help="Fallback media format if none detected (e.g., mp3)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.bucket:
        raise SystemExit("Missing --bucket or TRANSCRIBE_BUCKET in .env.")

    conn = sqlite3.connect(args.db)
    try:
        ensure_table(conn)
        rows = get_pending_recordings(conn, limit=args.limit or None)
        if not rows:
            print("No new recordings to transcribe.")
            return
        s3 = boto3.client("s3")
        transcribe = boto3.client("transcribe")

        for call_id, recording_url, duration in rows:
            print(f"Transcribing call {call_id}")
            status, error_message, transcript_text = process_recording(
                conn,
                s3,
                transcribe,
                args.bucket,
                call_id,
                recording_url,
                duration,
                args.language_code,
                args.poll_seconds,
                args.delete_after,
                args.verbose,
                args.preview_chars,
                args.default_media_format,
            )
            if args.verbose:
                if status == "failed":
                    print(f"Call {call_id} failed: {error_message}")
                else:
                    preview_len = len(transcript_text or "")
                    print(f"Call {call_id} completed ({preview_len} chars).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
