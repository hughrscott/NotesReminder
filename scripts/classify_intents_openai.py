import argparse
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI


load_dotenv()


BUCKETS = [
    "New Lead – Trial Request",
    "New Lead – Pricing/Info",
    "New Lead – Schedule/Availability",
    "Active Student – Reschedule",
    "Active Student – Cancel/Refund/Account",
    "Active Student – General Admin",
    "Staff/Internal",
    "Vendor/Sales Call",
    "Wrong Number / Spam / Robocall",
    "Voicemail-only / No Content",
    "Follow-up / Callback",
    "Unknown / Unclear",
]


SYSTEM_PROMPT = f"""You classify phone calls for a music school.
Return strict JSON with keys: bucket, confidence, reason.
bucket must be one of:
{json.dumps(BUCKETS, indent=2)}

Rules:
- If direction is "outbound", do NOT classify as a New Lead bucket. Prefer Follow-up / Callback or Active Student – General Admin unless transcript proves otherwise.
- If caller refers to "my lesson", "my child", "reschedule", "cancel", "already enrolled", or confirms existing schedule => Active Student bucket.
- New Lead buckets require BOTH: (a) lesson/program interest AND (b) clear school context (e.g., School of Rock or "your school").
- If caller asks about trials, pricing, availability, or how to start AND is not an existing student AND school context is clear => New Lead bucket.
- If transcript content is not about music lessons/programs or is clearly unrelated (insurance, prescriptions, marketing, etc.), classify as Vendor/Sales Call or Wrong Number/Spam.
- If transcript is voicemail system or empty => Voicemail-only / No Content.
- If caller is returning a call or asks for a callback => Follow-up / Callback.
- Use transcript + summary + intent + outcome. Do not guess beyond evidence.
"""


def parse_args():
    parser = argparse.ArgumentParser(description="Classify call intents with OpenAI.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--model", default="gpt-4o-mini")
    parser.add_argument("--sleep", type=float, default=0.2)
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).isoformat(timespec="seconds"))
    parser.add_argument("--version", default="v2-direction-school-context")
    parser.add_argument("--force", action="store_true", help="Reclassify even if intent_bucket_ai is set")
    parser.add_argument("--log-file", default="logs/classify_intents_errors.log")
    return parser.parse_args()


def ensure_column(conn):
    columns = {row[1] for row in conn.execute("PRAGMA table_info(recording_transcripts)").fetchall()}
    if "intent_bucket_ai" not in columns:
        conn.execute("ALTER TABLE recording_transcripts ADD COLUMN intent_bucket_ai TEXT")
    if "intent_bucket_ai_confidence" not in columns:
        conn.execute("ALTER TABLE recording_transcripts ADD COLUMN intent_bucket_ai_confidence TEXT")
    if "intent_bucket_ai_reason" not in columns:
        conn.execute("ALTER TABLE recording_transcripts ADD COLUMN intent_bucket_ai_reason TEXT")
    if "intent_bucket_ai_run_id" not in columns:
        conn.execute("ALTER TABLE recording_transcripts ADD COLUMN intent_bucket_ai_run_id TEXT")
    if "intent_bucket_ai_version" not in columns:
        conn.execute("ALTER TABLE recording_transcripts ADD COLUMN intent_bucket_ai_version TEXT")
    if "intent_bucket_ai_updated_at" not in columns:
        conn.execute("ALTER TABLE recording_transcripts ADD COLUMN intent_bucket_ai_updated_at TEXT")
    conn.commit()


def build_payload(row):
    return {
        "call_id": row["call_id"],
        "direction": row["direction"],
        "intent": row["intent"],
        "outcome": row["outcome"],
        "summary": row["summary"],
        "transcript_text": row["transcript_text"],
    }


def parse_json(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start : end + 1])
        raise


def main():
    args = parse_args()
    client = OpenAI()
    log_path = Path(args.log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        ensure_column(conn)
        where_clause = """
            r.transcript_status = 'completed'
            AND r.transcript_text IS NOT NULL
            AND trim(r.transcript_text) != ''
        """
        if not args.force:
            where_clause += " AND (r.intent_bucket_ai IS NULL OR r.intent_bucket_ai = '')"
        rows = conn.execute(
            """
            SELECT r.call_id, r.intent, r.outcome, r.summary, r.transcript_text, d.direction
            FROM recording_transcripts r
            JOIN dialpad_calls d ON d.call_id = r.call_id
            WHERE
            """
            + where_clause
            + """
            GROUP BY r.call_id
            ORDER BY completed_at
            LIMIT ?
            """,
            (args.limit,),
        ).fetchall()

        if not rows:
            print("No transcripts to classify.")
            return

        for row in rows:
            payload = build_payload(row)
            retries = 3
            while True:
                try:
                    response = client.responses.create(
                        model=args.model,
                        input=[
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": json.dumps(payload, ensure_ascii=True)},
                        ],
                        temperature=0,
                    )
                    text = response.output_text
                    data = parse_json(text)
                    bucket = data.get("bucket", "Unknown / Unclear")
                    confidence = data.get("confidence", "low")
                    reason = data.get("reason", "")
                    if bucket not in BUCKETS:
                        bucket = "Unknown / Unclear"
                    conn.execute(
                        """
                        UPDATE recording_transcripts
                        SET intent_bucket_ai = ?,
                            intent_bucket_ai_confidence = ?,
                            intent_bucket_ai_reason = ?,
                            intent_bucket_ai_run_id = ?,
                            intent_bucket_ai_version = ?,
                            intent_bucket_ai_updated_at = ?
                        WHERE call_id = ?
                        """,
                        (
                            bucket,
                            confidence,
                            reason,
                            args.run_id,
                            args.version,
                            datetime.now(timezone.utc).isoformat(timespec="seconds"),
                            row["call_id"],
                        ),
                    )
                    conn.commit()
                    print(f"{row['call_id']} -> {bucket} ({confidence})")
                    break
                except Exception as exc:
                    with log_path.open("a", encoding="utf-8") as f:
                        f.write(
                            f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} "
                            f"call_id={row['call_id']} error={exc}\n"
                        )
                    retries -= 1
                    if retries < 0:
                        print(f"{row['call_id']} failed after retries: {exc}")
                        break
                    time.sleep(2)
            time.sleep(args.sleep)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
