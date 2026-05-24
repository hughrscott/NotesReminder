import argparse
import json
import sqlite3
import time
from datetime import datetime, timezone

from openai import OpenAI


ANALYSIS_FIELDS = (
    "intent",
    "sentiment",
    "outcome",
    "action_items",
    "urgency",
    "topic",
    "summary",
)


def get_pending_transcripts(conn, limit=None, force=False):
    base_sql = """
        SELECT call_id, transcript_text
        FROM recording_transcripts
        WHERE transcript_status = 'completed'
          AND transcript_text IS NOT NULL
          AND trim(transcript_text) != ''
    """
    if not force:
        base_sql += " AND (intent IS NULL OR intent = '')"
    base_sql += " ORDER BY completed_at"
    if limit:
        base_sql += " LIMIT ?"
        return conn.execute(base_sql, (limit,)).fetchall()
    return conn.execute(base_sql).fetchall()


def has_meaningful_text(text: str) -> bool:
    return any(char.isalnum() for char in text or "")


def build_prompt(transcript):
    instructions = (
        "Extract structured call metadata. Return strict JSON with keys:\n"
        "intent (short label), sentiment (positive|neutral|negative|mixed),\n"
        "outcome (resolved|follow_up|voicemail|unknown),\n"
        "action_items (array of short strings), urgency (low|medium|high),\n"
        "topic (short label), summary (1-2 sentences).\n"
        "Use empty strings or empty arrays if unknown."
    )
    return f"{instructions}\n\nTranscript:\n{transcript}"


def parse_json_response(text):
    if not text or not text.strip():
        raise ValueError("Empty response from model")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start : end + 1])
        raise


def normalize_analysis(payload):
    normalized = {}
    for key in ANALYSIS_FIELDS:
        value = payload.get(key, "")
        if key == "action_items":
            if isinstance(value, list):
                normalized[key] = json.dumps(value)
            elif value:
                normalized[key] = json.dumps([str(value)])
            else:
                normalized[key] = json.dumps([])
        else:
            normalized[key] = str(value or "").strip()
    return normalized


def store_analysis(conn, call_id, analysis):
    completed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.execute(
        """
        UPDATE recording_transcripts
        SET intent = ?,
            sentiment = ?,
            outcome = ?,
            action_items = ?,
            urgency = ?,
            topic = ?,
            summary = ?,
            completed_at = ?
        WHERE call_id = ?
        """,
        (
            analysis["intent"],
            analysis["sentiment"],
            analysis["outcome"],
            analysis["action_items"],
            analysis["urgency"],
            analysis["topic"],
            analysis["summary"],
            completed_at,
            call_id,
        ),
    )
    conn.commit()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyze call transcripts with OpenAI and store tags."
    )
    parser.add_argument(
        "--db",
        default="reminders.db",
        help="Path to the SQLite database (default: reminders.db)",
    )
    parser.add_argument(
        "--model",
        default="gpt-4o-mini",
        help="OpenAI model name (default: gpt-4o-mini)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of transcripts to analyze (0 = no limit)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-analyze even if intent already exists",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Seconds to sleep between requests",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    client = OpenAI()

    conn = sqlite3.connect(args.db)
    try:
        rows = get_pending_transcripts(conn, limit=args.limit or None, force=args.force)
        if not rows:
            print("No transcripts to analyze.")
            return
        for call_id, transcript in rows:
            print(f"Analyzing call {call_id}")
            if not has_meaningful_text(transcript):
                print(f"Call {call_id} skipped: transcript has no meaningful text.")
                continue
            prompt = build_prompt(transcript)
            try:
                response = client.responses.create(
                    model=args.model,
                    input=prompt,
                    temperature=0,
                )
                text = response.output_text
                payload = parse_json_response(text)
                analysis = normalize_analysis(payload)
                store_analysis(conn, call_id, analysis)
            except Exception as exc:
                print(f"Call {call_id} failed: {exc}")
                time.sleep(args.sleep)
                continue
            time.sleep(args.sleep)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
