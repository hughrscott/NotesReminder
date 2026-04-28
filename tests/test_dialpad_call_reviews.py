import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema, utc_now_iso
from scripts.extract_dialpad_call_reviews import parse_call_review_text, upsert_call_review
from source_completeness import build_source_completeness_report


class DialpadCallReviewTests(unittest.TestCase):
    def open_db(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = Path(tmp.name) / "test.db"
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE call_logs (
                call_id TEXT PRIMARY KEY,
                external_number TEXT,
                date_started TEXT,
                direction TEXT,
                category TEXT,
                name TEXT,
                school_code TEXT,
                school_name TEXT,
                voicemail_transcript TEXT,
                voicemail_recording_url TEXT,
                recording_url TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE recording_transcripts (
                call_id TEXT PRIMARY KEY,
                recording_url TEXT,
                transcript_text TEXT,
                outcome TEXT,
                summary TEXT
            )
            """
        )
        ensure_lead_followup_schema(conn)
        return conn

    def test_parse_call_review_text_extracts_sanitized_sections(self):
        parsed = parse_call_review_text(
            "https://dialpad.com/callhistory/callreview/call-123",
            """
            Call Review
            Recap
            Caller asked about a trial lesson and next steps.
            Action Items
            1. Follow up with lesson options.
            2. Confirm trial availability.
            Transcript
            West U (Front Desk) 8:22 PM Thanks for calling.
            Caller 8:23 PM I am interested in guitar lessons.
            Call audio seek slider
            0:00/0:56
            """,
        )

        self.assertEqual(parsed["call_review_id"], "call-123")
        self.assertEqual(parsed["recap_text"], "Caller asked about a trial lesson and next steps.")
        self.assertEqual(json.loads(parsed["action_items_json"]), ["Follow up with lesson options.", "Confirm trial availability."])
        self.assertIn("Thanks for calling.", parsed["transcript_text"])
        self.assertEqual(parsed["transcript_available"], 1)
        self.assertEqual(parsed["recap_available"], 1)
        self.assertEqual(parsed["action_items_available"], 1)
        self.assertEqual(parsed["audio_available"], 1)

    def test_call_review_upsert_is_idempotent_and_reported(self):
        conn = self.open_db()
        now = utc_now_iso()
        row = {
            "call_review_id": "call-123",
            "call_id": "call-123",
            "voice_event_id": "voice-123",
            "call_review_url": "https://dialpad.com/callhistory/callreview/call-123",
            "event_at": "2026-04-27T20:22:05",
            "transcript_text": "Caller asked about lessons.",
            "recap_text": "Trial interest.",
            "action_items_json": json.dumps(["Follow up."]),
            "speaker_turns_json": json.dumps([{"speaker": "Caller", "time": "8:23 PM", "text": "Interested."}]),
            "transcript_available": 1,
            "recap_available": 1,
            "action_items_available": 1,
            "audio_available": 1,
            "extraction_status": "success",
            "raw_json": json.dumps({"source": "test"}),
            "updated_at": now,
        }

        upsert_call_review(conn, row)
        upsert_call_review(conn, {**row, "recap_text": "Updated recap.", "updated_at": utc_now_iso()})

        stored = conn.execute("SELECT * FROM dialpad_call_reviews").fetchall()
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0]["recap_text"], "Updated recap.")

        report = build_source_completeness_report(conn, window_days=7, pike13_lookahead_days=30)
        dialpad = report["sources"]["dialpad"]
        self.assertEqual(dialpad["call_review_rows"], 1)
        self.assertEqual(dialpad["call_review_transcript_rows"], 1)
        self.assertEqual(dialpad["call_review_recap_rows"], 1)
        self.assertEqual(dialpad["call_review_action_item_rows"], 1)


if __name__ == "__main__":
    unittest.main()
