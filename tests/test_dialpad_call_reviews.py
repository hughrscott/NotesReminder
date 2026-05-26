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
            1.
            Follow up with lesson options.
            2. Confirm trial availability.
            Transcript
            WU
            West U (Front Desk) 8:22 PM
            Thanks for calling.
            Caller
            8:23 PM
            I am interested in guitar lessons.
            Call audio seek slider
            0:00/0:56
            """,
        )

        self.assertEqual(parsed["call_review_id"], "call-123")
        self.assertEqual(parsed["recap_text"], "Caller asked about a trial lesson and next steps.")
        self.assertEqual(json.loads(parsed["action_items_json"]), ["Follow up with lesson options.", "Confirm trial availability."])
        self.assertIn("Thanks for calling.", parsed["transcript_text"])
        self.assertEqual(json.loads(parsed["speaker_turns_json"])[0]["speaker"], "West U (Front Desk)")
        self.assertEqual(json.loads(parsed["speaker_turns_json"])[1]["speaker"], "Caller")
        self.assertEqual(parsed["transcript_available"], 1)
        self.assertEqual(parsed["recap_available"], 1)
        self.assertEqual(parsed["action_items_available"], 1)
        self.assertEqual(parsed["audio_available"], 1)

    def test_parse_call_review_text_handles_rendered_dialpad_call_review(self):
        parsed = parse_call_review_text(
            "https://dialpad.com/callhistory/callreview/5646748416811008?",
            """
            CALL HISTORY / CALL REVIEW
            Calvin Barnhill's call with Kate Hall
            WEST U
            May 21, 2026 @ 2:07 pm - 2:11 pm Duration: 4 min
            Transcript search by keyword
            Add to playlist
            #Moments
            Action Item
            Interesting Question
            3
            Call Purpose
            1
            Positive Sentiment
            1
            Others
            Time
            3
            Date
            2
            0:00/3:19
            15
            15
            1x
            Show callers
            Recap
            Transcript
            Excerpts
            Calvin Barnhill 2:07 PM
            School of rock in west, this is Calvin. Can I help you?
            Kate Hall 2:07 PM
            Hi, yes, my name is Kate and I just signed my daughter up for the green day band camp and I had two questions.
            Calvin Barnhill 2:08 PM
            Okay.
            Mm-hmm yeah. I'm looking at a roster right now.
            Comments
            Transcript
            """,
        )

        turns = json.loads(parsed["speaker_turns_json"])
        self.assertEqual(parsed["call_review_id"], "5646748416811008")
        self.assertEqual(len(turns), 3)
        self.assertEqual(turns[0]["speaker"], "Calvin Barnhill")
        self.assertEqual(turns[1]["speaker"], "Kate Hall")
        self.assertIn("roster", turns[2]["text"])
        self.assertIn("green day band camp", parsed["transcript_text"])
        self.assertEqual(parsed["audio_available"], 1)

    def test_parse_call_review_text_handles_rendered_recap_tab_row(self):
        parsed = parse_call_review_text(
            "https://dialpad.com/callhistory/callreview/call-456",
            """
            CONVERSATION HISTORY / CALL REVIEW
            Recap
            Transcript
            Excerpts
            Caller asked whether the student would be too old for camp.
            Calvin Barnhill 2:07 PM
            School of rock in west, this is Calvin.
            """,
        )

        self.assertEqual(parsed["recap_text"], "Caller asked whether the student would be too old for camp.")
        self.assertEqual(parsed["recap_available"], 1)

    def test_parse_call_review_text_ignores_no_ai_recap_available(self):
        parsed = parse_call_review_text(
            "https://dialpad.com/callhistory/callreview/call-789",
            """
            Recap
            Transcript
            Excerpts
            No AI Recap available
            Transcript
            Greer Thomas
            3:15 PM
            Please leave a message.
            """,
        )

        self.assertIsNone(parsed["recap_text"])
        self.assertEqual(parsed["recap_available"], 0)

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

    def test_call_review_upsert_clears_stale_recap_when_source_has_none(self):
        conn = self.open_db()
        now = utc_now_iso()
        base = {
            "call_review_id": "call-123",
            "call_id": "call-123",
            "voice_event_id": "voice-123",
            "call_review_url": "https://dialpad.com/callhistory/callreview/call-123",
            "event_at": "2026-04-27T20:22:05",
            "transcript_text": "Caller asked about lessons.",
            "recap_text": "Transcript",
            "action_items_json": json.dumps([]),
            "speaker_turns_json": json.dumps([{"speaker": "Caller", "time": "8:23 PM", "text": "Interested."}]),
            "transcript_available": 1,
            "recap_available": 1,
            "action_items_available": 0,
            "audio_available": 1,
            "extraction_status": "success",
            "raw_json": json.dumps({"source": "test"}),
            "updated_at": now,
        }

        upsert_call_review(conn, base)
        upsert_call_review(
            conn,
            {
                **base,
                "recap_text": None,
                "recap_available": 0,
                "updated_at": utc_now_iso(),
            },
        )

        stored = conn.execute("SELECT recap_text, recap_available FROM dialpad_call_reviews").fetchone()
        self.assertIsNone(stored["recap_text"])
        self.assertEqual(stored["recap_available"], 0)


if __name__ == "__main__":
    unittest.main()
