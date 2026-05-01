import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema, utc_now_iso
from scripts.extract_dialpad_daily_intake import conversation_history_window_url, enrich_daily_row
from scripts.unmatched_inbound_report import fetch_unmatched_rows, render_report


class UnmatchedInboundReportTests(unittest.TestCase):
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

    def test_window_url_uses_conversation_history_days_filter(self):
        self.assertEqual(
            conversation_history_window_url(2),
            "https://dialpad.com/conversationhistory?days=0-2",
        )
        self.assertEqual(
            conversation_history_window_url(7),
            "https://dialpad.com/conversationhistory?days=0-7",
        )

    def test_enrich_daily_row_preserves_source_timestamp_diagnostics(self):
        row = {"raw_json": json.dumps({"extraction": "conversation_history_dom"})}
        enriched = enrich_daily_row(row, 2, "West U", {"school_filter_applied": True})
        raw = json.loads(enriched["raw_json"])
        self.assertTrue(raw["daily_intake"])
        self.assertEqual(raw["window_days"], 2)
        self.assertEqual(raw["source_timestamp_field"], "event_at")
        self.assertEqual(raw["import_timestamp_field"], "updated_at")
        self.assertTrue(raw["filter_diagnostics"]["school_filter_applied"])

    def test_report_is_sanitized_and_counts_unmatched_inbound(self):
        conn = self.open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, call_id, phone, phone_normalized,
                contact_name, direction, event_at, school, department, outcome,
                voicemail_transcript, source_url, raw_text, raw_json, updated_at
            )
            VALUES (
                'voice-1', 'conversation_history', 'voicemail', 'call-1',
                '(713) 555-1212', '7135551212', 'Sensitive Caller', 'inbound',
                date('now'), 'West U', 'WESTU', 'voicemail',
                'Sensitive transcript text',
                'https://dialpad.com/callhistory/callreview/call-1?external_endpoint=7135551212',
                'raw sensitive row', '{}', ?
            )
            """,
            (now,),
        )
        rows, start = fetch_unmatched_rows(conn, "West U", 2, 10)
        markdown = render_report(rows, "West U", 2, start)

        self.assertIn("Unmatched inbound communications: 1", markdown)
        self.assertIn("Possible leads not in HubSpot: 1", markdown)
        self.assertIn("Voicemails: 1", markdown)
        self.assertIn("[source](https://dialpad.com/callhistory/callreview/call-1)", markdown)

        forbidden = [
            "Sensitive Caller",
            "(713) 555-1212",
            "7135551212",
            "Sensitive transcript text",
            "raw sensitive row",
            "external_endpoint",
        ]
        for value in forbidden:
            self.assertNotIn(value, markdown)


if __name__ == "__main__":
    unittest.main()
