import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema, utc_now_iso
from scripts.lead_attention_report import build_attention_rows, render_report


class LeadAttentionReportTests(unittest.TestCase):
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

    def seed_lead_with_communications(self, conn):
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO hubspot_deals (
                deal_id, deal_name, stage, owner, school, create_date,
                last_contacted, follow_up_needed, source_url, raw_text, updated_at
            )
            VALUES ('deal-123', 'Sensitive Customer Name', 'Scheduled Trial', 'Owner A',
                    'West U', '2026-04-21', '2026-04-22', 'Yes',
                    'https://app.hubspot.com/deal/123', 'raw deal text', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, email_normalized, phone, phone_normalized,
                associated_deal_ids, source_url, raw_text, raw_json, updated_at
            )
            VALUES ('contact-123', 'Sensitive Customer Name', 'customer@example.com',
                    '(713) 555-1212', '7135551212', 'deal-123',
                    'https://app.hubspot.com/contact/123', 'raw contact text', ?, ?)
            """,
            (json.dumps({"trusted": True, "rejected_emails": []}), now),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_threads (
                thread_id, phone, phone_normalized, contact_name, school, source_url, raw_text, updated_at
            )
            VALUES ('thread-123', '(713) 555-1212', '7135551212',
                    'Sensitive Customer Name', 'West U',
                    'https://dialpad.com/thread/123', 'raw thread text', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_messages (
                message_id, thread_id, message_at, direction, body, source_url, raw_text, raw_json, updated_at
            )
            VALUES ('msg-123', 'thread-123', '2026-04-23T10:00:00', 'inbound',
                    'Sensitive SMS body', 'https://dialpad.com/message/123',
                    'raw sms text', ?, ?)
            """,
            (json.dumps({"extraction_source": "thread_detail", "direction_source": "observed"}), now),
        )
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, call_id, phone, phone_normalized,
                contact_name, direction, event_at, school, department, outcome,
                source_url, raw_text, raw_json, updated_at
            )
            VALUES ('voice-123', 'conversation_history', 'call', 'call-123',
                    '(713) 555-1212', '7135551212', 'Sensitive Customer Name',
                    'inbound', '2026-04-24T11:00:00', 'West U', 'WESTU',
                    'Conversation History row',
                    'https://dialpad.com/callhistory/callreview/call-123?source=session-history%3Adays%3D0-30%26external_endpoint%3D7135551212',
                    'raw voice text', ?, ?)
            """,
            (json.dumps({"extraction": "conversation_history_dom"}), now),
        )
        conn.execute(
            """
            INSERT INTO dialpad_call_reviews (
                call_review_id, call_id, voice_event_id, call_review_url, event_at,
                transcript_text, recap_text, action_items_json, speaker_turns_json,
                transcript_available, recap_available, action_items_available,
                audio_available, extraction_status, raw_json, updated_at
            )
            VALUES ('call-123', 'call-123', 'voice-123',
                    'https://dialpad.com/callhistory/callreview/call-123',
                    '2026-04-24T11:00:00', 'Sensitive transcript text',
                    'Sensitive recap text', '["Sensitive action item"]', '[]',
                    1, 1, 1, 1, 'success', '{}', ?)
            """,
            (now,),
        )

    def test_report_includes_evidence_counts_without_sensitive_content(self):
        conn = self.open_db()
        self.seed_lead_with_communications(conn)

        rows, start = build_attention_rows(conn, school="West U", window_days=7, limit=10)
        markdown = render_report(rows, school="West U", window_days=7, window_start_value=start)

        self.assertIn("deal-123", markdown)
        self.assertIn("follow_up_needed", markdown)
        self.assertIn("Candidate leads with matched Dialpad communications: 1", markdown)
        self.assertIn("Candidate leads with call-review transcripts: 1", markdown)
        self.assertIn("[HubSpot](https://app.hubspot.com/deal/123)", markdown)
        self.assertIn("[Dialpad 1](https://dialpad.com/callhistory/callreview/call-123)", markdown)

        for forbidden in [
            "Sensitive Customer Name",
            "(713) 555-1212",
            "7135551212",
            "external_endpoint",
            "Sensitive SMS body",
            "Sensitive transcript text",
            "Sensitive recap text",
            "Sensitive action item",
        ]:
            self.assertNotIn(forbidden, markdown)


if __name__ == "__main__":
    unittest.main()
