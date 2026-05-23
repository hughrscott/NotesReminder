import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema, utc_now_iso
from notesreminder.lib.person_identity import (
    customer_lifecycle_summary,
    person_journey,
    refresh_person_identities,
)


class PersonJourneyTests(unittest.TestCase):
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

    def seed_journey(self, conn):
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO hubspot_deals (
                deal_id, deal_name, stage, owner, school, create_date,
                trial_date, source_url, raw_text, updated_at
            )
            VALUES ('deal-1', 'Customer One', 'Scheduled Trial', 'Owner',
                    'West U', '2026-04-01T09:00:00', '2026-04-04',
                    'https://hubspot/deal-1', 'raw deal', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, email_normalized, phone_normalized,
                associated_deal_ids, raw_json, updated_at
            )
            VALUES ('contact-1', 'Customer One', 'customer@example.com',
                    '7135551212', 'deal-1', ?, ?)
            """,
            (json.dumps({"trusted": True}), now),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_threads (
                thread_id, phone_normalized, contact_name, school, updated_at
            )
            VALUES ('thread-1', '7135551212', 'Customer One', 'West U', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_messages (
                message_id, thread_id, message_at, direction, body, source_url,
                raw_text, raw_json, updated_at
            )
            VALUES ('sms-1', 'thread-1', '2026-04-02T10:00:00', 'inbound',
                    'Sensitive SMS body', 'https://dialpad/message-1',
                    'raw sms', '{}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, email_normalized, phone_normalized,
                school, updated_at
            )
            VALUES ('pike-1', 'Customer One', 'customer@example.com',
                    '7135551212', 'West U', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO pike13_visits (
                visit_id, person_id, service, starts_at, status,
                first_visit_flag, school, source_url, updated_at
            )
            VALUES ('visit-1', 'pike-1', 'Trial Lesson', '2026-04-04T12:00:00',
                    'completed', 1, 'West U', 'https://pike/visit-1', ?)
            """,
            (now,),
        )
        refresh_person_identities(conn)
        return conn.execute("SELECT person_id FROM persons LIMIT 1").fetchone()["person_id"]

    def test_person_journey_is_chronological_and_sanitized_by_default(self):
        conn = self.open_db()
        person_id = self.seed_journey(conn)

        result = person_journey(conn, person_id, limit=10)

        self.assertEqual(result["person_ids"], [person_id])
        self.assertEqual([event["event_type"] for event in result["events"]], [
            "lead_created",
            "dialpad_sms",
            "pike13_trial_visit",
        ])
        serialized = json.dumps(result)
        self.assertNotIn("Sensitive SMS body", serialized)
        self.assertNotIn("https://dialpad/message-1", serialized)
        self.assertNotIn("detail", serialized)

    def test_person_journey_can_include_sensitive_detail_when_requested(self):
        conn = self.open_db()
        person_id = self.seed_journey(conn)

        result = person_journey(conn, person_id, limit=10, include_sensitive=True)

        serialized = json.dumps(result)
        self.assertIn("Sensitive SMS body", serialized)
        self.assertIn("https://dialpad/message-1", serialized)

    def test_customer_lifecycle_summary_counts_events(self):
        conn = self.open_db()
        person_id = self.seed_journey(conn)

        summary = customer_lifecycle_summary(conn, person_id)

        self.assertEqual(summary["event_count"], 3)
        self.assertEqual(summary["first_event"]["event_type"], "lead_created")
        self.assertEqual(summary["latest_event"]["event_type"], "pike13_trial_visit")
        self.assertEqual(summary["event_counts"]["hubspot:lead_created"], 1)


if __name__ == "__main__":
    unittest.main()
