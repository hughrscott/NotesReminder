import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema, utc_now_iso
from notesreminder.lib.person_identity import person_details, person_search, refresh_person_identities


class PersonIdentityTests(unittest.TestCase):
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

    def seed_matched_person(self, conn):
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO hubspot_deals (
                deal_id, deal_name, stage, school, create_date,
                pike13_person_id, source_url, raw_text, updated_at
            )
            VALUES ('deal-1', 'Customer One', 'Scheduled Trial', 'West U',
                    '2026-04-22', 'pike-1', 'https://hubspot/deal-1',
                    'raw deal', ?)
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
            INSERT INTO pike13_people (
                person_id, full_name, email_normalized, phone_normalized,
                school, source_url, raw_text, updated_at
            )
            VALUES ('pike-1', 'Customer One', 'customer@example.com',
                    '7135551212', 'West U', 'https://pike/person/pike-1',
                    'raw person', ?)
            """,
            (now,),
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
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, phone_normalized,
                contact_name, event_at, school, updated_at
            )
            VALUES ('voice-1', 'conversation_history', 'call', '7135551212',
                    'Customer One', '2026-04-23T10:00:00', 'West U', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO school_email_messages (
                message_id, school_mailbox, school, direction, message_at,
                external_email_normalized, updated_at
            )
            VALUES ('email-1', 'westu@schoolofrock.com', 'West U', 'inbound',
                    '2026-04-23T11:00:00', 'customer@example.com', ?)
            """,
            (now,),
        )

    def test_refresh_person_identities_links_exact_source_records(self):
        conn = self.open_db()
        self.seed_matched_person(conn)

        first = refresh_person_identities(conn)
        second = refresh_person_identities(conn)

        self.assertEqual(first, second)
        self.assertEqual(first["persons"], 1)
        self.assertEqual(first["conflicts"], 0)
        person_id = conn.execute("SELECT person_id FROM persons").fetchone()["person_id"]
        self.assertTrue(person_id.startswith("person_"))
        for table, column, source_id_column, source_id in (
            ("hubspot_deals", "person_id", "deal_id", "deal-1"),
            ("hubspot_contacts", "person_id", "contact_id", "contact-1"),
            ("pike13_people", "person_identity_id", "person_id", "pike-1"),
            ("dialpad_sms_threads", "person_id", "thread_id", "thread-1"),
            ("dialpad_voice_events", "person_id", "event_id", "voice-1"),
            ("school_email_messages", "person_id", "message_id", "email-1"),
        ):
            stored = conn.execute(
                f"SELECT {column} FROM {table} WHERE {source_id_column} = ?",
                (source_id,),
            ).fetchone()[column]
            self.assertEqual(stored, person_id)

    def test_person_search_and_details_return_resolved_identity(self):
        conn = self.open_db()
        self.seed_matched_person(conn)
        refresh_person_identities(conn)
        result = person_search(conn, "customer@example.com", limit=5)
        self.assertEqual(len(result), 1)
        details = person_details(conn, result[0]["person_id"])
        self.assertEqual(details["person"]["primary_email"], "customer@example.com")
        identity_types = {row["identity_type"] for row in details["identities"]}
        self.assertIn("email", identity_types)
        self.assertIn("phone", identity_types)
        self.assertIn("pike13_person", identity_types)

    def test_duplicate_source_identities_are_marked_as_conflicts(self):
        conn = self.open_db()
        now = utc_now_iso()
        for person_id in ("pike-1", "pike-2"):
            conn.execute(
                """
                INSERT INTO pike13_people (
                    person_id, full_name, email_normalized, phone_normalized,
                    school, updated_at
                )
                VALUES (?, ?, 'shared@example.com', NULL, 'West U', ?)
                """,
                (person_id, person_id, now),
            )

        summary = refresh_person_identities(conn)
        self.assertEqual(summary["persons"], 1)
        self.assertEqual(summary["conflicts"], 1)
        conflict = conn.execute("SELECT conflict_type FROM person_resolution_conflicts").fetchone()
        self.assertEqual(conflict["conflict_type"], "multiple_pike13_person")


if __name__ == "__main__":
    unittest.main()
