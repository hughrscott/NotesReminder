import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema, utc_now_iso
from scripts.backfill_dialpad_sms_thread_phones import (
    backfill_sms_thread_phones,
    backfill_voice_event_phones,
)


class DialpadSmsPhoneBackfillTests(unittest.TestCase):
    def open_db(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = Path(tmp.name) / "test.db"
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        ensure_lead_followup_schema(conn)
        return conn

    def test_backfills_unique_same_school_name_match_only(self):
        conn = self.open_db()
        now = utc_now_iso()
        contacts = [
            ("hubspot-1", "Unique Parent", "The Heights", "7135551212"),
            ("hubspot-2", "Ambiguous Parent", "The Heights", "7135550001"),
            ("hubspot-3", "Ambiguous Parent", "The Heights", "7135550002"),
            ("hubspot-4", "Unique Parent", "West University Place", "8325551212"),
        ]
        for contact_id, name, school, phone in contacts:
            conn.execute(
                """
                INSERT INTO hubspot_contacts (
                    contact_id, full_name, school, phone_normalized, raw_json, updated_at
                )
                VALUES (?, ?, ?, ?, '{}', ?)
                """,
                (contact_id, name, school, phone, now),
            )
        for thread_id, name, school in [
            ("thread-unique", "Unique Parent", "The Heights"),
            ("thread-ambiguous", "Ambiguous Parent", "The Heights"),
            ("thread-other-school", "Unique Parent", "West U"),
        ]:
            conn.execute(
                """
                INSERT INTO dialpad_sms_threads (
                    thread_id, feed_id, contact_name, school, raw_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (thread_id, thread_id, name, school, json.dumps({"extraction": "message_list_row"}), now),
            )
        conn.commit()

        dry = backfill_sms_thread_phones(conn, execute=False)
        applied = backfill_sms_thread_phones(conn, execute=True)

        self.assertEqual(dry["matched_threads"], 2)
        self.assertEqual(applied["updated_threads"], 2)
        rows = {
            row["thread_id"]: row
            for row in conn.execute(
                "SELECT thread_id, phone_normalized, raw_json FROM dialpad_sms_threads ORDER BY thread_id"
            )
        }
        self.assertIsNone(rows["thread-ambiguous"]["phone_normalized"])
        self.assertEqual(rows["thread-unique"]["phone_normalized"], "7135551212")
        self.assertEqual(rows["thread-other-school"]["phone_normalized"], "8325551212")
        raw = json.loads(rows["thread-unique"]["raw_json"])
        self.assertEqual(raw["phone_backfill_method"], "hubspot_unique_same_school_name")

    def test_backfills_voice_events_from_phone_label_and_reversed_name(self):
        conn = self.open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, school, phone_normalized, raw_json, updated_at
            )
            VALUES ('hubspot-1', 'Emily Trof', 'West University Place', '7135551212', '{}', ?)
            """,
            (now,),
        )
        for event_id, name in [
            ("event-reversed-name", "Trof Emily"),
            ("event-phone-label", "(832) 555-1212"),
        ]:
            conn.execute(
                """
                INSERT INTO dialpad_voice_events (
                    event_id, source_view, event_type, contact_name, school, event_at, raw_json, updated_at
                )
                VALUES (?, 'conversation_history', 'call', ?, 'West U', '2026-06-10', '{}', ?)
                """,
                (event_id, name, now),
            )
        conn.commit()

        dry = backfill_voice_event_phones(conn, execute=False)
        applied = backfill_voice_event_phones(conn, execute=True)

        self.assertEqual(dry["matched_events"], 2)
        self.assertEqual(applied["updated_events"], 2)
        rows = {
            row["event_id"]: row
            for row in conn.execute("SELECT event_id, phone_normalized, raw_json FROM dialpad_voice_events")
        }
        self.assertEqual(rows["event-reversed-name"]["phone_normalized"], "7135551212")
        self.assertEqual(rows["event-phone-label"]["phone_normalized"], "8325551212")
        raw = json.loads(rows["event-reversed-name"]["raw_json"])
        self.assertEqual(raw["phone_backfill_method"], "hubspot_unique_same_school_name")

    def test_backfills_sms_threads_from_unique_same_school_pike13_name(self):
        conn = self.open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, school, phone_normalized, raw_json, updated_at
            )
            VALUES ('person-1', 'Pike13 Parent', 'The Heights', '8325551212', '{}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_threads (
                thread_id, feed_id, contact_name, school, raw_json, updated_at
            )
            VALUES ('thread-pike13', 'thread-pike13', 'Pike13 Parent', 'The Heights',
                    '{"extraction": "message_list_row"}', ?)
            """,
            (now,),
        )

        dry = backfill_sms_thread_phones(conn, execute=False)
        applied = backfill_sms_thread_phones(conn, execute=True)

        self.assertEqual(dry["matched_threads"], 1)
        self.assertEqual(applied["updated_threads"], 1)
        row = conn.execute(
            "SELECT phone_normalized, raw_json FROM dialpad_sms_threads WHERE thread_id = 'thread-pike13'"
        ).fetchone()
        self.assertEqual(row["phone_normalized"], "8325551212")
        raw = json.loads(row["raw_json"])
        self.assertEqual(raw["phone_backfill_method"], "pike13_unique_same_school_name")


if __name__ == "__main__":
    unittest.main()
