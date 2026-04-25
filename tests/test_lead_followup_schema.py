import sqlite3
import tempfile
import unittest
from pathlib import Path

from lead_followup_schema import (
    ensure_lead_followup_schema,
    finish_import_run,
    normalize_email,
    normalize_phone,
    start_import_run,
    utc_now_iso,
)


class LeadFollowupSchemaTests(unittest.TestCase):
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
                school_name TEXT,
                voicemail_transcript TEXT,
                voicemail_recording_url TEXT,
                recording_url TEXT
            )
            """
        )
        ensure_lead_followup_schema(conn)
        return conn

    def test_normalizers(self):
        self.assertEqual(normalize_email(" Test@Example.COM "), "test@example.com")
        self.assertEqual(normalize_phone("+1 (713) 555-1212"), "7135551212")
        self.assertIsNone(normalize_email(""))
        self.assertIsNone(normalize_phone(""))

    def test_schema_is_repeatable(self):
        conn = self.open_db()
        ensure_lead_followup_schema(conn)
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        for table in {
            "source_import_runs",
            "hubspot_deals",
            "hubspot_contacts",
            "hubspot_tasks",
            "hubspot_activities",
            "dialpad_sms_threads",
            "dialpad_sms_messages",
            "pike13_people",
            "pike13_visits",
            "pike13_plans_passes",
            "identity_matches",
        }:
            self.assertIn(table, tables)

    def test_import_run_logging(self):
        conn = self.open_db()
        run_id = start_import_run(conn, "hubspot", "extract_hubspot_leads.py", "2025-01-01")
        finish_import_run(conn, run_id, "success", rows_seen=3, rows_inserted=2, rows_updated=1)
        row = conn.execute("SELECT * FROM source_import_runs WHERE id = ?", (run_id,)).fetchone()
        self.assertEqual(row["status"], "success")
        self.assertEqual(row["rows_seen"], 3)
        self.assertEqual(row["rows_inserted"], 2)
        self.assertEqual(row["rows_updated"], 1)

    def test_curated_views_return_actionable_rows(self):
        conn = self.open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO hubspot_deals (
                deal_id, deal_name, stage, owner, school, create_date, last_contacted,
                follow_up_needed, trial_date, pike13_person_id, updated_at
            )
            VALUES ('deal-1', 'M Sample', 'Scheduled Trial', 'Owner A', 'West U',
                    '2025-01-01', '2025-01-03', 'Yes', '2025-01-10', '15046380', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO hubspot_tasks (
                task_id, deal_id, owner, due_date, status, title, task_type, updated_at
            )
            VALUES ('task-1', 'deal-1', 'Owner A', '2025-01-04', 'open', 'Call parent', 'call', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_threads (
                thread_id, phone, phone_normalized, contact_name, school, updated_at
            )
            VALUES ('thread-1', '(713) 555-1212', '7135551212', 'M Sample', 'West U', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_messages (
                message_id, thread_id, message_at, direction, body, updated_at
            )
            VALUES ('msg-1', 'thread-1', '2025-01-05', 'inbound', 'Can we reschedule?', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, phone_normalized, school, updated_at
            )
            VALUES ('15046380', 'M Sample', '7135551212', 'West U', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO pike13_visits (
                visit_id, person_id, event_id, service, starts_at, status,
                no_show_flag, school, updated_at
            )
            VALUES ('visit-1', '15046380', 'event-1', 'Trial Lesson',
                    '2025-01-10', 'No Show', 1, 'West U', ?)
            """,
            (now,),
        )

        stale = conn.execute("SELECT * FROM vw_stale_leads WHERE deal_id = 'deal-1'").fetchone()
        self.assertEqual(stale["risk_reason"], "follow_up_needed")

        unanswered = conn.execute("SELECT * FROM vw_unanswered_messages").fetchall()
        self.assertEqual(len(unanswered), 1)
        self.assertEqual(unanswered[0]["thread_id"], "thread-1")

        no_show = conn.execute("SELECT * FROM vw_no_show_followup").fetchone()
        self.assertEqual(no_show["deal_id"], "deal-1")

        timeline = conn.execute(
            "SELECT source, event_type FROM vw_lead_timeline WHERE person_or_lead LIKE '%Sample%'"
        ).fetchall()
        self.assertTrue(any(row["source"] == "hubspot" for row in timeline))
        self.assertTrue(any(row["source"] == "dialpad" for row in timeline))

        conversion = conn.execute(
            "SELECT conversion_state FROM vw_lead_conversion_path WHERE deal_id = 'deal-1'"
        ).fetchone()
        self.assertEqual(conversion["conversion_state"], "trial_no_show")


if __name__ == "__main__":
    unittest.main()
