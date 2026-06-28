import sqlite3
import unittest

from lead_followup_schema import ensure_lead_followup_schema, utc_now_iso
from notesreminder.reports.dashboard_readiness import build_dashboard_readiness


class DashboardReadinessTests(unittest.TestCase):
    def open_db(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        ensure_lead_followup_schema(conn)
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS lessons (
                lesson_id TEXT PRIMARY KEY,
                lesson_date TEXT
            );
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY,
                lesson_date TEXT
            );
            CREATE TABLE IF NOT EXISTS pike13_visits (
                visit_id TEXT PRIMARY KEY,
                person_id TEXT,
                starts_at TEXT,
                school TEXT,
                updated_at TEXT
            );
            CREATE TABLE IF NOT EXISTS school_email_messages (
                message_id TEXT PRIMARY KEY,
                school TEXT,
                message_at TEXT
            );
            CREATE TABLE IF NOT EXISTS dialpad_voice_events (
                event_id TEXT PRIMARY KEY,
                event_at TEXT,
                school TEXT,
                direction TEXT,
                phone_normalized TEXT
            );
            CREATE TABLE IF NOT EXISTS dialpad_sms_messages (
                message_id TEXT PRIMARY KEY,
                thread_id TEXT,
                message_at TEXT,
                direction TEXT,
                updated_at TEXT
            );
            DROP VIEW IF EXISTS vw_dialpad_communications;
            CREATE VIEW vw_dialpad_communications AS
            SELECT 'call' AS channel, event_id AS communication_id, event_at, school, direction, phone_normalized
            FROM dialpad_voice_events
            UNION ALL
            SELECT 'sms' AS channel, m.message_id AS communication_id, m.message_at AS event_at, 'West U' AS school,
                   m.direction, t.phone_normalized
            FROM dialpad_sms_messages m
            LEFT JOIN dialpad_sms_threads t ON t.thread_id = m.thread_id;
            """
        )
        return conn

    def seed_fresh_sources(self, conn, as_of="2026-06-23"):
        now = utc_now_iso()
        conn.execute("INSERT INTO lessons VALUES ('lesson-1', ?)", (as_of,))
        conn.execute("INSERT INTO reminders (lesson_date) VALUES (?)", (as_of,))
        conn.execute(
            "INSERT INTO pike13_visits (visit_id, person_id, starts_at, school, updated_at) VALUES ('visit-1', 'person-1', ?, 'West U', ?)",
            (as_of, now),
        )
        conn.execute(
            """
            INSERT INTO school_email_messages (
                message_id, school_mailbox, school, direction, message_at, updated_at
            )
            VALUES ('email-1', 'westu@schoolofrock.com', 'West U', 'outbound', ?, ?)
            """,
            (as_of, now),
        )
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, event_at, school, direction, phone_normalized, updated_at
            )
            VALUES ('voice-1', ?, 'West U', 'outbound', '7135551212', ?)
            """,
            (as_of, now),
        )
        for month in ("2026-01", "2026-02", "2026-03", "2026-04", "2026-05"):
            for index in range(10):
                conn.execute(
                    """
                    INSERT INTO dialpad_voice_events (
                        event_id, event_at, school, direction, phone_normalized, updated_at
                    )
                    VALUES (?, ?, 'West U', 'outbound', '7135551212', ?)
                    """,
                    (f"voice-{month}-{index}", f"{month}-15", now),
                )
        conn.execute(
            """
            INSERT INTO dialpad_sms_threads (
                thread_id, phone_normalized, school, updated_at
            )
            VALUES ('thread-1', '7135551212', 'West U', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_messages (
                message_id, thread_id, message_at, direction, updated_at
            )
            VALUES ('sms-1', 'thread-1', ?, 'outbound', ?)
            """,
            (as_of, now),
        )

    def test_blocks_stale_sources_and_bad_lead_spine(self):
        conn = self.open_db()
        now = utc_now_iso()
        self.seed_fresh_sources(conn, as_of="2026-06-01")
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, create_date, school, raw_json, updated_at
            )
            VALUES ('contact-blank', 'Blank Lead', '2026-06-20', '', '{"trusted": 1}', ?)
            """,
            (now,),
        )

        report = build_dashboard_readiness(conn, as_of="2026-06-23", schools=("West U",))

        self.assertEqual(report["status"], "blocked")
        self.assertIn("stale_lessons_latest_2026-06-01", report["blockers"])
        self.assertIn("hubspot_mtd_unassigned_school_1", report["data_quality_flags"])
        self.assertNotIn("hubspot_mtd_unassigned_school_1", report["blockers"])
        self.assertIn("stale_dialpad_sms_messages_latest_2026-06-01", report["blockers"])

    def test_historical_call_backfill_gaps_block_readiness(self):
        conn = self.open_db()
        now = utc_now_iso()
        self.seed_fresh_sources(conn)
        conn.execute(
            """
            DELETE FROM dialpad_voice_events
            WHERE event_at >= '2026-02-01' AND event_at < '2026-03-01'
            """
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, create_date, phone, phone_normalized,
                email, email_normalized, school, raw_json, updated_at
            )
            VALUES ('contact-ready', 'Ready Lead', '2026-06-23',
                    '(713) 555-1212', '7135551212',
                    'ready@example.com', 'ready@example.com',
                    'West University Place', '{"trusted": 1}', ?)
            """,
            (now,),
        )

        report = build_dashboard_readiness(conn, as_of="2026-06-23", schools=("West U",))

        self.assertIn("West U:ytd_call_backfill_gap_2026-02", report["blockers"])

    def test_ready_when_sources_and_lead_spine_are_current(self):
        conn = self.open_db()
        now = utc_now_iso()
        self.seed_fresh_sources(conn)
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, create_date, phone, phone_normalized,
                email, email_normalized, school, raw_json, updated_at
            )
            VALUES ('contact-ready', 'Ready Lead', '2026-06-23',
                    '(713) 555-1212', '7135551212',
                    'ready@example.com', 'ready@example.com',
                    'West University Place', '{"trusted": 1}', ?)
            """,
            (now,),
        )

        report = build_dashboard_readiness(conn, as_of="2026-06-23", schools=("West U",))

        self.assertEqual(report["status"], "ready")
        self.assertTrue(report["ready_for_management_use"])
        self.assertEqual(report["hubspot_lead_spine"]["schools"][0]["mtd_phone_rate"], 1.0)
        self.assertEqual(report["hubspot_lead_spine"]["schools"][0]["mtd_identity_rate"], 1.0)

    def test_unmapped_voice_rows_are_flagged_not_blocking(self):
        conn = self.open_db()
        now = utc_now_iso()
        self.seed_fresh_sources(conn)
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, event_at, school, direction, phone_normalized, raw_json, updated_at
            )
            VALUES ('voice-unmapped', '2026-06-23', '', 'inbound', '7135559999',
                    '{"display_entry_point": "(832) 555-0000"}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_threads (
                thread_id, phone_normalized, school, updated_at
            )
            VALUES ('thread-no-phone', '', 'West U', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_messages (
                message_id, thread_id, message_at, direction, updated_at
            )
            VALUES ('sms-no-phone', 'thread-no-phone', '2026-06-23', 'outbound', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, create_date, phone, phone_normalized,
                email, email_normalized, school, raw_json, updated_at
            )
            VALUES ('contact-ready', 'Ready Lead', '2026-06-23',
                    '(713) 555-1212', '7135551212',
                    'ready@example.com', 'ready@example.com',
                    'West University Place', '{"trusted": 1}', ?)
            """,
            (now,),
        )

        report = build_dashboard_readiness(conn, as_of="2026-06-23", schools=("West U",))

        self.assertEqual(report["status"], "ready")
        self.assertIn("dialpad_voice_mtd_unmapped_school_1", report["data_quality_flags"])
        self.assertIn("dialpad_voice_ytd_unmapped_school_1", report["data_quality_flags"])
        self.assertIn("dialpad_voice_ytd_unmapped_entry_point_rows_1", report["data_quality_flags"])
        self.assertIn("dialpad_sms_ytd_no_phone_rows_1", report["data_quality_flags"])
        self.assertEqual(report["communication_coverage"]["unmapped_voice_mtd"], 1)
        self.assertEqual(report["communication_coverage"]["unmapped_voice_ytd"], 1)
        self.assertEqual(report["communication_coverage"]["sms_ytd_no_phone_rows"], 1)
        self.assertEqual(report["communication_coverage"]["sms_ytd_no_phone_school_attributed_rows"], 1)
        self.assertEqual(report["communication_coverage"]["voice_ytd_unmapped_known_entry_point_rows"], 1)
        self.assertEqual(report["communication_coverage"]["voice_ytd_unmapped_no_safe_evidence_rows"], 0)

    def test_low_hubspot_identity_coverage_is_a_blocker(self):
        conn = self.open_db()
        now = utc_now_iso()
        self.seed_fresh_sources(conn)
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, create_date, phone, phone_normalized,
                email, email_normalized, school, raw_json, updated_at
            )
            VALUES ('contact-identified', 'Identified Lead', '2026-06-23',
                    '(713) 555-1212', '7135551212',
                    'ready@example.com', 'ready@example.com',
                    'West University Place', '{"trusted": 1}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, create_date, school, raw_json, updated_at
            )
            VALUES ('contact-missing-identity', 'Missing Identity', '2026-06-23',
                    'West University Place', '{"trusted": 1}', ?)
            """,
            (now,),
        )

        report = build_dashboard_readiness(conn, as_of="2026-06-23", schools=("West U",))

        school = report["hubspot_lead_spine"]["schools"][0]
        self.assertEqual(school["mtd_identity_rows"], 1)
        self.assertEqual(school["mtd_identity_rate"], 0.5)
        self.assertIn("West U:hubspot_mtd_identity_coverage_below_70pct", report["blockers"])


if __name__ == "__main__":
    unittest.main()
