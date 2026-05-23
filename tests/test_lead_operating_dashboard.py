import importlib
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

import mcp_server
from build_reporting_schema import backfill_reporting
from lead_followup_schema import ensure_lead_followup_schema, upsert_school_email_message, utc_now_iso
from lead_operating_dashboard import build_snapshot, render_snapshot_markdown, window_for_period


def open_db(path=":memory:"):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE reminders (
            lesson_id TEXT,
            pike13_lesson_id TEXT,
            school TEXT,
            instructor_name TEXT,
            lesson_date TEXT,
            lesson_time TEXT,
            lesson_type TEXT,
            students TEXT,
            location TEXT,
            note_completed INTEGER,
            attendance_status TEXT,
            notes_text TEXT,
            note_timestamp TEXT,
            note_score REAL,
            note_score_explanation TEXT,
            note_score_model TEXT,
            note_score_version TEXT,
            note_score_updated_at TEXT,
            note_score_hash TEXT,
            last_checked TEXT
        )
        """
    )
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
    ensure_lead_followup_schema(conn)
    return conn


def seed_dashboard_data(conn):
    conn.execute(
        """
        INSERT INTO reminders (
            lesson_id, school, instructor_name, lesson_date, lesson_time,
            lesson_type, students, note_completed, attendance_status,
            notes_text, note_score, last_checked
        )
        VALUES
            ('lesson-1', 'westu-sor', 'Teacher One', '2026-05-02', '4:00 PM',
             'Private Lesson', 'Student One', 1, 'present', 'Good note', 8.0, '2026-05-08'),
            ('lesson-2', 'westu-sor', 'Teacher One', '2026-05-03', '5:00 PM',
             'Private Lesson', 'Student Two', 0, 'present', NULL, NULL, '2026-05-08'),
            ('lesson-group', 'westu-sor', 'Teacher One', '2026-05-03', '6:00 PM',
             'Group Lesson', 'Student Three, Student Four', 0, 'present', NULL, NULL, '2026-05-08')
        """
    )
    backfill_reporting(conn)
    conn.execute(
        """
        INSERT INTO source_import_runs (
            source, extractor, started_at, finished_at, status, window_start, window_end,
            rows_seen, rows_inserted, rows_updated
        )
        VALUES ('hubspot', 'proof', '2026-05-08T10:00:00+00:00', '2026-05-08T10:01:00+00:00',
                'success', '2026-05-01', '2026-05-09', 1, 1, 0)
        """
    )
    conn.execute(
        """
        INSERT INTO hubspot_deals (
            deal_id, deal_name, stage, school, create_date, last_activity_date, last_contacted,
            trial_date, pike13_person_id, lead_source, updated_at
        )
        VALUES ('deal-1', 'Private Student | West University Place', 'Scheduled Trial/Tour',
                'West University Place', '2026-05-02', '2026-05-02', '2026-05-02',
                '2026-05-03', 'person-1', 'Website', '2026-05-08T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO hubspot_contacts (
            contact_id, full_name, email, email_normalized, phone, phone_normalized,
            school, associated_deal_ids, raw_json, updated_at
        )
        VALUES ('contact-1', 'Private Student', 'lead@example.com', 'lead@example.com',
                '7135551212', '7135551212', 'West University Place', 'deal-1',
                '{"trusted": 1}', '2026-05-08T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO pike13_people (
            person_id, full_name, email, email_normalized, phone, phone_normalized, school, updated_at
        )
        VALUES ('person-1', 'Private Student', 'lead@example.com', 'lead@example.com',
                '7135551212', '7135551212', 'West U', '2026-05-08T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO pike13_visits (
            visit_id, person_id, service, starts_at, status, first_visit_flag,
            attendance_confirmed_flag, checked_in_flag, instructor, school, updated_at
        )
        VALUES ('visit-1', 'person-1', 'Trial - Guitar', '2026-05-03T14:00:00',
                'Complete', 1, 1, 1, 'Calvin Barnhill', 'West U', '2026-05-08T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO pike13_visits (
            visit_id, person_id, service, starts_at, status, first_visit_flag,
            attendance_confirmed_flag, checked_in_flag, instructor, school, updated_at
        )
        VALUES ('visit-old', 'person-1', 'Trial - Vocals', '2026-04-01T14:00:00',
                'Complete', 1, 1, 1, 'Calvin Barnhill', 'West U', '2026-05-08T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO pike13_plans_passes (
            plan_pass_id, person_id, name, status, starts_at, school, payer_name, updated_at
        )
        VALUES ('plan-1', 'person-1', 'Lessons Only - 45 Minute Lessons', 'Active',
                '2026-05-04', 'West U', 'Private Payer', '2026-05-08T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO dialpad_sms_threads (
            thread_id, phone, phone_normalized, contact_name, school, updated_at
        )
        VALUES ('thread-1', '7135551212', '7135551212', 'Private Student',
                'West University Place', '2026-05-08T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO dialpad_sms_messages (
            message_id, thread_id, message_at, direction, body, updated_at
        )
        VALUES ('message-1', 'thread-1', '2026-05-02T10:00:00', 'outbound',
                'Private SMS body', '2026-05-08T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO dialpad_voice_events (
            event_id, event_type, phone, phone_normalized, contact_name, direction,
            event_at, school, outcome, updated_at
        )
        VALUES ('voice-1', 'call', '7135551212', '7135551212', 'Private Student',
                'outbound', '2026-05-02T11:00:00', 'West University Place',
                'connected', '2026-05-08T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO recording_downloads (
            call_id, voice_event_id, event_at, school, status, file_path, file_sha256,
            downloaded_at, updated_at
        )
        VALUES ('call-1', 'voice-1', '2026-05-02T11:00:00', 'West University Place',
                'success', '/private/audio.mp3', 'abc123', '2026-05-08T00:00:00+00:00',
                '2026-05-08T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO recording_transcripts (
            call_id, recording_url, transcript_status, created_at
        )
        VALUES ('call-1', 'https://private.example/audio.mp3', 'pending', '2026-05-08T00:00:00+00:00')
        """
    )
    upsert_school_email_message(
        conn,
        {
            "message_id": "email-1",
            "thread_id": "thread-email-1",
            "school_mailbox": "westu@schoolofrock.com",
            "school": "West University Place",
            "direction": "outbound",
            "message_at": "2026-05-02T09:00:00",
            "from_email": "westu@schoolofrock.com",
            "from_email_normalized": "westu@schoolofrock.com",
            "to_emails": '["lead@example.com"]',
            "to_emails_normalized": '["lead@example.com"]',
            "cc_emails": "[]",
            "cc_emails_normalized": "[]",
            "external_email_normalized": "lead@example.com",
            "subject": "Private subject",
            "snippet": "Private snippet",
            "body": "Private email body",
            "source_url": "https://mail.google.com/private",
            "raw_text": "Private raw",
            "raw_json": "{}",
            "updated_at": utc_now_iso(),
        },
    )


class LeadOperatingDashboardTests(unittest.TestCase):
    def test_default_period_windows(self):
        self.assertEqual(window_for_period("daily", "2026-05-09"), ("2026-05-08", "2026-05-09"))
        self.assertEqual(window_for_period("weekly", "2026-05-09"), ("2026-04-27", "2026-05-03"))
        self.assertEqual(window_for_period("monthly", "2026-05-09"), ("2026-05-01", "2026-05-09"))
        self.assertEqual(window_for_period("monthly", "2026-05-01"), ("2026-04-01", "2026-04-30"))

    def test_snapshot_metrics_and_markdown_are_sanitized(self):
        conn = open_db()
        seed_dashboard_data(conn)

        snapshot = build_snapshot(
            conn,
            "weekly",
            start_date="2026-05-01",
            end_date="2026-05-09",
            school="West U",
        )
        markdown = render_snapshot_markdown(snapshot)

        self.assertEqual(snapshot["funnel_counts"]["hubspot_leads"], 1)
        self.assertEqual(snapshot["funnel_counts"]["pike13_first_visits"], 1)
        self.assertEqual(snapshot["funnel_counts"]["attended"], 1)
        self.assertEqual(snapshot["funnel_counts"]["converted"], 1)
        self.assertEqual(snapshot["communications"]["dialpad_calls"], 1)
        self.assertEqual(snapshot["communications"]["dialpad_sms"], 1)
        self.assertEqual(snapshot["communications"]["school_email"], 1)
        self.assertEqual(snapshot["notes_operations"]["reportable_lessons"], 2)
        self.assertEqual(snapshot["notes_operations"]["completed_notes"], 1)
        self.assertEqual(snapshot["notes_operations"]["missing_notes"], 1)
        self.assertEqual(snapshot["notes_operations"]["league_score"], 40.0)
        self.assertEqual(snapshot["dialpad_recordings"]["success"], 1)
        self.assertEqual(snapshot["transcription_queue"]["pending"], 1)
        self.assertIn("Weekly Lead Dashboard", markdown)
        self.assertIn("Calvin Barnhill", markdown)

        forbidden = [
            "Private Student",
            "lead@example.com",
            "7135551212",
            "Private SMS body",
            "Private email body",
            "/private/audio.mp3",
            "https://mail.google.com/private",
        ]
        for value in forbidden:
            self.assertNotIn(value, markdown)

    def test_mcp_weekly_snapshot_matches_shared_snapshot_logic(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = Path(tmp.name) / "lead.db"
        conn = open_db(str(db_path))
        seed_dashboard_data(conn)
        conn.commit()
        conn.close()

        original_path = mcp_server.LEAD_DB_PATH
        mcp_server.LEAD_DB_PATH = str(db_path)
        self.addCleanup(setattr, mcp_server, "LEAD_DB_PATH", original_path)

        mcp_snapshot = json.loads(mcp_server.weekly_snapshot(as_of="2026-05-09", school="West U"))
        direct_conn = sqlite3.connect(db_path)
        direct_conn.row_factory = sqlite3.Row
        direct_snapshot = build_snapshot(direct_conn, "weekly", as_of="2026-05-09", school="West U")
        direct_conn.close()

        self.assertEqual(mcp_snapshot["window"], direct_snapshot["window"])
        self.assertEqual(mcp_snapshot["funnel_counts"], direct_snapshot["funnel_counts"])
        self.assertEqual(mcp_snapshot["communications"], direct_snapshot["communications"])
        self.assertEqual(mcp_snapshot["notes_operations"], direct_snapshot["notes_operations"])

    def test_mcp_daily_and_monthly_snapshots_match_shared_logic(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = Path(tmp.name) / "lead.db"
        conn = open_db(str(db_path))
        seed_dashboard_data(conn)
        conn.commit()
        conn.close()

        original_path = mcp_server.LEAD_DB_PATH
        mcp_server.LEAD_DB_PATH = str(db_path)
        self.addCleanup(setattr, mcp_server, "LEAD_DB_PATH", original_path)

        for tool, period in ((mcp_server.daily_snapshot, "daily"), (mcp_server.monthly_snapshot, "monthly")):
            mcp_snapshot = json.loads(tool(as_of="2026-05-09", school="West U"))
            direct_conn = sqlite3.connect(db_path)
            direct_conn.row_factory = sqlite3.Row
            direct_snapshot = build_snapshot(direct_conn, period, as_of="2026-05-09", school="West U")
            direct_conn.close()
            self.assertEqual(mcp_snapshot["window"], direct_snapshot["window"])
            self.assertEqual(mcp_snapshot["funnel_counts"], direct_snapshot["funnel_counts"])
            self.assertEqual(mcp_snapshot["notes_operations"], direct_snapshot["notes_operations"])

    def test_mcp_defaults_lead_tools_to_main_db(self):
        original_db = os.environ.get("REMINDERS_DB_PATH")
        original_lead = os.environ.get("LEAD_INTELLIGENCE_DB_PATH")
        try:
            os.environ["REMINDERS_DB_PATH"] = "/tmp/unified-reminders.db"
            os.environ.pop("LEAD_INTELLIGENCE_DB_PATH", None)
            importlib.reload(mcp_server)
            self.assertEqual(mcp_server.LEAD_DB_PATH, "/tmp/unified-reminders.db")
        finally:
            if original_db is None:
                os.environ.pop("REMINDERS_DB_PATH", None)
            else:
                os.environ["REMINDERS_DB_PATH"] = original_db
            if original_lead is None:
                os.environ.pop("LEAD_INTELLIGENCE_DB_PATH", None)
            else:
                os.environ["LEAD_INTELLIGENCE_DB_PATH"] = original_lead
            importlib.reload(mcp_server)


if __name__ == "__main__":
    unittest.main()
