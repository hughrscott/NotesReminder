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
from notesreminder.dashboard.server import _window_kwargs, lead_dashboard_html, normalize_school_slug
from notesreminder.reports.operations_dashboard import (
    build_operations_dashboard,
    funnel_metrics,
    render_operations_dashboard_html,
)


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
                'West University Place', 'May 2, 2026 at 9:15 AM CDT', '2026-05-02', '2026-05-02',
                '2026-05-03', 'person-1', 'Website', '2026-05-08T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO hubspot_contacts (
            contact_id, full_name, email, email_normalized, phone, phone_normalized,
            school, create_date, pike13_person_id, associated_deal_ids, raw_json, updated_at
        )
        VALUES ('contact-1', 'Private Student', 'lead@example.com', 'lead@example.com',
                '7135551212', '7135551212', 'West University Place', '2026-05-02',
                'person-1', 'deal-1',
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
        self.assertEqual(snapshot["funnel_rates"]["lead_to_trial_rate"], 1.0)
        self.assertEqual(snapshot["funnel_rates"]["trial_to_conversion_rate"], 1.0)
        self.assertEqual(snapshot["communications"]["dialpad_calls"], 1)
        self.assertEqual(snapshot["communications"]["dialpad_sms"], 1)
        self.assertEqual(snapshot["communications"]["school_email"], 1)
        self.assertEqual(snapshot["lead_followup_pareto"]["coverage"]["leads"], 1)
        self.assertEqual(snapshot["lead_followup_pareto"]["coverage"]["outbound_7d_leads"], 1)
        self.assertEqual(snapshot["lead_followup_pareto"]["coverage"]["pre_lead_inbound_origin_leads"], 0)
        self.assertEqual(snapshot["lead_followup_pareto"]["grid_status"], "ready")
        self.assertEqual(snapshot["lead_followup_pareto"]["blockers"], [])
        self.assertEqual(snapshot["lead_followup_pareto"]["overall_total"]["trial_rate"], 1.0)
        pareto_row = snapshot["lead_followup_pareto"]["grid"][0]
        self.assertEqual(pareto_row["response_time"], "Same / next day")
        self.assertEqual(pareto_row["cells"]["Active"]["leads"], 1)
        self.assertEqual(pareto_row["cells"]["Active"]["trial_rate"], 1.0)
        self.assertEqual(snapshot["notes_operations"]["reportable_lessons"], 2)
        self.assertEqual(snapshot["notes_operations"]["completed_notes"], 1)
        self.assertEqual(snapshot["notes_operations"]["missing_notes"], 1)
        self.assertEqual(snapshot["notes_operations"]["league_score"], 40.0)
        self.assertEqual(snapshot["dialpad_recordings"]["success"], 1)
        self.assertEqual(snapshot["transcription_queue"]["pending"], 1)
        self.assertIn("Weekly Lead Dashboard", markdown)
        self.assertIn("lead_to_trial_rate: 100%", markdown)
        self.assertIn("trial_to_conversion_rate: 100%", markdown)
        self.assertIn("Lead Follow-Up Pareto", markdown)
        self.assertIn("| Same / next day | 0 | 0 | 1 / 100% | 0 | 1 / 100% |", markdown)
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

    def test_pareto_blocks_instead_of_showing_misleading_zero_grid(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, create_date, email, email_normalized,
                school, lead_source, raw_json, updated_at
            )
            VALUES ('contact-no-followup', 'Private Student', '2026-05-02',
                    'lead@example.com', 'lead@example.com', 'West University Place',
                    'Website', '{"trusted": 1}', ?)
            """,
            (now,),
        )

        snapshot = build_snapshot(
            conn,
            "weekly",
            start_date="2026-05-01",
            end_date="2026-05-09",
            school="West U",
        )
        markdown = render_snapshot_markdown(snapshot)
        pareto = snapshot["lead_followup_pareto"]

        self.assertEqual(pareto["grid_status"], "blocked")
        self.assertIn("matched_communication_coverage_below_10pct", pareto["blockers"])
        self.assertIn("no_matched_outbound_followup_7d", pareto["blockers"])
        self.assertEqual(pareto["coverage"]["communication_coverage_rate"], 0.0)
        self.assertEqual(pareto["coverage"]["outbound_7d_leads"], 0)
        self.assertIn("Insufficient matched communication data", markdown)
        self.assertIn("matched_communication_coverage_below_10pct", markdown)

    def test_pareto_blocks_when_contact_spine_has_no_school_leads(self):
        conn = open_db()

        snapshot = build_snapshot(
            conn,
            "weekly",
            start_date="2026-05-01",
            end_date="2026-05-09",
            school="The Heights",
        )
        markdown = render_snapshot_markdown(snapshot)
        pareto = snapshot["lead_followup_pareto"]

        self.assertEqual(pareto["coverage"]["leads"], 0)
        self.assertEqual(pareto["grid_status"], "blocked")
        self.assertIn("no_contact_spine_leads_for_school_window", pareto["blockers"])
        self.assertIn("Backfill HubSpot contact lead spine", pareto["recommended_action"])
        self.assertIn("Insufficient matched communication data", markdown)
        self.assertIn("no_contact_spine_leads_for_school_window", markdown)

    def test_dashboard_handles_legacy_hubspot_contacts_schema(self):
        conn = open_db()
        conn.execute("DROP TABLE hubspot_contacts")
        conn.execute(
            """
            CREATE TABLE hubspot_contacts (
                contact_id TEXT PRIMARY KEY,
                full_name TEXT,
                email_normalized TEXT,
                phone_normalized TEXT,
                school TEXT,
                associated_deal_ids TEXT,
                raw_json TEXT,
                updated_at TEXT NOT NULL,
                person_id TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, email_normalized, phone_normalized,
                school, associated_deal_ids, raw_json, updated_at, person_id
            )
            VALUES ('legacy-contact-1', 'Private Student', 'lead@example.com',
                    '7135551212', 'West University Place', '', '{"trusted": 1}',
                    '2026-05-02', 'person-1')
            """
        )
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, email_normalized, phone_normalized, school, updated_at
            )
            VALUES ('person-1', 'lead@example.com', '7135551212', 'West U', '2026-05-02T00:00:00+00:00')
            """
        )

        snapshot = build_snapshot(
            conn,
            "monthly",
            start_date="2026-01-01",
            end_date="2026-06-22",
            school="West U",
        )

        self.assertEqual(snapshot["funnel_counts"]["hubspot_leads"], 1)
        self.assertEqual(snapshot["performance"]["hubspot_source_counts"][0]["source"], "unknown")

    def test_exception_queue_includes_customer_names_and_groups_by_customer(self):
        conn = open_db()
        conn.execute(
            """
            INSERT INTO hubspot_deals (
                deal_id, deal_name, stage, school, create_date, updated_at
            )
            VALUES ('deal-exception', 'Private Student | West University Place',
                    'Contacted', 'West University Place', '2026-05-02',
                    '2026-05-08T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, email, email_normalized, phone, phone_normalized,
                school, associated_deal_ids, raw_json, updated_at
            )
            VALUES ('contact-exception', 'Private Student', 'lead@example.com',
                    'lead@example.com', '7135551212', '7135551212',
                    'West University Place', 'deal-exception', '{"trusted": 1}',
                    '2026-05-08T00:00:00+00:00')
            """
        )

        snapshot = build_snapshot(
            conn,
            "weekly",
            start_date="2026-05-01",
            end_date="2026-05-09",
            school="West U",
        )
        markdown = render_snapshot_markdown(snapshot)
        exception_queue = snapshot["exception_queue"]

        self.assertEqual(exception_queue["items"][0]["customer_name"], "Private Student")
        self.assertEqual(exception_queue["customer_groups"][0]["customer_name"], "Private Student")
        self.assertIn("By Customer", markdown)
        self.assertIn("Private Student", markdown)
        self.assertRegex(markdown, r"lead_[0-9a-f]{10}")
        for forbidden in ["lead@example.com", "7135551212"]:
            self.assertNotIn(forbidden, markdown)

    def test_pareto_keeps_pre_lead_inbound_origin_separate_from_followup(self):
        conn = open_db()
        seed_dashboard_data(conn)
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, event_type, phone, phone_normalized, contact_name, direction,
                event_at, school, outcome, updated_at
            )
            VALUES ('voice-prelead', 'call', '7135551212', '7135551212', 'Private Student',
                    'inbound', '2026-05-01T17:00:00', 'West University Place',
                    'connected', '2026-05-08T00:00:00+00:00')
            """
        )

        snapshot = build_snapshot(
            conn,
            "weekly",
            start_date="2026-05-01",
            end_date="2026-05-09",
            school="West U",
        )
        coverage = snapshot["lead_followup_pareto"]["coverage"]

        self.assertEqual(coverage["pre_lead_inbound_origin_leads"], 1)
        self.assertEqual(coverage["pre_lead_inbound_call_leads"], 1)
        self.assertEqual(coverage["outbound_7d_leads"], 1)
        self.assertEqual(snapshot["lead_followup_pareto"]["grid_status"], "ready")

    def test_contacted_headline_counts_unknown_direction_communication(self):
        conn = open_db()
        seed_dashboard_data(conn)
        conn.execute("DELETE FROM dialpad_sms_messages")
        conn.execute("DELETE FROM school_email_messages")
        conn.execute("DELETE FROM dialpad_voice_events")
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, event_type, phone, phone_normalized, contact_name, direction,
                event_at, school, outcome, updated_at
            )
            VALUES ('voice-unknown-direction', 'call', '7135551212', '7135551212',
                    'Private Student', 'unknown', '2026-05-02T11:00:00',
                    'West University Place', 'connected', '2026-05-08T00:00:00+00:00')
            """
        )

        snapshot = build_snapshot(
            conn,
            "weekly",
            start_date="2026-05-01",
            end_date="2026-05-09",
            school="West U",
        )
        coverage = snapshot["lead_followup_pareto"]["coverage"]

        self.assertEqual(snapshot["funnel_counts"]["contacted"], 1)
        self.assertEqual(snapshot["funnel_counts"]["communication_contacted_7d"], 1)
        self.assertEqual(snapshot["funnel_counts"]["outbound_contacted_7d"], 0)
        self.assertEqual(coverage["communication_7d_leads"], 1)
        self.assertEqual(coverage["outbound_7d_leads"], 0)

    def test_followup_matching_rejects_cross_school_dialpad_rows(self):
        conn = open_db()
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, create_date, phone, phone_normalized,
                school, raw_json, updated_at
            )
            VALUES ('heights-contact', 'Heights Student', '2026-05-02',
                    '7135551212', '7135551212', 'The Heights',
                    '{"trusted": 1}', '2026-05-08T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, event_type, phone, phone_normalized, contact_name, direction,
                event_at, school, outcome, updated_at
            )
            VALUES ('westu-call-same-phone', 'call', '7135551212', '7135551212',
                    'Heights Student', 'outbound', '2026-05-02T11:00:00',
                    'West U', 'connected', '2026-05-08T00:00:00+00:00')
            """
        )

        snapshot = build_snapshot(
            conn,
            "weekly",
            start_date="2026-05-01",
            end_date="2026-05-09",
            school="The Heights",
        )
        coverage = snapshot["lead_followup_pareto"]["coverage"]

        self.assertEqual(snapshot["funnel_counts"]["hubspot_leads"], 1)
        self.assertEqual(snapshot["funnel_counts"]["contacted"], 0)
        self.assertEqual(coverage["matched_communication_leads"], 0)
        self.assertEqual(coverage["communication_7d_leads"], 0)

    def test_source_data_freshness_uses_school_specific_sms_dates(self):
        conn = open_db()
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, create_date, phone, phone_normalized,
                school, raw_json, updated_at
            )
            VALUES ('heights-contact', 'Heights Student', '2026-05-02',
                    '7135551212', '7135551212', 'The Heights',
                    '{"trusted": 1}', '2026-05-08T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_threads (
                thread_id, phone, phone_normalized, contact_name, school, updated_at
            )
            VALUES ('westu-thread', '7135551212', '7135551212', 'West U Student',
                    'West U', '2026-05-08T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_messages (
                message_id, thread_id, message_at, direction, body, updated_at
            )
            VALUES ('westu-message', 'westu-thread', '2026-05-02T10:00:00',
                    'outbound', 'Private SMS body', '2026-05-08T00:00:00+00:00')
            """
        )

        snapshot = build_snapshot(
            conn,
            "weekly",
            start_date="2026-05-01",
            end_date="2026-05-09",
            school="The Heights",
        )
        freshness = snapshot["source_data_freshness"]

        self.assertIsNone(freshness["latest_dates"]["dialpad_sms"])
        self.assertEqual(freshness["latest_dates"]["school_sms_rows"], 0)
        self.assertIn("missing_dialpad_sms_data", freshness["flags"])
        self.assertIn("missing_school_sms_data", freshness["flags"])

    def test_snapshot_counts_hubspot_contact_leads_without_deals(self):
        conn = open_db()
        seed_dashboard_data(conn)
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, create_date, email, email_normalized,
                school, lead_source, raw_json, updated_at
            )
            VALUES ('contact-only', 'Contact Only', '2026-05-04', 'contact@example.com',
                    'contact@example.com', 'West University Place', 'Offline Sources',
                    '{"trusted": 1}', '2026-05-08T00:00:00+00:00')
            """
        )

        snapshot = build_snapshot(
            conn,
            "weekly",
            start_date="2026-05-01",
            end_date="2026-05-09",
            school="West U",
        )

        self.assertEqual(snapshot["funnel_counts"]["hubspot_leads"], 2)
        self.assertIn(
            {"source": "Offline Sources", "leads": 1},
            snapshot["performance"]["hubspot_source_counts"],
        )

    def test_snapshot_counts_hubspot_contact_school_from_matched_pike13_person(self):
        conn = open_db()
        seed_dashboard_data(conn)
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, school, updated_at
            )
            VALUES ('person-school-only', 'School Only', 'West U',
                    '2026-05-08T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, create_date, email, email_normalized,
                pike13_person_id, lead_source, raw_json, updated_at
            )
            VALUES ('contact-school-only', 'School Only', '2026-05-04', 'school@example.com',
                    'school@example.com', 'person-school-only', 'Offline Sources',
                    '{"trusted": 1}', '2026-05-08T00:00:00+00:00')
            """
        )

        snapshot = build_snapshot(
            conn,
            "weekly",
            start_date="2026-05-01",
            end_date="2026-05-09",
            school="West U",
        )

        self.assertEqual(snapshot["funnel_counts"]["hubspot_leads"], 2)
        self.assertIn(
            {"source": "Offline Sources", "leads": 1},
            snapshot["performance"]["hubspot_source_counts"],
        )

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

    def test_curated_mcp_dashboard_tools_return_sanitized_stable_shapes(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = Path(tmp.name) / "lead.db"
        conn = open_db(str(db_path))
        seed_dashboard_data(conn)
        conn.commit()
        conn.close()

        original_db = mcp_server.DB_PATH
        original_lead = mcp_server.LEAD_DB_PATH
        mcp_server.DB_PATH = str(db_path)
        mcp_server.LEAD_DB_PATH = str(db_path)
        self.addCleanup(setattr, mcp_server, "DB_PATH", original_db)
        self.addCleanup(setattr, mcp_server, "LEAD_DB_PATH", original_lead)

        lead_snapshot = json.loads(
            mcp_server.lead_dashboard_snapshot(
                school="West U",
                start_date="2026-05-01",
                end_date="2026-05-09",
            )
        )
        pareto = json.loads(
            mcp_server.lead_followup_pareto(
                school="West U",
                start_date="2026-05-01",
                end_date="2026-05-09",
            )
        )
        operations = json.loads(mcp_server.operations_scorecard(period="monthly", as_of="2026-05-09"))
        instructors = json.loads(
            mcp_server.instructor_conversion_table(
                school="West U",
                start_date="2026-05-01",
                end_date="2026-05-09",
            )
        )

        self.assertEqual(lead_snapshot["funnel_counts"]["hubspot_leads"], 1)
        self.assertIn("lead_to_trial_rate", lead_snapshot["funnel_rates"])
        self.assertIn("trial_to_conversion_rate", lead_snapshot["funnel_rates"])
        self.assertEqual(pareto["lead_followup_pareto"]["grid_status"], "ready")
        self.assertEqual(operations["dashboard_type"], "operations_scorecard")
        self.assertEqual(instructors["row_count"], 1)
        self.assertEqual(instructors["rows"][0]["converted_trials"], 1)

        serialized = json.dumps(
            {
                "lead_snapshot": lead_snapshot,
                "pareto": pareto,
                "operations": operations,
                "instructors": instructors,
            }
        )
        self.assertIn("Private Student", serialized)
        for forbidden in ["lead@example.com", "7135551212", "Private SMS body"]:
            self.assertNotIn(forbidden, serialized)

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

    def test_operations_dashboard_html_is_aggregate_and_sanitized(self):
        conn = open_db()
        seed_dashboard_data(conn)

        report = build_operations_dashboard(
            conn,
            period="weekly",
            as_of="2026-05-09",
            schools=("West U",),
        )
        html = render_operations_dashboard_html(report)

        self.assertIn("School Operations Scorecard", html)
        self.assertIn("West U", html)
        self.assertIn("Instructor Notes Ranking MTD", html)
        self.assertIn("Instructor Trial Conversion YTD", html)
        self.assertIn("Lead To First Response", html)
        self.assertEqual(report["totals"]["mtd_new_leads"], 1)
        self.assertEqual(report["totals"]["mtd_conversions"], 1)
        self.assertEqual(report["school_reports"][0]["conversion_ytd"][0]["converted_trials"], 1)

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
            self.assertNotIn(value, html)

    def test_operations_dashboard_reports_only_unassigned_hubspot_school_blockers(self):
        conn = open_db()
        seed_dashboard_data(conn)
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, school, updated_at
            )
            VALUES ('person-blank-school', 'Blank School Lead', 'West U', '2026-05-08T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, school, create_date, pike13_person_id, raw_json, updated_at
            )
            VALUES
                ('contact-blank-inferred', 'Blank School Lead', NULL, '2026-05-04',
                 'person-blank-school', '{"trusted": 1}', '2026-05-08T00:00:00+00:00'),
                ('contact-blank-unassigned', 'Unassigned Lead', NULL, '2026-05-05',
                 NULL, '{"trusted": 1}', '2026-05-08T00:00:00+00:00')
            """
        )

        report = build_operations_dashboard(
            conn,
            period="weekly",
            as_of="2026-05-09",
            schools=("West U",),
        )

        quality = report["hubspot_school_assignment"]
        self.assertEqual(quality["total"], 3)
        self.assertEqual(quality["assigned_school"], 1)
        self.assertEqual(quality["blank_school"], 2)
        self.assertEqual(quality["usable_for_dashboard_schools"], 2)
        self.assertEqual(quality["unassigned_to_dashboard_school"], 1)
        self.assertIn("hubspot_contacts_unassigned_school_1", quality["flags"])
        self.assertNotIn("hubspot_contacts_blank_school_2", quality["flags"])

    def test_operations_funnel_parses_hubspot_dates_and_maps_unified_person_ids(self):
        conn = open_db()
        conn.execute(
            """
            INSERT INTO hubspot_deals (
                deal_id, deal_name, stage, school, create_date, person_id, updated_at
            )
            VALUES ('deal-unified', 'Display Date Lead', 'Scheduled Trial/Tour',
                    'West University Place', 'May 2, 2026 at 9:15 AM CDT',
                    'person-unified', '2026-05-08T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO person_identities (
                person_id, identity_type, identity_value, source_system, source_table,
                source_id, confidence, evidence, created_at
            )
            VALUES ('person-unified', 'pike13_person', 'pike-person-1', 'pike13',
                    'pike13_people', 'pike-person-1', 0.99, 'test', '2026-05-08T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO pike13_visits (
                visit_id, person_id, service, starts_at, status, first_visit_flag,
                attendance_confirmed_flag, checked_in_flag, instructor, school, updated_at
            )
            VALUES ('visit-unified', 'pike-person-1', 'Trial - Drums', '2026-05-03T14:00:00',
                    'Complete', 1, 1, 1, 'Teacher Two', 'West U', '2026-05-08T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO pike13_plans_passes (
                plan_pass_id, person_id, name, status, starts_at, school, payer_name, updated_at
            )
            VALUES ('plan-unified', 'pike-person-1', 'Lessons Only - 45 Minute Lessons',
                    'Active', '2026-05-02', 'West U', 'Payer', '2026-05-08T00:00:00+00:00')
            """
        )

        funnel = funnel_metrics(conn, start_date="2026-05-01", end_date="2026-05-09", school="West U")

        self.assertEqual(funnel["new_leads"], 1)
        self.assertEqual(funnel["leads_to_trial"], 1)
        self.assertEqual(funnel["trial_lessons"], 1)
        self.assertEqual(funnel["trials_converted"], 1)

    def test_dashboard_service_helpers_normalize_school_and_escape_html(self):
        self.assertEqual(normalize_school_slug("heights"), "The Heights")
        self.assertEqual(normalize_school_slug("west-university-place"), "West U")
        with self.assertRaises(ValueError):
            _window_kwargs("yearly")
        with self.assertRaises(ValueError):
            _window_kwargs("monthly", start_date="2026-01-01")

        conn = open_db()
        seed_dashboard_data(conn)
        snapshot = build_snapshot(
            conn,
            "weekly",
            start_date="2026-05-01",
            end_date="2026-05-09",
            school="West U",
        )
        snapshot["school"] = "<script>West U</script>"
        html = lead_dashboard_html(snapshot)

        self.assertIn("&lt;script&gt;West U&lt;/script&gt;", html)
        self.assertNotIn("<script>West U</script>", html)


if __name__ == "__main__":
    unittest.main()
