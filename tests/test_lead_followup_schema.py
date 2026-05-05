import json
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from lead_followup_schema import (
    ensure_lead_followup_schema,
    finish_import_run,
    normalize_email,
    normalize_phone,
    start_import_run,
    utc_now_iso,
)
from source_completeness import build_source_completeness_report, import_run_summary


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
            "dialpad_voice_events",
            "dialpad_call_reviews",
            "dialpad_target_searches",
            "dialpad_route_discoveries",
            "source_route_discoveries",
            "pike13_people",
            "pike13_visits",
            "pike13_plans_passes",
            "identity_matches",
            "communication_ai_insights",
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

    def test_import_run_summary_uses_latest_completed_when_running_run_is_stale(self):
        conn = self.open_db()
        completed_id = start_import_run(
            conn,
            "dialpad_call_reviews",
            "extract_dialpad_call_reviews.py",
            "2026-04-25",
        )
        finish_import_run(
            conn,
            completed_id,
            "success",
            rows_seen=117,
            rows_inserted=117,
        )
        conn.execute(
            """
            INSERT INTO source_import_runs (
                source, extractor, started_at, status, rows_seen, rows_inserted, rows_updated
            )
            VALUES (
                'dialpad_call_reviews', 'extract_dialpad_call_reviews.py',
                '2026-05-01T00:00:00+00:00', 'running', 0, 0, 0
            )
            """
        )

        summary = import_run_summary(
            conn,
            "dialpad_call_reviews",
            now=datetime.fromisoformat("2026-05-01T12:00:00+00:00"),
        )
        self.assertEqual(summary["status"], "success")
        self.assertEqual(summary["rows_seen"], 117)
        self.assertEqual(summary["stale_running_run"]["status"], "running")
        self.assertEqual(
            summary["stale_running_run"]["started_at"],
            "2026-05-01T00:00:00+00:00",
        )

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
        self.assertEqual(unanswered[0]["message_id"], "msg-1")

        unanswered_comms = conn.execute("SELECT * FROM vw_unanswered_communications").fetchall()
        self.assertEqual(len(unanswered_comms), 1)
        self.assertEqual(unanswered_comms[0]["communication_id"], "msg-1")

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

    def test_unanswered_communications_use_later_outbound_call_followup(self):
        conn = self.open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO call_logs (
                call_id, external_number, date_started, direction, category, name,
                school_name, voicemail_transcript
            )
            VALUES ('call-in-1', '(713) 555-1212', '2025-01-05T10:00:00',
                    'inbound', 'Voicemail', 'M Sample', 'West U', 'Please call me back')
            """
        )
        rows = conn.execute("SELECT * FROM vw_unanswered_communications").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event_type"], "voicemail")

        conn.execute(
            """
            INSERT INTO call_logs (
                call_id, external_number, date_started, direction, category, name,
                school_name, voicemail_transcript
            )
            VALUES ('call-out-1', '(713) 555-1212', '2025-01-05T12:00:00',
                    'outbound', 'Call', 'M Sample', 'West U', NULL)
            """
        )
        rows = conn.execute("SELECT * FROM vw_unanswered_communications").fetchall()
        self.assertEqual(rows, [])

        conn.execute(
            """
            INSERT INTO communication_ai_insights (
                source_table, source_id, model, prompt_version, sentiment,
                intent, summary, created_at
            )
            VALUES ('call_logs', 'call-in-1', 'test-model', 'v1',
                    'concerned', 'callback_request', 'Caller requested follow-up.', ?)
            """,
            (now,),
        )
        insight = conn.execute("SELECT * FROM communication_ai_insights").fetchone()
        self.assertEqual(insight["intent"], "callback_request")

    def test_dialpad_daily_intake_marks_unmatched_inbound_and_later_followup(self):
        conn = self.open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, call_id, phone, phone_normalized,
                contact_name, direction, event_at, school, department, outcome,
                source_url, raw_text, raw_json, updated_at
            )
            VALUES (
                'daily-in-1', 'conversation_history', 'missed_call', 'call-1',
                '(713) 555-1212', '7135551212', 'Sensitive Name', 'inbound',
                '2026-04-28T10:00:00', 'West U', 'WESTU', 'missed',
                'https://dialpad.com/conversationhistory?external_endpoint=7135551212',
                'raw sensitive row', '{}', ?
            )
            """,
            (now,),
        )
        unmatched = conn.execute("SELECT * FROM vw_unmatched_dialpad_inbound").fetchall()
        self.assertEqual(len(unmatched), 1)
        self.assertEqual(unmatched[0]["action_status"], "possible_lead_not_in_hubspot")
        self.assertEqual(unmatched[0]["has_later_outbound_followup"], 0)

        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, call_id, phone, phone_normalized,
                contact_name, direction, event_at, school, department, outcome,
                source_url, raw_text, raw_json, updated_at
            )
            VALUES (
                'daily-out-1', 'conversation_history', 'call', 'call-2',
                '(713) 555-1212', '7135551212', 'Sensitive Name', 'outbound',
                '2026-04-28T11:00:00', 'West U', 'WESTU', 'called back',
                'https://dialpad.com/conversationhistory', 'raw sensitive row',
                '{}', ?
            )
            """,
            (now,),
        )
        daily = conn.execute(
            "SELECT * FROM vw_dialpad_daily_intake WHERE communication_id = 'daily-in-1'"
        ).fetchone()
        self.assertEqual(daily["has_later_outbound_followup"], 1)
        self.assertEqual(daily["action_status"], "followed_up")
        unmatched = conn.execute("SELECT * FROM vw_unmatched_dialpad_inbound").fetchall()
        self.assertEqual(len(unmatched), 1)
        self.assertEqual(unmatched[0]["has_later_outbound_followup"], 1)

    def test_dialpad_daily_intake_excludes_trusted_hubspot_phone_from_unmatched(self):
        conn = self.open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, email_normalized, phone_normalized,
                associated_deal_ids, raw_json, updated_at
            )
            VALUES ('contact-1', 'Sensitive Name', 'parent@example.com', '7135551212',
                    'deal-1', ?, ?)
            """,
            (json.dumps({"trusted": True}), now),
        )
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, call_id, phone, phone_normalized,
                contact_name, direction, event_at, school, department, outcome,
                source_url, raw_text, raw_json, updated_at
            )
            VALUES (
                'daily-in-1', 'conversation_history', 'voicemail', 'call-1',
                '(713) 555-1212', '7135551212', 'Sensitive Name', 'inbound',
                '2026-04-28T10:00:00', 'West U', 'WESTU', 'voicemail',
                'https://dialpad.com/callhistory/callreview/call-1',
                'raw sensitive row', '{}', ?
            )
            """,
            (now,),
        )
        daily = conn.execute("SELECT * FROM vw_dialpad_daily_intake").fetchone()
        self.assertEqual(daily["match_status"], "matched_hubspot")
        unmatched = conn.execute("SELECT * FROM vw_unmatched_dialpad_inbound").fetchall()
        self.assertEqual(unmatched, [])

    def test_source_route_discovery_is_repeatable(self):
        conn = self.open_db()
        run_id = start_import_run(conn, "pike13_route_discovery", "discover_pike13_routes.py")
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO source_route_discoveries (
                route_id, run_id, source, route_name, route_url, status,
                loaded_at, visible_row_count, visible_link_count,
                source_timestamp_visible, transcript_link_visible,
                recording_link_visible, expected_controls_json, blocker,
                raw_json, updated_at
            )
            VALUES (
                'route-1', ?, 'pike13', 'known_person', 'https://westu-sor.pike13.com/people/1',
                'partial', ?, 12, 3, 1, 0, 0, '[]',
                'Route loaded, but expected Pike13 fields were not visible.',
                '{"raw_page_text_not_stored": true}', ?
            )
            """,
            (run_id, now, now),
        )
        finish_import_run(conn, run_id, "success", rows_seen=1, rows_inserted=1)
        report = build_source_completeness_report(conn, window_days=7, pike13_lookahead_days=30)
        route = report["sources"]["pike13"]["route_discovery"]
        self.assertEqual(route["rows"], 1)
        self.assertEqual(route["partial_routes"], 1)
        self.assertEqual(route["source_timestamp_routes"], 1)

    def test_source_completeness_report_identifies_partial_and_matching(self):
        conn = self.open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO hubspot_deals (
                deal_id, deal_name, stage, school, create_date,
                pike13_person_id, source_url, raw_text, updated_at
            )
            VALUES ('deal-1', 'M Sample | West U', 'Scheduled Trial', 'West U',
                    'Apr 20, 2026 at 9:00 AM CDT', '15046380', 'https://hubspot/deal-1', 'raw deal', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, email_normalized, phone_normalized,
                associated_deal_ids, source_url, raw_text, raw_json, updated_at
            )
            VALUES ('contact-1', 'M Sample', 'parent@example.com', '7135551212',
                    'deal-1', 'https://hubspot/contact-1', 'raw contact', ?, ?)
            """,
            (json.dumps({"trusted": True, "rejected_emails": []}), now),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_threads (
                thread_id, phone, phone_normalized, contact_name, school, source_url, raw_text, updated_at
            )
            VALUES ('thread-1', '(713) 555-1212', '7135551212', 'M Sample',
                    'West U', 'https://dialpad/thread-1', 'raw thread', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_messages (
                message_id, thread_id, message_at, direction, body, source_url, raw_text, raw_json, updated_at
            )
            VALUES ('msg-1', 'thread-1', '2026-04-21', 'inbound',
                    'I want lessons', 'https://dialpad/msg-1', 'raw sms', ?, ?)
            """,
            (
                json.dumps(
                    {
                        "extraction_source": "thread_detail",
                        "direction_source": "observed",
                        "source_timestamp_field": "message_at",
                    }
                ),
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, email_normalized, phone_normalized,
                membership_state, school, source_url, raw_text, updated_at
            )
            VALUES ('15046380', 'M Sample', 'parent@example.com', '7135551212',
                    'No Membership', 'West U', 'https://pike13/person', 'raw person', ?)
            """,
            (now,),
        )

        report = build_source_completeness_report(conn, window_days=7, pike13_lookahead_days=30)
        self.assertEqual(report["window"]["days"], 7)
        self.assertEqual(report["sources"]["hubspot"]["rows"], 1)
        self.assertEqual(report["sources"]["hubspot"]["field_coverage"]["create_date"]["fill_rate"], 100.0)
        self.assertEqual(report["sources"]["hubspot"]["contact_quality"]["trusted_rows"], 1)
        self.assertEqual(report["sources"]["hubspot"]["contact_quality"]["trusted_customer_email_rows"], 1)
        self.assertEqual(report["sources"]["dialpad"]["sms_rows"], 1)
        self.assertEqual(report["sources"]["dialpad"]["sms_extraction_sources"]["thread_detail"], 1)
        self.assertEqual(report["sources"]["dialpad"]["sms_direction_sources"]["observed"], 1)
        self.assertEqual(report["sources"]["dialpad"]["future_sms_timestamp_rows"], 0)
        self.assertEqual(report["sources"]["pike13"]["people_rows"], 1)
        self.assertIn(report["overall_status"], {"partial", "blocked"})
        self.assertGreaterEqual(report["matching"]["rows"], 1)

    def test_source_completeness_blocks_future_dialpad_timestamps(self):
        conn = self.open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_sms_threads (
                thread_id, phone, phone_normalized, source_url, raw_text, updated_at
            )
            VALUES ('future-thread', '(713) 555-1212', '7135551212',
                    'https://dialpad/thread', 'raw thread', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_messages (
                message_id, thread_id, message_at, direction, body, source_url, raw_text, raw_json, updated_at
            )
            VALUES ('future-msg', 'future-thread', '2099-01-01', 'inbound',
                    'Future dated message', 'https://dialpad/msg', 'raw sms', ?, ?)
            """,
            (json.dumps({"extraction_source": "thread_detail", "direction_source": "observed"}), now),
        )

        report = build_source_completeness_report(conn, window_days=7, pike13_lookahead_days=30)
        dialpad = report["sources"]["dialpad"]
        self.assertEqual(dialpad["future_sms_timestamp_rows"], 1)
        self.assertEqual(dialpad["status"], "blocked")
        self.assertTrue(any("future source timestamps" in blocker for blocker in dialpad["blockers"]))

    def test_source_completeness_reports_conversation_history_access_proof(self):
        conn = self.open_db()
        now = utc_now_iso()
        run_id = start_import_run(
            conn,
            "dialpad_voice",
            "extract_dialpad_voice.py",
            "2026-04-21",
        )
        finish_import_run(
            conn,
            run_id,
            "success",
            rows_seen=1,
            rows_inserted=1,
            metadata={
                "views": ["conversation_history"],
                "view_summaries": {
                    "conversation_history": {
                        "rows": 1,
                        "ai_action_rows": 1,
                        "recording_action_rows": 1,
                        "recording_or_transcript_url_rows": 0,
                    }
                },
            },
        )
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, direction, contact_name, event_at,
                school, department, source_url, raw_text, raw_json, updated_at
            )
            VALUES ('conv-1', 'conversation_history', 'call', 'inbound',
                    'M Sample', '2026-04-27T20:22:05', 'West U', 'WESTU',
                    'https://dialpad.com/conversationhistory', 'raw row', ?, ?)
            """,
            (
                json.dumps(
                    {
                        "extraction": "conversation_history_table",
                        "source_timestamp_field": "event_at",
                        "recording_action_visible": True,
                        "transcript_status": "ai_icon_visible",
                    }
                ),
                now,
            ),
        )

        report = build_source_completeness_report(conn, window_days=7, pike13_lookahead_days=30)
        dialpad = report["sources"]["dialpad"]
        self.assertEqual(dialpad["conversation_history_rows"], 1)
        self.assertEqual(dialpad["conversation_history_ai_action_rows"], 1)
        self.assertEqual(dialpad["conversation_history_recording_action_rows"], 1)
        self.assertFalse(any("Conversation History proof" in blocker for blocker in dialpad["blockers"]))

    def test_source_completeness_blocks_conversation_history_without_transcript_or_recording_access(self):
        conn = self.open_db()
        now = utc_now_iso()
        run_id = start_import_run(
            conn,
            "dialpad_voice",
            "extract_dialpad_voice.py",
            "2026-04-21",
        )
        finish_import_run(
            conn,
            run_id,
            "success",
            rows_seen=1,
            rows_inserted=1,
            metadata={
                "views": ["conversation_history"],
                "view_summaries": {
                    "conversation_history": {
                        "rows": 1,
                        "ai_action_rows": 0,
                        "recording_action_rows": 0,
                        "recording_or_transcript_url_rows": 0,
                    }
                },
            },
        )
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, direction, contact_name, event_at,
                school, department, source_url, raw_text, raw_json, updated_at
            )
            VALUES ('conv-1', 'conversation_history', 'call', 'inbound',
                    'M Sample', '2026-04-27T20:22:05', 'West U', 'WESTU',
                    'https://dialpad.com/conversationhistory', 'raw row', ?, ?)
            """,
            (
                json.dumps(
                    {
                        "extraction": "conversation_history_table",
                        "source_timestamp_field": "event_at",
                        "recording_action_visible": False,
                        "transcript_status": "not_visible",
                    }
                ),
                now,
            ),
        )

        report = build_source_completeness_report(conn, window_days=7, pike13_lookahead_days=30)
        dialpad = report["sources"]["dialpad"]
        self.assertEqual(dialpad["conversation_history_rows"], 1)
        self.assertEqual(dialpad["conversation_history_ai_action_rows"], 0)
        self.assertEqual(dialpad["conversation_history_recording_action_rows"], 0)
        self.assertEqual(dialpad["status"], "blocked")
        self.assertTrue(any("AI transcript actions" in blocker for blocker in dialpad["blockers"]))
        self.assertTrue(any("recording/play access" in blocker for blocker in dialpad["blockers"]))

    def test_source_completeness_uses_existing_reminders_as_pike13_lesson_visits(self):
        conn = self.open_db()
        conn.execute(
            """
            CREATE TABLE reminders (
                lesson_id TEXT PRIMARY KEY,
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
                pike13_lesson_id TEXT,
                note_score REAL,
                last_checked TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO reminders (
                lesson_id, school, instructor_name, lesson_date, lesson_time,
                lesson_type, students, location, note_completed, attendance_status,
                notes_text, note_timestamp, pike13_lesson_id, note_score, last_checked
            )
            VALUES
                ('lesson-1', 'westu-sor', 'Instructor A', '2026-04-21', '4:00 PM',
                 'Trial - Guitar', 'Student One', 'Room 1', 1, 'complete',
                 'Raw note text', '2026-04-21T22:00:00', 'pike-1', 8.0, '2026-04-21'),
                ('lesson-2', 'westu-sor', 'Instructor B', '2026-04-22', '5:00 PM',
                 'Drum Lessons - 45 minutes', 'Student Two', 'Room 2', 0, 'no show',
                 '', NULL, 'pike-2', NULL, '2026-04-22'),
                ('lesson-3', 'westu-sor', 'Instructor C', '2026-04-23', '6:00 PM',
                 'Guitar Lessons - 45 minutes', 'Student Three', 'Room 3', 0, 'canceled',
                 '', NULL, 'pike-3', NULL, '2026-04-23')
            """
        )
        route_run_id = start_import_run(conn, "pike13_route_discovery", "test", metadata={"route_count": 2})
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO source_route_discoveries (
                route_id, run_id, source, route_name, route_url, status, loaded_at,
                visible_row_count, visible_link_count, source_timestamp_visible,
                transcript_link_visible, recording_link_visible, expected_controls_json,
                blocker, raw_json, updated_at
            )
            VALUES
                ('route-person', ?, 'pike13', 'known_person', 'https://westu-sor.pike13.com/people/1',
                 'usable', ?, 10, 5, 1, 0, 0, '[]', NULL, '{}', ?),
                ('route-events', ?, 'pike13', 'visits_or_events', 'https://westu-sor.pike13.com/events',
                 'usable', ?, 4, 1, 0, 0, 0, '[]', NULL, '{}', ?)
            """,
            (route_run_id, now, now, route_run_id, now, now),
        )
        finish_import_run(conn, route_run_id, "success", rows_seen=2, rows_inserted=2, rows_updated=0)

        report = build_source_completeness_report(conn, window_days=7, pike13_lookahead_days=30)
        pike13 = report["sources"]["pike13"]

        self.assertEqual(pike13["status"], "partial")
        self.assertEqual(pike13["lesson_visit_rows"], 3)
        self.assertEqual(pike13["completed_note_rows"], 1)
        self.assertEqual(pike13["missing_note_rows"], 2)
        self.assertEqual(pike13["no_show_rows"], 1)
        self.assertEqual(pike13["canceled_rows"], 1)
        self.assertEqual(pike13["trial_lesson_rows"], 1)
        self.assertEqual(pike13["note_score_coverage"]["filled"], 1)
        self.assertTrue(
            any(
                "route discovery can load Pike13 pages, but the extractor did not find visit/event IDs" in blocker
                for blocker in pike13["blockers"]
            )
        )


if __name__ == "__main__":
    unittest.main()
