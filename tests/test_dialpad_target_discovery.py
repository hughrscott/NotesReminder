import json
import sqlite3
import tempfile
import unittest
from datetime import date
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema, start_import_run, utc_now_iso
from scripts.discover_dialpad_targets import (
    classify_target_search_result,
    conversation_history_days_for_target,
    conversation_history_participant_url,
    expected_conversation_history_scope,
    filter_voice_rows_to_target_window,
    render_route_map_report,
    render_target_coverage_report,
    route_discovery_summary,
    route_probe_row,
    sanitize_dialpad_url,
    select_target_candidates,
    selected_school_scopes_match,
    school_scope_matches,
    target_hash,
    target_search_summary,
    targeted_sms_rows_from_text,
    upsert_route_discovery,
    upsert_targeted_sms_rows,
    upsert_target_search,
    voice_rows_relative_to_lead_date,
)


class DialpadTargetDiscoveryTests(unittest.TestCase):
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

    def seed_candidate(self, conn):
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO hubspot_deals (
                deal_id, deal_name, stage, owner, school, create_date,
                follow_up_needed, source_url, raw_text, updated_at
            )
            VALUES ('deal-123', 'Sensitive Customer Name', 'New Lead', 'Owner A',
                    'West U', date('now'), 'Yes', 'https://hubspot/deal-123',
                    'raw deal', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, email_normalized, phone, phone_normalized,
                associated_deal_ids, raw_text, raw_json, updated_at
            )
            VALUES ('contact-123', 'Sensitive Customer Name', 'customer@example.com',
                    '(713) 555-1212', '7135551212', 'deal-123',
                    'raw contact', ?, ?)
            """,
            (json.dumps({"trusted": True, "rejected_emails": []}), now),
        )

    def test_select_target_candidates_uses_lead_attention_candidates(self):
        conn = self.open_db()
        self.seed_candidate(conn)

        targets = select_target_candidates(conn, school="West U", window_days=7, limit=25)

        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0]["deal_id"], "deal-123")
        self.assertEqual(targets[0]["target_type"], "phone")
        self.assertEqual(targets[0]["target_hash"], target_hash("7135551212"))
        self.assertEqual(targets[0]["lead_date"], str(conn.execute("SELECT date('now')").fetchone()[0]))

    def test_select_target_candidates_can_use_ytd_hubspot_contact_spine(self):
        conn = self.open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, create_date, phone, phone_normalized,
                school, hubspot_deal_name, associated_deal_ids, raw_json, updated_at
            )
            VALUES ('contact-ytd', 'Sensitive Customer Name', '2026-05-20',
                    '(832) 555-9090', '8325559090', 'The Heights',
                    'Sensitive Customer Name | The Heights', 'deal-ytd',
                    '{"trusted": 1}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, create_date, phone, phone_normalized,
                school, hubspot_deal_name, associated_deal_ids, raw_json, updated_at
            )
            VALUES ('contact-wrong-school', 'Other Lead', '2026-05-20',
                    '(713) 555-9091', '7135559091', 'West University Place',
                    'Other Lead | West University Place', 'deal-west',
                    '{"trusted": 1}', ?)
            """,
            (now,),
        )

        targets = select_target_candidates(
            conn,
            school="The Heights",
            candidate_source="hubspot-contacts",
            start_date="2026-01-01",
            end_date="2026-06-03",
            limit=25,
        )

        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0]["deal_id"], "deal-ytd")
        self.assertEqual(targets[0]["contact_id"], "contact-ytd")
        self.assertEqual(targets[0]["school"], "The Heights")
        self.assertEqual(targets[0]["target_type"], "phone")
        self.assertEqual(targets[0]["target_hash"], target_hash("8325559090"))
        self.assertEqual(targets[0]["lead_date"], "2026-05-20")
        self.assertEqual(targets[0]["window_start"], "2026-01-01")

    def test_hubspot_contact_target_source_uses_matched_pike13_school(self):
        conn = self.open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, school, updated_at
            )
            VALUES ('pike-school-match', 'Matched Lead', 'The Heights', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, create_date, phone, phone_normalized,
                school, pike13_person_id, associated_deal_ids, raw_json, updated_at
            )
            VALUES ('contact-pike-school', 'Matched Lead', '2026-06-01',
                    '(832) 555-0101', '8325550101', '',
                    'pike-school-match', 'deal-pike-school',
                    '{"trusted": 1}', ?)
            """,
            (now,),
        )

        targets = select_target_candidates(
            conn,
            school="The Heights",
            candidate_source="hubspot-contacts",
            start_date="2026-01-01",
            end_date="2026-06-30",
            limit=25,
        )

        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0]["contact_id"], "contact-pike-school")
        self.assertEqual(targets[0]["school"], "The Heights")
        self.assertEqual(targets[0]["target_hash"], target_hash("8325550101"))

    def test_hubspot_no_dialpad_match_target_source_filters_existing_phone_matches(self):
        conn = self.open_db()
        now = utc_now_iso()
        for contact_id, phone in (
            ("contact-missing", "8325550101"),
            ("contact-missing-duplicate-phone", "8325550101"),
            ("contact-present", "8325550102"),
        ):
            conn.execute(
                """
                INSERT INTO hubspot_contacts (
                    contact_id, full_name, create_date, phone, phone_normalized,
                    school, associated_deal_ids, raw_json, updated_at
                )
                VALUES (?, 'Sensitive Lead', '2026-06-01', ?, ?,
                        'The Heights', ?, '{"trusted": 1}', ?)
                """,
                (contact_id, phone, phone, f"deal-{contact_id}", now),
            )
        conn.execute(
            """
            INSERT INTO dialpad_sms_threads (
                thread_id, phone, phone_normalized, contact_name, school, updated_at
            )
            VALUES ('thread-present', '8325550102', '8325550102', 'Sensitive Lead',
                    'The Heights', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_messages (
                message_id, thread_id, message_at, direction, body, updated_at
            )
            VALUES ('message-present', 'thread-present', '2026-06-01T10:00:00',
                    'outbound', 'Private body', ?)
            """,
            (now,),
        )

        targets = select_target_candidates(
            conn,
            school="The Heights",
            candidate_source="hubspot-no-dialpad-match",
            start_date="2026-01-01",
            end_date="2026-06-30",
            limit=25,
        )

        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0]["contact_id"], "contact-missing")
        self.assertEqual(targets[0]["target_hash"], target_hash("8325550101"))

    def test_hubspot_contact_target_source_requires_explicit_window(self):
        conn = self.open_db()

        with self.assertRaises(ValueError):
            select_target_candidates(
                conn,
                school="West U",
                candidate_source="hubspot-contacts",
                start_date="2026-01-01",
            )

    def test_classify_target_search_result(self):
        phone = "7135551212"
        self.assertEqual(
            classify_target_search_result(
                "Your conversation with (713) 555-1212 will appear here.",
                [],
                phone,
            ),
            "not_found_after_route_search",
        )
        self.assertEqual(
            classify_target_search_result(
                "Call with (713) 555-1212",
                [{"href": "https://dialpad.com/callhistory/callreview/abc123", "text": "AI"}],
                phone,
            ),
            "found_call_review",
        )
        self.assertEqual(
            classify_target_search_result("Missed call from (713) 555-1212", [], phone),
            "found_call",
        )
        self.assertEqual(
            classify_target_search_result("No matching records", [], phone),
            "not_found_after_route_search",
        )

    def test_conversation_history_participant_url_and_sanitization(self):
        url = conversation_history_participant_url("(713) 922-9723")

        self.assertEqual(
            url,
            "https://dialpad.com/conversationhistory?days=0-30&external_endpoint=7139229723",
        )
        self.assertEqual(
            sanitize_dialpad_url(url),
            "https://dialpad.com/conversationhistory?days=0-30",
        )

    def test_conversation_history_days_cover_lead_creation_date(self):
        self.assertEqual(
            conversation_history_days_for_target(
                {"lead_date": "2026-05-21"},
                today=date(2026, 6, 1),
            ),
            "0-30",
        )
        self.assertEqual(
            conversation_history_days_for_target(
                {"lead_date": "2026-01-15"},
                today=date(2026, 6, 1),
            ),
            "0-138",
        )

    def test_conversation_history_school_scope_must_match_requested_school(self):
        self.assertEqual(expected_conversation_history_scope("The Heights"), "The Heights")
        self.assertEqual(expected_conversation_history_scope("West University Place"), "West U")
        self.assertTrue(school_scope_matches("The Heights", "The Heights"))
        self.assertFalse(school_scope_matches("West U", "The Heights"))
        self.assertTrue(selected_school_scopes_match(["The Heights"], "The Heights"))
        self.assertFalse(selected_school_scopes_match(["West U"], "The Heights"))
        self.assertFalse(selected_school_scopes_match(["The Heights", "West U"], "The Heights"))
        self.assertFalse(selected_school_scopes_match(["1 Office"], "The Heights"))

    def test_voice_rows_filter_to_lead_creation_date(self):
        rows = [
            {"event_at": "2026-05-07T17:13:38", "event_type": "call"},
            {"event_at": "2026-05-20T14:45:26", "event_type": "call"},
            {"event_at": "2026-05-22T20:36:29", "event_type": "call"},
        ]

        filtered = filter_voice_rows_to_target_window(rows, {"lead_date": "2026-05-20"})

        self.assertEqual([row["event_at"] for row in filtered], ["2026-05-20T14:45:26", "2026-05-22T20:36:29"])

    def test_voice_rows_classify_pre_and_post_lead_contact(self):
        rows = [
            {"event_at": "2026-05-07T17:13:38", "event_type": "call"},
            {"event_at": "2026-05-20T14:45:26", "event_type": "call"},
            {"event_at": None, "event_type": "call"},
        ]

        grouped = voice_rows_relative_to_lead_date(rows, {"lead_date": "2026-05-20"})

        self.assertEqual(len(grouped["pre_lead"]), 1)
        self.assertEqual(len(grouped["post_lead"]), 1)
        self.assertEqual(len(grouped["undated"]), 1)

    def test_targeted_sms_rows_are_redacted_and_queryable_as_communications(self):
        conn = self.open_db()
        target = {
            "deal_id": "deal-123",
            "contact_id": "contact-123",
            "target_hash": target_hash("7135551212"),
            "target_type": "phone",
            "target_value": "7135551212",
            "school": "West U",
            "lead_date": "2026-06-01",
        }
        sms_rows = targeted_sms_rows_from_text(
            "\n".join(
                [
                    "Messages",
                    "(713) 555-1212",
                    "You: Sensitive SMS body should not be stored",
                    "Jun 3",
                ]
            ),
            "https://dialpad.com/app/history/messages?q=7135551212",
            target,
            5,
        )

        changed = upsert_targeted_sms_rows(conn, sms_rows)
        row = conn.execute(
            """
            SELECT channel, event_at, school, body, source_url
            FROM vw_dialpad_communications
            WHERE channel = 'sms'
            """
        ).fetchone()

        self.assertGreater(changed, 0)
        self.assertEqual(row["channel"], "sms")
        self.assertEqual(row["event_at"], "2026-06-03")
        self.assertEqual(row["school"], "West U")
        self.assertEqual(row["body"], "[redacted targeted SMS evidence]")
        self.assertNotIn("7135551212", row["source_url"])

    def test_route_discovery_summary_and_report_are_sanitized(self):
        conn = self.open_db()
        run_id = start_import_run(conn, "dialpad_route_discovery", "test")
        row = route_probe_row(
            run_id,
            {
                "name": "conversation_history",
                "url": "https://dialpad.com/conversationhistory",
                "daily_refresh": True,
                "targeted_search": True,
                "date_filter": True,
                "school_filter": True,
                "keyword_filter": True,
                "required_filter_state": "Office/group set to West U.",
            },
            "usable",
            "Conversation history Call Voicemail Messages Keyword Past 7 days",
            [{"href": "https://dialpad.com/callhistory/callreview/abc123", "text": "AI"}],
            {"school_filter_applied": True, "date_filter_visible": True, "keyword_filter_visible": True},
        )
        row["raw_json"] = json.dumps({"phone": "(713) 555-1212", "customer": "Sensitive Customer Name"})
        upsert_route_discovery(conn, row)

        summary = route_discovery_summary(conn, run_id)
        markdown = render_route_map_report(summary, school="West U")

        self.assertEqual(summary["routes_checked"], 1)
        self.assertEqual(summary["usable_routes"], 1)
        self.assertEqual(summary["call_review_routes"], 1)
        self.assertIn("Dialpad Route Map", markdown)
        self.assertIn("conversation_history", markdown)
        self.assertIn("Usable routes: 1", markdown)
        for forbidden in [
            "Sensitive Customer Name",
            "(713) 555-1212",
            "7135551212",
            "customer@example.com",
            "Sensitive SMS body",
            "Sensitive transcript",
        ]:
            self.assertNotIn(forbidden, markdown)

    def test_target_search_summary_and_report_are_sanitized(self):
        conn = self.open_db()
        run_id = start_import_run(conn, "dialpad_target_search", "test")
        upsert_target_search(
            conn,
            {
                "search_id": f"{run_id}:deal-123:{target_hash('7135551212')}",
                "run_id": run_id,
                "deal_id": "deal-123",
                "contact_id": None,
                "target_hash": target_hash("7135551212"),
                "target_type": "phone",
                "school": "West U",
                "searched_at": utc_now_iso(),
                "search_paths_json": json.dumps([{"path": "global_search", "outcome": "not_found"}]),
                "outcome": "not_found",
                "found_sms_count": 0,
                "found_voice_count": 0,
                "found_call_review_count": 0,
                "source_url_count": 0,
                "first_event_at": None,
                "latest_event_at": None,
                "raw_json": json.dumps({"customer": "Sensitive Customer Name", "phone": "(713) 555-1212"}),
                "updated_at": utc_now_iso(),
            },
        )

        summary = target_search_summary(conn, run_id)
        markdown = render_target_coverage_report(summary, school="West U", window_days=7)

        self.assertEqual(summary["targets_searched"], 1)
        self.assertEqual(summary["targets_not_found"], 1)
        self.assertIn("Candidate targets searched: 1", markdown)
        self.assertIn("not_found: 1", markdown)
        for forbidden in [
            "Sensitive Customer Name",
            "(713) 555-1212",
            "7135551212",
            "customer@example.com",
            "Sensitive SMS body",
            "Sensitive transcript",
        ]:
            self.assertNotIn(forbidden, markdown)

    def test_target_search_summary_counts_school_scope_mismatch_as_blocked(self):
        conn = self.open_db()
        run_id = start_import_run(conn, "dialpad_target_search", "test")
        upsert_target_search(
            conn,
            {
                "search_id": f"{run_id}:deal-123:{target_hash('7135551212')}",
                "run_id": run_id,
                "deal_id": "deal-123",
                "contact_id": "contact-123",
                "target_hash": target_hash("7135551212"),
                "target_type": "phone",
                "school": "The Heights",
                "searched_at": utc_now_iso(),
                "search_paths_json": json.dumps(
                    [
                        {
                            "path": "conversation_history",
                            "outcome": "school_scope_mismatch",
                            "selector": "active_school_scope",
                        }
                    ]
                ),
                "outcome": "school_scope_mismatch",
                "found_sms_count": 0,
                "found_voice_count": 0,
                "found_call_review_count": 0,
                "source_url_count": 0,
                "first_event_at": None,
                "latest_event_at": None,
                "raw_json": "{}",
                "updated_at": utc_now_iso(),
            },
        )

        summary = target_search_summary(conn, run_id)
        markdown = render_target_coverage_report(summary, school="The Heights", window_days=7)

        self.assertEqual(summary["targets_searched"], 1)
        self.assertEqual(summary["outcomes"]["school_scope_mismatch"], 1)
        self.assertEqual(summary["targets_blocked_or_unsupported"], 1)
        self.assertIn("school_scope_mismatch: 1", markdown)


if __name__ == "__main__":
    unittest.main()
