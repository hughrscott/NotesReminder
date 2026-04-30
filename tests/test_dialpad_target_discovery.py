import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema, start_import_run, utc_now_iso
from scripts.discover_dialpad_targets import (
    classify_target_search_result,
    conversation_history_participant_url,
    render_route_map_report,
    render_target_coverage_report,
    route_discovery_summary,
    route_probe_row,
    sanitize_dialpad_url,
    select_target_candidates,
    target_hash,
    target_search_summary,
    upsert_route_discovery,
    upsert_target_search,
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

    def test_classify_target_search_result(self):
        phone = "7135551212"
        self.assertEqual(
            classify_target_search_result(
                "Messages thread for (713) 555-1212 with recent text message",
                [],
                phone,
            ),
            "found_sms",
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


if __name__ == "__main__":
    unittest.main()
