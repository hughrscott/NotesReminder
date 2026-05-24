import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import mcp_server
from lead_followup_schema import ensure_lead_followup_schema
from notesreminder.reports.communication_insights import (
    PROMPT_VERSION,
    collect_source_events,
    generate_insights,
    render_review_markdown,
)


def seed_communication_data(conn):
    ensure_lead_followup_schema(conn)
    conn.execute(
        """
        INSERT INTO dialpad_voice_events (
            event_id, event_type, call_id, direction, event_at, school,
            source_url, raw_text, updated_at
        )
        VALUES ('voice-1', 'call', 'call-1', 'incoming', '2026-05-21T14:00:00',
                'West U', 'https://dialpad.test/call', '', '2026-05-21T14:01:00')
        """
    )
    conn.execute(
        """
        INSERT INTO dialpad_call_reviews (
            call_review_id, call_id, voice_event_id, call_review_url, event_at,
            transcript_text, recap_text, transcript_available, recap_available,
            action_items_available, audio_available, extraction_status, raw_json,
            updated_at
        )
        VALUES ('review-1', 'call-1', 'voice-1', 'https://dialpad.test/review',
                '2026-05-21T14:00:00',
                'The parent asked to schedule a trial and requested a call back today.',
                'Trial interest with callback request.', 1, 1, 0, 0, 'success', '{}',
                '2026-05-21T14:05:00')
        """
    )
    conn.execute(
        """
        INSERT INTO school_email_messages (
            message_id, thread_id, school_mailbox, school, direction, message_at,
            from_email, from_email_normalized, to_emails, to_emails_normalized,
            subject, snippet, body, source_url, raw_text, raw_json, updated_at
        )
        VALUES ('email-1', 'thread-1', 'westu@example.test', 'West U', 'inbound',
                '2026-05-21T15:00:00', 'parent@example.test', 'parent@example.test',
                'school@example.test', 'school@example.test', 'Concern',
                'I am worried about the schedule.',
                'I am worried and need to reschedule the lesson.', 'https://mail.test/msg',
                '', '{}', '2026-05-21T15:01:00')
        """
    )


class CommunicationInsightTests(unittest.TestCase):
    def test_collects_events_and_stores_auditable_insights(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        seed_communication_data(conn)

        events = collect_source_events(conn, "2026-05-21", "2026-05-21", school="West U", limit=10)
        self.assertEqual([event.source_table for event in events], ["dialpad_call_reviews", "school_email_messages"])

        report = generate_insights(conn, "2026-05-21", "2026-05-21", school="West U", limit=10)
        self.assertEqual(report["rows_written"], 2)
        self.assertFalse(report["sensitive_content_included"])
        rows = conn.execute(
            """
            SELECT source_table, source_id, intent, urgency, evidence_json,
                   review_status, recommendation
            FROM communication_ai_insights
            ORDER BY source_table
            """
        ).fetchall()
        self.assertEqual(len(rows), 2)
        for row in rows:
            evidence = json.loads(row["evidence_json"])
            self.assertEqual(evidence["prompt_version"], PROMPT_VERSION)
            self.assertEqual(evidence["source_table"], row["source_table"])
            self.assertEqual(evidence["source_id"], row["source_id"])
            self.assertEqual(row["review_status"], "pending_human_review")
            self.assertTrue(row["recommendation"])

    def test_review_markdown_is_sanitized(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        seed_communication_data(conn)

        report = generate_insights(conn, "2026-05-21", "2026-05-21", school="West U", limit=10, dry_run=True)
        markdown = render_review_markdown(report)
        self.assertIn("Experimental Communication Insights", markdown)
        self.assertIn("trial_or_tour_interest", markdown)
        self.assertNotIn("parent@example.test", markdown)
        self.assertNotIn("requested a call back today", markdown)
        self.assertNotIn("https://dialpad.test", markdown)

    def test_mcp_dry_run_uses_shared_logic(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = Path(tmp.name) / "insights.db"
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        seed_communication_data(conn)
        conn.commit()
        conn.close()

        original_path = mcp_server.LEAD_DB_PATH
        mcp_server.LEAD_DB_PATH = str(db_path)
        self.addCleanup(setattr, mcp_server, "LEAD_DB_PATH", original_path)

        mcp_report = json.loads(
            mcp_server.experimental_communication_insights(
                start_date="2026-05-21",
                end_date="2026-05-21",
                school="West U",
                limit=10,
                dry_run=True,
            )
        )
        self.assertEqual(mcp_report["rows_seen"], 2)
        self.assertEqual(mcp_report["rows_written"], 0)
        self.assertTrue(mcp_report["dry_run"])


if __name__ == "__main__":
    unittest.main()
