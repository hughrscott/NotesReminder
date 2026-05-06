import tempfile
import unittest
from pathlib import Path

from scripts.progress_dashboard import render_dashboard


def fake_report():
    return {
        "overall_status": "blocked",
        "window": {
            "days": 7,
            "start": "2026-04-21",
            "end": "2026-04-28",
            "pike13_lookahead_days": 30,
            "pike13_lookahead_end": "2026-05-28",
        },
        "sources": {
            "hubspot": {
                "status": "ready",
                "rows": 25,
                "blockers": [],
                "field_coverage": {
                    "deal_id": {"fill_rate": 100.0, "filled": 25, "total": 25},
                    "stage": {"fill_rate": 100.0, "filled": 25, "total": 25},
                    "school": {"fill_rate": 100.0, "filled": 25, "total": 25},
                    "create_date": {"fill_rate": 84.0, "filled": 21, "total": 25},
                    "source_url": {"fill_rate": 100.0, "filled": 25, "total": 25},
                },
                "contact_quality": {"trusted_rows": 24},
                "latest_import_run": {
                    "status": "success",
                    "finished_at": "2026-04-27T17:53:00+00:00",
                    "rows_seen": 25,
                    "rows_inserted": 74,
                    "rows_updated": 0,
                },
            },
            "dialpad": {
                "status": "ready",
                "blockers": [],
                "sms_rows": 32,
                "voice_rows": 34054,
                "conversation_history_rows": 25,
                "conversation_history_recording_or_transcript_url_rows": 19,
                "call_review_rows": 12,
                "call_review_transcript_rows": 10,
                "call_review_recap_rows": 8,
                "call_review_action_item_rows": 4,
                "daily_intake": {
                    "daily_intake_rows": 40,
                    "communication_window_rows": 45,
                    "daily_inbound_rows": 9,
                    "unmatched_inbound_rows": 3,
                    "possible_lead_not_in_hubspot_rows": 2,
                    "no_followup_rows": 1,
                    "latest_daily_intake_at": "2026-04-29T18:08:44",
                    "discovery_fallback_required": False,
                },
                "route_discovery": {
                    "rows": 7,
                    "usable_routes": 2,
                    "partial_routes": 4,
                    "blocked_routes": 1,
                    "sms_routes": 1,
                    "voice_routes": 5,
                    "call_review_routes": 1,
                },
                "target_search": {
                    "rows": 8,
                    "targets_found": 0,
                    "targets_not_found": 6,
                    "filter_not_supported_rows": 2,
                    "ui_blocked_rows": 0,
                    "auth_blocked_rows": 0,
                },
                "future_sms_timestamp_rows": 0,
                "future_voice_timestamp_rows": 0,
                "sms_field_coverage": {
                    "message_at": {"fill_rate": 100.0, "filled": 32, "total": 32},
                    "direction": {"fill_rate": 100.0, "filled": 32, "total": 32},
                },
                "voice_field_coverage": {
                    "event_at": {"fill_rate": 100.0, "filled": 34045, "total": 34054},
                },
                "latest_sms_import_run": {
                    "status": "success",
                    "finished_at": "2026-04-27T21:00:24+00:00",
                    "rows_seen": 7,
                    "rows_inserted": 119,
                    "rows_updated": 0,
                },
                "latest_voice_import_run": {
                    "status": "success",
                    "finished_at": "2026-04-28T17:59:50+00:00",
                    "rows_seen": 25,
                    "rows_inserted": 25,
                    "rows_updated": 0,
                },
                "latest_daily_intake_import_run": {
                    "status": "success",
                    "finished_at": "2026-04-29T18:15:00+00:00",
                    "rows_seen": 40,
                    "rows_inserted": 5,
                    "rows_updated": 35,
                },
                "latest_call_review_import_run": {
                    "status": "success",
                    "finished_at": "2026-04-29T18:16:00+00:00",
                    "rows_seen": 12,
                    "rows_inserted": 12,
                    "rows_updated": 0,
                    "stale_running_run": {
                        "status": "running",
                        "started_at": "2026-04-30T00:00:00+00:00",
                    },
                },
                "latest_route_discovery_import_run": {
                    "status": "success",
                    "finished_at": "2026-04-29T09:55:00+00:00",
                    "rows_seen": 7,
                    "rows_inserted": 7,
                    "rows_updated": 0,
                },
                "latest_target_search_import_run": {
                    "status": "success",
                    "finished_at": "2026-04-29T10:00:00+00:00",
                    "rows_seen": 8,
                    "rows_inserted": 0,
                    "rows_updated": 0,
                },
            },
            "pike13": {
                "status": "partial",
                "blockers": ["Rich Pike13 lead/outcome visits are not loaded."],
                "lesson_visit_rows": 15679,
                "completed_note_rows": 8514,
                "missing_note_rows": 7165,
                "no_show_rows": 1768,
                "canceled_rows": 720,
                "trial_lesson_rows": 196,
                "latest_lesson_date": "2026-04-18",
                "note_score_coverage": {"fill_rate": 3.0, "filled": 472, "total": 15679},
                "people_rows": 1,
                "visit_rows": 0,
                "rich_visit_rows": 0,
                "report_backed_first_visit_rows": 0,
                "event_enriched_visit_rows": 0,
                "plan_pass_rows": 1,
                "plan_enrichment_rows": 0,
                "window_plus_lookahead_visit_rows": 0,
                "route_discovery": {
                    "rows": 2,
                    "usable_routes": 1,
                    "partial_routes": 1,
                    "blocked_routes": 0,
                },
                "latest_import_run": {
                    "status": "error",
                    "finished_at": "2026-04-28T16:47:33+00:00",
                    "rows_seen": 0,
                    "rows_inserted": 0,
                    "rows_updated": 0,
                },
                "latest_route_discovery_import_run": {
                    "status": "success",
                    "finished_at": "2026-04-29T18:20:00+00:00",
                    "rows_seen": 2,
                    "rows_inserted": 2,
                    "rows_updated": 0,
                },
            },
        },
        "matching": {
            "status": "ready",
            "rows": 2,
            "matched_hubspot_deals": 0,
            "matched_hubspot_contacts": 1,
            "by_match_type": [
                {"match_type": "email_exact", "rows": 1},
                {"match_type": "phone_exact", "rows": 1},
            ],
        },
        "first_value": {
            "status": "ready",
            "report_ready": True,
            "blockers": [],
            "call_review_url_rows": 19,
            "call_review_transcript_rows": 10,
            "call_review_recap_rows": 8,
            "matched_hubspot_deals": 0,
            "matched_hubspot_contacts": 1,
            "candidate_leads": 10,
            "candidate_leads_with_trusted_phone": 6,
            "candidate_leads_with_dialpad_comms": 2,
        },
    }


class ProgressDashboardTests(unittest.TestCase):
    def test_dashboard_renders_operational_status_without_customer_content(self):
        markdown = render_dashboard(fake_report())

        self.assertIn("Overall status: **BLOCKED**", markdown)
        self.assertIn("### HubSpot - READY", markdown)
        self.assertIn("Rows: 25", markdown)
        self.assertIn("Call-review transcript/audio access URLs: 19", markdown)
        self.assertIn("Call-review rows: 12", markdown)
        self.assertIn("Call-review transcripts: 10", markdown)
        self.assertIn("Call-review recaps: 8", markdown)
        self.assertIn("Daily intake tagged rows: 40", markdown)
        self.assertIn("Dialpad communication rows in proof window: 45", markdown)
        self.assertIn("Unmatched inbound rows: 3", markdown)
        self.assertIn("Possible leads not in HubSpot: 2", markdown)
        self.assertIn("Inbound rows without later outbound follow-up: 1", markdown)
        self.assertIn("Latest daily intake import: SUCCESS", markdown)
        self.assertIn("Route-map probes: 7", markdown)
        self.assertIn("Route-map usable/partial/blocked: 2/4/1", markdown)
        self.assertIn("Latest route-map import: SUCCESS", markdown)
        self.assertIn("Targeted lead-phone searches: 8", markdown)
        self.assertIn("Targeted searches not found/blocked: 8", markdown)
        self.assertIn("Latest target-search import: SUCCESS", markdown)
        self.assertIn("Latest call-review import: SUCCESS", markdown)
        self.assertIn("Ignored stale RUNNING run", markdown)
        self.assertIn("## First Value Report - READY", markdown)
        self.assertIn("Report ready: yes", markdown)
        self.assertIn("Lead-attention candidates: 10", markdown)
        self.assertIn("Candidates with matched Dialpad communications: 2", markdown)
        self.assertIn("Lesson visit rows: 15679", markdown)
        self.assertIn("Completed notes: 8514", markdown)
        self.assertIn("Missing notes: 7165", markdown)
        self.assertIn("No-shows: 1768", markdown)
        self.assertIn("Canceled lessons: 720", markdown)
        self.assertIn("Trial lesson rows: 196", markdown)
        self.assertIn("Report-backed first visit rows: 0", markdown)
        self.assertIn("Event-enriched visit rows: 0", markdown)
        self.assertIn("Plan/conversion enriched rows: 0", markdown)
        self.assertIn("Route fallback probes: 2", markdown)
        self.assertIn("Latest route fallback import: SUCCESS", markdown)
        self.assertIn("Pike13: Rich Pike13 lead/outcome visits are not loaded.", markdown)
        self.assertIn("Identity matches: 2", markdown)
        self.assertIn("## Future AI Readiness", markdown)
        self.assertIn("Sentiment/coaching analysis: ready for limited proof", markdown)
        self.assertIn("Lesson-note quality/current-student operations: ready for existing notes data", markdown)
        self.assertIn("AI lead-management automation: not ready", markdown)

        forbidden = [
            "Christina Alten",
            "(713) 555-1212",
            "Hello, this is a transcript",
            "Can we reschedule?",
            "Student One",
            "Raw note text",
        ]
        for value in forbidden:
            self.assertNotIn(value, markdown)

    def test_dashboard_can_be_written_to_markdown_file(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        output = Path(tmp.name) / "lead_intelligence_status.md"

        output.write_text(render_dashboard(fake_report()), encoding="utf-8")

        self.assertTrue(output.exists())
        self.assertIn("Lead Intelligence Progress Dashboard", output.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
