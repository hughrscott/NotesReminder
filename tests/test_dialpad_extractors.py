import json
import unittest
from datetime import datetime

from scripts.extract_dialpad_sms import (
    detect_department as detect_sms_department,
    extract_message_lines,
    is_dialpad_app_page as is_sms_app_page,
    is_login_page as is_sms_login_page,
    normalize_dialpad_date as normalize_sms_date,
    sms_extraction_source,
)
from scripts.extract_dialpad_voice import (
    conversation_history_row_from_dom,
    is_dialpad_app_page as is_voice_app_page,
    is_login_page as is_voice_login_page,
    parse_conversation_history_rows,
    rows_from_visible_text,
    summarize_view,
)


class DialpadExtractorTests(unittest.TestCase):
    def test_sms_parser_ignores_navigation_labels(self):
        messages = extract_message_lines(
            "\n".join(
                [
                    "Messages",
                    "Calls",
                    "Voicemails",
                    "Today",
                    "Inbound: Can you call me?",
                    "Outbound: Yes, calling now.",
                ]
            ),
            now=datetime(2026, 4, 27),
        )
        self.assertEqual([row["direction"] for row in messages], ["inbound", "outbound"])
        self.assertEqual(messages[0]["body"], "Can you call me?")
        self.assertEqual(messages[0]["message_at"], "2026-04-27")

    def test_sms_parser_normalizes_dialpad_dates_and_infers_auto_reply_inbound(self):
        messages = extract_message_lines(
            "\n".join(
                [
                    "Fri Apr 10",
                    "Sorry, I can’t talk right now.",
                    "4/17/2025",
                    "You: Calling now.",
                ]
            ),
            now=datetime(2026, 4, 27),
        )
        self.assertEqual(messages[0]["message_at"], "2026-04-10")
        self.assertEqual(messages[0]["direction"], "inbound")
        self.assertEqual(messages[1]["message_at"], "2025-04-17")
        self.assertEqual(messages[1]["direction"], "outbound")

    def test_sms_parser_ignores_app_shell_and_parses_history_list_snippets(self):
        messages = extract_message_lines(
            "\n".join(
                [
                    "The power of Dialpad. On your desktop.",
                    "Download",
                    "Multiple tabs detected.",
                    "Dialpad supports only one active app tab. Having multiple Dialpad tabs may cause you to miss calls.",
                    "Unread messages",
                    "MR",
                    "Manttari Robert",
                    "Robo? OKLD TRNI",
                    '"Hahaha "',
                    "Fri Apr 10",
                    "(833) 694-5895",
                    '"It is Wine Futures Friday at Soda Rock! Quantities are limited."',
                    "Fri Mar 13",
                ]
            ),
            now=datetime(2026, 4, 27),
            default_direction="inbound",
        )
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0]["body"], "Hahaha")
        self.assertEqual(messages[0]["message_at"], "2026-04-10")
        self.assertEqual(messages[0]["direction"], "inbound")
        self.assertEqual(messages[0]["direction_source"], "inferred")
        self.assertEqual(messages[0]["timestamp_source"], "visible_date")
        self.assertEqual(messages[1]["message_at"], "2026-03-13")

    def test_sms_marks_thread_detail_and_department_context(self):
        self.assertEqual(sms_extraction_source("https://dialpad.com/app/feed/123456"), "thread_detail")
        self.assertEqual(sms_extraction_source("https://dialpad.com/app/history/messages"), "message_list")
        self.assertEqual(detect_sms_department("Departments\nWESTU\nMessages"), ("West U", "WESTU"))

    def test_extractors_detect_dialpad_login_pages(self):
        login_text = "Log in to Dialpad\nWORK EMAIL\nPASSWORD"
        self.assertTrue(is_sms_login_page("https://dialpad.com/login", login_text))
        self.assertTrue(is_voice_login_page("https://dialpad.com/login", login_text))
        app_text = "Search Dialpad\nDepartments\nMessages\nCalls"
        self.assertTrue(is_sms_app_page("https://dialpad.com/app/history/messages", app_text))
        self.assertTrue(is_voice_app_page("https://dialpad.com/app/history/calls", app_text))
        self.assertFalse(is_sms_app_page("https://dialpad.okta.com", app_text))

    def test_sms_date_normalizer_handles_relative_and_short_dates(self):
        now = datetime(2026, 4, 27)
        self.assertEqual(normalize_sms_date("Today", now=now), "2026-04-27")
        self.assertEqual(normalize_sms_date("Yesterday", now=now), "2026-04-26")
        self.assertEqual(normalize_sms_date("Mon Feb 2", now=now), "2026-02-02")
        self.assertEqual(normalize_sms_date("Thu Dec 18", now=now), "2025-12-18")

    def test_voice_parser_preserves_voicemail_transcript_text(self):
        rows = rows_from_visible_text(
            "voicemails",
            "https://dialpad.com/app/history/voicemails",
            "\n".join(
                [
                    "Voicemails",
                    "Calls",
                    "Missed",
                    "Hello, this is a voicemail transcript asking for a callback about lessons.",
                    "Missed call & voicemail",
                ]
            ),
            limit=10,
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["event_type"], "voicemail")
        self.assertIn("callback", rows[0]["voicemail_transcript"])
        self.assertEqual(rows[1]["event_type"], "voicemail")

    def test_voice_parser_extracts_call_history_blocks(self):
        rows = rows_from_visible_text(
            "calls",
            "https://dialpad.com/app/history/calls",
            "\n".join(
                [
                    "Calls",
                    "(832) 886-3081",
                    "9s",
                    "Mon, Feb 2",
                    "Incoming",
                    "JASON BUTLER",
                    "1s",
                    "Thu, Jan 15",
                    "Incoming",
                ]
            ),
            limit=10,
            now=datetime(2026, 4, 27),
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["event_type"], "call")
        self.assertEqual(rows[0]["direction"], "inbound")
        self.assertEqual(rows[0]["phone_normalized"], "8328863081")
        self.assertEqual(rows[0]["event_at"], "2026-02-02")
        self.assertEqual(rows[1]["contact_name"], "JASON BUTLER")
        self.assertEqual(rows[1]["event_at"], "2026-01-15")

    def test_voice_parser_extracts_missed_call_and_voicemail(self):
        rows = rows_from_visible_text(
            "missed",
            "https://dialpad.com/app/history/missed",
            "\n".join(
                [
                    "West Newton In",
                    "Missed call & voicemail",
                    "Fri Aug 29",
                    "WILSON NC",
                    "Missed call",
                    "Tue Jul 15",
                ]
            ),
            limit=10,
            now=datetime(2026, 4, 27),
        )
        self.assertEqual(rows[0]["event_type"], "voicemail")
        self.assertEqual(rows[0]["direction"], "inbound")
        self.assertEqual(rows[0]["event_at"], "2025-08-29")
        self.assertEqual(rows[1]["event_type"], "missed_call")

    def test_voice_parser_records_diagnostics_and_recording_links(self):
        rows = rows_from_visible_text(
            "recordings",
            "https://dialpad.com/app/history/recordings",
            "\n".join(
                [
                    "Departments",
                    "WESTU",
                    "Recording",
                    "Mon Apr 20",
                    "(713) 555-1212",
                    "This transcript says the parent wants to reschedule the trial lesson.",
                ]
            ),
            limit=10,
            now=datetime(2026, 4, 27),
            links=[{"href": "https://dialpad.com/app/recordings/rec_123456", "text": "Recording"}],
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event_type"], "recording")
        self.assertEqual(rows[0]["event_at"], "2026-04-20")
        self.assertEqual(rows[0]["phone_normalized"], "7135551212")
        self.assertEqual(rows[0]["department"], "WESTU")
        self.assertIn("recordings/rec_123456", rows[0]["recording_url"])
        self.assertIn("reschedule", rows[0]["transcript_summary"])

    def test_voice_view_summary_reports_transcript_and_link_availability(self):
        rows = rows_from_visible_text(
            "voicemails",
            "https://dialpad.com/app/history/voicemails",
            "\n".join(
                [
                    "Tue Apr 21",
                    "(713) 555-1212",
                    "Missed call & voicemail",
                    "This is a voicemail transcript with a clear callback request.",
                ]
            ),
            limit=10,
            now=datetime(2026, 4, 27),
            links=[{"href": "https://dialpad.com/app/history/voicemails", "text": "Download"}],
        )
        summary = summarize_view(
            "voicemails",
            "https://dialpad.com/app/history/voicemails",
            rows,
            [{"href": "https://dialpad.com/app/history/voicemails", "text": "Download"}],
        )
        self.assertGreaterEqual(summary["rows"], 1)
        self.assertGreaterEqual(summary["transcript_rows"], 1)
        self.assertGreaterEqual(summary["voicemail_transcript_rows"], 1)
        self.assertTrue(summary["availability"]["download_link_visible"])

    def test_conversation_history_rows_preserve_ai_and_recording_access(self):
        text = "\n".join(
            [
                "Conversation history",
                "User & Contact Center",
                "Channel",
                "Participant",
                "Date & Time",
                "Duration",
                "West U (Front Desk)",
                "West U",
                "Christina Alten",
                "Apr 27, 2026",
                "8:22:05 PM",
                "1m 4s",
                "56s",
                "▶",
                "✦",
            ]
        )
        rows = parse_conversation_history_rows(
            "https://dialpad.com/conversationhistory",
            text,
            limit=10,
            now=datetime(2026, 4, 28),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source_view"], "conversation_history")
        self.assertEqual(rows[0]["event_at"], "2026-04-27T20:22:05")
        self.assertEqual(rows[0]["contact_name"], "Christina Alten")
        self.assertEqual(rows[0]["department"], "WESTU")
        self.assertIn("Christina Alten", rows[0]["raw_text"])
        summary = summarize_view("conversation_history", "https://dialpad.com/conversationhistory", rows, [])
        self.assertEqual(summary["ai_action_rows"], 1)
        self.assertEqual(summary["recording_action_rows"], 1)

    def test_conversation_history_dom_rows_preserve_call_review_access(self):
        row = conversation_history_row_from_dom(
            "https://dialpad.com/conversationhistory",
            {
                "cells": [
                    "West U (Front Desk)\nWest U",
                    "",
                    "Christina Alten",
                    "Apr 27, 2026\n8:22:05 PM",
                    "1m 4s",
                    "56s",
                    "-",
                    "-",
                    "",
                    "",
                ],
                "button_labels": ["Outbound (Connected)"],
                "links": [
                    {
                        "href": "https://dialpad.com/callhistory/callreview/5713343127035904?source=session-history%3A",
                        "text": "",
                        "label": "View call summary",
                    }
                ],
                "action_button_count": 2,
                "text": "West U (Front Desk) West U Outbound (Connected) Christina Alten Apr 27, 2026 8:22:05 PM",
            },
            index=0,
            now=datetime(2026, 4, 28),
        )
        self.assertIsNotNone(row)
        self.assertEqual(row["event_id"], "5713343127035904")
        self.assertEqual(row["call_id"], "5713343127035904")
        self.assertEqual(row["event_at"], "2026-04-27T20:22:05")
        self.assertEqual(row["direction"], "outbound")
        self.assertEqual(row["source_url"], "https://dialpad.com/callhistory/callreview/5713343127035904?source=session-history%3A")
        raw = json.loads(row["raw_json"])
        self.assertEqual(raw["transcript_status"], "call_review_visible")
        self.assertTrue(raw["ai_action_visible"])
        self.assertTrue(raw["recording_action_visible"])


if __name__ == "__main__":
    unittest.main()
