import json
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema
from scripts.extract_dialpad_sms import (
    click_messages_tab_with_dom_fallback,
    contact_phone_from_detail_text,
    detect_department as detect_sms_department,
    existing_message_list_contact_phone,
    extract_message_lines,
    is_dialpad_app_page as is_sms_app_page,
    is_login_page as is_sms_login_page,
    message_list_state_status,
    message_list_row_to_records,
    normalize_department,
    normalize_dialpad_date as normalize_sms_date,
    department_school,
    sms_extraction_source,
)
from scripts.extract_dialpad_sms_api import contact_search_url, feed_row_to_sms_rows, millis_to_iso, sms_direction
from scripts.extract_dialpad_voice import (
    conversation_history_api_item_to_row,
    conversation_history_row_from_dom,
    infer_direction as infer_voice_direction,
    is_dialpad_app_page as is_voice_app_page,
    is_dialpad_preboot_shell,
    is_login_page as is_voice_login_page,
    parse_conversation_history_rows,
    rows_from_visible_text,
    summarize_view,
    upsert_voice_event,
)


class DialpadExtractorTests(unittest.TestCase):
    def test_dialpad_sms_api_row_conversion_redacts_body(self):
        thread, message = feed_row_to_sms_rows(
            {
                "feed_type": "TextMessage",
                "message_id": "msg-1",
                "date": 1782318856167,
                "delivery_result": "Accepted",
                "orientation": "internal",
                "text": "Sensitive message body",
                "from_phone": "(713) 555-1212",
            },
            {"contact_key": "contact-1", "primary_phone": "(713) 555-1212", "unread": 0},
            "office-1",
            "West U",
            "WESTU",
        )

        self.assertEqual(message["message_at"], "2026-06-24T16:34:16.167000+00:00")
        self.assertEqual(message["direction"], "outbound")
        self.assertEqual(message["body"], "[redacted Dialpad SMS API message]")
        self.assertEqual(thread["school"], "West U")
        self.assertEqual(thread["phone_normalized"], "7135551212")
        self.assertNotIn("Sensitive", message["raw_json"])

    def test_dialpad_sms_api_helpers(self):
        self.assertEqual(millis_to_iso(1782318856167), "2026-06-24T16:34:16.167000+00:00")
        self.assertEqual(sms_direction({"orientation": "external"}, "office-1"), "inbound")
        self.assertEqual(sms_direction({"delivery_result": "Accepted"}, "office-1"), "outbound")
        self.assertEqual(
            contact_search_url("target key", "(713) 870-9993", 10),
            "https://dialpad.com/api/contact/?filter=all&target_key=target+key&limit=10&search=%28713%29+870-9993",
        )

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

    def test_sms_parser_handles_feed_detail_dates_before_messages(self):
        messages = extract_message_lines(
            "\n".join(
                [
                    "5/23/2024",
                    "(713) 555-1212",
                    "5:29 pm",
                    "Hi, this is a message from the department.",
                    "6/26/2024",
                    "(713) 555-1212",
                    "3:45 pm",
                    "Reply STOP to opt-out.",
                ]
            ),
            date_follows_message=False,
        )
        self.assertEqual(messages[0]["message_at"], "2024-05-23")
        self.assertEqual(messages[0]["body"], "Hi, this is a message from the department.")
        self.assertEqual(messages[1]["message_at"], "2024-06-26")

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
        self.assertEqual(detect_sms_department("Departments\nHEIGHTS\nMessages"), ("The Heights", "HEIGHTS"))
        self.assertEqual(normalize_department("The Heights"), "HEIGHTS")
        self.assertEqual(normalize_department("westu-sor"), "WESTU")
        self.assertEqual(department_school("HEIGHTS"), ("The Heights", "HEIGHTS"))

    def test_extractors_detect_dialpad_login_pages(self):
        login_text = "Log in to Dialpad\nWORK EMAIL\nPASSWORD"
        self.assertTrue(is_sms_login_page("https://dialpad.com/login", login_text))
        self.assertTrue(is_voice_login_page("https://dialpad.com/login", login_text))
        app_text = "Search Dialpad\nDepartments\nMessages\nCalls"
        self.assertTrue(is_sms_app_page("https://dialpad.com/app/history/messages", app_text))
        self.assertTrue(is_voice_app_page("https://dialpad.com/app/history/calls", app_text))
        call_review_text = "CALL HISTORY / CALL REVIEW\nRecap\nTranscript\nExcerpts\nTranscript search by keyword"
        self.assertTrue(is_voice_app_page("https://dialpad.com/callhistory/callreview/5646748416811008", call_review_text))
        preboot_text = "We're having trouble connecting.\nClear your browser's cache.\nRun a system diagnostic test."
        self.assertTrue(is_dialpad_preboot_shell(preboot_text))
        self.assertFalse(is_voice_app_page("https://dialpad.com/callhistory/callreview/5646748416811008", preboot_text))
        self.assertFalse(is_sms_app_page("https://dialpad.okta.com", app_text))

    def test_sms_date_normalizer_handles_relative_and_short_dates(self):
        now = datetime(2026, 4, 27)
        self.assertEqual(normalize_sms_date("Today", now=now), "2026-04-27")
        self.assertEqual(normalize_sms_date("Yesterday", now=now), "2026-04-26")
        self.assertEqual(normalize_sms_date("Monday", now=now), "2026-04-27")
        self.assertEqual(normalize_sms_date("Friday", now=now), "2026-04-24")
        self.assertEqual(normalize_sms_date("Mon Feb 2", now=now), "2026-02-02")
        self.assertEqual(normalize_sms_date("Thu Dec 18", now=now), "2025-12-18")

    def test_sms_message_list_row_records_are_sanitized(self):
        thread, message = message_list_row_to_records(
            {
                "contact": "(713) 555-1212",
                "snippet": '"Hi, this is Amanda from School of Rock - The Heights."',
                "operator": "Amanda De Leon",
                "date_text": "Wed Jun 17",
                "row_text": "(713) 555-1212 Hi, this is Amanda from School of Rock - The Heights.",
            },
            "The Heights",
            "HEIGHTS",
            "https://dialpad.com/app/history/messages",
            now=datetime(2026, 6, 24),
        )

        self.assertEqual(thread["phone_normalized"], "7135551212")
        self.assertEqual(thread["school"], "The Heights")
        self.assertEqual(message["message_at"], "2026-06-17")
        self.assertEqual(message["direction"], "outbound")
        self.assertEqual(message["body"], "[redacted Dialpad SMS web list snippet]")
        self.assertNotIn("Amanda", message["raw_json"])
        self.assertEqual(json.loads(message["raw_json"])["direction_source"], "message_list_operator_present")

    def test_sms_message_list_row_uses_supplied_phone(self):
        thread, message = message_list_row_to_records(
            {
                "contact": "Sensitive Customer",
                "phone": "(832) 555-1212",
                "snippet": '"Please call me back."',
                "date_text": "Today",
            },
            "The Heights",
            "HEIGHTS",
            "https://dialpad.com/app/history/messages",
            now=datetime(2026, 6, 24),
        )

        self.assertEqual(thread["contact_name"], "Sensitive Customer")
        self.assertEqual(thread["phone_normalized"], "8325551212")
        self.assertEqual(message["message_at"], "2026-06-24")
        self.assertEqual(message["direction"], "inbound")
        raw = json.loads(message["raw_json"])
        self.assertTrue(raw["phone_supplied"])
        self.assertTrue(raw["raw_body_redacted"])
        self.assertEqual(raw["direction_source"], "message_list_no_operator")

    def test_sms_detail_phone_extraction_ignores_school_numbers(self):
        phone = contact_phone_from_detail_text(
            "\n".join(
                [
                    "The Heights",
                    "Voice",
                    "(281) 909-7625",
                    "Profile",
                    "Sensitive Customer",
                    "Other:",
                    "(432) 413-9024",
                    "Media",
                ]
            )
        )

        self.assertEqual(phone["phone"], "(432) 413-9024")
        self.assertEqual(phone["phone_normalized"], "4324139024")

    def test_sms_message_list_row_records_detail_enrichment_safely(self):
        thread, message = message_list_row_to_records(
            {
                "contact": "Sensitive Customer",
                "phone": "(832) 555-1212",
                "phone_source": "dialpad_thread_detail",
                "detail_url": "https://dialpad.com/app/feed/customer-key/office-key",
                "snippet": '"Please call me back."',
                "date_text": "Today",
            },
            "The Heights",
            "HEIGHTS",
            "https://dialpad.com/app/history/messages",
            now=datetime(2026, 6, 24),
        )

        self.assertEqual(thread["phone_normalized"], "8325551212")
        raw = json.loads(message["raw_json"])
        self.assertEqual(raw["phone_source"], "dialpad_thread_detail")
        self.assertTrue(raw["detail_url_hash"].startswith("dialpad_sms_detail_url_"))
        self.assertNotIn("customer-key", message["raw_json"])
        self.assertNotIn("Please call me", message["raw_json"])

    def test_sms_message_list_contact_phone_reuses_existing_identity(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as handle:
            conn = sqlite3.connect(handle.name)
            conn.row_factory = sqlite3.Row
            ensure_lead_followup_schema(conn)
            thread, _ = message_list_row_to_records(
                {
                    "contact": "Sensitive Customer",
                    "phone": "(832) 555-1212",
                    "snippet": '"Please call me back."',
                    "date_text": "Today",
                },
                "The Heights",
                "HEIGHTS",
                "https://dialpad.com/app/history/messages",
                now=datetime(2026, 6, 24),
            )
            conn.execute(
                """
                INSERT INTO dialpad_sms_threads (
                    thread_id, feed_id, phone, phone_normalized, contact_name,
                    school, department, updated_at
                )
                VALUES (:thread_id, :feed_id, :phone, :phone_normalized, :contact_name,
                        :school, :department, :updated_at)
                """,
                thread,
            )

            phone = existing_message_list_contact_phone(conn, "HEIGHTS", "Sensitive Customer")

            self.assertEqual(phone["phone_normalized"], "8325551212")
            self.assertEqual(phone["phone_source"], "existing_thread_identity")

    def test_sms_message_list_state_blocks_live_calls_shell(self):
        state = message_list_state_status(
            {
                "selected_tab": "Live Calls",
                "message_row_count": 0,
                "contact_row_count": 0,
            }
        )

        self.assertFalse(state["ready"])
        self.assertIn("selected_tab_not_messages:live calls", state["blockers"])
        self.assertIn("no_message_list_rows", state["blockers"])

    def test_sms_message_list_state_requires_rows_even_on_messages_tab(self):
        state = message_list_state_status(
            {
                "selected_tab": "Messages",
                "message_row_count": 0,
                "contact_row_count": 12,
            }
        )

        self.assertFalse(state["ready"])
        self.assertEqual(state["blockers"], ["no_message_list_rows"])

    def test_sms_messages_tab_dom_fallback_uses_stable_selector(self):
        class FakePage:
            def __init__(self):
                self.calls = []

            def evaluate(self, script, selector):
                self.calls.append((script, selector))
                return selector == "#dt-tab-group-inbox-tab-messages"

        page = FakePage()

        clicked = click_messages_tab_with_dom_fallback(page, "#dt-tab-group-inbox-tab-messages")

        self.assertTrue(clicked)
        self.assertEqual(page.calls[0][1], "#dt-tab-group-inbox-tab-messages")
        self.assertIn("scrollIntoView", page.calls[0][0])

    def test_sms_messages_tab_dom_fallback_can_use_visible_tab_text(self):
        class FakePage:
            def __init__(self):
                self.calls = []

            def evaluate(self, script, selector):
                self.calls.append((script, selector))
                return selector == "__text_locator__" and "text === 'Messages'" in script

        page = FakePage()

        clicked = click_messages_tab_with_dom_fallback(page, "__text_locator__")

        self.assertTrue(clicked)
        self.assertEqual(page.calls[0][1], "__text_locator__")

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

    def test_conversation_history_api_item_maps_entry_point_school(self):
        row = conversation_history_api_item_to_row(
            {
                "createdAt": "2026-06-24 15:51:05",
                "itemId": 4553158101770240,
                "itemType": "call",
                "participant": {"name": "Kelly Timms"},
                "rowData": {
                    "direction": "inbound",
                    "detailedState": "missed_voicemail",
                    "displayEntryPoint": "(281) 909-7625",
                    "displayExternalEndpoint": "(832) 882-8761",
                    "duration": 36,
                    "durationConnected": 0,
                    "hasAnyRecordings": False,
                },
            }
        )

        self.assertIsNotNone(row)
        self.assertEqual(row["event_id"], "4553158101770240")
        self.assertEqual(row["call_id"], "4553158101770240")
        self.assertEqual(row["event_at"], "2026-06-24T15:51:05")
        self.assertEqual(row["school"], "The Heights")
        self.assertEqual(row["department"], "HEIGHTS")
        self.assertEqual(row["direction"], "inbound")
        self.assertEqual(row["event_type"], "voicemail")
        self.assertEqual(row["phone_normalized"], "8328828761")
        self.assertEqual(
            row["source_url"],
            "https://dialpad.com/callhistory/callreview/4553158101770240?source=conversation-history-api",
        )
        raw = json.loads(row["raw_json"])
        self.assertEqual(raw["extraction"], "conversation_history_api")
        self.assertEqual(raw["school_mapping"], "entry_point")

    def test_conversation_history_api_item_leaves_unknown_school_unmapped(self):
        row = conversation_history_api_item_to_row(
            {
                "createdAt": "2026-06-24 15:37:21",
                "itemId": 5713343127035904,
                "itemType": "call",
                "participant": {"name": "(832) 907-0231"},
                "rowData": {
                    "direction": "outbound",
                    "detailedState": "outbound",
                    "displayEntryPoint": "(999) 999-9999",
                    "displayExternalEndpoint": "(832) 907-0231",
                },
            }
        )

        self.assertIsNotNone(row)
        self.assertIsNone(row["school"])
        self.assertIsNone(row["department"])
        self.assertIsNone(row["contact_name"])
        self.assertEqual(row["phone_normalized"], "8329070231")
        raw = json.loads(row["raw_json"])
        self.assertEqual(raw["school_mapping"], "unmapped_entry_point")

    def test_conversation_history_dom_rows_handle_leading_action_cell(self):
        row = conversation_history_row_from_dom(
            "https://dialpad.com/conversationhistory",
            {
                "cells": [
                    "",
                    "West U (Front Desk)\nWest U",
                    "",
                    "(713) 555-1212",
                    "Jun 24, 2026\n3:00:01 PM",
                    "49s",
                    "38s",
                    "-",
                    "-",
                    "",
                    "",
                ],
                "button_labels": ["Inbound (Answered)"],
                "links": [
                    {
                        "href": "https://dialpad.com/callhistory/callreview/5713343127035904?source=session-history%3A",
                        "text": "",
                        "label": "View call summary",
                    }
                ],
                "action_button_count": 2,
                "text": "West U (Front Desk) West U Inbound (Answered) (713) 555-1212 Jun 24, 2026 3:00:01 PM",
            },
            index=0,
            now=datetime(2026, 6, 24),
        )

        self.assertIsNotNone(row)
        self.assertEqual(row["school"], "West U")
        self.assertEqual(row["direction"], "inbound")
        self.assertEqual(row["phone_normalized"], "7135551212")
        self.assertEqual(row["event_at"], "2026-06-24T15:00:01")

    def test_conversation_history_dom_rows_extract_school_from_staff_cell(self):
        row = conversation_history_row_from_dom(
            "https://dialpad.com/conversationhistory",
            {
                "cells": [
                    "",
                    "Logan Gill The Heights",
                    "",
                    "(713) 555-1212",
                    "Jun 24, 2026\n3:00:01 PM",
                    "49s",
                    "38s",
                    "-",
                    "-",
                    "",
                    "",
                ],
                "button_labels": ["Outbound (Connected)"],
                "links": [],
                "action_button_count": 1,
                "text": "Logan Gill The Heights Outbound (Connected) (713) 555-1212 Jun 24, 2026 3:00:01 PM",
            },
            index=0,
            now=datetime(2026, 6, 24),
        )

        self.assertIsNotNone(row)
        self.assertEqual(row["school"], "The Heights")
        self.assertEqual(row["department"], "HEIGHTS")
        self.assertEqual(row["direction"], "outbound")

    def test_conversation_history_dom_rows_do_not_store_staff_as_school(self):
        row = conversation_history_row_from_dom(
            "https://dialpad.com/conversationhistory",
            {
                "cells": [
                    "",
                    "Hugh Scott",
                    "",
                    "(713) 555-1212",
                    "Jun 24, 2026\n3:00:01 PM",
                    "49s",
                    "38s",
                    "-",
                    "-",
                    "",
                    "",
                ],
                "button_labels": ["Outbound (Connected)"],
                "links": [],
                "action_button_count": 1,
                "text": "Hugh Scott Outbound (Connected) (713) 555-1212 Jun 24, 2026 3:00:01 PM",
            },
            index=0,
            now=datetime(2026, 6, 24),
        )

        self.assertIsNotNone(row)
        self.assertIsNone(row["school"])
        self.assertEqual(row["direction"], "outbound")

    def test_conversation_history_upsert_clears_prior_staff_school(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        conn = sqlite3.connect(Path(tmp.name) / "test.db")
        ensure_lead_followup_schema(conn)
        row = conversation_history_row_from_dom(
            "https://dialpad.com/conversationhistory",
            {
                "cells": [
                    "",
                    "Hugh Scott",
                    "",
                    "(713) 555-1212",
                    "Jun 24, 2026\n3:00:01 PM",
                    "49s",
                    "38s",
                    "-",
                    "-",
                    "",
                    "",
                ],
                "button_labels": ["Outbound (Connected)"],
                "links": [],
                "action_button_count": 1,
                "text": "Hugh Scott Outbound (Connected) (713) 555-1212 Jun 24, 2026 3:00:01 PM",
            },
            index=0,
            now=datetime(2026, 6, 24),
        )
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, contact_name, direction,
                event_at, school, department, outcome, raw_text, raw_json, updated_at
            )
            VALUES (?, 'conversation_history', 'call', 'Parent', 'outbound',
                    '2026-06-24T15:00:01', 'Hugh Scott', NULL, 'old', 'old', '{}', 'now')
            """,
            (row["event_id"],),
        )

        upsert_voice_event(conn, row)

        stored = conn.execute(
            "SELECT school FROM dialpad_voice_events WHERE event_id = ?",
            (row["event_id"],),
        ).fetchone()
        self.assertIsNone(stored[0])

    def test_voice_direction_infers_plain_missed_as_inbound(self):
        self.assertEqual(infer_voice_direction("Missed"), "inbound")


if __name__ == "__main__":
    unittest.main()
