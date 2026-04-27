import unittest
from datetime import datetime

from scripts.extract_dialpad_sms import extract_message_lines, normalize_dialpad_date as normalize_sms_date
from scripts.extract_dialpad_voice import rows_from_visible_text


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
        self.assertEqual(messages[1]["message_at"], "2026-03-13")

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


if __name__ == "__main__":
    unittest.main()
