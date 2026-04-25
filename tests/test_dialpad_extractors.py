import unittest

from scripts.extract_dialpad_sms import extract_message_lines
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
            )
        )
        self.assertEqual([row["direction"] for row in messages], ["inbound", "outbound"])
        self.assertEqual(messages[0]["body"], "Inbound: Can you call me?")

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


if __name__ == "__main__":
    unittest.main()
