import json
import sqlite3
import unittest

from lead_followup_schema import ensure_lead_followup_schema, utc_now_iso
from scripts.backfill_dialpad_voice_schools import (
    apply_inferred_rows,
    infer_call_review_school_rows,
    infer_entry_point_school_rows,
    infer_requested_scope_school_rows,
    infer_voice_school_rows,
)


def open_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_lead_followup_schema(conn)
    return conn


class DialpadVoiceSchoolBackfillTests(unittest.TestCase):
    def test_infers_known_entry_point_school(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-entry', 'conversation_history', 'call', '7135551212',
                    'inbound', '2026-06-20', '',
                    '{"display_entry_point": "(832) 762-3476", "school_mapping": "unmapped_entry_point"}',
                    ?)
            """,
            (now,),
        )

        inferred, unmatched = infer_entry_point_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(len(inferred), 1)
        self.assertEqual(inferred[0]["school"], "West U")
        self.assertEqual(inferred[0]["department"], "WESTU")
        self.assertEqual(inferred[0]["mapping_source"], "entry_point")
        self.assertEqual(unmatched, [])

    def test_unknown_entry_point_remains_unmatched(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-entry', 'conversation_history', 'call', '7135551212',
                    'inbound', '2026-06-20', '',
                    '{"display_entry_point": "(832) 669-3635", "school_mapping": "unmapped_entry_point"}',
                    ?)
            """,
            (now,),
        )

        inferred, unmatched = infer_entry_point_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(inferred, [])
        self.assertEqual(len(unmatched), 1)

    def test_infers_school_from_single_call_review_visible_label(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, call_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-review-label', 'call-review-label', 'conversation_history', 'call', '',
                    'inbound', '2026-06-20', '', '{}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_call_reviews (
                call_review_id, call_id, voice_event_id, call_review_url, event_at,
                transcript_available, recap_available, action_items_available, audio_available,
                extraction_status, raw_json, updated_at
            )
            VALUES ('call-review-label', 'call-review-label', 'voice-review-label',
                    'https://dialpad.com/callhistory/callreview/call-review-label', '2026-06-20',
                    0, 0, 0, 1, 'partial', '{"visible_school_labels": ["West U"]}', ?)
            """,
            (now,),
        )

        inferred, ambiguous = infer_call_review_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(len(inferred), 1)
        self.assertEqual(inferred[0]["school"], "West U")
        self.assertEqual(inferred[0]["department"], "WESTU")
        self.assertEqual(inferred[0]["mapping_source"], "call_review_visible_school_label")
        self.assertEqual(ambiguous, [])

    def test_conflicting_call_review_visible_labels_are_ambiguous(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, call_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-review-label', 'call-review-label', 'conversation_history', 'call', '',
                    'inbound', '2026-06-20', '', '{}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_call_reviews (
                call_review_id, call_id, voice_event_id, call_review_url, event_at,
                transcript_available, recap_available, action_items_available, audio_available,
                extraction_status, raw_json, updated_at
            )
            VALUES ('call-review-label', 'call-review-label', 'voice-review-label',
                    'https://dialpad.com/callhistory/callreview/call-review-label', '2026-06-20',
                    0, 0, 0, 1, 'partial',
                    '{"visible_school_labels": ["West U", "The Heights"]}', ?)
            """,
            (now,),
        )

        inferred, ambiguous = infer_call_review_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(inferred, [])
        self.assertEqual(len(ambiguous), 1)

    def test_infers_requested_school_when_filter_scope_was_applied(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-scope', 'conversation_history', 'call', '',
                    'inbound', '2026-06-20', '',
                    '{"requested_school": "West U", "school_filter_applied": true}',
                    ?)
            """,
            (now,),
        )

        inferred, unmatched = infer_requested_scope_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(len(inferred), 1)
        self.assertEqual(inferred[0]["school"], "West U")
        self.assertEqual(inferred[0]["department"], "WESTU")
        self.assertEqual(inferred[0]["mapping_source"], "requested_school_filter_scope")
        self.assertEqual(unmatched, [])

    def test_requested_school_scope_mismatch_is_not_inferred(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-scope', 'conversation_history', 'call', '',
                    'inbound', '2026-06-20', '',
                    '{"requested_school": "West U", "school_filter_applied": true, "scope_school_mismatch": true}',
                    ?)
            """,
            (now,),
        )

        inferred, unmatched = infer_requested_scope_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(inferred, [])
        self.assertEqual(len(unmatched), 1)

    def test_infers_unique_school_from_identity_phone(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-1', 'conversation_history', 'call', '7135551212',
                    'inbound', '2026-06-20', '', '{"school_mapping": "unmapped_entry_point"}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, phone_normalized, school, raw_json, updated_at
            )
            VALUES ('contact-1', '7135551212', 'West University Place', '{"trusted": 1}', ?)
            """,
            (now,),
        )

        inferred, ambiguous, unmatched = infer_voice_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(len(inferred), 1)
        self.assertEqual(inferred[0]["school"], "West U")
        self.assertEqual(inferred[0]["department"], "WESTU")
        self.assertEqual(ambiguous, [])
        self.assertEqual(unmatched, [])

    def test_conflicting_identity_schools_are_not_inferred(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-1', 'conversation_history', 'call', '7135551212',
                    'inbound', '2026-06-20', '', '{}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, phone_normalized, school, raw_json, updated_at
            )
            VALUES ('contact-1', '7135551212', 'West University Place', '{"trusted": 1}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, phone_normalized, school, updated_at
            )
            VALUES ('person-1', '7135551212', 'The Heights', ?)
            """,
            (now,),
        )

        inferred, ambiguous, unmatched = infer_voice_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(inferred, [])
        self.assertEqual(len(ambiguous), 1)
        self.assertEqual(unmatched, [])

    def test_infers_unique_school_from_legacy_call_log_phone(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            CREATE TABLE call_logs (
                call_id TEXT PRIMARY KEY,
                external_number TEXT,
                school_name TEXT,
                school_code TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-call-log', 'conversation_history', 'call', '7135551212',
                    'inbound', '2026-06-20', '', '{}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO call_logs (
                call_id, external_number, school_name, school_code
            )
            VALUES ('call-1', '+17135551212', 'The Heights', 'theheights-sor')
            """
        )

        inferred, ambiguous, unmatched = infer_voice_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(len(inferred), 1)
        self.assertEqual(inferred[0]["school"], "The Heights")
        self.assertEqual(inferred[0]["department"], "HEIGHTS")
        self.assertEqual(inferred[0]["mapping_source"], "legacy_call_log_phone_unique")
        self.assertEqual(ambiguous, [])
        self.assertEqual(unmatched, [])

    def test_infers_unique_school_from_dialpad_sms_thread_phone(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-sms-phone', 'conversation_history', 'call', '7135551212',
                    'inbound', '2026-06-20', '', '{}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_threads (
                thread_id, phone_normalized, school, updated_at
            )
            VALUES ('sms-thread-1', '7135551212', 'The Heights', ?)
            """,
            (now,),
        )

        inferred, ambiguous, unmatched = infer_voice_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(len(inferred), 1)
        self.assertEqual(inferred[0]["school"], "The Heights")
        self.assertEqual(inferred[0]["department"], "HEIGHTS")
        self.assertEqual(inferred[0]["mapping_source"], "dialpad_sms_phone_unique")
        self.assertEqual(ambiguous, [])
        self.assertEqual(unmatched, [])

    def test_infers_unique_school_from_other_dialpad_communication_phone(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES
                ('voice-unmapped', 'conversation_history', 'call', '7135551212',
                 'inbound', '2026-06-20', '', '{}', ?),
                ('voice-known', 'conversation_history', 'call', '7135551212',
                 'outbound', '2026-06-19', 'The Heights', '{}', ?)
            """
            ,
            (now, now),
        )

        inferred, ambiguous, unmatched = infer_voice_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(len(inferred), 1)
        self.assertEqual(inferred[0]["school"], "The Heights")
        self.assertEqual(inferred[0]["department"], "HEIGHTS")
        self.assertEqual(inferred[0]["mapping_source"], "dialpad_communication_phone_unique")
        self.assertEqual(ambiguous, [])
        self.assertEqual(unmatched, [])

    def test_conflicting_dialpad_sms_thread_schools_are_not_inferred(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-sms-phone', 'conversation_history', 'call', '7135551212',
                    'inbound', '2026-06-20', '', '{}', ?)
            """,
            (now,),
        )
        conn.executemany(
            """
            INSERT INTO dialpad_sms_threads (
                thread_id, phone_normalized, school, updated_at
            )
            VALUES (?, '7135551212', ?, ?)
            """,
            [
                ("sms-thread-1", "The Heights", now),
                ("sms-thread-2", "West U", now),
            ],
        )

        inferred, ambiguous, unmatched = infer_voice_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(inferred, [])
        self.assertEqual(len(ambiguous), 1)
        self.assertEqual(unmatched, [])

    def test_conflicting_legacy_call_log_schools_are_not_inferred(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            CREATE TABLE call_logs (
                call_id TEXT PRIMARY KEY,
                external_number TEXT,
                school_name TEXT,
                school_code TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-call-log', 'conversation_history', 'call', '7135551212',
                    'inbound', '2026-06-20', '', '{}', ?)
            """,
            (now,),
        )
        conn.executemany(
            """
            INSERT INTO call_logs (
                call_id, external_number, school_name, school_code
            )
            VALUES (?, '+17135551212', ?, ?)
            """,
            [
                ("call-1", "The Heights", "theheights-sor"),
                ("call-2", "West U", "westu-sor"),
            ],
        )

        inferred, ambiguous, unmatched = infer_voice_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(inferred, [])
        self.assertEqual(len(ambiguous), 1)
        self.assertEqual(unmatched, [])

    def test_infers_school_from_unambiguous_call_review_marker(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, call_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-review', 'call-review-1', 'conversation_history', 'call',
                    '7135551212', 'outbound', '2026-06-20', '', '{}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_call_reviews (
                call_review_id, call_id, voice_event_id, call_review_url,
                transcript_text, recap_text, extraction_status, raw_json, updated_at
            )
            VALUES ('call-review-1', 'call-review-1', 'voice-review', 'https://dialpad/review',
                    'Hi, this is Amanda from School of Rock The Heights.', '',
                    'success', '{}', ?)
            """,
            (now,),
        )

        inferred, ambiguous = infer_call_review_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(len(inferred), 1)
        self.assertEqual(inferred[0]["school"], "The Heights")
        self.assertEqual(inferred[0]["department"], "HEIGHTS")
        self.assertEqual(inferred[0]["mapping_source"], "call_review_school_marker")
        self.assertEqual(ambiguous, [])

    def test_infers_west_u_from_school_of_rock_west_call_review_marker(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, call_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-review', 'call-review-1', 'conversation_history', 'call',
                    '7135551212', 'outbound', '2026-06-20', '', '{}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_call_reviews (
                call_review_id, call_id, voice_event_id, call_review_url,
                transcript_text, recap_text, extraction_status, raw_json, updated_at
            )
            VALUES ('call-review-1', 'call-review-1', 'voice-review', 'https://dialpad/review',
                    'School of Rock West, this is the front desk.', '',
                    'success', '{}', ?)
            """,
            (now,),
        )

        inferred, ambiguous = infer_call_review_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(len(inferred), 1)
        self.assertEqual(inferred[0]["school"], "West U")
        self.assertEqual(inferred[0]["department"], "WESTU")
        self.assertEqual(ambiguous, [])

    def test_conflicting_call_review_markers_are_not_inferred(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, call_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-review', 'call-review-1', 'conversation_history', 'call',
                    '7135551212', 'outbound', '2026-06-20', '', '{}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dialpad_call_reviews (
                call_review_id, call_id, voice_event_id, call_review_url,
                transcript_text, recap_text, extraction_status, raw_json, updated_at
            )
            VALUES ('call-review-1', 'call-review-1', 'voice-review', 'https://dialpad/review',
                    'West U and The Heights are both mentioned here.', '',
                    'success', '{}', ?)
            """,
            (now,),
        )

        inferred, ambiguous = infer_call_review_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(inferred, [])
        self.assertEqual(ambiguous, [{"event_id": "voice-review"}])

    def test_apply_updates_raw_json_with_evidence(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-1', 'conversation_history', 'call', '7135551212',
                    'inbound', '2026-06-20', '', '{}', ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, phone_normalized, school, updated_at
            )
            VALUES ('person-1', '7135551212', 'The Heights', ?)
            """,
            (now,),
        )
        inferred, _, _ = infer_voice_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(apply_inferred_rows(conn, inferred), 1)
        row = conn.execute("SELECT school, department, raw_json FROM dialpad_voice_events").fetchone()
        raw = json.loads(row["raw_json"])
        self.assertEqual(row["school"], "The Heights")
        self.assertEqual(row["department"], "HEIGHTS")
        self.assertEqual(raw["school_mapping"], "identity_phone_unique")

    def test_apply_updates_raw_json_with_entry_point_evidence(self):
        conn = open_db()
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO dialpad_voice_events (
                event_id, source_view, event_type, phone_normalized, direction,
                event_at, school, raw_json, updated_at
            )
            VALUES ('voice-entry', 'conversation_history', 'call', '7135551212',
                    'inbound', '2026-06-20', '',
                    '{"display_entry_point": "(832) 560-2761", "school_mapping": "unmapped_entry_point"}',
                    ?)
            """,
            (now,),
        )
        inferred, _ = infer_entry_point_school_rows(conn, "2026-01-01", "2026-06-24")

        self.assertEqual(apply_inferred_rows(conn, inferred), 1)
        row = conn.execute("SELECT school, department, raw_json FROM dialpad_voice_events").fetchone()
        raw = json.loads(row["raw_json"])
        self.assertEqual(row["school"], "West U")
        self.assertEqual(row["department"], "WESTU")
        self.assertEqual(raw["school_mapping"], "entry_point")
        self.assertEqual(raw["school_mapping_source"], "known_dialpad_entry_point")


if __name__ == "__main__":
    unittest.main()
