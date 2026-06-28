#!/usr/bin/env python3
import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import ensure_lead_followup_schema, normalize_phone, utc_now_iso  # noqa: E402
from notesreminder.reports.dashboard_sql import register_dashboard_sql_functions  # noqa: E402
from scripts.extract_dialpad_voice import school_from_entry_point  # noqa: E402


SCHOOL_ALIASES = {
    "the heights": ("The Heights", "HEIGHTS"),
    "heights": ("The Heights", "HEIGHTS"),
    "west u": ("West U", "WESTU"),
    "west university": ("West U", "WESTU"),
    "west university place": ("West U", "WESTU"),
    "westu": ("West U", "WESTU"),
}
CALL_REVIEW_SCHOOL_PATTERNS = {
    "The Heights": re.compile(
        r"\b(the\s+heights|heights\s+school|school\s+of\s+rock\s+(?:the\s+)?heights)\b",
        re.IGNORECASE,
    ),
    "West U": re.compile(
        r"\b(west\s*u|west\s+university|west\s+university\s+place|school\s+of\s+rock\s+(?:in\s+)?west(?:\s*u)?)\b",
        re.IGNORECASE,
    ),
}


def normalize_school(value):
    lowered = str(value or "").strip().lower()
    if not lowered:
        return None, None
    for key, school in SCHOOL_ALIASES.items():
        if key in lowered:
            return school
    return None, None


def candidate_schools_for_phone(conn, phone):
    schools = {}
    for table, column in (
        ("hubspot_contacts", "school"),
        ("pike13_people", "school"),
    ):
        if not _table_exists(conn, table):
            continue
        for value, in conn.execute(
            f"""
            SELECT DISTINCT {column}
            FROM {table}
            WHERE phone_normalized = ?
              AND COALESCE({column}, '') != ''
            """,
            (phone,),
        ).fetchall():
            school, department = normalize_school(value)
            if school:
                schools[school] = department
    return schools


def candidate_call_log_schools_for_phone(conn, phone):
    if not _table_exists(conn, "call_logs"):
        return {}
    schools = {}
    for row in conn.execute(
        """
        SELECT DISTINCT external_number, school_name, school_code
        FROM call_logs
        WHERE COALESCE(external_number, '') != ''
          AND COALESCE(school_name, school_code, '') != ''
        """
    ).fetchall():
        if normalize_phone(row["external_number"]) != phone:
            continue
        school, department = normalize_school(row["school_name"] or row["school_code"])
        if school:
            schools[school] = department
    return schools


def candidate_sms_thread_schools_for_phone(conn, phone):
    if not _table_exists(conn, "dialpad_sms_threads"):
        return {}
    schools = {}
    for value, in conn.execute(
        """
        SELECT DISTINCT school
        FROM dialpad_sms_threads
        WHERE phone_normalized = ?
          AND COALESCE(school, '') != ''
        """,
        (phone,),
    ).fetchall():
        school, department = normalize_school(value)
        if school:
            schools[school] = department
    return schools


def candidate_dialpad_communication_schools_for_phone(conn, phone):
    if not _table_exists(conn, "dialpad_voice_events"):
        return {}
    schools = {}
    for value, in conn.execute(
        """
        SELECT DISTINCT school
        FROM dialpad_voice_events
        WHERE phone_normalized = ?
          AND COALESCE(school, '') != ''
        """,
        (phone,),
    ).fetchall():
        school, department = normalize_school(value)
        if school:
            schools[school] = department
    return schools


def _table_exists(conn, table):
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (table,),
        ).fetchone()
        is not None
    )


def infer_voice_school_rows(conn, start_date, end_date):
    ensure_lead_followup_schema(conn)
    register_dashboard_sql_functions(conn)
    conn.row_factory = sqlite3.Row
    inferred = []
    ambiguous = []
    unmatched = []
    for row in conn.execute(
        """
        SELECT event_id, phone_normalized, raw_json
        FROM dialpad_voice_events
        WHERE dashboard_date(event_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
          AND COALESCE(school, '') = ''
          AND COALESCE(phone_normalized, '') != ''
        """,
        {"start": start_date, "end": end_date},
    ).fetchall():
        schools = candidate_schools_for_phone(conn, row["phone_normalized"])
        mapping_source = "identity_phone_unique"
        if not schools:
            schools = candidate_sms_thread_schools_for_phone(conn, row["phone_normalized"])
            mapping_source = "dialpad_sms_phone_unique"
        if not schools:
            schools = candidate_dialpad_communication_schools_for_phone(conn, row["phone_normalized"])
            mapping_source = "dialpad_communication_phone_unique"
        if not schools:
            schools = candidate_call_log_schools_for_phone(conn, row["phone_normalized"])
            mapping_source = "legacy_call_log_phone_unique"
        if len(schools) == 1:
            school, department = next(iter(schools.items()))
            inferred.append(
                {
                    "event_id": row["event_id"],
                    "phone_normalized": row["phone_normalized"],
                    "school": school,
                    "department": department,
                    "raw_json": row["raw_json"],
                    "mapping_source": mapping_source,
                }
            )
        elif schools:
            ambiguous.append({"event_id": row["event_id"], "schools": sorted(schools)})
        else:
            unmatched.append({"event_id": row["event_id"]})
    return inferred, ambiguous, unmatched


def infer_entry_point_school_rows(conn, start_date, end_date):
    ensure_lead_followup_schema(conn)
    register_dashboard_sql_functions(conn)
    conn.row_factory = sqlite3.Row
    inferred = []
    unmatched = []
    for row in conn.execute(
        """
        SELECT event_id, raw_json
        FROM dialpad_voice_events
        WHERE dashboard_date(event_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
          AND COALESCE(school, '') = ''
          AND COALESCE(raw_json, '') != ''
        """,
        {"start": start_date, "end": end_date},
    ).fetchall():
        try:
            raw = json.loads(row["raw_json"] or "{}")
            if not isinstance(raw, dict):
                raw = {}
        except json.JSONDecodeError:
            raw = {}
        entry_point = raw.get("display_entry_point") or raw.get("displayEntryPoint")
        school, department = school_from_entry_point(entry_point)
        if school:
            inferred.append(
                {
                    "event_id": row["event_id"],
                    "school": school,
                    "department": department,
                    "raw_json": row["raw_json"],
                    "mapping_source": "entry_point",
                }
            )
        elif entry_point:
            unmatched.append({"event_id": row["event_id"], "entry_point": entry_point})
    return inferred, unmatched


def infer_requested_scope_school_rows(conn, start_date, end_date):
    ensure_lead_followup_schema(conn)
    register_dashboard_sql_functions(conn)
    conn.row_factory = sqlite3.Row
    inferred = []
    unmatched = []
    for row in conn.execute(
        """
        SELECT event_id, raw_json
        FROM dialpad_voice_events
        WHERE dashboard_date(event_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
          AND COALESCE(school, '') = ''
          AND COALESCE(raw_json, '') != ''
        """,
        {"start": start_date, "end": end_date},
    ).fetchall():
        try:
            raw = json.loads(row["raw_json"] or "{}")
            if not isinstance(raw, dict):
                raw = {}
        except json.JSONDecodeError:
            raw = {}
        requested_school = raw.get("requested_school")
        school_filter_applied = raw.get("school_filter_applied")
        if school_filter_applied is None and isinstance(raw.get("filter_diagnostics"), dict):
            school_filter_applied = raw["filter_diagnostics"].get("school_filter_applied")
        scope_mismatch = raw.get("scope_school_mismatch")
        if scope_mismatch is None and isinstance(raw.get("filter_diagnostics"), dict):
            scope_mismatch = raw["filter_diagnostics"].get("scope_school_mismatch")
        school, department = normalize_school(requested_school)
        if school and school_filter_applied is True and not scope_mismatch:
            inferred.append(
                {
                    "event_id": row["event_id"],
                    "school": school,
                    "department": department,
                    "raw_json": row["raw_json"],
                    "mapping_source": "requested_school_filter_scope",
                }
            )
        elif requested_school or school_filter_applied:
            unmatched.append({"event_id": row["event_id"]})
    return inferred, unmatched


def school_from_call_review_text(*values):
    text = " ".join(str(value or "") for value in values)
    matches = [school for school, pattern in CALL_REVIEW_SCHOOL_PATTERNS.items() if pattern.search(text)]
    if len(matches) != 1:
        return None, None
    return normalize_school(matches[0])


def infer_call_review_school_rows(conn, start_date, end_date):
    ensure_lead_followup_schema(conn)
    register_dashboard_sql_functions(conn)
    conn.row_factory = sqlite3.Row
    if not _table_exists(conn, "dialpad_call_reviews"):
        return [], []
    inferred = []
    ambiguous = []
    for row in conn.execute(
        """
        SELECT
            v.event_id,
            v.raw_json AS voice_raw_json,
            cr.transcript_text,
            cr.recap_text,
            cr.raw_json AS review_raw_json
        FROM dialpad_voice_events v
        JOIN dialpad_call_reviews cr
          ON cr.voice_event_id = v.event_id
          OR cr.call_id = v.call_id
          OR cr.call_review_id = v.call_id
        WHERE dashboard_date(v.event_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
          AND COALESCE(v.school, '') = ''
        """,
        {"start": start_date, "end": end_date},
    ).fetchall():
        try:
            review_raw = json.loads(row["review_raw_json"] or "{}")
            if not isinstance(review_raw, dict):
                review_raw = {}
        except json.JSONDecodeError:
            review_raw = {}
        visible_labels = {
            normalize_school(label)[0]
            for label in (review_raw.get("visible_school_labels") or [])
            if normalize_school(label)[0]
        }
        if len(visible_labels) == 1:
            school = next(iter(visible_labels))
            _, department = normalize_school(school)
            inferred.append(
                {
                    "event_id": row["event_id"],
                    "school": school,
                    "department": department,
                    "raw_json": row["voice_raw_json"],
                    "mapping_source": "call_review_visible_school_label",
                }
            )
            continue
        if len(visible_labels) > 1:
            ambiguous.append({"event_id": row["event_id"]})
            continue
        values = (row["transcript_text"], row["recap_text"], row["review_raw_json"])
        school, department = school_from_call_review_text(*values)
        if school:
            inferred.append(
                {
                    "event_id": row["event_id"],
                    "school": school,
                    "department": department,
                    "raw_json": row["voice_raw_json"],
                    "mapping_source": "call_review_school_marker",
                }
            )
            continue
        text = " ".join(str(value or "") for value in values)
        if sum(1 for pattern in CALL_REVIEW_SCHOOL_PATTERNS.values() if pattern.search(text)) > 1:
            ambiguous.append({"event_id": row["event_id"]})
    return inferred, ambiguous


def apply_inferred_rows(conn, rows):
    updated = 0
    now = utc_now_iso()
    for row in rows:
        try:
            raw = json.loads(row.get("raw_json") or "{}")
            if not isinstance(raw, dict):
                raw = {}
        except json.JSONDecodeError:
            raw = {}
        mapping_source = row.get("mapping_source") or "identity_phone_unique"
        raw["school_mapping"] = mapping_source
        source_labels = {
            "entry_point": "known_dialpad_entry_point",
            "identity_phone_unique": "hubspot_or_pike13_phone_unique",
            "dialpad_sms_phone_unique": "dialpad_sms_phone_unique",
            "dialpad_communication_phone_unique": "dialpad_communication_phone_unique",
            "legacy_call_log_phone_unique": "legacy_call_log_phone_unique",
            "call_review_school_marker": "call_review_school_marker",
            "call_review_visible_school_label": "call_review_visible_school_label",
            "requested_school_filter_scope": "requested_school_filter_scope",
            "targeted_conversation_history_scope": "targeted_conversation_history_scope",
        }
        raw["school_mapping_source"] = source_labels.get(mapping_source, mapping_source)
        raw["inferred_school"] = row["school"]
        raw["inferred_department"] = row["department"]
        cursor = conn.execute(
            """
            UPDATE dialpad_voice_events
            SET school = ?,
                department = ?,
                raw_json = ?,
                updated_at = ?
            WHERE event_id = ?
              AND COALESCE(school, '') = ''
            """,
            (
                row["school"],
                row["department"],
                json.dumps(raw, sort_keys=True),
                now,
                row["event_id"],
            ),
        )
        updated += cursor.rowcount if cursor.rowcount is not None else 0
    return updated


def run(args):
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        entry_point_inferred, entry_point_unmatched = infer_entry_point_school_rows(
            conn, args.start_date, args.end_date
        )
        requested_scope_inferred, requested_scope_unmatched = infer_requested_scope_school_rows(
            conn, args.start_date, args.end_date
        )
        phone_inferred, ambiguous, unmatched = infer_voice_school_rows(conn, args.start_date, args.end_date)
        review_inferred, review_ambiguous = infer_call_review_school_rows(conn, args.start_date, args.end_date)
        call_log_phone_inferred = sum(
            1 for row in phone_inferred if row.get("mapping_source") == "legacy_call_log_phone_unique"
        )
        sms_phone_inferred = sum(
            1 for row in phone_inferred if row.get("mapping_source") == "dialpad_sms_phone_unique"
        )
        inferred_by_id = {row["event_id"]: row for row in phone_inferred}
        inferred_by_id.update({row["event_id"]: row for row in review_inferred})
        inferred_by_id.update({row["event_id"]: row for row in entry_point_inferred})
        inferred_by_id.update({row["event_id"]: row for row in requested_scope_inferred})
        inferred = list(inferred_by_id.values())
        updated = 0
        if args.apply:
            before = conn.total_changes
            apply_inferred_rows(conn, inferred)
            updated = conn.total_changes - before
            conn.commit()
        return {
            "inferred": len(inferred),
            "ambiguous": len(ambiguous) + len(review_ambiguous),
            "unmatched": len(unmatched),
            "entry_point_inferred": len(entry_point_inferred),
            "entry_point_unmatched": len(entry_point_unmatched),
            "requested_scope_inferred": len(requested_scope_inferred),
            "requested_scope_unmatched": len(requested_scope_unmatched),
            "phone_inferred": len(phone_inferred),
            "sms_phone_inferred": sms_phone_inferred,
            "call_log_phone_inferred": call_log_phone_inferred,
            "call_review_inferred": len(review_inferred),
            "call_review_ambiguous": len(review_ambiguous),
            "updated": updated,
            "applied": bool(args.apply),
        }
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Infer school for unmapped Dialpad voice rows from unique HubSpot/Pike13 phone identity.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
