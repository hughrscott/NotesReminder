from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date, datetime, timezone

from notesreminder.extractors.dialpad_targets import select_hubspot_contact_targets
from notesreminder.reports.dashboard_sql import register_dashboard_sql_functions
from notesreminder.reports.lead_operating_dashboard import (
    contact_date_expr,
    contact_person_expr,
    column_exists,
    hubspot_contact_school_clause,
    hubspot_lead_count,
    lead_followup_pareto_grid,
    table_exists,
)


DEFAULT_SCHOOLS = ("West U", "The Heights")
SOURCE_DATE_EXPRESSIONS = {
    "lessons": ("lessons", "lesson_date"),
    "reminders": ("reminders", "lesson_date"),
    "hubspot_contacts": ("hubspot_contacts", "COALESCE(NULLIF(create_date, ''), NULLIF(updated_at, ''))"),
    "pike13_visits": ("pike13_visits", "starts_at"),
    "school_email_messages": ("school_email_messages", "message_at"),
    "dialpad_voice_events": ("dialpad_voice_events", "event_at"),
    "dialpad_sms_messages": ("dialpad_sms_messages", "message_at"),
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def month_start(as_of: str) -> str:
    return date.fromisoformat(as_of).replace(day=1).isoformat()


def year_start(as_of: str) -> str:
    return date.fromisoformat(as_of).replace(month=1, day=1).isoformat()


def complete_months_ytd(as_of: str) -> list[str]:
    day = date.fromisoformat(as_of)
    current = day.replace(month=1, day=1)
    stop = day.replace(day=1)
    months = []
    while current < stop:
        months.append(current.strftime("%Y-%m"))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months


def scalar(conn: sqlite3.Connection, sql: str, params: dict | None = None):
    row = conn.execute(sql, params or {}).fetchone()
    return row[0] if row else None


def _count(conn: sqlite3.Connection, sql: str, params: dict | None = None) -> int:
    return int(scalar(conn, sql, params) or 0)


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 4) if denominator else None


def source_freshness_gate(conn: sqlite3.Connection, as_of: str) -> dict:
    sources = {}
    blockers = []
    for source, (table, expression) in SOURCE_DATE_EXPRESSIONS.items():
        if not table_exists(conn, table):
            sources[source] = {
                "status": "missing",
                "table": table,
                "latest_date": None,
                "expected_through": as_of,
                "rows": 0,
            }
            blockers.append(f"missing_{source}")
            continue
        latest = scalar(conn, f"SELECT MAX(dashboard_date({expression})) FROM {table}")
        rows = _count(conn, f"SELECT COUNT(*) FROM {table}")
        status = "ready" if latest and latest >= as_of else "stale" if latest else "missing"
        if status != "ready":
            blockers.append(f"{status}_{source}_latest_{latest or 'none'}")
        sources[source] = {
            "status": status,
            "table": table,
            "latest_date": latest,
            "expected_through": as_of,
            "rows": rows,
        }
    return {"status": "ready" if not blockers else "blocked", "sources": sources, "blockers": blockers}


def _hubspot_filtered_count(conn: sqlite3.Connection, school: str, start_date: str, end_date: str, where_sql: str) -> int:
    school_sql, school_params = hubspot_contact_school_clause(school, conn)
    lead_date_sql = contact_date_expr(conn)
    person_sql = contact_person_expr(conn)
    return _count(
        conn,
        f"""
        SELECT COUNT(*)
        FROM hubspot_contacts c
        LEFT JOIN pike13_people pp ON pp.person_id = {person_sql}
        WHERE {lead_date_sql} BETWEEN dashboard_date(:start) AND dashboard_date(:end)
          AND {school_sql}
          AND ({where_sql})
        """,
        {"start": start_date, "end": end_date, **school_params},
    )


def hubspot_spine_gate(
    conn: sqlite3.Connection,
    *,
    as_of: str,
    schools: tuple[str, ...] = DEFAULT_SCHOOLS,
    min_mtd_phone_rate: float = 0.5,
    min_mtd_identity_rate: float = 0.7,
) -> dict:
    if not table_exists(conn, "hubspot_contacts"):
        return {"status": "blocked", "blockers": ["missing_hubspot_contacts"], "schools": []}
    ytd_start = year_start(as_of)
    mtd_start = month_start(as_of)
    total_ytd = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM hubspot_contacts
        WHERE dashboard_date(NULLIF(create_date, ''), NULLIF(updated_at, ''))
          BETWEEN dashboard_date(:start) AND dashboard_date(:end)
        """,
        {"start": ytd_start, "end": as_of},
    )
    total_mtd = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM hubspot_contacts
        WHERE dashboard_date(NULLIF(create_date, ''), NULLIF(updated_at, ''))
          BETWEEN dashboard_date(:start) AND dashboard_date(:end)
        """,
        {"start": mtd_start, "end": as_of},
    )
    usable_ytd_ids = set()
    usable_mtd_ids = set()
    school_rows = []
    blockers = []
    data_quality_flags = []
    for school in schools:
        school_sql, school_params = hubspot_contact_school_clause(school, conn)
        lead_date_sql = contact_date_expr(conn)
        person_sql = contact_person_expr(conn)
        for row in conn.execute(
            f"""
            SELECT c.contact_id, {lead_date_sql} AS lead_date
            FROM hubspot_contacts c
            LEFT JOIN pike13_people pp ON pp.person_id = {person_sql}
            WHERE {lead_date_sql} BETWEEN dashboard_date(:start) AND dashboard_date(:end)
              AND {school_sql}
            """,
            {"start": ytd_start, "end": as_of, **school_params},
        ).fetchall():
            usable_ytd_ids.add(row["contact_id"])
            if row["lead_date"] and row["lead_date"] >= mtd_start:
                usable_mtd_ids.add(row["contact_id"])

        ytd_leads = hubspot_lead_count(conn, ytd_start, as_of, school)
        mtd_leads = hubspot_lead_count(conn, mtd_start, as_of, school)
        ytd_phone = _hubspot_filtered_count(conn, school, ytd_start, as_of, "COALESCE(c.phone_normalized, c.phone, '') != ''")
        mtd_phone = _hubspot_filtered_count(conn, school, mtd_start, as_of, "COALESCE(c.phone_normalized, c.phone, '') != ''")
        ytd_email = _hubspot_filtered_count(conn, school, ytd_start, as_of, "COALESCE(c.email_normalized, c.email, '') != ''")
        mtd_email = _hubspot_filtered_count(conn, school, mtd_start, as_of, "COALESCE(c.email_normalized, c.email, '') != ''")
        ytd_identity = _hubspot_filtered_count(
            conn,
            school,
            ytd_start,
            as_of,
            "COALESCE(c.phone_normalized, c.phone, c.email_normalized, c.email, '') != ''",
        )
        mtd_identity = _hubspot_filtered_count(
            conn,
            school,
            mtd_start,
            as_of,
            "COALESCE(c.phone_normalized, c.phone, c.email_normalized, c.email, '') != ''",
        )
        dialpad_targets_ytd = len(select_hubspot_contact_targets(conn, school, ytd_start, as_of, limit=100000))
        mtd_phone_rate = _rate(mtd_phone, mtd_leads)
        mtd_identity_rate = _rate(mtd_identity, mtd_leads)
        school_blockers = []
        if mtd_leads and (mtd_phone_rate or 0) < min_mtd_phone_rate:
            school_blockers.append(f"hubspot_mtd_phone_coverage_below_{int(min_mtd_phone_rate * 100)}pct")
        if mtd_leads and (mtd_identity_rate or 0) < min_mtd_identity_rate:
            school_blockers.append(f"hubspot_mtd_identity_coverage_below_{int(min_mtd_identity_rate * 100)}pct")
        school_rows.append(
            {
                "school": school,
                "ytd_leads": ytd_leads,
                "mtd_leads": mtd_leads,
                "ytd_phone_rows": ytd_phone,
                "mtd_phone_rows": mtd_phone,
                "ytd_phone_rate": _rate(ytd_phone, ytd_leads),
                "mtd_phone_rate": mtd_phone_rate,
                "ytd_email_rows": ytd_email,
                "mtd_email_rows": mtd_email,
                "ytd_email_rate": _rate(ytd_email, ytd_leads),
                "mtd_email_rate": _rate(mtd_email, mtd_leads),
                "ytd_identity_rows": ytd_identity,
                "mtd_identity_rows": mtd_identity,
                "ytd_identity_rate": _rate(ytd_identity, ytd_leads),
                "mtd_identity_rate": mtd_identity_rate,
                "dialpad_target_eligible_ytd": dialpad_targets_ytd,
                "blockers": school_blockers,
            }
        )
        blockers.extend(f"{school}:{blocker}" for blocker in school_blockers)
    unassigned_ytd = max(total_ytd - len(usable_ytd_ids), 0)
    unassigned_mtd = max(total_mtd - len(usable_mtd_ids), 0)
    if unassigned_mtd:
        data_quality_flags.append(f"hubspot_mtd_unassigned_school_{unassigned_mtd}")
    if unassigned_ytd:
        data_quality_flags.append(f"hubspot_ytd_unassigned_school_{unassigned_ytd}")
    return {
        "status": "ready" if not blockers else "blocked",
        "window": {"ytd_start": ytd_start, "mtd_start": mtd_start, "end": as_of},
        "total_ytd": total_ytd,
        "total_mtd": total_mtd,
        "usable_ytd": len(usable_ytd_ids),
        "usable_mtd": len(usable_mtd_ids),
        "unassigned_ytd": unassigned_ytd,
        "unassigned_mtd": unassigned_mtd,
        "schools": school_rows,
        "blockers": blockers,
        "data_quality_flags": data_quality_flags,
    }


def communication_coverage_gate(
    conn: sqlite3.Connection,
    *,
    as_of: str,
    schools: tuple[str, ...] = DEFAULT_SCHOOLS,
    min_matched_lead_rate: float = 0.5,
    min_known_call_direction_rate: float = 0.5,
    min_historical_monthly_call_rows: int = 10,
) -> dict:
    if not table_exists(conn, "vw_dialpad_communications"):
        return {"status": "blocked", "blockers": ["missing_vw_dialpad_communications"], "schools": []}
    blockers = []
    data_quality_flags = []
    rows = []
    mtd_start = month_start(as_of)
    unmapped_voice_ytd = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM dialpad_voice_events
        WHERE dashboard_date(event_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
          AND COALESCE(school, '') = ''
        """,
        {"start": year_start(as_of), "end": as_of},
    )
    unmapped_voice_mtd = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM dialpad_voice_events
        WHERE dashboard_date(event_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
          AND COALESCE(school, '') = ''
        """,
        {"start": mtd_start, "end": as_of},
    )
    if unmapped_voice_mtd:
        data_quality_flags.append(f"dialpad_voice_mtd_unmapped_school_{unmapped_voice_mtd}")
    if unmapped_voice_ytd:
        data_quality_flags.append(f"dialpad_voice_ytd_unmapped_school_{unmapped_voice_ytd}")
    sms_ytd_no_phone_rows = 0
    sms_ytd_no_phone_school_attributed_rows = 0
    if table_exists(conn, "dialpad_sms_messages") and table_exists(conn, "dialpad_sms_threads"):
        sms_ytd_no_phone_rows = _count(
            conn,
            """
            SELECT COUNT(*)
            FROM dialpad_sms_messages m
            LEFT JOIN dialpad_sms_threads t ON t.thread_id = m.thread_id
            WHERE dashboard_date(m.message_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
              AND COALESCE(t.phone_normalized, '') = ''
            """,
            {"start": year_start(as_of), "end": as_of},
        )
        sms_ytd_no_phone_school_attributed_rows = _count(
            conn,
            """
            SELECT COUNT(*)
            FROM dialpad_sms_messages m
            LEFT JOIN dialpad_sms_threads t ON t.thread_id = m.thread_id
            WHERE dashboard_date(m.message_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
              AND COALESCE(t.phone_normalized, '') = ''
              AND COALESCE(t.school, t.department, '') != ''
            """,
            {"start": year_start(as_of), "end": as_of},
        )
        if sms_ytd_no_phone_rows:
            data_quality_flags.append(f"dialpad_sms_ytd_no_phone_rows_{sms_ytd_no_phone_rows}")
    voice_ytd_unmapped_known_entry_point_rows = 0
    voice_ytd_unmapped_missing_entry_point_rows = 0
    voice_ytd_unmapped_no_safe_evidence_rows = 0
    if table_exists(conn, "dialpad_voice_events"):
        entry_point_expr = (
            "COALESCE(json_extract(raw_json, '$.display_entry_point'), '')"
            if column_exists(conn, "dialpad_voice_events", "raw_json")
            else "''"
        )
        not_excluded_sql = (
            "COALESCE(json_extract(raw_json, '$.excluded_from_communication_view'), 0) != 1"
            if column_exists(conn, "dialpad_voice_events", "raw_json")
            else "1=1"
        )
        voice_ytd_unmapped_known_entry_point_rows = _count(
            conn,
            f"""
            SELECT COUNT(*)
            FROM dialpad_voice_events
            WHERE dashboard_date(event_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
              AND COALESCE(school, '') = ''
              AND {entry_point_expr} != ''
              AND {not_excluded_sql}
            """,
            {"start": year_start(as_of), "end": as_of},
        )
        voice_ytd_unmapped_missing_entry_point_rows = _count(
            conn,
            f"""
            SELECT COUNT(*)
            FROM dialpad_voice_events
            WHERE dashboard_date(event_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
              AND COALESCE(school, '') = ''
              AND {entry_point_expr} = ''
              AND {not_excluded_sql}
            """,
            {"start": year_start(as_of), "end": as_of},
        )
        voice_ytd_unmapped_no_safe_evidence_rows = _count(
            conn,
            f"""
            SELECT COUNT(*)
            FROM dialpad_voice_events v
            WHERE dashboard_date(v.event_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
              AND COALESCE(v.school, '') = ''
              AND {entry_point_expr.replace("raw_json", "v.raw_json")} = ''
              AND {not_excluded_sql.replace("raw_json", "v.raw_json")}
              AND NOT (
                  COALESCE(json_extract(v.raw_json, '$.requested_school'), '') != ''
                  AND COALESCE(json_extract(v.raw_json, '$.school_filter_applied'), 0) = 1
                  AND COALESCE(json_extract(v.raw_json, '$.scope_school_mismatch'), 0) != 1
              )
              AND NOT EXISTS (
                  SELECT 1 FROM hubspot_contacts h
                  WHERE h.phone_normalized = v.phone_normalized
                    AND COALESCE(h.school, '') != ''
              )
              AND NOT EXISTS (
                  SELECT 1 FROM pike13_people p
                  WHERE p.phone_normalized = v.phone_normalized
                    AND COALESCE(p.school, '') != ''
              )
              AND NOT EXISTS (
                  SELECT 1 FROM dialpad_sms_threads t
                  WHERE t.phone_normalized = v.phone_normalized
                    AND COALESCE(t.school, '') != ''
              )
              AND NOT EXISTS (
                  SELECT 1 FROM dialpad_voice_events known
                  WHERE known.phone_normalized = v.phone_normalized
                    AND COALESCE(known.school, '') != ''
              )
              AND NOT EXISTS (
                  SELECT 1 FROM dialpad_call_reviews cr
                  WHERE (cr.voice_event_id = v.event_id OR cr.call_id = v.call_id OR cr.call_review_id = v.call_id)
                    AND json_array_length(COALESCE(json_extract(cr.raw_json, '$.visible_school_labels'), '[]')) = 1
              )
            """,
            {"start": year_start(as_of), "end": as_of},
        )
        if voice_ytd_unmapped_known_entry_point_rows:
            data_quality_flags.append(
                f"dialpad_voice_ytd_unmapped_entry_point_rows_{voice_ytd_unmapped_known_entry_point_rows}"
            )
        if voice_ytd_unmapped_missing_entry_point_rows:
            data_quality_flags.append(
                f"dialpad_voice_ytd_missing_entry_point_rows_{voice_ytd_unmapped_missing_entry_point_rows}"
            )
    phone_expr = "phone_normalized" if column_exists(conn, "vw_dialpad_communications", "phone_normalized") else "''"
    direction_expr = "direction" if column_exists(conn, "vw_dialpad_communications", "direction") else "''"
    complete_months = complete_months_ytd(as_of)
    for school in schools:
        school_like = "%height%" if "height" in school.lower() else "%west%"
        call_row = conn.execute(
            """
            SELECT
                COUNT(*) AS rows,
                SUM(CASE WHEN COALESCE({phone_expr}, '') != '' THEN 1 ELSE 0 END) AS with_phone,
                SUM(CASE WHEN LOWER(COALESCE({direction_expr}, '')) IN ('inbound', 'outbound') THEN 1 ELSE 0 END)
                    AS known_direction
            FROM vw_dialpad_communications
            WHERE channel = 'call'
              AND dashboard_date(event_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
              AND LOWER(COALESCE(school, '')) LIKE :school_like
            """.format(phone_expr=phone_expr, direction_expr=direction_expr),
            {"start": mtd_start, "end": as_of, "school_like": school_like},
        ).fetchone()
        sms_row = conn.execute(
            """
            SELECT
                COUNT(*) AS rows,
                SUM(CASE WHEN COALESCE({phone_expr}, '') != '' THEN 1 ELSE 0 END) AS with_phone
            FROM vw_dialpad_communications
            WHERE channel = 'sms'
              AND dashboard_date(event_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
              AND LOWER(COALESCE(school, '')) LIKE :school_like
            """.format(phone_expr=phone_expr),
            {"start": mtd_start, "end": as_of, "school_like": school_like},
        ).fetchone()
        call_rows = int(call_row["rows"] or 0)
        call_with_phone = int(call_row["with_phone"] or 0)
        call_known_direction = int(call_row["known_direction"] or 0)
        sms_rows = int(sms_row["rows"] or 0)
        sms_with_phone = int(sms_row["with_phone"] or 0)
        pareto = lead_followup_pareto_grid(conn, mtd_start, as_of, school)
        pareto_coverage = pareto.get("coverage", {})
        matched_rate = pareto_coverage.get("communication_coverage_rate")
        lead_identity_rate = pareto_coverage.get("lead_identity_rate")
        call_known_direction_rate = _rate(call_known_direction, call_rows)
        historical_call_months = []
        for month in complete_months:
            historical_rows = _count(
                conn,
                """
                SELECT COUNT(*)
                FROM vw_dialpad_communications
                WHERE channel = 'call'
                  AND substr(dashboard_date(event_at), 1, 7) = :month
                  AND LOWER(COALESCE(school, '')) LIKE :school_like
                """,
                {"month": month, "school_like": school_like},
            )
            historical_call_months.append({"month": month, "call_rows": historical_rows})
        school_blockers = []
        for item in historical_call_months:
            if item["call_rows"] < min_historical_monthly_call_rows:
                school_blockers.append(f"ytd_call_backfill_gap_{item['month']}")
        if sms_rows == 0:
            school_blockers.append("missing_mtd_sms_rows")
        if call_rows and (call_known_direction_rate or 0) < min_known_call_direction_rate:
            school_blockers.append(f"mtd_call_direction_coverage_below_{int(min_known_call_direction_rate * 100)}pct")
        if pareto_coverage.get("leads") and (matched_rate or 0) < min_matched_lead_rate:
            if lead_identity_rate is not None and lead_identity_rate < 0.7:
                data_quality_flags.append(
                    f"{school}:mtd_matched_communication_coverage_identity_limited"
                )
            else:
                school_blockers.append(f"mtd_matched_communication_coverage_below_{int(min_matched_lead_rate * 100)}pct")
        rows.append(
            {
                "school": school,
                "mtd_call_rows": call_rows,
                "mtd_call_phone_rows": call_with_phone,
                "mtd_call_phone_rate": _rate(call_with_phone, call_rows),
                "mtd_call_known_direction_rows": call_known_direction,
                "mtd_call_known_direction_rate": call_known_direction_rate,
                "mtd_sms_rows": sms_rows,
                "mtd_sms_phone_rows": sms_with_phone,
                "mtd_sms_phone_rate": _rate(sms_with_phone, sms_rows),
                "mtd_matched_communication_rate": matched_rate,
                "mtd_lead_identity_rate": lead_identity_rate,
                "historical_call_months": historical_call_months,
                "blockers": school_blockers,
            }
        )
        blockers.extend(f"{school}:{blocker}" for blocker in school_blockers)
    return {
        "status": "ready" if not blockers else "blocked",
        "schools": rows,
        "unmapped_voice_ytd": unmapped_voice_ytd,
        "unmapped_voice_mtd": unmapped_voice_mtd,
        "sms_ytd_no_phone_rows": sms_ytd_no_phone_rows,
        "sms_ytd_no_phone_school_attributed_rows": sms_ytd_no_phone_school_attributed_rows,
        "voice_ytd_unmapped_known_entry_point_rows": voice_ytd_unmapped_known_entry_point_rows,
        "voice_ytd_unmapped_missing_entry_point_rows": voice_ytd_unmapped_missing_entry_point_rows,
        "voice_ytd_unmapped_no_safe_evidence_rows": voice_ytd_unmapped_no_safe_evidence_rows,
        "blockers": blockers,
        "data_quality_flags": data_quality_flags,
    }


def build_dashboard_readiness(conn: sqlite3.Connection, *, as_of: str | None = None, schools: tuple[str, ...] = DEFAULT_SCHOOLS) -> dict:
    as_of = as_of or date.today().isoformat()
    conn.row_factory = sqlite3.Row
    register_dashboard_sql_functions(conn)
    freshness = source_freshness_gate(conn, as_of)
    hubspot = hubspot_spine_gate(conn, as_of=as_of, schools=schools)
    communications = communication_coverage_gate(conn, as_of=as_of, schools=schools)
    blockers = freshness["blockers"] + hubspot["blockers"] + communications["blockers"]
    data_quality_flags = hubspot.get("data_quality_flags", []) + communications.get("data_quality_flags", [])
    return {
        "report_type": "dashboard_readiness",
        "generated_at": utc_now_iso(),
        "as_of": as_of,
        "status": "ready" if not blockers else "blocked",
        "ready_for_management_use": not blockers,
        "blockers": blockers,
        "data_quality_flags": data_quality_flags,
        "source_freshness": freshness,
        "hubspot_lead_spine": hubspot,
        "communication_coverage": communications,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only readiness gate for NotesReminder dashboards.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--as-of", default=date.today().isoformat())
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument("--strict", action="store_true", help="Return exit code 2 when the dashboard is blocked.")
    args = parser.parse_args(argv)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        report = build_dashboard_readiness(conn, as_of=args.as_of)
    finally:
        conn.close()
    print(json.dumps(report, indent=2 if args.pretty else None, sort_keys=True, default=str))
    return 2 if args.strict and report["status"] != "ready" else 0


if __name__ == "__main__":
    raise SystemExit(main())
