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
    hubspot_contact_school_clause,
    hubspot_lead_count,
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
        dialpad_targets_ytd = len(select_hubspot_contact_targets(conn, school, ytd_start, as_of, limit=100000))
        mtd_phone_rate = _rate(mtd_phone, mtd_leads)
        school_blockers = []
        if mtd_leads and (mtd_phone_rate or 0) < min_mtd_phone_rate:
            school_blockers.append(f"hubspot_mtd_phone_coverage_below_{int(min_mtd_phone_rate * 100)}pct")
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
                "dialpad_target_eligible_ytd": dialpad_targets_ytd,
                "blockers": school_blockers,
            }
        )
        blockers.extend(f"{school}:{blocker}" for blocker in school_blockers)
    unassigned_ytd = max(total_ytd - len(usable_ytd_ids), 0)
    unassigned_mtd = max(total_mtd - len(usable_mtd_ids), 0)
    if unassigned_mtd:
        blockers.append(f"hubspot_mtd_unassigned_school_{unassigned_mtd}")
    if unassigned_ytd:
        blockers.append(f"hubspot_ytd_unassigned_school_{unassigned_ytd}")
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
    }


def communication_coverage_gate(conn: sqlite3.Connection, *, as_of: str, schools: tuple[str, ...] = DEFAULT_SCHOOLS) -> dict:
    if not table_exists(conn, "vw_dialpad_communications"):
        return {"status": "blocked", "blockers": ["missing_vw_dialpad_communications"], "schools": []}
    blockers = []
    rows = []
    mtd_start = month_start(as_of)
    for school in schools:
        call_rows = _count(
            conn,
            """
            SELECT COUNT(*)
            FROM vw_dialpad_communications
            WHERE channel = 'call'
              AND dashboard_date(event_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
              AND LOWER(COALESCE(school, '')) LIKE :school_like
            """,
            {"start": mtd_start, "end": as_of, "school_like": "%height%" if "height" in school.lower() else "%west%"},
        )
        sms_rows = _count(
            conn,
            """
            SELECT COUNT(*)
            FROM vw_dialpad_communications
            WHERE channel = 'sms'
              AND dashboard_date(event_at) BETWEEN dashboard_date(:start) AND dashboard_date(:end)
              AND LOWER(COALESCE(school, '')) LIKE :school_like
            """,
            {"start": mtd_start, "end": as_of, "school_like": "%height%" if "height" in school.lower() else "%west%"},
        )
        school_blockers = []
        if sms_rows == 0:
            school_blockers.append("missing_mtd_sms_rows")
        rows.append({"school": school, "mtd_call_rows": call_rows, "mtd_sms_rows": sms_rows, "blockers": school_blockers})
        blockers.extend(f"{school}:{blocker}" for blocker in school_blockers)
    return {"status": "ready" if not blockers else "blocked", "schools": rows, "blockers": blockers}


def build_dashboard_readiness(conn: sqlite3.Connection, *, as_of: str | None = None, schools: tuple[str, ...] = DEFAULT_SCHOOLS) -> dict:
    as_of = as_of or date.today().isoformat()
    conn.row_factory = sqlite3.Row
    register_dashboard_sql_functions(conn)
    freshness = source_freshness_gate(conn, as_of)
    hubspot = hubspot_spine_gate(conn, as_of=as_of, schools=schools)
    communications = communication_coverage_gate(conn, as_of=as_of, schools=schools)
    blockers = freshness["blockers"] + hubspot["blockers"] + communications["blockers"]
    return {
        "report_type": "dashboard_readiness",
        "generated_at": utc_now_iso(),
        "as_of": as_of,
        "status": "ready" if not blockers else "blocked",
        "ready_for_management_use": not blockers,
        "blockers": blockers,
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
