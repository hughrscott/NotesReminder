"""Static operations scorecard rendering."""

from __future__ import annotations

import html
import json
import sqlite3
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

from notesreminder.reports.dashboard_sql import register_dashboard_sql_functions
from notesreminder.reports.lead_operating_dashboard import (
    DEFAULT_SCHOOL,
    build_exception_queue,
    contact_date_expr,
    contact_person_expr,
    hubspot_lead_count,
    hubspot_contact_school_clause,
    pike13_outcomes,
    school_aliases,
    source_data_freshness,
    source_freshness,
    table_exists,
    trial_cohort_conversion_count,
)


DEFAULT_SCHOOLS = (DEFAULT_SCHOOL, "The Heights")
RESPONSE_BUCKETS = ("<5m", "5-15m", "15-60m", "1-24h", ">24h", "no response")
OPERATIONS_EXCEPTION_DETAIL_LIMIT = 50


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_day(value: str | date | None) -> date:
    if value is None:
        return date.today()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def scorecard_windows(as_of: str | date | None) -> dict[str, dict[str, str]]:
    day = parse_day(as_of)
    return {
        "mtd": {"start": day.replace(day=1).isoformat(), "end": day.isoformat()},
        "ytd": {"start": day.replace(month=1, day=1).isoformat(), "end": day.isoformat()},
        "response": {"start": (day.fromordinal(day.toordinal() - 6)).isoformat(), "end": day.isoformat()},
    }


def _school_filter(
    alias: str,
    school: str,
    *,
    fields: tuple[str, ...] = ("school",),
    contains: bool = False,
) -> tuple[str, dict[str, str]]:
    aliases = school_aliases(school)
    if not aliases:
        return "1=1", {}
    params = {f"{alias}_school_{index}": value for index, value in enumerate(aliases)}
    if contains:
        like_params = {key: f"%{value}%" for key, value in params.items()}
        clauses = [
            f"LOWER(COALESCE({alias}.{field}, '')) LIKE :{key}"
            for field in fields
            for key in like_params
        ]
        return "(" + " OR ".join(clauses) + ")", like_params
    placeholders = ", ".join(f":{key}" for key in params)
    clauses = [f"LOWER(COALESCE({alias}.{field}, '')) IN ({placeholders})" for field in fields]
    return "(" + " OR ".join(clauses) + ")", params


def _rate(numerator: int | float, denominator: int | float) -> float:
    return round(100.0 * numerator / denominator, 1) if denominator else 0.0


def _canonical_dashboard_school(value: str | None, schools: tuple[str, ...] = DEFAULT_SCHOOLS) -> str | None:
    value = (value or "").strip().lower()
    if not value:
        return None
    for school in schools:
        aliases = school_aliases(school)
        if value in aliases or any(alias and alias in value for alias in aliases):
            return "The Heights" if "height" in value else "West U"
    return None


def _school_sets_by_identity(
    conn: sqlite3.Connection,
    *,
    start_date: str,
    end_date: str,
    schools: tuple[str, ...],
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    email_schools: dict[str, set[str]] = {}
    phone_schools: dict[str, set[str]] = {}
    if table_exists(conn, "vw_school_email_communications"):
        for row in conn.execute(
            """
            SELECT LOWER(TRIM(external_email_normalized)) AS identity, school
            FROM vw_school_email_communications
            WHERE COALESCE(external_email_normalized, '') != ''
              AND date(substr(event_at, 1, 10)) BETWEEN date(:start) AND date(:end)
            """,
            {"start": start_date, "end": end_date},
        ):
            school = _canonical_dashboard_school(row["school"], schools)
            if school:
                email_schools.setdefault(row["identity"], set()).add(school)
    if table_exists(conn, "vw_dialpad_communications"):
        for row in conn.execute(
            """
            SELECT phone_normalized AS identity, school
            FROM vw_dialpad_communications
            WHERE COALESCE(phone_normalized, '') != ''
              AND date(substr(event_at, 1, 10)) BETWEEN date(:start) AND date(:end)
            """,
            {"start": start_date, "end": end_date},
        ):
            school = _canonical_dashboard_school(row["school"], schools)
            if school:
                phone_schools.setdefault(row["identity"], set()).add(school)
    return email_schools, phone_schools


def _blank_hubspot_school_inference(
    conn: sqlite3.Connection,
    *,
    start_date: str,
    end_date: str,
    schools: tuple[str, ...],
) -> dict:
    person_sql = contact_person_expr(conn)
    email_schools, phone_schools = _school_sets_by_identity(
        conn,
        start_date=start_date,
        end_date=end_date,
        schools=schools,
    )
    inferred_by_school = Counter()
    evidence_counts = Counter()
    inferred_contact_ids: set[str] = set()
    ambiguous = 0
    no_evidence = 0
    rows = conn.execute(
        f"""
        SELECT
            c.contact_id,
            LOWER(TRIM(COALESCE(c.email_normalized, ''))) AS email_normalized,
            c.phone_normalized,
            c.hubspot_deal_name,
            pp.school AS pike13_school
        FROM hubspot_contacts c
        LEFT JOIN pike13_people pp ON pp.person_id = {person_sql}
        WHERE date(c.create_date) BETWEEN date(:start) AND date(:end)
          AND COALESCE(TRIM(c.school), '') = ''
        """,
        {"start": start_date, "end": end_date},
    ).fetchall()
    for row in rows:
        signals: list[tuple[str, str]] = []
        pike13_school = _canonical_dashboard_school(row["pike13_school"], schools)
        if pike13_school:
            signals.append(("pike13", pike13_school))
        deal_school = _canonical_dashboard_school(row["hubspot_deal_name"], schools)
        if deal_school:
            signals.append(("deal_name", deal_school))
        if row["email_normalized"]:
            signals.extend(("email", school) for school in email_schools.get(row["email_normalized"], set()))
        phone = row["phone_normalized"]
        if phone:
            signals.extend(("phone", school) for school in phone_schools.get(phone, set()))
        signal_schools = {school for _, school in signals}
        if len(signal_schools) == 1:
            school = next(iter(signal_schools))
            inferred_contact_ids.add(row["contact_id"])
            inferred_by_school[school] += 1
            for source, _ in set(signals):
                evidence_counts[source] += 1
        elif len(signal_schools) > 1:
            ambiguous += 1
        else:
            no_evidence += 1
    inferred_total = sum(inferred_by_school.values())
    return {
        "inferred_total": inferred_total,
        "inferred_contact_ids": inferred_contact_ids,
        "inferred_by_school": dict(sorted(inferred_by_school.items())),
        "evidence_counts": dict(sorted(evidence_counts.items())),
        "ambiguous_school_evidence": ambiguous,
        "no_school_evidence": no_evidence,
    }


def _hubspot_school_assignment_quality(
    conn: sqlite3.Connection,
    *,
    start_date: str,
    end_date: str,
    schools: tuple[str, ...] = DEFAULT_SCHOOLS,
) -> dict:
    if not table_exists(conn, "hubspot_contacts"):
        return {
            "status": "missing",
            "total": 0,
            "usable_for_dashboard_schools": 0,
            "assigned_school": 0,
            "blank_school": 0,
            "other_school": 0,
            "unassigned_to_dashboard_school": 0,
            "missing_create_date": 0,
        }
    aliases = {alias for school in schools for alias in school_aliases(school)}
    person_sql = contact_person_expr(conn)
    rows = conn.execute(
        """
        SELECT LOWER(TRIM(COALESCE(school, ''))) AS school, COUNT(*) AS rows
        FROM hubspot_contacts c
        WHERE date(c.create_date) BETWEEN date(:start) AND date(:end)
        GROUP BY LOWER(TRIM(COALESCE(school, '')))
        """,
        {"start": start_date, "end": end_date},
    ).fetchall()
    missing_create_date = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM hubspot_contacts c
            WHERE COALESCE(c.create_date, '') = ''
              AND date(c.updated_at) BETWEEN date(:start) AND date(:end)
            """,
            {"start": start_date, "end": end_date},
        ).fetchone()[0]
        or 0
    )
    total = sum(int(row["rows"] or 0) for row in rows)
    blank = sum(int(row["rows"] or 0) for row in rows if not row["school"])
    assigned = sum(int(row["rows"] or 0) for row in rows if row["school"] in aliases)
    other = total - blank - assigned
    inferred = _blank_hubspot_school_inference(
        conn,
        start_date=start_date,
        end_date=end_date,
        schools=schools,
    )
    usable_contact_ids = set()
    for school in schools:
        school_sql, school_params = hubspot_contact_school_clause(school, conn)
        for row in conn.execute(
            f"""
            SELECT c.contact_id
            FROM hubspot_contacts c
            LEFT JOIN pike13_people pp ON pp.person_id = {person_sql}
            WHERE date(c.create_date) BETWEEN date(:start) AND date(:end)
              AND {school_sql}
            """,
            {"start": start_date, "end": end_date, **school_params},
        ).fetchall():
            usable_contact_ids.add(row["contact_id"])
    usable_contact_ids.update(inferred["inferred_contact_ids"])
    usable = len(usable_contact_ids)
    unassigned = max(total - usable, 0)
    flags = []
    if unassigned:
        flags.append(f"hubspot_contacts_unassigned_school_{unassigned}")
    if other:
        flags.append(f"hubspot_contacts_unrecognized_school_{other}")
    return {
        "status": "ready" if not flags else "attention",
        "total": total,
        "usable_for_dashboard_schools": usable,
        "assigned_school": assigned,
        "blank_school": blank,
        "inferred_blank_school": inferred["inferred_total"],
        "inferred_blank_school_by_school": inferred["inferred_by_school"],
        "inferred_blank_school_evidence_counts": inferred["evidence_counts"],
        "ambiguous_blank_school_evidence": inferred["ambiguous_school_evidence"],
        "blank_school_without_evidence": inferred["no_school_evidence"],
        "other_school": other,
        "unassigned_to_dashboard_school": unassigned,
        "missing_create_date": missing_create_date,
        "flags": flags,
    }


def _rows(conn: sqlite3.Connection, sql: str, params: dict | None = None) -> list[dict]:
    return [dict(row) for row in conn.execute(sql, params or {}).fetchall()]


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if not table_exists(conn, table):
        return False
    return any(row[1] == column for row in conn.execute(f"PRAGMA table_info({table})"))


def _dialpad_identity_gaps(conn: sqlite3.Connection, *, start_date: str, end_date: str) -> dict:
    metrics = {
        "sms_no_phone_rows": 0,
        "sms_no_phone_school_attributed_rows": 0,
        "voice_unmapped_school_rows": 0,
        "voice_unmapped_known_entry_point_rows": 0,
        "voice_unmapped_missing_entry_point_rows": 0,
        "voice_unmapped_no_safe_evidence_rows": 0,
    }
    flags = []
    if table_exists(conn, "dialpad_sms_messages") and table_exists(conn, "dialpad_sms_threads"):
        metrics["sms_no_phone_rows"] = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM dialpad_sms_messages m
                LEFT JOIN dialpad_sms_threads t ON t.thread_id = m.thread_id
                WHERE date(m.message_at) BETWEEN date(:start) AND date(:end)
                  AND COALESCE(t.phone_normalized, '') = ''
                """,
                {"start": start_date, "end": end_date},
            ).fetchone()[0]
            or 0
        )
        metrics["sms_no_phone_school_attributed_rows"] = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM dialpad_sms_messages m
                LEFT JOIN dialpad_sms_threads t ON t.thread_id = m.thread_id
                WHERE date(m.message_at) BETWEEN date(:start) AND date(:end)
                  AND COALESCE(t.phone_normalized, '') = ''
                  AND COALESCE(t.school, t.department, '') != ''
                """,
                {"start": start_date, "end": end_date},
            ).fetchone()[0]
            or 0
        )
    if table_exists(conn, "dialpad_voice_events"):
        entry_point_expr = (
            "COALESCE(json_extract(raw_json, '$.display_entry_point'), '')"
            if _column_exists(conn, "dialpad_voice_events", "raw_json")
            else "''"
        )
        not_excluded_sql = (
            "COALESCE(json_extract(raw_json, '$.excluded_from_communication_view'), 0) != 1"
            if _column_exists(conn, "dialpad_voice_events", "raw_json")
            else "1=1"
        )
        row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS unmapped_school,
                SUM(CASE WHEN {entry_point_expr} != '' THEN 1 ELSE 0 END) AS known_entry_point,
                SUM(CASE WHEN {entry_point_expr} = '' THEN 1 ELSE 0 END) AS missing_entry_point
            FROM dialpad_voice_events
            WHERE date(event_at) BETWEEN date(:start) AND date(:end)
              AND COALESCE(school, '') = ''
              AND {not_excluded_sql}
            """,
            {"start": start_date, "end": end_date},
        ).fetchone()
        metrics["voice_unmapped_school_rows"] = int(row["unmapped_school"] or 0)
        metrics["voice_unmapped_known_entry_point_rows"] = int(row["known_entry_point"] or 0)
        metrics["voice_unmapped_missing_entry_point_rows"] = int(row["missing_entry_point"] or 0)
        safe_evidence_row = conn.execute(
            f"""
            SELECT COUNT(*) AS rows
            FROM dialpad_voice_events v
            WHERE date(v.event_at) BETWEEN date(:start) AND date(:end)
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
            {"start": start_date, "end": end_date},
        ).fetchone()
        metrics["voice_unmapped_no_safe_evidence_rows"] = int(safe_evidence_row["rows"] or 0)
    if metrics["sms_no_phone_rows"]:
        flags.append(f"dialpad_sms_no_phone_rows_{metrics['sms_no_phone_rows']}")
    if metrics["voice_unmapped_school_rows"]:
        flags.append(f"dialpad_voice_unmapped_school_rows_{metrics['voice_unmapped_school_rows']}")
    if metrics["voice_unmapped_known_entry_point_rows"]:
        flags.append(
            "dialpad_voice_unmapped_entry_point_rows_"
            f"{metrics['voice_unmapped_known_entry_point_rows']}"
        )
    if metrics["voice_unmapped_missing_entry_point_rows"]:
        flags.append(
            "dialpad_voice_missing_entry_point_rows_"
            f"{metrics['voice_unmapped_missing_entry_point_rows']}"
        )
    return {"status": "ready" if not flags else "attention", "window": {"start": start_date, "end": end_date}, **metrics, "flags": flags}


def metric_status_from_freshness(data_freshness: dict) -> dict[str, dict[str, object]]:
    flags = list(data_freshness.get("flags", []))

    def blocked_by(*prefixes: str) -> dict[str, object]:
        blockers = [
            flag
            for flag in flags
            if any(flag.startswith(prefix) for prefix in prefixes)
        ]
        return {
            "status": "blocked" if blockers else "ready",
            "blockers": blockers,
        }

    communication_prefixes = (
        "missing_school_email",
        "stale_school_email",
        "gmail_auth_required",
        "missing_dialpad_calls",
        "stale_dialpad_calls",
        "missing_dialpad_sms",
        "stale_dialpad_sms",
        "missing_school_sms",
    )
    return {
        "leads": blocked_by("missing_hubspot_contacts", "stale_hubspot_contacts"),
        "notes": blocked_by("missing_lessons", "stale_lessons"),
        "trials": blocked_by("missing_pike13_visits", "stale_pike13_visits"),
        "conversions": blocked_by("missing_pike13_visits", "stale_pike13_visits"),
        "no_shows": blocked_by("missing_pike13_visits", "stale_pike13_visits"),
        "communications": blocked_by(*communication_prefixes),
        "contacted": blocked_by(*communication_prefixes),
        "response": blocked_by(*communication_prefixes),
        "outbound_calls": blocked_by(*communication_prefixes),
    }


def apply_response_quality_status(
    metric_status: dict[str, dict[str, object]],
    response: dict,
    *,
    min_identity_rate: float = 70.0,
    min_matched_communication_rate: float = 50.0,
) -> None:
    lead_count = int(response.get("lead_count") or 0)
    if lead_count <= 0:
        return
    coverage = response.get("coverage", {})
    blockers = []
    identity_rate = float(coverage.get("identity_key_rate") or 0.0)
    matched_rate = float(coverage.get("any_matched_communication_rate") or 0.0)
    if identity_rate < min_identity_rate:
        blockers.append(f"recent_lead_identity_coverage_below_{int(min_identity_rate)}pct")
    elif matched_rate < min_matched_communication_rate:
        blockers.append(f"recent_matched_communication_coverage_below_{int(min_matched_communication_rate)}pct")
    if not blockers:
        return
    for metric in ("contacted", "response"):
        current = metric_status.setdefault(metric, {"status": "ready", "blockers": []})
        current_blockers = list(current.get("blockers", []))
        for blocker in blockers:
            if blocker not in current_blockers:
                current_blockers.append(blocker)
        current["blockers"] = current_blockers
        current["status"] = "blocked"


def instructor_note_scores(
    conn: sqlite3.Connection,
    *,
    start_date: str,
    end_date: str,
    school: str,
    limit: int,
) -> list[dict]:
    school_sql, school_params = _school_filter("s", school, fields=("school_code", "school_name"))
    return _rows(
        conn,
        f"""
        SELECT
            COALESCE(NULLIF(i.instructor_name, ''), 'unknown') AS instructor_name,
            COUNT(*) AS reportable_lessons,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END) AS completed_notes,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 0 THEN 1 ELSE 0 END) AS missing_notes,
            ROUND(
                SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN COALESCE(n.note_score, 0) ELSE 0 END)
                / NULLIF(COUNT(*), 0),
                2
            ) AS average_note_score
        FROM lessons l
        JOIN schools s ON s.school_id = l.school_id
        LEFT JOIN instructors i ON i.instructor_id = l.instructor_id
        LEFT JOIN lesson_notes n ON n.lesson_id = l.lesson_id
        WHERE date(l.lesson_date) BETWEEN date(:start) AND date(:end)
          AND {school_sql}
          AND COALESCE(l.lesson_is_reportable, 0) = 1
        GROUP BY COALESCE(NULLIF(i.instructor_name, ''), 'unknown')
        HAVING reportable_lessons > 0
        ORDER BY average_note_score DESC, reportable_lessons DESC, instructor_name
        LIMIT :limit
        """,
        {"start": start_date, "end": end_date, "limit": limit, **school_params},
    )


def instructor_trial_conversions_ytd(
    conn: sqlite3.Connection,
    *,
    start_date: str,
    end_date: str,
    school: str,
    limit: int,
) -> list[dict]:
    school_sql, school_params = _school_filter("v", school)
    rows = _rows(
        conn,
        f"""
        SELECT
            v.visit_id,
            COALESCE(NULLIF(v.instructor, ''), 'unknown') AS instructor_name,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM pike13_plans_passes pp
                    WHERE pp.person_id = v.person_id
                      AND LOWER(COALESCE(pp.name, '')) NOT LIKE '%trial%'
                      AND LOWER(COALESCE(pp.name, '')) NOT LIKE '%free%'
                      AND date(COALESCE(NULLIF(pp.starts_at, ''), NULLIF(pp.next_invoice_at, ''), pp.updated_at))
                          BETWEEN date(v.starts_at, '-30 day') AND date(v.starts_at, '+30 day')
                )
                THEN 1 ELSE 0
            END AS converted_trial
        FROM pike13_visits v
        WHERE date(v.starts_at) BETWEEN date(:start) AND date(:end)
          AND COALESCE(v.person_id, '') != ''
          AND {school_sql}
          AND (COALESCE(v.first_visit_flag, 0) = 1 OR LOWER(COALESCE(v.service, '')) LIKE '%trial%')
        """,
        {"start": start_date, "end": end_date, **school_params},
    )
    totals: dict[str, dict[str, int | str]] = {}
    for row in rows:
        names = [item.strip() for item in str(row["instructor_name"] or "unknown").split(",") if item.strip()]
        if not names:
            names = ["unknown"]
        for name in names:
            current = totals.setdefault(
                name,
                {"instructor_name": name, "trial_lessons": 0, "converted_trials": 0},
            )
            current["trial_lessons"] += 1
            current["converted_trials"] += int(row["converted_trial"] or 0)
    ranked = sorted(
        totals.values(),
        key=lambda item: (
            -float(item["converted_trials"]) / max(float(item["trial_lessons"]), 1.0),
            -int(item["converted_trials"]),
            -int(item["trial_lessons"]),
            str(item["instructor_name"]),
        ),
    )
    return ranked[:limit]


def funnel_metrics(conn: sqlite3.Connection, *, start_date: str, end_date: str, school: str) -> dict:
    register_dashboard_sql_functions(conn)
    pike13 = pike13_outcomes(conn, start_date, end_date, school)
    trial_count = int(pike13.get("first_visits", 0) or 0)
    converted_count = trial_cohort_conversion_count(conn, start_date, end_date, school)
    data = {
        "new_leads": hubspot_lead_count(conn, start_date, end_date, school),
        "leads_to_trial": trial_count,
        "trial_lessons": trial_count,
        "trials_converted": converted_count,
    }
    data["lead_to_trial_rate"] = _rate(data.get("leads_to_trial", 0), data.get("new_leads", 0))
    data["trial_to_conversion_rate"] = _rate(data.get("trials_converted", 0), data.get("leads_to_trial", 0))
    return data


def outbound_calls(conn: sqlite3.Connection, *, start_date: str, end_date: str, school: str) -> int:
    school_sql, school_params = _school_filter("v", school, contains=True)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS rows
        FROM vw_dialpad_communications v
        WHERE date(v.event_at) BETWEEN date(:start) AND date(:end)
          AND {school_sql}
          AND v.channel = 'call'
          AND LOWER(COALESCE(v.direction, '')) = 'outbound'
        """,
        {"start": start_date, "end": end_date, **school_params},
    ).fetchone()
    return int(row["rows"] or 0)


def no_shows(conn: sqlite3.Connection, *, start_date: str, end_date: str, school: str) -> int:
    school_sql, school_params = _school_filter("v", school)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS rows
        FROM pike13_visits v
        WHERE date(v.starts_at) BETWEEN date(:start) AND date(:end)
          AND {school_sql}
          AND (
              COALESCE(v.no_show_flag, 0) = 1
              OR LOWER(COALESCE(v.status, '')) LIKE '%no show%'
          )
        """,
        {"start": start_date, "end": end_date, **school_params},
    ).fetchone()
    return int(row["rows"] or 0)


def lead_response_distribution(
    conn: sqlite3.Connection,
    *,
    start_date: str,
    end_date: str,
    school: str,
    limit: int,
) -> dict:
    register_dashboard_sql_functions(conn)
    school_sql, school_params = hubspot_contact_school_clause(school, conn)
    comm_school_sql, comm_school_params = _school_filter("comm", school, contains=True)
    email_school_sql, email_school_params = _school_filter("email", school, contains=True)
    lead_date_sql = contact_date_expr(conn)
    person_sql = contact_person_expr(conn)
    rows = _rows(
        conn,
        f"""
        WITH lead_rows AS (
            SELECT
                c.contact_id,
                {lead_date_sql} AS create_at,
                c.email_normalized AS contact_email,
                c.phone_normalized AS contact_phone,
                pp.email_normalized AS pike13_email,
                pp.phone_normalized AS pike13_phone,
                CASE
                    WHEN COALESCE(c.phone_normalized, '') != '' THEN 'phone:' || c.phone_normalized
                    WHEN COALESCE(pp.phone_normalized, '') != '' THEN 'phone:' || pp.phone_normalized
                    WHEN COALESCE(c.email_normalized, '') != '' THEN 'email:' || c.email_normalized
                    WHEN COALESCE(pp.email_normalized, '') != '' THEN 'email:' || pp.email_normalized
                    ELSE 'contact:' || c.contact_id
                END AS lead_id
            FROM hubspot_contacts c
            LEFT JOIN pike13_people pp
              ON pp.person_id = {person_sql}
            WHERE {lead_date_sql}
                BETWEEN dashboard_date(:start) AND dashboard_date(:end)
              AND {school_sql}
        ),
        leads AS (
            SELECT lead_id, MIN(create_at) AS create_at, COUNT(*) AS contact_rows
            FROM lead_rows
            GROUP BY lead_id
        ),
        lead_phones AS (
            SELECT lead_id, contact_phone AS phone_normalized
            FROM lead_rows
            WHERE COALESCE(contact_phone, '') != ''
            UNION
            SELECT lead_id, pike13_phone AS phone_normalized
            FROM lead_rows
            WHERE COALESCE(pike13_phone, '') != ''
        ),
        lead_emails AS (
            SELECT lead_id, contact_email AS email_normalized
            FROM lead_rows
            WHERE COALESCE(contact_email, '') != ''
            UNION
            SELECT lead_id, pike13_email AS email_normalized
            FROM lead_rows
            WHERE COALESCE(pike13_email, '') != ''
        ),
        lead_keys AS (
            SELECT lead_id, 'phone' AS key_type, phone_normalized AS key_value
            FROM lead_phones
            WHERE COALESCE(phone_normalized, '') != ''
            UNION
            SELECT lead_id, 'email' AS key_type, email_normalized AS key_value
            FROM lead_emails
            WHERE COALESCE(email_normalized, '') != ''
        ),
        communications AS (
            SELECT
                lp.lead_id,
                comm.event_at,
                comm.channel,
                comm.direction,
                CASE WHEN {comm_school_sql} THEN 1 ELSE 0 END AS source_school_matches
            FROM lead_phones lp
            JOIN vw_dialpad_communications comm ON comm.phone_normalized = lp.phone_normalized
            WHERE COALESCE(lp.phone_normalized, '') != ''
              AND date(comm.event_at) BETWEEN date((SELECT create_at FROM leads l WHERE l.lead_id = lp.lead_id), '-7 days') AND date(:end)
            UNION ALL
            SELECT
                le.lead_id,
                email.event_at,
                'email' AS channel,
                email.direction,
                CASE WHEN {email_school_sql} THEN 1 ELSE 0 END AS source_school_matches
            FROM lead_emails le
            JOIN vw_school_email_communications email ON email.external_email_normalized = le.email_normalized
            WHERE COALESCE(le.email_normalized, '') != ''
              AND date(email.event_at) BETWEEN date((SELECT create_at FROM leads l WHERE l.lead_id = le.lead_id), '-7 days') AND date(:end)
        ),
        outbound AS (
            SELECT
                lp.lead_id,
                comm.event_at,
                comm.channel,
                CASE WHEN {comm_school_sql} THEN 1 ELSE 0 END AS source_school_matches
            FROM lead_phones lp
            JOIN vw_dialpad_communications comm ON comm.phone_normalized = lp.phone_normalized
            WHERE COALESCE(lp.phone_normalized, '') != ''
              AND LOWER(COALESCE(comm.direction, '')) = 'outbound'
              AND date(comm.event_at) <= date(:end)
            UNION ALL
            SELECT
                le.lead_id,
                email.event_at,
                'email',
                CASE WHEN {email_school_sql} THEN 1 ELSE 0 END AS source_school_matches
            FROM lead_emails le
            JOIN vw_school_email_communications email ON email.external_email_normalized = le.email_normalized
            WHERE COALESCE(le.email_normalized, '') != ''
              AND LOWER(COALESCE(email.direction, '')) = 'outbound'
              AND date(email.event_at) <= date(:end)
        ),
        first_response AS (
            SELECT l.lead_id, l.create_at, MIN(o.event_at) AS first_response_at
            FROM leads l
            LEFT JOIN outbound o ON o.lead_id = l.lead_id AND datetime(o.event_at) >= datetime(l.create_at)
            GROUP BY l.lead_id, l.create_at
        )
        SELECT
            lead_id,
            create_at,
            first_response_at,
            (SELECT contact_rows FROM leads l WHERE l.lead_id = first_response.lead_id) AS contact_rows,
            EXISTS(
                SELECT 1 FROM lead_keys lk WHERE lk.lead_id = first_response.lead_id
            ) AS has_identity_key,
            EXISTS(
                SELECT 1 FROM communications c WHERE c.lead_id = first_response.lead_id
            ) AS has_any_matched_communication,
            EXISTS(
                SELECT 1 FROM communications c
                WHERE c.lead_id = first_response.lead_id
                  AND c.source_school_matches = 1
            ) AS has_same_school_matched_communication,
            EXISTS(
                SELECT 1 FROM communications c
                WHERE c.lead_id = first_response.lead_id
                  AND c.source_school_matches = 0
            ) AS has_cross_school_matched_communication,
            EXISTS(
                SELECT 1
                FROM communications c
                WHERE c.lead_id = first_response.lead_id
                  AND LOWER(COALESCE(c.direction, '')) = 'inbound'
                  AND datetime(c.event_at) <= datetime(first_response.create_at)
            ) AS has_pre_lead_inbound_origin,
            EXISTS(
                SELECT 1
                FROM communications c
                WHERE c.lead_id = first_response.lead_id
                  AND LOWER(COALESCE(c.direction, '')) = 'outbound'
                  AND datetime(c.event_at) >= datetime(first_response.create_at)
            ) AS has_post_lead_outbound_followup,
            EXISTS(
                SELECT 1 FROM communications c
                WHERE c.lead_id = first_response.lead_id AND c.channel = 'sms'
            ) AS has_sms,
            EXISTS(
                SELECT 1 FROM communications c
                WHERE c.lead_id = first_response.lead_id AND c.channel = 'call'
            ) AS has_call,
            EXISTS(
                SELECT 1 FROM communications c
                WHERE c.lead_id = first_response.lead_id AND c.channel = 'email'
            ) AS has_email,
            CASE
                WHEN first_response_at IS NULL THEN NULL
                ELSE ROUND((julianday(first_response_at) - julianday(create_at)) * 24.0 * 60.0, 1)
            END AS response_minutes,
            CAST(strftime('%w', create_at) AS INTEGER) AS lead_weekday,
            CAST(strftime('%H', create_at) AS INTEGER) AS lead_hour
        FROM first_response
        ORDER BY create_at
        """,
        {
            "start": start_date,
            "end": end_date,
            **school_params,
            **comm_school_params,
            **email_school_params,
        },
    )
    bucket_counts = Counter({bucket: 0 for bucket in RESPONSE_BUCKETS})
    heatmap = Counter()
    weekday_labels = ("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")
    total_minutes = 0.0
    responded = 0
    identity_key_leads = 0
    any_matched_communication = 0
    same_school_matched_communication = 0
    cross_school_matched_communication = 0
    pre_lead_inbound_origin = 0
    post_lead_outbound_followup = 0
    sms_matched = 0
    call_matched = 0
    email_matched = 0
    contact_rows_total = 0
    for row in rows:
        minutes = row.get("response_minutes")
        if minutes is None:
            bucket = "no response"
        elif minutes < 5:
            bucket = "<5m"
        elif minutes < 15:
            bucket = "5-15m"
        elif minutes < 60:
            bucket = "15-60m"
        elif minutes <= 1440:
            bucket = "1-24h"
        else:
            bucket = ">24h"
        bucket_counts[bucket] += 1
        if minutes is not None:
            total_minutes += float(minutes)
            responded += 1
        identity_key_leads += 1 if row.get("has_identity_key") else 0
        any_matched_communication += 1 if row.get("has_any_matched_communication") else 0
        same_school_matched_communication += 1 if row.get("has_same_school_matched_communication") else 0
        cross_school_matched_communication += 1 if row.get("has_cross_school_matched_communication") else 0
        pre_lead_inbound_origin += 1 if row.get("has_pre_lead_inbound_origin") else 0
        post_lead_outbound_followup += 1 if row.get("has_post_lead_outbound_followup") else 0
        sms_matched += 1 if row.get("has_sms") else 0
        call_matched += 1 if row.get("has_call") else 0
        email_matched += 1 if row.get("has_email") else 0
        contact_rows_total += int(row.get("contact_rows") or 1)
        weekday = row.get("lead_weekday")
        hour = row.get("lead_hour")
        label = f"{weekday_labels[weekday] if weekday is not None else '?'} {int(hour or 0):02d}:00"
        heatmap[label] += 1
    heatmap_rows = [{"lead_created": key, "leads": value} for key, value in heatmap.most_common(limit)]
    lead_count = len(rows)
    return {
        "lead_count": lead_count,
        "contact_rows": contact_rows_total,
        "responded": responded,
        "no_response": bucket_counts["no response"],
        "average_response_minutes": round(total_minutes / responded, 1) if responded else None,
        "buckets": dict(bucket_counts),
        "lead_created_distribution": heatmap_rows,
        "coverage": {
            "identity_key_leads": identity_key_leads,
            "identity_key_rate": _rate(identity_key_leads, lead_count),
            "any_matched_communication": any_matched_communication,
            "any_matched_communication_rate": _rate(any_matched_communication, lead_count),
            "same_school_matched_communication": same_school_matched_communication,
            "same_school_matched_communication_rate": _rate(same_school_matched_communication, lead_count),
            "cross_school_matched_communication": cross_school_matched_communication,
            "cross_school_matched_communication_rate": _rate(cross_school_matched_communication, lead_count),
            "pre_lead_inbound_origin": pre_lead_inbound_origin,
            "pre_lead_inbound_origin_rate": _rate(pre_lead_inbound_origin, lead_count),
            "post_lead_outbound_followup": post_lead_outbound_followup,
            "post_lead_outbound_followup_rate": _rate(post_lead_outbound_followup, lead_count),
            "sms_matched": sms_matched,
            "call_matched": call_matched,
            "email_matched": email_matched,
        },
    }


def build_operations_dashboard(
    conn: sqlite3.Connection,
    *,
    period: str = "weekly",
    as_of: str | date | None = None,
    schools: tuple[str, ...] = DEFAULT_SCHOOLS,
    limit: int = 25,
) -> dict:
    conn.row_factory = sqlite3.Row
    windows = scorecard_windows(as_of)
    mtd = windows["mtd"]
    ytd = windows["ytd"]
    response = windows["response"]
    freshness = source_freshness(conn) if table_exists(conn, "source_import_runs") else {"status": "missing", "counts": {}}
    school_reports = []
    exception_summary: Counter[str] = Counter()
    exception_detail_limit = min(limit, OPERATIONS_EXCEPTION_DETAIL_LIMIT)

    for school in schools:
        exceptions = build_exception_queue(
            conn,
            ytd["start"],
            ytd["end"],
            school,
            exception_detail_limit,
            trial_limit=exception_detail_limit,
        )
        exception_summary.update(exceptions.get("summary", {}))
        mtd_funnel = funnel_metrics(conn, start_date=mtd["start"], end_date=mtd["end"], school=school)
        ytd_funnel = funnel_metrics(conn, start_date=ytd["start"], end_date=ytd["end"], school=school)
        data_freshness = source_data_freshness(conn, mtd["end"], school)
        metric_status = metric_status_from_freshness(data_freshness)
        lead_response = lead_response_distribution(
            conn, start_date=response["start"], end_date=response["end"], school=school, limit=limit
        )
        apply_response_quality_status(metric_status, lead_response)
        school_reports.append(
            {
                "school": school,
                "source_data_freshness": data_freshness,
                "metric_status": metric_status,
                "notes_mtd": instructor_note_scores(
                    conn, start_date=mtd["start"], end_date=mtd["end"], school=school, limit=limit
                ),
                "notes_ytd": instructor_note_scores(
                    conn, start_date=ytd["start"], end_date=ytd["end"], school=school, limit=limit
                ),
                "conversion_ytd": instructor_trial_conversions_ytd(
                    conn, start_date=ytd["start"], end_date=ytd["end"], school=school, limit=limit
                ),
                "funnel_mtd": mtd_funnel,
                "funnel_ytd": ytd_funnel,
                "outbound_calls_mtd": outbound_calls(
                    conn, start_date=mtd["start"], end_date=mtd["end"], school=school
                ),
                "lead_response": lead_response,
                "no_shows_mtd": no_shows(conn, start_date=mtd["start"], end_date=mtd["end"], school=school),
                "exceptions": exceptions,
            }
        )

    totals = {
        "mtd_new_leads": sum(item["funnel_mtd"].get("new_leads", 0) for item in school_reports),
        "mtd_leads_to_trial": sum(item["funnel_mtd"].get("leads_to_trial", 0) for item in school_reports),
        "mtd_conversions": sum(item["funnel_mtd"].get("trials_converted", 0) for item in school_reports),
        "mtd_outbound_calls": sum(item["outbound_calls_mtd"] for item in school_reports),
        "mtd_no_shows": sum(item["no_shows_mtd"] for item in school_reports),
        "response_leads": sum(item["lead_response"].get("lead_count", 0) for item in school_reports),
        "response_no_response": sum(item["lead_response"].get("no_response", 0) for item in school_reports),
    }
    totals["mtd_lead_to_trial_rate"] = _rate(totals["mtd_leads_to_trial"], totals["mtd_new_leads"])
    totals["mtd_trial_to_conversion_rate"] = _rate(totals["mtd_conversions"], totals["mtd_leads_to_trial"])
    hubspot_quality = _hubspot_school_assignment_quality(
        conn,
        start_date=ytd["start"],
        end_date=ytd["end"],
        schools=schools,
    )
    dialpad_identity_gaps = _dialpad_identity_gaps(conn, start_date=ytd["start"], end_date=ytd["end"])

    data_freshness_flags = sorted(
        {
            flag
            for item in school_reports
            for flag in item.get("source_data_freshness", {}).get("flags", [])
        }
    )
    data_quality_flags = sorted(
        data_freshness_flags + hubspot_quality.get("flags", []) + dialpad_identity_gaps.get("flags", [])
    )
    status = "ready" if freshness.get("status") == "ready" and not exception_summary and not data_quality_flags else "attention"
    return {
        "dashboard_type": "operations_scorecard",
        "generated_at": utc_now_iso(),
        "period": period,
        "as_of": parse_day(as_of).isoformat(),
        "windows": windows,
        "overall_status": status,
        "totals": totals,
        "school_reports": school_reports,
        "exception_summary": dict(sorted(exception_summary.items())),
        "source_freshness": freshness,
        "hubspot_school_assignment": hubspot_quality,
        "dialpad_identity_gaps": dialpad_identity_gaps,
        "source_data_freshness": {
            "status": "ready" if not data_quality_flags else "attention",
            "flags": data_quality_flags,
        },
    }


def _h(value) -> str:
    return html.escape(str(value if value is not None else ""))


def _format_number(value) -> str:
    if isinstance(value, float):
        return f"{value:.1f}"
    return str(value)


def _metric_card(label: str, value, detail: str = "") -> str:
    return (
        '<article class="metric">'
        f'<div class="metric-label">{_h(label)}</div>'
        f'<div class="metric-value">{_h(_format_number(value))}</div>'
        f'<div class="metric-detail">{_h(detail)}</div>'
        "</article>"
    )


def _blocked_card(label: str, blockers: list[str], detail: str = "source data blocked") -> str:
    blocker_text = ", ".join(blockers[:2])
    if len(blockers) > 2:
        blocker_text += f", +{len(blockers) - 2} more"
    return _metric_card(label, "Blocked", blocker_text or detail)


def _status_class(value: str) -> str:
    return "ready" if str(value).lower() == "ready" else "attention"


def _table(headers: list[str], rows: list[list]) -> str:
    if not rows:
        return '<p class="empty">None.</p>'
    header_html = "".join(f"<th>{_h(header)}</th>" for header in headers)
    row_html = []
    for row in rows:
        row_html.append("<tr>" + "".join(f"<td>{_h(value)}</td>" for value in row) + "</tr>")
    return f"<table><thead><tr>{header_html}</tr></thead><tbody>{''.join(row_html)}</tbody></table>"


def _flags_panel(report: dict) -> str:
    flags = report.get("source_data_freshness", {}).get("flags", [])
    if not flags:
        return ""
    hubspot_quality = report.get("hubspot_school_assignment", {})
    dialpad_gaps = report.get("dialpad_identity_gaps", {})
    rows = [[flag] for flag in flags]
    return f"""
    <section class="warning-panel">
      <h2>Data Quality Flags</h2>
      <p>
        This dashboard is in validation mode. Treat lead counts as HubSpot-derived, and review these source
        and identity flags before using note, trial, conversion, contacted, or response-rate metrics for
        management decisions.
      </p>
      <div class="warning-grid">
        <div>
          <h3>Active flags</h3>
          {_table(["Flag"], rows)}
        </div>
        <div>
          <h3>HubSpot lead spine</h3>
          {_table(
              ["Metric", "Rows"],
              [
                  ["YTD contacts", hubspot_quality.get("total", 0)],
                  ["Usable by dashboard school filter", hubspot_quality.get("usable_for_dashboard_schools", 0)],
                  ["Explicit school column match", hubspot_quality.get("assigned_school", 0)],
                  ["Blank school column", hubspot_quality.get("blank_school", 0)],
                  ["Blank school inferred from safe evidence", hubspot_quality.get("inferred_blank_school", 0)],
                  ["Blank school ambiguous evidence", hubspot_quality.get("ambiguous_blank_school_evidence", 0)],
                  ["Blank school without evidence", hubspot_quality.get("blank_school_without_evidence", 0)],
                  ["Unassigned to dashboard school", hubspot_quality.get("unassigned_to_dashboard_school", 0)],
                  ["Unrecognized school", hubspot_quality.get("other_school", 0)],
                  ["Missing create date excluded", hubspot_quality.get("missing_create_date", 0)],
              ],
          )}
        </div>
        <div>
          <h3>Dialpad identity gaps</h3>
          {_table(
              ["Metric", "Rows"],
              [
                  ["Captured SMS rows without phone identity", dialpad_gaps.get("sms_no_phone_rows", 0)],
                  ["Of those, school-attributed SMS rows", dialpad_gaps.get("sms_no_phone_school_attributed_rows", 0)],
                  ["Voice rows without school", dialpad_gaps.get("voice_unmapped_school_rows", 0)],
                  ["Voice rows with unmapped entry point", dialpad_gaps.get("voice_unmapped_known_entry_point_rows", 0)],
                  ["Voice rows missing entry point", dialpad_gaps.get("voice_unmapped_missing_entry_point_rows", 0)],
                  ["Voice rows with no safe school evidence", dialpad_gaps.get("voice_unmapped_no_safe_evidence_rows", 0)],
              ],
          )}
        </div>
      </div>
    </section>
    """


def _school_flags(item: dict) -> str:
    flags = item.get("source_data_freshness", {}).get("flags", [])
    if not flags:
        return '<p class="school-health ready-text">Source data current for this school.</p>'
    return (
        '<div class="school-health attention-text">'
        '<strong>Source data is not current for this school.</strong>'
        + _table(["Flag"], [[flag] for flag in flags])
        + "</div>"
    )


def _metric_ready(item: dict, metric: str) -> bool:
    return item.get("metric_status", {}).get(metric, {}).get("status") != "blocked"


def _metric_blockers(item: dict, metric: str) -> list[str]:
    return list(item.get("metric_status", {}).get(metric, {}).get("blockers", []))


def _blocked_table(metric_name: str, blockers: list[str]) -> str:
    rows = [[flag] for flag in blockers] or [["Source data is not current for this metric."]]
    return (
        f'<p class="empty">{_h(metric_name)} is blocked until source data is refreshed.</p>'
        + _table(["Blocker"], rows)
    )


def _notes_rows(rows: list[dict]) -> list[list]:
    return [
        [
            row["instructor_name"],
            row["reportable_lessons"],
            row["completed_notes"],
            row["missing_notes"],
            f"{row['average_note_score'] or 0:.2f}",
        ]
        for row in rows
    ]


def _conversion_rows(rows: list[dict]) -> list[list]:
    return [
        [
            row["instructor_name"],
            row["trial_lessons"],
            row["converted_trials"],
            f"{_rate(row['converted_trials'], row['trial_lessons']):.1f}%",
        ]
        for row in rows
    ]


def _funnel_rows(funnel: dict) -> list[list]:
    return [
        ["New leads", funnel.get("new_leads", 0)],
        ["Leads to trial", f"{funnel.get('leads_to_trial', 0)} ({funnel.get('lead_to_trial_rate', 0):.1f}%)"],
        [
            "Trial conversions",
            f"{funnel.get('trials_converted', 0)} ({funnel.get('trial_to_conversion_rate', 0):.1f}%)",
        ],
    ]


def _bucket_rows(response: dict) -> list[list]:
    buckets = response.get("buckets", {})
    return [[bucket, buckets.get(bucket, 0)] for bucket in RESPONSE_BUCKETS]


def _coverage_rows(response: dict) -> list[list]:
    coverage = response.get("coverage", {})
    lead_count = response.get("lead_count", 0)
    return [
        ["Recent customer leads", lead_count],
        ["HubSpot contact rows", response.get("contact_rows", lead_count)],
        [
            "Leads with phone/email identity",
            f"{coverage.get('identity_key_leads', 0)} ({coverage.get('identity_key_rate', 0):.1f}%)",
        ],
        [
            "Any matched communication",
            f"{coverage.get('any_matched_communication', 0)} ({coverage.get('any_matched_communication_rate', 0):.1f}%)",
        ],
        [
            "Same-school matched communication",
            (
                f"{coverage.get('same_school_matched_communication', 0)} "
                f"({coverage.get('same_school_matched_communication_rate', 0):.1f}%)"
            ),
        ],
        [
            "Cross-school/source matched communication",
            (
                f"{coverage.get('cross_school_matched_communication', 0)} "
                f"({coverage.get('cross_school_matched_communication_rate', 0):.1f}%)"
            ),
        ],
        [
            "Pre-lead inbound origin",
            f"{coverage.get('pre_lead_inbound_origin', 0)} ({coverage.get('pre_lead_inbound_origin_rate', 0):.1f}%)",
        ],
        [
            "Post-lead outbound follow-up",
            f"{coverage.get('post_lead_outbound_followup', 0)} ({coverage.get('post_lead_outbound_followup_rate', 0):.1f}%)",
        ],
        ["Matched SMS leads", coverage.get("sms_matched", 0)],
        ["Matched call leads", coverage.get("call_matched", 0)],
        ["Matched email leads", coverage.get("email_matched", 0)],
    ]


def render_operations_dashboard_html(report: dict) -> str:
    totals = report["totals"]
    windows = report["windows"]
    status = report["overall_status"]
    source_counts = report.get("source_freshness", {}).get("counts", {})
    source_rows = [[key, value] for key, value in sorted(source_counts.items())]
    source_flags = report.get("source_data_freshness", {}).get("flags", [])
    source_flag_rows = [[flag] for flag in source_flags] or [["none"]]
    exception_rows = [[key, value] for key, value in report.get("exception_summary", {}).items()]

    def aggregate_blockers(metric: str) -> list[str]:
        blockers = []
        for item in report.get("school_reports", []):
            blockers.extend(_metric_blockers(item, metric))
        return sorted(set(blockers))

    top_leads_card = (
        _blocked_card("MTD Leads", aggregate_blockers("leads"))
        if aggregate_blockers("leads")
        else _metric_card(
            "MTD Leads",
            totals["mtd_new_leads"],
            "trial rate blocked" if aggregate_blockers("trials") else f"{totals['mtd_lead_to_trial_rate']:.1f}% to trial",
        )
    )
    top_trial_card = (
        _blocked_card("MTD Trial Conv.", aggregate_blockers("conversions"))
        if aggregate_blockers("conversions")
        else _metric_card("MTD Trial Conv.", f"{totals['mtd_trial_to_conversion_rate']:.1f}%", f"{totals['mtd_conversions']} conversions")
    )
    top_outbound_card = (
        _blocked_card("Outbound Calls", aggregate_blockers("outbound_calls"))
        if aggregate_blockers("outbound_calls")
        else _metric_card("Outbound Calls", totals["mtd_outbound_calls"], "MTD")
    )
    top_noshow_card = (
        _blocked_card("No-Shows", aggregate_blockers("no_shows"))
        if aggregate_blockers("no_shows")
        else _metric_card("No-Shows", totals["mtd_no_shows"], "MTD")
    )
    top_response_card = (
        _blocked_card("Response Leads", aggregate_blockers("response"))
        if aggregate_blockers("response")
        else _metric_card("Response Leads", totals["response_leads"], f"{totals['response_no_response']} no response")
    )

    school_sections = []
    for item in report["school_reports"]:
        response = item["lead_response"]
        response_average = response.get("average_response_minutes")
        response_detail = (
            f"avg {response_average:.1f} min, {response['no_response']} no response"
            if response_average is not None
            else f"{response['no_response']} no response"
        )
        leads_card = (
            _blocked_card("MTD Leads", _metric_blockers(item, "leads"))
            if not _metric_ready(item, "leads")
            else _metric_card(
                "MTD Leads",
                item["funnel_mtd"]["new_leads"],
                (
                    "trial rate blocked"
                    if not _metric_ready(item, "trials")
                    else f"{item['funnel_mtd']['lead_to_trial_rate']:.1f}% to trial"
                ),
            )
        )
        ytd_leads_card = (
            _blocked_card("YTD Leads", _metric_blockers(item, "leads"))
            if not _metric_ready(item, "leads")
            else _metric_card(
                "YTD Leads",
                item["funnel_ytd"]["new_leads"],
                (
                    "trial rate blocked"
                    if not _metric_ready(item, "trials")
                    else f"{item['funnel_ytd']['lead_to_trial_rate']:.1f}% to trial"
                ),
            )
        )
        trial_card = (
            _blocked_card("MTD Trial Conv.", _metric_blockers(item, "conversions"))
            if not _metric_ready(item, "conversions")
            else _metric_card(
                "MTD Trial Conv.",
                f"{item['funnel_mtd']['trial_to_conversion_rate']:.1f}%",
                f"{item['funnel_mtd']['trials_converted']} conversions",
            )
        )
        outbound_card = (
            _blocked_card("Outbound Calls", _metric_blockers(item, "outbound_calls"))
            if not _metric_ready(item, "outbound_calls")
            else _metric_card("Outbound Calls", item["outbound_calls_mtd"], "MTD")
        )
        noshow_card = (
            _blocked_card("No-Shows", _metric_blockers(item, "no_shows"))
            if not _metric_ready(item, "no_shows")
            else _metric_card("No-Shows", item["no_shows_mtd"], "MTD")
        )
        response_card = (
            _blocked_card("First Response", _metric_blockers(item, "response"))
            if not _metric_ready(item, "response")
            else _metric_card("First Response", response["lead_count"], response_detail)
        )
        notes_mtd_table = (
            _blocked_table("Instructor Notes Ranking MTD", _metric_blockers(item, "notes"))
            if not _metric_ready(item, "notes")
            else _table(["Instructor", "Lessons", "Done", "Missing", "Avg Score"], _notes_rows(item["notes_mtd"]))
        )
        notes_ytd_table = (
            _blocked_table("Instructor Notes Ranking YTD", _metric_blockers(item, "notes"))
            if not _metric_ready(item, "notes")
            else _table(["Instructor", "Lessons", "Done", "Missing", "Avg Score"], _notes_rows(item["notes_ytd"]))
        )
        conversion_table = (
            _blocked_table("Instructor Trial Conversion YTD", _metric_blockers(item, "conversions"))
            if not _metric_ready(item, "conversions")
            else _table(["Instructor", "Trials", "Converted", "Rate"], _conversion_rows(item["conversion_ytd"]))
        )
        mtd_funnel_table = (
            _blocked_table("MTD Funnel trial/conversion metrics", _metric_blockers(item, "trials"))
            if not _metric_ready(item, "trials")
            else _table(["Metric", "Value"], _funnel_rows(item["funnel_mtd"]))
        )
        ytd_funnel_table = (
            _blocked_table("YTD Funnel trial/conversion metrics", _metric_blockers(item, "trials"))
            if not _metric_ready(item, "trials")
            else _table(["Metric", "Value"], _funnel_rows(item["funnel_ytd"]))
        )
        response_table = (
            _blocked_table("Lead To First Response", _metric_blockers(item, "response"))
            if not _metric_ready(item, "response")
            else _table(["Bucket", "Leads"], _bucket_rows(response))
        )
        coverage_table = _table(["Metric", "Value"], _coverage_rows(response))
        lead_created_table = (
            _blocked_table("Lead Created Distribution", _metric_blockers(item, "response"))
            if not _metric_ready(item, "response")
            else _table(["Day / Hour", "Leads"], [[row["lead_created"], row["leads"]] for row in response["lead_created_distribution"]])
        )
        school_sections.append(
            f"""
            <section class="school">
              <div class="section-heading">
                <h2>{_h(item["school"])}</h2>
                <span class="pill attention">Scorecard</span>
              </div>
              {_school_flags(item)}
              <div class="mini-grid">
                {leads_card}
                {trial_card}
                {ytd_leads_card}
                {outbound_card}
                {noshow_card}
                {response_card}
              </div>
              <div class="table-grid">
                <section>
                  <h3>Instructor Notes Ranking MTD</h3>
                  {notes_mtd_table}
                </section>
                <section>
                  <h3>Instructor Notes Ranking YTD</h3>
                  {notes_ytd_table}
                </section>
                <section>
                  <h3>Instructor Trial Conversion YTD</h3>
                  {conversion_table}
                </section>
                <section>
                  <h3>MTD Funnel</h3>
                  {mtd_funnel_table}
                </section>
                <section>
                  <h3>YTD Funnel</h3>
                  {ytd_funnel_table}
                </section>
                <section>
                  <h3>Lead To First Response</h3>
                  {response_table}
                </section>
                <section>
                  <h3>Lead Communication Coverage</h3>
                  {coverage_table}
                </section>
                <section>
                  <h3>Lead Created Distribution</h3>
                  {lead_created_table}
                </section>
                <section>
                  <h3>Open Follow-Up Queue</h3>
                  {_table(["Reason", "Count"], [[key, value] for key, value in item["exceptions"]["summary"].items()])}
                </section>
              </div>
            </section>
            """
        )

    html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>School Operations Scorecard</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f7f8fa;
      --panel: #ffffff;
      --text: #1d2430;
      --muted: #647084;
      --line: #d9dee7;
      --green: #147d4f;
      --amber: #9a5b00;
      --red: #9f1d20;
      --red-bg: #fff0f0;
      --red-line: #f0b8ba;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
      font-size: 14px;
      line-height: 1.45;
    }}
    header {{
      background: #fff;
      border-bottom: 1px solid var(--line);
      padding: 20px 28px;
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 20px;
    }}
    main {{ max-width: 1500px; margin: 0 auto; padding: 24px 28px 40px; }}
    h1, h2, h3, p {{ margin: 0; }}
    h1 {{ font-size: 24px; font-weight: 700; letter-spacing: 0; }}
    h2 {{ font-size: 19px; }}
    h3 {{ font-size: 13px; margin: 18px 0 8px; color: var(--muted); text-transform: uppercase; }}
    .subhead {{ color: var(--muted); margin-top: 4px; }}
    .pill {{
      display: inline-flex;
      min-height: 28px;
      align-items: center;
      padding: 4px 10px;
      border-radius: 4px;
      border: 1px solid var(--line);
      font-weight: 700;
      text-transform: uppercase;
      font-size: 12px;
    }}
    .pill.ready {{ color: var(--green); background: #ecf8f1; border-color: #b8dec7; }}
    .pill.attention {{ color: var(--amber); background: #fff7e5; border-color: #efd08f; }}
    .warning-panel {{
      background: var(--red-bg);
      border: 1px solid var(--red-line);
      border-radius: 6px;
      padding: 18px;
      margin-bottom: 20px;
    }}
    .warning-panel h2 {{ color: var(--red); margin-bottom: 6px; }}
    .warning-panel p {{ max-width: 980px; }}
    .warning-grid {{
      display: grid;
      grid-template-columns: minmax(0, 1.4fr) minmax(280px, .8fr);
      gap: 16px;
      margin-top: 12px;
    }}
    .school-health {{
      margin-top: 12px;
      padding: 10px 12px;
      border-radius: 6px;
      border: 1px solid var(--line);
    }}
    .school-health table {{ margin-top: 8px; }}
    .ready-text {{ color: var(--green); background: #ecf8f1; border-color: #b8dec7; }}
    .attention-text {{ color: var(--red); background: var(--red-bg); border-color: var(--red-line); }}
    .metrics, .mini-grid {{
      display: grid;
      grid-template-columns: repeat(6, minmax(120px, 1fr));
      gap: 12px;
    }}
    .metrics {{ margin-bottom: 20px; }}
    .metric, .school, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 6px;
    }}
    .metric {{ min-height: 102px; padding: 14px; }}
    .metric-label {{ color: var(--muted); font-size: 12px; font-weight: 700; text-transform: uppercase; }}
    .metric-value {{ font-size: 27px; font-weight: 750; margin-top: 6px; }}
    .metric-detail {{ color: var(--muted); margin-top: 4px; min-height: 20px; }}
    .school {{ padding: 18px; margin-top: 18px; }}
    .section-heading {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; }}
    .mini-grid {{ margin-top: 14px; }}
    .table-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }}
    table {{ width: 100%; border-collapse: collapse; table-layout: fixed; background: #fff; }}
    th, td {{
      text-align: left;
      padding: 8px 10px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
      overflow-wrap: anywhere;
    }}
    th {{ color: var(--muted); font-size: 12px; text-transform: uppercase; }}
    .supporting {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 18px; }}
    .panel {{ padding: 18px; }}
    .empty {{ color: var(--muted); padding: 10px 0; }}
    footer {{ color: var(--muted); margin-top: 18px; font-size: 12px; }}
    @media (max-width: 1120px) {{
      .metrics, .mini-grid {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
      .table-grid, .supporting, .warning-grid {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 680px) {{
      header {{ display: block; padding: 16px; }}
      main {{ padding: 16px; }}
      .metrics, .mini-grid {{ grid-template-columns: 1fr; }}
      .pill {{ margin-top: 12px; }}
    }}
  </style>
</head>
<body>
  <header>
    <div>
      <h1>School Operations Scorecard</h1>
      <p class="subhead">As of {_h(report["as_of"])}. MTD {_h(windows["mtd"]["start"])} to {_h(windows["mtd"]["end"])}. YTD {_h(windows["ytd"]["start"])} to {_h(windows["ytd"]["end"])}. Generated {_h(report["generated_at"])}.</p>
    </div>
    <span class="pill {_status_class(status)}">{_h(status)}</span>
  </header>
  <main>
    {_flags_panel(report)}
    <section class="metrics">
      {top_leads_card}
      {top_trial_card}
      {top_outbound_card}
      {top_noshow_card}
      {top_response_card}
      {_metric_card("Open Exceptions", sum(report.get("exception_summary", {}).values()), "YTD follow-up queue")}
    </section>
    {''.join(school_sections)}
    <div class="supporting">
      <section class="panel">
        <h2>Combined Follow-Up Exceptions</h2>
        {_table(["Reason", "Count"], exception_rows)}
      </section>
      <section class="panel">
        <h2>Source Freshness Counts</h2>
        {_table(["Source", "Rows"], source_rows)}
      </section>
      <section class="panel">
        <h2>Source Data Recency Flags</h2>
        {_table(["Flag"], source_flag_rows)}
      </section>
    </div>
    <footer>
      Trial conversion attribution: the instructor who taught the trial gets credit when the same Pike13 person has a non-trial, non-free plan/pass within 30 days of that trial. Note rankings score missing notes as 0 and completed notes by their LLM note score, averaged across reportable lessons.
    </footer>
  </main>
</body>
</html>
"""
    return html_doc


def dashboard_to_json(report: dict) -> str:
    return json.dumps(report, indent=2, sort_keys=True, default=str)


def write_operations_dashboard(report: dict, output_dir: str | Path) -> tuple[Path, Path]:
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    prefix = "school_operations_scorecard"
    html_path = output_root / f"{prefix}.html"
    json_path = output_root / f"{prefix}.json"
    html_path.write_text(render_operations_dashboard_html(report), encoding="utf-8")
    json_path.write_text(dashboard_to_json(report) + "\n", encoding="utf-8")
    return html_path, json_path
