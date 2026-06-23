import hashlib
import json
import re
import sqlite3
from collections import Counter
from datetime import date, datetime, timedelta, timezone

from notesreminder.reports.dashboard_sql import normalize_dashboard_date, register_dashboard_sql_functions
from notesreminder.reports.lead_gap_analysis import build_gap_report
from notesreminder.reports.trial_followup_intelligence import build_trial_followup_report


DEFAULT_SCHOOL = "West U"
DASHBOARD_PERIODS = ("daily", "weekly", "monthly")
FOLLOWUP_RESPONSE_BUCKETS = ("Same / next day", "2-3 days", "4-7 days", "No matched follow-up in 7 days")
FOLLOWUP_ENGAGEMENT_BUCKETS = ("None", "Light", "Active", "Two-way")


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_date(value):
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value))


def parse_event_datetime(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        pass
    normalized = normalize_dashboard_date(text)
    if normalized:
        return datetime.combine(date.fromisoformat(normalized), datetime.min.time())
    return None


def normalize_phone(value):
    if not value:
        return None
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return None
    return digits[-10:] if len(digits) >= 10 else digits


def previous_monday(day):
    return day - timedelta(days=day.weekday())


def window_for_period(period, as_of=None):
    day = parse_date(as_of) if as_of else date.today()
    if period == "daily":
        start = day - timedelta(days=1)
        end = day
    elif period == "weekly":
        this_monday = previous_monday(day)
        start = this_monday - timedelta(days=7)
        end = this_monday - timedelta(days=1)
    elif period == "monthly":
        if day.day == 1:
            end = day - timedelta(days=1)
            start = end.replace(day=1)
        else:
            start = day.replace(day=1)
            end = day
    else:
        raise ValueError(f"Unsupported dashboard period: {period}")
    return start.isoformat(), end.isoformat()


def school_aliases(school):
    normalized = " ".join(str(school or "").strip().lower().split())
    if not normalized or normalized == "all":
        return []
    if normalized in {"west u", "westu", "west university place"}:
        return ["west u", "westu", "west university place"]
    if normalized in {"the heights", "heights"}:
        return ["the heights", "heights"]
    return [normalized]


def school_label_matches(value, school):
    aliases = school_aliases(school)
    if not aliases:
        return True
    normalized = " ".join(str(value or "").strip().lower().split())
    if not normalized:
        return True
    return any(alias == normalized or alias in normalized for alias in aliases)


def hubspot_school(school):
    aliases = school_aliases(school)
    if "west university place" in aliases:
        return "West University Place"
    if "the heights" in aliases:
        return "The Heights"
    return school or ""


def pike13_school(school):
    aliases = school_aliases(school)
    if "west u" in aliases:
        return "West U"
    if "the heights" in aliases:
        return "The Heights"
    return school or ""


def school_clause(alias, school):
    aliases = school_aliases(school)
    if not aliases:
        return "1=1", {}
    params = {f"{alias}_school_{index}": value for index, value in enumerate(aliases)}
    placeholders = ", ".join(f":{key}" for key in params)
    return f"LOWER(COALESCE({alias}.school, '')) IN ({placeholders})", params


def hubspot_contact_school_clause(school, conn=None):
    aliases = school_aliases(school)
    if not aliases:
        return "1=1", {}
    params = {f"contact_school_{index}": value for index, value in enumerate(aliases)}
    exact_placeholders = ", ".join(f":{key}" for key in params)
    like_params = {f"contact_school_like_{index}": f"%{value}%" for index, value in enumerate(aliases)}
    school_expr = "LOWER(COALESCE(c.school, ''))"
    deal_name_expr = "c.hubspot_deal_name"
    if conn is not None:
        school_column = contact_column(conn, "school", fallback="''")
        school_expr = f"LOWER(COALESCE({school_column}, ''))"
        deal_name_expr = contact_column(conn, "hubspot_deal_name", fallback="''")
    like_sql = " OR ".join(
        f"LOWER(COALESCE({deal_name_expr}, '')) LIKE :contact_school_like_{index}"
        for index in range(len(aliases))
    )
    return (
        f"""(
            {school_expr} IN ({exact_placeholders})
            OR LOWER(COALESCE(pp.school, '')) IN ({exact_placeholders})
            OR {like_sql}
        )""",
        {**params, **like_params},
    )


def table_exists(conn, name):
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (name,),
        ).fetchone()
    )


def column_exists(conn, table, column):
    if not table_exists(conn, table):
        return False
    return any(row[1] == column for row in conn.execute(f"PRAGMA table_info({table})").fetchall())


def contact_column(conn, column, alias="c", fallback="NULL"):
    return f"{alias}.{column}" if column_exists(conn, "hubspot_contacts", column) else fallback


def contact_date_expr(conn, alias="c"):
    if column_exists(conn, "hubspot_contacts", "create_date"):
        return f"dashboard_date(NULLIF({alias}.create_date, ''), NULLIF({alias}.updated_at, ''))"
    return f"dashboard_date(NULLIF({alias}.updated_at, ''))"


def contact_person_expr(conn, alias="c"):
    parts = []
    if column_exists(conn, "hubspot_contacts", "pike13_person_id"):
        parts.append(f"NULLIF({alias}.pike13_person_id, '')")
    if column_exists(conn, "hubspot_contacts", "person_id"):
        parts.append(f"NULLIF({alias}.person_id, '')")
    if not parts:
        return "NULL"
    if len(parts) == 1:
        return parts[0]
    return "COALESCE(" + ", ".join(parts) + ")"


def scalar(conn, sql, params=None):
    row = conn.execute(sql, params or {}).fetchone()
    return row[0] if row else 0


def count_table(conn, table, where="1=1", params=None):
    if not table_exists(conn, table):
        return 0
    return int(scalar(conn, f"SELECT COUNT(*) FROM {table} WHERE {where}", params) or 0)


def count_by_date(conn, table, date_field, start_date, end_date, school="", alias="t", extra_where="1=1"):
    if not table_exists(conn, table):
        return 0
    school_sql, school_params = school_clause(alias, school)
    params = {"start": start_date, "end": end_date, **school_params}
    return int(
        scalar(
            conn,
            f"""
            SELECT COUNT(*)
            FROM {table} {alias}
            WHERE date({alias}.{date_field}) BETWEEN date(:start) AND date(:end)
              AND {school_sql}
              AND {extra_where}
            """,
            params,
        )
        or 0
    )


def count_view_by_date(conn, view, date_field, start_date, end_date, school="", extra_where="1=1"):
    if not table_exists(conn, view):
        return 0
    school_sql, school_params = school_clause("v", school)
    params = {"start": start_date, "end": end_date, **school_params}
    return int(
        scalar(
            conn,
            f"""
            SELECT COUNT(*)
            FROM {view} v
            WHERE date(v.{date_field}) BETWEEN date(:start) AND date(:end)
              AND {school_sql}
              AND {extra_where}
            """,
            params,
        )
        or 0
    )


def source_freshness(conn):
    if not table_exists(conn, "source_import_runs"):
        return {"status": "missing", "runs": [], "counts": {}}
    rows = conn.execute(
        """
        SELECT source, extractor, status, started_at, finished_at, rows_seen, rows_inserted, rows_updated, error
        FROM source_import_runs r
        WHERE id = (
            SELECT id
            FROM source_import_runs latest
            WHERE latest.source = r.source
              AND COALESCE(latest.extractor, '') = COALESCE(r.extractor, '')
            ORDER BY COALESCE(latest.finished_at, latest.started_at) DESC, latest.id DESC
            LIMIT 1
        )
        ORDER BY source, extractor
        """
    ).fetchall()
    counts = {
        "hubspot_deals": count_table(conn, "hubspot_deals"),
        "hubspot_contacts": count_table(conn, "hubspot_contacts"),
        "pike13_visits": count_table(conn, "pike13_visits"),
        "dialpad_voice_events": count_table(conn, "dialpad_voice_events"),
        "dialpad_sms_messages": count_table(conn, "dialpad_sms_messages"),
        "school_email_messages": count_table(conn, "school_email_messages"),
        "recording_downloads": count_table(conn, "recording_downloads"),
        "recording_transcripts": count_table(conn, "recording_transcripts"),
    }
    status = "ready" if rows else "missing"
    if any(row["status"] not in {"success", "completed"} for row in rows):
        status = "attention"
    if any(row["source"] == "hubspot" and int(row["rows_seen"] or 0) == 0 for row in rows):
        status = "attention"
    return {"status": status, "runs": [dict(row) for row in rows], "counts": counts}


def max_dashboard_date(conn, table, expression):
    if not table_exists(conn, table):
        return None
    try:
        return scalar(conn, f"SELECT MAX(dashboard_date({expression})) FROM {table}")
    except sqlite3.OperationalError:
        return None


def source_data_freshness(conn, end_date, school):
    latest = {
        "lessons": max_dashboard_date(conn, "lessons", "lesson_date"),
        "hubspot_contacts": (
            max_dashboard_date(conn, "hubspot_contacts", contact_date_expr(conn, "hubspot_contacts"))
            if table_exists(conn, "hubspot_contacts")
            else None
        ),
        "pike13_visits": max_dashboard_date(conn, "pike13_visits", "starts_at"),
        "school_email": max_dashboard_date(conn, "school_email_messages", "message_at"),
    }
    if table_exists(conn, "vw_dialpad_communications"):
        school_sql, school_params = school_clause("v", school)
        latest["dialpad_calls"] = scalar(
            conn,
            f"""
            SELECT MAX(dashboard_date(v.event_at))
            FROM vw_dialpad_communications v
            WHERE v.channel = 'call'
              AND {school_sql}
            """,
            school_params,
        )
        latest["dialpad_sms"] = scalar(
            conn,
            f"""
            SELECT MAX(dashboard_date(v.event_at))
            FROM vw_dialpad_communications v
            WHERE v.channel = 'sms'
              AND {school_sql}
            """,
            school_params,
        )
    else:
        latest["dialpad_calls"] = None
        latest["dialpad_sms"] = None
    flags = []
    for source, latest_date in latest.items():
        if not latest_date:
            flags.append(f"missing_{source}_data")
        elif latest_date < end_date:
            flags.append(f"stale_{source}_data_latest_{latest_date}")
    if table_exists(conn, "vw_dialpad_communications"):
        school_sql, school_params = school_clause("v", school)
        sms_rows = int(
            scalar(
                conn,
                f"""
                SELECT COUNT(*)
                FROM vw_dialpad_communications v
                WHERE v.channel = 'sms'
                  AND {school_sql}
                """,
                school_params,
            )
            or 0
        )
        latest["school_sms_rows"] = sms_rows
        if sms_rows == 0:
            flags.append("missing_school_sms_data")
    status = "ready" if not flags else "attention"
    return {"status": status, "latest_dates": latest, "flags": flags}


def metric_status_from_freshness(data_freshness):
    flags = list(data_freshness.get("flags", []))

    def blocked_by(*prefixes):
        blockers = [
            flag
            for flag in flags
            if any(flag.startswith(prefix) for prefix in prefixes)
        ]
        return {
            "status": "blocked" if blockers else "ready",
            "blockers": blockers,
        }

    communications = blocked_by(
        "missing_school_email",
        "stale_school_email",
        "missing_dialpad_calls",
        "stale_dialpad_calls",
        "missing_dialpad_sms",
        "stale_dialpad_sms",
        "missing_school_sms",
    )
    trials = blocked_by("missing_pike13_visits", "stale_pike13_visits")
    return {
        "leads": blocked_by("missing_hubspot_contacts", "stale_hubspot_contacts"),
        "contacted": communications,
        "communications": communications,
        "pareto": communications,
        "response": communications,
        "trials": trials,
        "conversions": trials,
        "notes": blocked_by("missing_lessons", "stale_lessons"),
    }


def pike13_outcomes(conn, start_date, end_date, school):
    if not table_exists(conn, "pike13_visits"):
        return {}
    base = """
        FROM pike13_visits v
        LEFT JOIN pike13_people p ON p.person_id = v.person_id
        WHERE date(v.starts_at) BETWEEN date(:start) AND date(:end)
          AND {school_sql}
    """
    school_sql, school_params = school_clause("v", school)
    if school_params:
        people_sql, people_params = school_clause("p", school)
        school_sql = f"({school_sql} OR {people_sql})"
        school_params = {**school_params, **people_params}
    params = {"start": start_date, "end": end_date, **school_params}
    def c(extra):
        return int(scalar(conn, f"SELECT COUNT(*) {base.format(school_sql=school_sql)} AND ({extra})", params) or 0)
    return {
        "scheduled": c("LOWER(COALESCE(v.status, '')) IN ('enrolled', 'scheduled', '')"),
        "attended": c(
            "COALESCE(v.attendance_confirmed_flag, 0) = 1 OR COALESCE(v.checked_in_flag, 0) = 1 "
            "OR LOWER(COALESCE(v.status, '')) LIKE '%complete%'"
        ),
        "canceled": c("COALESCE(v.canceled_flag, 0) = 1 OR LOWER(COALESCE(v.status, '')) LIKE '%cancel%'"),
        "no_show": c("COALESCE(v.no_show_flag, 0) = 1 OR LOWER(COALESCE(v.status, '')) LIKE '%no show%'"),
        "first_visits": c("COALESCE(v.first_visit_flag, 0) = 1 OR LOWER(COALESCE(v.service, '')) LIKE '%trial%'"),
        "terms_not_accepted": c("COALESCE(v.terms_accepted_flag, 1) = 0"),
    }


def conversion_count(conn, start_date, end_date, school):
    if not table_exists(conn, "pike13_plans_passes"):
        return 0
    school_sql, school_params = school_clause("pp", school)
    params = {"start": start_date, "end": end_date, **school_params}
    return int(
        scalar(
            conn,
            """
        SELECT COUNT(*)
        FROM pike13_plans_passes pp
        WHERE
        date(COALESCE(NULLIF(pp.starts_at, ''), NULLIF(pp.next_invoice_at, ''), pp.updated_at)) BETWEEN date(:start) AND date(:end)
        AND {school_sql}
        AND LOWER(COALESCE(pp.name, '')) NOT LIKE '%trial%'
        AND LOWER(COALESCE(pp.name, '')) NOT LIKE '%free%'
        """.format(school_sql=school_sql),
            params,
        )
        or 0
    )


def trial_cohort_conversion_count(conn, start_date, end_date, school):
    if not table_exists(conn, "pike13_visits") or not table_exists(conn, "pike13_plans_passes"):
        return 0
    school_sql, school_params = school_clause("v", school)
    params = {"start": start_date, "end": end_date, **school_params}
    return int(
        scalar(
            conn,
            """
        SELECT COUNT(DISTINCT v.visit_id)
        FROM pike13_visits v
        WHERE date(v.starts_at) BETWEEN date(:start) AND date(:end)
        AND {school_sql}
        AND (COALESCE(v.first_visit_flag, 0) = 1 OR LOWER(COALESCE(v.service, '')) LIKE '%trial%')
        AND EXISTS (
            SELECT 1
            FROM pike13_plans_passes pp
            WHERE pp.person_id = v.person_id
              AND LOWER(COALESCE(pp.name, '')) NOT LIKE '%trial%'
              AND LOWER(COALESCE(pp.name, '')) NOT LIKE '%free%'
              AND date(COALESCE(NULLIF(pp.starts_at, ''), NULLIF(pp.next_invoice_at, ''), pp.updated_at))
                  BETWEEN date(:start) AND date(v.starts_at, '+30 day')
        )
        """.format(school_sql=school_sql),
            params,
        )
        or 0
    )


def hubspot_lead_count(conn, start_date, end_date, school):
    if table_exists(conn, "hubspot_contacts"):
        school_sql, school_params = hubspot_contact_school_clause(school, conn)
        lead_date_sql = contact_date_expr(conn)
        person_sql = contact_person_expr(conn)
        params = {"start": start_date, "end": end_date, **school_params}
        count = int(
            scalar(
                conn,
                f"""
                SELECT COUNT(*)
                FROM hubspot_contacts c
                LEFT JOIN pike13_people pp ON pp.person_id = {person_sql}
                WHERE {lead_date_sql}
                    BETWEEN dashboard_date(:start) AND dashboard_date(:end)
                  AND {school_sql}
                """,
                params,
            )
            or 0
        )
        if count:
            return count
    if not table_exists(conn, "hubspot_deals"):
        return 0
    school_sql, school_params = school_clause("d", school)
    params = {"start": start_date, "end": end_date, **school_params}
    return int(
        scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM hubspot_deals d
            WHERE dashboard_date(NULLIF(d.create_date, ''), NULLIF(d.updated_at, ''))
                BETWEEN dashboard_date(:start) AND dashboard_date(:end)
              AND {school_sql}
            """.format(school_sql=school_sql),
            params,
        )
        or 0
    )


def communication_counts(conn, start_date, end_date, school):
    return {
        "dialpad_calls": count_view_by_date(
            conn,
            "vw_dialpad_communications",
            "event_at",
            start_date,
            end_date,
            school,
            "v.channel = 'call'",
        ),
        "dialpad_sms": count_view_by_date(
            conn,
            "vw_dialpad_communications",
            "event_at",
            start_date,
            end_date,
            school,
            "v.channel = 'sms'",
        ),
        "school_email": count_view_by_date(
            conn,
            "vw_school_email_communications",
            "event_at",
            start_date,
            end_date,
            school,
        ),
    }


def _followup_response_bucket(first_outbound_date, lead_date):
    if not first_outbound_date:
        return "No matched follow-up in 7 days"
    if isinstance(first_outbound_date, datetime):
        first_outbound_date = first_outbound_date.date()
    days = (first_outbound_date - lead_date).days
    if days <= 1:
        return "Same / next day"
    if days <= 3:
        return "2-3 days"
    if days <= 7:
        return "4-7 days"
    return "No matched follow-up in 7 days"


def _followup_engagement_bucket(outbound_count, inbound_count):
    if outbound_count > 0 and inbound_count > 0:
        return "Two-way"
    if outbound_count >= 2:
        return "Active"
    if outbound_count == 1:
        return "Light"
    return "None"


def _trial_rate_cell(leads=0, trials=0):
    rate = round(trials / leads, 4) if leads else None
    return {"leads": leads, "trials": trials, "trial_rate": rate}


def _rate(numerator=0, denominator=0):
    return round(numerator / denominator, 4) if denominator else None


def _is_outbound(direction):
    normalized = str(direction or "").strip().lower()
    return normalized in {"outbound", "sent"} or "out" in normalized


def _is_inbound(direction):
    normalized = str(direction or "").strip().lower()
    return normalized in {"inbound", "received"} or "in" in normalized


def _lead_followup_rows(conn, start_date, end_date, school):
    if not table_exists(conn, "hubspot_contacts"):
        return []
    school_sql, school_params = hubspot_contact_school_clause(school, conn)
    lead_date_sql = contact_date_expr(conn)
    person_sql = contact_person_expr(conn)
    pike13_person_sql = contact_column(conn, "pike13_person_id")
    person_id_sql = contact_column(conn, "person_id")
    return conn.execute(
        f"""
        SELECT
            c.contact_id,
            {lead_date_sql} AS lead_date,
            c.email_normalized AS contact_email,
            c.phone_normalized AS contact_phone,
            {pike13_person_sql} AS pike13_person_id,
            {person_id_sql} AS person_id,
            pp.email_normalized AS pike13_email,
            pp.phone_normalized AS pike13_phone
        FROM hubspot_contacts c
        LEFT JOIN pike13_people pp
          ON pp.person_id = {person_sql}
        WHERE {lead_date_sql}
            BETWEEN dashboard_date(:start) AND dashboard_date(:end)
          AND {school_sql}
        """,
        {"start": start_date, "end": end_date, **school_params},
    ).fetchall()


def _lead_trial_dates(conn):
    if not table_exists(conn, "pike13_visits"):
        return {}
    trial_dates = {}
    rows = conn.execute(
        """
        SELECT person_id, dashboard_date(NULLIF(starts_at, ''), NULLIF(updated_at, '')) AS trial_date
        FROM pike13_visits
        WHERE COALESCE(person_id, '') != ''
          AND (COALESCE(first_visit_flag, 0) = 1 OR LOWER(COALESCE(service, '')) LIKE '%trial%')
        """
    ).fetchall()
    for row in rows:
        if not row["trial_date"]:
            continue
        trial_dates.setdefault(row["person_id"], []).append(parse_date(row["trial_date"]))
    return trial_dates


def _lead_communications_by_identity(conn):
    by_email = {}
    by_phone = {}
    if table_exists(conn, "vw_school_email_communications"):
        for row in conn.execute(
            """
            SELECT communication_id, direction, event_at, external_email_normalized, school
            FROM vw_school_email_communications
            WHERE COALESCE(external_email_normalized, '') != ''
              AND COALESCE(event_at, '') != ''
            """
        ).fetchall():
            event_at = parse_event_datetime(row["event_at"])
            if not event_at:
                continue
            identity = str(row["external_email_normalized"]).strip().lower()
            by_email.setdefault(identity, []).append(
                {
                    "id": f"email:{row['communication_id']}",
                    "channel": "email",
                    "direction": row["direction"],
                    "event_at": event_at,
                    "school": row["school"],
                }
            )
    if table_exists(conn, "vw_dialpad_communications"):
        for row in conn.execute(
            """
            SELECT communication_id, channel, direction, event_at, phone_normalized, school
            FROM vw_dialpad_communications
            WHERE COALESCE(phone_normalized, '') != ''
              AND COALESCE(event_at, '') != ''
            """
        ).fetchall():
            event_at = parse_event_datetime(row["event_at"])
            phone = normalize_phone(row["phone_normalized"])
            if not event_at or not phone:
                continue
            by_phone.setdefault(phone, []).append(
                {
                    "id": f"dialpad:{row['communication_id']}",
                    "channel": row["channel"],
                    "direction": row["direction"],
                    "event_at": event_at,
                    "school": row["school"],
                }
            )
    return by_email, by_phone


def lead_followup_pareto_grid(conn, start_date, end_date, school):
    rows = _lead_followup_rows(conn, start_date, end_date, school)
    trial_dates = _lead_trial_dates(conn)
    communications_by_email, communications_by_phone = _lead_communications_by_identity(conn)
    cells = {
        (response, engagement): {"leads": 0, "trials": 0}
        for response in FOLLOWUP_RESPONSE_BUCKETS
        for engagement in FOLLOWUP_ENGAGEMENT_BUCKETS
    }
    coverage = {
        "leads": 0,
        "trials": 0,
        "matched_communication_leads": 0,
        "communication_7d_leads": 0,
        "outbound_7d_leads": 0,
        "inbound_7d_leads": 0,
        "pre_lead_inbound_origin_leads": 0,
        "sms_7d_leads": 0,
        "outbound_sms_7d_leads": 0,
        "inbound_sms_7d_leads": 0,
        "pre_lead_inbound_sms_leads": 0,
        "pre_lead_inbound_call_leads": 0,
        "call_7d_leads": 0,
        "outbound_call_7d_leads": 0,
        "email_7d_leads": 0,
        "outbound_email_7d_leads": 0,
    }
    for row in rows:
        lead_date = parse_date(row["lead_date"])
        if not lead_date:
            continue
        emails = {
            str(value).strip().lower()
            for value in (row["contact_email"], row["pike13_email"])
            if value and str(value).strip()
        }
        phones = {
            phone
            for phone in (normalize_phone(row["contact_phone"]), normalize_phone(row["pike13_phone"]))
            if phone
        }
        communications = {}
        for email in emails:
            for communication in communications_by_email.get(email, []):
                communications[communication["id"]] = communication
        for phone in phones:
            for communication in communications_by_phone.get(phone, []):
                communications[communication["id"]] = communication
        communications = {
            communication_id: communication
            for communication_id, communication in communications.items()
            if school_label_matches(communication.get("school"), school)
        }

        window_end = lead_date + timedelta(days=7)
        window_communications = [
            communication
            for communication in communications.values()
            if lead_date <= communication["event_at"].date() <= window_end
        ]
        pre_lead_communications = [
            communication
            for communication in communications.values()
            if communication["event_at"].date() < lead_date
        ]
        outbound = [communication for communication in window_communications if _is_outbound(communication["direction"])]
        inbound = [communication for communication in window_communications if _is_inbound(communication["direction"])]
        pre_lead_inbound = [communication for communication in pre_lead_communications if _is_inbound(communication["direction"])]
        sms = [communication for communication in window_communications if communication.get("channel") == "sms"]
        outbound_sms = [communication for communication in sms if _is_outbound(communication["direction"])]
        inbound_sms = [communication for communication in sms if _is_inbound(communication["direction"])]
        pre_lead_inbound_sms = [communication for communication in pre_lead_inbound if communication.get("channel") == "sms"]
        pre_lead_inbound_calls = [communication for communication in pre_lead_inbound if communication.get("channel") == "call"]
        calls = [communication for communication in window_communications if communication.get("channel") == "call"]
        outbound_calls = [communication for communication in calls if _is_outbound(communication["direction"])]
        emails = [communication for communication in window_communications if communication.get("channel") == "email"]
        outbound_emails = [communication for communication in emails if _is_outbound(communication["direction"])]
        first_outbound_date = min((communication["event_at"].date() for communication in outbound), default=None)
        response_bucket = _followup_response_bucket(first_outbound_date, lead_date)
        engagement_bucket = _followup_engagement_bucket(len(outbound), len(inbound))
        person_id = row["pike13_person_id"] or row["person_id"]
        trial_scheduled = bool(
            person_id
            and any(trial_date >= lead_date for trial_date in trial_dates.get(person_id, []))
        )

        cells[(response_bucket, engagement_bucket)]["leads"] += 1
        cells[(response_bucket, engagement_bucket)]["trials"] += 1 if trial_scheduled else 0
        coverage["leads"] += 1
        coverage["trials"] += 1 if trial_scheduled else 0
        coverage["matched_communication_leads"] += 1 if communications else 0
        coverage["communication_7d_leads"] += 1 if window_communications else 0
        coverage["outbound_7d_leads"] += 1 if outbound else 0
        coverage["inbound_7d_leads"] += 1 if inbound else 0
        coverage["pre_lead_inbound_origin_leads"] += 1 if pre_lead_inbound else 0
        coverage["sms_7d_leads"] += 1 if sms else 0
        coverage["outbound_sms_7d_leads"] += 1 if outbound_sms else 0
        coverage["inbound_sms_7d_leads"] += 1 if inbound_sms else 0
        coverage["pre_lead_inbound_sms_leads"] += 1 if pre_lead_inbound_sms else 0
        coverage["pre_lead_inbound_call_leads"] += 1 if pre_lead_inbound_calls else 0
        coverage["call_7d_leads"] += 1 if calls else 0
        coverage["outbound_call_7d_leads"] += 1 if outbound_calls else 0
        coverage["email_7d_leads"] += 1 if emails else 0
        coverage["outbound_email_7d_leads"] += 1 if outbound_emails else 0

    grid = []
    for response in FOLLOWUP_RESPONSE_BUCKETS:
        row_cells = {}
        row_leads = 0
        row_trials = 0
        for engagement in FOLLOWUP_ENGAGEMENT_BUCKETS:
            cell = cells[(response, engagement)]
            row_cells[engagement] = _trial_rate_cell(cell["leads"], cell["trials"])
            row_leads += cell["leads"]
            row_trials += cell["trials"]
        grid.append(
            {
                "response_time": response,
                "cells": row_cells,
                "row_total": _trial_rate_cell(row_leads, row_trials),
            }
        )
    column_totals = {}
    for engagement in FOLLOWUP_ENGAGEMENT_BUCKETS:
        leads = sum(cells[(response, engagement)]["leads"] for response in FOLLOWUP_RESPONSE_BUCKETS)
        trials = sum(cells[(response, engagement)]["trials"] for response in FOLLOWUP_RESPONSE_BUCKETS)
        column_totals[engagement] = _trial_rate_cell(leads, trials)
    coverage["communication_coverage_rate"] = round(coverage["matched_communication_leads"] / coverage["leads"], 4) if coverage["leads"] else None
    coverage["communication_7d_rate"] = round(coverage["communication_7d_leads"] / coverage["leads"], 4) if coverage["leads"] else None
    coverage["outbound_7d_rate"] = round(coverage["outbound_7d_leads"] / coverage["leads"], 4) if coverage["leads"] else None
    coverage["pre_lead_inbound_origin_rate"] = round(coverage["pre_lead_inbound_origin_leads"] / coverage["leads"], 4) if coverage["leads"] else None
    coverage["sms_7d_rate"] = round(coverage["sms_7d_leads"] / coverage["leads"], 4) if coverage["leads"] else None
    coverage["outbound_sms_7d_rate"] = round(coverage["outbound_sms_7d_leads"] / coverage["leads"], 4) if coverage["leads"] else None
    coverage["outbound_call_7d_rate"] = round(coverage.get("outbound_call_7d_leads", 0) / coverage["leads"], 4) if coverage["leads"] else None
    coverage["outbound_email_7d_rate"] = round(coverage.get("outbound_email_7d_leads", 0) / coverage["leads"], 4) if coverage["leads"] else None
    coverage["lead_to_trial_rate"] = round(coverage["trials"] / coverage["leads"], 4) if coverage["leads"] else None
    data_quality_flags = []
    blockers = []
    if not coverage["leads"]:
        data_quality_flags.append("no_contact_spine_leads_for_school_window")
        blockers.append("no_contact_spine_leads_for_school_window")
    if coverage["leads"] and coverage["communication_coverage_rate"] is not None and coverage["communication_coverage_rate"] < 0.5:
        data_quality_flags.append("low_matched_communication_coverage")
    if coverage["leads"] and (coverage["communication_coverage_rate"] or 0) < 0.1:
        blockers.append("matched_communication_coverage_below_10pct")
    if coverage["leads"] and coverage["communication_7d_leads"] == 0:
        data_quality_flags.append("no_matched_communication_7d")
    if coverage["leads"] and coverage["outbound_7d_leads"] == 0:
        data_quality_flags.append("no_matched_outbound_followup_7d")
        blockers.append("no_matched_outbound_followup_7d")
    grid_status = "ready"
    if blockers:
        grid_status = "blocked"
    elif data_quality_flags:
        grid_status = "attention"
    return {
        "response_buckets": list(FOLLOWUP_RESPONSE_BUCKETS),
        "engagement_buckets": list(FOLLOWUP_ENGAGEMENT_BUCKETS),
        "coverage": coverage,
        "data_quality_flags": data_quality_flags,
        "grid_status": grid_status,
        "blockers": blockers,
        "recommended_action": (
            "Backfill HubSpot contact lead spine for this school/window before using this grid for performance judgment."
            if "no_contact_spine_leads_for_school_window" in blockers
            else "Run targeted Dialpad/email backfill and matching before using this grid for performance judgment."
            if blockers
            else "Review coverage flags before using this grid for performance judgment."
            if data_quality_flags
            else "Grid is ready for directional performance review."
        ),
        "grid": grid,
        "column_totals": column_totals,
        "overall_total": _trial_rate_cell(coverage["leads"], coverage["trials"]),
        "cell_format": "leads / lead-to-trial rate",
    }


def notes_operations(conn, start_date, end_date, school):
    if not table_exists(conn, "lessons") or not table_exists(conn, "lesson_notes"):
        return {
            "reportable_lessons": 0,
            "completed_notes": 0,
            "missing_notes": 0,
            "completion_rate": 0.0,
            "league_score": 0.0,
        }
    aliases = school_aliases(school)
    if aliases:
        school_params = {f"school_{index}": value for index, value in enumerate(aliases)}
        placeholders = ", ".join(f":{key}" for key in school_params)
        school_sql = (
            f"(LOWER(COALESCE(s.school_code, '')) IN ({placeholders}) "
            f"OR LOWER(COALESCE(s.school_name, '')) IN ({placeholders}))"
        )
    else:
        school_params = {}
        school_sql = "1=1"
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS reportable_lessons,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END) AS completed_notes,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 0 THEN 1 ELSE 0 END) AS missing_notes,
            ROUND(
                100.0 * SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
                1
            ) AS completion_rate,
            ROUND(
                100.0 * SUM(CASE WHEN n.note_score IS NOT NULL THEN n.note_score / 10.0 ELSE 0 END) / NULLIF(COUNT(*), 0),
                1
            ) AS league_score
        FROM lessons l
        JOIN schools s ON s.school_id = l.school_id
        LEFT JOIN lesson_notes n ON n.lesson_id = l.lesson_id
        WHERE date(l.lesson_date) BETWEEN date(:start) AND date(:end)
          AND {school_sql}
          AND COALESCE(l.lesson_is_reportable, 0) = 1
        """,
        {"start": start_date, "end": end_date, **school_params},
    ).fetchone()
    return {
        "reportable_lessons": row["reportable_lessons"] or 0,
        "completed_notes": row["completed_notes"] or 0,
        "missing_notes": row["missing_notes"] or 0,
        "completion_rate": row["completion_rate"] or 0.0,
        "league_score": row["league_score"] or 0.0,
    }


def recording_status(conn, start_date, end_date, school):
    if not table_exists(conn, "recording_downloads"):
        return {"downloads": {}, "transcription_queue": {}}
    school_sql, school_params = school_clause("r", school)
    rows = conn.execute(
        f"""
        SELECT COALESCE(status, 'unknown') AS status, COUNT(*) AS rows
        FROM recording_downloads r
        WHERE date(COALESCE(NULLIF(event_at, ''), downloaded_at, updated_at)) BETWEEN date(:start) AND date(:end)
          AND {school_sql}
        GROUP BY COALESCE(status, 'unknown')
        ORDER BY status
        """,
        {"start": start_date, "end": end_date, **school_params},
    ).fetchall()
    transcript_rows = []
    if table_exists(conn, "recording_transcripts"):
        transcript_rows = conn.execute(
            """
            SELECT COALESCE(transcript_status, 'unknown') AS status, COUNT(*) AS rows
            FROM recording_transcripts
            GROUP BY COALESCE(transcript_status, 'unknown')
            ORDER BY status
            """
        ).fetchall()
    return {
        "downloads": {row["status"]: row["rows"] for row in rows},
        "transcription_queue": {row["status"]: row["rows"] for row in transcript_rows},
    }


def top_counts(conn, sql, params=None):
    return [dict(row) for row in conn.execute(sql, params or {}).fetchall()]


def performance_sections(conn, start_date, end_date, school):
    register_dashboard_sql_functions(conn)
    staff = []
    if table_exists(conn, "pike13_visits"):
        school_sql, school_params = school_clause("v", school)
        staff = top_counts(
            conn,
            f"""
            SELECT COALESCE(NULLIF(instructor, ''), 'unknown') AS staff, COUNT(*) AS trials
            FROM pike13_visits v
            WHERE date(starts_at) BETWEEN date(:start) AND date(:end)
              AND {school_sql}
              AND (COALESCE(first_visit_flag, 0) = 1 OR LOWER(COALESCE(service, '')) LIKE '%trial%')
            GROUP BY COALESCE(NULLIF(instructor, ''), 'unknown')
            ORDER BY trials DESC, staff
            LIMIT 10
            """,
            {"start": start_date, "end": end_date, **school_params},
        )
    sources = []
    if table_exists(conn, "hubspot_contacts"):
        school_sql, school_params = hubspot_contact_school_clause(school, conn)
        lead_date_sql = contact_date_expr(conn)
        person_sql = contact_person_expr(conn)
        lead_source_sql = contact_column(conn, "lead_source", fallback="''")
        marketing_source_sql = contact_column(conn, "marketing_source", fallback="''")
        record_source_sql = contact_column(conn, "record_source_detail", fallback="''")
        source_expr = (
            f"COALESCE(NULLIF({lead_source_sql}, ''), "
            f"NULLIF({marketing_source_sql}, ''), "
            f"NULLIF({record_source_sql}, ''), "
            "'unknown')"
        )
        sources = top_counts(
            conn,
            f"""
            SELECT {source_expr} AS source, COUNT(*) AS leads
            FROM hubspot_contacts c
            LEFT JOIN pike13_people pp ON pp.person_id = {person_sql}
            WHERE {lead_date_sql}
                BETWEEN dashboard_date(:start) AND dashboard_date(:end)
              AND {school_sql}
            GROUP BY {source_expr}
            ORDER BY leads DESC, source
            LIMIT 10
            """,
            {"start": start_date, "end": end_date, **school_params},
        )
    if not sources and table_exists(conn, "hubspot_deals"):
        school_sql, school_params = school_clause("d", school)
        sources = top_counts(
            conn,
            f"""
            SELECT COALESCE(NULLIF(lead_source, ''), NULLIF(marketing_source, ''), 'unknown') AS source, COUNT(*) AS leads
            FROM hubspot_deals d
            WHERE dashboard_date(NULLIF(create_date, ''), NULLIF(updated_at, ''))
                BETWEEN dashboard_date(:start) AND dashboard_date(:end)
              AND {school_sql}
            GROUP BY COALESCE(NULLIF(lead_source, ''), NULLIF(marketing_source, ''), 'unknown')
            ORDER BY leads DESC, source
            LIMIT 10
            """,
            {"start": start_date, "end": end_date, **school_params},
        )
    return {"staff_trial_counts": staff, "hubspot_source_counts": sources}


def customer_group_key(name):
    normalized = " ".join(str(name or "").strip().lower().split())
    return normalized or "unknown customer"


def group_exception_items_by_customer(items):
    groups = {}
    for item in items:
        key = customer_group_key(item.get("customer_name"))
        group = groups.setdefault(
            key,
            {
                "customer_name": item.get("customer_name") or "Unknown customer",
                "refs": [],
                "items": [],
                "reason_codes": [],
            },
        )
        if item.get("lead_ref") and item["lead_ref"] not in group["refs"]:
            group["refs"].append(item["lead_ref"])
        if item.get("reason_code") and item["reason_code"] not in group["reason_codes"]:
            group["reason_codes"].append(item["reason_code"])
        group["items"].append(item)
    return sorted(
        groups.values(),
        key=lambda group: (len(group["items"]) * -1, str(group["customer_name"]).lower()),
    )


def build_exception_queue(conn, start_date, end_date, school=DEFAULT_SCHOOL, limit=50):
    gap = build_gap_report(
        conn,
        school=hubspot_school(school),
        limit=max(limit, 50),
        start_date=start_date,
        end_date=end_date,
    )
    trial = build_trial_followup_report(conn, start_date, end_date, pike13_school(school))
    items = []
    for row in gap["rows"]:
        if row["gap_category"] in {"ready_for_review", "hubspot_only_with_outreach", "excluded_stage"}:
            continue
        items.append(
            {
                "exception_type": row["gap_category"],
                "diagnostic_area": row["diagnostic_area"],
                "lead_ref": row["lead_ref"],
                "customer_name": row.get("customer_name") or "Unknown customer",
                "school": row["school"],
                "stage": row["stage"],
                "reason_code": row["gap_category"],
            }
        )
    for row in trial["rows"]:
        if row["followup_status"] in {"outreach_found", "post_no_show_followup_found"}:
            continue
        items.append(
            {
                "exception_type": "trial_followup",
                "diagnostic_area": "communication",
                "lead_ref": row["trial_ref"],
                "customer_name": row.get("customer_name") or "Unknown customer",
                "school": row["school"],
                "stage": row["outcome"],
                "reason_code": row["followup_status"],
            }
        )
    customer_groups = group_exception_items_by_customer(items)
    return {
        "summary": dict(sorted(Counter(item["reason_code"] for item in items).items())),
        "items": items[:limit],
        "customer_groups": customer_groups[:limit],
        "truncated": len(items) > limit,
    }


def build_snapshot(conn, period, start_date=None, end_date=None, as_of=None, school=DEFAULT_SCHOOL, limit=50):
    if period not in DASHBOARD_PERIODS:
        raise ValueError(f"period must be one of: {', '.join(DASHBOARD_PERIODS)}")
    if not start_date or not end_date:
        start_date, end_date = window_for_period(period, as_of)
    conn.row_factory = sqlite3.Row
    register_dashboard_sql_functions(conn)
    gap = build_gap_report(conn, hubspot_school(school), limit=500, start_date=start_date, end_date=end_date)
    trial = build_trial_followup_report(conn, start_date, end_date, pike13_school(school))
    pike13 = pike13_outcomes(conn, start_date, end_date, school)
    communications = communication_counts(conn, start_date, end_date, school)
    notes = notes_operations(conn, start_date, end_date, school)
    recordings = recording_status(conn, start_date, end_date, school)
    lead_followup_pareto = lead_followup_pareto_grid(conn, start_date, end_date, school)
    data_freshness = source_data_freshness(conn, end_date, school)
    followup_coverage = lead_followup_pareto.get("coverage", {})
    legacy_deal_contacted = sum(1 for row in gap["rows"] if row.get("outreach_evidence_found"))
    contacted = followup_coverage.get("communication_7d_leads", 0)
    trial_expected = sum(1 for row in gap["rows"] if row.get("trial_expected"))
    trial_cohort_converted = trial_cohort_conversion_count(conn, start_date, end_date, school)
    funnel_counts = {
        "hubspot_leads": hubspot_lead_count(conn, start_date, end_date, school),
        "contacted": contacted,
        "any_matched_communication": followup_coverage.get("matched_communication_leads", 0),
        "communication_contacted_7d": followup_coverage.get("communication_7d_leads", 0),
        "outbound_contacted_7d": followup_coverage.get("outbound_7d_leads", 0),
        "call_contacted_7d": followup_coverage.get("call_7d_leads", 0),
        "sms_contacted_7d": followup_coverage.get("sms_7d_leads", 0),
        "email_contacted_7d": followup_coverage.get("email_7d_leads", 0),
        "outbound_call_contacted_7d": followup_coverage.get("outbound_call_7d_leads", 0),
        "outbound_sms_contacted_7d": followup_coverage.get("outbound_sms_7d_leads", 0),
        "outbound_email_contacted_7d": followup_coverage.get("outbound_email_7d_leads", 0),
        "legacy_deal_contacted": legacy_deal_contacted,
        "trial_scheduled_or_expected": trial_expected,
        "pike13_first_visits": pike13.get("first_visits", 0),
        "attended": pike13.get("attended", 0),
        "no_show": pike13.get("no_show", 0),
        "canceled": pike13.get("canceled", 0),
        "converted": conversion_count(conn, start_date, end_date, school),
        "trial_cohort_converted": trial_cohort_converted,
    }
    funnel_rates = {
        "lead_to_trial_rate": _rate(funnel_counts["pike13_first_visits"], funnel_counts["hubspot_leads"]),
        "trial_to_conversion_rate": _rate(trial_cohort_converted, funnel_counts["pike13_first_visits"]),
    }
    return {
        "dashboard_type": period,
        "generated_at": utc_now_iso(),
        "school": school,
        "window": {"start": start_date, "end": end_date},
        "source_freshness": source_freshness(conn),
        "source_data_freshness": data_freshness,
        "metric_status": metric_status_from_freshness(data_freshness),
        "funnel_counts": funnel_counts,
        "funnel_rates": funnel_rates,
        "outreach_health": {
            "hubspot_only_unworked": gap["summary"].get("hubspot_only_unworked_rows", 0),
            "pre_trial_outreach_missing": trial["summary"].get("pre_trial_outreach_missing_rows", 0),
            "post_trial_outreach_missing": trial["summary"].get("post_trial_outreach_missing_rows", 0),
            "by_followup_status": trial["summary"].get("by_followup_status", {}),
            "by_identity_status": trial["summary"].get("by_identity_status", {}),
        },
        "pike13_outcomes": {
            **pike13,
            "converted": conversion_count(conn, start_date, end_date, school),
        },
        "communications": communications,
        "lead_followup_pareto": lead_followup_pareto,
        "notes_operations": notes,
        "dialpad_recordings": recordings["downloads"],
        "transcription_queue": recordings["transcription_queue"],
        "performance": performance_sections(conn, start_date, end_date, school),
        "lead_gap": gap["summary"],
        "trial_followup": trial["summary"],
        "exception_queue": build_exception_queue(conn, start_date, end_date, school, limit),
    }


def _markdown_counts(mapping):
    if not mapping:
        return "- None."
    return "\n".join(f"- {key}: {value}" for key, value in mapping.items())


def _markdown_table(rows, columns):
    if not rows:
        return "- None."
    lines = [
        "| " + " | ".join(label for _, label in columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(key, "")).replace("|", "\\|") for key, _ in columns) + " |")
    return "\n".join(lines)


def _markdown_exception_groups(groups):
    if not groups:
        return "- None."
    lines = [
        "| Customer | Refs | Reasons | Rows |",
        "| --- | --- | --- | ---: |",
    ]
    for group in groups:
        refs = ", ".join(group.get("refs", []))
        reasons = ", ".join(group.get("reason_codes", []))
        lines.append(
            "| {customer} | {refs} | {reasons} | {rows} |".format(
                customer=str(group.get("customer_name", "")).replace("|", "\\|"),
                refs=refs.replace("|", "\\|"),
                reasons=reasons.replace("|", "\\|"),
                rows=len(group.get("items", [])),
            )
        )
    return "\n".join(lines)


def _format_rate(value):
    if value is None:
        return "-"
    return f"{value * 100:.0f}%"


def _format_grid_cell(cell):
    if not cell or not cell.get("leads"):
        return "0"
    return f"{cell['leads']} / {_format_rate(cell.get('trial_rate'))}"


def _markdown_followup_pareto(pareto):
    if not pareto:
        return "- None."
    coverage = pareto.get("coverage", {})
    if pareto.get("grid_status") == "blocked":
        return "\n".join(
            [
                "- Insufficient matched communication data to populate the Pareto grid.",
                f"- Blockers: {', '.join(pareto.get('blockers', [])) or 'none'}",
                f"- Leads reviewed: {coverage.get('leads', 0)}",
                f"- Matched communication leads: {coverage.get('matched_communication_leads', 0)}",
                f"- Matched outbound communications within 7 days: {coverage.get('outbound_7d_leads', 0)}",
                f"- Pre-lead inbound-origin leads: {coverage.get('pre_lead_inbound_origin_leads', 0)}",
                f"- Matched SMS communications within 7 days: {coverage.get('sms_7d_leads', 0)}",
                f"- Recommended action: {pareto.get('recommended_action')}",
            ]
        )
    engagement_buckets = pareto.get("engagement_buckets", [])
    lines = [
        "| Response time ↓ / Engagement → | "
        + " | ".join([*engagement_buckets, "Row total"])
        + " |",
        "|---|" + "|".join("---:" for _ in [*engagement_buckets, "Row total"]) + "|",
    ]
    for row in pareto.get("grid", []):
        cells = [_format_grid_cell(row.get("cells", {}).get(bucket)) for bucket in engagement_buckets]
        cells.append(_format_grid_cell(row.get("row_total")))
        lines.append(f"| {row.get('response_time', '')} | " + " | ".join(cells) + " |")
    totals = [_format_grid_cell(pareto.get("column_totals", {}).get(bucket)) for bucket in engagement_buckets]
    lines.append("| Column total | " + " | ".join([*totals, _format_grid_cell(pareto.get("overall_total"))]) + " |")
    return "\n".join(lines)


def render_snapshot_markdown(snapshot):
    window = snapshot["window"]
    lines = [
        f"# {snapshot['dashboard_type'].title()} Lead Dashboard",
        "",
        f"School: {snapshot['school']}",
        f"Window: {window['start']} to {window['end']}",
        "",
        "## Source Freshness",
        "",
        f"- Status: {snapshot['source_freshness']['status']}",
        _markdown_counts(snapshot["source_freshness"].get("counts", {})),
        "",
        "### Source Data Recency",
        "",
        f"- Status: {snapshot.get('source_data_freshness', {}).get('status', 'unknown')}",
        _markdown_counts(snapshot.get("source_data_freshness", {}).get("latest_dates", {})),
        _markdown_counts(
            {
                "flags": ", ".join(snapshot.get("source_data_freshness", {}).get("flags", [])) or "none",
            }
        ),
        "",
        "## Funnel Counts",
        "",
        _markdown_counts(snapshot["funnel_counts"]),
        _markdown_counts(
            {
                "lead_to_trial_rate": _format_rate(snapshot.get("funnel_rates", {}).get("lead_to_trial_rate")),
                "trial_to_conversion_rate": _format_rate(snapshot.get("funnel_rates", {}).get("trial_to_conversion_rate")),
            }
        ),
        "",
        "## Outreach Health",
        "",
        _markdown_counts(
            {
                "hubspot_only_unworked": snapshot["outreach_health"]["hubspot_only_unworked"],
                "pre_trial_outreach_missing": snapshot["outreach_health"]["pre_trial_outreach_missing"],
                "post_trial_outreach_missing": snapshot["outreach_health"]["post_trial_outreach_missing"],
            }
        ),
        "",
        "## Pike13 Outcomes",
        "",
        _markdown_counts(snapshot["pike13_outcomes"]),
        "",
        "## Dialpad and Gmail Coverage",
        "",
        _markdown_counts(snapshot["communications"]),
        "",
        "## Lead Follow-Up Pareto",
        "",
        "- Cell format: leads / lead-to-trial rate",
        "- Engagement buckets: None = no matched outbound touch; Light = one matched outbound touch; Active = two or more matched outbound touches; Two-way = at least one matched outbound and one matched inbound touch.",
        "- Response timing uses the first captured outbound communication in the first 7 days after lead creation.",
        _markdown_counts(snapshot["lead_followup_pareto"].get("coverage", {})),
        _markdown_counts(
            {
                "grid_status": snapshot["lead_followup_pareto"].get("grid_status", "unknown"),
                "data_quality_flags": ", ".join(snapshot["lead_followup_pareto"].get("data_quality_flags", [])) or "none",
                "blockers": ", ".join(snapshot["lead_followup_pareto"].get("blockers", [])) or "none",
            }
        ),
        "",
        _markdown_followup_pareto(snapshot["lead_followup_pareto"]),
        "",
        "## Notes Operations",
        "",
        _markdown_counts(snapshot.get("notes_operations", {})),
        "",
        "## Recording and Transcription Coverage",
        "",
        "### Recording Downloads",
        "",
        _markdown_counts(snapshot["dialpad_recordings"]),
        "",
        "### Transcription Queue",
        "",
        _markdown_counts(snapshot["transcription_queue"]),
        "",
        "## Performance",
        "",
        "### Staff Trial Counts",
        "",
        _markdown_table(snapshot["performance"]["staff_trial_counts"], [("staff", "Staff"), ("trials", "Trials")]),
        "",
        "### HubSpot Source Counts",
        "",
        _markdown_table(snapshot["performance"]["hubspot_source_counts"], [("source", "Source"), ("leads", "Leads")]),
        "",
        "## Exception Queue",
        "",
        _markdown_counts(snapshot["exception_queue"]["summary"]),
        "",
        "### By Customer",
        "",
        _markdown_exception_groups(snapshot["exception_queue"].get("customer_groups", [])),
        "",
        "### Rows",
        "",
        _markdown_table(
            snapshot["exception_queue"]["items"],
            [
                ("customer_name", "Customer"),
                ("lead_ref", "Lead"),
                ("school", "School"),
                ("stage", "Stage"),
                ("diagnostic_area", "Area"),
                ("reason_code", "Reason"),
            ],
        ),
        "",
        "_This dashboard includes customer names for operational review. It still excludes emails, phones, message bodies, transcripts, raw page text, screenshots, source URLs, and audio paths._",
        "",
    ]
    return "\n".join(lines)


def snapshot_to_json(snapshot):
    return json.dumps(snapshot, indent=2, sort_keys=True, default=str)


def timeline_ref(value):
    digest = hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()
    return f"event_{digest[:10]}"


def lead_evidence_timeline(conn, search, start_date="", end_date="", limit=100, include_sensitive=False):
    if not table_exists(conn, "vw_lead_timeline"):
        return {"rows": [], "row_count": 0, "truncated": False}
    if not search or not str(search).strip():
        raise ValueError("search is required.")
    limit = max(1, min(int(limit or 100), 200))
    needle = f"%{str(search).strip().lower()}%"
    rows = conn.execute(
        """
        SELECT source, event_type, source_id, deal_id, contact_id, pike13_person_id, event_at,
               school, owner, person_or_lead, title, detail, source_url
        FROM vw_lead_timeline
        WHERE (
            LOWER(COALESCE(deal_id, '')) LIKE :needle
            OR LOWER(COALESCE(contact_id, '')) LIKE :needle
            OR LOWER(COALESCE(pike13_person_id, '')) LIKE :needle
            OR LOWER(COALESCE(person_or_lead, '')) LIKE :needle
            OR LOWER(COALESCE(title, '')) LIKE :needle
            OR LOWER(COALESCE(detail, '')) LIKE :needle
        )
          AND (:start = '' OR date(event_at) >= date(:start))
          AND (:end = '' OR date(event_at) <= date(:end))
        ORDER BY event_at
        LIMIT :limit
        """,
        {"needle": needle, "start": start_date or "", "end": end_date or "", "limit": limit + 1},
    ).fetchall()
    data = []
    for row in rows[:limit]:
        item = {
            "event_ref": timeline_ref(row["source_id"]),
            "source": row["source"],
            "event_type": row["event_type"],
            "event_at": row["event_at"],
            "school": row["school"],
            "title": row["title"],
        }
        if include_sensitive:
            item.update(
                {
                    "source_id": row["source_id"],
                    "deal_id": row["deal_id"],
                    "contact_id": row["contact_id"],
                    "pike13_person_id": row["pike13_person_id"],
                    "person_or_lead": row["person_or_lead"],
                    "owner": row["owner"],
                    "detail": row["detail"],
                    "source_url": row["source_url"],
                }
            )
        data.append(item)
    return {"rows": data, "row_count": len(data), "truncated": len(rows) > limit}
