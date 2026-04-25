import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema, upsert_identity_match


DEFAULT_WINDOW_DAYS = 7
DEFAULT_PIKE13_LOOKAHEAD_DAYS = 30


def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso_date_days_ago(days, now=None):
    base = now or utc_now()
    return (base - timedelta(days=days)).date().isoformat()


def iso_date_days_ahead(days, now=None):
    base = now or utc_now()
    return (base + timedelta(days=days)).date().isoformat()


def count(conn, sql, params=None):
    return conn.execute(sql, params or {}).fetchone()[0]


def latest(conn, sql, params=None):
    value = conn.execute(sql, params or {}).fetchone()[0]
    return value


def percent(part, whole):
    if not whole:
        return 0.0
    return round((part / whole) * 100.0, 1)


def status_for(required_rates, blockers=None, min_ready=80.0, min_partial=40.0):
    blockers = blockers or []
    if blockers:
        return "blocked"
    if not required_rates:
        return "blocked"
    worst = min(required_rates)
    if worst >= min_ready:
        return "ready"
    if worst >= min_partial:
        return "partial"
    return "blocked"


def field_coverage(conn, table, fields, where_sql="1=1", params=None):
    total = count(conn, f"SELECT COUNT(*) FROM {table} WHERE {where_sql}", params)
    coverage = {}
    for field in fields:
        filled = count(
            conn,
            f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE {where_sql}
              AND {field} IS NOT NULL
              AND CAST({field} AS TEXT) != ''
            """,
            params,
        )
        coverage[field] = {
            "filled": filled,
            "total": total,
            "fill_rate": percent(filled, total),
        }
    return total, coverage


def valid_date_coverage(conn, table, field, where_sql="1=1", params=None):
    total = count(conn, f"SELECT COUNT(*) FROM {table} WHERE {where_sql}", params)
    filled = count(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {table}
        WHERE {where_sql}
          AND {field} IS NOT NULL
          AND CAST({field} AS TEXT) != ''
          AND (
              date({field}) IS NOT NULL
              OR CAST({field} AS TEXT) GLOB '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]'
              OR CAST({field} AS TEXT) GLOB '[0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]'
              OR CAST({field} AS TEXT) GLOB '[0-9][0-9]/[0-9]/[0-9][0-9][0-9][0-9]'
              OR CAST({field} AS TEXT) GLOB '[0-9]/[0-9]/[0-9][0-9][0-9][0-9]'
              OR CAST({field} AS TEXT) GLOB '[A-Z][a-z][a-z] [0-9], [0-9][0-9][0-9][0-9]*'
              OR CAST({field} AS TEXT) GLOB '[A-Z][a-z][a-z] [0-9][0-9], [0-9][0-9][0-9][0-9]*'
          )
        """,
        params,
    )
    return {"filled": filled, "total": total, "fill_rate": percent(filled, total)}


def value_coverage(conn, table, field, values, where_sql="1=1", params=None):
    total = count(conn, f"SELECT COUNT(*) FROM {table} WHERE {where_sql}", params)
    placeholders = ", ".join(f":value_{i}" for i, _ in enumerate(values))
    query_params = dict(params or {})
    for i, value in enumerate(values):
        query_params[f"value_{i}"] = value
    filled = count(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {table}
        WHERE {where_sql}
          AND LOWER(COALESCE({field}, '')) IN ({placeholders})
        """,
        query_params,
    )
    return {"filled": filled, "total": total, "fill_rate": percent(filled, total)}


def refresh_identity_matches(conn):
    """Populate deterministic identity matches from the current source tables."""
    inserted = 0

    for deal_id, pike13_person_id in conn.execute(
        """
        SELECT deal_id, pike13_person_id
        FROM hubspot_deals
        WHERE pike13_person_id IS NOT NULL AND pike13_person_id != ''
        """
    ).fetchall():
        before = conn.total_changes
        upsert_identity_match(
            conn,
            "hubspot",
            "hubspot_deals",
            deal_id,
            "pike13",
            "pike13_people",
            pike13_person_id,
            "pike13_person_id",
            0.99,
            "HubSpot deal includes direct Pike13 person ID",
        )
        inserted += conn.total_changes - before

    for contact_id, email in conn.execute(
        """
        SELECT contact_id, email_normalized
        FROM hubspot_contacts
        WHERE email_normalized IS NOT NULL AND email_normalized != ''
        """
    ).fetchall():
        for person_id, in conn.execute(
            """
            SELECT person_id
            FROM pike13_people
            WHERE email_normalized = ?
            """,
            (email,),
        ).fetchall():
            before = conn.total_changes
            upsert_identity_match(
                conn,
                "hubspot",
                "hubspot_contacts",
                contact_id,
                "pike13",
                "pike13_people",
                person_id,
                "email_exact",
                0.95,
                f"Exact normalized email match: {email}",
            )
            inserted += conn.total_changes - before

    for contact_id, phone in conn.execute(
        """
        SELECT contact_id, phone_normalized
        FROM hubspot_contacts
        WHERE phone_normalized IS NOT NULL AND phone_normalized != ''
        """
    ).fetchall():
        for table, source_id_col, source_system in (
            ("pike13_people", "person_id", "pike13"),
            ("dialpad_sms_threads", "thread_id", "dialpad"),
            ("dialpad_voice_events", "event_id", "dialpad"),
        ):
            for target_id, in conn.execute(
                f"""
                SELECT {source_id_col}
                FROM {table}
                WHERE phone_normalized = ?
                """,
                (phone,),
            ).fetchall():
                before = conn.total_changes
                upsert_identity_match(
                    conn,
                    "hubspot",
                    "hubspot_contacts",
                    contact_id,
                    source_system,
                    table,
                    target_id,
                    "phone_exact",
                    0.90,
                    f"Exact normalized phone match: {phone}",
                )
                inserted += conn.total_changes - before

    for deal_id, deal_name, school in conn.execute(
        """
        SELECT deal_id, deal_name, school
        FROM hubspot_deals
        WHERE deal_name IS NOT NULL AND deal_name != ''
          AND school IS NOT NULL AND school != ''
        """
    ).fetchall():
        lead_name = (deal_name or "").split("|")[0].strip().lower()
        school_l = (school or "").lower()
        if not lead_name or not school_l:
            continue
        for person_id, full_name, person_school in conn.execute(
            """
            SELECT person_id, full_name, school
            FROM pike13_people
            WHERE full_name IS NOT NULL AND full_name != ''
              AND school IS NOT NULL AND school != ''
            """
        ).fetchall():
            if lead_name == (full_name or "").strip().lower() and school_l in (person_school or "").lower():
                before = conn.total_changes
                upsert_identity_match(
                    conn,
                    "hubspot",
                    "hubspot_deals",
                    deal_id,
                    "pike13",
                    "pike13_people",
                    person_id,
                    "name_school_exact",
                    0.70,
                    f"Exact name plus school match: {lead_name} / {school}",
                )
                inserted += conn.total_changes - before

    return inserted


def import_run_summary(conn, source):
    row = conn.execute(
        """
        SELECT source, status, started_at, finished_at, rows_seen, rows_inserted, rows_updated, error
        FROM source_import_runs
        WHERE source = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (source,),
    ).fetchone()
    return dict(row) if row else None


def hubspot_section(conn, window_start):
    total, coverage = field_coverage(
        conn,
        "hubspot_deals",
        [
            "deal_id",
            "deal_name",
            "stage",
            "owner",
            "school",
            "create_date",
            "last_activity_date",
            "last_contacted",
            "source_url",
            "raw_text",
        ],
    )
    for field in ("create_date", "last_activity_date", "last_contacted"):
        coverage[field] = valid_date_coverage(conn, "hubspot_deals", field)
    recent = count(
        conn,
        """
        SELECT COUNT(*)
        FROM hubspot_deals
        WHERE date(create_date) >= date(:window_start)
           OR date(last_activity_date) >= date(:window_start)
           OR date(last_contacted) >= date(:window_start)
           OR date(updated_at) >= date(:window_start)
        """,
        {"window_start": window_start},
    )
    required_rates = [
        coverage["deal_id"]["fill_rate"],
        coverage["deal_name"]["fill_rate"],
        coverage["stage"]["fill_rate"],
        coverage["school"]["fill_rate"],
        coverage["create_date"]["fill_rate"],
        coverage["source_url"]["fill_rate"],
        coverage["raw_text"]["fill_rate"],
    ]
    blockers = []
    if total == 0:
        blockers.append("No HubSpot deals loaded.")
    if coverage["stage"]["fill_rate"] < 80:
        blockers.append("Deal stage coverage is below readiness threshold.")
    if coverage["create_date"]["fill_rate"] < 80:
        blockers.append("Deal create date coverage is below readiness threshold.")
    return {
        "status": status_for(required_rates, blockers),
        "rows": total,
        "recent_window_rows": recent,
        "latest_timestamp": latest(
            conn,
            """
            SELECT MAX(value)
            FROM (
                SELECT create_date AS value FROM hubspot_deals WHERE date(create_date) IS NOT NULL
                UNION ALL
                SELECT last_activity_date FROM hubspot_deals WHERE date(last_activity_date) IS NOT NULL
                UNION ALL
                SELECT last_contacted FROM hubspot_deals WHERE date(last_contacted) IS NOT NULL
                UNION ALL
                SELECT updated_at FROM hubspot_deals WHERE date(updated_at) IS NOT NULL
            )
            """,
        ),
        "field_coverage": coverage,
        "latest_import_run": import_run_summary(conn, "hubspot"),
        "blockers": blockers,
    }


def dialpad_section(conn, window_start):
    sms_total, sms_coverage = field_coverage(
        conn,
        "dialpad_sms_messages",
        ["message_id", "thread_id", "message_at", "direction", "body", "source_url", "raw_text"],
    )
    sms_coverage["message_at"] = valid_date_coverage(conn, "dialpad_sms_messages", "message_at")
    sms_coverage["direction"] = value_coverage(
        conn,
        "dialpad_sms_messages",
        "direction",
        ("inbound", "outbound"),
    )
    voice_total, voice_coverage = field_coverage(
        conn,
        "vw_dialpad_communications",
        ["communication_id", "event_at", "direction", "phone_normalized", "source_url"],
        "channel = 'call'",
    )
    voice_coverage["event_at"] = valid_date_coverage(conn, "vw_dialpad_communications", "event_at", "channel = 'call'")
    voice_coverage["direction"] = value_coverage(
        conn,
        "vw_dialpad_communications",
        "direction",
        ("inbound", "outbound"),
        "channel = 'call'",
    )
    recent_sms = count(
        conn,
        "SELECT COUNT(*) FROM dialpad_sms_messages WHERE date(COALESCE(message_at, updated_at)) >= date(:window_start)",
        {"window_start": window_start},
    )
    recent_voice = count(
        conn,
        "SELECT COUNT(*) FROM vw_dialpad_communications WHERE channel = 'call' AND date(event_at) >= date(:window_start)",
        {"window_start": window_start},
    )
    transcript_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM vw_dialpad_communications
        WHERE has_transcript = 1
        """
    )
    required_rates = [
        sms_coverage["message_id"]["fill_rate"],
        sms_coverage["body"]["fill_rate"],
        voice_coverage["communication_id"]["fill_rate"],
    ]
    blockers = []
    if sms_total == 0 and voice_total == 0:
        blockers.append("No Dialpad communications loaded.")
    if sms_total and sms_coverage["direction"]["fill_rate"] < 80:
        blockers.append("Dialpad SMS direction coverage is below readiness threshold.")
    if voice_total and voice_coverage["event_at"]["fill_rate"] < 80:
        blockers.append("Dialpad voice event timestamp coverage is below readiness threshold.")
    return {
        "status": status_for(required_rates, blockers),
        "sms_rows": sms_total,
        "voice_rows": voice_total,
        "recent_window_sms_rows": recent_sms,
        "recent_window_voice_rows": recent_voice,
        "transcript_or_summary_rows": transcript_rows,
        "latest_timestamp": latest(
            conn,
            "SELECT MAX(event_at) FROM vw_dialpad_communications WHERE date(event_at) IS NOT NULL",
        ),
        "sms_field_coverage": sms_coverage,
        "voice_field_coverage": voice_coverage,
        "latest_sms_import_run": import_run_summary(conn, "dialpad_sms"),
        "latest_voice_import_run": import_run_summary(conn, "dialpad_voice"),
        "blockers": blockers,
    }


def pike13_section(conn, window_start, lookahead_end):
    people_total, people_coverage = field_coverage(
        conn,
        "pike13_people",
        ["person_id", "full_name", "email_normalized", "phone_normalized", "membership_state", "source_url", "raw_text"],
    )
    visits_total, visits_coverage = field_coverage(
        conn,
        "pike13_visits",
        ["visit_id", "person_id", "service", "starts_at", "status", "source_url", "raw_text"],
    )
    visits_coverage["starts_at"] = valid_date_coverage(conn, "pike13_visits", "starts_at")
    plans_total, plans_coverage = field_coverage(
        conn,
        "pike13_plans_passes",
        ["plan_pass_id", "person_id", "name", "status", "starts_at", "ends_at", "source_url", "raw_text"],
    )
    plans_coverage["starts_at"] = valid_date_coverage(conn, "pike13_plans_passes", "starts_at")
    plans_coverage["ends_at"] = valid_date_coverage(conn, "pike13_plans_passes", "ends_at")
    recent_visits = count(
        conn,
        """
        SELECT COUNT(*)
        FROM pike13_visits
        WHERE date(starts_at) BETWEEN date(:window_start) AND date(:lookahead_end)
        """,
        {"window_start": window_start, "lookahead_end": lookahead_end},
    )
    required_rates = [
        people_coverage["person_id"]["fill_rate"],
        people_coverage["full_name"]["fill_rate"],
        visits_coverage["visit_id"]["fill_rate"] if visits_total else 0,
        visits_coverage["starts_at"]["fill_rate"] if visits_total else 0,
        plans_coverage["plan_pass_id"]["fill_rate"] if plans_total else 0,
    ]
    blockers = []
    if people_total == 0:
        blockers.append("No Pike13 people loaded.")
    if visits_total == 0:
        blockers.append("No Pike13 visits loaded.")
    if visits_total and visits_coverage["starts_at"]["fill_rate"] < 80:
        blockers.append("Pike13 visit date coverage is below readiness threshold.")
    return {
        "status": status_for(required_rates, blockers),
        "people_rows": people_total,
        "visit_rows": visits_total,
        "plan_pass_rows": plans_total,
        "window_plus_lookahead_visit_rows": recent_visits,
        "latest_timestamp": latest(
            conn,
            """
            SELECT MAX(value)
            FROM (
                SELECT starts_at AS value FROM pike13_visits WHERE date(starts_at) IS NOT NULL
                UNION ALL
                SELECT updated_at FROM pike13_visits WHERE date(updated_at) IS NOT NULL
            )
            """,
        ),
        "people_field_coverage": people_coverage,
        "visit_field_coverage": visits_coverage,
        "plan_pass_field_coverage": plans_coverage,
        "latest_import_run": import_run_summary(conn, "pike13"),
        "blockers": blockers,
    }


def matching_section(conn):
    refresh_identity_matches(conn)
    total = count(conn, "SELECT COUNT(*) FROM identity_matches")
    by_type = [
        dict(row)
        for row in conn.execute(
            """
            SELECT match_type, COUNT(*) AS rows, ROUND(AVG(confidence), 3) AS avg_confidence
            FROM identity_matches
            GROUP BY match_type
            ORDER BY rows DESC, match_type
            """
        ).fetchall()
    ]
    matched_deals = count(
        conn,
        """
        SELECT COUNT(DISTINCT source_id)
        FROM identity_matches
        WHERE source_table = 'hubspot_deals'
        """
    )
    matched_contacts = count(
        conn,
        """
        SELECT COUNT(DISTINCT source_id)
        FROM identity_matches
        WHERE source_table = 'hubspot_contacts'
        """
    )
    return {
        "status": "ready" if total else "partial",
        "rows": total,
        "matched_hubspot_deals": matched_deals,
        "matched_hubspot_contacts": matched_contacts,
        "by_match_type": by_type,
    }


def build_source_completeness_report(conn, window_days=DEFAULT_WINDOW_DAYS, pike13_lookahead_days=DEFAULT_PIKE13_LOOKAHEAD_DAYS):
    ensure_lead_followup_schema(conn)
    conn.row_factory = sqlite3.Row
    now = utc_now()
    window_start = iso_date_days_ago(window_days, now)
    window_end = now.date().isoformat()
    lookahead_end = iso_date_days_ahead(pike13_lookahead_days, now)
    report = {
        "window": {
            "days": window_days,
            "start": window_start,
            "end": window_end,
            "pike13_lookahead_days": pike13_lookahead_days,
            "pike13_lookahead_end": lookahead_end,
        },
        "sources": {
            "hubspot": hubspot_section(conn, window_start),
            "dialpad": dialpad_section(conn, window_start),
            "pike13": pike13_section(conn, window_start, lookahead_end),
        },
        "matching": matching_section(conn),
    }
    statuses = [source["status"] for source in report["sources"].values()]
    if any(status == "blocked" for status in statuses):
        overall = "blocked"
    elif any(status == "partial" for status in statuses):
        overall = "partial"
    else:
        overall = "ready"
    report["overall_status"] = overall
    return report


def main():
    parser = argparse.ArgumentParser(description="Report source completeness for lead follow-up proof windows.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--pike13-lookahead-days", type=int, default=DEFAULT_PIKE13_LOOKAHEAD_DAYS)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        report = build_source_completeness_report(conn, args.window_days, args.pike13_lookahead_days)
        conn.commit()
    finally:
        conn.close()

    print(json.dumps(report, indent=2 if args.pretty else None, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
