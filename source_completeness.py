import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema, upsert_identity_match


DEFAULT_WINDOW_DAYS = 7
DEFAULT_PIKE13_LOOKAHEAD_DAYS = 30
STALE_RUNNING_RUN_HOURS = 6


def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0)


def parse_iso_datetime(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_date_days_ago(days, now=None):
    base = now or utc_now()
    return (base - timedelta(days=days)).date().isoformat()


def iso_date_days_ahead(days, now=None):
    base = now or utc_now()
    return (base + timedelta(days=days)).date().isoformat()


def count(conn, sql, params=None):
    return conn.execute(sql, params or {}).fetchone()[0]


def table_exists(conn, name):
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (name,),
        ).fetchone()
    )


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


def json_value_counts(conn, table, json_field, json_path, where_sql="1=1", params=None):
    rows = conn.execute(
        f"""
        SELECT COALESCE(json_extract({json_field}, :json_path), 'missing') AS value, COUNT(*) AS rows
        FROM {table}
        WHERE {where_sql}
        GROUP BY value
        ORDER BY rows DESC, value
        """,
        dict(params or {}, json_path=json_path),
    ).fetchall()
    return {str(row[0]): row[1] for row in rows}


def trusted_contact_condition():
    return """
        LOWER(COALESCE(email_normalized, '')) NOT LIKE '%@schoolofrock.com'
        AND COALESCE(json_extract(raw_json, '$.trusted'), 0) = 1
    """


def contact_quality_summary(conn):
    total = count(conn, "SELECT COUNT(*) FROM hubspot_contacts")
    trusted = count(conn, f"SELECT COUNT(*) FROM hubspot_contacts WHERE {trusted_contact_condition()}")
    customer_email = count(
        conn,
        f"""
        SELECT COUNT(*)
        FROM hubspot_contacts
        WHERE {trusted_contact_condition()}
          AND email_normalized IS NOT NULL
          AND email_normalized != ''
        """,
    )
    phone = count(
        conn,
        f"""
        SELECT COUNT(*)
        FROM hubspot_contacts
        WHERE {trusted_contact_condition()}
          AND phone_normalized IS NOT NULL
          AND phone_normalized != ''
        """,
    )
    internal_email = count(
        conn,
        """
        SELECT COUNT(*)
        FROM hubspot_contacts
        WHERE LOWER(COALESCE(email_normalized, '')) LIKE '%@schoolofrock.com'
        """,
    )
    rejected_internal = count(
        conn,
        """
        SELECT COUNT(*)
        FROM hubspot_contacts
        WHERE COALESCE(json_array_length(json_extract(raw_json, '$.rejected_emails')), 0) > 0
        """,
    )
    return {
        "rows": total,
        "trusted_rows": trusted,
        "trusted_customer_email_rows": customer_email,
        "trusted_phone_rows": phone,
        "stored_internal_email_rows": internal_email,
        "rows_with_rejected_internal_emails": rejected_internal,
    }


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
        f"""
        SELECT contact_id, email_normalized
        FROM hubspot_contacts
        WHERE email_normalized IS NOT NULL AND email_normalized != ''
          AND {trusted_contact_condition()}
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
        f"""
        SELECT contact_id, phone_normalized
        FROM hubspot_contacts
        WHERE phone_normalized IS NOT NULL AND phone_normalized != ''
          AND {trusted_contact_condition()}
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


def _import_run_dict(row):
    summary = dict(row)
    metadata = summary.pop("metadata_json", None)
    if metadata:
        try:
            summary["metadata"] = json.loads(metadata)
        except json.JSONDecodeError:
            summary["metadata"] = metadata
    return summary


def _stale_running_run(summary, now=None, stale_after_hours=STALE_RUNNING_RUN_HOURS):
    if (summary.get("status") or "").lower() != "running":
        return False
    started_at = parse_iso_datetime(summary.get("started_at"))
    if not started_at:
        return False
    age = (now or utc_now()) - started_at
    return age.total_seconds() >= stale_after_hours * 3600


def import_run_summary(conn, source, now=None, stale_after_hours=STALE_RUNNING_RUN_HOURS):
    row = conn.execute(
        """
        SELECT id, source, status, started_at, finished_at, rows_seen, rows_inserted, rows_updated, error, metadata_json
        FROM source_import_runs
        WHERE source = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (source,),
    ).fetchone()
    if not row:
        return None
    summary = _import_run_dict(row)
    if not _stale_running_run(summary, now=now, stale_after_hours=stale_after_hours):
        return summary

    fallback = conn.execute(
        """
        SELECT id, source, status, started_at, finished_at, rows_seen, rows_inserted, rows_updated, error, metadata_json
        FROM source_import_runs
        WHERE source = ?
          AND LOWER(COALESCE(status, '')) != 'running'
        ORDER BY id DESC
        LIMIT 1
        """,
        (source,),
    ).fetchone()
    stale_info = {
        "id": summary.get("id"),
        "source": summary.get("source"),
        "status": summary.get("status"),
        "started_at": summary.get("started_at"),
        "rows_seen": summary.get("rows_seen"),
        "rows_inserted": summary.get("rows_inserted"),
        "rows_updated": summary.get("rows_updated"),
    }
    if fallback:
        fallback_summary = _import_run_dict(fallback)
        fallback_summary["stale_running_run"] = stale_info
        return fallback_summary

    summary["status"] = "stale"
    summary["stale_running_run"] = stale_info
    return summary


def dialpad_target_search_summary(conn):
    latest_run = import_run_summary(conn, "dialpad_target_search")
    if not latest_run:
        return {
            "latest_import_run": None,
            "rows": 0,
            "targets_found": 0,
            "targets_with_sms": 0,
            "targets_with_calls_or_call_reviews": 0,
            "targets_not_found": 0,
            "filter_not_supported_rows": 0,
            "ui_blocked_rows": 0,
            "auth_blocked_rows": 0,
            "parse_error_rows": 0,
            "outcomes": {},
        }
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS rows,
            SUM(CASE WHEN outcome IN ('found_sms', 'found_call', 'found_voicemail', 'found_call_review') THEN 1 ELSE 0 END) AS targets_found,
            SUM(CASE WHEN outcome = 'found_sms' THEN 1 ELSE 0 END) AS targets_with_sms,
            SUM(CASE WHEN outcome IN ('found_call', 'found_voicemail', 'found_call_review') THEN 1 ELSE 0 END) AS targets_with_calls_or_call_reviews,
            SUM(CASE WHEN outcome IN ('not_found', 'not_found_after_route_search') THEN 1 ELSE 0 END) AS targets_not_found,
            SUM(CASE WHEN outcome = 'filter_not_supported' THEN 1 ELSE 0 END) AS filter_not_supported_rows,
            SUM(CASE WHEN outcome = 'ui_blocked' THEN 1 ELSE 0 END) AS ui_blocked_rows,
            SUM(CASE WHEN outcome = 'auth_blocked' THEN 1 ELSE 0 END) AS auth_blocked_rows,
            SUM(CASE WHEN outcome = 'parse_error' THEN 1 ELSE 0 END) AS parse_error_rows
        FROM dialpad_target_searches
        WHERE run_id = (
            SELECT id
            FROM source_import_runs
            WHERE source = 'dialpad_target_search'
            ORDER BY id DESC
            LIMIT 1
        )
        """
    ).fetchone()
    outcomes = {
        item["outcome"]: item["rows"]
        for item in conn.execute(
            """
            SELECT outcome, COUNT(*) AS rows
            FROM dialpad_target_searches
            WHERE run_id = (
                SELECT id
                FROM source_import_runs
                WHERE source = 'dialpad_target_search'
                ORDER BY id DESC
                LIMIT 1
            )
            GROUP BY outcome
            ORDER BY outcome
            """
        ).fetchall()
    }
    return {
        "latest_import_run": latest_run,
        "rows": row["rows"] or 0,
        "targets_found": row["targets_found"] or 0,
        "targets_with_sms": row["targets_with_sms"] or 0,
        "targets_with_calls_or_call_reviews": row["targets_with_calls_or_call_reviews"] or 0,
        "targets_not_found": row["targets_not_found"] or 0,
        "filter_not_supported_rows": row["filter_not_supported_rows"] or 0,
        "ui_blocked_rows": row["ui_blocked_rows"] or 0,
        "auth_blocked_rows": row["auth_blocked_rows"] or 0,
        "parse_error_rows": row["parse_error_rows"] or 0,
        "outcomes": outcomes,
    }


def dialpad_route_discovery_summary(conn):
    latest_run = import_run_summary(conn, "dialpad_route_discovery")
    if not latest_run:
        return {
            "latest_import_run": None,
            "rows": 0,
            "usable_routes": 0,
            "partial_routes": 0,
            "blocked_routes": 0,
            "sms_routes": 0,
            "voice_routes": 0,
            "voicemail_routes": 0,
            "call_review_routes": 0,
            "date_filter_routes": 0,
            "school_filter_routes": 0,
            "keyword_filter_routes": 0,
            "statuses": {},
        }
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS rows,
            SUM(CASE WHEN status = 'usable' THEN 1 ELSE 0 END) AS usable_routes,
            SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) AS partial_routes,
            SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END) AS blocked_routes,
            SUM(CASE WHEN sms_signal_visible = 1 THEN 1 ELSE 0 END) AS sms_routes,
            SUM(CASE WHEN voice_signal_visible = 1 THEN 1 ELSE 0 END) AS voice_routes,
            SUM(CASE WHEN voicemail_signal_visible = 1 THEN 1 ELSE 0 END) AS voicemail_routes,
            SUM(CASE WHEN call_review_url_count > 0 THEN 1 ELSE 0 END) AS call_review_routes,
            SUM(CASE WHEN supports_date_filter = 1 THEN 1 ELSE 0 END) AS date_filter_routes,
            SUM(CASE WHEN supports_school_filter = 1 THEN 1 ELSE 0 END) AS school_filter_routes,
            SUM(CASE WHEN supports_keyword_filter = 1 THEN 1 ELSE 0 END) AS keyword_filter_routes
        FROM dialpad_route_discoveries
        WHERE run_id = (
            SELECT id
            FROM source_import_runs
            WHERE source = 'dialpad_route_discovery'
            ORDER BY id DESC
            LIMIT 1
        )
        """
    ).fetchone()
    statuses = {
        item["status"]: item["rows"]
        for item in conn.execute(
            """
            SELECT status, COUNT(*) AS rows
            FROM dialpad_route_discoveries
            WHERE run_id = (
                SELECT id
                FROM source_import_runs
                WHERE source = 'dialpad_route_discovery'
                ORDER BY id DESC
                LIMIT 1
            )
            GROUP BY status
            ORDER BY status
            """
        ).fetchall()
    }
    return {
        "latest_import_run": latest_run,
        "rows": row["rows"] or 0,
        "usable_routes": row["usable_routes"] or 0,
        "partial_routes": row["partial_routes"] or 0,
        "blocked_routes": row["blocked_routes"] or 0,
        "sms_routes": row["sms_routes"] or 0,
        "voice_routes": row["voice_routes"] or 0,
        "voicemail_routes": row["voicemail_routes"] or 0,
        "call_review_routes": row["call_review_routes"] or 0,
        "date_filter_routes": row["date_filter_routes"] or 0,
        "school_filter_routes": row["school_filter_routes"] or 0,
        "keyword_filter_routes": row["keyword_filter_routes"] or 0,
        "statuses": statuses,
    }


def source_route_discovery_summary(conn, source):
    latest_run = import_run_summary(conn, f"{source}_route_discovery")
    if not latest_run:
        return {
            "latest_import_run": None,
            "rows": 0,
            "usable_routes": 0,
            "partial_routes": 0,
            "blocked_routes": 0,
            "source_timestamp_routes": 0,
            "transcript_link_routes": 0,
            "recording_link_routes": 0,
            "statuses": {},
        }
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS rows,
            SUM(CASE WHEN status = 'usable' THEN 1 ELSE 0 END) AS usable_routes,
            SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) AS partial_routes,
            SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END) AS blocked_routes,
            SUM(CASE WHEN source_timestamp_visible = 1 THEN 1 ELSE 0 END) AS source_timestamp_routes,
            SUM(CASE WHEN transcript_link_visible = 1 THEN 1 ELSE 0 END) AS transcript_link_routes,
            SUM(CASE WHEN recording_link_visible = 1 THEN 1 ELSE 0 END) AS recording_link_routes
        FROM source_route_discoveries
        WHERE run_id = (
            SELECT id
            FROM source_import_runs
            WHERE source = ?
            ORDER BY id DESC
            LIMIT 1
        )
        """,
        (f"{source}_route_discovery",),
    ).fetchone()
    statuses = {
        item["status"]: item["rows"]
        for item in conn.execute(
            """
            SELECT status, COUNT(*) AS rows
            FROM source_route_discoveries
            WHERE run_id = (
                SELECT id
                FROM source_import_runs
                WHERE source = ?
                ORDER BY id DESC
                LIMIT 1
            )
            GROUP BY status
            ORDER BY status
            """,
            (f"{source}_route_discovery",),
        ).fetchall()
    }
    return {
        "latest_import_run": latest_run,
        "rows": row["rows"] or 0,
        "usable_routes": row["usable_routes"] or 0,
        "partial_routes": row["partial_routes"] or 0,
        "blocked_routes": row["blocked_routes"] or 0,
        "source_timestamp_routes": row["source_timestamp_routes"] or 0,
        "transcript_link_routes": row["transcript_link_routes"] or 0,
        "recording_link_routes": row["recording_link_routes"] or 0,
        "statuses": statuses,
    }


def dialpad_daily_intake_summary(conn, window_start):
    latest_run = import_run_summary(conn, "dialpad_daily_intake")
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS communication_window_rows,
            SUM(CASE WHEN is_inbound_needing_followup = 1 THEN 1 ELSE 0 END) AS daily_inbound_rows,
            SUM(CASE WHEN is_inbound_needing_followup = 1 AND match_status != 'matched_hubspot' THEN 1 ELSE 0 END) AS unmatched_inbound_rows,
            SUM(CASE WHEN action_status = 'possible_lead_not_in_hubspot' THEN 1 ELSE 0 END) AS possible_lead_not_in_hubspot_rows,
            SUM(CASE WHEN is_inbound_needing_followup = 1 AND has_later_outbound_followup = 0 THEN 1 ELSE 0 END) AS no_followup_rows,
            SUM(CASE WHEN match_status = 'matched_hubspot' THEN 1 ELSE 0 END) AS matched_hubspot_rows,
            MAX(event_at) AS latest_daily_intake_at
        FROM vw_dialpad_daily_intake
        WHERE date(event_at) >= date(:window_start)
        """,
        {"window_start": window_start},
    ).fetchone()
    tagged_daily_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM dialpad_voice_events
        WHERE date(event_at) >= date(:window_start)
          AND COALESCE(
                json_extract(
                    CASE WHEN json_valid(COALESCE(raw_json, '{}')) THEN raw_json ELSE '{}' END,
                    '$.daily_intake'
                ),
                0
              ) = 1
        """,
        {"window_start": window_start},
    )
    discovery_required = False
    if latest_run and latest_run.get("status") in {"blocked", "error", "partial"}:
        discovery_required = True
    return {
        "latest_import_run": latest_run,
        "daily_intake_rows": tagged_daily_rows,
        "communication_window_rows": row["communication_window_rows"] or 0,
        "daily_inbound_rows": row["daily_inbound_rows"] or 0,
        "unmatched_inbound_rows": row["unmatched_inbound_rows"] or 0,
        "possible_lead_not_in_hubspot_rows": row["possible_lead_not_in_hubspot_rows"] or 0,
        "no_followup_rows": row["no_followup_rows"] or 0,
        "matched_hubspot_rows": row["matched_hubspot_rows"] or 0,
        "latest_daily_intake_at": row["latest_daily_intake_at"],
        "discovery_fallback_required": discovery_required,
    }


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
        "contact_quality": contact_quality_summary(conn),
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
    future_sms = count(
        conn,
        "SELECT COUNT(*) FROM dialpad_sms_messages WHERE date(message_at) > date('now')",
    )
    future_voice = count(
        conn,
        "SELECT COUNT(*) FROM vw_dialpad_communications WHERE channel = 'call' AND date(event_at) > date('now')",
    )
    sms_extraction_sources = json_value_counts(
        conn,
        "dialpad_sms_messages",
        "raw_json",
        "$.extraction_source",
    )
    sms_direction_sources = json_value_counts(
        conn,
        "dialpad_sms_messages",
        "raw_json",
        "$.direction_source",
    )
    sms_inferred_direction_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM dialpad_sms_messages
        WHERE COALESCE(json_extract(raw_json, '$.direction_source'), '') = 'inferred'
        """,
    )
    voice_source_id_status = json_value_counts(
        conn,
        "dialpad_voice_events",
        "raw_json",
        "$.source_id_status",
    )
    voice_transcript_status = json_value_counts(
        conn,
        "dialpad_voice_events",
        "raw_json",
        "$.transcript_status",
    )
    sms_department_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM dialpad_sms_threads
        WHERE COALESCE(department, '') != '' OR COALESCE(school, '') != ''
        """,
    )
    voice_department_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM vw_dialpad_communications
        WHERE channel = 'call'
          AND (COALESCE(department, '') != '' OR COALESCE(school, '') != '')
        """,
    )
    transcript_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM vw_dialpad_communications
        WHERE has_transcript = 1
        """
    )
    transcript_by_event_type = {
        row[0]: row[1]
        for row in conn.execute(
            """
            SELECT event_type, COUNT(*)
            FROM vw_dialpad_communications
            WHERE has_transcript = 1
            GROUP BY event_type
            ORDER BY event_type
            """
        ).fetchall()
    }
    browser_voice_transcript_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM dialpad_voice_events
        WHERE COALESCE(voicemail_transcript, transcript_summary) IS NOT NULL
          AND COALESCE(voicemail_transcript, transcript_summary) != ''
        """,
    )
    browser_voice_recording_url_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM dialpad_voice_events
        WHERE COALESCE(recording_url, '') != ''
        """,
    )
    call_review_rows = count(conn, "SELECT COUNT(*) FROM dialpad_call_reviews")
    call_review_transcript_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM dialpad_call_reviews
        WHERE transcript_available = 1
           OR COALESCE(transcript_text, '') != ''
        """,
    )
    call_review_recap_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM dialpad_call_reviews
        WHERE recap_available = 1
           OR COALESCE(recap_text, '') != ''
        """,
    )
    call_review_action_item_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM dialpad_call_reviews
        WHERE action_items_available = 1
           OR COALESCE(action_items_json, '[]') NOT IN ('', '[]')
        """,
    )
    call_review_audio_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM dialpad_call_reviews
        WHERE audio_available = 1
        """,
    )
    call_review_status_counts = {
        row[0]: row[1]
        for row in conn.execute(
            """
            SELECT extraction_status, COUNT(*)
            FROM dialpad_call_reviews
            GROUP BY extraction_status
            ORDER BY extraction_status
            """
        ).fetchall()
    }
    latest_sms_import_run = import_run_summary(conn, "dialpad_sms")
    latest_voice_import_run = import_run_summary(conn, "dialpad_voice")
    latest_call_review_import_run = import_run_summary(conn, "dialpad_call_reviews")
    latest_daily_intake_import_run = import_run_summary(conn, "dialpad_daily_intake")
    target_search = dialpad_target_search_summary(conn)
    route_discovery = dialpad_route_discovery_summary(conn)
    daily_intake = dialpad_daily_intake_summary(conn, window_start)
    latest_voice_view_summaries = {}
    conversation_history_proof = {}
    conversation_history_rows = 0
    conversation_history_ai_action_rows = 0
    conversation_history_recording_action_rows = 0
    conversation_history_recording_or_transcript_url_rows = 0
    if latest_voice_import_run:
        latest_voice_view_summaries = latest_voice_import_run.get("metadata", {}).get("view_summaries", {})
        conversation_history_proof = latest_voice_view_summaries.get("conversation_history", {})
        conversation_history_rows = conversation_history_proof.get("rows", 0) or 0
        conversation_history_ai_action_rows = conversation_history_proof.get("ai_action_rows", 0) or 0
        conversation_history_recording_action_rows = conversation_history_proof.get("recording_action_rows", 0) or 0
        conversation_history_recording_or_transcript_url_rows = (
            conversation_history_proof.get("recording_or_transcript_url_rows", 0) or 0
        )
    required_rates = [
        sms_coverage["message_id"]["fill_rate"],
        sms_coverage["body"]["fill_rate"],
        sms_coverage["message_at"]["fill_rate"] if sms_total else 100.0,
        sms_coverage["direction"]["fill_rate"] if sms_total else 100.0,
        voice_coverage["communication_id"]["fill_rate"],
        voice_coverage["event_at"]["fill_rate"] if voice_total else 100.0,
        voice_coverage["direction"]["fill_rate"] if voice_total else 100.0,
    ]
    blockers = []
    if sms_total == 0 and voice_total == 0:
        blockers.append("No Dialpad communications loaded.")
    if sms_total and sms_coverage["direction"]["fill_rate"] < 80:
        blockers.append("Dialpad SMS direction coverage is below readiness threshold.")
    if sms_total and sms_coverage["message_at"]["fill_rate"] < 95:
        blockers.append("Dialpad SMS source timestamp coverage is below 95% readiness threshold.")
    if voice_total and voice_coverage["event_at"]["fill_rate"] < 80:
        blockers.append("Dialpad voice event timestamp coverage is below readiness threshold.")
    if future_sms:
        blockers.append("Dialpad SMS has future source timestamps that need review.")
    if future_voice:
        blockers.append("Dialpad voice has future source timestamps that need review.")
    if latest_sms_import_run and latest_sms_import_run.get("status") == "error":
        blockers.append("Latest Dialpad SMS import run failed.")
    if latest_voice_import_run and latest_voice_import_run.get("status") == "error":
        blockers.append("Latest Dialpad voice import run failed.")
    if latest_daily_intake_import_run and latest_daily_intake_import_run.get("status") == "blocked":
        blockers.append("Latest Dialpad daily intake was blocked; discovery fallback review is required.")
    if latest_daily_intake_import_run and latest_daily_intake_import_run.get("status") == "partial":
        blockers.append("Latest Dialpad daily intake was partial; review route discovery before sync/upload.")
    if latest_voice_import_run and latest_voice_import_run.get("status") == "success":
        if "voicemails" in latest_voice_view_summaries and latest_voice_view_summaries["voicemails"].get("transcript_rows", 0) == 0:
            blockers.append("Latest Dialpad voice proof did not capture visible voicemail transcript rows.")
        if "recordings" in latest_voice_view_summaries and not latest_voice_view_summaries["recordings"].get("availability", {}).get("transcript_link_visible"):
            blockers.append("Latest Dialpad recording proof did not find visible call/recording transcript links.")
        if conversation_history_rows and conversation_history_ai_action_rows == 0:
            blockers.append("Latest Dialpad Conversation History proof did not capture visible AI transcript actions.")
        if conversation_history_rows and (
            conversation_history_recording_action_rows == 0
            and conversation_history_recording_or_transcript_url_rows == 0
        ):
            blockers.append("Latest Dialpad Conversation History proof did not capture recording/play access.")
    return {
        "status": status_for(required_rates, blockers),
        "sms_rows": sms_total,
        "voice_rows": voice_total,
        "recent_window_sms_rows": recent_sms,
        "recent_window_voice_rows": recent_voice,
        "future_sms_timestamp_rows": future_sms,
        "future_voice_timestamp_rows": future_voice,
        "transcript_or_summary_rows": transcript_rows,
        "transcript_or_summary_rows_by_event_type": transcript_by_event_type,
        "browser_voice_transcript_rows": browser_voice_transcript_rows,
        "browser_voice_recording_url_rows": browser_voice_recording_url_rows,
        "conversation_history_rows": conversation_history_rows,
        "conversation_history_ai_action_rows": conversation_history_ai_action_rows,
        "conversation_history_recording_action_rows": conversation_history_recording_action_rows,
        "conversation_history_recording_or_transcript_url_rows": conversation_history_recording_or_transcript_url_rows,
        "call_review_rows": call_review_rows,
        "call_review_transcript_rows": call_review_transcript_rows,
        "call_review_recap_rows": call_review_recap_rows,
        "call_review_action_item_rows": call_review_action_item_rows,
        "call_review_audio_rows": call_review_audio_rows,
        "call_review_status_counts": call_review_status_counts,
        "sms_extraction_sources": sms_extraction_sources,
        "sms_direction_sources": sms_direction_sources,
        "sms_inferred_direction_rows": sms_inferred_direction_rows,
        "voice_source_id_status": voice_source_id_status,
        "voice_transcript_status": voice_transcript_status,
        "sms_department_or_school_rows": sms_department_rows,
        "voice_department_or_school_rows": voice_department_rows,
        "latest_timestamp": latest(
            conn,
            "SELECT MAX(event_at) FROM vw_dialpad_communications WHERE date(event_at) IS NOT NULL",
        ),
        "sms_field_coverage": sms_coverage,
        "voice_field_coverage": voice_coverage,
        "latest_sms_import_run": latest_sms_import_run,
        "latest_voice_import_run": latest_voice_import_run,
        "latest_call_review_import_run": latest_call_review_import_run,
        "latest_daily_intake_import_run": latest_daily_intake_import_run,
        "daily_intake": daily_intake,
        "target_search": target_search,
        "route_discovery": route_discovery,
        "latest_target_search_import_run": target_search["latest_import_run"],
        "latest_route_discovery_import_run": route_discovery["latest_import_run"],
        "blockers": blockers,
    }


def pike13_section(conn, window_start, lookahead_end):
    route_discovery = source_route_discovery_summary(conn, "pike13")
    lesson_visit_metrics = {
        "lesson_visit_rows": 0,
        "completed_note_rows": 0,
        "missing_note_rows": 0,
        "no_show_rows": 0,
        "canceled_rows": 0,
        "trial_lesson_rows": 0,
        "note_text_rows": 0,
        "note_timestamp_rows": 0,
        "note_score_rows": 0,
        "latest_lesson_date": None,
        "lesson_visit_window_rows": 0,
        "note_score_coverage": {"filled": 0, "total": 0, "fill_rate": 0.0},
    }
    if table_exists(conn, "reminders"):
        lesson_row = conn.execute(
            """
            SELECT
                COUNT(*) AS lesson_visit_rows,
                SUM(CASE WHEN COALESCE(note_completed, 0) = 1 THEN 1 ELSE 0 END) AS completed_note_rows,
                SUM(CASE WHEN COALESCE(note_completed, 0) = 1 THEN 0 ELSE 1 END) AS missing_note_rows,
                SUM(CASE WHEN LOWER(COALESCE(attendance_status, '')) LIKE '%no show%' THEN 1 ELSE 0 END) AS no_show_rows,
                SUM(CASE WHEN LOWER(COALESCE(attendance_status, '')) LIKE '%cancel%' THEN 1 ELSE 0 END) AS canceled_rows,
                SUM(CASE WHEN LOWER(COALESCE(lesson_type, '')) LIKE '%trial%' THEN 1 ELSE 0 END) AS trial_lesson_rows,
                SUM(CASE WHEN COALESCE(notes_text, '') != '' THEN 1 ELSE 0 END) AS note_text_rows,
                SUM(CASE WHEN COALESCE(note_timestamp, '') != '' THEN 1 ELSE 0 END) AS note_timestamp_rows,
                SUM(CASE WHEN note_score IS NOT NULL THEN 1 ELSE 0 END) AS note_score_rows,
                MAX(lesson_date) AS latest_lesson_date,
                SUM(CASE WHEN date(lesson_date) >= date(:window_start) THEN 1 ELSE 0 END) AS lesson_visit_window_rows
            FROM reminders
            WHERE lesson_id IS NOT NULL
              AND lesson_id != ''
            """,
            {"window_start": window_start},
        ).fetchone()
        for key in lesson_visit_metrics:
            if key in {"latest_lesson_date", "note_score_coverage"}:
                continue
            lesson_visit_metrics[key] = lesson_row[key] or 0
        lesson_visit_metrics["latest_lesson_date"] = lesson_row["latest_lesson_date"]
        lesson_visit_metrics["note_score_coverage"] = {
            "filled": lesson_visit_metrics["note_score_rows"],
            "total": lesson_visit_metrics["lesson_visit_rows"],
            "fill_rate": percent(
                lesson_visit_metrics["note_score_rows"],
                lesson_visit_metrics["lesson_visit_rows"],
            ),
        }
    people_total, people_coverage = field_coverage(
        conn,
        "pike13_people",
        ["person_id", "full_name", "email_normalized", "phone_normalized", "membership_state", "source_url", "raw_text"],
    )
    visits_total, visits_coverage = field_coverage(
        conn,
        "pike13_visits",
        [
            "visit_id",
            "person_id",
            "service",
            "starts_at",
            "status",
            "instructor",
            "source_url",
            "raw_text",
        ],
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
    first_visit_rows = count(conn, "SELECT COUNT(*) FROM pike13_visits WHERE COALESCE(first_visit_flag, 0) = 1")
    report_backed_first_visit_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM pike13_visits
        WHERE COALESCE(first_visit_flag, 0) = 1
          AND COALESCE(raw_json, '') LIKE '%first_visits_report%'
        """,
    )
    event_enriched_visit_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM pike13_visits
        WHERE COALESCE(raw_json, '') LIKE '%event_enriched%'
           OR COALESCE(attendance_confirmed_flag, 0) = 1
           OR COALESCE(checked_in_flag, 0) = 1
           OR terms_accepted_flag IS NOT NULL
        """,
    )
    plan_enrichment_rows = count(
        conn,
        """
        SELECT COUNT(*)
        FROM pike13_plans_passes
        WHERE COALESCE(payer_name, '') != ''
           OR COALESCE(next_invoice_at, '') != ''
           OR terms_accepted_flag IS NOT NULL
        """,
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
        if route_discovery["rows"] and route_discovery["blocked_routes"] == 0:
            blockers.append(
                "Rich Pike13 lead/outcome visits are not loaded; route discovery can load Pike13 pages, "
                "but the extractor did not find visit/event IDs."
            )
        elif route_discovery["blocked_routes"]:
            blockers.append(
                "Rich Pike13 lead/outcome visits are not loaded; Pike13 route discovery has blocked routes."
            )
        else:
            blockers.append("Rich Pike13 lead/outcome visits are not loaded.")
    if visits_total and visits_coverage["starts_at"]["fill_rate"] < 80:
        blockers.append("Pike13 visit date coverage is below readiness threshold.")
    if lesson_visit_metrics["lesson_visit_rows"] == 0 and visits_total == 0:
        blockers.append("No existing lesson visits or rich Pike13 visits are loaded.")
    if visits_total:
        status = status_for(required_rates, blockers)
    elif lesson_visit_metrics["lesson_visit_rows"]:
        status = "partial"
    else:
        status = "blocked"
    return {
        "status": status,
        "people_rows": people_total,
        "visit_rows": visits_total,
        "rich_visit_rows": visits_total,
        "plan_pass_rows": plans_total,
        "first_visit_rows": first_visit_rows,
        "report_backed_first_visit_rows": report_backed_first_visit_rows,
        "event_enriched_visit_rows": event_enriched_visit_rows,
        "plan_enrichment_rows": plan_enrichment_rows,
        "window_plus_lookahead_visit_rows": recent_visits,
        **lesson_visit_metrics,
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
        "route_discovery": route_discovery,
        "latest_route_discovery_import_run": route_discovery["latest_import_run"],
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


def first_value_candidate_counts(conn, window_start):
    trusted_contact = """
        COALESCE(
            json_extract(
                CASE WHEN json_valid(COALESCE(c.raw_json, '{}')) THEN c.raw_json ELSE '{}' END,
                '$.trusted'
            ),
            0
        ) = 1
        AND LOWER(COALESCE(c.email_normalized, '')) NOT LIKE '%@schoolofrock.com'
    """
    row = conn.execute(
        f"""
        WITH candidates AS (
            SELECT s.deal_id
            FROM vw_stale_leads s
            JOIN hubspot_deals d ON d.deal_id = s.deal_id
            WHERE LOWER(COALESCE(s.stage, '')) NOT LIKE '%not a lead%'
              AND (
                  date(d.create_date) >= date(:window_start)
               OR date(d.last_contacted) >= date(:window_start)
               OR date(d.last_activity_date) >= date(:window_start)
               OR date(d.updated_at) >= date(:window_start)
               OR LOWER(COALESCE(d.follow_up_needed, '')) IN ('yes', 'true', '1', 'follow up needed')
              )
        ),
        candidate_contacts AS (
            SELECT DISTINCT candidates.deal_id, c.phone_normalized
            FROM candidates
            JOIN hubspot_contacts c ON instr(COALESCE(c.associated_deal_ids, ''), candidates.deal_id) > 0
            WHERE COALESCE(c.phone_normalized, '') != ''
              AND {trusted_contact}
        ),
        candidate_comms AS (
            SELECT DISTINCT candidate_contacts.deal_id
            FROM candidate_contacts
            JOIN vw_dialpad_communications comms
              ON comms.phone_normalized = candidate_contacts.phone_normalized
            WHERE date(comms.event_at) >= date(:window_start)
              AND date(comms.event_at) <= date('now')
        )
        SELECT
            (SELECT COUNT(*) FROM candidates) AS candidate_leads,
            (SELECT COUNT(DISTINCT deal_id) FROM candidate_contacts) AS candidate_leads_with_trusted_phone,
            (SELECT COUNT(*) FROM candidate_comms) AS candidate_leads_with_dialpad_comms
        """,
        {"window_start": window_start},
    ).fetchone()
    return {
        "candidate_leads": row["candidate_leads"] or 0,
        "candidate_leads_with_trusted_phone": row["candidate_leads_with_trusted_phone"] or 0,
        "candidate_leads_with_dialpad_comms": row["candidate_leads_with_dialpad_comms"] or 0,
    }


def first_value_section(conn, sources, matching, window_start):
    hubspot = sources.get("hubspot", {})
    dialpad = sources.get("dialpad", {})
    candidate_counts = first_value_candidate_counts(conn, window_start)
    blockers = []
    if hubspot.get("status") != "ready":
        blockers.append("HubSpot lead spine is not ready.")
    if dialpad.get("conversation_history_recording_or_transcript_url_rows", 0) == 0:
        blockers.append("Dialpad call-review URLs are not loaded.")
    if dialpad.get("call_review_transcript_rows", 0) == 0 and dialpad.get("call_review_recap_rows", 0) == 0:
        blockers.append("Dialpad call-review transcripts or recaps are not loaded.")
    if (
        matching.get("matched_hubspot_deals", 0) == 0
        and matching.get("matched_hubspot_contacts", 0) == 0
    ):
        blockers.append("No deterministic HubSpot matches are available.")
    if candidate_counts["candidate_leads_with_dialpad_comms"] == 0:
        target_search = dialpad.get("target_search", {})
        if not target_search.get("rows"):
            blockers.append("No lead-attention candidates have matched Dialpad communication evidence; targeted Dialpad discovery is required.")
        elif target_search.get("targets_found"):
            blockers.append("Targeted Dialpad discovery found evidence, but it is not yet matched into lead-attention communications.")
        else:
            blockers.append("Targeted Dialpad discovery did not find matched communications for current lead-attention candidates.")
    if not blockers:
        status = "ready"
    elif dialpad.get("conversation_history_recording_or_transcript_url_rows", 0) or matching.get("rows", 0):
        status = "partial"
    else:
        status = "blocked"
    return {
        "status": status,
        "report_ready": not blockers,
        "blockers": blockers,
        "call_review_url_rows": dialpad.get("conversation_history_recording_or_transcript_url_rows", 0),
        "call_review_transcript_rows": dialpad.get("call_review_transcript_rows", 0),
        "call_review_recap_rows": dialpad.get("call_review_recap_rows", 0),
        "matched_hubspot_deals": matching.get("matched_hubspot_deals", 0),
        "matched_hubspot_contacts": matching.get("matched_hubspot_contacts", 0),
        **candidate_counts,
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
    report["first_value"] = first_value_section(conn, report["sources"], report["matching"], window_start)
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
