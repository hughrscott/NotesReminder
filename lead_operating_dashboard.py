import hashlib
import json
import sqlite3
from collections import Counter
from datetime import date, datetime, timedelta, timezone

from lead_gap_analysis import build_gap_report
from trial_followup_intelligence import build_trial_followup_report


DEFAULT_SCHOOL = "West U"
DASHBOARD_PERIODS = ("daily", "weekly", "monthly")


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_date(value):
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value))


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


def table_exists(conn, name):
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (name,),
        ).fetchone()
    )


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
    return {"status": status, "runs": [dict(row) for row in rows], "counts": counts}


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
    if table_exists(conn, "hubspot_deals"):
        school_sql, school_params = school_clause("d", school)
        sources = top_counts(
            conn,
            f"""
            SELECT COALESCE(NULLIF(lead_source, ''), NULLIF(marketing_source, ''), 'unknown') AS source, COUNT(*) AS leads
            FROM hubspot_deals d
            WHERE date(COALESCE(NULLIF(create_date, ''), NULLIF(updated_at, ''))) BETWEEN date(:start) AND date(:end)
              AND {school_sql}
            GROUP BY COALESCE(NULLIF(lead_source, ''), NULLIF(marketing_source, ''), 'unknown')
            ORDER BY leads DESC, source
            LIMIT 10
            """,
            {"start": start_date, "end": end_date, **school_params},
        )
    return {"staff_trial_counts": staff, "hubspot_source_counts": sources}


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
                "school": row["school"],
                "stage": row["outcome"],
                "reason_code": row["followup_status"],
            }
        )
    return {
        "summary": dict(sorted(Counter(item["reason_code"] for item in items).items())),
        "items": items[:limit],
        "truncated": len(items) > limit,
    }


def build_snapshot(conn, period, start_date=None, end_date=None, as_of=None, school=DEFAULT_SCHOOL, limit=50):
    if period not in DASHBOARD_PERIODS:
        raise ValueError(f"period must be one of: {', '.join(DASHBOARD_PERIODS)}")
    if not start_date or not end_date:
        start_date, end_date = window_for_period(period, as_of)
    conn.row_factory = sqlite3.Row
    gap = build_gap_report(conn, hubspot_school(school), limit=500, start_date=start_date, end_date=end_date)
    trial = build_trial_followup_report(conn, start_date, end_date, pike13_school(school))
    pike13 = pike13_outcomes(conn, start_date, end_date, school)
    communications = communication_counts(conn, start_date, end_date, school)
    recordings = recording_status(conn, start_date, end_date, school)
    contacted = sum(1 for row in gap["rows"] if row.get("outreach_evidence_found"))
    trial_expected = sum(1 for row in gap["rows"] if row.get("trial_expected"))
    return {
        "dashboard_type": period,
        "generated_at": utc_now_iso(),
        "school": school,
        "window": {"start": start_date, "end": end_date},
        "source_freshness": source_freshness(conn),
        "funnel_counts": {
            "hubspot_leads": gap["summary"]["rows_reviewed"],
            "contacted": contacted,
            "trial_scheduled_or_expected": trial_expected,
            "pike13_first_visits": pike13.get("first_visits", 0),
            "attended": pike13.get("attended", 0),
            "no_show": pike13.get("no_show", 0),
            "canceled": pike13.get("canceled", 0),
            "converted": conversion_count(conn, start_date, end_date, school),
        },
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
        "## Funnel Counts",
        "",
        _markdown_counts(snapshot["funnel_counts"]),
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
        _markdown_table(
            snapshot["exception_queue"]["items"],
            [
                ("lead_ref", "Lead"),
                ("school", "School"),
                ("stage", "Stage"),
                ("diagnostic_area", "Area"),
                ("reason_code", "Reason"),
            ],
        ),
        "",
        "_This dashboard is sanitized: broad sections exclude customer names, emails, phones, message bodies, transcripts, raw page text, screenshots, source URLs, and audio paths._",
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
