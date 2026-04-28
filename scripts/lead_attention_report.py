#!/usr/bin/env python3
import argparse
import re
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import ensure_lead_followup_schema  # noqa: E402
from source_completeness import DEFAULT_WINDOW_DAYS  # noqa: E402


DEFAULT_OUTPUT = "outputs/progress/lead_attention_report.md"
DEFAULT_SCHOOL = "West U"
MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def utc_today():
    return datetime.now(timezone.utc).date()


def window_start(days):
    return (utc_today() - timedelta(days=days)).isoformat()


def school_patterns(school):
    value = (school or "").strip().lower()
    if value in {"west u", "westu", "west university", "west university place"}:
        return ["%west u%", "%westu%", "%west university%"]
    if value in {"heights", "the heights"}:
        return ["%heights%"]
    return [f"%{value}%"] if value else ["%"]


def school_where(alias, patterns):
    clauses = [f"LOWER(COALESCE({alias}.school, '')) LIKE :school_{i}" for i, _ in enumerate(patterns)]
    return "(" + " OR ".join(clauses) + ")"


def school_params(patterns):
    return {f"school_{i}": pattern for i, pattern in enumerate(patterns)}


def trusted_contact_sql():
    return """
        COALESCE(
            json_extract(
                CASE WHEN json_valid(COALESCE(raw_json, '{}')) THEN raw_json ELSE '{}' END,
                '$.trusted'
            ),
            0
        ) = 1
        AND LOWER(COALESCE(email_normalized, '')) NOT LIKE '%@schoolofrock.com'
    """


def fetch_candidate_leads(conn, school, window_days, limit):
    start = window_start(window_days)
    patterns = school_patterns(school)
    params = {"window_start": start, "limit": limit, **school_params(patterns)}
    rows = conn.execute(
        f"""
        SELECT
            s.deal_id,
            s.stage,
            s.owner,
            s.school,
            s.create_date,
            s.last_contacted,
            s.last_activity_date,
            s.follow_up_needed,
            s.days_since_last_touch,
            s.risk_reason,
            s.source_url,
            d.updated_at
        FROM vw_stale_leads s
        JOIN hubspot_deals d ON d.deal_id = s.deal_id
        WHERE {school_where('s', patterns)}
          AND LOWER(COALESCE(s.stage, '')) NOT LIKE '%not a lead%'
          AND (
              date(d.create_date) >= date(:window_start)
              OR date(d.last_contacted) >= date(:window_start)
              OR date(d.last_activity_date) >= date(:window_start)
              OR date(d.updated_at) >= date(:window_start)
              OR LOWER(COALESCE(d.follow_up_needed, '')) IN ('yes', 'true', '1', 'follow up needed')
          )
        ORDER BY
            CASE s.risk_reason
                WHEN 'follow_up_needed' THEN 0
                WHEN 'overdue_task' THEN 1
                WHEN 'missing_touch_date' THEN 2
                ELSE 3
            END,
            COALESCE(s.days_since_last_touch, 9999) DESC,
            s.deal_id
        LIMIT :limit
        """,
        params,
    ).fetchall()
    return rows, start


def deal_phone_keys(conn, deal_id):
    rows = conn.execute(
        f"""
        SELECT DISTINCT phone_normalized
        FROM hubspot_contacts
        WHERE COALESCE(phone_normalized, '') != ''
          AND instr(COALESCE(associated_deal_ids, ''), ?) > 0
          AND {trusted_contact_sql()}
        """,
        (deal_id,),
    ).fetchall()
    return [row["phone_normalized"] for row in rows]


def communication_evidence(conn, phones, lead_start):
    if not phones:
        return {
            "matched_communication_count": 0,
            "last_communication_at": None,
            "call_review_url_count": 0,
            "transcript_count": 0,
            "recap_count": 0,
            "source_urls": [],
        }
    placeholders = ", ".join(f":phone_{i}" for i, _ in enumerate(phones))
    params = {f"phone_{i}": phone for i, phone in enumerate(phones)}
    params["lead_start"] = lead_start
    summary = conn.execute(
        f"""
        SELECT
            COUNT(*) AS matched_communication_count,
            MAX(c.event_at) AS last_communication_at,
            COUNT(DISTINCT CASE
                WHEN COALESCE(c.source_url, '') LIKE '%dialpad.com/callhistory/callreview/%'
                THEN c.source_url
            END) AS call_review_url_count,
            SUM(CASE
                WHEN cr.transcript_available = 1 OR COALESCE(cr.transcript_text, '') != ''
                THEN 1 ELSE 0
            END) AS transcript_count,
            SUM(CASE
                WHEN cr.recap_available = 1 OR COALESCE(cr.recap_text, '') != ''
                THEN 1 ELSE 0
            END) AS recap_count
        FROM vw_dialpad_communications c
        LEFT JOIN dialpad_call_reviews cr
          ON cr.call_review_url = c.source_url
          OR cr.voice_event_id = c.communication_id
          OR cr.call_id = c.communication_id
        WHERE c.phone_normalized IN ({placeholders})
          AND date(c.event_at) >= date(:lead_start)
          AND date(c.event_at) <= date('now')
        """,
        params,
    ).fetchone()
    urls = [
        row["source_url"]
        for row in conn.execute(
            f"""
            SELECT DISTINCT c.source_url
            FROM vw_dialpad_communications c
            WHERE c.phone_normalized IN ({placeholders})
              AND date(c.event_at) >= date(:lead_start)
              AND date(c.event_at) <= date('now')
              AND COALESCE(c.source_url, '') != ''
            ORDER BY c.event_at DESC
            LIMIT 3
            """,
            params,
        ).fetchall()
    ]
    return {
        "matched_communication_count": summary["matched_communication_count"] or 0,
        "last_communication_at": summary["last_communication_at"],
        "call_review_url_count": summary["call_review_url_count"] or 0,
        "transcript_count": summary["transcript_count"] or 0,
        "recap_count": summary["recap_count"] or 0,
        "source_urls": urls,
    }


def evidence_start_for_lead(row, fallback_start):
    for field in ("create_date", "last_activity_date", "last_contacted"):
        value = row[field]
        if value and len(str(value)) >= 10 and str(value)[4:5] == "-" and str(value)[7:8] == "-":
            return str(value)[:10]
    return fallback_start


def last_touch(row, evidence):
    for value in (
        evidence.get("last_communication_at"),
        row["last_contacted"],
        row["last_activity_date"],
        row["create_date"],
    ):
        value = clean_timestamp(value)
        if value:
            return value
    return "unknown"


def timestamp_date(value):
    value = str(value or "").strip()
    if len(value) >= 10 and value[4:5] == "-" and value[7:8] == "-":
        try:
            return datetime.fromisoformat(value[:10]).date()
        except ValueError:
            return None
    match = re.search(r"\b([A-Z][a-z]{2})\s+(\d{1,2}),\s+(\d{4})\b", value)
    if match:
        month, day, year = match.groups()
        month_number = MONTHS.get(month.lower())
        if month_number:
            return datetime(int(year), month_number, int(day)).date()
    return None


def clean_timestamp(value):
    value = str(value or "").strip()
    if not value or value.lower() in {"details", "- deal", "- display deal"}:
        return None
    parsed = timestamp_date(value)
    if parsed and parsed > utc_today():
        return None
    return value


def source_links(row, evidence):
    links = []
    if row["source_url"]:
        links.append(f"[HubSpot]({row['source_url']})")
    for index, url in enumerate(evidence.get("source_urls") or [], start=1):
        links.append(f"[Dialpad {index}]({url})")
    return ", ".join(links) if links else "none"


def table_cell(value):
    if value is None:
        return ""
    return str(value).replace("|", "/").replace("\n", " ").strip()


def build_attention_rows(conn, school=DEFAULT_SCHOOL, window_days=DEFAULT_WINDOW_DAYS, limit=25):
    ensure_lead_followup_schema(conn)
    conn.row_factory = sqlite3.Row
    candidates, start = fetch_candidate_leads(conn, school, window_days, limit)
    rows = []
    for candidate in candidates:
        phones = deal_phone_keys(conn, candidate["deal_id"])
        evidence = communication_evidence(conn, phones, evidence_start_for_lead(candidate, start))
        rows.append(
            {
                "deal_id": candidate["deal_id"],
                "stage": candidate["stage"] or "unknown",
                "school": candidate["school"] or "unknown",
                "owner": candidate["owner"] or "unknown",
                "last_touch": last_touch(candidate, evidence),
                "risk_reason": candidate["risk_reason"] or "unknown",
                "matched_communication_count": evidence["matched_communication_count"],
                "call_review_url_count": evidence["call_review_url_count"],
                "transcript_count": evidence["transcript_count"],
                "recap_count": evidence["recap_count"],
                "source_links": source_links(candidate, evidence),
            }
        )
    return rows, start


def render_report(rows, school=DEFAULT_SCHOOL, window_days=DEFAULT_WINDOW_DAYS, window_start_value=None):
    window_start_value = window_start_value or window_start(window_days)
    rows_with_comms = sum(1 for row in rows if row["matched_communication_count"])
    rows_with_transcripts = sum(1 for row in rows if row["transcript_count"])
    rows_with_recaps = sum(1 for row in rows if row["recap_count"])
    status = "ready for review" if rows and rows_with_comms else "not ready"
    lines = [
        "# Lead Attention Report",
        "",
        f"Status: **{status}**",
        f"School: **{school}**",
        f"Window: **{window_start_value}** through **{utc_today().isoformat()}** ({window_days} days)",
        "",
        "## Summary",
        "",
        f"- Candidate leads needing attention: {len(rows)}",
        f"- Candidate leads with matched Dialpad communications: {rows_with_comms}",
        f"- Candidate leads with call-review transcripts: {rows_with_transcripts}",
        f"- Candidate leads with call-review recaps: {rows_with_recaps}",
        "",
        "## Attention Rows",
        "",
        "| Deal ID | Stage | School | Owner | Last touch | Risk reason | Matched comms | Call-review URLs | Transcripts | Recaps | Source URLs |",
        "| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    if rows:
        for row in rows:
            lines.append(
                "| {deal_id} | {stage} | {school} | {owner} | {last_touch} | {risk_reason} | "
                "{matched_communication_count} | {call_review_url_count} | {transcript_count} | {recap_count} | {source_links} |".format(
                    **{key: table_cell(value) for key, value in row.items()}
                )
            )
    else:
        lines.append("| none | none | none | none | none | none | 0 | 0 | 0 | 0 | none |")
    lines.extend(
        [
            "",
            "_This report intentionally excludes customer names, phone numbers, SMS bodies, transcripts, transcript summaries, raw lesson notes, and call summaries._",
            "",
        ]
    )
    return "\n".join(lines)


def build_report(db_path, school, window_days, limit):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows, start = build_attention_rows(conn, school, window_days, limit)
        conn.commit()
    finally:
        conn.close()
    return render_report(rows, school, window_days, start)


def main():
    parser = argparse.ArgumentParser(description="Generate a sanitized lead-attention Markdown report.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--school", default=DEFAULT_SCHOOL)
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--print", action="store_true", dest="print_output")
    args = parser.parse_args()

    markdown = build_report(args.db, args.school, args.window_days, args.limit)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")
    if args.print_output:
        print(markdown)
    else:
        print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
