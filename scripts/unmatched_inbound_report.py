#!/usr/bin/env python3
import argparse
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import ensure_lead_followup_schema  # noqa: E402
from scripts.lead_attention_report import sanitize_report_url, school_params, school_patterns, school_where  # noqa: E402


DEFAULT_OUTPUT = "outputs/progress/unmatched_inbound_report.md"
DEFAULT_SCHOOL = "West U"


def utc_today():
    return datetime.now(timezone.utc).date()


def window_start(days):
    return (utc_today() - timedelta(days=days)).isoformat()


def table_cell(value):
    if value is None:
        return ""
    return str(value).replace("|", "/").replace("\n", " ").strip()


def source_link(url):
    url = sanitize_report_url(url)
    return f"[source]({url})" if url else "none"


def fetch_unmatched_rows(conn, school=DEFAULT_SCHOOL, window_days=2, limit=50):
    ensure_lead_followup_schema(conn)
    conn.row_factory = sqlite3.Row
    start = window_start(window_days)
    patterns = school_patterns(school)
    params = {"window_start": start, "limit": limit, **school_params(patterns)}
    rows = conn.execute(
        f"""
        SELECT
            communication_id,
            source_table,
            channel,
            event_type,
            direction,
            event_at,
            school,
            department,
            source_url,
            has_transcript,
            has_later_outbound_followup,
            match_status,
            action_status
        FROM vw_unmatched_dialpad_inbound u
        WHERE date(u.event_at) >= date(:window_start)
          AND {school_where('u', patterns)}
        ORDER BY
            CASE action_status
                WHEN 'possible_lead_not_in_hubspot' THEN 0
                WHEN 'matched_pike13_review' THEN 1
                ELSE 2
            END,
            event_at DESC,
            communication_id
        LIMIT :limit
        """,
        params,
    ).fetchall()
    return rows, start


def summarize_rows(rows):
    summary = {
        "unmatched_inbound_rows": len(rows),
        "possible_lead_not_in_hubspot_rows": 0,
        "no_later_outbound_followup_rows": 0,
        "missed_call_rows": 0,
        "voicemail_rows": 0,
        "inbound_sms_rows": 0,
        "matched_pike13_only_rows": 0,
        "transcript_available_rows": 0,
    }
    for row in rows:
        if row["action_status"] == "possible_lead_not_in_hubspot":
            summary["possible_lead_not_in_hubspot_rows"] += 1
        if not row["has_later_outbound_followup"]:
            summary["no_later_outbound_followup_rows"] += 1
        if row["event_type"] == "missed_call":
            summary["missed_call_rows"] += 1
        if row["event_type"] == "voicemail":
            summary["voicemail_rows"] += 1
        if row["channel"] == "sms" and row["direction"] == "inbound":
            summary["inbound_sms_rows"] += 1
        if row["match_status"] == "matched_pike13_only":
            summary["matched_pike13_only_rows"] += 1
        if row["has_transcript"]:
            summary["transcript_available_rows"] += 1
    return summary


def render_report(rows, school=DEFAULT_SCHOOL, window_days=2, window_start_value=None):
    window_start_value = window_start_value or window_start(window_days)
    summary = summarize_rows(rows)
    status = "ready for review" if rows else "clear"
    lines = [
        "# Unmatched Inbound Dialpad Report",
        "",
        f"Status: **{status}**",
        f"School: **{school}**",
        f"Window: **{window_start_value}** through **{utc_today().isoformat()}** ({window_days} days)",
        "",
        "## Summary",
        "",
        f"- Unmatched inbound communications: {summary['unmatched_inbound_rows']}",
        f"- Possible leads not in HubSpot: {summary['possible_lead_not_in_hubspot_rows']}",
        f"- Rows without later outbound follow-up: {summary['no_later_outbound_followup_rows']}",
        f"- Missed calls: {summary['missed_call_rows']}",
        f"- Voicemails: {summary['voicemail_rows']}",
        f"- Inbound SMS rows: {summary['inbound_sms_rows']}",
        f"- Pike13-only phone matches: {summary['matched_pike13_only_rows']}",
        f"- Transcript/summary available rows: {summary['transcript_available_rows']}",
        "",
        "## Rows",
        "",
        "| Communication ID | Channel | Event type | Direction | Event at | School | Match status | Action status | Later outbound? | Transcript? | Source URL |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | --- |",
    ]
    if rows:
        for row in rows:
            lines.append(
                "| {communication_id} | {channel} | {event_type} | {direction} | {event_at} | {school} | "
                "{match_status} | {action_status} | {later} | {transcript} | {source_url} |".format(
                    communication_id=table_cell(row["communication_id"]),
                    channel=table_cell(row["channel"]),
                    event_type=table_cell(row["event_type"]),
                    direction=table_cell(row["direction"]),
                    event_at=table_cell(row["event_at"]),
                    school=table_cell(row["school"]),
                    match_status=table_cell(row["match_status"]),
                    action_status=table_cell(row["action_status"]),
                    later="yes" if row["has_later_outbound_followup"] else "no",
                    transcript="yes" if row["has_transcript"] else "no",
                    source_url=source_link(row["source_url"]),
                )
            )
    else:
        lines.append("| none | none | none | none | none | none | none | none | no | no | none |")
    lines.extend(
        [
            "",
            "_This report intentionally excludes customer names, phone numbers, email addresses, SMS bodies, transcripts, recaps, raw lesson notes, and call summaries._",
            "",
        ]
    )
    return "\n".join(lines)


def build_report(db_path, school, window_days, limit):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows, start = fetch_unmatched_rows(conn, school, window_days, limit)
        conn.commit()
    finally:
        conn.close()
    return render_report(rows, school, window_days, start)


def main():
    parser = argparse.ArgumentParser(description="Generate a sanitized report of inbound Dialpad communications without trusted HubSpot matches.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--school", default=DEFAULT_SCHOOL)
    parser.add_argument("--window-days", type=int, default=2)
    parser.add_argument("--limit", type=int, default=50)
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
