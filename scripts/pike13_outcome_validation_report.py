#!/usr/bin/env python3
import argparse
import sqlite3
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import ensure_lead_followup_schema  # noqa: E402


DEFAULT_DB = "outputs/lead_intelligence/lead_intelligence_working.db"
DEFAULT_OUTPUT = "outputs/progress/pike13_outcome_validation.md"


def bool_value(value):
    return bool(value) and str(value).strip() not in {"0", "false", "False"}


def classify_outcome(row, today=None):
    today = today or date.today().isoformat()
    status = (row.get("status") or "").lower()
    service_date = (row.get("starts_at") or "")[:10]
    if bool_value(row.get("canceled_flag")) or "cancel" in status:
        return "canceled"
    if bool_value(row.get("no_show_flag")) or "no show" in status or "noshow" in status:
        return "no-show"
    if bool_value(row.get("has_conversion_plan")):
        return "converted"
    if service_date and service_date > today and (bool_value(row.get("enrolled_flag")) or "enrolled" in status):
        return "scheduled"
    if bool_value(row.get("attendance_confirmed_flag")) or bool_value(row.get("checked_in_flag")) or "complete" in status:
        return "attended-not-converted"
    if bool_value(row.get("enrolled_flag")) or "enrolled" in status:
        return "scheduled"
    return "unknown"


def conversion_plan_case():
    return """
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM pike13_plans_passes pp
                WHERE pp.person_id = v.person_id
                  AND LOWER(COALESCE(pp.name, '')) NOT LIKE '%trial%'
                  AND LOWER(COALESCE(pp.name, '')) NOT LIKE '%free%'
                  AND (
                      COALESCE(pp.starts_at, '') != ''
                      OR COALESCE(pp.next_invoice_at, '') != ''
                      OR COALESCE(pp.payer_name, '') != ''
                  )
            )
            THEN 1 ELSE 0
        END
    """


def fetch_validation_rows(conn, school, start_date, end_date, limit):
    params = {"school": school, "start_date": start_date, "end_date": end_date, "limit": limit}
    rows = conn.execute(
        f"""
        SELECT
            v.visit_id,
            v.person_id,
            v.service,
            v.instructor,
            v.starts_at,
            v.status,
            COALESCE(v.first_visit_flag, 0) AS first_visit_flag,
            COALESCE(v.attendance_confirmed_flag, 0) AS attendance_confirmed_flag,
            COALESCE(v.checked_in_flag, 0) AS checked_in_flag,
            COALESCE(v.enrolled_flag, 0) AS enrolled_flag,
            COALESCE(v.no_show_flag, 0) AS no_show_flag,
            COALESCE(v.canceled_flag, 0) AS canceled_flag,
            COALESCE(v.waiver_flag, 0) AS waiver_flag,
            v.terms_accepted_flag,
            {conversion_plan_case()} AS has_conversion_plan,
            COUNT(pp.plan_pass_id) AS plan_pass_rows,
            SUM(CASE WHEN COALESCE(pp.payer_name, '') != '' THEN 1 ELSE 0 END) AS payer_rows,
            SUM(CASE WHEN COALESCE(pp.next_invoice_at, '') != '' THEN 1 ELSE 0 END) AS next_invoice_rows,
            SUM(CASE WHEN pp.terms_accepted_flag = 1 THEN 1 ELSE 0 END) AS accepted_terms_rows,
            SUM(CASE WHEN pp.terms_accepted_flag = 0 THEN 1 ELSE 0 END) AS unaccepted_terms_rows
        FROM pike13_visits v
        LEFT JOIN pike13_plans_passes pp ON pp.person_id = v.person_id
        WHERE COALESCE(v.first_visit_flag, 0) = 1
          AND (:school = '' OR COALESCE(v.school, '') = :school)
          AND date(v.starts_at) BETWEEN date(:start_date) AND date(:end_date)
        GROUP BY v.visit_id
        ORDER BY date(v.starts_at), v.visit_id
        LIMIT :limit
        """,
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def summarize_rows(rows, today=None):
    summary = {
        "total": len(rows),
        "scheduled": 0,
        "attended-not-converted": 0,
        "converted": 0,
        "canceled": 0,
        "no-show": 0,
        "unknown": 0,
        "terms_or_waiver_attention": 0,
    }
    classified = []
    for row in rows:
        outcome = classify_outcome(row, today)
        summary[outcome] += 1
        if bool_value(row.get("waiver_flag")) or row.get("terms_accepted_flag") == 0 or row.get("unaccepted_terms_rows"):
            summary["terms_or_waiver_attention"] += 1
        classified.append({**row, "outcome": outcome})
    return summary, classified


def render_report(summary, rows, school, start_date, end_date):
    lines = [
        "# Pike13 Outcome Validation",
        "",
        f"School: {school or 'all'}",
        f"Window: {start_date} through {end_date}",
        "",
        "## Summary",
        "",
        f"- First visit rows: {summary['total']}",
        f"- Scheduled: {summary['scheduled']}",
        f"- Attended not converted: {summary['attended-not-converted']}",
        f"- Converted: {summary['converted']}",
        f"- Canceled: {summary['canceled']}",
        f"- No-show: {summary['no-show']}",
        f"- Unknown: {summary['unknown']}",
        f"- Terms/waiver attention rows: {summary['terms_or_waiver_attention']}",
        "",
        "## Rows",
        "",
        "| Date | Outcome | Service | Instructor | Status | Signals |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        signals = []
        if bool_value(row.get("attendance_confirmed_flag")):
            signals.append("attendance confirmed")
        if bool_value(row.get("checked_in_flag")):
            signals.append("checked in")
        if bool_value(row.get("enrolled_flag")):
            signals.append("enrolled")
        if bool_value(row.get("has_conversion_plan")):
            signals.append("conversion plan")
        if row.get("payer_rows"):
            signals.append("payer")
        if row.get("next_invoice_rows"):
            signals.append("next invoice")
        if bool_value(row.get("waiver_flag")):
            signals.append("waiver")
        if row.get("terms_accepted_flag") == 0 or row.get("unaccepted_terms_rows"):
            signals.append("terms not accepted")
        lines.append(
            "| {date} | {outcome} | {service} | {instructor} | {status} | {signals} |".format(
                date=(row.get("starts_at") or "")[:10],
                outcome=row.get("outcome") or "unknown",
                service=clean_cell(row.get("service")),
                instructor=clean_cell(row.get("instructor")),
                status=clean_cell(row.get("status")),
                signals=clean_cell(", ".join(signals) or "none"),
            )
        )
    lines.extend(
        [
            "",
            "_This report intentionally excludes customer names, emails, phones, raw Pike13 text, notes, and source URLs._",
            "",
        ]
    )
    return "\n".join(lines)


def clean_cell(value):
    return str(value or "").replace("|", "\\|").replace("\n", " ").strip()


def build_report(db_path, school, start_date, end_date, limit):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_lead_followup_schema(conn)
    try:
        rows = fetch_validation_rows(conn, school, start_date, end_date, limit)
        summary, classified = summarize_rows(rows)
        return render_report(summary, classified, school, start_date, end_date)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Generate a sanitized Pike13 first-visit outcome validation report.")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--school", default="West U")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    markdown = build_report(args.db, args.school, args.start_date, args.end_date, args.limit)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(markdown, encoding="utf-8")
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
