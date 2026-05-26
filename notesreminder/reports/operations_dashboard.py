"""Static operations dashboard rendering.

This report intentionally reuses the lead operating dashboard snapshot logic so
the HTML view stays aligned with the existing Markdown/JSON dashboards.
"""

from __future__ import annotations

import html
import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from notesreminder.reports.lead_operating_dashboard import (
    DEFAULT_SCHOOL,
    build_snapshot,
    school_aliases,
)


DEFAULT_SCHOOLS = (DEFAULT_SCHOOL, "The Heights")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _school_filter_sql(school: str, *, school_alias: str = "s") -> tuple[str, dict[str, str]]:
    aliases = school_aliases(school)
    if not aliases:
        return "1=1", {}
    params = {f"school_{index}": value for index, value in enumerate(aliases)}
    placeholders = ", ".join(f":{key}" for key in params)
    return (
        f"(LOWER(COALESCE({school_alias}.school_code, '')) IN ({placeholders}) "
        f"OR LOWER(COALESCE({school_alias}.school_name, '')) IN ({placeholders}))",
        params,
    )


def _missing_notes_by_instructor(
    conn: sqlite3.Connection,
    *,
    start_date: str,
    end_date: str,
    school: str,
    limit: int,
) -> list[dict]:
    school_sql, school_params = _school_filter_sql(school)
    rows = conn.execute(
        f"""
        SELECT
            COALESCE(NULLIF(i.instructor_name, ''), 'unknown') AS instructor_name,
            COUNT(*) AS reportable_lessons,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 0 THEN 1 ELSE 0 END) AS missing_notes,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END) AS completed_notes,
            ROUND(
                100.0 * SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0),
                1
            ) AS completion_rate
        FROM lessons l
        JOIN schools s ON s.school_id = l.school_id
        LEFT JOIN instructors i ON i.instructor_id = l.instructor_id
        LEFT JOIN lesson_notes n ON n.lesson_id = l.lesson_id
        WHERE date(l.lesson_date) BETWEEN date(:start) AND date(:end)
          AND {school_sql}
          AND COALESCE(l.lesson_is_reportable, 0) = 1
        GROUP BY COALESCE(NULLIF(i.instructor_name, ''), 'unknown')
        HAVING missing_notes > 0
        ORDER BY missing_notes DESC, completion_rate ASC, instructor_name
        LIMIT :limit
        """,
        {"start": start_date, "end": end_date, "limit": limit, **school_params},
    ).fetchall()
    return [dict(row) for row in rows]


def _sum_snapshot_metric(snapshots: list[dict], section: str, metric: str) -> int | float:
    return sum((snapshot.get(section) or {}).get(metric, 0) or 0 for snapshot in snapshots)


def build_operations_dashboard(
    conn: sqlite3.Connection,
    *,
    period: str = "weekly",
    as_of: str | None = None,
    schools: tuple[str, ...] = DEFAULT_SCHOOLS,
    limit: int = 25,
) -> dict:
    conn.row_factory = sqlite3.Row
    school_reports = []
    for school in schools:
        snapshot = build_snapshot(conn, period, as_of=as_of, school=school, limit=limit)
        window = snapshot["window"]
        school_reports.append(
            {
                "school": school,
                "snapshot": snapshot,
                "missing_notes_by_instructor": _missing_notes_by_instructor(
                    conn,
                    start_date=window["start"],
                    end_date=window["end"],
                    school=school,
                    limit=limit,
                ),
            }
        )

    snapshots = [item["snapshot"] for item in school_reports]
    exception_summary: Counter[str] = Counter()
    for snapshot in snapshots:
        exception_summary.update(snapshot.get("exception_queue", {}).get("summary", {}))

    window = snapshots[0]["window"] if snapshots else {"start": "", "end": ""}
    totals = {
        "hubspot_leads": _sum_snapshot_metric(snapshots, "funnel_counts", "hubspot_leads"),
        "contacted": _sum_snapshot_metric(snapshots, "funnel_counts", "contacted"),
        "pike13_first_visits": _sum_snapshot_metric(snapshots, "funnel_counts", "pike13_first_visits"),
        "attended": _sum_snapshot_metric(snapshots, "funnel_counts", "attended"),
        "converted": _sum_snapshot_metric(snapshots, "funnel_counts", "converted"),
        "reportable_lessons": _sum_snapshot_metric(snapshots, "notes_operations", "reportable_lessons"),
        "completed_notes": _sum_snapshot_metric(snapshots, "notes_operations", "completed_notes"),
        "missing_notes": _sum_snapshot_metric(snapshots, "notes_operations", "missing_notes"),
        "dialpad_calls": _sum_snapshot_metric(snapshots, "communications", "dialpad_calls"),
        "dialpad_sms": _sum_snapshot_metric(snapshots, "communications", "dialpad_sms"),
        "school_email": _sum_snapshot_metric(snapshots, "communications", "school_email"),
    }
    totals["note_completion_rate"] = (
        round(100.0 * totals["completed_notes"] / totals["reportable_lessons"], 1)
        if totals["reportable_lessons"]
        else 0.0
    )
    statuses = [item["snapshot"]["source_freshness"]["status"] for item in school_reports]
    overall_status = "ready" if statuses and all(status == "ready" for status in statuses) else "attention"
    if totals["missing_notes"] or exception_summary:
        overall_status = "attention"

    return {
        "dashboard_type": f"{period}_operations",
        "generated_at": utc_now_iso(),
        "period": period,
        "window": window,
        "overall_status": overall_status,
        "totals": totals,
        "school_reports": school_reports,
        "exception_summary": dict(sorted(exception_summary.items())),
        "source_counts": snapshots[0].get("source_freshness", {}).get("counts", {}) if snapshots else {},
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


def render_operations_dashboard_html(report: dict) -> str:
    totals = report["totals"]
    window = report["window"]
    period = report["period"].title()
    status = report["overall_status"]
    source_rows = [[key, value] for key, value in sorted(report.get("source_counts", {}).items())]
    exception_rows = [[key, value] for key, value in report.get("exception_summary", {}).items()]

    school_sections = []
    for item in report["school_reports"]:
        snapshot = item["snapshot"]
        notes = snapshot["notes_operations"]
        funnel = snapshot["funnel_counts"]
        comms = snapshot["communications"]
        missing_rows = [
            [
                row["instructor_name"],
                row["reportable_lessons"],
                row["completed_notes"],
                row["missing_notes"],
                f"{row['completion_rate'] or 0:.1f}%",
            ]
            for row in item["missing_notes_by_instructor"]
        ]
        school_sections.append(
            f"""
            <section class="school">
              <div class="section-heading">
                <h2>{_h(item["school"])}</h2>
                <span class="pill {_status_class(snapshot["source_freshness"]["status"])}">{_h(snapshot["source_freshness"]["status"])}</span>
              </div>
              <div class="mini-grid">
                {_metric_card("Leads", funnel["hubspot_leads"], "HubSpot rows in window")}
                {_metric_card("Trials", funnel["pike13_first_visits"], f"Attended {funnel['attended']}, no-show {funnel['no_show']}")}
                {_metric_card("Conversions", funnel["converted"], "Non-trial plan/pass signals")}
                {_metric_card("Notes", f"{notes['completion_rate']:.1f}%", f"{notes['missing_notes']} missing of {notes['reportable_lessons']}")}
                {_metric_card("Calls", comms["dialpad_calls"], "Dialpad call events")}
                {_metric_card("Texts", comms["dialpad_sms"], "Dialpad SMS events")}
              </div>
              <h3>Missing Notes By Instructor</h3>
              {_table(["Instructor", "Lessons", "Done", "Missing", "Rate"], missing_rows)}
            </section>
            """
        )

    html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_h(period)} Operations Dashboard</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f7f8fa;
      --panel: #ffffff;
      --text: #1d2430;
      --muted: #647084;
      --line: #d9dee7;
      --blue: #245bc4;
      --green: #147d4f;
      --amber: #9a5b00;
      --red: #b42318;
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
      background: #ffffff;
      border-bottom: 1px solid var(--line);
      padding: 20px 28px;
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 20px;
    }}
    h1, h2, h3, p {{ margin: 0; }}
    h1 {{ font-size: 24px; font-weight: 700; letter-spacing: 0; }}
    h2 {{ font-size: 18px; }}
    h3 {{ font-size: 14px; margin: 18px 0 8px; color: var(--muted); }}
    main {{
      max-width: 1420px;
      margin: 0 auto;
      padding: 24px 28px 40px;
    }}
    .subhead {{ color: var(--muted); margin-top: 4px; }}
    .pill {{
      display: inline-flex;
      align-items: center;
      min-height: 28px;
      padding: 4px 10px;
      border-radius: 4px;
      border: 1px solid var(--line);
      font-weight: 700;
      text-transform: uppercase;
      font-size: 12px;
    }}
    .pill.ready {{ color: var(--green); background: #ecf8f1; border-color: #b8dec7; }}
    .pill.attention {{ color: var(--amber); background: #fff7e5; border-color: #efd08f; }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(6, minmax(120px, 1fr));
      gap: 12px;
      margin-bottom: 24px;
    }}
    .metric, .school, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 6px;
    }}
    .metric {{ padding: 14px; min-height: 104px; }}
    .metric-label {{ color: var(--muted); font-size: 12px; font-weight: 700; text-transform: uppercase; }}
    .metric-value {{ font-size: 28px; font-weight: 750; margin-top: 6px; }}
    .metric-detail {{ color: var(--muted); margin-top: 4px; min-height: 20px; }}
    .school-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
    }}
    .school, .panel {{ padding: 18px; }}
    .section-heading {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }}
    .mini-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
      margin-top: 14px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      background: #fff;
    }}
    th, td {{
      text-align: left;
      padding: 8px 10px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
      overflow-wrap: anywhere;
    }}
    th {{ color: var(--muted); font-size: 12px; text-transform: uppercase; }}
    .supporting {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      margin-top: 16px;
    }}
    .empty {{ color: var(--muted); padding: 10px 0; }}
    footer {{ color: var(--muted); margin-top: 18px; font-size: 12px; }}
    @media (max-width: 1050px) {{
      .metrics {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
      .school-grid, .supporting {{ grid-template-columns: 1fr; }}
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
      <h1>{_h(period)} Operations Dashboard</h1>
      <p class="subhead">Window: {_h(window["start"])} to {_h(window["end"])}. Generated {_h(report["generated_at"])}.</p>
    </div>
    <span class="pill {_status_class(status)}">{_h(status)}</span>
  </header>
  <main>
    <section class="metrics">
      {_metric_card("Leads", totals["hubspot_leads"], f"{totals['contacted']} contacted")}
      {_metric_card("Trials", totals["pike13_first_visits"], f"{totals['attended']} attended")}
      {_metric_card("Conversions", totals["converted"], "Pike13 plan/pass signals")}
      {_metric_card("Notes Complete", f"{totals['note_completion_rate']:.1f}%", f"{totals['missing_notes']} missing")}
      {_metric_card("Calls", totals["dialpad_calls"], "Dialpad call events")}
      {_metric_card("Messages", totals["dialpad_sms"] + totals["school_email"], f"{totals['dialpad_sms']} SMS, {totals['school_email']} email")}
    </section>
    <div class="school-grid">
      {''.join(school_sections)}
    </div>
    <div class="supporting">
      <section class="panel">
        <h2>Follow-Up Exceptions</h2>
        {_table(["Reason", "Count"], exception_rows)}
      </section>
      <section class="panel">
        <h2>Source Counts</h2>
        {_table(["Source", "Rows"], source_rows)}
      </section>
    </div>
    <footer>
      This dashboard is sanitized and aggregate-only. It excludes customer names, emails, phone numbers, message bodies, transcripts, lesson-note text, source URLs, screenshots, and audio paths.
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
    prefix = f"{report['period']}_operations_dashboard"
    html_path = output_root / f"{prefix}.html"
    json_path = output_root / f"{prefix}.json"
    html_path.write_text(render_operations_dashboard_html(report), encoding="utf-8")
    json_path.write_text(dashboard_to_json(report) + "\n", encoding="utf-8")
    return html_path, json_path
