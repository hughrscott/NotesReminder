import json
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema
from lead_gap_analysis import build_gap_report
from source_completeness import refresh_identity_matches


ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "outputs" / "lead_intelligence" / "lead_intelligence_working.db"
DEFAULT_START_DATE = "2026-04-27"
DEFAULT_END_DATE = "2026-05-03"
DEFAULT_SCHOOL = "West University Place"
DEFAULT_PIKE13_SCHOOL = "West U"
DEFAULT_PIKE13_BASE_URL = "https://westu-sor.pike13.com"
DEFAULT_MD_OUTPUT = ROOT / "outputs" / "progress" / "date_window_load_report.md"
DEFAULT_JSON_OUTPUT = ROOT / "outputs" / "progress" / "date_window_load_report.json"


SOURCE_LABELS = {
    "hubspot": "HubSpot",
    "pike13": "Pike13",
    "dialpad": "Dialpad",
    "dialpad_daily_intake": "Dialpad daily intake",
    "dialpad_voice": "Dialpad voice",
    "dialpad_sms": "Dialpad SMS",
    "dialpad_call_reviews": "Dialpad call reviews",
    "notes": "Notes/reminders",
}


def parse_iso_date(value, label):
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be YYYY-MM-DD: {value}") from exc


def validate_window(start_date, end_date):
    start = parse_iso_date(start_date, "start date")
    end = parse_iso_date(end_date, "end date")
    if start > end:
        raise ValueError(f"start date {start_date} must be on or before end date {end_date}")
    return start, end


def validate_target_db(db_path, root=ROOT):
    resolved = Path(db_path).expanduser().resolve()
    production = (Path(root) / "reminders.db").resolve()
    if resolved == production:
        raise ValueError("Refusing to run a lead-intelligence window load against production reminders.db")
    expected = (Path(root) / "outputs" / "lead_intelligence" / "lead_intelligence_working.db").resolve()
    if resolved != expected:
        raise ValueError(f"Date-window loads are restricted to {expected}")
    return resolved


def backup_db(db_path):
    source = Path(db_path).resolve()
    if not source.exists():
        raise FileNotFoundError(f"Working DB does not exist: {source}")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = source.with_name(f"{source.stem}.date-window-backup-{stamp}{source.suffix}")
    shutil.copy2(source, backup)
    return backup


def _quote_command(command):
    return " ".join(str(part) for part in command)


def run_command_step(name, command, cwd=ROOT, timeout=None):
    started = datetime.now(timezone.utc).replace(microsecond=0)
    before = time.monotonic()
    try:
        completed = subprocess.run(
            [str(part) for part in command],
            cwd=str(cwd),
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        status = "success" if completed.returncode == 0 else "error"
        error = "" if completed.returncode == 0 else _last_lines(completed.stderr or completed.stdout)
        return {
            "name": name,
            "command": _quote_command(command),
            "status": status,
            "returncode": completed.returncode,
            "started_at": started.isoformat(),
            "duration_seconds": round(time.monotonic() - before, 2),
            "stdout_tail": _last_lines(completed.stdout),
            "stderr_tail": _last_lines(completed.stderr),
            "error": error,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "name": name,
            "command": _quote_command(command),
            "status": "timeout",
            "returncode": None,
            "started_at": started.isoformat(),
            "duration_seconds": round(time.monotonic() - before, 2),
            "stdout_tail": _last_lines(exc.stdout),
            "stderr_tail": _last_lines(exc.stderr),
            "error": f"Timed out after {timeout} seconds",
        }


def _last_lines(value, limit=8):
    if value is None:
        return ""
    text = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return "\n".join(lines[-limit:])


def rolling_window_days(start_date, today=None):
    start = parse_iso_date(start_date, "start date")
    today = today or date.today()
    return max(1, (today - start).days)


def table_exists(conn, table):
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (table,),
        ).fetchone()
        is not None
    )


def scalar(conn, sql, params=None):
    try:
        return conn.execute(sql, params or {}).fetchone()[0] or 0
    except sqlite3.OperationalError:
        return 0


def summarize_window_counts(conn, start_date, end_date, school):
    params = {"start": start_date, "end": end_date, "school": school}
    pike13_school = "West U"
    return {
        "hubspot": {
            "rows_in_window": scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM hubspot_deals
                WHERE (:school = '' OR COALESCE(school, '') = :school)
                  AND (
                    date(create_date) BETWEEN date(:start) AND date(:end)
                    OR date(last_activity_date) BETWEEN date(:start) AND date(:end)
                    OR date(last_contacted) BETWEEN date(:start) AND date(:end)
                    OR date(trial_date) BETWEEN date(:start) AND date(:end)
                    OR date(date_entered_scheduled_trial_stage) BETWEEN date(:start) AND date(:end)
                    OR date(updated_at) BETWEEN date(:start) AND date(:end)
                  )
                """,
                params,
            ),
            "coverage_basis": "deal create/activity/contact/trial/stage/update dates",
        },
        "pike13": {
            "rows_in_window": scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM pike13_visits
                WHERE COALESCE(first_visit_flag, 0) = 1
                  AND (:pike13_school = '' OR COALESCE(school, '') = :pike13_school)
                  AND date(starts_at) BETWEEN date(:start) AND date(:end)
                """,
                {"start": start_date, "end": end_date, "pike13_school": pike13_school},
            ),
            "coverage_basis": "First Visits report-backed starts_at",
        },
        "dialpad": {
            "rows_in_window": scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM vw_dialpad_communications
                WHERE date(event_at) BETWEEN date(:start) AND date(:end)
                  AND (
                    COALESCE(school, '') = :school
                    OR LOWER(COALESCE(school, '')) LIKE '%west%'
                    OR LOWER(COALESCE(department, '')) LIKE '%west%'
                    OR LOWER(COALESCE(department, '')) LIKE '%westu%'
                  )
                """,
                params,
            ),
            "coverage_basis": "communication event_at after source load",
        },
        "notes": {
            "rows_in_window": scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM reminders
                WHERE date(lesson_date) BETWEEN date(:start) AND date(:end)
                  AND (
                    LOWER(COALESCE(school, '')) LIKE '%west%'
                    OR LOWER(COALESCE(school, '')) LIKE '%westu%'
                    OR LOWER(COALESCE(location, '')) LIKE '%west%'
                  )
                """,
                params,
            ),
            "coverage_basis": "production reminder lesson_date rows copied into working DB",
        },
    }


def import_run_summary(conn, start_date, end_date):
    if not table_exists(conn, "source_import_runs"):
        return []
    rows = conn.execute(
        """
        SELECT source, extractor, status, window_start, window_end, rows_seen, rows_inserted, rows_updated, error, metadata_json
        FROM source_import_runs
        WHERE date(started_at) >= date(:start, '-1 day')
        ORDER BY id DESC
        LIMIT 30
        """,
        {"start": start_date, "end": end_date},
    ).fetchall()
    result = []
    for row in rows:
        data = dict(row)
        metadata = {}
        try:
            metadata = json.loads(data.get("metadata_json") or "{}")
        except json.JSONDecodeError:
            metadata = {}
        data["metadata"] = _summarize_metadata(metadata)
        data.pop("metadata_json", None)
        result.append(data)
    return result


def _summarize_metadata(metadata):
    if not isinstance(metadata, dict):
        return {}
    summary = {}
    for key in ("auth_status", "pike13_auth_status", "first_visits_status", "window_start", "window_end", "url", "views"):
        if key in metadata:
            summary[key] = metadata[key]
    return summary


def screenshot_checklist(gap_report):
    counts = gap_report["summary"].get("by_gap_category", {})
    requests = []
    if counts.get("hubspot_only_unworked", 0):
        requests.append(
            {
                "gap": "hubspot_only_unworked",
                "count": counts["hubspot_only_unworked"],
                "request": "HubSpot deal detail plus Dialpad conversation/call-history filtered to the contact phone for one representative unworked lead.",
            }
        )
    if counts.get("scheduled_trial_missing_pike13", 0):
        requests.append(
            {
                "gap": "scheduled_trial_missing_pike13",
                "count": counts["scheduled_trial_missing_pike13"],
                "request": "HubSpot scheduled trial detail plus Pike13 First Visits/client search evidence for one representative unresolved lead.",
            }
        )
    if counts.get("missing_first_visit", 0):
        requests.append(
            {
                "gap": "missing_first_visit",
                "count": counts["missing_first_visit"],
                "request": "Pike13 First Visits report row plus the linked service/event page for one representative unresolved lead.",
            }
        )
    if counts.get("missing_conversion_signal", 0):
        requests.append(
            {
                "gap": "missing_conversion_signal",
                "count": counts["missing_conversion_signal"],
                "request": "Pike13 client Plans & Passes page, plan/pass detail page, and Activity tab for one attended unresolved trial.",
            }
        )
    if counts.get("missing_dialpad_match", 0) or counts.get("targeted_dialpad_not_wired", 0):
        requests.append(
            {
                "gap": "communication_gap",
                "count": counts.get("missing_dialpad_match", 0) + counts.get("targeted_dialpad_not_wired", 0),
                "request": "Dialpad conversation or call-history page filtered to the contact phone for one representative unresolved lead.",
            }
        )
    return requests


def build_date_window_report(db_path, start_date, end_date, school, steps, backup_path=None, notes=None, gap_limit=500):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_lead_followup_schema(conn)
    try:
        refresh_identity_matches(conn)
        gap_report = build_gap_report(conn, school=school, limit=gap_limit, start_date=start_date, end_date=end_date)
        conn.commit()
        summary = {
            "db": str(Path(db_path).resolve()),
            "window_start": start_date,
            "window_end": end_date,
            "school": school,
            "backup_path": str(backup_path) if backup_path else "",
            "source_counts": summarize_window_counts(conn, start_date, end_date, school),
            "steps": steps,
            "import_runs": import_run_summary(conn, start_date, end_date),
            "gap_summary": gap_report["summary"],
            "screenshot_checklist": screenshot_checklist(gap_report),
            "notes": notes or [],
        }
    finally:
        conn.close()
    return summary


def render_date_window_markdown(report):
    lines = [
        "# Date-Window Lead Load Report",
        "",
        f"Window: {report['window_start']} to {report['window_end']}",
        f"School: {report['school']}",
        f"Working DB: `{report['db']}`",
        f"Backup: `{report.get('backup_path') or 'not created'}`",
        "",
        "## Source Coverage",
        "",
        "| Source | Rows in window | Coverage basis |",
        "| --- | ---: | --- |",
    ]
    for source, data in report["source_counts"].items():
        lines.append(
            f"| {SOURCE_LABELS.get(source, source)} | {int(data.get('rows_in_window', 0))} | {clean(data.get('coverage_basis'))} |"
        )
    lines.extend(["", "## Extractor Steps", "", "| Step | Status | Return | Duration | Notes |", "| --- | --- | ---: | ---: | --- |"])
    for step in report["steps"]:
        note = step.get("error") or step.get("stdout_tail") or ""
        lines.append(
            f"| {clean(step['name'])} | {clean(step['status'])} | {clean(step.get('returncode', ''))} | {step.get('duration_seconds', 0)}s | {clean(note)} |"
        )
    lines.extend(["", "## Gap Diagnostics", ""])
    gap = report["gap_summary"]
    lines.extend(
        [
            f"- Rows reviewed: {gap['rows_reviewed']}",
            f"- Ready for review: {gap['ready_for_review_rows']}",
            f"- HubSpot-only unworked: {gap.get('hubspot_only_unworked_rows', 0)}",
            f"- HubSpot-only with outreach: {gap.get('hubspot_only_with_outreach_rows', 0)}",
            f"- Scheduled trial missing Pike13: {gap.get('scheduled_trial_missing_pike13_rows', 0)}",
            f"- Missing Dialpad match: {gap['missing_dialpad_match_rows']}",
            f"- Targeted Dialpad not wired: {gap['targeted_dialpad_not_wired_rows']}",
            "",
            "### Diagnostic Areas",
            "",
        ]
    )
    for area, count in gap.get("by_diagnostic_area", {}).items():
        lines.append(f"- {area}: {count}")
    lines.extend(["", "## Screenshot Requests", ""])
    checklist = report.get("screenshot_checklist", [])
    if checklist:
        for item in checklist:
            lines.append(f"- {item['gap']} ({item['count']}): {item['request']}")
    else:
        lines.append("- None from current unresolved cases.")
    if report.get("notes"):
        lines.extend(["", "## Load Notes", ""])
        for note in report["notes"]:
            lines.append(f"- {clean(note)}")
    lines.extend(
        [
            "",
            "_This report is sanitized: it excludes customer names, emails, phones, message bodies, notes, transcripts, raw page text, screenshots, and source URLs._",
            "",
        ]
    )
    return "\n".join(lines)


def clean(value):
    if value is None:
        return ""
    return str(value).replace("|", "\\|").replace("\n", " ").strip()


def report_to_json(report):
    return json.dumps(report, indent=2, sort_keys=True)


def python_executable():
    return sys.executable or "python3"
