"""Notes pipeline health reporting.

This module is intentionally read-only. It summarizes whether the production
notes pipeline is current enough to trust before broader refactor work proceeds.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable


DEFAULT_SCHOOLS = ("westu-sor", "theheights-sor")
SCHOOL_LABELS = {
    "westu-sor": "West U",
    "theheights-sor": "The Heights",
}
SUMMARY_RE = re.compile(
    r"Lesson notes summary for (?P<school>West U|The Heights) "
    r"\((?P<start>\d{4}-\d{2}-\d{2}) to (?P<end>\d{4}-\d{2}-\d{2})\)"
)


def normalize_lesson_time(value: str | None) -> str:
    if not value:
        return ""
    cleaned = value.strip()
    if " on " in cleaned:
        cleaned = cleaned.split(" on ", 1)[0].strip()
    return cleaned


def normalize_students(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def is_reportable_lesson(lesson_type: str | None, students: str | None, instructor: str | None) -> bool:
    """Return True when a lesson belongs in notes-health reportable counts."""
    lesson_type_text = (lesson_type or "").lower()
    if "admin" in lesson_type_text or "meeting" in lesson_type_text:
        return False
    students_text = normalize_students(students)
    if "," in students_text:
        return False
    instructor_text = (instructor or "").strip().lower()
    if not re.search(r"[a-zA-Z]", instructor_text):
        return False
    if "admin" in instructor_text or "trial" in instructor_text or "rookies" in instructor_text:
        return False
    return True


def date_range(start: date, end: date) -> list[date]:
    days = (end - start).days
    if days < 0:
        return []
    return [start + timedelta(days=offset) for offset in range(days + 1)]


def _safe_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def scan_notes_send_logs(logs_dir: str | Path = "logs") -> dict[str, dict[str, dict[str, str]]]:
    """Scan local logs for successful notes-summary SMTP deliveries."""
    root = Path(logs_dir)
    results: dict[str, dict[str, dict[str, str]]] = {}
    if not root.exists():
        return results
    for path in sorted(root.glob("*.log")):
        text = path.read_text(errors="replace")
        lines = text.splitlines()
        for index, line in enumerate(lines):
            match = SUMMARY_RE.search(line)
            if not match:
                continue
            school_label = match.group("school")
            school_code = next(
                (code for code, label in SCHOOL_LABELS.items() if label == school_label),
                school_label,
            )
            start = match.group("start")
            delivered = any(
                "Email delivered to SMTP server" in later
                for later in lines[index + 1 : index + 6]
            )
            if delivered:
                results.setdefault(school_code, {})[start] = {
                    "status": "delivered",
                    "log_file": str(path),
                }
    return results


def build_notes_pipeline_health(
    conn: sqlite3.Connection,
    *,
    as_of: str | date | None = None,
    lookback_days: int = 7,
    expected_lag_days: int = 1,
    schools: Iterable[str] = DEFAULT_SCHOOLS,
    logs_dir: str | Path = "logs",
) -> dict:
    """Build a sanitized notes pipeline health snapshot."""
    if as_of is None:
        as_of_date = date.today()
    elif isinstance(as_of, date):
        as_of_date = as_of
    else:
        as_of_date = date.fromisoformat(as_of)

    end_date = as_of_date - timedelta(days=1)
    start_date = end_date - timedelta(days=max(0, lookback_days - 1))
    expected_dates = [item.isoformat() for item in date_range(start_date, end_date)]
    log_sends = scan_notes_send_logs(logs_dir)

    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT school, instructor_name, lesson_date, lesson_time, lesson_type, students,
               note_completed, last_checked
        FROM reminders
        WHERE lesson_date BETWEEN ? AND ?
          AND school IN ({})
        """.format(",".join("?" for _ in schools)),
        [start_date.isoformat(), end_date.isoformat(), *schools],
    ).fetchall()

    by_school_date: dict[tuple[str, str], dict] = {}
    seen = set()
    for row in rows:
        school = row["school"]
        lesson_date = row["lesson_date"]
        instructor = (row["instructor_name"] or "").strip()
        lesson_type = (row["lesson_type"] or "").strip()
        students = normalize_students(row["students"])
        lesson_time = normalize_lesson_time(row["lesson_time"] or "")
        dedup_key = (school, lesson_date, instructor, lesson_time, lesson_type, students)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        bucket = by_school_date.setdefault(
            (school, lesson_date),
            {
                "school": school,
                "date": lesson_date,
                "total_lessons": 0,
                "reportable_lessons": 0,
                "with_notes": 0,
                "missing_notes": 0,
                "last_checked": None,
                "email_status": "not_found",
                "email_log": "",
            },
        )
        bucket["total_lessons"] += 1
        checked = row["last_checked"]
        if checked and (bucket["last_checked"] is None or checked > bucket["last_checked"]):
            bucket["last_checked"] = checked
        if is_reportable_lesson(lesson_type, students, instructor):
            bucket["reportable_lessons"] += 1
            if row["note_completed"]:
                bucket["with_notes"] += 1
            else:
                bucket["missing_notes"] += 1

    school_summaries = []
    overall_status = "ready"
    for school in schools:
        day_rows = []
        latest_lesson_date = None
        latest_last_checked = None
        for item in expected_dates:
            data = by_school_date.get(
                (school, item),
                {
                    "school": school,
                    "date": item,
                    "total_lessons": 0,
                    "reportable_lessons": 0,
                    "with_notes": 0,
                    "missing_notes": 0,
                    "last_checked": None,
                    "email_status": "not_found",
                    "email_log": "",
                },
            ).copy()
            send = log_sends.get(school, {}).get(item)
            if send:
                data["email_status"] = send["status"]
                data["email_log"] = send["log_file"]
            if data["total_lessons"] > 0:
                latest_lesson_date = item
            if data["last_checked"] and (
                latest_last_checked is None or data["last_checked"] > latest_last_checked
            ):
                latest_last_checked = data["last_checked"]
            day_rows.append(data)

        latest_lesson = _safe_date(latest_lesson_date)
        max_allowed_lag_date = as_of_date - timedelta(days=expected_lag_days)
        blockers = []
        if latest_lesson is None:
            blockers.append("No lesson rows found in the health window.")
        elif latest_lesson < max_allowed_lag_date:
            blockers.append(
                f"Latest lesson date {latest_lesson.isoformat()} is older than expected lag date {max_allowed_lag_date.isoformat()}."
            )
        status = "ready" if not blockers else "warning"
        if status != "ready":
            overall_status = "warning"

        school_summaries.append(
            {
                "school": school,
                "school_label": SCHOOL_LABELS.get(school, school),
                "status": status,
                "blockers": blockers,
                "latest_lesson_date": latest_lesson_date,
                "latest_last_checked": latest_last_checked,
                "window_total_lessons": sum(row["total_lessons"] for row in day_rows),
                "window_reportable_lessons": sum(row["reportable_lessons"] for row in day_rows),
                "window_missing_notes": sum(row["missing_notes"] for row in day_rows),
                "days": day_rows,
            }
        )

    return {
        "overall_status": overall_status,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "window": {
            "as_of": as_of_date.isoformat(),
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "lookback_days": lookback_days,
            "expected_lag_days": expected_lag_days,
        },
        "schools": school_summaries,
    }


def render_markdown(report: dict) -> str:
    lines = [
        "# Notes Pipeline Health",
        "",
        f"- Overall status: `{report['overall_status']}`",
        f"- Window: `{report['window']['start']}` to `{report['window']['end']}`",
        f"- Generated at: `{report['generated_at']}`",
        "",
        "| School | Status | Latest Lesson | Last Checked | Reportable Lessons | Missing Notes |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for school in report["schools"]:
        lines.append(
            "| {school_label} | {status} | {latest_lesson_date} | {latest_last_checked} | {window_reportable_lessons} | {window_missing_notes} |".format(
                **{key: (value if value is not None else "") for key, value in school.items()}
            )
        )
    lines.extend(["", "## Recent Coverage", ""])
    for school in report["schools"]:
        lines.extend(
            [
                f"### {school['school_label']}",
                "",
                "| Date | Total Rows | Reportable | With Notes | Missing | Email Evidence |",
                "| --- | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for row in school["days"]:
            lines.append(
                f"| {row['date']} | {row['total_lessons']} | {row['reportable_lessons']} | {row['with_notes']} | {row['missing_notes']} | {row['email_status']} |"
            )
        if school["blockers"]:
            lines.extend(["", "Blockers:"])
            lines.extend(f"- {item}" for item in school["blockers"])
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_report(report: dict, output_dir: str | Path = "outputs/progress") -> tuple[Path, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "notes_pipeline_health.json"
    md_path = out_dir / "notes_pipeline_health.md"
    json_path.write_text(json.dumps(report, indent=2, default=str) + "\n")
    md_path.write_text(render_markdown(report))
    return json_path, md_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate notes pipeline health dashboard.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--logs-dir", default="logs")
    parser.add_argument("--output-dir", default="outputs/progress")
    parser.add_argument("--as-of", default="")
    parser.add_argument("--lookback-days", type=int, default=7)
    args = parser.parse_args(argv)

    conn = sqlite3.connect(args.db)
    try:
        report = build_notes_pipeline_health(
            conn,
            as_of=args.as_of or None,
            lookback_days=args.lookback_days,
            logs_dir=args.logs_dir,
        )
    finally:
        conn.close()
    json_path, md_path = write_report(report, args.output_dir)
    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    print(f"Overall status: {report['overall_status']}")
    return 0 if report["overall_status"] == "ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
