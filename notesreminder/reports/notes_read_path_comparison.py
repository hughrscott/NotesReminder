"""Compare legacy reminders reads with normalized notes tables."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path

from build_reporting_schema import backfill_reporting, is_reportable_lesson
from notesreminder.reports.notes_pipeline_health import normalize_lesson_time, normalize_students


DEFAULT_SCHOOLS = ("westu-sor", "theheights-sor")


def _date_range(start: str, end: str) -> list[str]:
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    if end_date < start_date:
        return []
    return [(start_date + timedelta(days=offset)).isoformat() for offset in range((end_date - start_date).days + 1)]


def legacy_day_counts(conn, start_date, end_date, schools=DEFAULT_SCHOOLS):
    rows = conn.execute(
        """
        SELECT lesson_id, school, instructor_name, lesson_date, lesson_time,
               lesson_type, students, note_completed
        FROM reminders
        WHERE lesson_date BETWEEN ? AND ?
          AND school IN ({})
        """.format(",".join("?" for _ in schools)),
        [start_date, end_date, *schools],
    ).fetchall()
    buckets = {}
    seen = set()
    for row in rows:
        school = row["school"]
        lesson_date = row["lesson_date"]
        instructor = (row["instructor_name"] or "").strip()
        lesson_type = (row["lesson_type"] or "").strip()
        students = normalize_students(row["students"])
        lesson_time = normalize_lesson_time(row["lesson_time"] or "")
        key = (school, lesson_date, instructor, lesson_time, lesson_type, students)
        if key in seen:
            continue
        seen.add(key)
        bucket = buckets.setdefault(
            (school, lesson_date),
            {"school": school, "lesson_date": lesson_date, "total_lessons": 0, "reportable_lessons": 0, "completed_notes": 0, "missing_notes": 0},
        )
        bucket["total_lessons"] += 1
        if is_reportable_lesson(lesson_type, students, instructor):
            bucket["reportable_lessons"] += 1
            if row["note_completed"]:
                bucket["completed_notes"] += 1
            else:
                bucket["missing_notes"] += 1
    for school in schools:
        for item in _date_range(start_date, end_date):
            buckets.setdefault(
                (school, item),
                {"school": school, "lesson_date": item, "total_lessons": 0, "reportable_lessons": 0, "completed_notes": 0, "missing_notes": 0},
            )
    return buckets


def normalized_day_counts(conn, start_date, end_date, schools=DEFAULT_SCHOOLS):
    rows = conn.execute(
        """
        SELECT
            s.school_code AS school,
            l.lesson_date,
            COUNT(*) AS total_lessons,
            SUM(CASE WHEN COALESCE(l.lesson_is_reportable, 0) = 1 THEN 1 ELSE 0 END) AS reportable_lessons,
            SUM(CASE WHEN COALESCE(l.lesson_is_reportable, 0) = 1 AND COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END) AS completed_notes,
            SUM(CASE WHEN COALESCE(l.lesson_is_reportable, 0) = 1 AND COALESCE(n.note_completed, 0) = 0 THEN 1 ELSE 0 END) AS missing_notes
        FROM lessons l
        JOIN schools s ON s.school_id = l.school_id
        LEFT JOIN lesson_notes n ON n.lesson_id = l.lesson_id
        WHERE l.lesson_date BETWEEN ? AND ?
          AND s.school_code IN ({})
        GROUP BY s.school_code, l.lesson_date
        """.format(",".join("?" for _ in schools)),
        [start_date, end_date, *schools],
    ).fetchall()
    buckets = {
        (row["school"], row["lesson_date"]): {
            "school": row["school"],
            "lesson_date": row["lesson_date"],
            "total_lessons": row["total_lessons"] or 0,
            "reportable_lessons": row["reportable_lessons"] or 0,
            "completed_notes": row["completed_notes"] or 0,
            "missing_notes": row["missing_notes"] or 0,
        }
        for row in rows
    }
    for school in schools:
        for item in _date_range(start_date, end_date):
            buckets.setdefault(
                (school, item),
                {"school": school, "lesson_date": item, "total_lessons": 0, "reportable_lessons": 0, "completed_notes": 0, "missing_notes": 0},
            )
    return buckets


def compare_dicts(legacy, normalized, fields):
    mismatches = []
    for key in sorted(set(legacy) | set(normalized)):
        legacy_row = legacy.get(key, {})
        normalized_row = normalized.get(key, {})
        deltas = {
            field: (legacy_row.get(field, 0), normalized_row.get(field, 0))
            for field in fields
            if legacy_row.get(field, 0) != normalized_row.get(field, 0)
        }
        if deltas:
            mismatches.append({"key": key, "legacy": legacy_row, "normalized": normalized_row, "deltas": deltas})
    return mismatches


def rows_by_key(conn, sql, params, key_fields):
    return {
        tuple(row[field] for field in key_fields): dict(row)
        for row in conn.execute(sql, params).fetchall()
    }


def legacy_instructor_counts(conn, start_date, end_date, schools=DEFAULT_SCHOOLS):
    rows = conn.execute(
        """
        SELECT school, instructor_name, lesson_type, students, note_completed
        FROM reminders
        WHERE lesson_date BETWEEN ? AND ?
          AND school IN ({})
        """.format(",".join("?" for _ in schools)),
        [start_date, end_date, *schools],
    ).fetchall()
    buckets = {}
    for row in rows:
        instructor = (row["instructor_name"] or "").strip()
        students = normalize_students(row["students"])
        lesson_type = (row["lesson_type"] or "").strip()
        if not is_reportable_lesson(lesson_type, students, instructor):
            continue
        key = (row["school"], instructor)
        bucket = buckets.setdefault(
            key,
            {"school": row["school"], "instructor_name": instructor, "total_reportable_lessons": 0, "completed_notes": 0, "missing_notes": 0},
        )
        bucket["total_reportable_lessons"] += 1
        if row["note_completed"]:
            bucket["completed_notes"] += 1
        else:
            bucket["missing_notes"] += 1
    return buckets


def normalized_instructor_counts(conn, start_date, end_date, schools=DEFAULT_SCHOOLS):
    return rows_by_key(
        conn,
        """
        SELECT
            s.school_code AS school,
            COALESCE(i.instructor_name, '') AS instructor_name,
            COUNT(*) AS total_reportable_lessons,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END) AS completed_notes,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 0 THEN 1 ELSE 0 END) AS missing_notes
        FROM lessons l
        JOIN schools s ON s.school_id = l.school_id
        LEFT JOIN instructors i ON i.instructor_id = l.instructor_id
        LEFT JOIN lesson_notes n ON n.lesson_id = l.lesson_id
        WHERE l.lesson_date BETWEEN ? AND ?
          AND s.school_code IN ({})
          AND COALESCE(l.lesson_is_reportable, 0) = 1
        GROUP BY s.school_code, COALESCE(i.instructor_name, '')
        """.format(",".join("?" for _ in schools)),
        [start_date, end_date, *schools],
        ("school", "instructor_name"),
    )


def legacy_note_quality(conn, start_date, end_date, schools=DEFAULT_SCHOOLS):
    rows = conn.execute(
        """
        SELECT school, instructor_name, lesson_date, lesson_type, students, note_completed, note_score
        FROM reminders
        WHERE lesson_date BETWEEN ? AND ?
          AND school IN ({})
        """.format(",".join("?" for _ in schools)),
        [start_date, end_date, *schools],
    ).fetchall()
    buckets = {}
    for row in rows:
        instructor = (row["instructor_name"] or "").strip()
        students = normalize_students(row["students"])
        lesson_type = (row["lesson_type"] or "").strip()
        if not is_reportable_lesson(lesson_type, students, instructor):
            continue
        month = (row["lesson_date"] or "")[:7]
        key = (row["school"], instructor, month)
        bucket = buckets.setdefault(
            key,
            {
                "school": row["school"],
                "instructor_name": instructor,
                "score_month": month,
                "total_reportable_lessons": 0,
                "lessons_with_notes": 0,
                "scored_lessons": 0,
                "missing_notes": 0,
                "league_score": 0.0,
                "_score_sum": 0.0,
            },
        )
        bucket["total_reportable_lessons"] += 1
        if row["note_completed"]:
            bucket["lessons_with_notes"] += 1
        else:
            bucket["missing_notes"] += 1
        if row["note_score"] is not None:
            bucket["scored_lessons"] += 1
            bucket["_score_sum"] += float(row["note_score"]) / 10.0
    for bucket in buckets.values():
        total = bucket["total_reportable_lessons"]
        bucket["league_score"] = round(100.0 * bucket.pop("_score_sum") / total, 1) if total else 0.0
    return buckets


def normalized_note_quality(conn, start_date, end_date, schools=DEFAULT_SCHOOLS):
    rows = conn.execute(
        """
        SELECT
            s.school_code AS school,
            COALESCE(i.instructor_name, '') AS instructor_name,
            substr(l.lesson_date, 1, 7) AS score_month,
            COUNT(*) AS total_reportable_lessons,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END) AS lessons_with_notes,
            SUM(CASE WHEN n.note_score IS NOT NULL THEN 1 ELSE 0 END) AS scored_lessons,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 0 THEN 1 ELSE 0 END) AS missing_notes,
            ROUND(
                100.0 * SUM(CASE WHEN n.note_score IS NOT NULL THEN n.note_score / 10.0 ELSE 0 END) / NULLIF(COUNT(*), 0),
                1
            ) AS league_score
        FROM lessons l
        JOIN schools s ON s.school_id = l.school_id
        LEFT JOIN instructors i ON i.instructor_id = l.instructor_id
        LEFT JOIN lesson_notes n ON n.lesson_id = l.lesson_id
        WHERE l.lesson_date BETWEEN ? AND ?
          AND s.school_code IN ({})
          AND COALESCE(l.lesson_is_reportable, 0) = 1
        GROUP BY s.school_code, COALESCE(i.instructor_name, ''), substr(l.lesson_date, 1, 7)
        """.format(",".join("?" for _ in schools)),
        [start_date, end_date, *schools],
    ).fetchall()
    return {
        (row["school"], row["instructor_name"], row["score_month"]): {
            "school": row["school"],
            "instructor_name": row["instructor_name"],
            "score_month": row["score_month"],
            "total_reportable_lessons": row["total_reportable_lessons"] or 0,
            "lessons_with_notes": row["lessons_with_notes"] or 0,
            "scored_lessons": row["scored_lessons"] or 0,
            "missing_notes": row["missing_notes"] or 0,
            "league_score": row["league_score"] or 0.0,
        }
        for row in rows
    }


def build_notes_read_path_comparison(conn, start_date, end_date, schools=DEFAULT_SCHOOLS, rebuild=True):
    conn.row_factory = sqlite3.Row
    if rebuild:
        backfill_reporting(conn)
    base_counts = {
        "reminders": conn.execute("SELECT COUNT(*) FROM reminders").fetchone()[0],
        "lessons": conn.execute("SELECT COUNT(*) FROM lessons").fetchone()[0],
        "lesson_notes": conn.execute("SELECT COUNT(*) FROM lesson_notes").fetchone()[0],
        "lesson_attendance": conn.execute("SELECT COUNT(*) FROM lesson_attendance").fetchone()[0],
        "reminders_missing_lessons": conn.execute(
            """
            SELECT COUNT(*)
            FROM reminders r
            LEFT JOIN lessons l ON l.lesson_id = r.lesson_id
            WHERE r.lesson_id IS NOT NULL
              AND l.lesson_id IS NULL
            """
        ).fetchone()[0],
        "reminders_missing_lesson_notes": conn.execute(
            """
            SELECT COUNT(*)
            FROM reminders r
            LEFT JOIN lesson_notes n ON n.lesson_id = r.lesson_id
            WHERE r.lesson_id IS NOT NULL
              AND n.lesson_id IS NULL
            """
        ).fetchone()[0],
        "reminders_missing_lesson_attendance": conn.execute(
            """
            SELECT COUNT(*)
            FROM reminders r
            LEFT JOIN lesson_attendance a ON a.lesson_id = r.lesson_id
            WHERE r.lesson_id IS NOT NULL
              AND a.lesson_id IS NULL
            """
        ).fetchone()[0],
    }
    base_mismatches = []
    for table in ("lessons", "lesson_notes", "lesson_attendance"):
        if base_counts[table] != base_counts["reminders"]:
            base_mismatches.append({"metric": table, "legacy": base_counts["reminders"], "normalized": base_counts[table]})
    for metric in ("reminders_missing_lessons", "reminders_missing_lesson_notes", "reminders_missing_lesson_attendance"):
        if base_counts[metric]:
            base_mismatches.append({"metric": metric, "legacy": 0, "normalized": base_counts[metric]})

    day_mismatches = compare_dicts(
        legacy_day_counts(conn, start_date, end_date, schools),
        normalized_day_counts(conn, start_date, end_date, schools),
        ("total_lessons", "reportable_lessons", "completed_notes", "missing_notes"),
    )
    instructor_mismatches = compare_dicts(
        legacy_instructor_counts(conn, start_date, end_date, schools),
        normalized_instructor_counts(conn, start_date, end_date, schools),
        ("total_reportable_lessons", "completed_notes", "missing_notes"),
    )
    note_quality_mismatches = compare_dicts(
        legacy_note_quality(conn, start_date, end_date, schools),
        normalized_note_quality(conn, start_date, end_date, schools),
        ("total_reportable_lessons", "lessons_with_notes", "scored_lessons", "missing_notes", "league_score"),
    )
    mismatch_count = (
        len(base_mismatches)
        + len(day_mismatches)
        + len(instructor_mismatches)
        + len(note_quality_mismatches)
    )
    return {
        "status": "ready" if mismatch_count == 0 else "mismatch",
        "window": {"start": start_date, "end": end_date, "schools": list(schools)},
        "base_counts": base_counts,
        "mismatch_count": mismatch_count,
        "mismatches": {
            "base": base_mismatches,
            "school_day": day_mismatches,
            "instructor": instructor_mismatches,
            "note_quality": note_quality_mismatches,
        },
    }


def render_markdown(report):
    lines = [
        "# Notes Read-Path Comparison",
        "",
        f"- Status: `{report['status']}`",
        f"- Window: `{report['window']['start']}` to `{report['window']['end']}`",
        f"- Schools: `{', '.join(report['window']['schools'])}`",
        f"- Mismatches: `{report['mismatch_count']}`",
        "",
        "## Base Counts",
        "",
        "| Metric | Rows |",
        "| --- | ---: |",
    ]
    for key, value in report["base_counts"].items():
        lines.append(f"| {key} | {value} |")
    lines.extend(["", "## Mismatch Summary", ""])
    for section, rows in report["mismatches"].items():
        lines.append(f"- {section}: {len(rows)}")
    if report["mismatch_count"]:
        lines.extend(["", "## First Mismatches", ""])
        for section, rows in report["mismatches"].items():
            for row in rows[:5]:
                lines.append(f"- {section}: `{row}`")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Compare reminders reads with normalized notes-table reads.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--school", action="append", dest="schools")
    parser.add_argument("--output", default="outputs/progress/notes_read_path_comparison.md")
    parser.add_argument("--json-output", default="outputs/progress/notes_read_path_comparison.json")
    parser.add_argument("--no-rebuild", action="store_true")
    args = parser.parse_args()
    schools = tuple(args.schools or DEFAULT_SCHOOLS)
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        report = build_notes_read_path_comparison(
            conn,
            args.start_date,
            args.end_date,
            schools=schools,
            rebuild=not args.no_rebuild,
        )
        conn.commit()
    finally:
        conn.close()
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(render_markdown(report))
    Path(args.json_output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.json_output).write_text(json.dumps(report, indent=2, sort_keys=True))
    print(f"Wrote {args.output}")
    print(f"Wrote {args.json_output}")
    if report["status"] != "ready":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
