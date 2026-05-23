"""Read-only management scorecards and note-quality league tables."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta


PERIODS = ("mtd", "prior-week", "prior-month", "custom")


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (table,),
        ).fetchone()
    )


def school_aliases(school: str | None) -> set[str]:
    if not school:
        return set()
    value = school.strip().lower()
    aliases = {value}
    alias_map = {
        "west u": {"west u", "westu", "westu-sor", "west university place"},
        "westu": {"west u", "westu", "westu-sor", "west university place"},
        "westu-sor": {"west u", "westu", "westu-sor", "west university place"},
        "the heights": {"the heights", "heights", "theheights", "theheights-sor"},
        "heights": {"the heights", "heights", "theheights", "theheights-sor"},
        "theheights": {"the heights", "heights", "theheights", "theheights-sor"},
        "theheights-sor": {"the heights", "heights", "theheights", "theheights-sor"},
    }
    aliases.update(alias_map.get(value, set()))
    return aliases


def _parse_date(value: str | None, default: date | None = None) -> date:
    if not value:
        if default is None:
            raise ValueError("date value is required.")
        return default
    return datetime.strptime(value, "%Y-%m-%d").date()


def window_for_period(period: str, as_of: str | None = None) -> tuple[str, str]:
    today = _parse_date(as_of, date.today())
    if period == "mtd":
        start = today.replace(day=1)
        end = today
    elif period == "prior-week":
        current_monday = today - timedelta(days=today.weekday())
        start = current_monday - timedelta(days=7)
        end = current_monday - timedelta(days=1)
    elif period == "prior-month":
        first_this_month = today.replace(day=1)
        end = first_this_month - timedelta(days=1)
        start = end.replace(day=1)
    else:
        raise ValueError(f"Unsupported automatic period: {period}")
    return start.isoformat(), end.isoformat()


def _empty_summary(start_date: str, end_date: str, school: str | None = None) -> dict:
    return {
        "window": {"start_date": start_date, "end_date": end_date},
        "filters": {"school": school or ""},
        "school_league": [],
        "instructor_league": [],
        "status": "missing_reporting_tables",
    }


def _ranking_rows(rows: list[sqlite3.Row], label_keys: tuple[str, ...]) -> list[dict]:
    ranked = []
    previous_key = None
    current_rank = 0
    for index, row in enumerate(rows, start=1):
        row_dict = dict(row)
        ranking_key = (
            row_dict["league_score"],
            row_dict["completion_rate"],
            row_dict["reportable_lessons"],
        )
        if ranking_key != previous_key:
            current_rank = index
            previous_key = ranking_key
        result = {"rank": current_rank}
        for key in label_keys:
            value = row_dict.get(key)
            if key == "instructor_name" and (value is None or str(value).strip() == ""):
                value = "Unknown Instructor"
            result[key] = value
        result.update(
            {
                "reportable_lessons": row_dict["reportable_lessons"],
                "lessons_with_notes": row_dict["lessons_with_notes"],
                "missing_notes": row_dict["missing_notes"],
                "completion_rate": row_dict["completion_rate"],
                "scored_lessons": row_dict["scored_lessons"],
                "score_sum": row_dict["score_sum"],
                "league_score": row_dict["league_score"],
                "zero_inclusive_average_note_score": row_dict[
                    "zero_inclusive_average_note_score"
                ],
            }
        )
        ranked.append(result)
    return ranked


def _school_filter_sql(school: str | None) -> tuple[str, dict]:
    aliases = school_aliases(school)
    if not aliases:
        return "1=1", {}
    params = {f"school_{index}": value for index, value in enumerate(sorted(aliases))}
    placeholders = ", ".join(f":{key}" for key in params)
    return (
        f"(LOWER(COALESCE(s.school_code, '')) IN ({placeholders}) "
        f"OR LOWER(COALESCE(s.school_name, '')) IN ({placeholders}))",
        params,
    )


def _league_query(group_columns: tuple[str, ...], school_sql: str) -> str:
    labels = ", ".join(group_columns)
    return f"""
        SELECT
            {labels},
            COUNT(*) AS reportable_lessons,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END) AS lessons_with_notes,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 0 THEN 1 ELSE 0 END) AS missing_notes,
            ROUND(
                100.0 * SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
                1
            ) AS completion_rate,
            SUM(CASE WHEN n.note_score IS NOT NULL THEN 1 ELSE 0 END) AS scored_lessons,
            ROUND(
                SUM(CASE WHEN n.note_score IS NOT NULL THEN n.note_score / 10.0 ELSE 0 END),
                2
            ) AS score_sum,
            ROUND(
                100.0 * SUM(CASE WHEN n.note_score IS NOT NULL THEN n.note_score / 10.0 ELSE 0 END) / NULLIF(COUNT(*), 0),
                1
            ) AS league_score,
            ROUND(
                10.0 * SUM(CASE WHEN n.note_score IS NOT NULL THEN n.note_score / 10.0 ELSE 0 END) / NULLIF(COUNT(*), 0),
                2
            ) AS zero_inclusive_average_note_score
        FROM lessons l
        JOIN schools s ON s.school_id = l.school_id
        LEFT JOIN instructors i ON i.instructor_id = l.instructor_id
        LEFT JOIN lesson_notes n ON n.lesson_id = l.lesson_id
        WHERE date(l.lesson_date) BETWEEN date(:start_date) AND date(:end_date)
          AND COALESCE(l.lesson_is_reportable, 0) = 1
          AND {school_sql}
        GROUP BY {labels}
        ORDER BY league_score DESC, completion_rate DESC, reportable_lessons DESC, {labels}
    """


def build_note_quality_scorecard(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
    school: str | None = None,
) -> dict:
    """Build school and instructor note-quality league tables from normalized tables."""
    if not table_exists(conn, "lessons") or not table_exists(conn, "lesson_notes"):
        return _empty_summary(start_date, end_date, school)

    school_sql, school_params = _school_filter_sql(school)
    params = {"start_date": start_date, "end_date": end_date, **school_params}
    school_rows = conn.execute(
        _league_query(("s.school_code", "s.school_name"), school_sql),
        params,
    ).fetchall()
    instructor_rows = conn.execute(
        _league_query(("s.school_code", "s.school_name", "i.instructor_name"), school_sql),
        params,
    ).fetchall()
    return {
        "window": {"start_date": start_date, "end_date": end_date},
        "filters": {"school": school or ""},
        "status": "ready",
        "school_league": _ranking_rows(school_rows, ("school_code", "school_name")),
        "instructor_league": _ranking_rows(
            instructor_rows, ("school_code", "school_name", "instructor_name")
        ),
        "formula": {
            "missing_note": 0,
            "scored_note_component": "note_score / 10",
            "league_score": "SUM(score_component) / reportable_lessons * 100",
            "reportable_filter": "normalized lessons.lesson_is_reportable = 1; group and multi-student lessons excluded",
        },
        "sensitive_content_included": False,
    }


def build_note_quality_scorecard_for_period(
    conn: sqlite3.Connection,
    period: str = "mtd",
    as_of: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    school: str | None = None,
) -> dict:
    if period not in PERIODS:
        raise ValueError(f"period must be one of: {', '.join(PERIODS)}")
    if period == "custom":
        if not start_date or not end_date:
            raise ValueError("custom period requires start_date and end_date.")
        resolved_start, resolved_end = start_date, end_date
    else:
        if start_date or end_date:
            raise ValueError("start_date/end_date can only be used with custom period.")
        resolved_start, resolved_end = window_for_period(period, as_of)
    scorecard = build_note_quality_scorecard(conn, resolved_start, resolved_end, school)
    scorecard["period"] = period
    scorecard["as_of"] = as_of or ""
    return scorecard


def _markdown_table(headers: list[str], rows: list[list[object]]) -> list[str]:
    if not rows:
        return ["- None."]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join("" if value is None else str(value) for value in row) + " |")
    return lines


def render_scorecard_markdown(scorecard: dict) -> str:
    window = scorecard["window"]
    school_filter = scorecard.get("filters", {}).get("school") or "All schools"
    lines = [
        "# Note Quality Scorecard",
        "",
        f"Window: {window['start_date']} to {window['end_date']}",
        f"School filter: {school_filter}",
        f"Status: {scorecard.get('status', 'unknown')}",
        "",
        "## School League",
        "",
        *_markdown_table(
            [
                "Rank",
                "School",
                "Lessons",
                "With Notes",
                "Missing",
                "Completion %",
                "Score Sum",
                "League Score",
                "Avg Note Score",
            ],
            [
                [
                    row["rank"],
                    row["school_name"] or row["school_code"],
                    row["reportable_lessons"],
                    row["lessons_with_notes"],
                    row["missing_notes"],
                    row["completion_rate"],
                    row["score_sum"],
                    row["league_score"],
                    row["zero_inclusive_average_note_score"],
                ]
                for row in scorecard.get("school_league", [])
            ],
        ),
        "",
        "## Instructor League",
        "",
        *_markdown_table(
            [
                "Rank",
                "School",
                "Instructor",
                "Lessons",
                "With Notes",
                "Missing",
                "Completion %",
                "Score Sum",
                "League Score",
                "Avg Note Score",
            ],
            [
                [
                    row["rank"],
                    row["school_name"] or row["school_code"],
                    row["instructor_name"],
                    row["reportable_lessons"],
                    row["lessons_with_notes"],
                    row["missing_notes"],
                    row["completion_rate"],
                    row["score_sum"],
                    row["league_score"],
                    row["zero_inclusive_average_note_score"],
                ]
                for row in scorecard.get("instructor_league", [])
            ],
        ),
        "",
        "_This scorecard is sanitized and excludes customer names, lesson notes, transcripts, message bodies, source URLs, and audio paths._",
    ]
    return "\n".join(lines) + "\n"


def scorecard_to_json(scorecard: dict) -> str:
    return json.dumps(scorecard, indent=2, default=str)
