#!/usr/bin/env python3
import argparse
import csv
import json
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


SCORE_COLUMN_HINTS = [
    "score",
    "rating",
    "grade",
    "closing_score",
]
TEXT_HINTS = [
    "justification",
    "reason",
    "strength",
    "weakness",
    "improvement",
    "feedback",
]
KEY_HINTS = [
    "lesson_id",
    "pike13_lesson_id",
    "school",
    "lesson_date",
    "lesson_time",
    "instructor_name",
    "students",
    "scored_at",
]


@dataclass
class SourceConfig:
    table: str
    score_col: str
    lesson_id_col: Optional[str]
    pike13_col: Optional[str]
    school_col: Optional[str]
    date_col: Optional[str]
    time_col: Optional[str]
    instructor_col: Optional[str]
    students_col: Optional[str]
    justification_col: Optional[str]
    strength_col: Optional[str]
    improvement_col: Optional[str]
    scored_at_col: Optional[str]


def normalize_time(value: str) -> str:
    if not value:
        return ""
    cleaned = str(value).strip()
    if " on " in cleaned:
        cleaned = cleaned.split(" on ", 1)[0].strip()
    return cleaned


def normalize_students(value: str) -> str:
    if not value:
        return ""
    return " ".join(str(value).split())


def normalize_text(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip()


def connect_ro(path: Path) -> sqlite3.Connection:
    uri = f"file:{path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [r["name"] for r in rows]


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r["name"] for r in rows]


def is_score_like(col: str) -> bool:
    c = col.lower()
    return any(h in c for h in SCORE_COLUMN_HINTS)


def has_data(conn: sqlite3.Connection, table: str, col: str) -> bool:
    row = conn.execute(
        f"SELECT COUNT(*) AS c FROM {table} WHERE {col} IS NOT NULL AND TRIM(CAST({col} AS TEXT)) != ''"
    ).fetchone()
    return bool(row and row["c"] > 0)


def detect_source_config(
    conn: sqlite3.Connection,
    source_table: Optional[str] = None,
    score_column: Optional[str] = None,
) -> Optional[SourceConfig]:
    tables = [source_table] if source_table else list_tables(conn)
    candidates: List[Tuple[int, SourceConfig]] = []
    for table in tables:
        cols = table_columns(conn, table)
        lower = {c.lower(): c for c in cols}

        if score_column:
            if score_column not in cols:
                continue
            score_col = score_column
        else:
            score_cols = [c for c in cols if is_score_like(c)]
            score_cols = [c for c in score_cols if has_data(conn, table, c)]
            if not score_cols:
                continue
            score_col = score_cols[0]

        def pick(*names: str) -> Optional[str]:
            for n in names:
                if n in lower:
                    return lower[n]
            return None

        cfg = SourceConfig(
            table=table,
            score_col=score_col,
            lesson_id_col=pick("lesson_id"),
            pike13_col=pick("pike13_lesson_id"),
            school_col=pick("school"),
            date_col=pick("lesson_date", "date"),
            time_col=pick("lesson_time", "time"),
            instructor_col=pick("instructor_name", "instructor"),
            students_col=pick("students", "student_name"),
            justification_col=pick("justification", "closing_justification", "reason"),
            strength_col=pick("strength_or_weakness", "strength_weakness"),
            improvement_col=pick("improvement", "closing_improvement"),
            scored_at_col=pick("scored_at", "updated_at", "created_at", "note_timestamp"),
        )

        quality = 0
        for c in cols:
            if c.lower() in KEY_HINTS:
                quality += 1
            if any(h in c.lower() for h in TEXT_HINTS):
                quality += 1
        candidates.append((quality, cfg))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def discover_mode(paths: Iterable[Path], sample: int) -> int:
    found_any = 0
    for path in paths:
        print(f"\n=== {path} ===")
        if not path.exists():
            print("missing")
            continue
        try:
            conn = connect_ro(path)
        except sqlite3.Error as exc:
            print(f"not sqlite or unreadable: {exc}")
            continue
        try:
            tables = list_tables(conn)
            hit_count = 0
            for table in tables:
                cols = table_columns(conn, table)
                score_cols = [c for c in cols if is_score_like(c)]
                if not score_cols:
                    continue
                hit_count += 1
                found_any += 1
                print(f"table={table}")
                print(f"score_like_columns={score_cols}")
                select_cols = score_cols[:1] + [c for c in cols if c.lower() in KEY_HINTS][:7]
                rows = conn.execute(
                    f"SELECT {', '.join(select_cols)} FROM {table} "
                    f"WHERE {score_cols[0]} IS NOT NULL LIMIT ?",
                    (sample,),
                ).fetchall()
                print(f"sample_rows={len(rows)}")
                for r in rows:
                    print(json.dumps({k: r[k] for k in r.keys()}, default=str))
            if hit_count == 0:
                print("no score-like columns found")
        finally:
            conn.close()
    return found_any


def load_current_indices(conn: sqlite3.Connection):
    rows = conn.execute(
        """
        SELECT lesson_id, pike13_lesson_id, school, lesson_date, lesson_time,
               instructor_name, students
        FROM reminders
        """
    ).fetchall()

    by_pike13: Dict[str, sqlite3.Row] = {}
    by_lesson_id: Dict[str, sqlite3.Row] = {}
    by_tuple: Dict[Tuple[str, str, str, str, str], List[sqlite3.Row]] = defaultdict(list)

    for r in rows:
        lesson_id = normalize_text(r["lesson_id"])
        pike = normalize_text(r["pike13_lesson_id"])
        school = normalize_text(r["school"]).lower()
        date = normalize_text(r["lesson_date"])
        time = normalize_time(r["lesson_time"])
        instructor = normalize_text(r["instructor_name"]).lower()
        students = normalize_students(r["students"]).lower()
        key = (school, date, time, instructor, students)
        by_tuple[key].append(r)
        if lesson_id:
            by_lesson_id[lesson_id] = r
        if pike:
            by_pike13[pike] = r
    return by_pike13, by_lesson_id, by_tuple


def build_source_rows(conn: sqlite3.Connection, cfg: SourceConfig) -> List[sqlite3.Row]:
    cols = [cfg.score_col]
    for c in [
        cfg.lesson_id_col,
        cfg.pike13_col,
        cfg.school_col,
        cfg.date_col,
        cfg.time_col,
        cfg.instructor_col,
        cfg.students_col,
        cfg.justification_col,
        cfg.strength_col,
        cfg.improvement_col,
        cfg.scored_at_col,
    ]:
        if c and c not in cols:
            cols.append(c)
    sql = f"SELECT {', '.join(cols)} FROM {cfg.table} WHERE {cfg.score_col} IS NOT NULL"
    return conn.execute(sql).fetchall()


def row_value(row: sqlite3.Row, col: Optional[str]) -> str:
    if not col:
        return ""
    return normalize_text(row[col])


def compare_rows(
    source_rows: List[sqlite3.Row],
    cfg: SourceConfig,
    by_pike13,
    by_lesson_id,
    by_tuple,
):
    matched = []
    unmatched = []
    for row in source_rows:
        src_lesson_id = row_value(row, cfg.lesson_id_col)
        src_pike13 = row_value(row, cfg.pike13_col)
        src_school = row_value(row, cfg.school_col).lower()
        src_date = row_value(row, cfg.date_col)
        src_time = normalize_time(row_value(row, cfg.time_col))
        src_instructor = row_value(row, cfg.instructor_col).lower()
        src_students = normalize_students(row_value(row, cfg.students_col)).lower()
        tuple_key = (src_school, src_date, src_time, src_instructor, src_students)

        target = None
        match_method = ""
        if src_pike13 and src_pike13 in by_pike13:
            target = by_pike13[src_pike13]
            match_method = "pike13_lesson_id"
        elif src_lesson_id and src_lesson_id in by_lesson_id:
            target = by_lesson_id[src_lesson_id]
            match_method = "lesson_id"
        else:
            matches = by_tuple.get(tuple_key, [])
            if len(matches) == 1:
                target = matches[0]
                match_method = "normalized_tuple"

        payload = {
            "source_table": cfg.table,
            "source_score": row[cfg.score_col],
            "source_lesson_id": src_lesson_id,
            "source_pike13_lesson_id": src_pike13,
            "source_school": src_school,
            "source_lesson_date": src_date,
            "source_lesson_time": src_time,
            "source_instructor_name": src_instructor,
            "source_students": src_students,
            "source_justification": row_value(row, cfg.justification_col),
            "source_strength_or_weakness": row_value(row, cfg.strength_col),
            "source_improvement": row_value(row, cfg.improvement_col),
            "source_scored_at": row_value(row, cfg.scored_at_col),
        }
        if target is None:
            unmatched.append(payload)
            continue

        payload.update(
            {
                "match_method": match_method,
                "matched_lesson_id": normalize_text(target["lesson_id"]),
                "matched_pike13_lesson_id": normalize_text(target["pike13_lesson_id"]),
                "matched_school": normalize_text(target["school"]),
                "matched_lesson_date": normalize_text(target["lesson_date"]),
                "matched_lesson_time": normalize_time(target["lesson_time"]),
                "matched_instructor_name": normalize_text(target["instructor_name"]),
                "matched_students": normalize_students(target["students"]),
            }
        )
        matched.append(payload)
    return matched, unmatched


def write_csv(path: Path, rows: List[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["empty"])
        return
    fields = sorted(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def parse_args():
    parser = argparse.ArgumentParser(description="Recover and compare legacy lesson scores.")
    sub = parser.add_subparsers(dest="mode", required=True)

    d = sub.add_parser("discover", help="Inspect DB files for score-like columns.")
    d.add_argument(
        "--paths",
        nargs="+",
        default=["reminders.db", "reminders_mcp.db", "reminders.db.BAK2", "reminders.dbBAK"],
    )
    d.add_argument("--sample", type=int, default=3)

    c = sub.add_parser("compare", help="Compare source scored DB against current reminders.db.")
    c.add_argument("--current-db", default="reminders.db")
    c.add_argument("--source-db", required=True)
    c.add_argument("--source-table")
    c.add_argument("--score-column")
    c.add_argument("--sample", type=int, default=5)

    e = sub.add_parser("extract", help="Extract matched/unmatched score rows to CSV.")
    e.add_argument("--current-db", default="reminders.db")
    e.add_argument("--source-db", required=True)
    e.add_argument("--source-table")
    e.add_argument("--score-column")
    e.add_argument("--matched-out", default="outputs/matched_legacy_scores.csv")
    e.add_argument("--unmatched-out", default="outputs/unmatched_legacy_scores.csv")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.mode == "discover":
        paths = [Path(p).expanduser().resolve() for p in args.paths]
        hit_count = discover_mode(paths, args.sample)
        print(f"\nscore_like_tables_found={hit_count}")
        return

    current_path = Path(args.current_db).expanduser().resolve()
    source_path = Path(args.source_db).expanduser().resolve()
    if not current_path.exists():
        raise SystemExit(f"current db not found: {current_path}")
    if not source_path.exists():
        raise SystemExit(f"source db not found: {source_path}")

    with connect_ro(source_path) as src_conn:
        cfg = detect_source_config(src_conn, args.source_table, args.score_column)
        if cfg is None:
            raise SystemExit("No source score table detected. Use --source-table/--score-column.")
        print(f"source_table={cfg.table}")
        print(f"score_column={cfg.score_col}")
        source_rows = build_source_rows(src_conn, cfg)
        print(f"source_rows={len(source_rows)}")

    with connect_ro(current_path) as cur_conn:
        by_pike13, by_lesson_id, by_tuple = load_current_indices(cur_conn)
    matched, unmatched = compare_rows(source_rows, cfg, by_pike13, by_lesson_id, by_tuple)

    if args.mode == "compare":
        method_counts = defaultdict(int)
        for m in matched:
            method_counts[m["match_method"]] += 1
        print(f"matched={len(matched)} unmatched={len(unmatched)}")
        print(f"match_methods={dict(method_counts)}")
        if args.sample > 0:
            print("\nmatched_sample:")
            for row in matched[: args.sample]:
                print(json.dumps(row, default=str))
            print("\nunmatched_sample:")
            for row in unmatched[: args.sample]:
                print(json.dumps(row, default=str))
        return

    matched_out = Path(args.matched_out).expanduser().resolve()
    unmatched_out = Path(args.unmatched_out).expanduser().resolve()
    write_csv(matched_out, matched)
    write_csv(unmatched_out, unmatched)
    print(f"matched_rows={len(matched)} -> {matched_out}")
    print(f"unmatched_rows={len(unmatched)} -> {unmatched_out}")


if __name__ == "__main__":
    main()
