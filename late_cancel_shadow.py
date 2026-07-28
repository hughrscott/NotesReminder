#!/usr/bin/env python3
"""Track Pike13 late cancellations as a non-production shadow signal.

This module deliberately does not feed the actionable churn ranking. It stores
late-cancellation events, takes a weekly point-in-time observation for every
eligible current member (including zero-signal controls), and labels observations
only after 28 days of subsequent roster data exist.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "reminders.db"
MODELS_DIR = ROOT / "models"
SCHOOL_NAMES = {"westu-sor": "West U", "theheights-sor": "The Heights"}


def parse_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def truthy(value: Any) -> bool:
    return value in (True, 1, "1", "t", "true", "True", "T")


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS pike13_late_cancel_events (
            school_slug TEXT NOT NULL,
            visit_id TEXT NOT NULL,
            person_id TEXT NOT NULL,
            full_name TEXT NOT NULL,
            service_date TEXT NOT NULL,
            service_name TEXT,
            service_category TEXT,
            service_type TEXT,
            instructor_names TEXT,
            consider_member INTEGER NOT NULL,
            cancelled_to_start INTEGER,
            make_up_issued INTEGER,
            first_observed_at TEXT NOT NULL,
            last_observed_at TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            PRIMARY KEY (school_slug, visit_id)
        );
        CREATE INDEX IF NOT EXISTS idx_late_cancel_person_date
            ON pike13_late_cancel_events(school_slug, person_id, service_date);

        CREATE TABLE IF NOT EXISTS late_cancel_shadow_observations (
            as_of TEXT NOT NULL,
            school_slug TEXT NOT NULL,
            school_name TEXT NOT NULL,
            person_id TEXT NOT NULL,
            full_name TEXT NOT NULL,
            late_cancel_30d INTEGER NOT NULL,
            late_cancel_60d INTEGER NOT NULL,
            latest_late_cancel TEXT,
            days_since_last_visit INTEGER,
            future_visits INTEGER,
            completed_visits INTEGER,
            retained_28d INTEGER,
            evaluated_at TEXT,
            PRIMARY KEY (as_of, school_slug, person_id)
        );
        CREATE INDEX IF NOT EXISTS idx_shadow_maturity
            ON late_cancel_shadow_observations(as_of, evaluated_at);
        """
    )


def _as_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def ingest_late_cancel_records(
    conn: sqlite3.Connection,
    school_slug: str,
    scraped_at: str,
    records: Iterable[Mapping[str, Any]],
) -> int:
    """Upsert valid Pike13 late-cancellation records and return unique rows seen."""
    ensure_schema(conn)
    valid: dict[str, Mapping[str, Any]] = {}
    for record in records:
        if str(record.get("state") or "") != "late_canceled":
            continue
        visit_id = str(record.get("visit_id") or "").strip()
        person_id = str(record.get("person_id") or "").strip()
        service_date = parse_date(record.get("service_date"))
        if not visit_id or not person_id or service_date is None:
            continue
        valid[visit_id] = record

    with conn:
        for visit_id, record in valid.items():
            person_id = str(record["person_id"]).strip()
            service_date = str(record["service_date"])[:10]
            conn.execute(
                """
                INSERT INTO pike13_late_cancel_events (
                    school_slug, visit_id, person_id, full_name, service_date,
                    service_name, service_category, service_type, instructor_names,
                    consider_member, cancelled_to_start, make_up_issued,
                    first_observed_at, last_observed_at, raw_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(school_slug, visit_id) DO UPDATE SET
                    person_id=excluded.person_id,
                    full_name=excluded.full_name,
                    service_date=excluded.service_date,
                    service_name=excluded.service_name,
                    service_category=excluded.service_category,
                    service_type=excluded.service_type,
                    instructor_names=excluded.instructor_names,
                    consider_member=excluded.consider_member,
                    cancelled_to_start=excluded.cancelled_to_start,
                    make_up_issued=excluded.make_up_issued,
                    last_observed_at=excluded.last_observed_at,
                    raw_json=excluded.raw_json
                """,
                (
                    school_slug,
                    visit_id,
                    person_id,
                    str(record.get("full_name") or "").strip(),
                    service_date,
                    record.get("service_name"),
                    record.get("service_category"),
                    record.get("service_type"),
                    record.get("instructor_names"),
                    int(truthy(record.get("consider_member"))),
                    _as_int(record.get("cancelled_to_start")),
                    int(truthy(record.get("make_up_issued"))),
                    scraped_at,
                    scraped_at,
                    json.dumps(dict(record), ensure_ascii=True, default=str),
                ),
            )
    return len(valid)


def _latest_roster_rows(conn: sqlite3.Connection, as_of: date) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    rows: list[sqlite3.Row] = []
    for slug in SCHOOL_NAMES:
        stamp = conn.execute(
            """SELECT MAX(scraped_at) FROM pike13_current_member_snapshots
               WHERE school_slug=? AND DATE(scraped_at) <= DATE(?)""",
            (slug, as_of.isoformat()),
        ).fetchone()[0]
        if stamp is None:
            continue
        rows.extend(
            conn.execute(
                """SELECT * FROM pike13_current_member_snapshots
                   WHERE school_slug=? AND scraped_at=?""",
                (slug, stamp),
            ).fetchall()
        )
    return rows


def _eligible(row: Mapping[str, Any]) -> bool:
    plan_types = str(row["current_plan_types"] or "").casefold()
    return (
        str(row["person_state"] or "").casefold() == "active"
        and truthy(row["has_membership"])
        and not truthy(row["has_plan_on_hold"])
        and "recurring" in plan_types
        and (_as_int(row["completed_visits"]) or 0) >= 4
    )


def snapshot_current_members(conn: sqlite3.Connection, as_of: date) -> list[dict[str, Any]]:
    """Persist one weekly observation for every eligible member, including controls."""
    ensure_schema(conn)
    roster = [row for row in _latest_roster_rows(conn, as_of) if _eligible(row)]
    start_60 = (as_of - timedelta(days=59)).isoformat()
    start_30 = (as_of - timedelta(days=29)).isoformat()
    event_rows = conn.execute(
        """SELECT school_slug, person_id, service_date FROM pike13_late_cancel_events
           WHERE service_date BETWEEN ? AND ?
             AND consider_member=1
             AND LOWER(COALESCE(service_category,''))='lessons'""",
        (start_60, as_of.isoformat()),
    ).fetchall()
    events: dict[tuple[str, str], list[str]] = {}
    for event in event_rows:
        events.setdefault((event["school_slug"], event["person_id"]), []).append(
            event["service_date"]
        )

    observations: list[dict[str, Any]] = []
    with conn:
        for member in roster:
            key = (member["school_slug"], str(member["person_id"]))
            dates = events.get(key, [])
            observation = {
                "as_of": as_of.isoformat(),
                "school_slug": member["school_slug"],
                "school_name": member["school_name"],
                "person_id": str(member["person_id"]),
                "full_name": member["full_name"],
                "late_cancel_30d": sum(value >= start_30 for value in dates),
                "late_cancel_60d": len(dates),
                "latest_late_cancel": max(dates) if dates else None,
                "days_since_last_visit": _as_int(member["days_since_last_visit"]),
                "future_visits": _as_int(member["future_visits"]),
                "completed_visits": _as_int(member["completed_visits"]),
            }
            observations.append(observation)
            conn.execute(
                """INSERT OR REPLACE INTO late_cancel_shadow_observations (
                    as_of, school_slug, school_name, person_id, full_name,
                    late_cancel_30d, late_cancel_60d, latest_late_cancel,
                    days_since_last_visit, future_visits, completed_visits,
                    retained_28d, evaluated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,
                    COALESCE((SELECT retained_28d FROM late_cancel_shadow_observations
                              WHERE as_of=? AND school_slug=? AND person_id=?), NULL),
                    COALESCE((SELECT evaluated_at FROM late_cancel_shadow_observations
                              WHERE as_of=? AND school_slug=? AND person_id=?), NULL))""",
                (
                    *observation.values(),
                    as_of.isoformat(), member["school_slug"], str(member["person_id"]),
                    as_of.isoformat(), member["school_slug"], str(member["person_id"]),
                ),
            )
    return observations


def evaluate_matured_observations(
    conn: sqlite3.Connection, evaluation_date: date
) -> list[dict[str, Any]]:
    """Label unevaluated cohorts after 28 days using a subsequent complete roster."""
    ensure_schema(conn)
    cutoff = (evaluation_date - timedelta(days=28)).isoformat()
    matured = conn.execute(
        """SELECT * FROM late_cancel_shadow_observations
           WHERE as_of <= ? AND evaluated_at IS NULL ORDER BY as_of, school_slug, person_id""",
        (cutoff,),
    ).fetchall()
    result: list[dict[str, Any]] = []
    with conn:
        for row in matured:
            due_date = (
                date.fromisoformat(row["as_of"]) + timedelta(days=28)
            ).isoformat()
            source_ready = conn.execute(
                """SELECT 1 FROM pike13_current_member_snapshots
                   WHERE school_slug=? AND snapshot_date BETWEEN ? AND ? LIMIT 1""",
                (row["school_slug"], due_date, evaluation_date.isoformat()),
            ).fetchone()
            if source_ready is None:
                continue
            retained = int(
                conn.execute(
                    """SELECT 1 FROM pike13_current_member_snapshots
                       WHERE person_id=? AND snapshot_date BETWEEN ? AND ? LIMIT 1""",
                    (row["person_id"], due_date, evaluation_date.isoformat()),
                ).fetchone()
                is not None
            )
            conn.execute(
                """UPDATE late_cancel_shadow_observations
                   SET retained_28d=?, evaluated_at=?
                   WHERE as_of=? AND school_slug=? AND person_id=?""",
                (
                    retained,
                    evaluation_date.isoformat(),
                    row["as_of"],
                    row["school_slug"],
                    row["person_id"],
                ),
            )
            item = dict(row)
            item["retained_28d"] = retained
            item["evaluated_at"] = evaluation_date.isoformat()
            item["days_observed"] = (
                evaluation_date - date.fromisoformat(row["as_of"])
            ).days
            result.append(item)
    return result


def build_shadow_report(rows: list[Mapping[str, Any]], as_of: date) -> str:
    flagged = sorted(
        (row for row in rows if int(row["late_cancel_60d"]) > 0),
        key=lambda row: (
            -int(row["late_cancel_30d"]),
            -int(row["late_cancel_60d"]),
            row["school_name"],
            row["full_name"],
        ),
    )
    lines = [
        "LATE-CANCELLATION SHADOW SIGNAL",
        "SHADOW ONLY - DOES NOT AFFECT GM RANKING",
        f"AS OF {as_of.isoformat()}",
        "",
    ]
    for row in flagged:
        lines.append(
            f"{row['school_name']} | {row['full_name']} | "
            f"30D {row['late_cancel_30d']} | 60D {row['late_cancel_60d']} | "
            f"LATEST {row['latest_late_cancel']}"
        )
    if not flagged:
        lines.append("NO ELIGIBLE CURRENT MEMBER HAD A PRIVATE-LESSON LATE CANCELLATION IN 60 DAYS.")
    lines.extend(
        [
            "",
            f"CONTROL COHORT: {sum(int(row['late_cancel_60d']) == 0 for row in rows)} MEMBERS WITH ZERO EVENTS.",
            "OUTCOMES ARE LABELED ONLY AFTER 28 DAYS OF SUBSEQUENT ROSTER DATA.",
        ]
    )
    return "\n".join(lines).upper() + "\n"


def write_observations_csv(path: Path, rows: list[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "as_of", "school_slug", "school_name", "person_id", "full_name",
        "late_cancel_30d", "late_cancel_60d", "latest_late_cancel",
        "days_since_last_visit", "future_visits", "completed_visits",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows({field: row.get(field) for field in fields} for row in rows)


def import_json_file(
    conn: sqlite3.Connection, school_slug: str, path: Path, scraped_at: str
) -> int:
    payload = json.loads(path.read_text())
    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError(f"{path} does not contain a records list")
    return ingest_late_cancel_records(conn, school_slug, scraped_at, records)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today())
    parser.add_argument(
        "--import-json", action="append", default=[], metavar="SCHOOL=PATH",
        help="Import a captured Pike13 late-cancellation report before snapshotting",
    )
    parser.add_argument(
        "--output", type=Path, default=MODELS_DIR / "late_cancel_shadow_report.txt"
    )
    parser.add_argument(
        "--observations", type=Path,
        default=MODELS_DIR / "late_cancel_shadow_observations.csv",
    )
    args = parser.parse_args()
    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row
    try:
        ensure_schema(conn)
        scraped_at = datetime.now(timezone.utc).isoformat()
        for spec in args.import_json:
            school, separator, raw_path = spec.partition("=")
            if not separator or school not in SCHOOL_NAMES:
                raise ValueError("--import-json must be westu-sor=PATH or theheights-sor=PATH")
            count = import_json_file(conn, school, Path(raw_path), scraped_at)
            print(f"IMPORTED {count} UNIQUE LATE CANCELLATIONS FOR {school}")
        rows = snapshot_current_members(conn, args.as_of)
        evaluated = evaluate_matured_observations(conn, args.as_of)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(build_shadow_report(rows, args.as_of), encoding="ascii")
        write_observations_csv(args.observations, rows)
        print(f"SHADOW OBSERVATIONS: {len(rows)}")
        print(f"MATURED OBSERVATIONS EVALUATED: {len(evaluated)}")
        print(f"SHADOW REPORT: {args.output}")
        print(f"OBSERVATIONS CSV: {args.observations}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
