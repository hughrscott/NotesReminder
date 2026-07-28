from datetime import date
import sqlite3

from late_cancel_shadow import (
    build_shadow_report,
    ensure_schema,
    evaluate_matured_observations,
    ingest_late_cancel_records,
    snapshot_current_members,
)


def make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE pike13_current_member_snapshots (
            school_slug TEXT, school_name TEXT, scraped_at TEXT, snapshot_date TEXT,
            person_id TEXT, full_name TEXT, person_state TEXT, current_plans TEXT,
            current_plan_types TEXT, completed_visits INTEGER, future_visits INTEGER,
            days_since_last_visit INTEGER, has_membership INTEGER,
            has_plan_on_hold INTEGER
        );
        """
    )
    ensure_schema(conn)
    return conn


def add_roster(conn: sqlite3.Connection, snapshot_date: str, people: list[tuple[str, str]]) -> None:
    for person_id, name in people:
        conn.execute(
            """INSERT INTO pike13_current_member_snapshots VALUES
            ('westu-sor','West U',?,?,?,?,'active','Lessons Only','Recurring',
             20,8,4,1,0)""",
            (snapshot_date + "T06:00:00+00:00", snapshot_date, person_id, name),
        )


def late_record(person_id: str, visit_id: str, service_date: str, **overrides):
    row = {
        "person_id": person_id,
        "visit_id": visit_id,
        "full_name": f"Student {person_id}",
        "service_date": service_date,
        "service_name": "Guitar Lesson",
        "service_category": "Lessons",
        "service_type": "private_lesson",
        "state": "late_canceled",
        "consider_member": "t",
        "cancelled_to_start": 15,
        "make_up_issued": "f",
        "instructor_names": "Teacher",
    }
    row.update(overrides)
    return row


def test_ingest_is_idempotent_and_rejects_non_late_rows():
    conn = make_db()
    records = [
        late_record("1", "v1", "2026-07-10"),
        late_record("1", "v1", "2026-07-10"),
        late_record("2", "v2", "2026-07-11", state="completed"),
    ]

    stored = ingest_late_cancel_records(
        conn, "westu-sor", "2026-07-19T06:00:00+00:00", records
    )

    assert stored == 1
    assert conn.execute("SELECT COUNT(*) FROM pike13_late_cancel_events").fetchone()[0] == 1


def test_weekly_snapshot_tracks_signal_for_all_members_without_changing_ranking():
    conn = make_db()
    add_roster(conn, "2026-07-19", [("1", "Repeated Late"), ("2", "Control Member")])
    conn.execute(
        """INSERT INTO pike13_current_member_snapshots VALUES
        ('westu-sor','West U','2026-07-19T06:00:00+00:00','2026-07-19',
         '3','Mixed Plan Member','active','Make-up Lesson, Performance Program',
         'Recurring, Pass',20,8,4,1,0)"""
    )
    ingest_late_cancel_records(
        conn,
        "westu-sor",
        "2026-07-19T06:00:00+00:00",
        [
            late_record("1", "v1", "2026-07-15"),
            late_record("1", "v2", "2026-06-15"),
            late_record("1", "v3", "2026-07-16", service_category="Classes and Rehearsals"),
        ],
    )

    rows = snapshot_current_members(conn, date(2026, 7, 19))

    by_id = {row["person_id"]: row for row in rows}
    assert set(by_id) == {"1", "2", "3"}
    assert (by_id["1"]["late_cancel_30d"], by_id["1"]["late_cancel_60d"]) == (1, 2)
    assert (by_id["2"]["late_cancel_30d"], by_id["2"]["late_cancel_60d"]) == (0, 0)
    report = build_shadow_report(rows, date(2026, 7, 19))
    assert "SHADOW ONLY - DOES NOT AFFECT GM RANKING" in report
    assert "REPEATED LATE" in report
    assert "CONTROL MEMBER" not in report


def test_evaluation_labels_matured_observations_after_28_days():
    conn = make_db()
    add_roster(conn, "2026-06-21", [("1", "Retained Member"), ("2", "Departed Member")])
    snapshot_current_members(conn, date(2026, 6, 21))
    add_roster(conn, "2026-07-19", [("1", "Retained Member")])

    evaluated = evaluate_matured_observations(conn, date(2026, 7, 19))

    by_id = {row["person_id"]: row for row in evaluated}
    assert by_id["1"]["retained_28d"] == 1
    assert by_id["2"]["retained_28d"] == 0
    assert all(row["days_observed"] >= 28 for row in evaluated)


def test_evaluation_waits_for_a_roster_captured_after_the_28_day_due_date():
    conn = make_db()
    add_roster(conn, "2026-06-21", [("1", "Unresolved Member")])
    snapshot_current_members(conn, date(2026, 6, 21))

    assert evaluate_matured_observations(conn, date(2026, 7, 19)) == []
    stored = conn.execute(
        "SELECT retained_28d, evaluated_at FROM late_cancel_shadow_observations"
    ).fetchone()
    assert tuple(stored) == (None, None)
