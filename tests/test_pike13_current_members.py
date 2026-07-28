import json
import sqlite3
from pathlib import Path

import pytest

from scrape_pike13_current_members import (
    FULL_ROSTER_TIMEOUT_MS,
    coverage_request_bodies,
    full_roster_request,
    rows_to_records,
    store_snapshot,
)


FIELDS = [
    "person_id",
    "full_name",
    "person_state",
    "current_plans",
    "current_plan_types",
    "current_plan_revenue_category",
    "client_since_date",
    "last_visit_date",
    "days_since_last_visit",
    "completed_visits",
    "future_visits",
    "has_membership",
    "has_plan_on_hold",
]


def test_full_roster_request_allows_slow_pike13_reports():
    assert FULL_ROSTER_TIMEOUT_MS >= 120_000


def test_rows_are_mapped_by_field_name_not_assumed_position():
    row = [
        "15201916",
        "Jaelle Reyes",
        "active",
        "Lessons Only - 45 Minute Lessons",
        "Recurring",
        "Lessons",
        "2026-05-30",
        "2026-06-27",
        20,
        4,
        53,
        "t",
        "f",
    ]
    record = rows_to_records(FIELDS, [row])[0]
    assert record["current_plans"] == "Lessons Only - 45 Minute Lessons"
    assert record["days_since_last_visit"] == 20
    assert record["completed_visits"] == 4
    assert record["future_visits"] == 53


def test_rows_reject_schema_drift():
    with pytest.raises(ValueError, match="values for"):
        rows_to_records(["person_id", "full_name"], [["1"]])


def test_full_roster_request_expands_limit_without_mutating_original():
    original = {"data": {"type": "queries", "attributes": {"page": {}, "filter": []}}}
    expanded = full_roster_request(original, 145)
    assert expanded["data"]["attributes"]["page"] == {"limit": 145}
    assert original["data"]["attributes"]["page"] == {}


def test_coverage_requests_avoid_gateway_timeout_for_rosters_under_200():
    original = {"data": {"type": "queries", "attributes": {"page": {}, "filter": []}}}
    requests = coverage_request_bodies(original, 145)
    assert len(requests) == 2
    attrs = [request["data"]["attributes"] for request in requests]
    assert [item["page"] for item in attrs] == [{"limit": 100}, {"limit": 100}]
    assert [item["sort"] for item in attrs] == [["person_id"], ["person_id-"]]
    assert original["data"]["attributes"] == {"page": {}, "filter": []}


def test_store_snapshot_preserves_named_values(tmp_path: Path):
    db = tmp_path / "test.db"
    record = rows_to_records(
        FIELDS,
        [["1", "Student One", "active", "Rock 101", "Recurring", "Rock 101",
          "2025-01-01", "2026-07-10", 8, 90, 12, "t", "f"]],
    )[0]
    store_snapshot(db, "westu-sor", "West U", "2026-07-18T12:00:00+00:00", [record])
    conn = sqlite3.connect(db)
    row = conn.execute(
        "SELECT current_plans, days_since_last_visit, completed_visits, "
        "future_visits, has_membership, has_plan_on_hold, raw_json "
        "FROM pike13_current_member_snapshots"
    ).fetchone()
    assert row[:6] == ("Rock 101", 8, 90, 12, 1, 0)
    assert json.loads(row[6])["current_plans"] == "Rock 101"


def test_store_snapshot_rejects_empty_or_duplicate_roster(tmp_path: Path):
    db = tmp_path / "test.db"
    with pytest.raises(ValueError, match="empty roster"):
        store_snapshot(db, "westu-sor", "West U", "2026-07-18T12:00:00+00:00", [])

    record = rows_to_records(
        FIELDS,
        [["1", "Student One", "active", "Rock 101", "Recurring", "Rock 101",
          "2025-01-01", "2026-07-10", 8, 90, 12, "t", "f"]],
    )[0]
    with pytest.raises(ValueError, match="duplicate person_id"):
        store_snapshot(
            db, "westu-sor", "West U", "2026-07-18T12:00:00+00:00",
            [record, dict(record)],
        )
