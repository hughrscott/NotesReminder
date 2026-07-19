from datetime import date
import json

import pytest

from actionable_churn_report import (
    Attendance,
    DIRECT_RISK,
    build_attendance,
    display_plans,
    load_direct_evidence,
    load_hold_returns,
    norm_name,
    resolve_name,
    score_candidate,
)


def test_display_plans_removes_administrative_pass_labels():
    assert display_plans(
        "Make-Up Lesson - 45 Minutes, Performance Program - 45 Minute Lessons, Late Cancellation Fee"
    ) == "Performance Program - 45 Minute Lessons"
    assert display_plans("Make-Up Lesson") == "ACTIVE RECURRING MEMBERSHIP"


def test_direct_evidence_ignores_ambiguous_identity_components():
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE pike13_people (person_id TEXT, person_identity_id TEXT);
        CREATE TABLE persons (person_id TEXT, resolution_status TEXT);
        CREATE TABLE dialpad_voice_events (event_id TEXT, person_id TEXT);
        CREATE TABLE dialpad_call_reviews (
            voice_event_id TEXT, event_at TEXT, recap_text TEXT, transcript_text TEXT
        );
        CREATE TABLE school_email_messages (
            person_id TEXT, message_at TEXT, subject TEXT, body TEXT, snippet TEXT
        );
        """
    )
    conn.execute("INSERT INTO persons VALUES ('shared', 'conflict')")
    conn.execute("INSERT INTO pike13_people VALUES ('student-1', 'shared')")
    conn.execute("INSERT INTO dialpad_voice_events VALUES ('call-1', 'shared')")
    conn.execute(
        "INSERT INTO dialpad_call_reviews VALUES "
        "('call-1', '2026-07-17', 'cancel membership', '')"
    )

    assert load_direct_evidence(
        conn, [{"person_id": "student-1"}], date(2026, 7, 18)
    ) == {}

    conn.execute("UPDATE persons SET resolution_status='resolved'")
    assert load_direct_evidence(
        conn, [{"person_id": "student-1"}], date(2026, 7, 18)
    )["student-1"]


def test_attendance_uses_lessons_table_for_future_schedule_and_requires_both_schools():
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE lessons (
            school_id INTEGER, lesson_id TEXT, lesson_date TEXT,
            lesson_type TEXT, students_raw TEXT, instructor_id INTEGER
        );
        CREATE TABLE instructors (instructor_id INTEGER, instructor_name TEXT);
        CREATE TABLE lesson_notes (lesson_id TEXT, notes_text TEXT);
        CREATE TABLE roster (school_slug TEXT, full_name TEXT, person_id TEXT);
        INSERT INTO instructors VALUES (1, 'Teacher');
        INSERT INTO roster VALUES ('westu-sor', 'Student One', '1');
        INSERT INTO lessons VALUES
            (1, 'past-1', '2026-07-17', 'Guitar Lesson', 'Student One', 1),
            (1, 'makeup-1', '2026-07-18', 'MAKE UP Guitar Lessons - 45 Minutes', 'Student One', 1),
            (1, 'future-1', '2026-08-10', 'Guitar Lesson', 'Student One', 1),
            (2, 'past-2', '2026-07-18', 'Drum Lesson', 'Coverage Student', 1),
            (2, 'future-2', '2026-08-10', 'Drum Lesson', 'Coverage Student', 1);
        """
    )
    roster = conn.execute("SELECT * FROM roster").fetchall()

    attendance_rows, _ = build_attendance(conn, roster, date(2026, 7, 18))
    assert attendance_rows[("westu-sor", "1")].lifetime == 1
    assert attendance_rows[("westu-sor", "1")].future_28 == 1

    conn.execute("DELETE FROM lessons WHERE school_id=2 AND lesson_date > '2026-07-18'")
    with pytest.raises(RuntimeError, match="Future lesson coverage for theheights-sor"):
        build_attendance(conn, roster, date(2026, 7, 18))


def member_row(**overrides):
    row = {
        "person_id": "15201916",
        "full_name": "Jaelle Reyes",
        "school_slug": "westu-sor",
        "school_name": "West U",
        "person_state": "active",
        "current_plans": "Lessons Only - 45 Minute Lessons",
        "current_plan_types": "Recurring",
        "has_plan_on_hold": 0,
        "completed_visits": 40,
        "future_visits": 5,
        "last_visit_date": "2026-06-18",
        "days_since_last_visit": 30,
        "account_manager_names": "Parent Reyes",
        "account_manager_phones": "7135551212",
        "account_manager_emails": "parent@example.com",
        "guardian_name": "",
        "guardian_email": "",
    }
    row.update(overrides)
    return row


def attendance(**overrides):
    data = {
        "matched_name": "jalle reyes",
        "lifetime": 40,
        "recent_28": 0,
        "prior_28": 4,
        "earlier_28": 4,
        "baseline_28": 4.0,
        "tenure_days": 180,
        "last_recurring_visit": date(2026, 7, 10),
        "primary_instructor": "Alex Teacher",
        "concern_note": None,
    }
    data.update(overrides)
    return Attendance(**data)


def test_safe_typo_match_resolves_jaelle_to_jalle():
    available = {norm_name("Jalle Reyes"), norm_name("Antonio Reyes")}
    assert resolve_name("Jaelle Reyes", available) == "jalle reyes"


def test_ambiguous_fuzzy_name_is_not_forced():
    available = {norm_name("Jon Smith"), norm_name("John Smith")}
    assert resolve_name("Jonn Smith", available) is None


def test_30_day_absence_without_direct_evidence_is_verify_not_alert():
    result = score_candidate(member_row(), attendance(), [])
    assert result is not None
    assert result.tier == "VERIFY"
    assert "VERIFY" in result.action
    assert "BEFORE CONTACTING" in result.action


def test_direct_cancellation_evidence_promotes_to_alert():
    result = score_candidate(
        member_row(days_since_last_visit=8, last_visit_date="2026-07-10"),
        attendance(recent_28=4, baseline_28=4, prior_28=4, earlier_28=4),
        ["CALL 2026-07-10: parent wants to cancel the membership"],
    )
    assert result is not None
    assert result.tier == "ALERT"
    assert result.points >= 100
    assert "CALL" in result.action


def test_canceling_one_lesson_is_not_a_churn_alert():
    assert not DIRECT_RISK.search("I need to cancel tomorrow's guitar lesson due to work")
    assert DIRECT_RISK.search("I need to cancel the membership at the end of this month")


def test_missing_communication_does_not_create_a_candidate():
    result = score_candidate(
        member_row(days_since_last_visit=3, last_visit_date="2026-07-15"),
        attendance(recent_28=4, baseline_28=4, prior_28=4, earlier_28=4),
        [],
    )
    assert result is None


def test_recent_hold_without_future_visits_gets_schedule_recovery_action():
    result = score_candidate(
        member_row(
            full_name="Caleb Shannon",
            future_visits=0,
            days_since_last_visit=49,
            last_visit_date="2026-05-30",
        ),
        attendance(recent_28=0, prior_28=2, earlier_28=2, baseline_28=2),
        [],
        {
            "end_date": date(2026, 6, 30),
            "days_since_end": 18,
            "plan": "Little Wing",
        },
    )
    assert result is not None
    assert result.tier == "VERIFY"
    assert "HOLD ENDED 2026-06-30" in result.reasons[0]
    assert "RESTORE CALEB SHANNON'S POST-HOLD SCHEDULE" in result.action.upper()


def test_recent_hold_with_confirmed_schedule_is_suppressed():
    row = member_row(
        full_name="Everleigh Major",
        person_id="14559559",
        last_visit_date="2026-06-01",
        days_since_last_visit=47,
        future_visits=52,
    )
    recent_hold = {
        "end_date": date(2026, 6, 21),
        "days_since_end": 27,
        "plan": "Rookies",
    }

    assert score_candidate(row, attendance(future_28=0), [], recent_hold) is None


def test_recent_hold_with_schedule_does_not_hide_direct_negative_evidence():
    row = member_row(
        full_name="Example Student",
        person_id="123",
        last_visit_date="2026-06-01",
        days_since_last_visit=47,
        future_visits=4,
    )
    recent_hold = {
        "end_date": date(2026, 6, 21),
        "days_since_end": 27,
        "plan": "Rookies",
    }

    candidate = score_candidate(
        row,
        attendance(future_28=0),
        ["MEMBERSHIP CANCELLATION REQUEST"],
        recent_hold,
    )
    assert candidate is not None
    assert candidate.tier == "ALERT"


def test_ended_old_plan_with_new_schedule_is_suppressed():
    row = member_row(
        full_name="Marco Anderson",
        person_id="12228503",
        last_visit_date="2026-06-27",
        days_since_last_visit=21,
        future_visits=0,
    )
    old_plan_hold = {
        "end_date": date(2026, 6, 18),
        "days_since_end": 30,
        "plan": "Performance Program",
        "plan_ended": True,
    }

    assert score_candidate(row, attendance(future_28=1), [], old_plan_hold) is None


def test_ended_old_plan_without_schedule_never_gets_restore_action():
    row = member_row(
        full_name="Former Plan Student",
        person_id="124",
        last_visit_date="2026-06-01",
        days_since_last_visit=47,
        future_visits=0,
    )
    old_plan_hold = {
        "end_date": date(2026, 6, 21),
        "days_since_end": 27,
        "plan": "Old Plan",
        "plan_ended": True,
    }

    candidate = score_candidate(row, attendance(), [], old_plan_hold)
    assert candidate is not None
    assert "RESTORE" not in candidate.action


def test_hold_loader_separates_active_returns_from_recent_recoveries(tmp_path, monkeypatch):
    rows = [
        {
            "client": "Active Student",
            "plan": "Rookies",
            "on_hold": True,
            "hold_end": "Jul 25, 2026",
            "scraped_at": "2026-07-18",
        },
        {
            "client": "Caleb Shannon",
            "plan": "Little Wing",
            "on_hold": False,
            "hold_end": "Jun 30, 2026",
            "scraped_at": "2026-07-18",
        },
    ]
    for slug in ("westu-sor", "theheights-sor"):
        (tmp_path / f"pike13_holds_{slug}.json").write_text(json.dumps(rows))
    monkeypatch.setattr("actionable_churn_report.MODELS_DIR", tmp_path)
    upcoming, active, recent, warnings = load_hold_returns(date(2026, 7, 18))
    assert not warnings
    assert active == {"West U": 1, "The Heights": 1}
    assert len(upcoming) == 2
    assert recent[("westu-sor", "caleb shannon")]["days_since_end"] == 18


def test_hold_loader_fails_closed_on_stale_source_timestamp(tmp_path, monkeypatch):
    rows = [{
        "client": "Old Hold",
        "on_hold": True,
        "hold_end": "Jul 25, 2026",
        "scraped_at": "2026-07-10",
    }]
    for slug in ("westu-sor", "theheights-sor"):
        (tmp_path / f"pike13_holds_{slug}.json").write_text(json.dumps(rows))
    monkeypatch.setattr("actionable_churn_report.MODELS_DIR", tmp_path)

    with pytest.raises(RuntimeError, match="Hold snapshot .* is 8 days old"):
        load_hold_returns(date(2026, 7, 18))


def test_non_recurring_and_low_history_members_are_excluded():
    assert score_candidate(
        member_row(current_plan_types="Single_Visit_Pass"), attendance(), []
    ) is None
    assert score_candidate(
        member_row(completed_visits=3), attendance(lifetime=3), []
    ) is None


def test_note_concern_cannot_create_risk_without_attendance_signal():
    result = score_candidate(
        member_row(days_since_last_visit=2, last_visit_date="2026-07-16"),
        attendance(
            recent_28=4,
            baseline_28=4,
            prior_28=4,
            earlier_28=4,
            concern_note="Student seems frustrated",
        ),
        [],
    )
    assert result is None


def test_names_and_actions_are_ascii():
    result = score_candidate(
        member_row(full_name="Ricardo Quiñonez"), attendance(), []
    )
    assert result is not None
    combined = result.name + result.action
    combined.encode("ascii")
