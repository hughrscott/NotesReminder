#!/usr/bin/env python3
"""Generate an actionable weekly churn-prevention report.

This report deliberately ranks operational intervention priority rather than
claiming calibrated churn probabilities. It uses current Pike13 membership
snapshots, within-student recurring-attendance change, live last-visit dates,
active holds, and only positively observed communication/note evidence.
Missing communication, note-completeness scores, lifetime instructor count,
and Pike13's pass-plan end field never influence ranking.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable, Mapping

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "reminders.db"
MODELS_DIR = ROOT / "models"
MAX_ACTIONS = 10
MAX_ROSTER_AGE_DAYS = 2
MAX_ATTENDANCE_AGE_DAYS = 7
SCHOOL_ID_TO_SLUG = {1: "westu-sor", 2: "theheights-sor"}
NON_RECURRING_SERVICE = re.compile(
    r"\b(camp|trial|orientation|workshop|admin time|birthday party|"
    r"make[- ]?up|late cancellation|cancellation fee)\b",
    re.I,
)
ADMIN_PLAN_LABEL = re.compile(
    r"\b(camp|trial|orientation|workshop|admin time|birthday party|"
    r"make[- ]?up|late cancellation|cancellation fee)\b",
    re.I,
)
DIRECT_RISK = re.compile(
    r"(?:\bcancel(?:ing|led|lation)?\b.{0,35}\b(?:membership|enrollment|account|program)\b"
    r"|\b(?:membership|enrollment|account|program)\b.{0,35}\bcancel(?:ing|led|lation)?\b"
    r"|\bquit(?:ting)?\b|\bterminate\b|\brefund\b.{0,25}\b(?:membership|tuition|program)\b"
    r"|\bleav(?:e|ing) (?:the )?(?:school|program)\b|\bstop(?:ping)? lessons?\b"
    r"|\btoo expensive\b|\bcan(?:not|'t) afford\b|\bunhappy\b|\blost interest\b"
    r"|\bdoes(?:n't| not) want to continue\b)",
    re.I | re.S,
)
NOTE_CONCERN = re.compile(
    r"\b(wants? to quit|lost interest|doesn't want to|does not want to|"
    r"unmotivated|not practicing|frustrated|disengaged)\b",
    re.I,
)


def ascii_text(value: Any) -> str:
    text = "" if value is None else str(value)
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")


def norm_name(value: str) -> str:
    value = ascii_text(value).casefold()
    return re.sub(r"[^a-z0-9]+", " ", value).strip()


def parse_iso_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def parse_hold_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    for fmt in ("%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(value).strip(), fmt).date()
        except ValueError:
            pass
    return None


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass
class Attendance:
    matched_name: str | None = None
    lifetime: int = 0
    recent_28: int = 0
    prior_28: int = 0
    earlier_28: int = 0
    future_28: int = 0
    baseline_28: float = 0.0
    tenure_days: int = 0
    first_recurring_visit: date | None = None
    last_recurring_visit: date | None = None
    primary_instructor: str = "UNKNOWN"
    concern_note: str | None = None


@dataclass
class Candidate:
    person_id: str
    name: str
    school_slug: str
    school_name: str
    plans: str
    days_since_last_visit: int
    last_visit_date: date | None
    future_visits: int
    completed_visits: int
    account_manager: str
    contact: str
    attendance: Attendance
    direct_evidence: list[str] = field(default_factory=list)
    points: int = 0
    tier: str = "MONITOR"
    reasons: list[str] = field(default_factory=list)
    action: str = ""


@dataclass
class AuditCounts:
    raw_members: int = 0
    eligible_members: int = 0
    excluded_non_active: int = 0
    excluded_non_recurring: int = 0
    excluded_new_or_low_history: int = 0
    unmatched_attendance: int = 0


def latest_roster_rows(conn: sqlite3.Connection, as_of: date) -> tuple[list[sqlite3.Row], dict[str, str]]:
    conn.row_factory = sqlite3.Row
    table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='pike13_current_member_snapshots'"
    ).fetchone()
    if not table:
        raise RuntimeError(
            "No corrected Pike13 roster snapshots. Run scrape_pike13_current_members.py first."
        )
    latest = {
        row["school_slug"]: row["scraped_at"]
        for row in conn.execute(
            "SELECT school_slug, MAX(scraped_at) scraped_at "
            "FROM pike13_current_member_snapshots GROUP BY school_slug"
        )
    }
    missing = {"westu-sor", "theheights-sor"} - set(latest)
    if missing:
        raise RuntimeError(f"Missing current roster snapshots for: {', '.join(sorted(missing))}")
    for slug, stamp in latest.items():
        scraped = datetime.fromisoformat(stamp.replace("Z", "+00:00")).date()
        age = (as_of - scraped).days
        if age < 0 or age > MAX_ROSTER_AGE_DAYS:
            raise RuntimeError(f"Roster for {slug} is {age} days old; refresh before reporting")
    rows: list[sqlite3.Row] = []
    for slug, stamp in latest.items():
        rows.extend(
            conn.execute(
                "SELECT * FROM pike13_current_member_snapshots "
                "WHERE school_slug=? AND scraped_at=?",
                (slug, stamp),
            ).fetchall()
        )
    return rows, latest


def split_students(raw: str) -> Iterable[str]:
    for name in re.split(r",\s*", raw or ""):
        if name.strip():
            yield name.strip()


def resolve_name(target: str, available: set[str]) -> str | None:
    key = norm_name(target)
    if key in available:
        return key
    parts = key.split()
    if len(parts) < 2:
        return None
    same_last = [candidate for candidate in available if candidate.split()[-1:] == parts[-1:]]
    scored = sorted(
        ((SequenceMatcher(None, key, candidate).ratio(), candidate) for candidate in same_last),
        reverse=True,
    )
    if not scored or scored[0][0] < 0.90:
        return None
    if len(scored) > 1 and scored[0][0] - scored[1][0] < 0.10:
        return None
    return scored[0][1]


def build_attendance(
    conn: sqlite3.Connection,
    roster_rows: list[sqlite3.Row],
    as_of: date,
) -> tuple[dict[tuple[str, str], Attendance], date]:
    future_end = as_of + timedelta(days=28)
    minimum_future_coverage = as_of + timedelta(days=21)
    for school_id, slug in SCHOOL_ID_TO_SLUG.items():
        latest_future = parse_iso_date(
            conn.execute(
                "SELECT MAX(lesson_date) FROM lessons "
                "WHERE school_id=? AND DATE(lesson_date) > DATE(?) "
                "AND DATE(lesson_date) <= DATE(?)",
                (school_id, as_of.isoformat(), future_end.isoformat()),
            ).fetchone()[0]
        )
        if not latest_future or latest_future < minimum_future_coverage:
            raise RuntimeError(
                f"Future lesson coverage for {slug} ends at "
                f"{latest_future or 'NONE'}; refresh through {future_end} before reporting"
            )

    max_lesson = parse_iso_date(
        conn.execute(
            "SELECT MAX(lesson_date) FROM lessons WHERE DATE(lesson_date) <= DATE(?)",
            (as_of.isoformat(),),
        ).fetchone()[0]
    )
    if not max_lesson:
        raise RuntimeError("Lessons table is empty")
    age = (as_of - max_lesson).days
    if age < 0 or age > MAX_ATTENDANCE_AGE_DAYS:
        raise RuntimeError(
            f"Lesson attendance is {age} days stale (latest {max_lesson}); refresh before reporting"
        )

    per_school: dict[str, dict[str, list[tuple[date, str, str, str]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    query = """
        SELECT l.school_id, l.lesson_id, l.lesson_date, l.lesson_type,
               l.students_raw, COALESCE(i.instructor_name, 'UNKNOWN') instructor_name,
               COALESCE(ln.notes_text, '') notes_text
        FROM lessons l
        LEFT JOIN instructors i ON i.instructor_id=l.instructor_id
        LEFT JOIN lesson_notes ln ON ln.lesson_id=l.lesson_id
        WHERE l.students_raw IS NOT NULL AND TRIM(l.students_raw) != ''
          AND DATE(l.lesson_date) <= DATE(?)
    """
    for row in conn.execute(query, (future_end.isoformat(),)):
        slug = SCHOOL_ID_TO_SLUG.get(as_int(row[0]))
        lesson_date = parse_iso_date(row[2])
        if not slug or not lesson_date or NON_RECURRING_SERVICE.search(row[3] or ""):
            continue
        for student in split_students(row[4]):
            per_school[slug][norm_name(student)].append(
                (lesson_date, str(row[1]), row[5] or "UNKNOWN", row[6] or "")
            )

    result: dict[tuple[str, str], Attendance] = {}
    for member in roster_rows:
        slug, name = member["school_slug"], member["full_name"]
        available = {
            name
            for name, visits in per_school[slug].items()
            if any(visit[0] <= as_of for visit in visits)
        }
        matched = resolve_name(name, available)
        attendance = Attendance(matched_name=matched)
        if matched:
            visits = per_school[slug][matched]
            unique: dict[str, tuple[date, str, str, str]] = {v[1]: v for v in visits}
            visits = [visit for visit in unique.values() if visit[0] <= as_of]
            attendance.future_28 = sum(
                as_of < visit[0] <= future_end for visit in unique.values()
            )
            attendance.lifetime = len(visits)
            attendance.first_recurring_visit = min(v[0] for v in visits)
            attendance.last_recurring_visit = max(v[0] for v in visits)
            attendance.tenure_days = (as_of - attendance.first_recurring_visit).days
            recent_start = as_of - timedelta(days=27)
            prior_start = as_of - timedelta(days=55)
            earlier_start = as_of - timedelta(days=83)
            attendance.recent_28 = sum(recent_start <= v[0] <= as_of for v in visits)
            attendance.prior_28 = sum(prior_start <= v[0] < recent_start for v in visits)
            attendance.earlier_28 = sum(earlier_start <= v[0] < prior_start for v in visits)
            attendance.baseline_28 = (
                attendance.prior_28 + attendance.earlier_28
            ) / 2.0
            recent_visits = [v for v in visits if v[0] >= earlier_start]
            instructor_counts = Counter(v[2] for v in recent_visits if v[2] != "UNKNOWN")
            if instructor_counts:
                attendance.primary_instructor = instructor_counts.most_common(1)[0][0]
            concern_rows = sorted(
                (v for v in visits if v[0] >= as_of - timedelta(days=90)
                 and NOTE_CONCERN.search(v[3])),
                key=lambda v: v[0],
                reverse=True,
            )
            if concern_rows:
                clean = re.sub(r"\s+", " ", concern_rows[0][3]).strip()
                attendance.concern_note = clean[:160]
        result[(slug, str(member["person_id"]))] = attendance
    return result, max_lesson


def load_direct_evidence(
    conn: sqlite3.Connection,
    roster_rows: Iterable[Mapping[str, Any] | sqlite3.Row],
    as_of: date,
) -> dict[str, list[str]]:
    """Return only positively observed, identity-linked direct risk evidence."""
    person_ids = {str(row["person_id"]) for row in roster_rows}
    evidence: dict[str, list[str]] = defaultdict(list)
    since = (as_of - timedelta(days=120)).isoformat()

    calls = conn.execute(
        """
        SELECT pp.person_id, dcr.event_at,
               COALESCE(dcr.recap_text, '') || ' ' || COALESCE(dcr.transcript_text, '') text
        FROM pike13_people pp
        JOIN persons p ON p.person_id=pp.person_identity_id
                      AND p.resolution_status='resolved'
        JOIN dialpad_voice_events dve ON dve.person_id=pp.person_identity_id
        JOIN dialpad_call_reviews dcr ON dcr.voice_event_id=dve.event_id
        WHERE dcr.event_at >= ?
        """,
        (since,),
    )
    for person_id, event_at, text in calls:
        if str(person_id) in person_ids and DIRECT_RISK.search(text or ""):
            excerpt = re.sub(r"\s+", " ", text).strip()[:180]
            evidence[str(person_id)].append(f"CALL {str(event_at)[:10]}: {excerpt}")

    emails = conn.execute(
        """
        SELECT pp.person_id, sem.message_at, COALESCE(sem.subject, '') || ' ' ||
               COALESCE(sem.body, sem.snippet, '') text
        FROM pike13_people pp
        JOIN persons p ON p.person_id=pp.person_identity_id
                      AND p.resolution_status='resolved'
        JOIN school_email_messages sem ON sem.person_id=pp.person_identity_id
        WHERE sem.message_at >= ?
        """,
        (since,),
    )
    for person_id, event_at, text in emails:
        if str(person_id) in person_ids and DIRECT_RISK.search(text or ""):
            excerpt = re.sub(r"\s+", " ", text).strip()[:180]
            evidence[str(person_id)].append(f"EMAIL {str(event_at)[:10]}: {excerpt}")
    return evidence


def contact_for(row: Mapping[str, Any] | sqlite3.Row) -> tuple[str, str]:
    manager = ascii_text(
        row["account_manager_names"] or row["guardian_name"] or "THE FAMILY"
    )
    contact = ascii_text(
        row["account_manager_phones"]
        or row["account_manager_emails"]
        or row["guardian_email"]
        or "THE CONTACT LISTED IN PIKE13"
    )
    return manager, contact


def display_plans(value: Any) -> str:
    """Keep recurring program names while hiding administrative pass labels."""
    plans = [ascii_text(part).strip() for part in str(value or "").split(",")]
    plans = [plan for plan in plans if plan and not ADMIN_PLAN_LABEL.search(plan)]
    return ", ".join(plans) or "ACTIVE RECURRING MEMBERSHIP"


def score_candidate(
    row: Mapping[str, Any] | sqlite3.Row,
    attendance: Attendance,
    evidence: list[str],
    recent_hold: dict[str, Any] | None = None,
) -> Candidate | None:
    state = (row["person_state"] or "").casefold()
    plan_types = (row["current_plan_types"] or "").casefold()
    plans = display_plans(row["current_plans"])
    if state not in ("active", ""):
        return None
    if "recurring" not in plan_types:
        return None
    if as_int(row["has_plan_on_hold"]) == 1:
        return None
    if (
        as_int(row["completed_visits"]) < 4
        or attendance.lifetime < 4
        or attendance.tenure_days < 30
    ):
        return None

    last_visit = parse_iso_date(row["last_visit_date"])
    days = as_int(row["days_since_last_visit"], 999)
    manager, contact = contact_for(row)
    candidate = Candidate(
        person_id=str(row["person_id"]),
        name=ascii_text(row["full_name"]),
        school_slug=row["school_slug"],
        school_name=ascii_text(row["school_name"]),
        plans=plans,
        days_since_last_visit=days,
        last_visit_date=last_visit,
        # The lessons export can omit group-program schedules. A positive signal
        # from either source confirms a schedule; zero requires both to be zero.
        future_visits=max(attendance.future_28, as_int(row["future_visits"])),
        completed_visits=as_int(row["completed_visits"]),
        account_manager=manager,
        contact=contact,
        attendance=attendance,
        direct_evidence=evidence,
    )

    if evidence:
        candidate.points += 100
        candidate.reasons.append("DIRECT CANCELLATION OR DISSATISFACTION LANGUAGE IN LINKED COMMUNICATION")

    if days >= 30:
        candidate.points += 35
        candidate.reasons.append(f"PIKE13 LAST VISIT WAS {days} DAYS AGO")
    elif days >= 21:
        candidate.points += 45
        candidate.reasons.append(f"PIKE13 LAST VISIT WAS {days} DAYS AGO")
    elif days >= 15:
        candidate.points += 25
        candidate.reasons.append(f"PIKE13 LAST VISIT WAS {days} DAYS AGO")

    baseline = attendance.baseline_28
    recent = attendance.recent_28
    if baseline >= 4 and recent <= baseline * 0.5:
        candidate.points += 40
        candidate.reasons.append(
            f"SCHEDULED RECURRING LESSONS FELL FROM {baseline:.1f} TO {recent} PER 28 DAYS"
        )
    elif baseline >= 3 and recent <= 1:
        candidate.points += 30
        candidate.reasons.append(
            f"SCHEDULED RECURRING LESSONS FELL FROM {baseline:.1f} TO {recent} PER 28 DAYS"
        )
    elif baseline >= 2 and recent == 0:
        candidate.points += 25
        candidate.reasons.append(
            f"NO SCHEDULED RECURRING LESSONS IN 28 DAYS VERSUS {baseline:.1f} BASELINE"
        )

    if attendance.concern_note and candidate.points >= 25:
        candidate.points += 10
        candidate.reasons.append("RECENT INSTRUCTOR NOTE CONTAINS A DISENGAGEMENT CONCERN")

    recovered_with_schedule = bool(recent_hold) and candidate.future_visits > 0
    if recovered_with_schedule and not evidence:
        return None

    recovered_without_schedule = (
        bool(recent_hold)
        and not recent_hold.get("plan_ended", False)
        and candidate.future_visits == 0
    )
    if recovered_without_schedule:
        assert recent_hold is not None
        hold_end = recent_hold["end_date"]
        days_since_hold = recent_hold["days_since_end"]
        candidate.points += 60
        candidate.reasons.insert(
            0,
            f"HOLD ENDED {hold_end.isoformat()} ({days_since_hold} DAYS AGO); PIKE13 SHOWS NO FUTURE VISITS",
        )

    if evidence:
        candidate.tier = "ALERT"
    elif recovered_without_schedule or days >= 30 or attendance.matched_name is None:
        candidate.tier = "VERIFY"
    else:
        candidate.tier = "MONITOR"

    if candidate.points < 25:
        return None

    instructor = ascii_text(attendance.primary_instructor)
    if candidate.tier == "ALERT":
        candidate.action = (
            f"Call {candidate.account_manager} at {candidate.contact} today. A linked message "
            "contains cancellation or dissatisfaction language. Ask what would help "
            f"{candidate.name} stay enrolled and offer a specific schedule, instructor, or "
            "program fix."
        )
    elif recovered_without_schedule:
        assert recent_hold is not None
        hold_end = recent_hold["end_date"]
        candidate.action = (
            f"Restore {candidate.name}'s post-hold schedule today. The hold ended "
            f"{hold_end.isoformat()}, the membership is active, and Pike13 shows no future "
            f"visits. Confirm the restart with {instructor}, then contact "
            f"{candidate.account_manager} at {candidate.contact}."
        )
    elif candidate.tier == "VERIFY":
        candidate.action = (
            f"Check Pike13 today before contacting the family. Verify {candidate.name}'s hold, "
            f"billing, and schedule status; the account is active but the last visit was "
            f"{days} days ago. If the gap is unexplained, ask {instructor} for context, then "
            f"call {candidate.account_manager} at {candidate.contact}."
        )
    else:
        candidate.action = (
            f"Ask {instructor} whether {candidate.name}'s recurring lesson frequency change "
            f"({attendance.baseline_28:.1f} to {attendance.recent_28} per 28 days) is vacation, "
            f"scheduling, or disengagement. If it is unexplained, contact "
            f"{candidate.account_manager} at {candidate.contact} to remove the barrier."
        )
    return candidate


def build_candidates(
    conn: sqlite3.Connection,
    roster_rows: list[sqlite3.Row],
    attendance: dict[tuple[str, str], Attendance],
    direct_evidence: dict[str, list[str]],
    recent_holds: dict[tuple[str, str], dict[str, Any]],
) -> tuple[list[Candidate], AuditCounts]:
    audit = AuditCounts(raw_members=len(roster_rows))
    candidates: list[Candidate] = []
    for row in roster_rows:
        state = (row["person_state"] or "").casefold()
        types = (row["current_plan_types"] or "").casefold()
        att = attendance[(row["school_slug"], str(row["person_id"]))]
        if state not in ("active", ""):
            audit.excluded_non_active += 1
            continue
        if "recurring" not in types or as_int(row["has_plan_on_hold"]) == 1:
            audit.excluded_non_recurring += 1
            continue
        if (
            as_int(row["completed_visits"]) < 4
            or att.lifetime < 4
            or att.tenure_days < 30
        ):
            audit.excluded_new_or_low_history += 1
            continue
        audit.eligible_members += 1
        if not att.matched_name:
            audit.unmatched_attendance += 1
        candidate = score_candidate(
            row,
            att,
            direct_evidence.get(str(row["person_id"]), []),
            recent_holds.get((row["school_slug"], norm_name(row["full_name"]))),
        )
        if candidate:
            candidates.append(candidate)
    tier_order = {"ALERT": 0, "VERIFY": 1, "MONITOR": 2}
    candidates.sort(key=lambda c: (tier_order[c.tier], -c.points, -c.days_since_last_visit, c.name))
    return candidates[:MAX_ACTIONS], audit


def load_hold_returns(
    as_of: date,
) -> tuple[
    list[dict[str, Any]],
    dict[str, int],
    dict[tuple[str, str], dict[str, Any]],
    list[str],
]:
    upcoming: list[dict[str, Any]] = []
    active_counts: dict[str, int] = {}
    recent_holds: dict[tuple[str, str], dict[str, Any]] = {}
    warnings: list[str] = []
    for slug, school in (("westu-sor", "West U"), ("theheights-sor", "The Heights")):
        path = MODELS_DIR / f"pike13_holds_{slug}.json"
        if not path.exists():
            raise RuntimeError(f"Missing hold snapshot for {school}; refresh before reporting")
        rows = json.loads(path.read_text())
        snapshot_dates = [
            parsed
            for parsed in (parse_iso_date(row.get("scraped_at")) for row in rows)
            if parsed is not None
        ]
        if not snapshot_dates:
            raise RuntimeError(
                f"Hold snapshot for {school} has no source scraped_at timestamp; refresh before reporting"
            )
        snapshot_date = max(snapshot_dates)
        age = (as_of - snapshot_date).days
        if age < 0 or age > MAX_ROSTER_AGE_DAYS:
            raise RuntimeError(f"Hold snapshot for {school} is {age} days old; refresh before reporting")
        unique_clients = {
            norm_name(r.get("client", ""))
            for r in rows
            if r.get("client") and r.get("on_hold")
        }
        active_counts[school] = len(unique_clients)
        seen: set[tuple[str, date]] = set()
        for row in rows:
            end = parse_hold_date(row.get("hold_end"))
            name = ascii_text(row.get("client", "")).strip()
            if (
                end
                and name
                and not row.get("on_hold")
                and 0 <= (as_of - end).days <= 30
            ):
                key = (slug, norm_name(name))
                previous = recent_holds.get(key)
                if previous is None or end > previous["end_date"]:
                    recent_holds[key] = {
                        "school": school,
                        "name": name,
                        "end_date": end,
                        "days_since_end": (as_of - end).days,
                        "plan": ascii_text(row.get("plan", "")),
                        "plan_ended": bool(row.get("ended") or row.get("canceled")),
                    }
            if not row.get("on_hold"):
                continue
            if not end or not name or not (1 <= (end - as_of).days <= 14):
                continue
            key = (norm_name(name), end)
            if key in seen:
                continue
            seen.add(key)
            upcoming.append(
                {
                    "school": school,
                    "name": name,
                    "end_date": end,
                    "days_until": (end - as_of).days,
                    "plan": ascii_text(row.get("plan", "")),
                    "account_manager": ascii_text(row.get("account_managers", "")),
                    "contact": ascii_text(
                        row.get("account_phones") or row.get("account_emails") or ""
                    ),
                }
            )
    upcoming.sort(key=lambda r: (r["end_date"], r["school"], r["name"]))
    return upcoming, active_counts, recent_holds, warnings


def sentence_case_evidence(value: str) -> str:
    """Turn internal all-caps evidence into readable report prose."""
    text = value.strip().rstrip(".")
    if text.isupper():
        text = text.lower().capitalize()
    return text.replace("Pike13", "Pike13").replace("pike13", "Pike13")


def format_candidate(candidate: Candidate) -> list[str]:
    att = candidate.attendance
    lines = [
        f"{candidate.name} - {candidate.school_name}",
        f"Program: {candidate.plans}",
        "Why this surfaced:",
    ]
    for reason in candidate.reasons[:3]:
        lines.append(f"  - {sentence_case_evidence(reason)}")
    lines.extend(
        [
            f"Instructor: {ascii_text(att.primary_instructor)}",
            (
                "Scheduled recurring lessons (current / prior / earlier 28-day windows): "
                f"{att.recent_28} / {att.prior_28} / {att.earlier_28}"
            ),
            "Next step:",
            f"  {candidate.action}",
        ]
    )
    return lines


def format_report(
    as_of: date,
    candidates: list[Candidate],
    audit: AuditCounts,
    roster_stamps: dict[str, str],
    attendance_through: date,
    hold_returns: list[dict[str, Any]],
    active_holds: dict[str, int],
    warnings: list[str],
    conn: sqlite3.Connection,
) -> str:
    call_max = conn.execute("SELECT MAX(event_at) FROM dialpad_call_reviews").fetchone()[0]
    email_max = conn.execute("SELECT MAX(message_at) FROM school_email_messages").fetchone()[0]
    call_date = parse_iso_date(call_max)
    email_date = parse_iso_date(email_max)
    alert_count = sum(candidate.tier == "ALERT" for candidate in candidates)
    verify_count = sum(candidate.tier == "VERIFY" for candidate in candidates)
    monitor_count = sum(candidate.tier == "MONITOR" for candidate in candidates)
    lines = [
        f"Weekly retention worklist - {as_of.strftime('%B %d, %Y')}",
        "",
        "A short list of students who may need a schedule fix, an account check, or instructor context.",
        "Appearing here does not mean a family is expected to cancel.",
        "",
        "At a glance",
        "-----------",
        f"- {len(candidates)} students need review this week",
        f"- {alert_count} direct follow-ups",
        f"- {verify_count} Pike13 checks before family contact",
        f"- {monitor_count} instructor checks",
        "",
    ]
    stale_comms = []
    for source, source_date in (("CALLS", call_date), ("EMAIL", email_date)):
        if source_date is None:
            stale_comms.append(f"{source.lower()} missing")
        elif (as_of - source_date).days > 7:
            stale_comms.append(f"{source.lower()} through {source_date.isoformat()}")
    if stale_comms:
        lines.append(
            "Data note: direct communication coverage may be incomplete; "
            + ", ".join(stale_comms)
            + "."
        )
    if audit.unmatched_attendance:
        lines.append(
            f"Data note: {audit.unmatched_attendance} eligible member(s) lack a safe attendance name match."
        )
    for warning in warnings:
        lines.append(f"Data note: {sentence_case_evidence(warning)}.")
    if stale_comms or audit.unmatched_attendance or warnings:
        lines.append("")

    for tier, title in (
        ("ALERT", "Contact today"),
        ("VERIFY", "Check Pike13 before contacting"),
        ("MONITOR", "Ask the instructor first"),
    ):
        group = [c for c in candidates if c.tier == tier]
        if not group:
            continue
        lines.extend([f"{title} ({len(group)})", "-" * (len(title) + len(str(len(group))) + 3), ""])
        for candidate in group:
            lines.extend(format_candidate(candidate))
            lines.append("")

    if not candidates:
        lines.extend([
            "No retention follow-up needed this week",
            "---------------------------------------",
            "No current member met the evidence threshold. There is no outreach quota to fill.",
            "",
        ])

    if hold_returns:
        grouped: dict[tuple[str, date], list[dict[str, Any]]] = defaultdict(list)
        for row in hold_returns:
            grouped[(row["school"], row["end_date"])].append(row)
        lines.extend(["Upcoming hold returns", "---------------------"])
        for (school, end), rows in sorted(grouped.items(), key=lambda item: item[0][1]):
            due = max(as_of, end - timedelta(days=7))
            lines.append(
                f"- {school}: {len(rows)} {'student' if len(rows) == 1 else 'students'} "
                f"return {end.strftime('%B %d')}; confirm schedules by {due.strftime('%B %d')}."
            )
        lines.append(
            "Use the companion worklist and contact only families without a confirmed post-hold schedule."
        )
        lines.append("")

    lines.extend([
        "About this worklist",
        "-------------------",
        (
            f"Reviewed {audit.eligible_members} established recurring members from a current "
            f"roster of {audit.raw_members}. The list is capped at {MAX_ACTIONS} actions per week."
        ),
        (
            "It uses current membership status, last visit, recent hold status, and changes in "
            "scheduled recurring lessons. Direct cancellation or dissatisfaction language is "
            "treated as a prompt follow-up."
        ),
        (
            "Active holds, non-recurring plans, and members without enough history are left out. "
            "Missing calls, emails, or notes never count as evidence of low risk."
        ),
        f"Attendance data through {attendance_through.isoformat()}; calls through {str(call_max)[:10]}; email through {str(email_max)[:10]}.",
        f"Active holds: West U {active_holds.get('West U', 0)}; The Heights {active_holds.get('The Heights', 0)}.",
        "",
    ])
    return "\n".join(lines)


def write_hold_worklist(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["school", "name", "end_date", "days_until", "plan", "account_manager", "contact"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, "end_date": row["end_date"].isoformat()})


def generate(
    db_path: Path = DB_PATH,
    as_of: date | None = None,
) -> tuple[str, list[Candidate], list[dict[str, Any]]]:
    as_of = as_of or date.today()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        roster_rows, roster_stamps = latest_roster_rows(conn, as_of)
        attendance, attendance_through = build_attendance(conn, roster_rows, as_of)
        direct = load_direct_evidence(conn, roster_rows, as_of)
        hold_returns, active_holds, recent_holds, warnings = load_hold_returns(as_of)
        candidates, audit = build_candidates(
            conn, roster_rows, attendance, direct, recent_holds
        )
        report = format_report(
            as_of, candidates, audit, roster_stamps, attendance_through,
            hold_returns, active_holds, warnings, conn,
        )
        return report, candidates, hold_returns
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--as-of", type=date.fromisoformat, default=None)
    parser.add_argument("--output", type=Path, default=MODELS_DIR / "actionable_churn_report.txt")
    parser.add_argument("--hold-worklist", type=Path, default=MODELS_DIR / "hold_return_worklist.csv")
    args = parser.parse_args()
    report, candidates, holds = generate(args.db, args.as_of)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report)
    write_hold_worklist(args.hold_worklist, holds)
    print(report)
    print(f"WROTE: {args.output}")
    print(f"WROTE: {args.hold_worklist}")
    print(f"ACTIONABLE MEMBERS: {len(candidates)}")


if __name__ == "__main__":
    main()
