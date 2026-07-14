#!/usr/bin/env python3
"""
SOR Churn Heuristic Report — Action-Focused for GMs.

Design: Don't flood with 300 names. Surface the 30-40 students who need
a concrete action TODAY. Each student gets ONE recommended action.
"""

import os, sqlite3, re
from datetime import date, timedelta
import pandas as pd
import numpy as np

DB = "reminders.db"
MDIR = "models"

SCHOOLS = {1: "West University Place", 2: "The Heights"}


def load_data():
    c = sqlite3.connect(DB)
    lessons = pd.read_sql_query("""
        SELECT lesson_id, school_id, instructor_id, lesson_date, lesson_time,
               lesson_type, students_raw
        FROM lessons WHERE students_raw IS NOT NULL AND students_raw != ''
    """, c)
    notes = pd.read_sql_query("""
        SELECT lesson_id, note_completed, note_score FROM lesson_notes
    """, c)
    c.close()
    lessons["lesson_date"] = pd.to_datetime(lessons["lesson_date"])
    rows = []
    for _, r in lessons.iterrows():
        names = [n.strip() for n in re.split(r',\s*', str(r["students_raw"])) if n.strip()]
        for name in names:
            rows.append(dict(
                lesson_id=r["lesson_id"], school_id=r["school_id"],
                instructor_id=r["instructor_id"],
                lesson_date=r["lesson_date"], lesson_time=r["lesson_time"],
                lesson_type=r["lesson_type"],
                student_name=name,
            ))
    return pd.DataFrame(rows), notes


def compute_features(lessons, notes, today):
    today_ts = pd.Timestamp(today)
    ws = today_ts - timedelta(days=60)
    we = today_ts

    note_agg = notes.groupby("lesson_id").agg(
        note_score=("note_score", "max"),
        note_completed=("note_completed", "max"),
    ).reset_index()

    rows = []
    for name, g in lessons.groupby("student_name"):
        if len(g) < 5:
            continue
        g = g.sort_values("lesson_date")
        dt = g["lesson_date"]
        last = dt.max()
        first = dt.min()
        days_idle = (today_ts - last).days
        if days_idle > 60:  # not currently active
            continue

        win = g[(dt >= ws) & (dt <= we)]
        n_win = len(win)
        if n_win == 0:
            continue

        # Attendance: recent 14d vs prior 45d
        recent = win[win["lesson_date"] >= (we - timedelta(days=14))]
        prior = win[win["lesson_date"] < (we - timedelta(days=14))]
        n_recent = len(recent)
        n_prior = len(prior)
        ratio = n_recent / max(n_prior, 1) if n_prior > 0 else (n_recent if n_recent > 0 else 0.01)

        # Teacher changes in last 45 days
        recent45 = win[win["lesson_date"] >= (we - timedelta(days=45))]
        instructors = recent45["instructor_id"].dropna()
        n_teacher_changes = instructors.nunique()

        # Notes in last 3 lessons
        last3 = g.tail(3)
        last3_ids = set(last3["lesson_id"].values)
        last3_notes = notes[notes["lesson_id"].isin(last3_ids)]
        notes_missing = 3 - len(last3_notes)

        # Note quality
        win_notes = win.merge(note_agg, on="lesson_id", how="left")
        ns = win_notes["note_score"].dropna()
        avg_note_score = ns.mean() if len(ns) > 0 else 0.0

        school = int(g["school_id"].mode().iloc[0]) if not g["school_id"].empty else 0
        total_lessons = len(g)
        tenure_days = (today_ts - first).days if pd.notna(first) else 0

        rows.append(dict(
            student_name=name, school_id=school,
            days_idle=days_idle, attendance_ratio=ratio,
            n_teacher_changes=n_teacher_changes,
            notes_missing_last3=notes_missing,
            avg_note_score=avg_note_score,
            total_lessons=total_lessons,
            tenure_days=tenure_days,
        ))
    return pd.DataFrame(rows)


def classify_students(df):
    """One recommended action per student, priority-ordered."""
    alerts = []  # (student_name, school, priority, reason, action)

    for _, r in df.iterrows():
        prio = 0
        reason = ""
        action = ""

        # 🔴 CRITICAL — student may have already quit (>21 days, not just summer)
        if r["days_idle"] >= 28:
            prio = 3
            reason = f"Gone {int(r['days_idle'])} days — may have quietly quit"
            action = "📞 CALL parent. If no answer, try text and email within 24h."

        # 🔴 CRITICAL — attendance collapsed AND idle > 10d (not just this week)
        elif r["attendance_ratio"] < 0.2 and r["days_idle"] >= 10:
            prio = 3
            reason = "Attendance has collapsed — barely showing up for weeks"
            action = "📞 CALL parent. Ask what changed — schedule? Interest? Teacher?"

        # 🟡 HIGH — fading (moderate decline with absence)
        elif r["attendance_ratio"] < 0.3 and r["days_idle"] >= 14:
            prio = 2
            reason = f"Fading — {int(r['days_idle'])}d idle with declining attendance"
            action = "📱 TEXT parent to check in. Offer schedule adjustment."

        # 🟡 HIGH — low note quality (instructor disengaged)
        elif r["avg_note_score"] > 0 and r["avg_note_score"] < 2.5:
            prio = 2
            reason = f"Note quality {r['avg_note_score']:.1f}/5 — instructor may be disengaged"
            action = "👥 Check in with instructor. Is student struggling?"

        # 🟡 HIGH — severe teacher instability (5+ different teachers, not just summer rotation)
        elif r["n_teacher_changes"] >= 5:
            prio = 2
            reason = f"{int(r['n_teacher_changes'])} different teachers in 45 days — no consistency at all"
            action = "📋 Assign a dedicated instructor. Notify parent."

        # 🟡 HIGH — missing notes (3 of last 3 lessons)
        elif r["notes_missing_last3"] >= 3:
            prio = 2
            reason = "No notes on any of the last 3 lessons — admin neglect"
            action = "📝 Complete lesson notes. Parents need feedback to stay engaged."

        # 🟢 LOW — moderate teacher churn (3-4 teachers, worth watching)
        elif r["n_teacher_changes"] >= 3:
            prio = 1
            reason = f"{int(r['n_teacher_changes'])} teachers recently — worth watching"
            action = "👀 Monitor. If it hits 5 different teachers, escalate."

        # 🟢 LOW — idle but attendance pattern OK
        elif r["days_idle"] >= 21:
            prio = 1
            reason = f"{int(r['days_idle'])}d idle but attendance pattern OK"
            action = "👀 Monitor. If idle hits 28d, escalate to critical."

        if prio == 0:
            continue  # no flag — skip

        alerts.append((
            r["student_name"], r["school_id"], prio,
            int(r["days_idle"]), r["attendance_ratio"],
            reason, action
        ))

    return sorted(alerts, key=lambda x: (-x[2], x[3]))  # priority desc, then idle desc


def print_report(alerts, school_id, school_name):
    school_alerts = [a for a in alerts if a[1] == school_id]
    if not school_alerts:
        return

    critical = [a for a in school_alerts if a[2] == 3]
    high = [a for a in school_alerts if a[2] == 2]
    low = [a for a in school_alerts if a[2] == 1]

    print(f"\n{'='*75}")
    print(f"  {school_name}")
    print(f"  🔴 Critical: {len(critical)}   🟡 Action Needed: {len(high)}   🟢 Monitor: {len(low)}")
    print(f"{'='*75}")

    if critical:
        print(f"\n  ── 🔴 CRITICAL — Act Today ({len(critical)} students) ──")
        for name, _, _, idle, ratio, reason, action in critical[:10]:
            print(f"\n  ▸ {name}  ({idle}d idle, ratio {ratio:.2f})")
            print(f"    {reason}")
            print(f"    → {action}")
        if len(critical) > 10:
            print(f"\n  ... and {len(critical)-10} more (see models/churn_watch.csv for full list)")

    if high:
        print(f"\n  ── 🟡 ACTION NEEDED — This Week ({len(high)} students) ──")
        for name, _, _, idle, ratio, reason, action in high[:10]:
            print(f"\n  ▸ {name}  ({idle}d idle, ratio {ratio:.2f})")
            print(f"    {reason}")
            print(f"    → {action}")
        if len(high) > 10:
            print(f"\n  ... and {len(high)-10} more (see models/churn_watch.csv for full list)")

    if low:
        print(f"\n  ── 🟢 MONITOR — Watch List ({len(low)} students) ──")
        names_only = [a[0] for a in low[:12]]
        print(f"  {', '.join(names_only)}")
        if len(low) > 12:
            print(f"  ... and {len(low)-12} more (see models/churn_watch.csv for full list)")


def main():
    today = date.today()
    print(f"╔═════════════════════════════════════════════════════╗")
    print(f"║  SOR Churn Watch — {today}                ║")
    print(f"║  Action-focused. One task per student.            ║")
    print(f"╚═════════════════════════════════════════════════════╝")

    lessons, notes = load_data()
    df = compute_features(lessons, notes, today)

    alerts = classify_students(df)

    critical = sum(1 for a in alerts if a[2] == 3)
    high = sum(1 for a in alerts if a[2] == 2)
    low = sum(1 for a in alerts if a[2] == 1)
    total = len(alerts)
    clean = len(df) - total

    print(f"\n  📊 SUMMARY: {len(df)} active students")
    print(f"  🔴 Critical: {critical}  |  🟡 Action: {high}  |  🟢 Monitor: {low}")
    print(f"  ✅ No action needed: {clean}")

    for sid, sname in SCHOOLS.items():
        print_report(alerts, sid, sname)

    # Save CSV
    out_rows = []
    for name, sid, prio, idle, ratio, reason, action in alerts:
        out_rows.append(dict(
            student_name=name, school=SCHOOLS.get(sid, str(sid)),
            priority=prio, days_idle=idle, attendance_ratio=ratio,
            reason=reason, action=action,
        ))
    pd.DataFrame(out_rows).to_csv(f"{MDIR}/churn_watch.csv", index=False)
    print(f"\n  💾 Saved → {MDIR}/churn_watch.csv")


if __name__ == "__main__":
    main()
