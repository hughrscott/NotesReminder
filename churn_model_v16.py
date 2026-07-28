#!/usr/bin/env python3
"""
churn_model_v16.py — Composite risk scoring using ACTUAL membership data.
No ML — just weighted scoring on confirmed members only.
Uses: late cancellations, attendance recency, lesson frequency, note scores, call sentiment.
"""
import sqlite3, json, re
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "reminders.db"

TODAY = datetime.now()
REF_DATE = pd.Timestamp(TODAY)

print("=" * 60)
print(f"SOR CHURN v16 — Membership-Based Risk Scoring")
print(f"Report: {TODAY.strftime('%B %d, %Y')}")
print("=" * 60)

conn = sqlite3.connect(str(DB_PATH))

# ── 1. Load membership roster ──
print("\n[1] Loading current members...")
members = pd.read_sql_query("""
    SELECT student_name, current_plan, signup_date, last_visit, next_lesson,
           last_lesson, total_lessons, lessons_last30d, phone, email
    FROM member_roster_snapshots
    WHERE snapshot_date = ?
""", conn, params=(TODAY.strftime('%Y-%m-%d'),))

print(f"  Members: {len(members)}")

# ── 2. Load late cancellations ──
print("[2] Loading late cancellations...")
late_cancels = pd.read_sql_query("""
    SELECT student_name, event_date, 
           json_extract(raw_json, '$[1]') as service_date
    FROM membership_events
    WHERE event_type = 'late_cancel'
""", conn)

# Count per student in last 30 days
late_cancels['event_date'] = pd.to_datetime(late_cancels['event_date'])
recent_cutoff = REF_DATE - timedelta(days=30)
recent_late = late_cancels[late_cancels['event_date'] >= recent_cutoff]
late_counts = recent_late.groupby('student_name').size().to_dict()

# ── 3. Load lesson data for attendance patterns ──
print("[3] Loading lesson data...")
lessons_raw = pd.read_sql_query("""
    SELECT l.lesson_id, l.students_raw, l.lesson_date, l.instructor_id
    FROM lessons l
    WHERE l.students_raw IS NOT NULL AND l.students_raw != ''
      AND l.lesson_date IS NOT NULL
    ORDER BY l.lesson_date
""", conn, parse_dates=["lesson_date"])

# Expand group lessons
expanded = []
for _, row in lessons_raw.iterrows():
    for name in re.split(r',\s*', str(row["students_raw"])):
        name = name.strip().lower()
        if name:
            expanded.append({"student": name, "lesson_date": row["lesson_date"],
                           "instructor_id": row["instructor_id"], "lesson_id": row["lesson_id"]})
lessons = pd.DataFrame(expanded)

# ── 4. Load notes ──
notes = pd.read_sql_query("SELECT lesson_id, note_score FROM lesson_notes WHERE note_score IS NOT NULL", conn)

# ── 5. Load call sentiment ──
calls = pd.read_sql_query("""
    SELECT pp.full_name as student_name, rt.sentiment, dve.event_at
    FROM identity_matches im
    JOIN pike13_people pp ON im.target_id = pp.person_id AND im.target_system='pike13'
    JOIN hubspot_contacts hc ON im.source_id = hc.contact_id AND im.source_system='hubspot'
    JOIN dialpad_voice_events dve ON hc.phone_normalized = dve.phone_normalized
    LEFT JOIN recording_transcripts rt ON dve.event_id = rt.call_id
    WHERE dve.event_at IS NOT NULL
""", conn, parse_dates=["event_at"])
conn.close()

# ── 6. Compute risk scores ──
print("\n[4] Computing risk scores...")

results = []

for _, member in members.iterrows():
    name = member['student_name']
    name_lower = name.lower()
    student_lessons = lessons[lessons['student'] == name_lower]
    
    # --- Days since last visit (weight: 25%) ---
    if len(student_lessons) > 0:
        last_lesson_date = student_lessons['lesson_date'].max()
        days_since = (REF_DATE - last_lesson_date).days
        days_score = 40 if days_since > 21 else (20 if days_since > 14 else 0)
        # Gemini: no next lesson scheduled = major red flag
        next_lesson = member.get('next_lesson')
        has_next = bool(next_lesson) and str(next_lesson) not in ('None','')
        if not has_next:
            days_score = min(days_score + 40, 100)
    else:
        days_since = 999
        days_score = 100
    
    # --- Late cancels (weight: 35%) — Gemini: use ratio, not raw count ---
    late_count = late_counts.get(name, 0)
    if len(student_lessons) > 0:
        d90_lessons = len(student_lessons[student_lessons['lesson_date'] >= REF_DATE - timedelta(days=90)])
        late_ratio = late_count / max(d90_lessons, 1)
        late_score = min(late_ratio * 400, 100)  # 25%→100, 10%→40, 5%→20
    else:
        late_score = 0
    
    # --- Lesson frequency decline (weight: 20%) ---
    if len(student_lessons) >= 8:
        d30 = student_lessons[student_lessons['lesson_date'] >= REF_DATE - timedelta(days=30)]
        d60 = student_lessons[(student_lessons['lesson_date'] >= REF_DATE - timedelta(days=60)) & 
                             (student_lessons['lesson_date'] < REF_DATE - timedelta(days=30))]
        recent_count = len(d30)
        prior_count = max(len(d60), 1)
        decline_ratio = recent_count / prior_count
        freq_score = max(0, min((1 - decline_ratio) * 100, 100))  # 50% drop → 50, 75% drop → 75
    else:
        freq_score = 50  # unknown, neutral
    
    # --- Note sentiment (weight: 10%) ---
    if len(student_lessons) > 0:
        recent_lessons = student_lessons[student_lessons['lesson_date'] >= REF_DATE - timedelta(days=90)]
        matching_notes = notes[notes['lesson_id'].isin(recent_lessons['lesson_id'])]
        if len(matching_notes) > 0:
            avg_note = matching_notes['note_score'].mean()
            note_score_val = max(0, min((10 - avg_note) * 20, 100))
        else:
            note_score_val = 50
    else:
        note_score_val = 50
    
    # --- Call sentiment (weight: 10%) ---
    student_calls = calls[calls['student_name'].str.lower() == name_lower]
    recent_calls = student_calls[student_calls['event_at'] >= REF_DATE - timedelta(days=90)]
    has_negative = any(recent_calls['sentiment'] == 'negative') if len(recent_calls) > 0 else False
    call_score = 80 if has_negative else (20 if len(student_calls) > 0 else 50)
    
    # --- Composite score ---
    composite = (
        late_score * 0.35 +
        days_score * 0.25 +
        freq_score * 0.20 +
        note_score_val * 0.10 +
        call_score * 0.10
    )
    
    results.append({
        'student': name,
        'plan': member.get('current_plan', ''),
        'composite': round(composite, 1),
        'late_count': late_count,
        'days_since': days_since,
        'freq_score': round(freq_score, 1),
        'note_score_val': round(note_score_val, 1),
        'has_negative_call': has_negative,
        'last_lesson': str(last_lesson_date.date()) if len(student_lessons) > 0 else 'never',
    })

df = pd.DataFrame(results).sort_values('composite', ascending=False)

# ── 7. Report ──
print(f"\n{'='*60}")
print("LIKELY LEAVERS — CALL THIS WEEK")
print(f"{'='*60}")
print(f"{'Student':25s} {'Risk':>4s} {'Late':>4s} {'Days':>4s} {'Freq':>5s} {'Note':>4s} {'Call':>5s}  Plan")
print(f"{'─'*25} {'─'*4} {'─'*4} {'─'*4} {'─'*5} {'─'*4} {'─'*5}  {'─'*30}")

for _, row in df.head(10).iterrows():
    call_flag = "⚠️" if row['has_negative_call'] else "✓"
    print(f"{row['student'][:25]:25s} {row['composite']:4.0f} {int(row['late_count']):4d} {int(row['days_since']):4d} {row['freq_score']:5.0f} {row['note_score_val']:4.0f} {call_flag:>5s}  {str(row['plan'])[:30]}")

print(f"\n{'='*60}")
print("SUMMARY")
print(f"{'='*60}")
print(f"Total members: {len(members)}")
print(f"Top 5 (highest risk):")
for i, (_, row) in enumerate(df.head(5).iterrows()):
    reasons = []
    if row['late_count'] >= 2:
        reasons.append(f"{int(row['late_count'])} late cancels")
    if row['days_since'] >= 14:
        reasons.append(f"last visit {int(row['days_since'])}d ago")
    if row['has_negative_call']:
        reasons.append("negative call sentiment")
    if row['note_score_val'] >= 60:
        reasons.append("low note scores")
    print(f"  {i+1}. {row['student']} (risk {row['composite']:.0f}) — {', '.join(reasons) if reasons else 'declining attendance'}")

print(f"\nLate cancel leaders (top 5):")
for name, count in sorted(late_counts.items(), key=lambda x: -x[1])[:5]:
    print(f"  {name}: {count}")

print(f"\n✅ v16 scoring complete. AUC tracking begins with next week's leaver report.")
print(f"   Expected: of the top 5 flagged, some should appear as leavers in future weeks.")
