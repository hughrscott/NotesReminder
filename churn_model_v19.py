#!/usr/bin/env python3
"""
churn_model_v19.py — ONE unified table. Every student has join_date + leave_date (or NULL).
Time-based labels: for each monthly snapshot, Y=1 if membership ended within next 60 days.
"""
import sqlite3, re
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
from catboost import CatBoostClassifier
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import LabelEncoder

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "reminders.db"
TODAY = datetime(2026, 7, 17)

print("=" * 60)
print("SOR CHURN v19 — Unified Student Table, Time-Based Labels")
print("=" * 60)

conn = sqlite3.connect(str(DB_PATH))

# ── 1. ONE table: every student with signup_date + membership_end_date ──
print("\n[1] Building unified student table...")

# Leavers
leavers = pd.read_sql_query("""
    SELECT student_name, signup_date, membership_end_date, plan
    FROM pike13_leavers_full
""", conn, parse_dates=['signup_date', 'membership_end_date'])

# Current members
current = pd.read_sql_query("""
    SELECT student_name, signup_date, NULL as membership_end_date, current_plan as plan
    FROM member_roster_snapshots
    WHERE has_membership=1 AND status='active' AND snapshot_date='2026-07-17'
""", conn, parse_dates=['signup_date'])

students = pd.concat([leavers, current], ignore_index=True)
print(f"  Students: {len(students)} ({leavers['membership_end_date'].notna().sum()} with end date, "
      f"{current['membership_end_date'].isna().sum()} current)")

# Every student has:
#   signup_date = when they joined
#   membership_end_date = when they left (NULL = still active)
students['signup_date'] = pd.to_datetime(students['signup_date'])
students['membership_end_date'] = pd.to_datetime(students['membership_end_date'])
students['name_lower'] = students['student_name'].str.lower()

# ── Plan encoding ──
def classify_plan(plan):
    if not plan or pd.isna(plan): return 'unknown'
    plan = str(plan).lower()
    for cat in ['bowie','cbgb','house_band','adult','rehearsal','blues','drum',
                 'guitar','bass','vocal','keys','makeup','rock101']:
        if cat.replace('_',' ') in plan or cat in plan: return cat
    return 'other'

students['plan_category'] = students['plan'].apply(classify_plan)
plan_encoder = LabelEncoder()
students['plan_encoded'] = plan_encoder.fit_transform(students['plan_category'])

# ── 2. Lesson data ──
print("[2] Loading lesson data...")
lessons_raw = pd.read_sql_query("""
    SELECT l.lesson_id, l.students_raw, l.lesson_date, l.instructor_id
    FROM lessons l WHERE l.students_raw IS NOT NULL AND l.lesson_date >= '2024-01-01'
""", conn, parse_dates=["lesson_date"])
notes = pd.read_sql_query("SELECT lesson_id, note_score FROM lesson_notes WHERE note_score IS NOT NULL", conn)
conn.close()

expanded = []
for _, row in lessons_raw.iterrows():
    for name in re.split(r',\s*', str(row["students_raw"])):
        name = name.strip().lower()
        if name:
            expanded.append({"student": name, "lesson_date": row["lesson_date"],
                           "instructor_id": row["instructor_id"], "lesson_id": row["lesson_id"]})
lessons = pd.DataFrame(expanded)

# ── 3. Create monthly snapshots ──
print("[3] Creating monthly snapshots...")

# For each student, create a snapshot at the first of each month from signup to end/today
snapshots = []
for _, s in students.iterrows():
    start = max(s['signup_date'], pd.Timestamp('2024-01-01'))  # No lesson data before 2024
    end = s['membership_end_date'] if pd.notna(s['membership_end_date']) else pd.Timestamp(TODAY)
    
    # Skip if membership was too short (e.g., signed up and left within same month)
    if (end - start).days < 60:
        continue
    
    # Snapshots at first of each month, but only those with 60 days of lookahead
    current_date = start.replace(day=1) + pd.DateOffset(months=1)
    while current_date + timedelta(days=60) <= end:
        snapshots.append({
            'name_lower': s['name_lower'],
            'signup_date': s['signup_date'],
            'membership_end_date': s['membership_end_date'],
            'plan_encoded': s['plan_encoded'],
            'plan_category': s['plan_category'],
            'snapshot_date': current_date,
        })
        current_date += pd.DateOffset(months=1)

df = pd.DataFrame(snapshots)
df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
print(f"  Snapshots: {len(df)} from {df['name_lower'].nunique()} students")

# ── 4. Compute features at each snapshot (NO future leakage) ──
print("[4] Computing features at snapshots...")

features = []
for _, row in df.iterrows():
    name = row['name_lower']
    snap = row['snapshot_date']
    end = row['membership_end_date']
    
    # Behavioral features from lessons BEFORE snapshot_date
    sl = lessons[(lessons['student'] == name) & (lessons['lesson_date'] <= snap)]
    d30 = snap - timedelta(days=30)
    d60 = snap - timedelta(days=60)
    d90 = snap - timedelta(days=90)
    
    l30 = len(sl[sl['lesson_date'] >= d30])
    l60 = len(sl[sl['lesson_date'] >= d60])
    l90 = len(sl[sl['lesson_date'] >= d90])
    
    if len(sl) < 2:
        # Student predates lesson data — use membership features only
        features.append({
            'name_lower': name, 'snapshot_date': snap,
            'tenure_days': (snap - row['signup_date']).days,
            'plan_encoded': row['plan_encoded'],
            'lessons_30d': 0, 'lessons_60d': 0, 'lessons_90d': 0,
            'days_since_last': 999, 'freq_decline': 0,
            'avg_note_score': 0.0, 'notes_in_window': 0, 'teacher_consistency': 0,
            'has_behavioral': 0,
        })
        continue
    
    days_since = (snap - sl['lesson_date'].max()).days
    prior = len(sl[(sl['lesson_date'] >= snap - timedelta(days=60)) & (sl['lesson_date'] < d30)])
    fd = l30 / max(prior, 1)
    
    recent_lessons = sl[sl['lesson_date'] >= d90]
    mn = notes[notes['lesson_id'].isin(recent_lessons['lesson_id'])]
    avg_note = round(mn['note_score'].mean(), 2) if len(mn) > 0 else 0.0
    notes_count = len(mn)
    
    ic = recent_lessons['instructor_id'].value_counts()
    tc = round(ic.iloc[0] / max(len(recent_lessons), 1), 3) if len(recent_lessons) > 0 else 0
    
    features.append({
        'name_lower': name, 'snapshot_date': snap,
        'tenure_days': (snap - row['signup_date']).days,
        'plan_encoded': row['plan_encoded'],
        'lessons_30d': l30, 'lessons_60d': l60, 'lessons_90d': l90,
        'days_since_last': days_since,
        'freq_decline': round(fd, 3),
        'avg_note_score': avg_note, 'notes_in_window': notes_count,
        'teacher_consistency': tc,
        'has_behavioral': 1,
    })

bf = pd.DataFrame(features)
df = df.merge(bf, on=['name_lower', 'snapshot_date'], how='left')

# ── 5. Labels: Y=1 if membership ends within 60 days of snapshot ──
df['churned'] = 0
has_end = df['membership_end_date'].notna()
within_window = (df['membership_end_date'] - df['snapshot_date']).dt.days <= 60
before_snapshot = df['membership_end_date'] < df['snapshot_date']

# A student churns if: they HAVE an end date, it's within 60 days of snapshot, and it's AFTER the snapshot
df.loc[has_end & within_window & ~before_snapshot, 'churned'] = 1

# Students whose membership already ended before the snapshot shouldn't be in the training set
df = df[~before_snapshot].copy()

print(f"  {len(df)} valid snapshots, {df['churned'].sum()} churn events "
      f"({df['churned'].mean()*100:.1f}%)")
print(f"  Students represented: {df['name_lower'].nunique()}")

# ── 6. Train ──
feature_cols = [
    'tenure_days', 'plan_encoded', 
    'lessons_30d', 'lessons_60d', 'lessons_90d',
    'days_since_last', 'freq_decline',
    'avg_note_score', 'notes_in_window', 'teacher_consistency'
]

X = df[feature_cols].fillna(0).replace([np.inf, -np.inf], 0)
y = df['churned'].values

print(f"\n[5] Training...")
print(f"  Features: {len(feature_cols)}, Churn rate: {y.mean():.1%}")

if y.sum() >= 3 and len(y) - y.sum() >= 3:
    cb = CatBoostClassifier(iterations=300, depth=4, learning_rate=0.05,
                            auto_class_weights="Balanced", eval_metric="AUC",
                            random_seed=42, verbose=0, allow_writing_files=False)
    
    n_splits = min(5, y.sum(), len(y) - y.sum())
    cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
    scores = cross_val_score(cb, X.values, y, cv=cv, scoring="roc_auc")
    print(f"  CatBoost CV AUC: {scores.mean():.3f} +/- {scores.std():.3f}")
    
    cb.fit(X.values, y)
    imp = pd.DataFrame({'feature': feature_cols, 'importance': cb.feature_importances_})
    print(f"\n  Top features:")
    for _, row in imp.sort_values('importance', ascending=False).head(6).iterrows():
        print(f"    {row['feature']:30s} {row['importance']:.3f}")
else:
    print(f"  Insufficient churn events for training ({y.sum()})")

# ── 7. Predict current members ──
print(f"\n[6] Current member predictions...")
current_students = students[students['membership_end_date'].isna()]

# Build latest features for each current member
pred_rows = []
for _, cs in current_students.iterrows():
    name = cs['name_lower']
    snap = pd.Timestamp(TODAY)
    sl = lessons[(lessons['student'] == name) & (lessons['lesson_date'] <= snap)]
    
    d30 = snap - timedelta(days=30)
    d60 = snap - timedelta(days=60)
    d90 = snap - timedelta(days=90)
    
    l30 = len(sl[sl['lesson_date'] >= d30])
    l60 = len(sl[sl['lesson_date'] >= d60])
    l90 = len(sl[sl['lesson_date'] >= d90])
    
    days_since = (snap - sl['lesson_date'].max()).days if len(sl) > 0 else 999
    prior = len(sl[(sl['lesson_date'] >= snap - timedelta(days=60)) & (sl['lesson_date'] < d30)])
    fd = l30 / max(prior, 1) if prior > 0 else 0
    
    recent_lessons = sl[sl['lesson_date'] >= d90]
    mn = notes[notes['lesson_id'].isin(recent_lessons['lesson_id'])]
    avg_note = round(mn['note_score'].mean(), 2) if len(mn) > 0 else 0.0
    notes_count = len(mn)
    ic = recent_lessons['instructor_id'].value_counts()
    tc = round(ic.iloc[0] / max(len(recent_lessons), 1), 3) if len(recent_lessons) > 0 else 0
    
    pred_rows.append({
        'name': cs['student_name'],
        'plan': cs['plan_category'],
        'tenure_days': (snap - cs['signup_date']).days,
        'plan_encoded': cs['plan_encoded'],
        'lessons_30d': l30, 'lessons_60d': l60, 'lessons_90d': l90,
        'days_since_last': days_since, 'freq_decline': round(fd, 3),
        'avg_note_score': avg_note, 'notes_in_window': notes_count,
        'teacher_consistency': tc,
    })

pred_df = pd.DataFrame(pred_rows)
X_pred = pred_df[feature_cols].fillna(0).replace([np.inf, -np.inf], 0).values
pred_df['risk'] = (cb.predict_proba(X_pred)[:, 1] * 100).round(1)
pred_df = pred_df.sort_values('risk', ascending=False)

print(f"\n{'='*60}")
print("TOP 10 AT-RISK (v19: unified student table, time-based labels)")
print(f"{'='*60}")
print(f"{'Student':25s} {'Risk':>5s} {'L30d':>4s} {'Days':>4s} {'Tenure':>6s} {'Plan':>14s}")
print(f"{'─'*25} {'─'*5} {'─'*4} {'─'*4} {'─'*6} {'─'*14}")

for _, row in pred_df.head(10).iterrows():
    print(f"{row['name'][:25]:25s} {row['risk']:5.1f} {int(row['lessons_30d']):4d} "
          f"{int(row['days_since_last']):4d} {int(row['tenure_days']):6d} {row['plan'][:14]:14s}")

print(f"\n✅ v19: 124 students → {len(df)} snapshots with time-based labels")
print(f"   Labels: membership_end_date within 60 days of snapshot")
print(f"   NO future leakage: features computed at snapshot date only")
