#!/usr/bin/env python3
"""
churn_model_v18_final.py — 124 students, REAL membership end dates as labels.
74 leavers (2020-2026), 50 current members.
Dual feature sets: membership features for all, behavioral for recent cohort.
"""
import sqlite3, re, json
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
print("SOR CHURN v18 FINAL — 124 Students, Real End Dates")
print("=" * 60)

conn = sqlite3.connect(str(DB_PATH))

# ── 1. Load all leavers ──
print("\n[1] Loading 74 leavers...")
leavers = pd.read_sql_query("""
    SELECT student_name, membership_end_date, signup_date, last_visit, plan
    FROM pike13_leavers_full
""", conn, parse_dates=['membership_end_date', 'signup_date', 'last_visit'])
leavers['churned'] = 1
print(f"  Leavers: {len(leavers)}")
print(f"  Range: {leavers['membership_end_date'].min().date()} to {leavers['membership_end_date'].max().date()}")

# ── 2. Current members ──
print("[2] Loading 50 current members...")
current = pd.read_sql_query("""
    SELECT student_name, signup_date, last_visit, current_plan as plan
    FROM member_roster_snapshots
    WHERE has_membership=1 AND status='active' AND snapshot_date='2026-07-17'
""", conn, parse_dates=['signup_date', 'last_visit'])
current['churned'] = 0
current['membership_end_date'] = pd.NaT
print(f"  Current: {len(current)}")

# ── 3. Combine ──
df = pd.concat([leavers, current], ignore_index=True)
df['name_lower'] = df['student_name'].str.lower()
print(f"\n  Total: {len(df)} students ({df['churned'].sum()} churned)")

# ── 4. Membership features ──
df['signup_year'] = pd.to_datetime(df['signup_date']).dt.year.fillna(2019).astype(int)

# Tenure: for leavers it's end_date - signup. For current it's today - signup.
# But we must compute this at an OBSERVATION POINT, not at the end.
# For leavers with recent dates, observe 30 days before membership_end.
# For leavers before 2024, we don't have behavioral data anyway.
df['membership_end_date'] = pd.to_datetime(df['membership_end_date'])
df['signup_date'] = pd.to_datetime(df['signup_date'])

# Observation point for leavers: 30 days before membership_end (or signup+30, whichever is later)
df['obs_date'] = df.apply(lambda r: 
    r['membership_end_date'] - timedelta(days=30) if pd.notna(r['membership_end_date'])
    else pd.Timestamp(TODAY), axis=1)

df['tenure_at_obs'] = (df['obs_date'] - df['signup_date']).dt.days.clip(lower=1)

# Plan category
def classify_plan(plan):
    if not plan or pd.isna(plan): return 'unknown'
    plan = str(plan).lower()
    if 'bowie' in plan: return 'bowie'
    if 'cbgb' in plan: return 'cbgb'
    if 'house band' in plan: return 'house_band'
    if 'adult' in plan: return 'adult'
    if 'rehearsal' in plan: return 'rehearsal'
    if 'blues' in plan: return 'blues'
    if 'drum' in plan: return 'drum'
    if 'guitar' in plan: return 'guitar'
    if 'bass' in plan: return 'bass'
    if 'vocal' in plan or 'voice' in plan: return 'vocal'
    if 'keyboard' in plan or 'keys' in plan: return 'keys'
    if 'make' in plan: return 'makeup'
    if 'rock 101' in plan: return 'rock101'
    return 'other'

df['plan_category'] = df['plan'].apply(classify_plan)
plan_encoder = LabelEncoder()
df['plan_encoded'] = plan_encoder.fit_transform(df['plan_category'])

print(f"  Plans: {df['plan_category'].value_counts().to_dict()}")

# ── 5. Behavioral features (only for students with lesson data) ──
print("\n[3] Behavioral features (lessons, notes, calls)...")

# Load lesson data
lessons_raw = pd.read_sql_query("""
    SELECT l.lesson_id, l.students_raw, l.lesson_date, l.instructor_id
    FROM lessons l WHERE l.students_raw IS NOT NULL AND l.lesson_date >= '2024-01-01'
""", conn, parse_dates=["lesson_date"])

notes = pd.read_sql_query("SELECT lesson_id, note_score FROM lesson_notes WHERE note_score IS NOT NULL", conn)

# Expand lessons
expanded = []
for _, row in lessons_raw.iterrows():
    for name in re.split(r',\s*', str(row["students_raw"])):
        name = name.strip().lower()
        if name:
            expanded.append({"student": name, "lesson_date": row["lesson_date"],
                           "instructor_id": row["instructor_id"], "lesson_id": row["lesson_id"]})
lessons = pd.DataFrame(expanded)

# Build behavioral features for each student at their observation point
behavioral_features = []
for _, student_row in df.iterrows():
    name_lower = student_row['name_lower']
    obs = student_row['obs_date']
    is_churned = student_row['churned']
    
    sl = lessons[(lessons['student'] == name_lower) & (lessons['lesson_date'] <= obs)]
    
    if len(sl) < 2:
        # No lesson data available — skip behavioral features
        behavioral_features.append({
            'name_lower': name_lower,
            'has_behavioral_data': 0,
            'lessons_30d': 0, 'lessons_90d': 0, 'days_since_last': 999,
            'freq_decline': 0, 'avg_note_score': 0.0, 'notes_in_window': 0,
            'teacher_consistency': 0, 'tenure_lessons': 0,
        })
        continue
    
    d30 = obs - timedelta(days=30)
    d90 = obs - timedelta(days=90)
    
    l30 = len(sl[sl['lesson_date'] >= d30])
    l90 = len(sl[sl['lesson_date'] >= d90])
    days_since = (obs - sl['lesson_date'].max()).days
    
    # Frequency decline
    prior = len(sl[(sl['lesson_date'] >= obs - timedelta(days=60)) & (sl['lesson_date'] < d30)])
    fd = l30 / max(prior, 1)
    
    # Notes
    recent = sl[sl['lesson_date'] >= d90]
    mn = notes[notes['lesson_id'].isin(recent['lesson_id'])]
    avg_note = round(mn['note_score'].mean(), 2) if len(mn) > 0 else 0.0
    notes_count = len(mn)
    
    # Teacher consistency
    ic = recent['instructor_id'].value_counts()
    tc = round(ic.iloc[0] / max(len(recent), 1), 3) if len(recent) > 0 else 0
    
    behavioral_features.append({
        'name_lower': name_lower,
        'has_behavioral_data': 1,
        'lessons_30d': l30, 'lessons_90d': l90,
        'days_since_last': days_since,
        'freq_decline': round(fd, 3),
        'avg_note_score': avg_note,
        'notes_in_window': notes_count,
        'teacher_consistency': tc,
        'tenure_lessons': (obs - sl['lesson_date'].min()).days,
    })

bf = pd.DataFrame(behavioral_features)
df = df.merge(bf, on='name_lower', how='left')

# Fill defaults for students without behavioral data
for col in ['has_behavioral_data', 'lessons_30d', 'lessons_90d', 'days_since_last',
            'freq_decline', 'avg_note_score', 'notes_in_window', 'teacher_consistency', 'tenure_lessons']:
    df[col] = df[col].fillna(0)

students_with_behavioral = df['has_behavioral_data'].sum()
print(f"  Students with behavioral data: {int(students_with_behavioral)}/{len(df)}")

conn.close()

# ── 6. Feature sets ──
membership_features = ['tenure_at_obs', 'plan_encoded', 'signup_year']
behavioral_feature_list = ['lessons_30d', 'lessons_90d', 'days_since_last', 
                           'freq_decline', 'avg_note_score', 'notes_in_window',
                           'teacher_consistency', 'tenure_lessons']

# Model A: membership-only (all 124 students)
# Model B: membership + behavioral (only students with behavioral data)

print(f"\n[4] Training...")

# ── Model A: All 124 students, membership features ──
print(f"\n  Model A: All {len(df)} students, membership features only")
X_a = df[membership_features].fillna(0).replace([np.inf, -np.inf], 0).values
y_a = df['churned'].values

cb_a = CatBoostClassifier(iterations=200, depth=4, learning_rate=0.1,
                          auto_class_weights="Balanced", eval_metric="AUC",
                          random_seed=42, verbose=0, allow_writing_files=False)

n_splits_a = min(5, min(y_a.sum(), len(y_a) - y_a.sum()))
cv_a = StratifiedKFold(n_splits=max(n_splits_a, 2), shuffle=True, random_state=42)
scores_a = cross_val_score(cb_a, X_a, y_a, cv=cv_a, scoring="roc_auc")
print(f"    CatBoost CV AUC: {scores_a.mean():.3f} +/- {scores_a.std():.3f}")

cb_a.fit(X_a, y_a)
imp_a = pd.DataFrame({'feature': membership_features, 'importance': cb_a.feature_importances_})
print(f"    Importance: {dict(zip(imp_a['feature'], imp_a['importance'].round(3)))}")

# ── Model B: Students with behavioral data ──
mask_b = df['has_behavioral_data'] == 1
df_b = df[mask_b]
if len(df_b) >= 10 and df_b['churned'].sum() >= 3:
    print(f"\n  Model B: {len(df_b)} students with behavioral data ({df_b['churned'].sum()} churned)")
    all_features_b = membership_features + behavioral_feature_list
    X_b = df_b[all_features_b].fillna(0).replace([np.inf, -np.inf], 0).values
    y_b = df_b['churned'].values
    
    cb_b = CatBoostClassifier(iterations=200, depth=4, learning_rate=0.1,
                              auto_class_weights="Balanced", eval_metric="AUC",
                              random_seed=42, verbose=0, allow_writing_files=False)
    
    n_splits_b = min(5, min(y_b.sum(), len(y_b) - y_b.sum()))
    if n_splits_b >= 2:
        cv_b = StratifiedKFold(n_splits=n_splits_b, shuffle=True, random_state=42)
        scores_b = cross_val_score(cb_b, X_b, y_b, cv=cv_b, scoring="roc_auc")
        print(f"    CatBoost CV AUC: {scores_b.mean():.3f} +/- {scores_b.std():.3f}")
    else:
        cb_b.fit(X_b, y_b)
        print(f"    (insufficient for CV, trained directly)")
    
    cb_b.fit(X_b, y_b)
    imp_b = pd.DataFrame({'feature': all_features_b, 'importance': cb_b.feature_importances_})
    print(f"    Top features:")
    for _, row in imp_b.sort_values('importance', ascending=False).head(5).iterrows():
        print(f"      {row['feature']:30s} {row['importance']:.3f}")
else:
    print(f"\n  Model B: Insufficient data ({len(df_b)} students, {df_b['churned'].sum()} churned)")

# ── 7. Predict current members ──
print(f"\n[5] Predicting on 50 current members...")

# Use Model B if available, otherwise Model A
model = cb_b if 'cb_b' in dir() else cb_a
feats = all_features_b if 'all_features_b' in dir() else membership_features

current_mask = df['churned'] == 0
current_df = df[current_mask].copy()
X_pred = current_df[feats].fillna(0).replace([np.inf, -np.inf], 0).values
current_df['risk'] = (model.predict_proba(X_pred)[:, 1] * 100).round(1)
current_df = current_df.sort_values('risk', ascending=False)

print(f"\n{'='*60}")
print("TOP 10 AT-RISK (v18: real membership end dates)")
print(f"{'='*60}")
print(f"{'Student':25s} {'Risk':>5s} {'Tenure':>6s} {'L30d':>4s} {'Plan':>14s}")
print(f"{'─'*25} {'─'*5} {'─'*6} {'─'*4} {'─'*14}")

for _, row in current_df.head(10).iterrows():
    tenure_d = int(row['tenure_at_obs'])
    l30 = int(row['lessons_30d'])
    print(f"{row['student_name'][:25]:25s} {row['risk']:5.1f} {tenure_d:6d} {l30:4d} {row['plan_category'][:14]:14s}")

print(f"\n✅ v18 final: Trained on {len(df)} students ({df['churned'].sum()} churned)")
print(f"   Labels: actual Pike13 membership_end_date")
print(f"   {int(students_with_behavioral)}/{len(df)} students with behavioral features")
