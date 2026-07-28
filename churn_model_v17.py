#!/usr/bin/env python3
"""
churn_model_v17.py — Retrain CatBoost using MEMBERS-ONLY historical data.
Creates monthly snapshots for each member's lesson history.
Labels: Y=1 if student stopped attending (proxy for membership cancellation).
Only includes confirmed current members from the Pike13 roster.
"""
import sqlite3, re, json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
import pandas as pd
import numpy as np
from catboost import CatBoostClassifier
from xgboost import XGBClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "reminders.db"
TODAY = datetime(2026, 7, 17)

print("=" * 60)
print("SOR CHURN v17 — MEMBERS-ONLY CatBoost Training")
print("=" * 60)

conn = sqlite3.connect(str(DB_PATH))

# ── 1. Load current members ──
print("\n[1] Loading current members...")
member_names = set()
for row in conn.execute("""
    SELECT DISTINCT LOWER(student_name) FROM member_roster_snapshots
    WHERE snapshot_date = ? AND school = 'westu-sor'
""", (TODAY.strftime('%Y-%m-%d'),)).fetchall():
    member_names.add(row[0])

print(f"  {len(member_names)} current members")

# ── 2. Load ALL lessons for these members ──
print("[2] Loading historical lessons...")
lessons_raw = pd.read_sql_query("""
    SELECT l.lesson_id, l.students_raw, l.lesson_date, l.instructor_id
    FROM lessons l
    WHERE l.students_raw IS NOT NULL AND l.students_raw != ''
      AND l.lesson_date >= '2024-01-01'
    ORDER BY l.lesson_date
""", conn, parse_dates=["lesson_date"])

# Expand and filter to members only
all_lessons = []
for _, row in lessons_raw.iterrows():
    for name in re.split(r',\s*', str(row["students_raw"])):
        name = name.strip().lower()
        if name in member_names:
            all_lessons.append({
                "student": name,
                "lesson_date": row["lesson_date"],
                "instructor_id": row["instructor_id"],
                "lesson_id": row["lesson_id"]
            })
lessons = pd.DataFrame(all_lessons)
print(f"  {len(lessons)} lessons across {lessons['student'].nunique()} members")

# ── 3. Notes and calls ──
notes = pd.read_sql_query("SELECT lesson_id, note_score FROM lesson_notes WHERE note_score IS NOT NULL", conn)
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

# ── 4. Create monthly snapshots ──
print("[3] Creating monthly training snapshots...")

start_date = pd.Timestamp('2024-06-01')
end_date = pd.Timestamp(TODAY)
snapshot_dates = pd.date_range(start_date, end_date, freq='MS')  # first of each month

feature_cols = [
    "lessons_30d", "lessons_60d", "lessons_90d",
    "freq_decline", "days_since_last", "max_gap_days",
    "avg_gap_days", "gap_std", "tenure_days",
    "teacher_consistency", "avg_note_score", "notes_in_window",
    "has_negative_call_30d", "call_urgency_avg"
]

train_rows = []

for ref_date in snapshot_dates:
    ref_ts = pd.Timestamp(ref_date)
    
    for student in member_names:
        # Get lessons up to ref_date
        pre = lessons[(lessons['student'] == student) & (lessons['lesson_date'] <= ref_ts)]
        if len(pre) < 4:
            continue
        
        d30 = ref_ts - timedelta(days=30)
        d60 = ref_ts - timedelta(days=60)
        d90 = ref_ts - timedelta(days=90)
        
        l30 = len(pre[pre['lesson_date'] >= d30])
        l60 = len(pre[pre['lesson_date'] >= d60])
        l90 = len(pre[pre['lesson_date'] >= d90])
        
        older = len(pre[(pre['lesson_date'] >= d60) & (pre['lesson_date'] < d30)])
        fd = l30 / max(older, 1)
        
        last_date = pre['lesson_date'].max()
        dsl = (ref_ts - last_date).days
        
        dates = pre['lesson_date'].sort_values()
        gaps = dates.diff().dropna().dt.days
        mg = int(gaps.max()) if len(gaps) > 0 else 0
        ag = round(gaps.mean(), 1) if len(gaps) > 0 else 999
        gs = round(gaps.std(), 1) if len(gaps) > 1 else 0
        
        ten = (ref_ts - dates.min()).days
        
        recent = pre[pre['lesson_date'] >= d90]
        ic = recent['instructor_id'].value_counts()
        tc = round(ic.iloc[0] / len(recent), 3) if len(recent) > 0 else 0
        
        win_notes = recent.merge(notes, on='lesson_id', how='left')
        ns = win_notes['note_score'].dropna()
        an = round(ns.mean(), 2) if len(ns) > 0 else 0.0
        nc = len(ns)
        
        # Calls
        sc = calls[(calls['student_name'].str.lower() == student) & (calls['event_at'] <= ref_ts)]
        if len(sc) > 0:
            neg = 1 if len(sc[(sc['event_at'] >= d30) & (sc['sentiment'] == 'negative')]) > 0 else 0
            urg = round(pd.to_numeric(sc['urgency'], errors='coerce').dropna().mean(), 2) if len(sc) > 0 else 0.0
        else:
            neg = 0
            urg = 0.0
        
        # Label: did they attend in the 60 days AFTER ref_date?
        post = lessons[(lessons['student'] == student) & 
                      (lessons['lesson_date'] > ref_ts) & 
                      (lessons['lesson_date'] <= ref_ts + timedelta(days=60))]
        y = 0 if len(post) > 0 else 1  # 1 = churned (no future attendance)
        
        # Skip snapshots too close to today (no label available)
        if ref_ts > pd.Timestamp(TODAY) - timedelta(days=70):
            continue
        
        train_rows.append({
            "student": student,
            "snapshot_date": ref_ts,
            "lessons_30d": l30, "lessons_60d": l60, "lessons_90d": l90,
            "freq_decline": round(fd, 3), "days_since_last": dsl,
            "max_gap_days": mg, "avg_gap_days": ag, "gap_std": gs,
            "tenure_days": ten, "teacher_consistency": tc,
            "avg_note_score": an, "notes_in_window": nc,
            "has_negative_call_30d": neg, "call_urgency_avg": urg,
            "churned": y
        })

df = pd.DataFrame(train_rows)
print(f"  {len(df)} snapshots, {df['churned'].sum()} churned ({df['churned'].mean()*100:.0f}%)")

if len(df) < 20 or df['churned'].sum() < 3:
    print("\n❌ INSUFFICIENT DATA for training")
    print(f"   Need at least 20 snapshots and 3 churn events. Got {len(df)}/{df['churned'].sum()}.")
    exit(1)

# ── 5. Train ──
print("\n[4] Training models...")
X = df[feature_cols].fillna(0).replace([np.inf, -np.inf], 0)
y = df['churned']

print(f"  Features: {len(feature_cols)}, Class balance: {y.mean():.1%} churn")

# CatBoost
cb = CatBoostClassifier(iterations=300, depth=4, learning_rate=0.05,
                        auto_class_weights="Balanced",
                        eval_metric="AUC", random_seed=42,
                        verbose=0, allow_writing_files=False)

# XGBoost
xgb = XGBClassifier(n_estimators=200, max_depth=4, learning_rate=0.05,
                    subsample=0.8, colsample_bytree=0.8,
                    scale_pos_weight=len(y)/sum(y) if sum(y) > 0 else 1,
                    eval_metric="logloss", random_state=42, verbosity=0)

# LR baseline
lr = LogisticRegression(penalty="l2", C=1.0, class_weight="balanced",
                        solver="liblinear", max_iter=1000, random_state=42)

# Cross-validation
n_splits = min(5, min(y.sum(), len(y) - y.sum()))
cv = StratifiedKFold(n_splits=max(n_splits, 2), shuffle=True, random_state=42)

for name, model, use_scaler in [("CatBoost", cb, False), ("XGBoost", xgb, False), ("LogisticRegression", lr, True)]:
    X_arr = X.values
    if use_scaler:
        X_arr = StandardScaler().fit_transform(X_arr)
    try:
        scores = cross_val_score(model, X_arr, y, cv=cv, scoring="roc_auc")
        print(f"  {name:25s} CV AUC: {scores.mean():.3f} ± {scores.std():.3f}")
    except Exception as e:
        print(f"  {name:25s} ERROR: {e}")

# Feature importance
cb.fit(X.values, y)
print(f"\n[5] Top features (CatBoost):")
importances = pd.DataFrame({'feature': feature_cols, 'importance': cb.feature_importances_})
for _, row in importances.sort_values('importance', ascending=False).head(8).iterrows():
    print(f"  {row['feature']:30s} {row['importance']:.3f}")

print(f"\n✅ v17 trained on {len(df)} member snapshots from {lessons['student'].nunique()} students")
print(f"   v15 baseline: 0.879 (but inflated by trialist predictions)")
print(f"   v17 is the FIRST model trained exclusively on paying members")
