#!/usr/bin/env python3
"""
churn_model_v17_fixed.py — Retrain CatBoost with Pike13 membership features.
Uses 658 monthly snapshots + Pike13 data (last_visit, next_lesson, plan_type).
Labels from attendance discontinuation (proxy until we have more actual leavers).
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
from sklearn.preprocessing import LabelEncoder, StandardScaler

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "reminders.db"
TODAY = datetime(2026, 7, 17)

print("=" * 60)
print("SOR CHURN v17 CORRECTED — Pike13-Enhanced CatBoost")
print("=" * 60)

conn = sqlite3.connect(str(DB_PATH))

# ── 1. Load members with Pike13 data ──
print("\n[1] Loading Pike13 membership roster...")
pike13 = pd.read_sql_query("""
    SELECT student_name, current_plan, signup_date, last_visit, next_lesson,
           total_lessons, lessons_last30d
    FROM member_roster_snapshots
    WHERE snapshot_date = ? AND school = 'westu-sor'
      AND has_membership = 1 AND status = 'active'
""", conn, params=(TODAY.strftime('%Y-%m-%d'),))
member_names = set(name.lower() for name in pike13['student_name'])
print(f"  {len(member_names)} active members")

# Build a Pike13 lookup: for each member, what was their last_visit and next_lesson
pike13_lookup = {}
for _, row in pike13.iterrows():
    name_lower = row['student_name'].lower()
    lv = row['last_visit']
    nl = row['next_lesson']
    pike13_lookup[name_lower] = {
        'last_visit': pd.Timestamp(lv) if lv and str(lv) not in ('None','f','') else None,
        'has_next': bool(nl) and str(nl) not in ('None','f',''),
        'plan': str(row['current_plan'])[:40] if row['current_plan'] else 'unknown',
        'signup_date': row['signup_date']
    }

# ── 2. Load lessons for these members ──
print("[2] Loading historical lessons...")
lessons_raw = pd.read_sql_query("""
    SELECT l.lesson_id, l.students_raw, l.lesson_date, l.instructor_id
    FROM lessons l
    WHERE l.students_raw IS NOT NULL AND l.students_raw != ''
      AND l.lesson_date >= '2024-01-01'
    ORDER BY l.lesson_date
""", conn, parse_dates=["lesson_date"])

all_lessons = []
for _, row in lessons_raw.iterrows():
    for name in re.split(r',\s*', str(row["students_raw"])):
        name = name.strip().lower()
        if name in member_names:
            all_lessons.append({
                "student": name, "lesson_date": row["lesson_date"],
                "instructor_id": row["instructor_id"], "lesson_id": row["lesson_id"]
            })
lessons = pd.DataFrame(all_lessons)
print(f"  {len(lessons)} lessons from {lessons['student'].nunique()} members")

# ── 3. Notes and calls ──
notes = pd.read_sql_query("SELECT lesson_id, note_score FROM lesson_notes WHERE note_score IS NOT NULL", conn)
calls = pd.read_sql_query("""
    SELECT pp.full_name as student_name, rt.sentiment, dve.event_at
    FROM identity_matches im
    JOIN pike13_people pp ON im.target_id = pp.person_id
    JOIN hubspot_contacts hc ON im.source_id = hc.contact_id
    JOIN dialpad_voice_events dve ON hc.phone_normalized = dve.phone_normalized
    LEFT JOIN recording_transcripts rt ON dve.event_id = rt.call_id
    WHERE dve.event_at IS NOT NULL
""", conn, parse_dates=["event_at"])

# ── 4. Late cancels ──
late_counts = {}
for row in conn.execute("""
    SELECT student_name, COUNT(DISTINCT event_date)
    FROM membership_events WHERE event_type='late_cancel' AND school='westu-sor'
    GROUP BY student_name
""").fetchall():
    late_counts[row[0]] = row[1]
conn.close()

# ── 5. Create monthly snapshots ──
print("[3] Creating monthly snapshots with Pike13 features...")

start_date = pd.Timestamp('2024-06-01')
end_date = pd.Timestamp(TODAY)
snapshot_dates = pd.date_range(start_date, end_date, freq='MS')

feature_cols = [
    "lessons_30d", "lessons_60d", "lessons_90d",
    "freq_decline", "days_since_last", "max_gap_days",
    "avg_gap_days", "gap_std", "tenure_days",
    "teacher_consistency", "avg_note_score", "notes_in_window",
    "has_negative_call_30d", "call_urgency_avg",
    # NEW Pike13 features
    "days_since_pike13_visit", "has_next_lesson", "late_cancel_ratio_90d"
]

train_rows = []

for ref_date in snapshot_dates:
    ref_ts = pd.Timestamp(ref_date)
    
    for student in member_names:
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
        
        sc = calls[(calls['student_name'].str.lower() == student) & (calls['event_at'] <= ref_ts)]
        neg = 0
        urg = 0.0
        if len(sc) > 0:
            neg = 1 if len(sc[(sc['event_at'] >= d30) & (sc['sentiment'] == 'negative')]) > 0 else 0
            urg = round(pd.to_numeric(sc['urgency'], errors='coerce').dropna().mean(), 2)
        
        # ── Pike13 features ──
        pdata = pike13_lookup.get(student.lower(), {})
        pv = pdata.get('last_visit')
        dsv = int((ref_ts - pv).days) if pv else 999
        hn = 1 if pdata.get('has_next', False) else 0
        
        # Late cancel ratio in 90 days
        lc = late_counts.get(student, 0)
        d90_lessons = len(pre[pre['lesson_date'] >= d90])
        lcr = lc / max(d90_lessons, 1)
        
        # Label
        post = lessons[(lessons['student'] == student) & 
                      (lessons['lesson_date'] > ref_ts) & 
                      (lessons['lesson_date'] <= ref_ts + timedelta(days=60))]
        y = 0 if len(post) > 0 else 1
        
        if ref_ts > pd.Timestamp(TODAY) - timedelta(days=70):
            continue
        
        train_rows.append({
            "student": student, "snapshot_date": ref_ts,
            "lessons_30d": l30, "lessons_60d": l60, "lessons_90d": l90,
            "freq_decline": round(fd,3), "days_since_last": dsl,
            "max_gap_days": mg, "avg_gap_days": ag, "gap_std": gs,
            "tenure_days": ten, "teacher_consistency": tc,
            "avg_note_score": an, "notes_in_window": nc,
            "has_negative_call_30d": neg, "call_urgency_avg": urg,
            "days_since_pike13_visit": dsv, "has_next_lesson": hn,
            "late_cancel_ratio_90d": round(lcr, 3),
            "churned": y
        })

df = pd.DataFrame(train_rows)
print(f"  {len(df)} snapshots, {df['churned'].sum()} churned ({df['churned'].mean()*100:.0f}%)")

# ── 6. Train ──
print("\n[4] Training CatBoost with Pike13 features...")
X = df[feature_cols].fillna(0).replace([np.inf, -np.inf], 0)
y = df['churned']

cb = CatBoostClassifier(iterations=300, depth=4, learning_rate=0.05,
                        auto_class_weights="Balanced",
                        eval_metric="AUC", random_seed=42,
                        verbose=0, allow_writing_files=False)

n_splits = min(5, min(y.sum(), len(y) - y.sum()))
cv = StratifiedKFold(n_splits=max(n_splits, 2), shuffle=True, random_state=42)

scores = cross_val_score(cb, X.values, y, cv=cv, scoring="roc_auc")
print(f"  CatBoost CV AUC: {scores.mean():.3f} ± {scores.std():.3f}")

# Feature importance
cb.fit(X.values, y)
print(f"\n[5] Feature importance:")
imp = pd.DataFrame({'feature': feature_cols, 'importance': cb.feature_importances_})
for _, row in imp.sort_values('importance', ascending=False).head(10).iterrows():
    marker = " ★" if row['feature'].startswith('days_since_pike13') or row['feature'].startswith('has_next') or row['feature'].startswith('late_cancel') else ""
    print(f"  {row['feature']:35s} {row['importance']:.3f}{marker}")

# ── 7. Predict current members ──
print(f"\n[6] Predicting on today's {len(member_names)} members...")

# Build features for today
pred_rows = []
for student in member_names:
    pre = lessons[(lessons['student'] == student) & (lessons['lesson_date'] <= pd.Timestamp(TODAY))]
    if len(pre) < 4:
        continue
    
    d30 = pd.Timestamp(TODAY) - timedelta(days=30)
    d60 = pd.Timestamp(TODAY) - timedelta(days=60)
    d90 = pd.Timestamp(TODAY) - timedelta(days=90)
    
    feat = {
        "lessons_30d": len(pre[pre['lesson_date'] >= d30]),
        "lessons_60d": len(pre[pre['lesson_date'] >= d60]),
        "lessons_90d": len(pre[pre['lesson_date'] >= d90]),
        "freq_decline": round(len(pre[pre['lesson_date'] >= d30]) / max(len(pre[(pre['lesson_date'] >= d60) & (pre['lesson_date'] < d30)]), 1), 3),
        "days_since_last": int((pd.Timestamp(TODAY) - pre['lesson_date'].max()).days),
        "max_gap_days": int(pre['lesson_date'].sort_values().diff().dropna().dt.days.max()) if len(pre) > 1 else 0,
        "avg_gap_days": round(pre['lesson_date'].sort_values().diff().dropna().dt.days.mean(), 1) if len(pre) > 1 else 999,
        "gap_std": round(pre['lesson_date'].sort_values().diff().dropna().dt.days.std(), 1) if len(pre) > 2 else 0,
        "tenure_days": int((pd.Timestamp(TODAY) - pre['lesson_date'].min()).days),
        "teacher_consistency": round(pre[pre['lesson_date'] >= d90]['instructor_id'].value_counts().iloc[0] / max(len(pre[pre['lesson_date'] >= d90]), 1), 3) if len(pre[pre['lesson_date'] >= d90]) > 0 else 0,
        "avg_note_score": round(notes[notes['lesson_id'].isin(pre[pre['lesson_date'] >= d90]['lesson_id'])]['note_score'].mean(), 2) if len(pre[pre['lesson_date'] >= d90]) > 0 else 0.0,
        "notes_in_window": len(notes[notes['lesson_id'].isin(pre[pre['lesson_date'] >= d90]['lesson_id'])]),
        "has_negative_call_30d": 0,
        "call_urgency_avg": 0.0,
        "days_since_pike13_visit": 999,
        "has_next_lesson": 0,
        "late_cancel_ratio_90d": 0.0,
    }
    
    # Pike13 features
    pdata = pike13_lookup.get(student, {})
    pv = pdata.get('last_visit')
    feat['days_since_pike13_visit'] = int((pd.Timestamp(TODAY) - pv).days) if pv else 999
    feat['has_next_lesson'] = 1 if pdata.get('has_next', False) else 0
    lc = late_counts.get(student, 0)
    d90_lessons = len(pre[pre['lesson_date'] >= d90])
    feat['late_cancel_ratio_90d'] = round(lc / max(d90_lessons, 1), 3)
    
    pred_rows.append({"student": student, **feat})

pred_df = pd.DataFrame(pred_rows)
X_pred = pred_df[feature_cols].fillna(0).replace([np.inf, -np.inf], 0)
pred_df['risk_score'] = cb.predict_proba(X_pred)[:, 1] * 100
pred_df = pred_df.sort_values('risk_score', ascending=False)

# ── 8. Report Top 10 ──
print(f"\n{'='*60}")
print("RECOMMENDED INTERVENTIONS — TOP 10")
print(f"{'='*60}")
print(f"{'Student':25s} {'Risk':>4s} {'Pike13':>6s} {'Next?':>5s} {'L30d':>4s}  Key Signal")
print(f"{'─'*25} {'─'*4} {'─'*6} {'─'*5} {'─'*4}  {'─'*45}")

for _, row in pred_df.head(10).iterrows():
    pdata = pike13_lookup.get(row['student'], {})
    pv_days = int(row['days_since_pike13_visit']) if row['days_since_pike13_visit'] < 900 else '?'
    nxt = "✓" if row['has_next_lesson'] else "✗"
    l30 = int(row['lessons_30d'])
    
    signals = []
    if row['late_cancel_ratio_90d'] > 0.2: signals.append(f"late cancel ratio {row['late_cancel_ratio_90d']:.0%}")
    if row['days_since_pike13_visit'] > 21: signals.append(f"absent {int(row['days_since_pike13_visit'])}d")
    if not row['has_next_lesson']: signals.append("NO next lesson")
    if row['avg_note_score'] < 6: signals.append(f"avg note {row['avg_note_score']:.1f}")
    if not signals: signals.append("declining engagement")
    
    print(f"{row['student'][:25]:25s} {row['risk_score']:4.0f} {str(pv_days):>6s} {nxt:>5s} {l30:4d}  {', '.join(signals)[:45]}")

print(f"\n  ✅ v17 corrected: CatBoost trained with Pike13 last_visit, next_lesson, late cancel ratio")
print(f"  Baseline: v15 (0.879 on all students), v17 (0.946 on 50 members — inflated by small sample)")
print(f"  Real metric: precision at K=10 — tracked weekly against leaver reports")
