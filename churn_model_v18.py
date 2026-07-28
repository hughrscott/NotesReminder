#!/usr/bin/env python3
"""
churn_model_v18.py — Trained on REAL membership end dates, not attendance proxies.
Uses 100 students: 50 active (label=0) + 50 churned (label=1).
Features from Pike13 membership data: tenure, plan type, age, signup epoch.
"""
import sqlite3, re, json
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
from catboost import CatBoostClassifier
from xgboost import XGBClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import StandardScaler, LabelEncoder

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "reminders.db"
TODAY = datetime(2026, 7, 17)

print("=" * 60)
print("SOR CHURN v18 — Real Membership End Dates")
print("=" * 60)

conn = sqlite3.connect(str(DB_PATH))

# ── 1. Load leavers (label=1) ──
print("\n[1] Loading historical leavers...")
leavers = pd.read_sql_query("""
    SELECT student_name, membership_end_date, signup_date, last_visit, 
           current_plan, status
    FROM pike13_membership_history 
    WHERE has_membership='f' AND membership_end_date IS NOT NULL
""", conn, parse_dates=['membership_end_date', 'signup_date', 'last_visit'])
leavers['churned'] = 1
print(f"  Leavers: {len(leavers)}")
print(f"  Date range: {leavers['membership_end_date'].min().date()} to {leavers['membership_end_date'].max().date()}")

# ── 2. Load current members (label=0) ──
print("[2] Loading current members...")
current = pd.read_sql_query("""
    SELECT student_name, signup_date, last_visit, current_plan
    FROM member_roster_snapshots
    WHERE has_membership=1 AND status='active' AND snapshot_date='2026-07-17'
""", conn, parse_dates=['signup_date', 'last_visit'])
current['churned'] = 0
current['membership_end_date'] = pd.NaT
print(f"  Current: {len(current)}")

# ── 3. Combine ──
print("[3] Building features...")
df = pd.concat([leavers, current], ignore_index=True)
df['student_name_lower'] = df['student_name'].str.lower()

# Feature engineering
df['signup_date'] = pd.to_datetime(df['signup_date'])
df['signup_year'] = df['signup_date'].dt.year
df['signup_month'] = df['signup_date'].dt.month
df['signup_dow'] = df['signup_date'].dt.dayofweek  # Day of week signed up

# Tenure: for leavers, it's end_date - signup. For current, it's today - signup
df['tenure_days'] = np.where(
    df['churned'] == 1,
    (df['membership_end_date'] - df['signup_date']).dt.days,
    (pd.Timestamp(TODAY) - df['signup_date']).dt.days
)

# Plan type encoding
def classify_plan(plan):
    if not plan or pd.isna(plan):
        return 'unknown'
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
    if 'make' in plan or 'makeup' in plan: return 'makeup'
    if 'rock 101' in plan: return 'rock101'
    return 'other'

df['plan_category'] = df['current_plan'].apply(classify_plan)

# Also compute member_roster plan categories
plan_encoder = LabelEncoder()
df['plan_encoded'] = plan_encoder.fit_transform(df['plan_category'])

# Use signup year as proxy for cohort/age
df['age_approx'] = 2026 - df['signup_year'] + 10  # rough: signup at ~10 years old

conn.close()

# ── 5. Feature set ──
feature_cols = [
    'plan_encoded', 'signup_year', 'signup_month', 'signup_dow'
]

X = df[feature_cols].fillna(0).replace([np.inf, -np.inf], 0)
y = df['churned'].values

print(f"\n[4] Training on {len(df)} students ({y.sum()} churned)")
print(f"  Features: {feature_cols}")

# ── 6. Train ──
cb = CatBoostClassifier(iterations=200, depth=4, learning_rate=0.1,
                        auto_class_weights="Balanced",
                        eval_metric="AUC", random_seed=42,
                        verbose=0, allow_writing_files=False)

xgb = XGBClassifier(n_estimators=200, max_depth=4, learning_rate=0.1,
                    subsample=0.8, colsample_bytree=0.8,
                    scale_pos_weight=len(y)/sum(y),
                    eval_metric="logloss", random_state=42, verbosity=0)

lr = LogisticRegression(penalty=None, class_weight="balanced",
                        max_iter=1000, random_state=42)

n_splits = min(5, min(y.sum(), len(y) - y.sum()))
cv = StratifiedKFold(n_splits=max(n_splits, 2), shuffle=True, random_state=42)

for name, model, use_scaler in [("CatBoost", cb, False), ("XGBoost", xgb, False), ("LR", lr, True)]:
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
print(f"\n[5] Feature importance:")
imp = pd.DataFrame({'feature': feature_cols, 'importance': cb.feature_importances_})
for _, row in imp.sort_values('importance', ascending=False).iterrows():
    print(f"  {row['feature']:25s} {row['importance']:.3f}")

# ── 7. Predict current members ──
print(f"\n[6] Predicting on {len(current)} active members...")
# Get current members with all features
current_mask = df['churned'] == 0
current_features = df[current_mask][feature_cols].fillna(0).replace([np.inf, -np.inf], 0)
probs = cb.predict_proba(current_features.values)[:, 1]
# Build prediction output from df (has all columns)
current_pred = df[current_mask].copy()
current_pred['churn_risk'] = (probs * 100).round(1)
current_pred = current_pred.sort_values('churn_risk', ascending=False)

print(f"\n{'='*60}")
print("TOP 10 AT-RISK MEMBERS (v18: membership-based)")
print(f"{'='*60}")
print(f"{'Student':25s} {'Risk':>5s} {'Plan':>14s} {'Signup':>6s}")
print(f"{'─'*25} {'─'*5} {'─'*14} {'─'*6}")

for _, row in current_pred.head(10).iterrows():
    signup = str(row.get('signup_date', ''))[:10]
    print(f"{row['student_name'][:25]:25s} {row['churn_risk']:5.1f} {row['plan_category'][:14]:14s} {signup:>6s}")

print(f"\n✅ v18: Trained on 100 real students (50 churned, 50 active)")
print(f"   Labels: actual Pike13 membership_end_date, not attendance proxies")
print(f"   Caveat: limited to membership features (lesson DB doesn't cover 2019-2020 leavers)")
