#!/usr/bin/env python3
"""SOR Churn Model v11 — Four-phase rebuild after external model feedback.

Phase 1: 6-feature baseline (correct signs only — no attendance)
Phase 2: Seasonal matching (churned vs active by calendar month)
Phase 3: Survival analysis (Cox proportional hazards)
Phase 4: Better attendance (deviation from student's own history)

Feedback from both Codex (ChatGPT 5.6) and Gemini 3.5:    
  Consensus: seasonal matching, don't abandon attendance, try Cox PH.
"""
from __future__ import annotations

import os, sys, pickle, warnings
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression, LogisticRegressionCV
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.metrics import roc_auc_score, classification_report, precision_recall_curve
from lifelines import CoxPHFitter
from lifelines.utils import concordance_index

warnings.filterwarnings("ignore")

PROJ = Path(__file__).parent
MDIR = PROJ / "models"; MDIR.mkdir(exist_ok=True)
DB_PATH = PROJ / "reminders.db"
MODEL_PATH = MDIR / "churn_model_v11.pkl"
RISK_PATH = MDIR / "churn_risk_scores_v11.csv"

GAP_DAYS = 21      # observation gap: ignore last 21 days before churn label
WINDOW_DAYS = 60   # feature window: look back 60 days from ref_date
MIN_LESSONS = 5    # must have ≥5 lessons to include

TODAY = str(date.today())

# ═══════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════

def load_data():
    import sqlite3, re
    con = sqlite3.connect(str(DB_PATH))
    lessons = pd.read_sql_query("""
        SELECT lesson_id, school_id, instructor_id, lesson_date,
               lesson_type, students_raw
        FROM lessons WHERE students_raw IS NOT NULL AND students_raw != ''
    """, con)
    notes = pd.read_sql_query("""
        SELECT lesson_id, note_completed, note_score FROM lesson_notes
    """, con)
    con.close()

    lessons["lesson_date"] = pd.to_datetime(lessons["lesson_date"])

    # Expand students_raw (single or comma-separated) to one row per student
    rows = []
    for _, r in lessons.iterrows():
        names = [n.strip() for n in re.split(r',\s*', str(r["students_raw"])) if n.strip()]
        for name in names:
            rows.append(dict(
                lesson_id=r["lesson_id"], school_id=r["school_id"],
                instructor_id=r["instructor_id"],
                lesson_date=r["lesson_date"],
                lesson_type=r["lesson_type"],
                student_name=name,
            ))
    return pd.DataFrame(rows), notes


def load_sentiment():
    path = MDIR / "comm_sentiment.csv"
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    df["total_concern_hits"] = (
        df["total_cancel_hits"].fillna(0) + df["total_dissat_hits"].fillna(0) +
        df["total_schedule_hits"].fillna(0) + df["total_financial_hits"].fillna(0)
    )
    lookup = {}
    for _, r in df.iterrows():
        name = str(r["student_name"]).strip().lower()
        lookup[name] = dict(
            has_communication=1,
            communication_count=int(r.get("total_messages", 0) or 0),
            total_cancel_hits=int(r.get("total_cancel_hits", 0) or 0),
            total_concern_hits=int(r.get("total_concern_hits", 0) or 0),
            positive_hits=int(r.get("total_positive_hits", 0) or 0),
            avg_compound=float(r.get("avg_compound", 0) or 0),
            voicemail_sentiment=float(r.get("voicemail_sentiment", 0) or 0),
        )
    return lookup


# ═══════════════════════════════════════════════════════════════════
# FEATURES
# ═══════════════════════════════════════════════════════════════════

# Phase 1: 6 reliable features (all had correct signs in v10)
PHASE1_FEATURES = [
    "avg_note_score",        # strongest signal — instructors give worse notes before churn
    "membership_days",       # tenure — newer students churn more (survival curve)
    "total_lessons_lifetime",# investment — fewer total lessons = higher risk
    "teacher_consistency",   # same-instructor ratio
    "has_communication",     # sentinel — parents we can contact = engaged
    "communication_count",   # volume — more communication = engaged parents
]

# Phase 4: attendance features redesigned as deviation-from-self
PHASE4_ATTENDANCE_FEATURES = [
    "attendance_deviation",  # (current rate / student's 12-month avg) — self-referenced
    "idle_deviation",        # (days_since_last / student's avg_gap) — normalized idle
    "credit_accumulation",   # unused makeup credits accumulated in 90d
]


def compute_phase1_features(lessons_df, notes_df, ref_date):
    """Phase 1: 6 reliable features. No attendance data at all."""
    ref_ts = pd.Timestamp(ref_date)
    ws = ref_ts - timedelta(days=WINDOW_DAYS)

    dt = lessons_df["lesson_date"]
    win = lessons_df[(dt >= ws) & (dt <= ref_ts)]
    n_win = len(win)
    if n_win == 0:
        return None

    school = int(lessons_df["school_id"].mode().iloc[0]) if not lessons_df["school_id"].empty else 0
    all_dt = dt.sort_values()

    # tenure
    membership_days = (ref_ts - all_dt.min()).days
    total_lessons = len(lessons_df)

    # instructor
    win_inst = win["instructor_id"].dropna()
    teacher_consistency = win_inst.value_counts().iloc[0] / n_win if len(win_inst) > 0 else 0

    # notes
    note_agg = notes_df.groupby("lesson_id").note_score.max().reset_index()
    win_notes = win.merge(note_agg, on="lesson_id", how="left")
    ns = win_notes["note_score"].dropna()
    avg_note_score = ns.mean() if len(ns) > 0 else 0.0

    # lesson spacing (for phase 4)
    if len(all_dt) >= 3:
        spacings = all_dt.diff().dropna().dt.days
        avg_spacing = max(spacings.median(), 3.0)
        spacing_std = spacings.std()
    else:
        avg_spacing = 7.0
        spacing_std = 0.0

    # attendance deviation (phase 4): use 12-month self-baseline
    year_ago = ref_ts - timedelta(days=365)
    historical = lessons_df[(dt >= year_ago) & (dt < ref_ts)]
    if len(historical) >= 5:
        hist_span = (historical["lesson_date"].max() - historical["lesson_date"].min()).days or 1
        hist_rate = len(historical) / max(hist_span, 1) * 7  # lessons/week
        current_rate = n_win / WINDOW_DAYS * 7
        attendance_deviation = current_rate / max(hist_rate, 0.01)
    else:
        attendance_deviation = 1.0

    # idle deviation
    pre_ref = all_dt[all_dt < ref_ts]
    if len(pre_ref) >= 1:
        days_since_last = (ref_ts - pre_ref.iloc[-1]).days
    else:
        days_since_last = 0
    idle_deviation = days_since_last / max(avg_spacing, 1.0)

    # credit accumulation
    lessons_90d = len(lessons_df[(dt >= ref_ts - timedelta(days=90)) & (dt <= ref_ts)])
    expected_90d = 90.0 / max(avg_spacing, 1.0)
    credit_accumulation = max(0, expected_90d - lessons_90d)

    return dict(
        school_id=school,
        ref_month=ref_ts.month,
        avg_note_score=avg_note_score,
        membership_days=membership_days,
        total_lessons_lifetime=total_lessons,
        teacher_consistency=teacher_consistency,
        attendance_deviation=attendance_deviation,
        idle_deviation=idle_deviation,
        credit_accumulation=credit_accumulation,
        avg_spacing=avg_spacing,
    )


# ═══════════════════════════════════════════════════════════════════
# DATASET BUILDING
# ═══════════════════════════════════════════════════════════════════

def build_dataset(lessons_all, notes_all, today, mode="standard"):
    """Build labeled dataset. mode='standard' uses all data. 'seasonal' matches by month."""
    today_ts = pd.Timestamp(today)
    rows = []

    for name, g in lessons_all.groupby("student_name"):
        g = g.sort_values("lesson_date")
        if len(g) < MIN_LESSONS:
            continue

        last = g["lesson_date"].max()
        days_since_last = (today_ts - last).days

        if days_since_last >= 90:
            ref_date = last - timedelta(days=GAP_DAYS)
            label = 1  # churned
        elif days_since_last <= 60:
            ref_date = today_ts
            label = 0  # active
        else:
            continue  # ambiguous (60-90 day gap)

        feat = compute_phase1_features(g, notes_all, ref_date)
        if feat is None:
            continue

        feat["student_name"] = name
        feat["label"] = label
        feat["churn_month"] = last.month  # month they churned (for seasonal matching)
        rows.append(feat)

    df = pd.DataFrame(rows)
    return df


def seasonal_match(df):
    """Phase 2: For each churned student, pick an active student from the SAME month."""
    np.random.seed(42)
    matched = []

    for churn_month, churned_group in df[df["label"] == 1].groupby("churn_month"):
        active_pool = df[(df["label"] == 0) & (df["ref_month"] == churn_month)]
        n_churned = len(churned_group)

        if len(active_pool) >= n_churned:
            sampled = active_pool.sample(n=n_churned, random_state=42)
        else:
            sampled = active_pool  # take all available (undersample churned to match)

        # Take same number of churned as active available
        matched_churned = churned_group.sample(n=min(n_churned, len(active_pool)), random_state=42) if len(active_pool) < n_churned else churned_group

        matched.append(matched_churned)
        matched.append(sampled)

    result = pd.concat(matched, ignore_index=True)
    print(f"    Seasonal matching: {len(df)} → {len(result)} ({result['label'].sum()} churned, {(1-result['label']).sum()} active)")
    return result


# ═══════════════════════════════════════════════════════════════════
# PHASE 1: 6-FEATURE BASELINE
# ═══════════════════════════════════════════════════════════════════

def run_phase1(df, sent_lookup):
    """Phase 1: Train with only 6 reliable features."""
    print("\n" + "=" * 70)
    print("PHASE 1: 6-Feature Baseline (notes + tenure + comms, no attendance)")
    print("=" * 70)

    # Merge sentiment
    for col in ["has_communication", "communication_count"]:
        if col not in df.columns:
            df[col] = 0.0 if col in ("avg_compound", "voicemail_sentiment") else 0
    for i, row in df.iterrows():
        name = str(row.get("student_name", "")).strip().lower()
        s = sent_lookup.get(name, {})
        for col in ["has_communication", "communication_count"]:
            df.at[i, col] = s.get(col, 0)

    X = df[PHASE1_FEATURES].fillna(0)
    y = df["label"].values

    print(f"  Samples: {len(y)} ({sum(y)} churned, {sum(1-y)} active, {sum(y)/len(y)*100:.1f}% churn rate)")

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
    print(f"  Train: {len(X_train)}, Test: {len(X_test)}")

    scaler = StandardScaler()
    Xs_train = scaler.fit_transform(X_train)
    Xs_test = scaler.transform(X_test)

    # Cross-validation
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    model = LogisticRegression(penalty="l2", C=0.05, class_weight="balanced", solver="liblinear", max_iter=1000, random_state=42)
    cv_scores = cross_val_score(model, Xs_train, y_train, cv=skf, scoring="roc_auc")
    model.fit(Xs_train, y_train)

    y_prob = model.predict_proba(Xs_test)[:, 1]
    auc = roc_auc_score(y_test, y_prob)
    y_pred = model.predict(Xs_test)

    print(f"  CV AUC:  {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")
    print(f"  Test AUC: {auc:.3f}")
    print(f"\n{classification_report(y_test, y_pred, target_names=['Active', 'Churned'])}")

    # Coefficient sanity
    expected = {
        "avg_note_score": -1, "membership_days": -1, "total_lessons_lifetime": -1,
        "teacher_consistency": -1, "has_communication": 0, "communication_count": 0,
    }
    print("  Coefficients:")
    correct = 0
    for fname, coef in zip(PHASE1_FEATURES, model.coef_[0]):
        exp = expected.get(fname, 0)
        ok = (coef * exp) > 0 if exp != 0 else True
        mark = "✓" if ok else "⚠️"
        if ok: correct += 1
        print(f"    {mark} {fname:<30s} {coef:+8.4f}")
    print(f"  {correct}/{len(PHASE1_FEATURES)} correct signs")

    return model, scaler, auc


# ═══════════════════════════════════════════════════════════════════
# PHASE 2: SEASONAL MATCHING
# ═══════════════════════════════════════════════════════════════════

def run_phase2(df, sent_lookup):
    """Phase 2: Seasonally-matched dataset — churned vs active from same month."""
    print("\n" + "=" * 70)
    print("PHASE 2: Seasonal Matching (churned vs active by calendar month)")
    print("=" * 70)

    matched = seasonal_match(df)
    run_phase1(matched, sent_lookup)


# ═══════════════════════════════════════════════════════════════════
# PHASE 3: SURVIVAL ANALYSIS
# ═══════════════════════════════════════════════════════════════════

def run_phase3(lessons, notes, sent_lookup, today):
    """Phase 3: Cox Proportional Hazards survival analysis."""
    print("\n" + "=" * 70)
    print("PHASE 3: Survival Analysis (Cox Proportional Hazards)")
    print("=" * 70)

    today_ts = pd.Timestamp(today)
    rows = []

    for name, g in lessons.groupby("student_name"):
        g = g.sort_values("lesson_date")
        if len(g) < MIN_LESSONS:
            continue

        last = g["lesson_date"].max()
        days_since_last = (today_ts - last).days

        # Duration: days from first lesson to last lesson (or to today if active)
        first = g["lesson_date"].min()
        if days_since_last >= 90:
            duration = (last - first).days
            event = 1  # churned
            ref_date = last - timedelta(days=GAP_DAYS)
        else:
            duration = (today_ts - first).days
            event = 0  # censored (still active)
            ref_date = today_ts

        feat = compute_phase1_features(g, notes, ref_date)
        if feat is None:
            continue

        feat["student_name"] = name
        feat["duration"] = max(duration, 1)
        feat["event"] = event
        rows.append(feat)

    df = pd.DataFrame(rows)

    # Merge sentiment
    for i, row in df.iterrows():
        name = str(row.get("student_name", "")).strip().lower()
        s = sent_lookup.get(name, {})
        for col in ["has_communication", "communication_count"]:
            df.at[i, col] = s.get(col, 0)

    # Cox PH features
    cox_features = PHASE1_FEATURES + ["duration", "event"]

    # Clean: remove rows with NaN or inf
    cox_df = df[cox_features].fillna(0)
    cox_df = cox_df.replace([np.inf, -np.inf], 0)

    # Scale features
    scaler = StandardScaler()
    feature_cols = PHASE1_FEATURES
    cox_df[feature_cols] = scaler.fit_transform(cox_df[feature_cols])

    print(f"  Samples: {len(cox_df)} ({int(cox_df['event'].sum())} events, {int((1-cox_df['event']).sum())} censored)")

    try:
        cph = CoxPHFitter(penalizer=0.1)
        cph.fit(cox_df, duration_col="duration", event_col="event")
        c_index = concordance_index(cox_df["duration"], -cph.predict_partial_hazard(cox_df[feature_cols]), cox_df["event"])
        print(f"  Concordance Index: {c_index:.3f}")
        print("\n  Hazard Ratios (exp(coef)):")
        summary = cph.summary[["exp(coef)", "p"]]
        for fname in feature_cols:
            if fname in summary.index:
                hr = summary.loc[fname, "exp(coef)"]
                pval = summary.loc[fname, "p"]
                direction = "↑ risk" if hr > 1 else "↓ risk"
                sig = "*" if pval < 0.05 else ""
                print(f"    {fname:<30s} {hr:8.3f}  ({direction}){sig}")
    except Exception as e:
        print(f"  Cox PH failed: {e}")


# ═══════════════════════════════════════════════════════════════════
# PHASE 4: BETTER ATTENDANCE FEATURES
# ═══════════════════════════════════════════════════════════════════

def run_phase4(df, sent_lookup):
    """Phase 4: Add deviation-from-self attendance features to Phase 1 baseline."""
    print("\n" + "=" * 70)
    print("PHASE 4: Baseline + Deviation-from-Self Attendance Features")
    print("=" * 70)

    features = PHASE1_FEATURES + PHASE4_ATTENDANCE_FEATURES

    # Merge sentiment
    for col in ["has_communication", "communication_count"]:
        if col not in df.columns:
            df[col] = 0
    for i, row in df.iterrows():
        name = str(row.get("student_name", "")).strip().lower()
        s = sent_lookup.get(name, {})
        for col in ["has_communication", "communication_count"]:
            df.at[i, col] = s.get(col, 0)

    X = df[features].fillna(0)
    X = X.replace([np.inf, -np.inf], 0)
    y = df["label"].values

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

    scaler = StandardScaler()
    Xs_train = scaler.fit_transform(X_train)
    Xs_test = scaler.transform(X_test)

    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    model = LogisticRegression(penalty="l2", C=0.05, class_weight="balanced", solver="liblinear", max_iter=1000, random_state=42)
    cv_scores = cross_val_score(model, Xs_train, y_train, cv=skf, scoring="roc_auc")
    model.fit(Xs_train, y_train)

    y_prob = model.predict_proba(Xs_test)[:, 1]
    auc = roc_auc_score(y_test, y_prob)

    print(f"  CV AUC:  {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")
    print(f"  Test AUC: {auc:.3f}")

    # Coefficient sanity
    expected = {
        "avg_note_score": -1, "membership_days": -1, "total_lessons_lifetime": -1,
        "teacher_consistency": -1, "has_communication": 0, "communication_count": 0,
        "attendance_deviation": -1,  # declining from self = risky
        "idle_deviation": +1,        # longer idle than normal = risky
        "credit_accumulation": +1,   # accumulating unused credits = early warning
    }
    print("  Coefficients:")
    correct = 0
    for fname, coef in zip(features, model.coef_[0]):
        exp = expected.get(fname, 0)
        ok = (coef * exp) > 0 if exp != 0 else True
        mark = "✓" if ok else "⚠️"
        if ok: correct += 1
        print(f"    {mark} {fname:<30s} {coef:+8.4f}")
    print(f"  {correct}/{len(features)} correct signs")

    # Flagged rate
    all_prob = model.predict_proba(scaler.transform(X))[:, 1]
    flagged = (all_prob > 0.30).sum()
    print(f"  Flagged (>30%): {flagged}/{len(y)} ({flagged/len(y)*100:.0f}%)")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    print(f"╔{'═'*68}╗")
    print(f"║  SOR Churn Model v11 — {TODAY}                     ║")
    print(f"║  Phases 1-4: Baseline → Seasonal → Survival → Self-Ref ║")
    print(f"╚{'═'*68}╝")
    print(f"\n  Gap: {GAP_DAYS}d observation | Window: {WINDOW_DAYS}d | Min: {MIN_LESSONS} lessons")

    print("\n[0] Load data...")
    lessons, notes = load_data()
    sent_lookup = load_sentiment()
    print(f"    {len(lessons)} lessons, {lessons['student_name'].nunique()} students, {len(sent_lookup)} with comms")

    print("\n[1] Build dataset...")
    df = build_dataset(lessons, notes, TODAY)
    print(f"    {len(df)} labeled: {df['label'].sum()} churned, {(1-df['label']).sum()} active ({df['label'].mean()*100:.1f}% churn rate)")

    # Phase 1: 6-feature baseline
    run_phase1(df, sent_lookup)

    # Phase 2: Seasonal matching
    run_phase2(df, sent_lookup)

    # Phase 3: Survival analysis
    run_phase3(lessons, notes, sent_lookup, TODAY)

    # Phase 4: Better attendance
    run_phase4(df, sent_lookup)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print("  See coefficient signs above for each phase.")
    print("  Target: ≥all correct signs, honest AUC, 15-25% flagged.")


if __name__ == "__main__":
    main()
