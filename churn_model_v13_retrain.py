#!/usr/bin/env python3
"""
churn_model_v13_retrain.py — Retrain using Pike13 last_membership_end dates
with a configurable lookback window (30d or 90d before formal cancellation).

This implements the correct temporal structure:
- For leavers: ref_date = end_date - LOOKBACK_DAYS; features from [ref_date-60, ref_date]; label = 1
- For active: ref_date = today; features from [today-60, today]; label = 0

The model learns PRE-CHURN behavioral patterns, not the cancellation event itself.
"""

import sqlite3, json, pickle, warnings
from pathlib import Path
from datetime import date, timedelta
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.metrics import roc_auc_score, classification_report

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"
LEAVERS_PATH = MODELS_DIR / "pike13_leavers.json"
TODAY = date.today()

# ─── Config ───
LOOKBACK_DAYS = 15      # Days before Pike13 end_date to set ref_date (try 15, 30, 60, 90)
FEATURE_WINDOW_DAYS = 60  # Feature window before ref_date
MIN_LESSONS = 5
GAP_DAYS = 21          # Observation gap (same as v11)

# ─── Feature list (v12 baseline: 6 features with correct signs) ───
BASE_FEATURES = [
    "avg_note_score",
    "membership_days",
    "total_lessons_lifetime",
    "teacher_consistency",
    "has_communication",
    "communication_count",
]
# Add v12 keyword sentiment features
KEYWORD_FEATURES = [
    "has_cancellation",
    "has_positive",
    "has_frustration",
    "days_since_last_comm",
]
ALL_FEATURES = BASE_FEATURES + KEYWORD_FEATURES

# ─── Expected coefficient signs (for validation) ───
EXPECTED_SIGNS = {
    "avg_note_score": -1,
    "membership_days": -1,
    "total_lessons_lifetime": -1,
    "teacher_consistency": -1,
    "has_communication": -1,      # has comm = engaged = less churn
    "communication_count": -1,    # more comms = engaged
    "has_cancellation": +1,       # cancel language = more churn
    "has_positive": -1,           # positive language = less churn
    "has_frustration": +1,        # frustration = more churn
    "days_since_last_comm": +1,   # radio silence = more churn
}


def load_lessons_and_notes():
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
    return lessons, notes


def load_comm_sentiment_features():
    """Load v12 keyword sentiment features from comm_features_v12.csv"""
    path = MODELS_DIR / "comm_features_v12.csv"
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    lookup = {}
    for _, row in df.iterrows():
        name = str(row.get("student", "")).strip().lower()
        if name:
            lookup[name] = {
                "has_cancellation": int(row.get("has_cancellation", 0)),
                "has_positive": int(row.get("has_positive", 0)),
                "has_frustration": int(row.get("has_frustration", 0)),
                "days_since_last_comm": float(row.get("days_since_last_comm", 999)),
                "has_communication": 1,  # if they're in this file, they have comms
                "communication_count": int(row.get("comm_count", 0)),
            }
    print(f"  Loaded comm features for {len(lookup)} students")
    return lookup


def load_pike13_leavers():
    with open(LEAVERS_PATH) as f:
        return json.load(f)


def expand_student_lessons(lessons_df):
    """Expand comma-separated students_raw into one row per student."""
    rows = []
    import re
    for _, r in lessons_df.iterrows():
        names = [n.strip() for n in re.split(r',\s*', str(r["students_raw"])) if n.strip()]
        for name in names:
            rows.append(dict(
                lesson_id=r["lesson_id"], school_id=r["school_id"],
                instructor_id=r["instructor_id"],
                lesson_date=r["lesson_date"],
                lesson_type=r["lesson_type"],
                student_name=name,
            ))
    return pd.DataFrame(rows)


def compute_features_for_student(student_lessons, notes_df, ref_date, comm_lookup):
    """Compute all features for one student at their ref_date."""
    ref_ts = pd.Timestamp(ref_date)
    ws = ref_ts - timedelta(days=FEATURE_WINDOW_DAYS)
    we = ref_ts

    dt = student_lessons["lesson_date"]
    win = student_lessons[(dt >= ws) & (dt <= we)]
    n_win = len(win)

    if n_win == 0:
        return None

    school = int(student_lessons["school_id"].mode().iloc[0]) if not student_lessons["school_id"].empty else 0
    all_dt = dt.sort_values()

    # ── Tenure & total lessons ──
    membership_days = (ref_ts - all_dt.min()).days
    total_lessons = len(student_lessons)

    # ── Instructor consistency ──
    win_inst = win["instructor_id"].dropna()
    teacher_consistency = win_inst.value_counts().iloc[0] / n_win if len(win_inst) > 0 else 0

    # ── Notes ──
    note_agg = notes_df.groupby("lesson_id").note_score.max().reset_index()
    win_notes = win.merge(note_agg, on="lesson_id", how="left")
    ns = win_notes["note_score"].dropna()
    avg_note_score = ns.mean() if len(ns) > 0 else 0.0

    # ── Lesson spacing ──
    if len(all_dt) >= 3:
        spacings = all_dt.diff().dropna().dt.days
        avg_spacing = max(spacings.median(), 3.0)
    else:
        avg_spacing = 7.0

    # ── Communication features (from v12) ──
    student_key = str(student_lessons["student_name"].iloc[0]).strip().lower()
    comm = comm_lookup.get(student_key, {})

    return dict(
        school_id=school,
        avg_note_score=avg_note_score,
        membership_days=membership_days,
        total_lessons_lifetime=total_lessons,
        teacher_consistency=teacher_consistency,
        avg_spacing=avg_spacing,
        **comm,  # has_cancellation, has_positive, has_frustration, days_since_last_comm, has_communication, communication_count
    )


def build_dataset(lessons_expanded, notes_df, comm_lookup, leavers):
    """Build labeled dataset using ONLY Pike13 end dates with lookback.
    Active students = negatives. Proxy churned students EXCLUDED (not ground truth)."""
    today_ts = pd.Timestamp(TODAY)
    rows = []

    # Group lessons by student
    for name, group in lessons_expanded.groupby("student_name"):
        group = group.sort_values("lesson_date")
        if len(group) < MIN_LESSONS:
            continue

        name_lower = name.strip().lower()
        last_lesson = group["lesson_date"].max()
        days_since_last = (today_ts - last_lesson).days

        # ── Check if this student is a Pike13 leaver ──
        if name_lower in leavers:
            # CHURNED: use Pike13 end date as anchor
            leaver = leavers[name_lower]
            end_str = leaver.get("end_date", "")
            try:
                end_date = pd.Timestamp(end_str).date()  # parse "Jul 31, 2026"
            except:
                print(f"  ⚠️  Could not parse end_date for {name}: {end_str}")
                continue

            # Ref date = end_date - LOOKBACK_DAYS (e.g., 30/90 days before they formally cancelled)
            ref_date = end_date - timedelta(days=LOOKBACK_DAYS)

            # Skip if ref_date is before their first lesson (no features to extract)
            if ref_date < group["lesson_date"].min().date():
                continue

            # Also skip if ref_date is in the future
            if ref_date > TODAY:
                continue

            label = 1
            churn_month = end_date.month

        else:
            # ACTIVE ONLY: standard labeling (recent lessons = active)
            # EXCLUDE proxy churned (90-day idle) — they're not verified leavers
            if days_since_last <= 60:
                ref_date = TODAY
                label = 0
                churn_month = TODAY.month
            else:
                continue  # Skip ambiguous (60-90 days) and proxy churned (>90 days)

        feat = compute_features_for_student(group, notes_df, ref_date, comm_lookup)
        if feat is None:
            continue

        feat["student_name"] = name
        feat["label"] = label
        feat["churn_month"] = churn_month
        feat["ref_date"] = str(ref_date)
        rows.append(feat)

    df = pd.DataFrame(rows)
    return df


def train_and_evaluate(df):
    """Train logistic regression and validate coefficient signs."""
    print(f"\n{'='*70}")
    print(f"Training with LOOKBACK_DAYS={LOOKBACK_DAYS}, FEATURE_WINDOW={FEATURE_WINDOW_DAYS}")
    print(f"{'='*70}")

    # Ensure all feature columns exist
    for f in ALL_FEATURES:
        if f not in df.columns:
            df[f] = 0

    X = df[ALL_FEATURES].fillna(0)
    X = X.replace([np.inf, -np.inf], 0)
    y = df["label"].values

    print(f"  Samples: {len(y)} ({sum(y)} churned, {sum(1-y)} active, {sum(y)/len(y)*100:.1f}% churn rate)")

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

    scaler = StandardScaler()
    Xs_train = scaler.fit_transform(X_train)
    Xs_test = scaler.transform(X_test)

    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    model = LogisticRegression(penalty="l2", C=0.05, class_weight="balanced",
                               solver="liblinear", max_iter=1000, random_state=42)

    cv_scores = cross_val_score(model, Xs_train, y_train, cv=skf, scoring="roc_auc")
    model.fit(Xs_train, y_train)

    y_prob = model.predict_proba(Xs_test)[:, 1]
    auc = roc_auc_score(y_test, y_prob)
    y_pred = model.predict(Xs_test)

    print(f"  CV AUC:  {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")
    print(f"  Test AUC: {auc:.3f}")
    print(f"\n{classification_report(y_test, y_pred, target_names=['Active', 'Churned'])}")

    # Coefficient sanity check
    print("  Coefficients:")
    correct = 0
    for fname, coef in zip(ALL_FEATURES, model.coef_[0]):
        exp = EXPECTED_SIGNS.get(fname, 0)
        ok = (coef * exp) > 0 if exp != 0 else True
        mark = "✓" if ok else "⚠️"
        if ok:
            correct += 1
        print(f"    {mark} {fname:<30s} {coef:+8.4f} (expected: {'+' if exp>0 else '-' if exp<0 else '?'})")

    print(f"  {correct}/{len(ALL_FEATURES)} correct signs")

    # Save model
    model_data = {
        "model": model,
        "scaler": scaler,
        "features": ALL_FEATURES,
        "cv_auc": cv_scores.mean(),
        "test_auc": auc,
        "lookback_days": LOOKBACK_DAYS,
        "feature_window_days": FEATURE_WINDOW_DAYS,
        "correct_signs": correct,
        "total_features": len(ALL_FEATURES),
    }
    out_path = MODELS_DIR / f"churn_model_v13_lookback{LOOKBACK_DAYS}d.pkl"
    pickle.dump(model_data, open(out_path, "wb"))
    print(f"\n  Model saved: {out_path}")

    # Save risk scores for all students
    all_prob = model.predict_proba(scaler.transform(X))[:, 1]
    df["risk"] = all_prob
    df.to_csv(MODELS_DIR / f"v13_risk_scores_lookback{LOOKBACK_DAYS}d.csv", index=False)

    return model_data, df


def main():
    print(f"╔{'═'*68}╗")
    print(f"║  SOR Churn Model v13 — Pike13 Leaver Dates + Lookback      ║")
    print(f"║  Lookback: {LOOKBACK_DAYS}d before end_date  |  Window: {FEATURE_WINDOW_DAYS}d feature window  ║")
    print(f"╚{'═'*68}╝")

    print("\n[1] Loading data...")
    lessons_raw, notes = load_lessons_and_notes()
    lessons_expanded = expand_student_lessons(lessons_raw)
    print(f"    {len(lessons_expanded)} student-lesson rows, {lessons_expanded['student_name'].nunique()} students")

    comm_lookup = load_comm_sentiment_features()

    print("\n[2] Loading Pike13 leavers...")
    leavers = load_pike13_leavers()
    print(f"    {len(leavers)} students with last_membership_end dates")

    print("\n[3] Building dataset...")
    df = build_dataset(lessons_expanded, notes, comm_lookup, leavers)
    print(f"    {len(df)} labeled students: {df['label'].sum()} churned, {(1-df['label']).sum()} active")

    print("\n[4] Training...")
    model_data, merged = train_and_evaluate(df)

    print("\n" + "="*70)
    print("DONE")
    print("="*70)


if __name__ == "__main__":
    main()