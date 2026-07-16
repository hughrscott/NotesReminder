#!/usr/bin/env python3
"""
churn_model_v14.py — Smarter model focused on lesson frequency patterns,
not note scores. Tests multiple lookback windows (30/60/90d).
Adds: lesson frequency decline, gap patterns, recency-weighted features.
"""
import sqlite3, json, pickle, warnings, csv, re
from pathlib import Path
from datetime import date, timedelta
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.metrics import roc_auc_score

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"
LEAVERS_PATH = MODELS_DIR / "pike13_leavers.json"
TODAY = date.today()

MIN_LESSONS = 5


def load_data():
    con = sqlite3.connect(str(DB_PATH))
    lessons = pd.read_sql_query("""
        SELECT lesson_id, school_id, instructor_id, lesson_date, students_raw
        FROM lessons WHERE students_raw IS NOT NULL AND students_raw != ''
    """, con)
    notes = pd.read_sql_query("SELECT lesson_id, note_score FROM lesson_notes", con)

    # Load combined comms
    comms = {}
    comms_path = MODELS_DIR / "comms_final_deduped.csv"
    if comms_path.exists():
        with open(comms_path) as f:
            for row in csv.DictReader(f):
                name = row["student"].strip().lower()
                comms[name] = {
                    "total_comms": int(row["total"]),
                    "has_comms": 1 if int(row["total"]) > 0 else 0,
                }

    with open(LEAVERS_PATH) as f:
        leavers = json.load(f)

    con.close()
    lessons["lesson_date"] = pd.to_datetime(lessons["lesson_date"])
    return lessons, notes, comms, leavers


def expand_students(lessons):
    rows = []
    for _, r in lessons.iterrows():
        for n in re.split(r',\s*', str(r["students_raw"])):
            if n.strip():
                rows.append({"student": n.strip().lower(), "lesson_id": r["lesson_id"],
                            "lesson_date": r["lesson_date"], "school_id": r["school_id"],
                            "instructor_id": r["instructor_id"]})
    return pd.DataFrame(rows)


def compute_features(group, notes_df, ref_date, comms):
    """Compute lesson frequency and churn-signal features."""
    ref_ts = pd.Timestamp(ref_date)
    all_dates = group["lesson_date"].sort_values()

    # Cut at ref_date (only use data BEFORE the reference date)
    pre_data = group[group["lesson_date"] <= ref_ts]
    if len(pre_data) < MIN_LESSONS:
        return None

    # Lesson counts in different windows
    d30 = ref_ts - timedelta(days=30)
    d60 = ref_ts - timedelta(days=60)
    d90 = ref_ts - timedelta(days=90)

    total_lessons = len(pre_data)
    lessons_30d = len(pre_data[pre_data["lesson_date"] >= d30])
    lessons_60d = len(pre_data[pre_data["lesson_date"] >= d60])
    lessons_90d = len(pre_data[pre_data["lesson_date"] >= d90])

    # Frequency decline: ratio of recent (30d) to older (30-60d)
    older_30_60 = len(pre_data[(pre_data["lesson_date"] >= d60) & (pre_data["lesson_date"] < d30)])
    freq_decline = lessons_30d / max(older_30_60, 1)  # <1 means declining

    # Days since last lesson
    last_lesson = pre_data["lesson_date"].max()
    days_since_last = (ref_ts - last_lesson).days

    # Gap patterns
    dates = pre_data["lesson_date"].sort_values()
    gaps = dates.diff().dropna().dt.days
    max_gap = gaps.max() if len(gaps) > 0 else 0
    avg_gap = gaps.mean() if len(gaps) > 0 else 999
    gap_std = gaps.std() if len(gaps) > 1 else 0

    # Tenure
    first_lesson = dates.min()
    tenure_days = (ref_ts - first_lesson).days

    # Instructor consistency
    recent = pre_data[pre_data["lesson_date"] >= d90]
    inst_counts = recent["instructor_id"].value_counts()
    teacher_consistency = inst_counts.iloc[0] / len(recent) if len(recent) > 0 and len(inst_counts) > 0 else 0

    # Note scores (from window)
    win_notes = recent.merge(notes_df, on="lesson_id", how="left")
    ns = win_notes["note_score"].dropna()
    avg_note = ns.mean() if len(ns) > 0 else 0.0
    note_count = len(ns)

    # Comms
    student_key = group["student"].iloc[0]
    comm_data = comms.get(student_key, {"total_comms": 0, "has_comms": 0})

    return {
        "total_lessons": total_lessons,
        "lessons_30d": lessons_30d,
        "lessons_60d": lessons_60d,
        "lessons_90d": lessons_90d,
        "freq_decline_ratio": round(freq_decline, 3),
        "days_since_last": days_since_last,
        "max_gap_days": max_gap,
        "avg_gap_days": avg_gap,
        "gap_std": round(gap_std, 1),
        "tenure_days": tenure_days,
        "teacher_consistency": round(teacher_consistency, 3),
        "avg_note_score": round(avg_note, 2),
        "notes_in_window": note_count,
        "total_comms": comm_data["total_comms"],
        "has_comms": comm_data["has_comms"],
    }


def build_dataset(expanded, notes_df, comms, leavers, lookback_days, feature_window=90):
    today_ts = pd.Timestamp(TODAY)
    rows = []

    for name, group in expanded.groupby("student"):
        group = group.sort_values("lesson_date")
        name_l = name.strip().lower()

        if name_l in leavers:
            info = leavers[name_l]
            try:
                end_date = pd.Timestamp(info["end_date"]).date()
            except:
                continue
            ref_date = end_date - timedelta(days=lookback_days)
            if ref_date < group["lesson_date"].min().date() or ref_date > TODAY:
                continue
            label = 1
        else:
            last = group["lesson_date"].max()
            if (today_ts - last).days <= 60:
                ref_date = TODAY
                label = 0
            else:
                continue

        feat = compute_features(group, notes_df, ref_date, comms)
        if feat is None:
            continue
        feat["student"] = name
        feat["label"] = label
        feat["ref_date"] = str(ref_date)
        rows.append(feat)

    return pd.DataFrame(rows)


def train(df, features, tag, lookback):
    for f in features:
        if f not in df.columns:
            df[f] = 0

    X = df[features].fillna(0).replace([np.inf, -np.inf], 0)
    non_const = [c for c in X.columns if X[c].nunique() > 1]
    X = X[non_const]
    y = df["label"].values

    n = len(y)
    n1 = sum(y)
    print(f"  [{tag}] Lookback={lookback}d: {n} students ({n1} churned, {n-n1} active)")

    if n1 < 5 or n - n1 < 5:
        print("  Too few samples, skipping")
        return 0

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
    scaler = StandardScaler()
    Xs_train = scaler.fit_transform(X_train)
    Xs_test = scaler.transform(X_test)

    # Try different C values
    best_auc = 0
    best_c = 0.05
    for c in [0.01, 0.05, 0.1, 0.5, 1.0]:
        model = LogisticRegression(penalty="l2", C=c, class_weight="balanced",
                                   solver="liblinear", max_iter=2000, random_state=42)
        cv = cross_val_score(model, Xs_train, y_train, cv=min(5, n1, n-n1), scoring="roc_auc")
        if cv.mean() > best_auc:
            best_auc = cv.mean()
            best_c = c

    model = LogisticRegression(penalty="l2", C=best_c, class_weight="balanced",
                               solver="liblinear", max_iter=2000, random_state=42)
    model.fit(Xs_train, y_train)
    y_prob = model.predict_proba(Xs_test)[:, 1]
    auc = roc_auc_score(y_test, y_prob)

    print(f"  CV AUC: {best_auc:.3f} (C={best_c}), Test AUC: {auc:.3f}")

    # Show top coefficients
    coefs = sorted(zip(non_const, model.coef_[0]), key=lambda x: -abs(x[1]))
    print(f"  Top features:")
    for fname, coef in coefs[:8]:
        direction = "↓churn" if coef < 0 else "↑churn"
        print(f"    {fname:<25s} {coef:+8.4f} {direction}")

    return auc


def main():
    print("=" * 70)
    print("SOR Churn v14 — Lesson frequency patterns + multi-lookback")
    print("=" * 70)

    print("\n[1] Loading data...")
    lessons, notes, comms, leavers = load_data()
    expanded = expand_students(lessons)
    print(f"  {len(expanded)} student-lesson rows, {expanded['student'].nunique()} students, {len(leavers)} leavers")

    # BASE FEATURES (lesson frequency focused)
    features = [
        "total_lessons", "lessons_30d", "lessons_60d", "lessons_90d",
        "freq_decline_ratio", "days_since_last", "max_gap_days",
        "avg_gap_days", "gap_std", "tenure_days", "teacher_consistency",
        "avg_note_score", "notes_in_window",
        "total_comms", "has_comms",
    ]

    results = {}
    for lookback in [30, 60, 90]:
        print(f"\n[2] Building dataset (lookback={lookback}d)...")
        df = build_dataset(expanded, notes, comms, leavers, lookback)
        print(f"  {len(df)} labeled students")

        if len(df) < 20:
            continue

        print(f"\n[3] Training (lookback={lookback}d)...")
        auc = train(df, features, "v14", lookback)
        results[lookback] = auc

    print("\n" + "=" * 70)
    print("RESULTS:")
    for lb, auc in sorted(results.items()):
        print(f"  Lookback {lb}d: AUC = {auc:.3f}")
    print("=" * 70)


if __name__ == "__main__":
    main()
