#!/usr/bin/env python3
"""
churn_model_v15.py — Communication-aware churn model.
Adds 4 call-based features (sentiment, urgency, recency, volume) to v14's
14 lesson-frequency features. Uses CatBoost primary, XGBoost challenger,
Logistic Regression baseline. 5-fold stratified CV.
"""
import sqlite3, json, sys, re, warnings
from pathlib import Path
from datetime import timedelta

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier
from catboost import CatBoostClassifier

warnings.filterwarnings("ignore")

ROOT = Path(__file__).resolve().parent
MODELS_DIR = ROOT / "models"
DB_PATH = ROOT / "reminders.db"

MIN_LESSONS = 4


# ── Data loading ────────────────────────────────────────────────────────────

def load_data():
    """Load all source data from reminders.db."""
    conn = sqlite3.connect(str(DB_PATH))
    
    # Lessons with student info
    lessons = pd.read_sql_query("""
        SELECT l.lesson_id, l.students_raw, l.lesson_date, l.instructor_id,
               l.location,
               CAST(strftime('%Y', l.lesson_date) AS INTEGER) AS year
        FROM lessons l
        WHERE l.lesson_date IS NOT NULL
          AND l.students_raw IS NOT NULL AND l.students_raw != ''
        ORDER BY l.lesson_date
    """, conn, parse_dates=["lesson_date"])
    
    # Expand group lessons: one row per student
    expanded = []
    for _, row in lessons.iterrows():
        for name in re.split(r',\s*', str(row["students_raw"])):
            name = name.strip()
            if name:
                r = row.to_dict()
                r["student"] = name
                expanded.append(r)
    lessons = pd.DataFrame(expanded)
    
    # Notes with scores
    notes = pd.read_sql_query("""
        SELECT lesson_id, note_score
        FROM lesson_notes
    """, conn)
    
    # Leavers from v14's JSON file (Pike13 membership end dates)
    leavers_path = MODELS_DIR / "pike13_leavers.json"
    leaver_set = set()
    if leavers_path.exists():
        with open(leavers_path) as f:
            leavers = json.load(f)
        leaver_set = set(k.strip().lower() for k in leavers.keys())
    
    # ── v15: Call sentiment features via identity_matches ──
    # Build student -> call features mapping
    
    # Step 1: Get all phone numbers linked to Pike13 people via identity_matches
    phone_map = pd.read_sql_query("""
        SELECT DISTINCT 
            pp.full_name AS student_name,
            pp.phone_normalized,
            pp.person_id
        FROM pike13_people pp
        WHERE pp.phone_normalized IS NOT NULL AND pp.phone_normalized != ''
    """, conn)
    
    # Step 2: Also get phones via HubSpot → Pike13 identity matches
    id_phones = pd.read_sql_query("""
        SELECT DISTINCT 
            pp.full_name AS student_name,
            hc.phone_normalized,
            pp.person_id
        FROM identity_matches im
        JOIN hubspot_contacts hc ON im.source_id = hc.contact_id 
            AND im.source_system = 'hubspot'
            AND im.source_table = 'hubspot_contacts'
        JOIN pike13_people pp ON im.target_id = pp.person_id
            AND im.target_system = 'pike13'
        WHERE hc.phone_normalized IS NOT NULL AND hc.phone_normalized != ''
    """, conn)
    
    all_phones = pd.concat([phone_map, id_phones]).drop_duplicates(subset=["student_name", "phone_normalized"])
    # Normalize for joining: last 10 digits
    all_phones["phone_last10"] = all_phones["phone_normalized"].str.replace(r"[^0-9]", "", regex=True).str[-10:]
    
    # Step 3: Get call transcripts with sentiment
    call_sentiment = pd.read_sql_query("""
        SELECT 
            dve.phone_normalized,
            dve.event_at,
            rt.sentiment,
            rt.urgency,
            rt.intent,
            rt.outcome
        FROM dialpad_voice_events dve
        LEFT JOIN recording_transcripts rt ON dve.event_id = rt.call_id
        WHERE dve.phone_normalized IS NOT NULL
          AND dve.event_at IS NOT NULL
    """, conn, parse_dates=["event_at"])
    
    call_sentiment["phone_last10"] = call_sentiment["phone_normalized"].str.replace(r"[^0-9]", "", regex=True).str[-10:]
    
    # Step 4: Join calls → students via phone_last_10
    call_features = all_phones.merge(call_sentiment, on="phone_last10", how="inner")
    
    conn.close()
    
    return lessons, notes, call_features, leaver_set


# ── Feature engineering ─────────────────────────────────────────────────────

def compute_features(group, notes_df, ref_date, call_features_df):
    """Compute v15 features: 14 original + 4 new call-based features."""
    ref_ts = pd.Timestamp(ref_date)
    all_dates = group["lesson_date"].sort_values()
    
    pre_data = group[group["lesson_date"] <= ref_ts]
    if len(pre_data) < MIN_LESSONS:
        return None

    d30 = ref_ts - timedelta(days=30)
    d60 = ref_ts - timedelta(days=60)
    d90 = ref_ts - timedelta(days=90)

    # ── v14 features (lesson frequency) ──
    total_lessons = len(pre_data)
    lessons_30d = len(pre_data[pre_data["lesson_date"] >= d30])
    lessons_60d = len(pre_data[pre_data["lesson_date"] >= d60])
    lessons_90d = len(pre_data[pre_data["lesson_date"] >= d90])

    older_30_60 = len(pre_data[(pre_data["lesson_date"] >= d60) & (pre_data["lesson_date"] < d30)])
    freq_decline = lessons_30d / max(older_30_60, 1)

    last_lesson = pre_data["lesson_date"].max()
    days_since_last = (ref_ts - last_lesson).days

    dates = pre_data["lesson_date"].sort_values()
    gaps = dates.diff().dropna().dt.days
    max_gap = int(gaps.max()) if len(gaps) > 0 else 0
    avg_gap = round(gaps.mean(), 1) if len(gaps) > 0 else 999
    gap_std = round(gaps.std(), 1) if len(gaps) > 1 else 0

    first_lesson = dates.min()
    tenure_days = (ref_ts - first_lesson).days

    recent = pre_data[pre_data["lesson_date"] >= d90]
    inst_counts = recent["instructor_id"].value_counts()
    teacher_consistency = round(inst_counts.iloc[0] / len(recent), 3) if len(recent) > 0 and len(inst_counts) > 0 else 0

    win_notes = recent.merge(notes_df, on="lesson_id", how="left")
    ns = win_notes["note_score"].dropna()
    avg_note = round(ns.mean(), 2) if len(ns) > 0 else 0.0
    note_count = len(ns)

    # ── v15 features (call-based) ──
    student_name = group["student"].iloc[0]
    if student_name and call_features_df is not None:
        student_calls = call_features_df[
            (call_features_df["student_name"].str.lower() == str(student_name).lower()) &
            (call_features_df["event_at"] <= ref_ts)
        ]
    else:
        student_calls = pd.DataFrame()

    if len(student_calls) > 0:
        # Calls in last 30 days
        calls_30d = len(student_calls[student_calls["event_at"] >= d30])
        
        # Days since last call
        last_call = student_calls["event_at"].max()
        days_since_last_call = (ref_ts - last_call).days
        
        # Negative sentiment flag (last 30 days)
        recent_calls = student_calls[student_calls["event_at"] >= d30]
        neg_count = len(recent_calls[recent_calls["sentiment"] == "negative"])
        has_negative_call_30d = 1 if neg_count > 0 else 0
        
        # Average urgency (last 90 days)
        calls_90d = student_calls[student_calls["event_at"] >= d90]
        urgency_vals = pd.to_numeric(calls_90d["urgency"], errors="coerce").dropna()
        call_urgency_avg = round(urgency_vals.mean(), 2) if len(urgency_vals) > 0 else 0.0
    else:
        calls_30d = 0
        days_since_last_call = 999
        has_negative_call_30d = 0
        call_urgency_avg = 0.0

    return {
        # v14
        "total_lessons": total_lessons,
        "lessons_30d": lessons_30d,
        "lessons_60d": lessons_60d,
        "lessons_90d": lessons_90d,
        "freq_decline_ratio": round(freq_decline, 3),
        "days_since_last": days_since_last,
        "max_gap_days": max_gap,
        "avg_gap_days": avg_gap,
        "gap_std": gap_std,
        "tenure_days": tenure_days,
        "teacher_consistency": teacher_consistency,
        "avg_note_score": avg_note,
        "notes_in_window": note_count,
        "total_comms": 0,  # we replace this below
        "has_comms": 0,
        # v15
        "calls_last_30d": calls_30d,
        "days_since_last_call": days_since_last_call,
        "has_negative_call_30d": has_negative_call_30d,
        "call_urgency_avg": call_urgency_avg,
    }


def build_dataset(lessons, notes_df, call_features_df, leaver_set, lookback_days, feature_window=90):
    """Build train/test dataset for a given lookback window."""
    lessons["lesson_date"] = pd.to_datetime(lessons["lesson_date"])
    end_date = lessons["lesson_date"].max()
    start_date = lessons["lesson_date"].min()
    
    rows = []
    current = start_date + timedelta(days=feature_window + lookback_days)
    
    while current <= end_date:
        ref_date = current - timedelta(days=lookback_days)
        window_start = ref_date - timedelta(days=feature_window)
        
        # Students active in the feature window
        active = lessons[
            (lessons["lesson_date"] >= window_start) &
            (lessons["lesson_date"] <= ref_date)
        ]["student"].unique()
        
        for student in active:
            group = lessons[
                (lessons["student"] == student) &
                (lessons["lesson_date"] >= window_start) &
                (lessons["lesson_date"] <= ref_date)
            ]
            feat = compute_features(group, notes_df, ref_date, call_features_df)
            if feat is None:
                continue
            
            # Label: did this student churn in the lookback window?
            future_lessons = lessons[
                (lessons["student"] == student) &
                (lessons["lesson_date"] > ref_date) &
                (lessons["lesson_date"] <= current)
            ]
            churned = 1 if len(future_lessons) == 0 else 0
            
            # Also check leavers list
            if str(student).strip().lower() in leaver_set:
                churned = 1
            
            feat["student"] = student
            feat["ref_date"] = ref_date
            feat["churned"] = churned
            rows.append(feat)
        
        current += timedelta(days=7)  # weekly snapshots
    
    df = pd.DataFrame(rows)
    print(f"  Dataset: {len(df)} rows, {df['churned'].sum()} churned ({df['churned'].mean()*100:.1f}%)")
    return df


# ── Training ─────────────────────────────────────────────────────────────────

def train_and_evaluate(df, features, model_name, model, use_scaler=False, tag=""):
    """Train with 5-fold stratified CV, report AUC."""
    X = df[features].fillna(0).replace([np.inf, -np.inf], 0)
    y = df["churned"]
    
    # Remove constant columns
    non_const = [c for c in X.columns if X[c].nunique() > 1]
    X = X[non_const]
    
    if use_scaler:
        scaler = StandardScaler()
        X_arr = scaler.fit_transform(X)
    else:
        X_arr = X.values
    
    # Train/test split for final evaluation
    X_train, X_test, y_train, y_test = train_test_split(X_arr, y, test_size=0.2, stratify=y, random_state=42)
    
    # Cross-validation
    n_splits = min(5, y_train.sum(), (len(y_train) - y_train.sum()))
    if n_splits < 2:
        n_splits = 2
    cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
    
    cv_scores = cross_val_score(model, X_train, y_train, cv=cv, scoring="roc_auc")
    cv_auc = cv_scores.mean()
    cv_std = cv_scores.std()
    
    # Fit on full training set and evaluate on test
    model.fit(X_train, y_train)
    y_prob = model.predict_proba(X_test)[:, 1]
    
    from sklearn.metrics import roc_auc_score
    test_auc = roc_auc_score(y_test, y_prob)
    
    n_students = len(df["student"].unique()) if "student" in df.columns else len(df)
    n_churned = y.sum()
    
    print(f"  [{model_name}{' ' + tag if tag else ''}] {n_students} students ({n_churned} churned)")
    print(f"    CV AUC: {cv_auc:.3f} ± {cv_std:.3f}, Test AUC: {test_auc:.3f}")
    
    return {"model": model_name, "cv_auc": cv_auc, "cv_std": cv_std, "test_auc": test_auc}


def main():
    print("SOR Churn v15 — Communication-aware (CatBoost primary)")
    print(f"DB: {DB_PATH}")
    
    # Load data
    print("\n[1] Loading data...")
    lessons, notes, call_features, leaver_set = load_data()
    print(f"  Lessons: {len(lessons)}, Notes: {len(notes)}, Calls with sentiment: {len(call_features)}")
    students_with_calls = call_features["student_name"].nunique() if len(call_features) > 0 else 0
    print(f"  Students with linked calls: {students_with_calls}")
    
    # Feature list (v14 + v15 additions)
    features = [
        # v14
        "total_lessons", "lessons_30d", "lessons_60d", "lessons_90d",
        "freq_decline_ratio", "days_since_last", "max_gap_days",
        "avg_gap_days", "gap_std", "tenure_days", "teacher_consistency",
        "avg_note_score", "notes_in_window",
        # v15
        "calls_last_30d", "days_since_last_call",
        "has_negative_call_30d", "call_urgency_avg",
    ]
    
    results = {}
    
    for lookback in [30, 60, 90]:
        print(f"\n[2] Building dataset (lookback={lookback}d)...")
        df = build_dataset(lessons, notes, call_features, leaver_set, lookback)
        
        if len(df) < 20 or df["churned"].sum() < 3:
            print(f"  Skipping: insufficient data ({len(df)} rows, {df['churned'].sum()} churned)")
            continue
        
        print(f"\n[3] Training models (lookback={lookback}d)...")
        
        # Baseline: Logistic Regression
        lr = LogisticRegression(penalty="l2", C=1.0, class_weight="balanced",
                                solver="liblinear", max_iter=1000, random_state=42)
        r_lr = train_and_evaluate(df, features, "LogisticRegression", lr, use_scaler=True, tag=f"{lookback}d")
        
        # Challenger: XGBoost
        xgb = XGBClassifier(n_estimators=200, max_depth=4, learning_rate=0.05,
                            subsample=0.8, colsample_bytree=0.8,
                            scale_pos_weight=(len(df) - df["churned"].sum()) / max(df["churned"].sum(), 1),
                            eval_metric="logloss", random_state=42, verbosity=0)
        r_xgb = train_and_evaluate(df, features, "XGBoost", xgb, tag=f"{lookback}d")
        
        # Primary: CatBoost
        cb = CatBoostClassifier(iterations=300, depth=5, learning_rate=0.05,
                                l2_leaf_reg=3, border_count=64,
                                auto_class_weights="Balanced",
                                eval_metric="AUC", random_seed=42,
                                verbose=0, allow_writing_files=False)
        r_cb = train_and_evaluate(df, features, "CatBoost", cb, tag=f"{lookback}d")
        
        results[lookback] = {"lr": r_lr, "xgb": r_xgb, "cb": r_cb}
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY — v15 Results")
    print("=" * 60)
    for lookback, models in results.items():
        print(f"\nLookback={lookback}d:")
        for name, r in models.items():
            print(f"  {r['model']:25s}  CV: {r['cv_auc']:.3f} ± {r['cv_std']:.3f}  Test: {r['test_auc']:.3f}")
    
    # Best model
    best = max(
        ((lb, name, r) for lb, models in results.items() for name, r in models.items()),
        key=lambda x: x[2]["test_auc"]
    )
    print(f"\nBest: {best[1]} @ lookback={best[0]}d — Test AUC: {best[2]['test_auc']:.3f}")
    print(f"v14 baseline: AUC 0.866 (Logistic Regression, 90d lookback)")


if __name__ == "__main__":
    main()
