#!/usr/bin/env python3
"""
churn_model_v13_final.py — Final retrain with all enrichments.
- Full note coverage (11,206 scored with quality + engagement)
- Combined comms features (753 students, 91% coverage)
- Email sentiment (41 students)
- Compares baseline (v12 keyword) vs enhanced (all new features)
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
from sklearn.metrics import roc_auc_score, classification_report

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"
LEAVERS_PATH = MODELS_DIR / "pike13_leavers.json"
TODAY = date.today()

LOOKBACK_DAYS = 15
FEATURE_WINDOW_DAYS = 60
MIN_LESSONS = 5

# ─── Feature sets ───
BASE_FEATURES = [
    "avg_note_score", "membership_days", "total_lessons_lifetime",
    "teacher_consistency",
]

V12_KEYWORD_FEATURES = [
    "has_communication", "communication_count",
    "has_cancellation", "has_positive", "has_frustration",
    "days_since_last_comm",
]

NEW_COMMS_FEATURES = [
    "email_count", "sms_count", "voicemail_count", "total_comms",
    "has_email_comms", "has_sms_comms", "has_vm_comms",
]

NEW_EMAIL_SENTIMENT = [
    "email_pos_ratio", "email_neg_ratio",
    "email_cancel_count", "email_concern_count",
]

BASELINE_FEATURES = BASE_FEATURES + V12_KEYWORD_FEATURES
ENHANCED_FEATURES = BASELINE_FEATURES + NEW_COMMS_FEATURES + NEW_EMAIL_SENTIMENT

EXPECTED_SIGNS = {
    "avg_note_score": -1, "membership_days": -1, "total_lessons_lifetime": -1,
    "teacher_consistency": -1, "has_communication": -1, "communication_count": -1,
    "has_cancellation": +1, "has_positive": -1, "has_frustration": +1,
    "days_since_last_comm": +1,
    "email_count": -1, "sms_count": -1, "voicemail_count": -1, "total_comms": -1,
    "has_email_comms": -1, "has_sms_comms": -1, "has_vm_comms": -1,
    "email_pos_ratio": -1, "email_neg_ratio": +1,
    "email_cancel_count": +1, "email_concern_count": +1,
}


def load_lessons_and_notes():
    con = sqlite3.connect(str(DB_PATH))
    lessons = pd.read_sql_query("""
        SELECT lesson_id, school_id, instructor_id, lesson_date,
               lesson_type, students_raw
        FROM lessons WHERE students_raw IS NOT NULL AND students_raw != ''
    """, con)
    notes = pd.read_sql_query("""
        SELECT lesson_id, note_completed, note_score, note_score_explanation
        FROM lesson_notes
    """, con)
    con.close()
    lessons["lesson_date"] = pd.to_datetime(lessons["lesson_date"])
    print(f"  Lessons: {len(lessons)}, Notes: {len(notes)} ({notes['note_score'].notna().sum()} scored)")
    return lessons, notes


def load_comms_features():
    """Load combined comms from final deduped CSV."""
    path = MODELS_DIR / "comms_final_deduped.csv"
    if not path.exists():
        print("  WARNING: No combined comms file, using v3 fallback")
        path = MODELS_DIR / "comms_student_counts_v3.csv"
    if not path.exists():
        return {}

    lookup = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            name = row["student"].strip().lower()
            ec = int(row.get("email_count", 0))
            sc = int(row.get("sms_count", 0))
            vc = int(row.get("voicemail_count", 0))
            lookup[name] = {
                "email_count": ec, "sms_count": sc, "voicemail_count": vc,
                "total_comms": ec + sc + vc,
                "has_email_comms": 1 if ec > 0 else 0,
                "has_sms_comms": 1 if sc > 0 else 0,
                "has_vm_comms": 1 if vc > 0 else 0,
            }
    print(f"  Combined comms: {len(lookup)} students")
    return lookup


def load_email_sentiment():
    """Load email sentiment features."""
    path = MODELS_DIR / "email_sentiment_features_v2.csv"
    if not path.exists():
        return {}
    lookup = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            name = row["student"].strip().lower()
            lookup[name] = {
                "email_pos_ratio": float(row.get("email_pos_ratio", 0)),
                "email_neg_ratio": float(row.get("email_neg_ratio", 0)),
                "email_cancel_count": int(row.get("email_cancel_count", 0)),
                "email_concern_count": int(row.get("email_concern_count", 0)),
            }
    print(f"  Email sentiment: {len(lookup)} students")
    return lookup


def load_v12_keywords():
    """Load v12 keyword features for baseline comparison."""
    path = MODELS_DIR / "comm_features_v12.csv"
    if not path.exists():
        return {}
    lookup = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            name = row["student"].strip().lower()
            lookup[name] = {
                "has_cancellation": int(row.get("has_cancellation", 0)),
                "has_positive": int(row.get("has_positive", 0)),
                "has_frustration": int(row.get("has_frustration", 0)),
                "has_communication": 1,
                "communication_count": int(row.get("comm_count_v2", 0)),
                "days_since_last_comm": float(row.get("days_since_last_comm", 999)),
            }
    print(f"  v12 keywords: {len(lookup)} students")
    return lookup


def load_pike13_leavers():
    with open(LEAVERS_PATH) as f:
        return json.load(f)


def expand_student_lessons(lessons_df):
    rows = []
    for _, r in lessons_df.iterrows():
        names = [n.strip() for n in re.split(r',\s*', str(r["students_raw"])) if n.strip()]
        for name in names:
            rows.append(dict(
                lesson_id=r["lesson_id"], school_id=r["school_id"],
                instructor_id=r["instructor_id"], lesson_date=r["lesson_date"],
                lesson_type=r["lesson_type"], student_name=name,
            ))
    return pd.DataFrame(rows)


def compute_features(student_lessons, notes_df, ref_date, v12_lookup, comms_lookup, email_lookup):
    ref_ts = pd.Timestamp(ref_date)
    ws = ref_ts - timedelta(days=FEATURE_WINDOW_DAYS)
    we = ref_ts
    dt = student_lessons["lesson_date"]
    win = student_lessons[(dt >= ws) & (dt <= we)]
    n_win = len(win)
    if n_win == 0:
        return None

    all_dt = dt.sort_values()
    membership_days = (ref_ts - all_dt.min()).days
    total_lessons = len(student_lessons)
    win_inst = win["instructor_id"].dropna()
    teacher_consistency = win_inst.value_counts().iloc[0] / n_win if len(win_inst) > 0 else 0

    note_agg = notes_df.groupby("lesson_id").note_score.max().reset_index()
    win_notes = win.merge(note_agg, on="lesson_id", how="left")
    ns = win_notes["note_score"].dropna()
    avg_note_score = ns.mean() if len(ns) > 0 else 0.0

    student_key = str(student_lessons["student_name"].iloc[0]).strip().lower()

    feat = dict(
        avg_note_score=avg_note_score, membership_days=membership_days,
        total_lessons_lifetime=total_lessons, teacher_consistency=teacher_consistency,
    )

    # Merge all feature sources
    for src, lookup in [("v12", v12_lookup), ("comms", comms_lookup), ("email", email_lookup)]:
        if student_key in lookup:
            feat.update(lookup[student_key])

    # Fill defaults
    for f in ENHANCED_FEATURES:
        if f not in feat:
            feat[f] = 0

    return feat


def build_dataset(lessons_expanded, notes_df, v12_lookup, comms_lookup, email_lookup, leavers):
    today_ts = pd.Timestamp(TODAY)
    rows = []

    for name, group in lessons_expanded.groupby("student_name"):
        group = group.sort_values("lesson_date")
        if len(group) < MIN_LESSONS:
            continue
        name_lower = name.strip().lower()
        last_lesson = group["lesson_date"].max()
        days_since_last = (today_ts - last_lesson).days

        if name_lower in leavers:
            leaver = leavers[name_lower]
            end_str = leaver.get("end_date", "")
            try:
                end_date = pd.Timestamp(end_str).date()
            except:
                continue
            ref_date = end_date - timedelta(days=LOOKBACK_DAYS)
            if ref_date < group["lesson_date"].min().date() or ref_date > TODAY:
                continue
            label = 1
        else:
            if days_since_last <= 60:
                ref_date = TODAY
                label = 0
            else:
                continue

        feat = compute_features(group, notes_df, ref_date, v12_lookup, comms_lookup, email_lookup)
        if feat is None:
            continue
        feat["student_name"] = name
        feat["label"] = label
        feat["ref_date"] = str(ref_date)
        rows.append(feat)

    return pd.DataFrame(rows)


def train(df, features, tag):
    """Train logistic regression with given features."""
    for f in features:
        if f not in df.columns:
            df[f] = 0

    X = df[features].fillna(0).replace([np.inf, -np.inf], 0)

    # Drop constant columns
    non_const = [c for c in X.columns if X[c].nunique() > 1]
    dropped = [c for c in X.columns if c not in non_const]
    if dropped:
        print(f"    Dropping constant: {dropped}")
    X = X[non_const]
    y = df["label"].values

    n_churned = sum(y)
    print(f"    Samples: {len(y)} ({n_churned} churned, {len(y)-n_churned} active)")

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

    print(f"    CV AUC:  {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")
    print(f"    Test AUC: {auc:.3f}")

    # Coefficient signs
    correct = 0
    for fname, coef in zip(non_const, model.coef_[0]):
        exp = EXPECTED_SIGNS.get(fname, 0)
        ok = (coef * exp) > 0 if exp != 0 else True
        if ok: correct += 1
        mark = "✓" if ok else "⚠️"
        if not ok or abs(coef) > 0.5:
            print(f"      {mark} {fname:<30s} {coef:+8.4f}")

    print(f"    {correct}/{len(non_const)} correct signs")

    # Save
    out_path = MODELS_DIR / f"churn_model_v13_final_{tag}.pkl"
    pickle.dump({"model": model, "scaler": scaler, "features": non_const,
                 "cv_auc": cv_scores.mean(), "test_auc": auc}, open(out_path, "wb"))

    return auc


def main():
    print("=" * 70)
    print("SOR Churn v13 FINAL — Full notes + Combined comms + Email sentiment")
    print("=" * 70)

    print("\n[1] Loading data...")
    lessons, notes = load_lessons_and_notes()
    lessons_expanded = expand_student_lessons(lessons)
    v12_lookup = load_v12_keywords()
    comms_lookup = load_comms_features()
    email_lookup = load_email_sentiment()
    leavers = load_pike13_leavers()

    print(f"  Dataset students: {lessons_expanded['student_name'].nunique()}")
    print(f"  Leavers: {len(leavers)}")

    print("\n[2] Building dataset...")
    df = build_dataset(lessons_expanded, notes, v12_lookup, comms_lookup, email_lookup, leavers)
    print(f"  Labeled: {len(df)} ({df['label'].sum()} churned, {(1-df['label']).sum()} active)")

    # Feature coverage stats
    for feat_set, name in [(V12_KEYWORD_FEATURES, "v12 keyword"), 
                            (NEW_COMMS_FEATURES, "comms"),
                            (NEW_EMAIL_SENTIMENT, "email sentiment")]:
        coverage = sum(1 for _, r in df.iterrows() if any(r.get(f, 0) != 0 for f in feat_set))
        print(f"  {name} coverage: {coverage}/{len(df)} ({coverage/len(df)*100:.0f}%)")

    print(f"\n[3] TRAINING BASELINE (v12 keyword only)...")
    baseline_auc = train(df, BASELINE_FEATURES, "baseline")

    print(f"\n[4] TRAINING ENHANCED (v12 + comms + email)...")
    enhanced_auc = train(df, ENHANCED_FEATURES, "enhanced")

    print("\n" + "=" * 70)
    print(f"FINAL RESULTS: Baseline={baseline_auc:.3f} → Enhanced={enhanced_auc:.3f} (Δ={enhanced_auc-baseline_auc:+.3f})")
    print("=" * 70)


if __name__ == "__main__":
    main()
