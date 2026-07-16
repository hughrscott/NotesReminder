#!/usr/bin/env python3
"""
churn_model_v14_full.py — Full feature set: lesson frequency + note engagement +
comms VADER + comms keywords + email sentiment.
Tests multiple lookback windows (30/60/90d).
"""
import sqlite3, json, pickle, warnings, csv, re
from pathlib import Path
from datetime import date, timedelta
import numpy as np
import pandas as pd
from scipy import stats

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


def load_all():
    con = sqlite3.connect(str(DB_PATH))
    lessons = pd.read_sql_query("""
        SELECT lesson_id, school_id, instructor_id, lesson_date, students_raw
        FROM lessons WHERE students_raw IS NOT NULL AND students_raw != ''
    """, con)
    notes = pd.read_sql_query("SELECT lesson_id, note_score FROM lesson_notes WHERE note_score IS NOT NULL", con)
    
    # ── Comms VADER sentiment (v2) ──
    comms_v2 = {}
    v2_path = MODELS_DIR / "comm_features_v2.csv"
    if v2_path.exists():
        with open(v2_path) as f:
            for row in csv.DictReader(f):
                name = row["student"].strip().lower()
                comms_v2[name] = {
                    "comm_count": int(row.get("comm_count", 0)),
                    "comm_sources": int(row.get("comm_sources", 1)),
                    "days_since_last_comm": float(row.get("days_since_last_comm", 999)),
                    "recent_spike_ratio": float(row.get("recent_spike_ratio", 0)),
                    "longest_comm_gap_days": float(row.get("longest_comm_gap_days", 999)),
                    "comm_frequency_cv": float(row.get("comm_frequency_cv", 0)),
                    "avg_sentiment_compound": float(row.get("avg_sentiment_compound", 0)),
                    "avg_sentiment_neg": float(row.get("avg_sentiment_neg", 0)),
                    "avg_sentiment_pos": float(row.get("avg_sentiment_pos", 0)),
                    "sentiment_volatility": float(row.get("sentiment_volatility", 0)),
                    "sentiment_trend": float(row.get("sentiment_trend", 0)),
                    "neg_ratio": float(row.get("neg_ratio", 0)),
                    "pos_ratio": float(row.get("pos_ratio", 0)),
                    "reschedule_count": int(row.get("reschedule_count", 0)),
                    "question_count": int(row.get("question_count", 0)),
                }

    # ── Comms keyword features (v12) ──
    comms_v12 = {}
    v12_path = MODELS_DIR / "comm_features_v12.csv"
    if v12_path.exists():
        with open(v12_path) as f:
            for row in csv.DictReader(f):
                name = row["student"].strip().lower()
                comms_v12[name] = {
                    "has_frustration": int(row.get("has_frustration", 0)),
                    "has_cancellation": int(row.get("has_cancellation", 0)),
                    "has_financial": int(row.get("has_financial", 0)),
                    "has_positive": int(row.get("has_positive", 0)),
                    "frustration_hits": int(row.get("frustration_hits", 0)),
                    "cancellation_hits": int(row.get("cancellation_hits", 0)),
                    "positive_hits": int(row.get("positive_hits", 0)),
                    "avg_keyword_sentiment": float(row.get("avg_keyword_sentiment", 0.5)),
                    "comm_spike_7d": float(row.get("comm_spike_7d", 0)),
                    "comm_spike_30d": float(row.get("comm_spike_30d", 0)),
                }

    # ── Email sentiment ──
    email_sent = {}
    email_path = MODELS_DIR / "email_sentiment_features_v2.csv"
    if email_path.exists():
        with open(email_path) as f:
            for row in csv.DictReader(f):
                name = row["student"].strip().lower()
                email_sent[name] = {
                    "email_sentiment": float(row.get("avg_sentiment_compound", 0)),
                    "email_pos_ratio": float(row.get("pos_ratio", 0)),
                    "email_neg_ratio": float(row.get("neg_ratio", 0)),
                    "email_count": int(row.get("email_count", 0)),
                }

    with open(LEAVERS_PATH) as f:
        leavers = json.load(f)

    con.close()
    lessons["lesson_date"] = pd.to_datetime(lessons["lesson_date"])
    return lessons, notes, comms_v2, comms_v12, email_sent, leavers


def expand_students(lessons):
    rows = []
    for _, r in lessons.iterrows():
        for n in re.split(r',\s*', str(r["students_raw"])):
            if n.strip():
                rows.append({"student": n.strip().lower(), "lesson_id": r["lesson_id"],
                            "lesson_date": r["lesson_date"], "school_id": r["school_id"],
                            "instructor_id": r["instructor_id"]})
    return pd.DataFrame(rows)


def compute_note_engagement(group, notes_df, ref_ts):
    """Compute engagement scores from notes: trend, ratio, volatility."""
    merged = group.merge(notes_df, on="lesson_id", how="left")
    scores = merged.dropna(subset=["note_score"]).sort_values("lesson_date")
    
    if len(scores) < 3:
        return {"note_score_trend": 0, "notes_ratio": 0, "note_score_volatility": 0,
                "total_scored_notes": len(scores)}

    total_lessons = len(group[group["lesson_date"] <= ref_ts])
    total_scored = len(scores)
    notes_ratio = total_scored / max(total_lessons, 1)

    # Linear trend of note scores over time
    x = (scores["lesson_date"] - scores["lesson_date"].min()).dt.days.astype(float)
    if x.nunique() > 1:
        trend, _, _, _, _ = stats.linregress(x, scores["note_score"])
    else:
        trend = 0
    
    # Volatility
    volatility = scores["note_score"].std() if len(scores) > 1 else 0
    
    return {
        "note_score_trend": round(trend, 4),        # negative = declining quality
        "notes_ratio": round(notes_ratio, 3),        # higher = more engagement
        "note_score_volatility": round(volatility, 2),  # higher = inconsistent
        "total_scored_notes": total_scored,
    }


def compute_features(group, notes_df, ref_date, comms_v2, comms_v12, email_sent):
    ref_ts = pd.Timestamp(ref_date)
    pre_data = group[group["lesson_date"] <= ref_ts]
    if len(pre_data) < MIN_LESSONS:
        return None

    all_dates = pre_data["lesson_date"].sort_values()
    d30 = ref_ts - timedelta(days=30)
    d60 = ref_ts - timedelta(days=60)
    d90 = ref_ts - timedelta(days=90)

    # ── Lesson frequency features ──
    total_lessons = len(pre_data)
    lessons_30d = len(pre_data[pre_data["lesson_date"] >= d30])
    lessons_60d = len(pre_data[pre_data["lesson_date"] >= d60])
    lessons_90d = len(pre_data[pre_data["lesson_date"] >= d90])

    older = len(pre_data[(pre_data["lesson_date"] >= d60) & (pre_data["lesson_date"] < d30)])
    freq_decline = lessons_30d / max(older, 1)

    last_lesson = all_dates.max()
    days_since_last = (ref_ts - last_lesson).days
    tenure_days = (ref_ts - all_dates.min()).days

    gaps = all_dates.diff().dropna().dt.days
    max_gap = gaps.max() if len(gaps) > 0 else 0
    avg_gap = gaps.mean() if len(gaps) > 0 else 999
    gap_std = gaps.std() if len(gaps) > 1 else 0

    recent = pre_data[pre_data["lesson_date"] >= d90]
    inst_counts = recent["instructor_id"].value_counts()
    teacher_consistency = inst_counts.iloc[0] / len(recent) if len(recent) > 0 and len(inst_counts) > 0 else 0

    # ── Note engagement features ──
    note_eng = compute_note_engagement(pre_data, notes_df, ref_ts)
    avg_note = pre_data.merge(notes_df, on="lesson_id", how="left")["note_score"].mean()
    if pd.isna(avg_note):
        avg_note = 0

    # ── Comms features ──
    student_key = group["student"].iloc[0]
    v2 = comms_v2.get(student_key, {})
    v12 = comms_v12.get(student_key, {})
    email = email_sent.get(student_key, {})

    feat = {
        # Lesson frequency
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
        
        # Note engagement
        "avg_note_score": round(avg_note, 2),
        "note_score_trend": note_eng.get("note_score_trend", 0),
        "notes_ratio": note_eng.get("notes_ratio", 0),
        "note_score_volatility": note_eng.get("note_score_volatility", 0),
        "total_scored_notes": note_eng.get("total_scored_notes", 0),
        
        # Comms VADER sentiment
        "comm_sentiment": v2.get("avg_sentiment_compound", 0),
        "comm_sentiment_pos": v2.get("avg_sentiment_pos", 0),
        "comm_sentiment_neg": v2.get("avg_sentiment_neg", 0),
        "comm_sentiment_volatility": v2.get("sentiment_volatility", 0),
        "comm_sentiment_trend": v2.get("sentiment_trend", 0),
        "comm_neg_ratio": v2.get("neg_ratio", 0),
        "comm_pos_ratio": v2.get("pos_ratio", 0),
        "comm_count": v2.get("comm_count", 0),
        "comm_sources": v2.get("comm_sources", 0),
        "days_since_last_comm": v2.get("days_since_last_comm", 999),
        "comm_recent_spike": v2.get("recent_spike_ratio", 0),
        "comm_frequency_cv": v2.get("comm_frequency_cv", 0),
        "reschedule_count": v2.get("reschedule_count", 0),
        
        # Comms keyword flags
        "has_frustration": v12.get("has_frustration", 0),
        "has_cancellation": v12.get("has_cancellation", 0),
        "has_positive": v12.get("has_positive", 0),
        "has_financial": v12.get("has_financial", 0),
        "frustration_hits": v12.get("frustration_hits", 0),
        "cancellation_hits": v12.get("cancellation_hits", 0),
        "positive_hits": v12.get("positive_hits", 0),
        "comm_spike_7d": v12.get("comm_spike_7d", 0),
        "comm_spike_30d": v12.get("comm_spike_30d", 0),
        
        # Email sentiment
        "email_sentiment": email.get("email_sentiment", 0),
        "email_pos_ratio": email.get("email_pos_ratio", 0),
        "email_neg_ratio": email.get("email_neg_ratio", 0),
        "email_count": email.get("email_count", 0),
    }

    return feat


def build_dataset(expanded, notes_df, comms_v2, comms_v12, email_sent, leavers, lookback_days):
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
        
        feat = compute_features(group, notes_df, ref_date, comms_v2, comms_v12, email_sent)
        if feat is None:
            continue
        feat["student"] = name
        feat["label"] = label
        feat["ref_date"] = str(ref_date)
        rows.append(feat)

    return pd.DataFrame(rows)


def train_and_report(df, features, tag, lookback):
    for f in features:
        if f not in df.columns:
            df[f] = 0

    X = df[features].fillna(0).replace([np.inf, -np.inf], 0)
    non_const = [c for c in X.columns if X[c].nunique() > 1]
    dropped = [c for c in features if c not in non_const]
    X = X[non_const]
    y = df["label"].values

    n, n1 = len(y), sum(y)
    if n1 < 5 or n - n1 < 5:
        return 0, []

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
    scaler = StandardScaler()
    Xs_train = scaler.fit_transform(X_train)
    Xs_test = scaler.transform(X_test)

    # Try C values
    best_auc, best_c = 0, 0.05
    for c in [0.01, 0.05, 0.1, 0.5, 1.0]:
        model = LogisticRegression(penalty="l2", C=c, class_weight="balanced",
                                   solver="liblinear", max_iter=2000, random_state=42)
        cv = cross_val_score(model, Xs_train, y_train, cv=min(5, n1, n-n1), scoring="roc_auc")
        if cv.mean() > best_auc:
            best_auc, best_c = cv.mean(), c

    model = LogisticRegression(penalty="l2", C=best_c, class_weight="balanced",
                               solver="liblinear", max_iter=2000, random_state=42)
    model.fit(Xs_train, y_train)
    auc = roc_auc_score(y_test, model.predict_proba(Xs_test)[:, 1])

    coefs = sorted(zip(non_const, model.coef_[0]), key=lambda x: -abs(x[1]))

    print(f"  [{tag}] Lookback={lookback}d: n={n} ({n1} churned/{n-n1} active)")
    if dropped:
        print(f"  Dropped constants: {dropped}")
    print(f"  CV AUC: {best_auc:.3f} (C={best_c}), Test AUC: {auc:.3f}")

    # Group features by category
    freq_feats = set(f for f in non_const if f in [
        "total_lessons","lessons_30d","lessons_60d","lessons_90d","freq_decline_ratio",
        "days_since_last","max_gap_days","avg_gap_days","gap_std","tenure_days","teacher_consistency"
    ])
    note_feats = set(f for f in non_const if f.startswith("note_") or f.startswith("avg_note"))
    comm_feats = set(f for f in non_const if f.startswith("comm_") or f.startswith("has_") or f.startswith("positive_") or 
                     f.startswith("frustration_") or f.startswith("cancellation_") or f.startswith("reschedule"))
    email_feats = set(f for f in non_const if f.startswith("email_"))

    for category, cat_set, label in [
        (freq_feats, freq_feats, "📊 LESSON FREQUENCY"),
        (note_feats, note_feats, "📝 NOTE ENGAGEMENT"),
        (comm_feats, comm_feats, "💬 COMMS SENTIMENT"),
        (email_feats, email_feats, "📧 EMAIL SENTIMENT"),
    ]:
        if cat_set:
            print(f"  {label}:")
            for fname, coef in coefs:
                if fname in cat_set:
                    print(f"    {fname:<30s} {coef:+8.4f}")

    return auc, coefs


def main():
    print("=" * 70)
    print("SOR Churn v14 FULL — Frequency + Notes + Comms + Email")
    print("=" * 70)

    print("\n[1] Loading all feature sources...")
    lessons, notes, comms_v2, comms_v12, email_sent, leavers = load_all()
    expanded = expand_students(lessons)
    print(f"  {expanded['student'].nunique()} students, {len(leavers)} leavers")
    print(f"  Comms VADER: {len(comms_v2)} students")
    print(f"  Comms keywords: {len(comms_v12)} students")
    print(f"  Email sentiment: {len(email_sent)} students")

    ALL_FEATURES = [
        "total_lessons","lessons_30d","lessons_60d","lessons_90d",
        "freq_decline_ratio","days_since_last","max_gap_days",
        "avg_gap_days","gap_std","tenure_days","teacher_consistency",
        "avg_note_score","note_score_trend","notes_ratio","note_score_volatility","total_scored_notes",
        "comm_sentiment","comm_sentiment_pos","comm_sentiment_neg",
        "comm_sentiment_volatility","comm_sentiment_trend",
        "comm_neg_ratio","comm_pos_ratio","comm_count","comm_sources",
        "days_since_last_comm","comm_recent_spike","comm_frequency_cv","reschedule_count",
        "has_frustration","has_cancellation","has_positive","has_financial",
        "frustration_hits","cancellation_hits","positive_hits",
        "comm_spike_7d","comm_spike_30d",
        "email_sentiment","email_pos_ratio","email_neg_ratio","email_count",
    ]

    print(f"  Total features: {len(ALL_FEATURES)}")

    for lookback in [30, 60, 90]:
        print(f"\n[2] Dataset (lookback={lookback}d)...")
        df = build_dataset(expanded, notes, comms_v2, comms_v12, email_sent, leavers, lookback)
        print(f"  {len(df)} labeled students")
        
        # Feature coverage
        for cat, feats in [
            ("Note engagement", ["note_score_trend","notes_ratio","note_score_volatility"]),
            ("Comms VADER", ["comm_sentiment","comm_sentiment_pos"]),
            ("Comms keywords", ["has_frustration","has_cancellation","has_positive"]),
            ("Email sentiment", ["email_sentiment","email_pos_ratio"]),
        ]:
            available = [f for f in feats if f in df.columns]
            if available:
                cov = (df[available[0]] != 0).sum()
                print(f"  {cat}: {cov}/{len(df)} coverage")

        if len(df) < 20:
            continue

        print(f"\n[3] Training (lookback={lookback}d)...")
        train_and_report(df, ALL_FEATURES, "v14_full", lookback)

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
