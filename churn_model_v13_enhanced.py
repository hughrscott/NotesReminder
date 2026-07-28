#!/usr/bin/env python3
"""
churn_model_v13_enhanced.py — Enhanced retrain with real comm sentiment + transcript features.
Adds VADER sentiment from comm_features_v2.csv and recording_transcript name-matched features
to the v13 model. Compares AUC before/after.
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

# ─── Base features (from v13) ───
BASE_FEATURES = [
    "avg_note_score", "membership_days", "total_lessons_lifetime",
    "teacher_consistency",
]

# ─── v12 keyword features ───
V12_FEATURES = [
    "has_communication", "communication_count",
    "has_cancellation", "has_positive", "has_frustration",
    "days_since_last_comm",
]

# ─── NEW v2 VADER sentiment features ───
V2_FEATURES = [
    "avg_sentiment_compound", "sentiment_volatility", "sentiment_trend",
    "neg_ratio", "pos_ratio", "reschedule_count", "question_count",
    "comm_sources", "recent_spike_ratio", "longest_comm_gap_days",
]

# ─── NEW transcript sentiment features ───
TRANSCRIPT_FEATURES = [
    "has_call_transcript",
    "call_sentiment_positive_ratio", "call_sentiment_negative_ratio",
    "call_sentiment_neutral_ratio",
    "transcript_count",
]

ALL_FEATURES = BASE_FEATURES + V12_FEATURES + V2_FEATURES + TRANSCRIPT_FEATURES

EXPECTED_SIGNS = {
    "avg_note_score": -1, "membership_days": -1, "total_lessons_lifetime": -1,
    "teacher_consistency": -1, "has_communication": -1, "communication_count": -1,
    "has_cancellation": +1, "has_positive": -1, "has_frustration": +1,
    "days_since_last_comm": +1,
    "avg_sentiment_compound": -1, "sentiment_volatility": +1, "sentiment_trend": -1,
    "neg_ratio": +1, "pos_ratio": -1,
    "reschedule_count": +1, "question_count": -1,
    "comm_sources": -1, "recent_spike_ratio": -1, "longest_comm_gap_days": +1,
    "has_call_transcript": -1,
    "call_sentiment_positive_ratio": -1, "call_sentiment_negative_ratio": +1,
    "call_sentiment_neutral_ratio": 0,
    "transcript_count": -1,
}


def load_student_names(con):
    """Build set of known student names from lessons + pike13_people."""
    names = set()
    for row in con.execute("SELECT full_name FROM pike13_people WHERE full_name IS NOT NULL"):
        names.add(str(row[0]).strip().lower())
    for row in con.execute("SELECT students_raw FROM lessons WHERE students_raw IS NOT NULL AND students_raw != ''"):
        for n in re.split(r',\s*', str(row[0])):
            if n.strip():
                names.add(n.strip().lower())
    return names


def build_transcript_bridge(con):
    """Match recording_transcripts to students by name in summary/transcript fields."""
    known_names = load_student_names(con)
    student_transcripts = {}  # student_name -> list of {sentiment, intent, outcome}

    rows = con.execute("""
        SELECT call_id, sentiment, intent, outcome, topic, summary, transcript_text
        FROM recording_transcripts
        WHERE sentiment IS NOT NULL AND sentiment != ''
    """).fetchall()

    matched = 0
    for row in rows:
        text = " ".join(str(v or "") for v in [row["summary"], row["transcript_text"], row["topic"]]).lower()
        # Find student names in text
        found = []
        for name in known_names:
            if len(name) >= 4 and name in text:
                found.append(name)
        if found:
            # Use longest match (avoids "Noah" matching inside "Noah's")
            best = max(found, key=len)
            if best not in student_transcripts:
                student_transcripts[best] = []
            student_transcripts[best].append(dict(row))
            matched += 1

    print(f"  Transcript bridge: {matched} transcripts matched to {len(student_transcripts)} students")
    return student_transcripts


def load_comm_features(con):
    """Load and merge v2 + v12 comm features."""
    lookup = {}

    # Load v2 (VADER sentiment)
    v2_path = MODELS_DIR / "comm_features_v2.csv"
    if v2_path.exists():
        with open(v2_path) as f:
            for row in csv.DictReader(f):
                name = row.get("student", "").strip().lower()
                if name:
                    lookup[name] = {
                        "avg_sentiment_compound": float(row.get("avg_sentiment_compound", 0)),
                        "sentiment_volatility": float(row.get("sentiment_volatility", 0)),
                        "sentiment_trend": float(row.get("sentiment_trend", 0)),
                        "neg_ratio": float(row.get("neg_ratio", 0)),
                        "pos_ratio": float(row.get("pos_ratio", 0)),
                        "reschedule_count": int(row.get("reschedule_count", 0)),
                        "question_count": int(row.get("question_count", 0)),
                        "comm_sources": int(row.get("comm_sources", 0)),
                        "recent_spike_ratio": float(row.get("recent_spike_ratio", 0)),
                        "longest_comm_gap_days": float(row.get("longest_comm_gap_days", 0)),
                        "has_communication": 1,
                        "communication_count": int(row.get("comm_count", 0)),
                        "days_since_last_comm": float(row.get("days_since_last_comm", 999)),
                    }
        print(f"  Loaded v2 features for {len(lookup)} students")

    # Merge v12 keyword features (overwrite/add to existing)
    v12_path = MODELS_DIR / "comm_features_v12.csv"
    if v12_path.exists():
        with open(v12_path) as f:
            for row in csv.DictReader(f):
                name = row.get("student", "").strip().lower()
                if not name:
                    continue
                if name not in lookup:
                    lookup[name] = {}
                comm_count_v2 = int(row.get("comm_count_v2", "0") or "0")
                days_last = float(row.get("days_since_last_comm", "999") or "999")
                lookup[name].update({
                    "has_cancellation": int(row.get("has_cancellation", 0)),
                    "has_positive": int(row.get("has_positive", 0)),
                    "has_frustration": int(row.get("has_frustration", 0)),
                    "has_financial": int(row.get("has_financial", 0)),
                    "has_communication": lookup[name].get("has_communication", int(comm_count_v2 > 0)),
                    "communication_count": lookup[name].get("communication_count", comm_count_v2),
                    "days_since_last_comm": lookup[name].get("days_since_last_comm", days_last),
                })
        print(f"  Merged v12 features: {len(lookup)} total students")

    # Defaults for missing
    defaults = {
        "avg_sentiment_compound": 0, "sentiment_volatility": 0, "sentiment_trend": 0,
        "neg_ratio": 0, "pos_ratio": 0, "reschedule_count": 0, "question_count": 0,
        "comm_sources": 0, "recent_spike_ratio": 0, "longest_comm_gap_days": 0,
        "has_cancellation": 0, "has_positive": 0, "has_frustration": 0, "has_financial": 0,
        "has_communication": 0, "communication_count": 0, "days_since_last_comm": 999,
    }
    for name in lookup:
        for k, v in defaults.items():
            lookup[name].setdefault(k, v)

    return lookup


def load_transcript_features(con, transcript_bridge):
    """Build per-student transcript sentiment features."""
    lookup = {}
    for student, transcripts in transcript_bridge.items():
        sentiments = [t["sentiment"] for t in transcripts if t["sentiment"]]
        n = len(sentiments)
        if n == 0:
            continue
        pos = sum(1 for s in sentiments if s == "positive")
        neg = sum(1 for s in sentiments if s == "negative")
        neu = sum(1 for s in sentiments if s == "neutral")
        lookup[student] = {
            "has_call_transcript": 1,
            "call_sentiment_positive_ratio": pos / n,
            "call_sentiment_negative_ratio": neg / n,
            "call_sentiment_neutral_ratio": neu / n,
            "transcript_count": n,
        }
    print(f"  Transcript features for {len(lookup)} students")
    return lookup


# ─── Reuse existing lesson/feature code from v13 ───

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


def compute_features_for_student(student_lessons, notes_df, ref_date, comm_lookup, transcript_lookup):
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
    membership_days = (ref_ts - all_dt.min()).days
    total_lessons = len(student_lessons)
    win_inst = win["instructor_id"].dropna()
    teacher_consistency = win_inst.value_counts().iloc[0] / n_win if len(win_inst) > 0 else 0

    note_agg = notes_df.groupby("lesson_id").note_score.max().reset_index()
    win_notes = win.merge(note_agg, on="lesson_id", how="left")
    ns = win_notes["note_score"].dropna()
    avg_note_score = ns.mean() if len(ns) > 0 else 0.0

    student_key = str(student_lessons["student_name"].iloc[0]).strip().lower()
    comm = comm_lookup.get(student_key, {})
    transcript = transcript_lookup.get(student_key, {})

    feat = dict(
        school_id=school, avg_note_score=avg_note_score,
        membership_days=membership_days, total_lessons_lifetime=total_lessons,
        teacher_consistency=teacher_consistency,
    )
    feat.update(comm)
    feat.update(transcript)
    # Fill defaults
    for f in ALL_FEATURES:
        feat.setdefault(f, 0)
    return feat


def build_dataset(lessons_expanded, notes_df, comm_lookup, transcript_lookup, leavers):
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

        feat = compute_features_for_student(group, notes_df, ref_date, comm_lookup, transcript_lookup)
        if feat is None:
            continue
        feat["student_name"] = name
        feat["label"] = label
        feat["ref_date"] = str(ref_date)
        rows.append(feat)

    return pd.DataFrame(rows)


def train_and_evaluate(df, tag):
    for f in ALL_FEATURES:
        if f not in df.columns:
            df[f] = 0

    X = df[ALL_FEATURES].fillna(0).replace([np.inf, -np.inf], 0)
    
    # Debug: show feature variance
    nz = [c for c in X.columns if X[c].sum() != 0]
    print(f"  Features with non-zero values: {len(nz)}/{len(X.columns)}")
    if len(nz) < len(X.columns):
        all_zero = [c for c in X.columns if c not in nz]
        print(f"  All-zero features: {all_zero}")
    
    # Drop constant columns (all same value)
    non_const = [c for c in X.columns if X[c].nunique() > 1]
    dropped = [c for c in X.columns if c not in non_const]
    if dropped:
        print(f"  Dropping constant features: {dropped}")
    X = X[non_const]
    y = df["label"].values

    print(f"\n  [{tag}] Samples: {len(y)} ({sum(y)} churned, {sum(1-y)} active)")

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

    print(f"  CV AUC:  {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")
    print(f"  Test AUC: {auc:.3f}")

    # Coefficient sanity check
    correct = 0
    active = non_const if non_const else ALL_FEATURES
    for fname, coef in zip(active, model.coef_[0]):
        exp = EXPECTED_SIGNS.get(fname, 0)
        ok = (coef * exp) > 0 if exp != 0 else True
        if ok:
            correct += 1
    print(f"  {correct}/{len(active)} correct signs")

    # Save
    model_data = {
        "model": model, "scaler": scaler, "features": ALL_FEATURES,
        "cv_auc": cv_scores.mean(), "test_auc": auc,
    }
    out_path = MODELS_DIR / f"churn_model_v13_enhanced_{tag}.pkl"
    pickle.dump(model_data, open(out_path, "wb"))
    print(f"  Saved: {out_path}")

    return model_data, auc


def main():
    print("=" * 70)
    print("SOR Churn Model v13 — Enhanced with Comm Sentiment + Transcript Features")
    print("=" * 70)

    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row

    print("\n[1] Building transcript bridge...")
    transcript_bridge = build_transcript_bridge(con)

    print("\n[2] Loading comm features...")
    comm_lookup = load_comm_features(con)

    print("\n[3] Loading transcript features...")
    transcript_lookup = load_transcript_features(con, transcript_bridge)

    print("\n[4] Loading lessons + leavers...")
    lessons_raw, notes = load_lessons_and_notes()
    lessons_expanded = expand_student_lessons(lessons_raw)
    leavers = load_pike13_leavers()
    print(f"    {len(lessons_expanded)} student-lesson rows, {lessons_expanded['student_name'].nunique()} students")
    print(f"    {len(leavers)} leavers")

    print("\n[5] Building dataset...")
    df = build_dataset(lessons_expanded, notes, comm_lookup, transcript_lookup, leavers)
    print(f"    {len(df)} labeled students: {df['label'].sum()} churned, {(1-df['label']).sum()} active")

    # ─── BASELINE: v12 features only ───
    print("\n[6a] TRAINING BASELINE (v12 keyword features only)...")
    baseline_features = BASE_FEATURES + V12_FEATURES
    saved_features = list(ALL_FEATURES)  # COPY
    ALL_FEATURES.clear()
    ALL_FEATURES.extend(baseline_features)
    _, baseline_auc = train_and_evaluate(df, "baseline_v12")

    # ─── ENHANCED: all features ───
    print("\n[6b] TRAINING ENHANCED (v12 + VADER + transcript)...")
    ALL_FEATURES.clear()
    ALL_FEATURES.extend(saved_features)
    _, enhanced_auc = train_and_evaluate(df, "enhanced")

    print("\n" + "=" * 70)
    print(f"RESULTS: Baseline AUC={baseline_auc:.3f} → Enhanced AUC={enhanced_auc:.3f} (Δ={enhanced_auc-baseline_auc:+.3f})")
    print("=" * 70)

    con.close()


if __name__ == "__main__":
    main()
