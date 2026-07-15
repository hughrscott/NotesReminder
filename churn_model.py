#!/usr/bin/env python3
"""
SOR Churn Model v9 — 21-Day Observation Gap.

Design (from Gemini v8 critique):
  - 21-day gap eliminates "exit behavior" leakage (makeup credits, admin cleanup)
  - Churned: ref_date = last_lesson - 21d, window = [ref-60, ref] = [last-81, last-21]
  - Active: ref_date = today, window = [today-60, today]
  - Requires ≥1 lesson in feature window (no "already gone" students)

Train/test:
  - 80/20 stratified split
  - 5-fold CV on training set only
  - Test set NEVER touches training pipeline
"""

import os, sqlite3, re, pickle
from datetime import date, timedelta
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, classification_report

DB = "reminders.db"
MDIR = "models"
os.makedirs(MDIR, exist_ok=True)

GAP_DAYS = 21  # observation gap — don't look at last 3 weeks before churn


def S(x):
    if pd.isna(x) or x is None: return ""
    return str(x).strip().lower()


# ── Data loading ──────────────────────────────────────────────

def load_data():
    c = sqlite3.connect(DB)
    lessons = pd.read_sql_query("""
        SELECT lesson_id, school_id, instructor_id, lesson_date,
               lesson_type, students_raw
        FROM lessons WHERE students_raw IS NOT NULL AND students_raw != ''
    """, c)
    notes = pd.read_sql_query("""
        SELECT lesson_id, note_completed, note_score FROM lesson_notes
    """, c)
    c.close()

    lessons["lesson_date"] = pd.to_datetime(lessons["lesson_date"])
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


# ── Feature engineering ───────────────────────────────────────

def compute_features(lessons_df, notes_df, ref_date, baselines, window_days=60):
    """v10: Self-referenced features (within-student trends, no seasonal confound)."""
    ref_ts = pd.Timestamp(ref_date)
    ws = ref_ts - timedelta(days=window_days)
    we = ref_ts

    dt = lessons_df["lesson_date"]
    win = lessons_df[(dt >= ws) & (dt <= we)]
    n_win = len(win)

    if n_win == 0:
        return None

    school = int(lessons_df["school_id"].mode().iloc[0]) if not lessons_df["school_id"].empty else 0

    # ── Recency (RFM core) ──
    all_dt = lessons_df["lesson_date"].sort_values()
    # For churned students: measure idle pattern 3-9 weeks BEFORE churn.
    # Use the gap between the student's last lesson BEFORE ref_date 
    # and the lesson before that — not the artificial 21-day observation gap.
    pre_ref = all_dt[all_dt < ref_ts]
    if len(pre_ref) >= 2:
        days_since_last = (ref_ts - pre_ref.iloc[-1]).days
        days_since_penultimate = (pre_ref.iloc[-1] - pre_ref.iloc[-2]).days
    elif len(pre_ref) == 1:
        days_since_last = (ref_ts - pre_ref.iloc[-1]).days
        days_since_penultimate = 0
    else:
        days_since_last = 0
        days_since_penultimate = 0

    # ── Frequency trend (self-referenced, within-student) ──
    # freq_trend_3mo: (lessons last 30d) / (lessons prior 30d)
    recent_30 = lessons_df[(dt >= ref_ts - timedelta(days=30)) & (dt <= ref_ts)]
    prior_30 = lessons_df[(dt >= ref_ts - timedelta(days=60)) & (dt < ref_ts - timedelta(days=30))]
    freq_trend_3mo = len(recent_30) / max(len(prior_30), 1)

    # ── Lesson cadence ──
    if len(all_dt) >= 3:
        spacings = all_dt.diff().dropna().dt.days
        avg_spacing = max(spacings.median(), 3.0)
        lesson_spacing_std = spacings.std() if len(spacings) >= 2 else 0.0
    else:
        avg_spacing = 7.0
        lesson_spacing_std = 0.0

    # ── Tenure & investment ──
    membership_days = (ref_ts - all_dt.min()).days
    total_lessons = len(lessons_df)

    # ── Instructor disruption ──
    win_inst = win["instructor_id"].dropna()
    teacher_consistency = win_inst.value_counts().iloc[0] / n_win if len(win_inst) > 0 else 0
    instructor_changes = win_inst.nunique()

    # ── Notes ──
    note_agg = notes_df.groupby("lesson_id").agg(
        note_score=("note_score", "max"),
    ).reset_index()
    win_notes = win.merge(note_agg, on="lesson_id", how="left")
    ns = win_notes["note_score"].dropna()
    avg_note_score = ns.mean() if len(ns) > 0 else 0.0

    # ── Credit accumulation / burn ──
    is_makeup = win["lesson_type"].str.contains("MAKE.?UP", case=False, na=False)
    makeup_ratio = is_makeup.sum() / n_win if n_win > 0 else 0.0

    # Makeup stockpile: estimated unused credits (90d accumulation)
    expected_90d = 90.0 / max(avg_spacing, 1.0)
    lessons_last_90d = len(lessons_df[(dt >= ref_ts - timedelta(days=90)) & (dt <= ref_ts)])
    makeup_stockpile = max(0, expected_90d - lessons_last_90d)

    # Missed in window (keeping for interaction)
    expected_in_window = 60.0 / max(avg_spacing, 1.0)
    missed_in_window = max(0, expected_in_window - n_win)
    makeup_x_missed = makeup_ratio * missed_in_window

    return dict(
        school_id=school,
        days_since_last_lesson=days_since_last,
        days_since_penultimate=days_since_penultimate,
        freq_trend_3mo=freq_trend_3mo,
        lesson_spacing_std=lesson_spacing_std,
        membership_days=membership_days,
        total_lessons_lifetime=total_lessons,
        teacher_consistency=teacher_consistency,
        instructor_changes=instructor_changes,
        avg_note_score=avg_note_score,
        makeup_ratio=makeup_ratio,
        makeup_stockpile=makeup_stockpile,
        missed_in_window=missed_in_window,
        makeup_x_missed=makeup_x_missed,
        n_lessons_window=n_win,
    )


# ── Build training dataset ────────────────────────────────────

def build_training_dataset(lessons_all, notes_all, today, baselines):
    """Build labeled dataset: churned (label=1) + active (label=0)."""
    today_ts = pd.Timestamp(today)
    X, y, meta = [], [], []

    n_churned_total = 0
    n_churned_in_window = 0
    n_active = 0

    for name, g in lessons_all.groupby("student_name"):
        g = g.sort_values("lesson_date")
        if len(g) < 5:
            continue

        last = g["lesson_date"].max()
        days_since_last = (today_ts - last).days

        if days_since_last >= 90:
            # CHURNED — use ref_date = last_lesson - GAP_DAYS
            n_churned_total += 1
            ref_date = last - timedelta(days=GAP_DAYS)
            feat = compute_features(g, notes_all, ref_date, baselines)
            if feat is None:
                continue
            n_churned_in_window += 1
            feat["student_name"] = name
            meta.append(feat)
            y.append(1)

        elif days_since_last <= 60:
            # ACTIVE — use ref_date = today
            n_active += 1
            feat = compute_features(g, notes_all, today_ts, baselines)
            if feat is None:
                continue
            feat["student_name"] = name
            meta.append(feat)
            y.append(0)

    print(f"  Churned total: {n_churned_total}, with ≥1 lesson in window: {n_churned_in_window}")
    print(f"  Active: {n_active}")
    return pd.DataFrame(meta), np.array(y)


# ── Train ──────────────────────────────────────────────────────

FEATURES = [
    # ── Recency (core RFM) ──
    "days_since_last_lesson",     # idle days — #1 churn predictor
    "days_since_penultimate",     # gap acceleration between last 2 lessons
    # ── Frequency trend (self-referenced) ──
    "freq_trend_3mo",             # (last 30d / prior 30d), within-student decline
    "lesson_spacing_std",         # irregularity of lesson schedule
    # ── Tenure & investment ──
    "membership_days",            # days since first lesson
    "total_lessons_lifetime",     # total lessons — investment proxy
    # ── Instructor ──
    "teacher_consistency",        # % with most frequent instructor
    "instructor_changes",         # distinct instructors in window — disruption
    # ── Notes ──
    "avg_note_score",             # strongest signal — instructor quality drops before churn
    # ── Credits ──
    "makeup_ratio",               # % makeup sessions (credit burn)
    "makeup_stockpile",           # estimated unused credits — early warning
    "missed_in_window",           # expected - actual in 60d window
    "makeup_x_missed",            # interaction: high both = summer break
    # ── Communication sentiment ──
    "has_communication",          # sentinel: any comms on file
    "communication_count",        # total messages matched — engagement volume
    "total_cancel_hits",          # cancel/quit phrases across all channels
    "total_concern_hits",         # cancel + dissat + schedule + financial
    "positive_hits",              # positive engagement phrases
    "avg_compound",               # VADER compound sentiment (overall tone)
    "voicemail_sentiment",        # voicemail-only VADER — most variance
]


def train_model(X_train, y_train):
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X_train)

    model = LogisticRegression(
        penalty="l2", C=0.05, class_weight="balanced",
        solver="liblinear", max_iter=1000, random_state=42,
    )
    cv_scores = cross_val_score(model, Xs, y_train, cv=skf, scoring="roc_auc")
    model.fit(Xs, y_train)

    return model, scaler, cv_scores


def load_baselines():
    """Load seasonal baselines v2 (pre-2025 data, 2023-2024).
    Returns dict of (school_id, month) → {'seasonal_index': float}"""
    # Try v2 first (pre-training baselines, no circularity)
    path = f"{MDIR}/seasonal_baselines_v2.pkl"
    if not os.path.exists(path):
        path = f"{MDIR}/seasonal_baselines.pkl"  # fallback to v1
    if not os.path.exists(path):
        print("    (No baselines — run seasonal_baselines_v2.py first)")
        return {}
    with open(path, "rb") as f:
        raw = pickle.load(f)
    # Normalize: return seasonal_index for (school_id, month)
    baselines = {}
    for key, val in raw.items():
        baselines[key] = val.get("seasonal_index", val) if isinstance(val, dict) else val
    return baselines


def load_sentiment():
    """Load pre-computed sentiment features, return dict name → {features}."""
    path = f"{MDIR}/comm_sentiment.csv"
    if not os.path.exists(path):
        print("    (No sentiment data — run comm_sentiment.py first)")
        return {}
    df = pd.read_csv(path)
    df["total_concern_hits"] = (
        df["total_cancel_hits"] + df["total_dissat_hits"] +
        df["total_schedule_hits"] + df["total_financial_hits"]
    )
    lookup = {}
    for _, r in df.iterrows():
        name = str(r["student_name"]).strip().lower()
        lookup[name] = dict(
            has_communication=1,
            communication_count=int(r.get("total_messages", 0)),
            total_cancel_hits=int(r.get("total_cancel_hits", 0)),
            total_concern_hits=int(r.get("total_concern_hits", 0)),
            positive_hits=int(r.get("total_positive_hits", 0)),
            avg_compound=float(r.get("avg_compound", 0) or 0),
            voicemail_sentiment=float(r.get("voicemail_sentiment", 0) or 0),
        )
    return lookup


def merge_sentiment(df, sent_lookup):
    """Add sentiment columns to a feature DataFrame (in-place)."""
    sent_cols = [
        "has_communication", "total_cancel_hits", "total_concern_hits",
        "positive_hits", "communication_count", "avg_compound",
        "voicemail_sentiment",
    ]
    for col in sent_cols:
        if col not in df.columns:
            df[col] = 0.0 if col in ("avg_compound", "voicemail_sentiment") else 0
    for i, row in df.iterrows():
        name = str(row.get("student_name", "")).strip().lower()
        if name in sent_lookup:
            s = sent_lookup[name]
            for col in sent_cols:
                df.at[i, col] = s.get(col, 0)


def predict_current(lessons_all, notes_all, model, scaler, sent_lookup, baselines, today):
    """Score all active students with today as ref_date."""
    today_ts = pd.Timestamp(today)
    results = []
    for name, g in lessons_all.groupby("student_name"):
        g = g.sort_values("lesson_date")
        if len(g) < 5:
            continue
        last = g["lesson_date"].max()
        days_idle = (today_ts - last).days
        if days_idle > 60:
            continue
        feat = compute_features(g, notes_all, today_ts, baselines)
        if feat is None:
            continue

        # Merge sentiment features
        s = sent_lookup.get(name.strip().lower(), {})
        for col in ["has_communication", "total_cancel_hits", "total_concern_hits",
                     "positive_hits", "communication_count", "avg_compound",
                     "voicemail_sentiment"]:
            feat[col] = s.get(col, 0)

        X_vals = np.array([[feat[f] for f in FEATURES]])
        Xs = scaler.transform(X_vals)
        risk = float(model.predict_proba(Xs)[0, 1])
        results.append(dict(
            student_name=name, school_id=feat["school_id"],
            risk=risk, days_idle=days_idle,
            days_since_last=feat.get("days_since_last_lesson", days_idle),
            avg_note_score=feat["avg_note_score"],
            freq_trend_3mo=feat.get("freq_trend_3mo", 0),
            teacher_consistency=feat["teacher_consistency"],
            instructor_changes=feat.get("instructor_changes", 0),
            has_communication=feat.get("has_communication", 0),
            cancel_hits=feat.get("total_cancel_hits", 0),
        ))
    return pd.DataFrame(results)


# ── Main ───────────────────────────────────────────────────────

def main():
    today = date.today()
    print(f"╔══════════════════════════════════════════════════════════════╗")
    print(f"║  SOR Churn Model v9 — {today}                       ║")
    print(f"║  {GAP_DAYS}-day observation gap | Logistic Regression | 5-fold CV ║")
    print(f"╚══════════════════════════════════════════════════════════════╝")
    print(f"\n  Design: features from 3-9 weeks before churn (gap={GAP_DAYS}d)")
    print(f"  This forces model to find 'fading' signals, not exit artifacts.\n")

    # ── 1. Load ──
    print("[1] Load data and baselines...")
    lessons, notes = load_data()
    baselines = load_baselines()
    print(f"    {lessons['student_name'].nunique():,} unique students, {len(notes):,} notes")
    print(f"    {len(baselines)} seasonal baselines loaded (school × month)")

    # ── 2. Build training set ──
    print("\n[2] Build training set...")
    X_df, y = build_training_dataset(lessons, notes, today, baselines)
    print(f"    Total labeled: {len(y)} ({sum(y)} churned, {sum(1-y)} active, "
          f"{sum(y)/len(y)*100:.1f}% churn rate)")

    # ── 2b. Merge sentiment features ──
    print("\n[2b] Merge communication sentiment features...")
    sent_lookup = load_sentiment()
    merge_sentiment(X_df, sent_lookup)
    n_with_comms = X_df["has_communication"].sum()
    n_with_cancel = (X_df["total_cancel_hits"] > 0).sum()
    print(f"    Students with comm data: {int(n_with_comms)} ({n_with_comms/len(X_df)*100:.0f}%)")
    print(f"    Students with cancel phrases: {int(n_with_cancel)}")

    X = X_df[FEATURES].fillna(0)

    # ── 3. Train/test split ──
    print("\n[3] Train/test split (80/20, stratified)...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )
    print(f"    Train: {len(X_train)} ({sum(y_train)} churned, {sum(1-y_train)} active)")
    print(f"    Test:  {len(X_test)} ({sum(y_test)} churned, {sum(1-y_test)} active)")

    # ── 4. Train with 5-fold CV (on training set only) ──
    print("\n[4] Train with 5-fold CV (on training set only)...")
    model, scaler, cv_scores = train_model(X_train, y_train)
    print(f"    CV AUC: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")

    # ── 5. Evaluate on held-out test set ──
    print("\n[5] Evaluate on HELD-OUT test set...")
    X_test_s = scaler.transform(X_test)
    y_pred_proba = model.predict_proba(X_test_s)[:, 1]
    y_pred = model.predict(X_test_s)
    auc_test = roc_auc_score(y_test, y_pred_proba)
    print(f"    Test AUC: {auc_test:.3f}")
    print(f"\n{classification_report(y_test, y_pred, target_names=['Active', 'Churned'])}")

    # Coefficients
    print("  Coefficients (+ = ↑ churn risk):")
    for fname, coef in zip(FEATURES, model.coef_[0]):
        sign = "↑" if coef > 0 else "↓"
        print(f"    {fname:<30s} {coef:+8.4f}  ({sign} risk)")

    # ── 6. Sanity check coefficient signs ──
    print("\n[6] Coefficient sanity check:")
    expected_signs = {
        "days_since_last_lesson": +1,     # longer idle = risky
        "days_since_penultimate": +1,     # wider gap = risky
        "freq_trend_3mo": -1,             # declining frequency = risky
        "lesson_spacing_std": +1,         # irregular schedule = risky
        "membership_days": -1,            # newer member = risky (survival)
        "total_lessons_lifetime": -1,     # fewer lessons = risky
        "teacher_consistency": -1,        # less consistent = risky
        "instructor_changes": +1,         # more changes = risky
        "avg_note_score": -1,             # lower notes = risky
        "makeup_ratio": +1,               # more makeup = burning credits
        "makeup_stockpile": +1,           # more unused credits = accumulating
        "missed_in_window": +1,           # more missed = accumulating
        "makeup_x_missed": -1,            # high both = summer break (safe)
        "has_communication": 0,           # sentinel — no expected sign
        "communication_count": 0,         # volume — ambiguous (more = engaged OR complaining)
        "total_cancel_hits": +1,          # more cancel phrases = risky (ambiguous)
        "total_concern_hits": +1,         # more concern phrases = risky
        "positive_hits": -1,              # more positive = engaged
        "avg_compound": -1,               # more negative sentiment = risky
        "voicemail_sentiment": -1,        # more negative voicemails = risky
    }
    wrong = 0
    for fname, coef in zip(FEATURES, model.coef_[0]):
        expected = expected_signs.get(fname, 0)
        is_ok = (coef * expected) > 0 if expected != 0 else True
        if not is_ok:
            wrong += 1
            print(f"    ⚠️  {fname}: coefficient {coef:+.4f} has WRONG sign (expected {expected:+d})")
        else:
            print(f"    ✓  {fname}: coefficient {coef:+.4f} — correct sign")
    if wrong == 0:
        print(f"    ✅ All {len(FEATURES)} coefficients have correct signs!")
    else:
        print(f"    ❌ {wrong}/{len(FEATURES)} coefficients have wrong signs — model may still have leakage")

    # ── 7. Predict current students ──
    print("\n[7] Predict on current students...")
    scores = predict_current(lessons, notes, model, scaler, sent_lookup, baselines, today)
    flagged = (scores["risk"] > 0.30).sum()
    print(f"    Active scored: {len(scores)}")
    print(f"    Flagged (>30%): {flagged} ({flagged/len(scores)*100:.0f}%)")

    # Save
    with open(f"{MDIR}/churn_model_v9.pkl", "wb") as f:
        pickle.dump(dict(model=model, scaler=scaler, feature_cols=FEATURES), f)
    scores.to_csv(f"{MDIR}/churn_risk_scores_v9.csv", index=False)
    print(f"    Saved → {MDIR}/churn_model_v9.pkl, churn_risk_scores_v9.csv")

    # ── 8. Top risks by school ──
    print(f"\n{'='*75}")
    print(f"  TOP RISKS — West U")
    print(f"{'='*75}")
    wu = scores[scores["school_id"] == 1].sort_values("risk", ascending=False).head(10)
    for _, r in wu.iterrows():
        print(f"  {r['risk']:5.0%}  {r['student_name']:<30s}  {int(r['days_idle'])}d idle")

    print(f"\n{'='*75}")
    print(f"  TOP RISKS — Heights")
    print(f"{'='*75}")
    th = scores[scores["school_id"] == 2].sort_values("risk", ascending=False).head(10)
    for _, r in th.iterrows():
        print(f"  {r['risk']:5.0%}  {r['student_name']:<30s}  {int(r['days_idle'])}d idle")

    # ── 9. Data honesty summary ──
    print(f"\n{'='*75}")
    print(f"  DATA HONESTY")
    print(f"{'='*75}")
    print(f"  Training: {len(y_train) + len(y_test)} labeled")
    print(f"    Churned used: {sum(y)} of 352 total churned")
    print(f"    Lost to gap:  {352 - sum(y)} ({352 - sum(y) if sum(y) < 352 else 0})")
    print(f"  Observation gap: {GAP_DAYS}d")
    print(f"  Feature window: [ref-60, ref] — 60 days of pre-churn behavior")
    print(f"  Train/Test: 80/20 stratified split, held-out test NEVER touches training")
    print(f"  CV: 5-fold stratified, on training set only")
    print(f"  Regularization: L2, C=0.05")


if __name__ == "__main__":
    main()
