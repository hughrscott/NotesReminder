#!/usr/bin/env python3
"""
SOR Churn Model v6 — Honest, Constrained, Logistic Regression.

Design principles (from Gemini critique of v5):
  1. 90-day observation gap — feature window ends 90d before today;
     label is what happened in the 90 days AFTER. No data leakage.
  2. Require ≥1 lesson in feature window — eliminates "already gone"
     students whose zero-attendance features trivially predict churn.
  3. Logistic Regression — simple, regularized, transparent. No
     XGBoost memorizing noise on 800 samples.
  4. 6 core features only — attendance, teacher, notes, seasonality.
     No communication features (pipeline still immature).
  5. Pike13 operational data only — the cleanest, most reliable source.
"""

import os, sqlite3, pickle, warnings, re
from datetime import date, timedelta
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")
DB = "reminders.db"
MDIR = "models"
os.makedirs(MDIR, exist_ok=True)

FEATURE_COLS = [
    "avg_weekly",        # lessons/week in feature window
    "weekly_trend",      # slope of weekly attendance
    "teacher_consistency",  # % of lessons with same instructor
    "avg_note_score",    # GPT note quality (0-5)
    "note_completion_rate",  # % of lessons with completed notes
    "month_of_year",     # seasonality from first lesson
]
GAP_DAYS = 90   # observation gap: predict churn 90 days ahead
WINDOW_DAYS = 60  # feature window size


# ── DATA LOADING ─────────────────────────────────────────────────────────

def load():
    c = sqlite3.connect(DB)
    lessons = pd.read_sql_query("""
        SELECT lesson_id, school_id, instructor_id, lesson_date, lesson_time,
               lesson_type, location, students_raw, lesson_is_group,
               lesson_student_count, lesson_is_reportable
        FROM lessons WHERE students_raw IS NOT NULL AND students_raw != ''
    """, c)
    notes = pd.read_sql_query("""
        SELECT lesson_id, note_completed, notes_text, note_score
        FROM lesson_notes
    """, c)
    c.close()

    lessons["lesson_date"] = pd.to_datetime(lessons["lesson_date"])

    # Split group lessons
    rows = []
    for _, r in lessons.iterrows():
        names = [n.strip() for n in re.split(r',\s*', str(r["students_raw"])) if n.strip()]
        for name in names:
            rows.append(dict(
                lesson_id=r["lesson_id"], school_id=r["school_id"],
                instructor_id=r["instructor_id"],
                lesson_date=r["lesson_date"], lesson_time=r["lesson_time"],
                lesson_type=r["lesson_type"],
                student_name=name,
            ))
    return pd.DataFrame(rows), notes


# ── FEATURE BUILDING ─────────────────────────────────────────────────────

def build_features(lessons, notes, ref_date):
    """Build features using a 60-day window ending at ref_date."""
    window_start = pd.Timestamp(ref_date) - timedelta(days=WINDOW_DAYS)
    window_end = pd.Timestamp(ref_date)

    # Note aggregation
    note_agg = notes.groupby("lesson_id").agg(
        note_score=("note_score", "max"),
        note_completed=("note_completed", "max"),
    ).reset_index()

    feature_rows = []
    for name, g in lessons.groupby("student_name"):
        g = g.sort_values("lesson_date")
        dt = g["lesson_date"]
        total_all = len(g)
        first = dt.min()
        last = dt.max()
        school = int(g["school_id"].mode().iloc[0]) if not g["school_id"].empty else 0

        # Only include students with activity in this window
        win = g[(dt >= window_start) & (dt <= window_end)]
        n_win = len(win)
        if n_win < 1:
            continue  # no activity in window → can't learn from them

        # Attendance
        weekly = []
        for d in pd.date_range(window_start, window_end, freq="W"):
            weekly.append(int(((dt >= d) & (dt < d + timedelta(days=7))).sum()))
        trend = np.polyfit(range(len(weekly)), weekly, 1)[0] if len(weekly) >= 2 else 0.0
        avg_weekly = np.mean(weekly) if weekly else 0.0

        # Teacher consistency
        win_instructors = win["instructor_id"].dropna()
        if len(win_instructors) > 0:
            teacher_consistency = win_instructors.value_counts().iloc[0] / n_win
        else:
            teacher_consistency = 0

        # Notes
        win_with_notes = win.merge(note_agg, on="lesson_id", how="left")
        ns = win_with_notes["note_score"].dropna()
        nc = win_with_notes["note_completed"].dropna()
        avg_note_score = ns.mean() if len(ns) > 0 else 0.0
        note_completion_rate = nc.mean() if len(nc) > 0 else 0.0

        # Seasonality
        month = first.month if pd.notna(first) else 0

        feature_rows.append(dict(
            student_name=name, school_id=school,
            n_lessons_window=n_win, total_lessons_all=total_all,
            avg_weekly=avg_weekly, weekly_trend=trend,
            teacher_consistency=teacher_consistency,
            avg_note_score=avg_note_score,
            note_completion_rate=note_completion_rate,
            month_of_year=month,
            last_lesson=last, first_lesson=first,
        ))

    return pd.DataFrame(feature_rows)


# ── LABELING ─────────────────────────────────────────────────────────────

def label_students(features_df, lessons, today):
    """Label: churned = no lesson in [today-90, today]; active = lesson in [today-60, today].
    The 90-day gap means features are from [today-180, today-90] — behavior BEFORE the label window."""
    today_ts = pd.Timestamp(today)

    df = features_df.copy()
    labels = []
    for _, row in df.iterrows():
        nm = row["student_name"]
        student_lessons = lessons[lessons["student_name"] == nm]
        if len(student_lessons) < 5:
            labels.append(-1)
            continue

        last_date = student_lessons["lesson_date"].max()
        days_since = (today_ts - last_date).days

        if days_since >= GAP_DAYS:
            labels.append(1)  # churned — no lesson in 90 days
        elif days_since <= 60:
            labels.append(0)  # active — recent lesson
        else:
            labels.append(-1)  # gray zone (61-89 days)

    df["label"] = labels
    return df


# ── TRAINING ─────────────────────────────────────────────────────────────

def train_model(df):
    labeled = df[df["label"] >= 0].copy()
    X = labeled[FEATURE_COLS].fillna(0)
    y = labeled["label"].astype(int)

    n_labeled = len(labeled)
    n_churned = int(y.sum())
    n_active = n_labeled - n_churned

    print(f"  Training set: {len(df)} total, {n_labeled} labeled")
    print(f"  Churned: {n_churned}  Active: {n_active}  ({n_churned/(n_labeled or 1):.1%})")

    if n_churned < 5 or n_active < 10:
        print("  ⚠️  Not enough examples")
        return None, None, None

    Xt, Xv, yt, yv = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
    sc = StandardScaler()
    Xts, Xvs = sc.fit_transform(Xt), sc.transform(Xv)

    model = LogisticRegression(
        penalty="l2", C=0.5, solver="liblinear",
        class_weight="balanced", random_state=42, max_iter=1000
    )
    model.fit(Xts, yt)
    yp = model.predict_proba(Xvs)[:, 1]
    yp_class = model.predict(Xvs)

    auc = roc_auc_score(yv, yp)
    cv = cross_val_score(model, Xts, yt, cv=3, scoring="roc_auc")

    print(f"  Model: Logistic Regression (L2, C=0.5, balanced)")
    print(f"  AUC: {auc:.3f}  CV: {cv.mean():.3f} ± {cv.std():.3f}")
    print(f"\n{classification_report(yv, yp_class, target_names=['Active', 'Churned'])}")

    # Show coefficients (interpretable!)
    coefs = pd.DataFrame({
        "feature": FEATURE_COLS,
        "coefficient": model.coef_[0],
        "abs_coef": np.abs(model.coef_[0]),
    }).sort_values("abs_coef", ascending=False)
    print("  Feature coefficients (+ = increases churn risk):")
    for _, row in coefs.iterrows():
        direction = "↑ risk" if row["coefficient"] > 0 else "↓ risk"
        print(f"    {row['feature']:<30s} {row['coefficient']:+.4f}  ({direction})")

    return model, sc


# ── PREDICTION ───────────────────────────────────────────────────────────

def predict_and_rank(model, scaler, df, today):
    X = df[FEATURE_COLS].fillna(0)
    active_mask = df["total_lessons_all"] >= 5
    X_active = X[active_mask]
    df_active = df[active_mask].copy()

    if model is not None and scaler is not None:
        Xs = scaler.transform(X_active)
        scores = model.predict_proba(Xs)[:, 1]
    else:
        scores = np.full(len(df_active), 0.5)

    out = df_active[["student_name", "school_id", "last_lesson",
                      "avg_weekly", "weekly_trend",
                      "teacher_consistency", "avg_note_score",
                      "note_completion_rate", "month_of_year"]].copy()
    out["churn_risk"] = scores

    today_dt = pd.Timestamp(today)
    out["days_idle_now"] = out["last_lesson"].apply(
        lambda x: (today_dt - pd.Timestamp(x)).days if pd.notna(x) else 365).astype(int)

    # Only score currently active students
    out = out[out["days_idle_now"] <= 60].copy()
    out = out.sort_values("churn_risk", ascending=False)

    return out


# ── DISPLAY ──────────────────────────────────────────────────────────────

SCHOOL_NAMES = {1: "West University Place", 2: "The Heights"}

def explain(row):
    """Simple rule-based explanation from 6 features."""
    trend = float(row.get("weekly_trend", 0))
    avg_wk = float(row.get("avg_weekly", 0))
    tchr = float(row.get("teacher_consistency", 0))
    note = float(row.get("avg_note_score", 0))
    note_comp = float(row.get("note_completion_rate", 0))
    idle = int(row.get("days_idle_now", 0))

    reasons, actions = [], []

    if trend < -0.2:
        reasons.append(f"Lesson frequency dropping ({trend:+.1f}/week)")
        actions.append("Suggest more convenient time slot")
    elif trend < -0.1:
        reasons.append(f"Attendance slowly declining ({trend:+.1f}/week)")
        actions.append("Monitor; check in if trend continues")

    if avg_wk < 0.5 and trend <= 0:
        reasons.append(f"Very low attendance ({avg_wk:.1f}/week)")
        actions.append("Reach out — student may have quietly stopped")

    if tchr < 0.4:
        reasons.append(f"Only {tchr:.0%} of lessons with same instructor")
        actions.append("Lock in consistent teacher assignment")

    if note > 0 and note < 3.0:
        reasons.append(f"Note quality {note:.1f}/5")
        actions.append("Review with instructor")

    if note_comp < 0.5:
        reasons.append(f"Notes incomplete ({note_comp:.0%})")
        actions.append("Complete notes; ensure parent feedback")

    if idle > 30:
        reasons.append(f"{idle}d since last lesson")
        actions.append("Send check-in text")

    if not reasons:
        reasons.append("Patterns stable; low concern")
        actions.append("Continue monitoring")

    return "; ".join(reasons[:3]), "; ".join(actions[:2])


def print_report(risks, top=10):
    for sid, sname in sorted(SCHOOL_NAMES.items()):
        sub = risks[risks["school_id"] == sid].head(top)
        if sub.empty: continue
        n_total = len(risks[risks["school_id"] == sid])
        print(f"\n{'='*70}")
        print(f"  {sname} — Top {len(sub)} of {n_total} active")
        print(f"{'='*70}")
        for i, (_, r) in enumerate(sub.iterrows(), 1):
            w, d = explain(r)
            print(f"\n  {i:2d}. {r['student_name']}")
            print(f"      Risk: {r['churn_risk']:.0%}  |  {int(r['days_idle_now'])}d idle"
                  f"  |  {r['avg_weekly']:.1f}/wk  |  trend {r['weekly_trend']:+.2f}")
            print(f"      Why: {w}")
            print(f"      Do:  {d}")


# ── MAIN ─────────────────────────────────────────────────────────────────

def main():
    today = date.today()
    print(f"Today: {today}")
    print(f"Gap: {GAP_DAYS}d  |  Feature window: {WINDOW_DAYS}d  |  Model: Logistic Regression\n")

    print("[1] Load data...")
    lessons, notes = load()
    print(f"    {len(lessons):,} student-records, {len(notes):,} notes")

    print(f"[2] Build training features (window ending {today - timedelta(days=GAP_DAYS)})...")
    train_ref = today - timedelta(days=GAP_DAYS)
    train_features = build_features(lessons, notes, train_ref)
    train_labeled = label_students(train_features, lessons, today)

    print("[3] Train model...")
    model, scaler = train_model(train_labeled)

    if model is None:
        print("  Not enough labeled data. Exiting.")
        return

    print(f"\n[4] Predict on current students (window ending today)...")
    current_features = build_features(lessons, notes, today)
    risks = predict_and_rank(model, scaler, current_features, today)

    # Save
    with open(f"{MDIR}/churn_model.pkl", "wb") as f:
        pickle.dump(dict(
            model=model, scaler=scaler, feature_cols=FEATURE_COLS,
            trained_at=today.isoformat(), gap_days=GAP_DAYS, window_days=WINDOW_DAYS
        ), f)
    risks.to_csv(f"{MDIR}/churn_risk_scores.csv", index=False)

    active_count = len(risks)
    flagged = (risks["churn_risk"] > 0.3).sum()
    print(f"\n  Active scored: {active_count}")
    print(f"  Flagged (>30%): {flagged}")
    print(f"  Saved → models/")

    print_report(risks)


if __name__ == "__main__":
    main()
