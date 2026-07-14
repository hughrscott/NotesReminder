#!/usr/bin/env python3
"""
SOR Churn Model v8 — Fixed Buffer Leakage + Reduced Collinearity.

Gemini v7 critique: 7-day buffer after last_lesson injects a zero-activity
tail that the model cheats on. Fixes:
  1. No buffer — ref_date = last_lesson exactly. Window = [last-60, last].
  2. Drop collinear stats — replace avg_weekly/std_weekly/weekly_trend
     with one ratio: lessons_last_14d / lessons_prior_45d (<1 = declining).
  3. Tighter L2 — C=0.05 to penalize coefficient inflation.
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
    "attendance_ratio",      # lessons_last_14d / lessons_prior_45d (<1 = declining)
    "lessons_per_week_14d",  # raw frequency in recent fortnight
    "teacher_consistency",
    "avg_note_score",
    "note_completion_rate",
    "month_of_year",
]


def load():
    c = sqlite3.connect(DB)
    lessons = pd.read_sql_query("""
        SELECT lesson_id, school_id, instructor_id, lesson_date, lesson_time,
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
                lesson_date=r["lesson_date"], lesson_time=r["lesson_time"],
                lesson_type=r["lesson_type"],
                student_name=name,
            ))
    return pd.DataFrame(rows), notes


def build_features_for_student(name, all_lessons, s_ref, notes):
    """Features for one student, ref_date = last_lesson (no buffer)."""
    g = all_lessons[all_lessons["student_name"] == name].sort_values("lesson_date")
    dt = g["lesson_date"]
    total_all = len(g)
    first = dt.min()
    last = dt.max()
    school = int(g["school_id"].mode().iloc[0]) if not g["school_id"].empty else 0

    ws = s_ref - timedelta(days=60)
    we = s_ref
    win = g[(dt >= ws) & (dt <= we)]
    n_win = len(win)
    if n_win < 1:
        return None

    # Simple declining ratio — recent 14d vs prior 45d
    recent = win[win["lesson_date"] >= (we - timedelta(days=14))]
    prior = win[win["lesson_date"] < (we - timedelta(days=14))]
    n_recent = len(recent)
    n_prior = len(prior)
    # Ratio: <1.0 = declining, >1.0 = increasing, 0 = stopped
    attendance_ratio = n_recent / max(n_prior, 1) if n_prior > 0 else (n_recent if n_recent > 0 else 0.01)
    lessons_per_week_14d = n_recent / 2.0  # ~weeks in 14 days

    # Teacher
    win_inst = win["instructor_id"].dropna()
    teacher_consistency = win_inst.value_counts().iloc[0] / n_win if len(win_inst) > 0 else 0

    # Notes
    note_agg = notes.groupby("lesson_id").agg(
        note_score=("note_score", "max"),
        note_completed=("note_completed", "max"),
    ).reset_index()
    win_notes = win.merge(note_agg, on="lesson_id", how="left")
    ns = win_notes["note_score"].dropna()
    nc = win_notes["note_completed"].dropna()
    avg_note_score = ns.mean() if len(ns) > 0 else 0.0
    note_completion_rate = nc.mean() if len(nc) > 0 else 0.0
    month = first.month if pd.notna(first) else 0

    return dict(
        student_name=name, school_id=school,
        n_lessons_window=n_win, total_lessons_all=total_all,
        attendance_ratio=attendance_ratio,
        lessons_per_week_14d=lessons_per_week_14d,
        teacher_consistency=teacher_consistency,
        avg_note_score=avg_note_score,
        note_completion_rate=note_completion_rate,
        month_of_year=month,
        last_lesson=last, first_lesson=first,
    )


def build_training_features(lessons, notes, today):
    """Per-student ref_dates. Churned: last_lesson (no buffer). Active: today."""
    today_ts = pd.Timestamp(today)
    student_info = {}
    for name, g in lessons.groupby("student_name"):
        if len(g) < 5:
            continue
        last = g["lesson_date"].max()
        days_since = (today_ts - last).days
        if days_since >= 90:
            status = "churned"
            ref = last  # NO buffer — exactly at last lesson
        elif days_since <= 60:
            status = "active"
            ref = today_ts
        else:
            status = "gray"
            ref = None
        student_info[name] = {"status": status, "ref": ref}

    rows = []
    for name, info in student_info.items():
        if info["status"] == "gray" or info["ref"] is None:
            continue
        feat = build_features_for_student(name, lessons, info["ref"], notes)
        if feat is None:
            continue
        feat["label"] = 1 if info["status"] == "churned" else 0
        rows.append(feat)
    return pd.DataFrame(rows)


def build_prediction_features(lessons, notes, today):
    today_ts = pd.Timestamp(today)
    rows = []
    for name, g in lessons.groupby("student_name"):
        if len(g) < 5:
            continue
        last = g["lesson_date"].max()
        if (today_ts - last).days > 60:
            continue
        feat = build_features_for_student(name, lessons, today_ts, notes)
        if feat is None:
            continue
        rows.append(feat)
    return pd.DataFrame(rows)


def train_model(df):
    labeled = df[df["label"] >= 0].copy()
    X = labeled[FEATURE_COLS].fillna(0)
    y = labeled["label"].astype(int)

    n_labeled = len(labeled)
    n_churned = int(y.sum())
    n_active = n_labeled - n_churned
    print(f"  Training: {n_labeled} labeled ({n_churned} churned, {n_active} active, "
          f"{n_churned/max(n_labeled,1):.1%} churn rate)")

    Xt, Xv, yt, yv = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
    sc = StandardScaler()
    Xts, Xvs = sc.fit_transform(Xt), sc.transform(Xv)

    model = LogisticRegression(
        penalty="l2", C=0.05, solver="liblinear",
        class_weight="balanced", random_state=42, max_iter=2000
    )
    model.fit(Xts, yt)
    yp = model.predict_proba(Xvs)[:, 1]
    yp_class = model.predict(Xvs)

    auc = roc_auc_score(yv, yp)
    cv = cross_val_score(model, Xts, yt, cv=5, scoring="roc_auc")

    print(f"  Model: Logistic Regression (L2, C=0.05, balanced, 5-fold CV)")
    print(f"  AUC: {auc:.3f}  CV: {cv.mean():.3f} ± {cv.std():.3f}")
    print(f"\n{classification_report(yv, yp_class, target_names=['Active', 'Churned'])}")

    coefs = pd.DataFrame({
        "feature": FEATURE_COLS,
        "coefficient": model.coef_[0],
        "abs_coef": np.abs(model.coef_[0]),
    }).sort_values("abs_coef", ascending=False)
    print("  Coefficients (+ = ↑ churn risk):")
    for _, row in coefs.iterrows():
        d = "↑ risk" if row["coefficient"] > 0 else "↓ risk"
        print(f"    {row['feature']:<30s} {row['coefficient']:+.4f}  ({d})")

    return model, sc


def predict_and_rank(model, scaler, df, today):
    X = df[FEATURE_COLS].fillna(0)
    Xs = scaler.transform(X) if scaler is not None else X
    scores = model.predict_proba(Xs)[:, 1] if model is not None else np.full(len(df), 0.5)

    out = df[["student_name", "school_id", "last_lesson",
              "attendance_ratio", "lessons_per_week_14d",
              "teacher_consistency", "avg_note_score",
              "note_completion_rate", "month_of_year"]].copy()
    out["churn_risk"] = scores
    today_dt = pd.Timestamp(today)
    out["days_idle_now"] = out["last_lesson"].apply(
        lambda x: (today_dt - pd.Timestamp(x)).days if pd.notna(x) else 365).astype(int)
    out = out.sort_values("churn_risk", ascending=False)
    return out


def explain(row):
    ratio = float(row.get("attendance_ratio", 1))
    freq = float(row.get("lessons_per_week_14d", 1))
    tchr = float(row.get("teacher_consistency", 0))
    note = float(row.get("avg_note_score", 0))
    note_comp = float(row.get("note_completion_rate", 0))
    idle = int(row.get("days_idle_now", 0))

    reasons, actions = [], []
    if ratio < 0.5:
        reasons.append(f"Attendance collapsing (ratio {ratio:.1f})")
        actions.append("Call parent — urgent retention check")
    elif ratio < 0.8:
        reasons.append(f"Attendance declining (ratio {ratio:.1f})")
        actions.append("Check in; suggest alternative time")
    if freq < 0.5:
        reasons.append(f"Low frequency ({freq:.1f}/wk)")
        actions.append("Reach out — may have quietly stopped")
    if tchr < 0.4:
        reasons.append(f"Only {tchr:.0%} same instructor")
        actions.append("Lock in consistent teacher")
    if note > 0 and note < 3.0:
        reasons.append(f"Note quality {note:.1f}/5")
        actions.append("Review with instructor")
    if note_comp < 0.5:
        reasons.append(f"Notes incomplete ({note_comp:.0%})")
        actions.append("Complete notes; parent feedback")
    if idle > 30:
        reasons.append(f"{idle}d idle")
        actions.append("Send check-in text")
    if not reasons:
        reasons.append("Patterns stable")
        actions.append("Continue monitoring")
    return "; ".join(reasons[:3]), "; ".join(actions[:2])


SCHOOL_NAMES = {1: "West University Place", 2: "The Heights"}


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
                  f"  |  ratio {r['attendance_ratio']:.2f}"
                  f"  |  {r['lessons_per_week_14d']:.1f}/wk")
            print(f"      Why: {w}")
            print(f"      Do:  {d}")


def main():
    today = date.today()
    print(f"Today: {today}  |  No buffer  |  Attendance ratio  |  C=0.05  |  Logistic Regression\n")

    print("[1] Load data...")
    lessons, notes = load()
    print(f"    {len(lessons):,} student-records, {len(notes):,} notes")

    print("[2] Build training set (ref=last_lesson, no buffer)...")
    train_df = build_training_features(lessons, notes, today)

    print("[3] Train model...")
    model, scaler = train_model(train_df)

    if model is None:
        print("  Not enough data.")
        return

    print("\n[4] Predict on current students...")
    pred_df = build_prediction_features(lessons, notes, today)
    risks = predict_and_rank(model, scaler, pred_df, today)

    with open(f"{MDIR}/churn_model.pkl", "wb") as f:
        pickle.dump(dict(
            model=model, scaler=scaler, feature_cols=FEATURE_COLS,
            trained_at=today.isoformat(),
        ), f)
    risks.to_csv(f"{MDIR}/churn_risk_scores.csv", index=False)

    n = len(risks)
    flagged = int((risks["churn_risk"] > 0.3).sum())
    print(f"\n  Active scored: {n}")
    print(f"  Flagged (>30%): {flagged} ({flagged/max(n,1)*100:.0f}%)")
    print(f"  Saved → models/")

    print_report(risks)


if __name__ == "__main__":
    main()
