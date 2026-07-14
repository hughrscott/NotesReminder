#!/usr/bin/env python3
"""
SOR Churn Report — Two-Tier System (v8 model + heuristic rules).

Tier 1 (Heuristic — act immediately):
  - attendance_ratio < 0.5 (lessons collapsing)
  - avg_note_score < 3.0 AND > 0 (instructor disengagement)
  - idle > 14 days (absentee flag)

Tier 2 (Model — human review):
  - v8 churn_risk > 50%

Output: Combined report showing what each tier catches, with overlap analysis.
"""

import os, sqlite3, re
from datetime import date, timedelta
import pandas as pd
import numpy as np

DB = "reminders.db"
MDIR = "models"

def S(x):
    if pd.isna(x) or x is None: return ""
    return str(x).strip().lower()

def load_data():
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

def compute_features(lessons, notes, today):
    """Compute features for all active students using today as ref_date."""
    today_ts = pd.Timestamp(today)
    ws = today_ts - timedelta(days=60)
    we = today_ts

    note_agg = notes.groupby("lesson_id").agg(
        note_score=("note_score", "max"),
        note_completed=("note_completed", "max"),
    ).reset_index()

    rows = []
    for name, g in lessons.groupby("student_name"):
        if len(g) < 5:
            continue
        last = g["lesson_date"].max()
        days_idle = (today_ts - last).days
        if days_idle > 60:
            continue

        g = g.sort_values("lesson_date")
        dt = g["lesson_date"]
        win = g[(dt >= ws) & (dt <= we)]
        n_win = len(win)

        if n_win == 0:
            continue

        # Attendance ratio: recent 14d / prior 45d
        recent = win[win["lesson_date"] >= (we - timedelta(days=14))]
        prior = win[win["lesson_date"] < (we - timedelta(days=14))]
        n_recent = len(recent)
        n_prior = len(prior)
        ratio = n_recent / max(n_prior, 1) if n_prior > 0 else (n_recent if n_recent > 0 else 0.01)
        freq_14d = n_recent / 2.0

        # Teacher
        win_inst = win["instructor_id"].dropna()
        teacher_consistency = win_inst.value_counts().iloc[0] / n_win if len(win_inst) > 0 else 0
        n_instructors = win_inst.nunique()

        # Notes
        win_notes = win.merge(note_agg, on="lesson_id", how="left")
        ns = win_notes["note_score"].dropna()
        nc = win_notes["note_completed"].dropna()
        avg_note_score = ns.mean() if len(ns) > 0 else 0.0
        note_completion_rate = nc.mean() if len(nc) > 0 else 0.0

        school = int(g["school_id"].mode().iloc[0]) if not g["school_id"].empty else 0
        first = g["lesson_date"].min()
        month = first.month if pd.notna(first) else 0

        rows.append(dict(
            student_name=name, school_id=school,
            last_lesson=last, days_idle=days_idle,
            attendance_ratio=ratio, freq_14d=freq_14d,
            lessons_per_week_14d=freq_14d,  # alias for model
            teacher_consistency=teacher_consistency,
            n_instructors=n_instructors,
            avg_note_score=avg_note_score,
            note_completion_rate=note_completion_rate,
            month_of_year=month,
            n_lessons_window=n_win,
        ))
    return pd.DataFrame(rows)

def apply_heuristics(df):
    """Tier 1: rule-based flags (act immediately).

    Thresholds tuned for operational reality:
    - Summer break in July makes attendance_ratio < 0.5 meaningless
    - Only flag when BOTH ratio is low AND student has been idle
    """
    flags = []

    # Rule 1: attendance collapsing AND idle (not just summer break)
    # ratio < 0.3 = severe drop, AND they've been gone for at least a week
    collapsing = (df["attendance_ratio"] < 0.3) & (df["days_idle"] > 0)
    flags.append(("⚠️  Severe attendance drop", collapsing,
                  "Call parent — urgent retention check"))

    # Rule 2: moderate decline with absence
    # Ratio 0.3-0.7 AND idle > 14 days = quietly fading
    fading = (df["attendance_ratio"] >= 0.3) & (df["attendance_ratio"] < 0.7) & (df["days_idle"] > 14)
    flags.append(("📉 Fading attendance", fading,
                  "Send text: 'Noticed a change — everything OK?'"))

    # Rule 3: instructor disengagement — notes < 3.0 and present
    disengaged = (df["avg_note_score"] > 0) & (df["avg_note_score"] < 3.0) & (df["days_idle"] <= 21)
    flags.append(("📝 Low note quality", disengaged,
                  "Review with instructor; offer guidance"))

    # Rule 4: extended absence with no recent activity
    gone = (df["days_idle"] > 28) & (df["attendance_ratio"] < 0.5)
    flags.append(("⏰ Extended absence >28d", gone,
                  "Verify they haven't quit; send re-engagement offer"))

    return flags

def run_report():
    today = date.today()
    print(f"╔══════════════════════════════════════════════════════════════╗")
    print(f"║  SOR Churn Report — Two-Tier System                        ║")
    print(f"║  {today}                                            ║")
    print(f"╚══════════════════════════════════════════════════════════════╝\n")

    # Load data
    lessons, notes = load_data()
    df = compute_features(lessons, notes, today)
    n_total = len(df)
    print(f"Active students (5+ lessons, ≤60d idle): {n_total}\n")

    # Load model scores if available
    model_scores = None
    try:
        import pickle
        with open(f"{MDIR}/churn_model.pkl", "rb") as f:
            saved = pickle.load(f)
        model = saved["model"]
        scaler = saved["scaler"]
        feature_cols = saved["feature_cols"]
        from sklearn.preprocessing import StandardScaler
        X = df[feature_cols].fillna(0)
        Xs = scaler.transform(X)
        model_scores = model.predict_proba(Xs)[:, 1]
        df["model_risk"] = model_scores
    except Exception as e:
        print(f"(Model not available: {e})")
        df["model_risk"] = 0.0

    # Apply heuristics
    flags = apply_heuristics(df)

    # Build combined flags
    df["tier1_reasons"] = ""
    df["tier1_count"] = 0
    for label, mask, action in flags:
        df.loc[mask, "tier1_reasons"] += label + "; "
        df.loc[mask, "tier1_count"] += 1
    df["tier1_flag"] = df["tier1_count"] > 0
    df["tier2_flag"] = df["model_risk"] > 0.50
    df["any_flag"] = df["tier1_flag"] | df["tier2_flag"]

    # Stats
    tier1 = df["tier1_flag"].sum()
    tier2 = df["tier2_flag"].sum()
    both = (df["tier1_flag"] & df["tier2_flag"]).sum()
    tier1_only = tier1 - both
    tier2_only = tier2 - both

    print("─── Coverage ───")
    print(f"  Tier 1 (heuristic):     {tier1:4d} ({tier1/n_total*100:5.1f}%)  → act immediately")
    print(f"  Tier 2 (model >50%):    {tier2:4d} ({tier2/n_total*100:5.1f}%)  → human review")
    print(f"  Both:                   {both:4d}  — strongest signal")
    print(f"  Tier 1 only:            {tier1_only:4d}  — heuristic caught, model missed")
    print(f"  Tier 2 only:            {tier2_only:4d}  — model caught, heuristic missed")
    print(f"  Any flag:               {df['any_flag'].sum():4d} ({df['any_flag'].sum()/n_total*100:5.1f}%)")
    print(f"  Clean (no flags):       {(~df['any_flag']).sum():4d}")

    # Show heuristic rules breakdown
    print(f"\n─── Heuristic Rule Breakdown ───")
    for label, mask, action in flags:
        n = mask.sum()
        print(f"  {label:<35s} {n:4d} students → {action}")

    # Top Tier 1 (heuristic only, worst offenders)
    print(f"\n{'='*80}")
    print(f"  TIER 1 — Act Immediately ({tier1_only} heuristic-only, sorted by severity)")
    print(f"{'='*80}")
    tier1_df = df[df["tier1_flag"]].sort_values(
        ["tier1_count", "attendance_ratio", "days_idle"],
        ascending=[False, True, False]
    )
    shown = 0
    for _, r in tier1_df.iterrows():
        total_flags = int(r["tier1_count"])
        reasons = r["tier1_reasons"].rstrip("; ")
        if total_flags >= 2:
            shown += 1
            model_risk = r.get("model_risk", 0)
            print(f"\n  🔴 {r['student_name']}  |  {int(r['days_idle'])}d idle"
                  f"  |  ratio {r['attendance_ratio']:.2f}"
                  f"  |  notes {r['avg_note_score']:.1f}/5"
                  f"  |  model {model_risk:.0%}")
            print(f"     Flags ({total_flags}): {reasons}")
            if shown >= 15:
                break

    # Top Tier 2 only (model caught, heuristic missed — the interesting ones)
    print(f"\n{'='*80}")
    print(f"  TIER 2 — Human Review ({tier2_only} model-only flags)")
    print(f"  These are the subtle cases the heuristics miss.")
    print(f"{'='*80}")
    tier2_only_df = df[df["tier2_flag"] & ~df["tier1_flag"]].sort_values(
        "model_risk", ascending=False
    )
    for i, (_, r) in enumerate(tier2_only_df.iterrows()):
        if i >= 15:
            break
        print(f"\n  🟡 {r['student_name']}  |  risk {r['model_risk']:.0%}"
              f"  |  {int(r['days_idle'])}d idle"
              f"  |  ratio {r['attendance_ratio']:.2f}"
              f"  |  {r['freq_14d']:.1f}/wk"
              f"  |  notes {r['avg_note_score']:.1f}/5"
              f"  |  {int(r['n_instructors'])} instructors"
              f"  |  consistency {r['teacher_consistency']:.0%}")

    # Summary table by school
    print(f"\n{'='*80}")
    print(f"  BY SCHOOL")
    print(f"{'='*80}")
    schools = {1: "West University Place", 2: "The Heights"}
    for sid, sname in schools.items():
        sdf = df[df["school_id"] == sid]
        s_total = len(sdf)
        s_t1 = sdf["tier1_flag"].sum()
        s_t2 = sdf["tier2_flag"].sum()
        s_any = sdf["any_flag"].sum()
        print(f"\n  {sname}:")
        print(f"    Active: {s_total}  |  Tier 1: {s_t1}  |  Tier 2: {s_t2}  |  Any: {s_any}")

    # Save
    out = df[["student_name", "school_id", "days_idle", "attendance_ratio",
              "freq_14d", "avg_note_score", "note_completion_rate",
              "teacher_consistency", "n_instructors",
              "model_risk", "tier1_count", "tier1_flag", "tier2_flag",
              "tier1_reasons"]].copy()
    out.to_csv(f"{MDIR}/churn_report.csv", index=False)
    print(f"\n  Saved → {MDIR}/churn_report.csv")

if __name__ == "__main__":
    run_report()
