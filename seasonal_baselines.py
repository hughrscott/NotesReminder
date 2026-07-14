#!/usr/bin/env python3
"""Compute seasonal baselines and add normalized attendance features to v9 churn model."""

import sqlite3, re, os, pickle
from datetime import date, timedelta
import pandas as pd
import numpy as np

DB = "reminders.db"
MDIR = "models"
os.makedirs(MDIR, exist_ok=True)

GAP_DAYS = 21


def compute_baselines():
    """Compute smoothed monthly attendance baselines per school.
    
    For months with 2 years of data (Jan-Jul): simple average.
    For months with 1 year (Aug-Dec): 70% raw month + 30% global average."""
    c = sqlite3.connect(DB)
    lessons = pd.read_sql_query("""
        SELECT lesson_id, school_id, lesson_date, students_raw
        FROM lessons WHERE students_raw IS NOT NULL AND students_raw != ''
    """, c)
    lessons["lesson_date"] = pd.to_datetime(lessons["lesson_date"])
    
    # Explode students
    rows = []
    for _, r in lessons.iterrows():
        names = [n.strip() for n in re.split(r',\s*', str(r["students_raw"])) if n.strip()]
        for name in names:
            rows.append(dict(
                school_id=int(r["school_id"]),
                lesson_date=r["lesson_date"],
                student_name=name,
            ))
    df = pd.DataFrame(rows)
    df["month"] = df["lesson_date"].dt.month
    df["year"] = df["lesson_date"].dt.year
    
    # Count unique students per school per month
    active = df.groupby(["school_id", "year", "month"])["student_name"].nunique().reset_index()
    active.columns = ["school_id", "year", "month", "active_students"]
    
    # Count lessons per school per month
    lesson_counts = df.groupby(["school_id", "year", "month"]).size().reset_index()
    lesson_counts.columns = ["school_id", "year", "month", "total_lessons"]
    
    merged = active.merge(lesson_counts, on=["school_id", "year", "month"])
    merged["lessons_per_student"] = merged["total_lessons"] / merged["active_students"]
    merged["weeks_in_month"] = 4.33  # average
    merged["lessons_per_student_per_week"] = merged["lessons_per_student"] / merged["weeks_in_month"]
    
    # Global average per school
    global_avg = merged.groupby("school_id")["lessons_per_student_per_week"].mean().to_dict()
    
    # Monthly baselines with smoothing
    baselines = {}
    for (school, month), group in merged.groupby(["school_id", "month"]):
        raw_avg = group["lessons_per_student_per_week"].mean()
        n_years = group["year"].nunique()
        
        if n_years >= 2:
            # Multiple years: simple average
            baselines[(school, month)] = float(raw_avg)
        else:
            # Single year: smooth with global average (70/30)
            global_av = global_avg.get(school, raw_avg)
            baselines[(school, month)] = float(0.70 * raw_avg + 0.30 * global_av)
    
    c.close()
    return baselines, merged


def print_baselines(baselines):
    """Print baseline table for review."""
    schools = {1: "West U", 2: "Heights"}
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    
    print(f"\n  {'Month':>5s}", end="")
    for sid in [1, 2]:
        print(f"  {schools[sid]:>8s}", end="")
    print()
    print("  " + "-" * 25)
    
    for m in range(1, 13):
        print(f"  {months[m-1]:>5s}", end="")
        for sid in [1, 2]:
            val = baselines.get((sid, m), 0)
            print(f"  {val:>8.2f}", end="")
        print()


def main():
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  Seasonal Baseline Computation                          ║")
    print("╚══════════════════════════════════════════════════════════╝")
    
    baselines, raw = compute_baselines()
    print(f"\n  Computed from {len(raw)} school-month observations across {raw['year'].nunique()} years")
    print_baselines(baselines)
    
    # Save baselines for churn model
    with open(f"{MDIR}/seasonal_baselines.pkl", "wb") as f:
        pickle.dump(baselines, f)
    print(f"\n  Saved → {MDIR}/seasonal_baselines.pkl")
    
    # Show seasonal pattern
    print(f"\n  Key insight:")
    for sid, sname in {1: "West U", 2: "Heights"}.items():
        vals = [baselines.get((sid, m), 0) for m in range(1, 13)]
        peak_m = vals.index(max(vals)) + 1
        trough_m = vals.index(min(vals)) + 1
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        print(f"  {sname}: peak in {months[peak_m-1]} ({max(vals):.2f}/wk), "
              f"trough in {months[trough_m-1]} ({min(vals):.2f}/wk)")


if __name__ == "__main__":
    main()
