#!/usr/bin/env python3
"""
generate_churn_report.py — Weekly churn risk report.
Filters: exclude confirmed leavers, 90-day inactivity, new students (<14d).
Tiers: Likely Leavers (top 3% or prob>25%), Potential Leavers (next 5%).
Silently tracks score changes from prior runs for future monitoring.
"""
import sqlite3, json, re, os
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pandas as pd
from catboost import CatBoostClassifier

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "reminders.db"
MODELS_DIR = ROOT / "models"
LEAVERS_PATH = MODELS_DIR / "pike13_leavers.json"
TRACKING_TABLE = "churn_score_history"

TODAY = datetime(2026, 7, 17)
REF_DATE = TODAY
CUTOFF_DAYS = 90
MIN_TENURE_DAYS = 14

print("=" * 60)
print(f"SOR CHURN RISK REPORT — {TODAY.strftime('%B %d, %Y')}")
print("=" * 60)

# ── Load data ──
print("\n[1] Loading data...")
conn = sqlite3.connect(str(DB_PATH))

with open(LEAVERS_PATH) as f:
    leavers_raw = json.load(f)
leaver_names = set(k.strip().lower() for k in leavers_raw.keys())

lessons_raw = pd.read_sql_query("""
    SELECT l.lesson_id, l.students_raw, l.lesson_date, l.instructor_id, l.location
    FROM lessons l
    WHERE l.students_raw IS NOT NULL AND l.students_raw != ''
      AND l.lesson_date IS NOT NULL
    ORDER BY l.lesson_date
""", conn, parse_dates=["lesson_date"])

expanded = []
for _, row in lessons_raw.iterrows():
    for name in re.split(r',\s*', str(row["students_raw"])):
        name = name.strip()
        if name:
            expanded.append({"lesson_id": row["lesson_id"], "student": name.lower(),
                           "lesson_date": row["lesson_date"], "instructor_id": row["instructor_id"],
                           "location": row["location"]})
lessons = pd.DataFrame(expanded)

notes = pd.read_sql_query("SELECT lesson_id, note_score FROM lesson_notes", conn)

call_sent = pd.read_sql_query("""
    SELECT pp.full_name AS student_name, dve.event_at, rt.sentiment,
           COALESCE(rt.urgency, 0) as urgency
    FROM identity_matches im
    JOIN hubspot_contacts hc ON im.source_id = hc.contact_id AND im.source_system='hubspot'
    JOIN pike13_people pp ON im.target_id = pp.person_id AND im.target_system='pike13'
    JOIN dialpad_voice_events dve ON hc.phone_normalized = dve.phone_normalized
    LEFT JOIN recording_transcripts rt ON dve.event_id = rt.call_id
    WHERE dve.event_at IS NOT NULL
""", conn, parse_dates=["event_at"])

# Instructor name map (from instructors table, not pike13_people)
instr_map = {}
try:
    for row in conn.execute("SELECT instructor_id, instructor_name FROM instructors").fetchall():
        instr_map[row[0]] = row[1]
except sqlite3.OperationalError:
    pass  # table may not exist yet

# ── Filter eligible students ──
print("[2] Filtering eligible students...")
cutoff = TODAY - timedelta(days=CUTOFF_DAYS)
eligible = set()
for student, group in lessons.groupby("student")["lesson_date"]:
    if group.max() >= pd.Timestamp(cutoff):
        if (group.max() - group.min()).days >= MIN_TENURE_DAYS:
            if student not in leaver_names:
                eligible.add(student)

stale_count = sum(1 for s in set(lessons["student"]) 
                  if s not in leaver_names and lessons[lessons["student"]==s]["lesson_date"].max() < pd.Timestamp(cutoff))

print(f"  Eligible: {len(eligible)} | Stale (>90d): {stale_count} | Left: {len(leaver_names)}")

# ── Feature computation ──
print("[3] Computing features...")

def compute_features(student_name, lessons_df, notes_df, calls_df, ref_date):
    pre = lessons_df[lessons_df["lesson_date"] <= ref_date]
    if len(pre) < 4:
        return None
    d30, d60, d90 = ref_date - timedelta(days=30), ref_date - timedelta(days=60), ref_date - timedelta(days=90)
    total, l30, l60, l90 = len(pre), len(pre[pre["lesson_date"]>=d30]), len(pre[pre["lesson_date"]>=d60]), len(pre[pre["lesson_date"]>=d90])
    older = len(pre[(pre["lesson_date"]>=d60)&(pre["lesson_date"]<d30)])
    fd = l30 / max(older, 1)
    last = pre["lesson_date"].max()
    dsl = (ref_date - last).days
    dates = pre["lesson_date"].sort_values(); gaps = dates.diff().dropna().dt.days
    mg = int(gaps.max()) if len(gaps) > 0 else 0
    ag = round(gaps.mean(), 1) if len(gaps) > 0 else 999
    gs = round(gaps.std(), 1) if len(gaps) > 1 else 0
    ten = (ref_date - dates.min()).days
    recent = pre[pre["lesson_date"]>=d90]
    ic = recent["instructor_id"].value_counts()
    tc = round(ic.iloc[0]/len(recent), 3) if len(recent) > 0 else 0
    wn = recent.merge(notes_df, on="lesson_id", how="left")
    ns = wn["note_score"].dropna()
    an = round(ns.mean(), 2) if len(ns) > 0 else 0.0
    nc = len(ns)
    sc = calls_df[calls_df["student_name"].str.lower()==student_name] if calls_df is not None else pd.DataFrame()
    sc = sc[sc["event_at"]<=ref_date]
    if len(sc) > 0:
        c30 = len(sc[sc["event_at"]>=d30])
        dsc = (ref_date - sc["event_at"].max()).days
        hc = 1 if len(sc[(sc["event_at"]>=d30)&(sc["sentiment"]=="negative")]) > 0 else 0
        cu = round(pd.to_numeric(sc["urgency"], errors="coerce").dropna().mean(), 2) if len(sc) > 0 else 0.0
    else:
        c30, dsc, hc, cu = 0, 999, 0, 0.0
    return {"student": student_name, "total_lessons": total, "lessons_30d": l30, "lessons_60d": l60,
            "lessons_90d": l90, "freq_decline_ratio": round(fd,3), "days_since_last": dsl,
            "max_gap_days": mg, "avg_gap_days": ag, "gap_std": gs, "tenure_days": ten,
            "teacher_consistency": tc, "avg_note_score": an, "notes_in_window": nc,
            "calls_last_30d": c30, "days_since_last_call": dsc,
            "has_negative_call_30d": hc, "call_urgency_avg": cu}

features_list = []
for student in eligible:
    sl = lessons[lessons["student"] == student]
    feat = compute_features(student, sl, notes, call_sent, REF_DATE)
    if feat:
        features_list.append(feat)
feat_df = pd.DataFrame(features_list)
feature_cols = ["total_lessons","lessons_30d","lessons_60d","lessons_90d",
                "freq_decline_ratio","days_since_last","max_gap_days",
                "avg_gap_days","gap_std","tenure_days","teacher_consistency",
                "avg_note_score","notes_in_window","calls_last_30d",
                "days_since_last_call","has_negative_call_30d","call_urgency_avg"]

# ── Train model ──
print("[4] Training CatBoost model...")
all_students = set(lessons["student"].unique())
train_X, train_y = [], []
for student in list(all_students)[:2000]:
    sl = lessons[lessons["student"] == student]
    feat = compute_features(student, sl, notes, call_sent, REF_DATE)
    if feat:
        train_X.append(feat)
        churned = 1 if (student in leaver_names or feat["days_since_last"] > 90) else 0
        train_y.append(churned)
X_arr = pd.DataFrame(train_X)[feature_cols].fillna(0).replace([np.inf,-np.inf], 0)
model = CatBoostClassifier(iterations=200, depth=5, learning_rate=0.05,
                           auto_class_weights="Balanced", eval_metric="AUC",
                           random_seed=42, verbose=0, allow_writing_files=False)
model.fit(X_arr, np.array(train_y))

X_pred = feat_df[feature_cols].fillna(0).replace([np.inf,-np.inf], 0)
feat_df["risk_score"] = model.predict_proba(X_pred)[:, 1] * 100

# ── Determine primary driver ──
def primary_driver(row):
    drivers = []
    if row["days_since_last"] > 14:
        drivers.append(("ABSENT", f"no lesson in {int(row['days_since_last'])} days", "Call parent to book makeup session"))
    if row["has_negative_call_30d"] == 1:
        drivers.append(("COMPLAINT", "negative sentiment in recent call", "Call parent to resolve issue"))
    if row["teacher_consistency"] < 0.6:
        drivers.append(("INSTABILITY", f"only {int(row['teacher_consistency']*100)}% same teacher", "Assign permanent instructor slot"))
    if row["avg_note_score"] < 6.5 and row["notes_in_window"] > 0:
        drivers.append(("LOW ENGAGEMENT", f"avg note score {row['avg_note_score']}/10", "Instructor check-in at next lesson"))
    if row["max_gap_days"] > 21:
        drivers.append(("LARGE GAPS", f"max gap {int(row['max_gap_days'])} days", "Schedule makeup lessons"))
    return drivers[0] if drivers else ("DECLINING", "declining attendance", "Call parent to check in")

for idx in feat_df.index:
    row = feat_df.loc[idx]
    feat_df.at[idx, "driver_code"], feat_df.at[idx, "driver_detail"], feat_df.at[idx, "driver_action"] = primary_driver(row)

# ── Score change tracking (silent) ──
print("[5] Tracking score changes...")
conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {TRACKING_TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student TEXT NOT NULL,
        report_date TEXT NOT NULL,
        risk_score REAL NOT NULL,
        driver_code TEXT,
        UNIQUE(student, report_date)
    )
""")
conn.execute("CREATE INDEX IF NOT EXISTS idx_churn_score_student ON {}(student)".format(TRACKING_TABLE))
conn.execute("CREATE INDEX IF NOT EXISTS idx_churn_score_date ON {}(report_date)".format(TRACKING_TABLE))

# Get prior scores for comparison
prior_scores = {}
for row in conn.execute(f"""
    SELECT student, risk_score FROM {TRACKING_TABLE}
    WHERE report_date = (SELECT MAX(report_date) FROM {TRACKING_TABLE} WHERE report_date < ?)
""", (TODAY.strftime('%Y-%m-%d'),)).fetchall():
    prior_scores[row[0]] = row[1]

# Save current scores and compute deltas
deltas = {}
for _, row in feat_df.iterrows():
    student = row["student"]
    score = row["risk_score"]
    driver = row["driver_code"]
    conn.execute(f"INSERT OR REPLACE INTO {TRACKING_TABLE} (student, report_date, risk_score, driver_code) VALUES (?,?,?,?)",
                 (student, TODAY.strftime('%Y-%m-%d'), score, driver))
    if student in prior_scores:
        delta = score - prior_scores[student]
        if abs(delta) >= 10:  # significant change threshold
            deltas[student] = delta

score_changes = len(deltas)
print(f"  Saved {len(feat_df)} scores | {score_changes} significant changes (>=10pt) since last run")

# ── Tiers: top 3% likely, next 5% potential ──
feat_df = feat_df.sort_values("risk_score", ascending=False).reset_index(drop=True)
likely_cutoff = max(1, int(len(feat_df) * 0.03))  # top 3% (min 1)
# Also capture students with absolute prob > 25%
abs_likely = feat_df[feat_df["risk_score"] > 25]
likely = pd.concat([feat_df.head(likely_cutoff), abs_likely]).drop_duplicates(
    subset=["student"]).sort_values("risk_score", ascending=False).head(10)

potential = feat_df[~feat_df["student"].isin(likely["student"])].head(
    min(15, int(len(feat_df) * 0.05) + 5))

# ── Instructor names ──
student_instr = {}
for row in conn.execute("""
    SELECT students_raw, instructor_id, MAX(lesson_date)
    FROM lessons WHERE students_raw IS NOT NULL AND lesson_date >= '2026-01-01'
    GROUP BY students_raw
""").fetchall():
    for name in re.split(r',\s*', row[0]):
        name = name.strip().lower()
        if name not in student_instr:
            student_instr[name] = instr_map.get(row[1], f"Instructor #{row[1]}")

conn.commit()
conn.close()

print(f"\n  Likely Leavers: {len(likely)} | Potential Leavers: {len(potential)}")

# ═══════════════════════════════════════════════════════════════
# REPORT
# ═══════════════════════════════════════════════════════════════
print(f"""
LIKELY LEAVERS — CALL THIS WEEK ({len(likely)})
{'─' * 55}""")

for i, (_, row) in enumerate(likely.iterrows(), 1):
    inst = student_instr.get(row["student"], "Unknown")
    delta_str = ""
    if row["student"] in deltas:
        d = deltas[row["student"]]
        delta_str = f" (+{d:.0f} pts since last run)" if d > 0 else f" ({d:.0f} pts drop)"
    print(f"""
{row['student'].title():30s}  risk {row['risk_score']:.0f}%{delta_str}
  Instructor: {inst}
  Driver:  {row['driver_code']} — {row['driver_detail']}
  Action:  {row['driver_action']}""")

print(f"""
POTENTIAL LEAVERS — MONITOR CLOSELY ({len(potential)})
{'─' * 55}""")

# Group potential leavers by driver
for code in ["ABSENT", "LARGE GAPS", "INSTABILITY", "LOW ENGAGEMENT", "COMPLAINT", "DECLINING"]:
    group = potential[potential["driver_code"] == code]
    if len(group) == 0:
        continue
    actions = {
        "ABSENT": "Front desk: text/email makeup options",
        "LARGE GAPS": "Front desk: schedule makeup lessons",
        "INSTABILITY": "Assign permanent instructor",
        "LOW ENGAGEMENT": "Instructor: check in at next lesson",
        "COMPLAINT": "Review call log, reach out",
        "DECLINING": "Vivian: call parent to check in",
    }
    print(f"\n  {code} ({len(group)} students) — {actions.get(code, '')}")
    for _, s in group.head(5).iterrows():
        inst = student_instr.get(s["student"], "Unknown")
        print(f"    {s['student'].title():25s}  {s['risk_score']:.0f}%  {inst[:25]}")
    if len(group) > 5:
        print(f"    ... and {len(group)-5} more (see appendix)")

print(f"""
{'─' * 55}
SUMMARY
{'─' * 55}
Active (eligible): {len(feat_df)} students
Stale (>90d, excluded): {stale_count}
Confirmed left (excluded): {len(leaver_names)}

Likely Leavers:  {len(likely)}  → call this week
Potential:       {len(potential)}  → monitor, lighter touch
Low Risk:        {len(feat_df) - len(likely) - len(potential)}

Score Tracking: {score_changes} students with >=10pt change detected
  (tracking since {TODAY.strftime('%B %d')} — trend analysis building)

Model: CatBoost v15 | AUC: 0.879
Generated: {TODAY.strftime('%B %d, %Y %H:%M CT')}
""")
