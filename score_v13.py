#!/usr/bin/env python3
"""score_v13.py — Score the active roster with the v13 churn model (ground-truth leavers).

Produces models/v13_risk_scores.csv with one row per student who has lesson history:
  student_id, student_name, school_id, v13_risk, v13_tier,
  ref_date, n_lessons_window, (raw features...)

The model + fitted scaler are loaded from v13_model.pkl (must contain a dict
{"model","scaler","features"}). If not, retrain-on-score is NOT done here; we
rely on churn_model_v13_build.py having produced a valid pkl.
"""
import sqlite3, pickle, json
from pathlib import Path
from datetime import date, timedelta
import numpy as np
import pandas as pd

import v13_features as vf

HERE = Path(__file__).parent
DB = HERE / "reminders.db"
MODELS = HERE / "models"
PKL = MODELS / "v13_model.pkl"
OUT = MODELS / "v13_risk_scores.csv"

def main():
    vf.set_db(DB)
    conn = sqlite3.connect(str(DB))

    with open(PKL, "rb") as f:
        bundle = pickle.load(f)
    if isinstance(bundle, dict):
        model = bundle["model"]; scaler = bundle["scaler"]; feats = bundle["features"]
    else:
        raise RuntimeError("v13_model.pkl missing scaler bundle — rerun churn_model_v13_build.py")

    phone2sid = vf.build_phone2sid(conn)
    print(f"[score] phone->student mappings: {len(phone2sid)}")

    # Scoring universe = students with a lesson in the last 90 days.
    # v13 was trained to separate actively-enrolled from recently-churned; scoring
    # long-idle students (already gone) produces garbage "critical" flags.
    cur = conn.cursor()
    cutoff = (date.today() - timedelta(days=90)).isoformat()
    cur.execute("""SELECT DISTINCT ls.student_id, s.student_name, s.school_id FROM lesson_students ls
                   JOIN lessons l ON ls.lesson_id=l.lesson_id
                   JOIN students s ON ls.student_id=s.student_id
                   WHERE l.lesson_date >= ? AND s.student_name IS NOT NULL""", (cutoff,))
    rows = cur.fetchall()
    print(f"[score] scoring universe: {len(rows)} students")

    today = date.today()
    out = []
    for sid, name, school in rows:
        try:
            vec, raw = vf.featurize(conn, phone2sid, sid, today)
        except Exception as e:
            continue
        if raw.get("n_win", 0) == 0 and raw.get("total_lessons_lifetime", 0) == 0:
            continue
        X = np.array([vec], dtype=float)
        p = float(model.predict_proba(scaler.transform(X))[:, 1][0])
        tier = "critical" if p >= 0.70 else ("high" if p >= 0.50 else ("watch" if p >= 0.30 else "low"))
        out.append({
            "student_id": sid, "student_name": name, "school_id": school,
            "v13_risk": round(p, 4), "v13_tier": tier, "ref_date": today.isoformat(),
            **{f: raw[f] for f in vf.FEATURES},
        })

    df = pd.DataFrame(out)
    df.to_csv(OUT, index=False)
    print(f"[score] wrote {OUT} ({len(df)} scored students)")
    # tier distribution
    print(df["v13_tier"].value_counts().to_dict())
    conn.close()

if __name__ == "__main__":
    main()
