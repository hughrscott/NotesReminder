#!/usr/bin/env python3
"""
SOR Churn Model v2 — splits group lessons, explains predictions per student,
groups output by school for GM dispatch.
"""
import os, sqlite3, pickle, warnings, re
from datetime import date, timedelta
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")
DB, MDIR = "reminders.db", "models"
os.makedirs(MDIR, exist_ok=True)

def S(x):
    if pd.isna(x) or x is None: return ""
    return str(x).strip().lower()
def P(x):
    return S(x).replace("+1","").replace("(","").replace(")","").replace("-","").replace(" ","")

# ── load + split groups ──

def load(cutoff=None):
    if cutoff is None: cutoff = date.today().isoformat()
    c = sqlite3.connect(DB)
    df = pd.read_sql_query("""
        SELECT lesson_id, school_id, lesson_date, lesson_time, lesson_type, location,
               students_raw, lesson_is_group, lesson_student_count, lesson_is_reportable
        FROM lessons WHERE students_raw IS NOT NULL AND students_raw != '' AND lesson_date <= ?
    """, c, params=[cutoff])
    people = pd.read_sql_query(
        "SELECT person_id, full_name, email_normalized, phone, membership_state, school FROM pike13_people", c)
    sms = pd.read_sql_query("SELECT * FROM dialpad_sms_messages", c)
    calls = pd.read_sql_query("SELECT * FROM dialpad_calls", c)
    emails = pd.read_sql_query(
        "SELECT message_id, external_email_normalized, direction, message_at, subject FROM school_email_messages WHERE message_at <= ?",
        c, params=[cutoff])
    c.close()

    df["lesson_date"] = pd.to_datetime(df["lesson_date"])
    if len(emails):
        emails["message_at"] = pd.to_datetime(emails["message_at"], format="mixed", utc=True)

    # Split group lessons into individual student rows
    rows = []
    for _, r in df.iterrows():
        names = [n.strip() for n in re.split(r',\s*', str(r["students_raw"])) if n.strip()]
        n_s = len(names) if names else 1
        per_count = float(r["lesson_student_count"] or 1) / n_s
        is_grp = 1 if n_s > 1 else int(r["lesson_is_group"] or 0)
        for name in names:
            rows.append(dict(
                lesson_id=r["lesson_id"], school_id=r["school_id"],
                lesson_date=r["lesson_date"], lesson_time=r["lesson_time"],
                lesson_type=r["lesson_type"], location=r["location"],
                student_name=name, is_group=is_grp,
                student_count=per_count, is_reportable=r["lesson_is_reportable"],
            ))
    lessons = pd.DataFrame(rows)
    return lessons, people, sms, calls, emails


# ── churn labels ──

def churn_labels(lessons, fc: date, churn_days=60):
    fc = pd.Timestamp(fc)
    a0 = fc - timedelta(days=30)
    c1 = fc + timedelta(days=churn_days)

    recs = []
    for nm, g in lessons.groupby("student_name"):
        g = g.sort_values("lesson_date")
        dt = g["lesson_date"]
        total = len(g)
        first = dt.min().date()
        last = dt.max().date()
        sch = int(g["school_id"].mode().iloc[0]) if not g["school_id"].empty else 0

        active_30 = int(((dt >= a0) & (dt < fc)).sum())
        after_60 = int(((dt >= fc) & (dt < c1)).sum())

        if active_30 > 0:
            churned = 1 if after_60 == 0 else 0
        else:
            churned = -1

        before = int((dt < fc).sum())
        before_g = g[dt < fc]
        ut = before_g["lesson_type"].nunique() if len(before_g) else 0
        gr = (before_g["is_group"].sum() / len(before_g)) if len(before_g) else 0.0

        last_before = dt[dt < fc].max().date() if (dt < fc).any() else None
        idle = (fc.date() - last_before).days if last_before else 365
        ten = (fc.date() - first).days if first else 0

        # windowed
        windows = []
        if first and first < fc.date():
            ws = first
            while ws < fc.date():
                we = ws + timedelta(days=30)
                windows.append(int(((dt.dt.date >= ws) & (dt.dt.date < we)).sum()))
                ws += timedelta(days=30)
        aw = np.mean(windows) if windows else 0.0
        sw = np.std(windows) if len(windows) > 1 else 0.0
        tr = np.polyfit(range(len(windows)), windows, 1)[0] if len(windows) >= 2 else 0.0
        wk = len(set(d.isocalendar()[0:2] for d in dt if d < fc))

        recs.append(dict(
            student_name=nm, school_id=sch, total_lessons=total,
            first_lesson=first, last_lesson=last,
            lessons_before_cutoff=before, active_30d_before=active_30,
            lessons_after_cutoff=after_60, churned=churned,
            days_since_last=idle, tenure_days=ten,
            unique_lesson_types=ut, group_lesson_ratio=gr,
            weeks_active=wk, avg_lessons_per_window=aw,
            std_lessons_per_window=sw, lesson_trend=tr,
        ))
    return pd.DataFrame(recs)


# ── comm features ──

def add_comm(df, people, sms, calls, emails, cutoff):
    fc = pd.Timestamp(cutoff, tz="UTC")
    nmap = {}
    for _, p in people.iterrows():
        n = S(p.get("full_name"))
        if n: nmap[n] = (S(p.get("email_normalized")), P(p.get("phone")))

    email_map = {}
    if len(emails) and "message_at" in emails.columns:
        pre = emails[pd.to_datetime(emails["message_at"], format="mixed", utc=True) < fc]
        for _, r in pre.iterrows():
            k = S(r.get("external_email_normalized"))
            if k:
                d = email_map.setdefault(k, {"t": 0, "i": 0})
                d["t"] += 1
                if str(r.get("direction", "")).lower() == "inbound": d["i"] += 1

    sms_p = {}
    if "phone_number" in sms.columns:
        for _, r in sms.iterrows():
            ph = P(r["phone_number"])
            if ph: sms_p[ph] = sms_p.get(ph, 0) + 1

    call_p = {}
    if "phone_number" in calls.columns:
        for _, r in calls.iterrows():
            ph = P(r["phone_number"])
            if ph: call_p[ph] = call_p.get(ph, 0) + 1

    out = []
    for _, s in df.iterrows():
        nm = S(s["student_name"])
        em, ph = nmap.get(nm, ("", ""))
        ed = email_map.get(em) or {}
        ec = ed.get("t", 0) if isinstance(ed, dict) else 0
        ei = ed.get("i", 0) if isinstance(ed, dict) else 0
        sc = sms_p.get(ph, 0)
        cc = call_p.get(ph, 0)
        out.append(dict(
            email_count=ec, email_inbound=ei, sms_count=sc, call_count=cc,
            total_communications=ec + sc + cc))
    return pd.concat([df.reset_index(drop=True), pd.DataFrame(out)], axis=1)


# ── train ──

def train(df):
    skip = {"student_name", "first_lesson", "last_lesson", "churned",
            "school_id", "lessons_after_cutoff", "active_30d_before"}
    cols = [c for c in df.columns if c not in skip and df[c].dtype in ("int64", "float64")]
    lab = df[df["churned"] >= 0].copy()
    X = lab[cols].fillna(0); y = lab["churned"].astype(int)
    n = len(df); nl = len(lab); nc = int(y.sum())
    print(f"  Students: {n} total, {nl} labeled, {nc} churned ({nc/nl:.1%})" if nl else "")
    if nc < 5:
        print("  Too few churned — heuristic fallback")
        return None, None, cols, None
    Xt, Xv, yt, yv = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
    sc = StandardScaler(); Xts, Xvs = sc.fit_transform(Xt), sc.transform(Xv)
    gb = GradientBoostingClassifier(n_estimators=100, max_depth=3, learning_rate=0.05, random_state=42)
    gb.fit(Xts, yt)
    yp = gb.predict_proba(Xvs)[:, 1]
    auc = roc_auc_score(yv, yp)
    cv = cross_val_score(gb, Xts, yt, cv=3, scoring="roc_auc")
    print(f"  AUC: {auc:.3f}  CV: {cv.mean():.3f} ± {cv.std():.3f}")
    imps = pd.DataFrame({"feature": cols, "importance": gb.feature_importances_})\
           .sort_values("importance", ascending=False)
    print("  Top features:")
    for _, row in imps.head(8).iterrows():
        print(f"    {row['feature']:<30s} {row['importance']:.4f}")
    return gb, sc, cols, imps


# ── explain ──

def _explain(row):
    idle = int(row.get("days_idle_now", row.get("days_since_last", 0)))
    trend = float(row.get("lesson_trend", 0))
    total = int(row.get("total_lessons", 0))
    avg = float(row.get("avg_lessons_per_window", 0))
    comms = int(row.get("total_communications", 0))
    group_r = float(row.get("group_lesson_ratio", 0))
    tenure = int(row.get("tenure_days", 0))

    reasons, actions = [], []

    if idle > 90:
        reasons.append(f"No lessons in {idle} days — likely already left")
        actions.append("Call parent to confirm status; consider win-back offer")
    elif idle > 45:
        reasons.append(f"Last lesson {idle} days ago — long gap")
        actions.append("Send re-engagement text: availability check")
    elif idle > 21:
        reasons.append(f"Gap of {idle} days since last lesson")
        actions.append("Check-in call: resolve scheduling issues")

    if trend < -0.3:
        reasons.append(f"Attendance declining sharply ({trend:+.1f}/window)")
        actions.append("Offer free trial of new instrument or group program")
    elif trend < -0.1:
        reasons.append(f"Attendance slightly declining ({trend:+.1f}/window)")
        actions.append("Ask if current time slot still works")

    if avg < 1.0 and total >= 4:
        reasons.append(f"Low frequency — {avg:.1f} lessons/month avg")
        actions.append("Suggest consistent weekly time slot")

    if total <= 4:
        reasons.append(f"New student — only {total} lessons")
        actions.append("Follow up on trial experience; gauge interest")

    if comms == 0 and idle > 30:
        reasons.append("No parent contact info on file")
        actions.append("Collect email/phone; verify parent can be reached")

    if group_r > 0.5 and trend < 0:
        reasons.append("Mostly in group lessons, attendance declining")
        actions.append("Offer 1-on-1 session to re-engage individually")

    if not reasons:
        reasons.append("Moderate risk based on attendance patterns")
        actions.append("Monitor 2 more weeks; escalate if no return")

    return "; ".join(reasons[:3]), "; ".join(actions[:2])


def score_and_explain(model, scaler, cols, df, today):
    X = df[cols].fillna(0)
    if scaler is not None: X = scaler.transform(X)
    if model is not None:
        scores = model.predict_proba(X)[:, 1]
    else:
        scores = (1.0 / (df["days_since_last"].clip(lower=1) / 7 + 1)
                  - df["lesson_trend"].clip(lower=-2, upper=0) * 0.4)
        s_min, s_max = scores.min(), scores.max()
        if s_max > s_min: scores = (scores - s_min) / (s_max - s_min)

    out = df[["student_name", "school_id", "total_lessons", "last_lesson",
              "days_since_last", "avg_lessons_per_window", "lesson_trend",
              "total_communications", "group_lesson_ratio", "tenure_days"]].copy()
    out["churn_risk"] = scores

    # Compute actual idle days (from today, not training cutoff)
    today_dt = pd.Timestamp(today)
    out["days_idle_now"] = out["last_lesson"].apply(
        lambda x: (today_dt.date() - pd.Timestamp(x).date()).days if pd.notna(x) else 365)
    out["days_idle_now"] = out["days_idle_now"].astype(int)

    whys, dos = [], []
    for _, r in out.iterrows():
        w, d = _explain(r)
        whys.append(w); dos.append(d)
    out["why_at_risk"] = whys
    out["recommended_action"] = dos
    return out.sort_values("churn_risk", ascending=False)


# ── display ──

SCHOOL_NAMES = {1: "West University Place", 2: "The Heights", 0: "Unknown"}

def print_by_school(risks, top=10):
    for sid, sname in sorted(SCHOOL_NAMES.items()):
        sub = risks[risks["school_id"] == sid]
        if sub.empty: continue
        print(f"\n{'='*70}")
        print(f"  {sname} (School {sid}) — Top {min(top, len(sub))}")
        print(f"{'='*70}")
        for i, (_, r) in enumerate(sub.head(top).iterrows(), 1):
            print(f"\n  {i:2d}. {r['student_name']}")
            print(f"      Risk: {r['churn_risk']:.0%}  |  {int(r['total_lessons'])} lessons"
                  f"  |  last: {str(r['last_lesson'])[:10]}  |  {int(r['days_idle_now'])}d idle")
            print(f"      Why: {r['why_at_risk']}")
            print(f"      Do:  {r['recommended_action']}")


# ── main ──

def main():
    today = date.today()
    tcut = today - timedelta(days=60)
    print(f"Cutoff: {tcut}  Today: {today}\n")

    print("[1] Load + split groups...")
    lessons, people, sms, calls, emails = load()
    print(f"    {len(lessons):,} student-records (group lessons split)")

    print("[2] Labels...")
    students = churn_labels(lessons, tcut)

    print("[3] Comm features...")
    full = add_comm(students, people, sms, calls, emails, tcut)

    print("[4] Train & score...")
    model, scaler, cols, imps = train(full)
    risks = score_and_explain(model, scaler, cols, full, today)
    with open(f"{MDIR}/churn_model.pkl", "wb") as f:
        pickle.dump(dict(model=model, scaler=scaler, feature_cols=cols,
                         importances=imps.to_dict("records") if imps is not None else [],
                         trained_at=today.isoformat(), cutoff=tcut.isoformat()), f)
    risks.to_csv(f"{MDIR}/churn_risk_scores.csv", index=False)
    print(f"  Saved → models/\n")

    print_by_school(risks, top=10)
    return model, risks


if __name__ == "__main__":
    main()
