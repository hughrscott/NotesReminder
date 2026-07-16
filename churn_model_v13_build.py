#!/usr/bin/env python3
"""
churn_model_v13_build.py — v13 churn model on Pike13 ground-truth leavers.

LABELS (from the two Person_Plan CSVs):
  Each Client ID's last_membership_end = max(End Date) across all their plans.
  CHURNER if last_end is in the past (>=21d ago, exit-behavior leakage guard).
  ACTIVE (negative) if last_end is recent OR the student is currently enrolled.

NEGATIVE CLASS (the missing piece in the first pass): currently-active students.
  We pull local students who have lessons in the LAST 60d (i.e. clearly active)
  and are NOT in the Pike13 churner set. Their ref_date = TODAY, label = 0.

FEATURES — ALL person-specific (no global leakage):
  From lessons/notes in [ref_date-60, ref_date]:
    avg_note_score, total_lessons_lifetime, teacher_consistency
  From person-matched comms (phone->student name->student_id) in window:
    has_communication, communication_count, days_since_last_comm
    has_cancellation, has_positive, has_frustration (keyword flags)
  membership_days = days from first lesson to ref_date (tenure)

COMMS MATCHING:
  comms_name_matches.json: phone -> student name.  We map name -> student_id
  via the same name2sid bridge, then pull that student's voicemails/SMS by
  joining phone digits.  To keep it simple and correct, we precompute
  phone->student_id from pike13_clients (Phone/Mobile) + students name bridge.
"""
import sqlite3, csv, re, unicodedata, json, warnings
from pathlib import Path
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.metrics import roc_auc_score, classification_report, confusion_matrix

DB = "/home/ubuntu/projects/hughrscott/NotesReminder/reminders.db"
CSV_DIR = Path("/home/ubuntu/pike13_csvs")
MATCHES = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models/comms_name_matches.json")
TODAY = date(2026, 7, 16)
GAP = 21  # exit-behavior leakage guard

def norm(s):
    if not s: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode()
    s = re.sub(r"[^a-z0-9 ]"," ", s.lower()).strip()
    return re.sub(r"\s+"," ", s)

def parse_d(s):
    try: return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except: return None

def digits(s):
    return re.sub(r"\D","", s or "")

# ───────────────────────── 1. Load Pike13 plan CSVs → churn labels ────────
plan_files = sorted(CSV_DIR.glob("*person_plan*Plans*Details*.csv"))
plan_rows = []
for pf in plan_files:
    with open(pf, newline="", encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f):
            plan_rows.append(r)
print(f"[load] plan rows: {len(plan_rows)} from {len(plan_files)} files")

from collections import defaultdict
by_client = defaultdict(list)
for r in plan_rows:
    cid = (r.get("Client ID") or "").strip()
    by_client[cid].append({
        "name": (r.get("Client") or "").strip(),
        "end": (r.get("End Date") or "").strip(),
        "canceled": (r.get("Canceled?") or "").strip().lower()=="yes",
    })

churners = {}   # cid -> last_end date
churner_names = set()
for cid, plans in by_client.items():
    ends = [d for d in (parse_d(p["end"]) for p in plans) if d]
    if not ends: continue
    last_end = max(ends)
    if last_end < (TODAY - timedelta(days=GAP)):
        churners[cid] = last_end
        churner_names.add(norm(plans[0]["name"]))
print(f"[derive] Pike13 churners (last end >{GAP}d ago): {len(churners)}")

# ───────────────────────── 2. Name bridge → local students ────────────────
conn = sqlite3.connect(DB); cur = conn.cursor()
cur.execute("SELECT student_id, student_name, school_id FROM students")
name2sid = {}
for sid, sname, sch in cur.fetchall():
    if not sname: continue
    n = norm(sname)
    name2sid.setdefault(n, sid)
    parts = n.split()
    if len(parts) >= 2:
        name2sid.setdefault(parts[0]+" "+parts[-1], sid)

# phone -> student_id (via pike13_clients name -> student)
cur.execute('SELECT "Client", "Phone", "Mobile Phone" FROM pike13_clients')
phone2sid = {}
for name, ph, mob in cur.fetchall():
    for raw in (ph, mob):
        d = digits(raw)
        if len(d) >= 10:
            sid = name2sid.get(norm(name)) or (name2sid.get((norm(name).split()[0]+" "+norm(name).split()[-1])) if len(norm(name).split())>=2 else None)
            if sid: phone2sid[d[-10:]] = sid

# ───────────────────────── 3. Active students (negatives) ─────────────────
cur.execute("""
    SELECT DISTINCT ls.student_id FROM lesson_students ls
    JOIN lessons l ON ls.lesson_id=l.lesson_id
    WHERE l.lesson_date >= ?
""", ((TODAY - timedelta(days=60)).isoformat(),))
active_sids = {r[0] for r in cur.fetchall()}
print(f"[active] students with a lesson in last 60d: {len(active_sids)}")

# ───────────────────────── 4. Feature engineering ─────────────────────────
KEYWORDS = {
    "cancellation": ["cancel","stop lessons","stopping lessons","not coming back","last lesson",
        "final lesson","quit","quitting","drop out","dropping out","no longer","discontinue",
        "end lessons","not continue","pulling him out","pulling her out","pulling them out",
        "won't be attending","not going to continue"],
    "positive": ["great","love","thank","awesome","excited","happy","wonderful","fantastic","amazing","appreciate"],
    "frustration": ["not happy","disappointed","frustrated","unacceptable","ridiculous","fed up",
        "upset","problem","issue with","complaint","not satisfied","waste of time","waste of money","broken"],
}
def kw_flags(text):
    t = (text or "").lower()
    return {
        "has_cancellation": int(any(x in t for x in KEYWORDS["cancellation"])),
        "has_positive": int(any(x in t for x in KEYWORDS["positive"])),
        "has_frustration": int(any(x in t for x in KEYWORDS["frustration"])),
    }

def lesson_features(sid, ref_date):
    lo = (ref_date - timedelta(days=60)).isoformat()
    hi = ref_date.isoformat()
    cur.execute("""SELECT l.lesson_date, l.instructor_id FROM lesson_students ls
                   JOIN lessons l ON ls.lesson_id=l.lesson_id
                   WHERE ls.student_id=? AND l.lesson_date BETWEEN ? AND ?""", (sid, lo, hi))
    lrows = cur.fetchall()
    n_win = len(lrows)
    instrs = [r[1] for r in lrows if r[1]]
    teacher_consistency = (len(set(instrs))/len(instrs)) if instrs else 0.0
    cur.execute("""SELECT COUNT(*) FROM lesson_students ls JOIN lessons l ON ls.lesson_id=l.lesson_id
                   WHERE ls.student_id=? AND l.lesson_date <= ?""", (sid, hi))
    total_lifetime = cur.fetchone()[0]
    cur.execute("""SELECT ln.note_score FROM lesson_students ls JOIN lessons l ON ls.lesson_id=l.lesson_id
                   JOIN lesson_notes ln ON ln.lesson_id=l.lesson_id
                   WHERE ls.student_id=? AND l.lesson_date BETWEEN ? AND ? AND ln.note_score IS NOT NULL""", (sid, lo, hi))
    scores = [r[0] for r in cur.fetchall()]
    avg_note = float(np.mean(scores)) if scores else 0.0
    cur.execute("""SELECT MIN(l.lesson_date) FROM lesson_students ls JOIN lessons l ON ls.lesson_id=l.lesson_id
                   WHERE ls.student_id=?""", (sid,))
    first = cur.fetchone()[0]
    fd = parse_d(first) if first else None
    membership_days = (ref_date - fd).days if fd else 0
    return dict(n_win=n_win, teacher_consistency=teacher_consistency,
                total_lessons_lifetime=total_lifetime, avg_note_score=avg_note,
                membership_days=max(membership_days,0))

def comm_features(sid, ref_date):
    lo = (ref_date - timedelta(days=60)).isoformat()
    hi = ref_date.isoformat()
    # gather texts for this student's phones
    texts = []
    last_dates = []
    for ph10 in set(phone2sid[k] for k in phone2sid if phone2sid[k]==sid):
        # voicemails
        cur.execute("SELECT transcription_text, date FROM dialpad_voicemails WHERE external_number LIKE ?", (f"%{ph10}",))
        for txt, dt in cur.fetchall():
            d = parse_d(dt) if dt else None
            if d and lo <= d.isoformat() <= hi:
                texts.append(txt or ""); last_dates.append(d)
        # sms
        cur.execute("""SELECT sm.body, sm.message_at FROM dialpad_sms_messages sm
                       JOIN dialpad_sms_threads th ON sm.thread_id=th.thread_id
                       WHERE th.phone_normalized LIKE ?""", (f"%{ph10}",))
        for txt, dt in cur.fetchall():
            d = parse_d(dt) if dt else None
            if d and lo <= d.isoformat() <= hi:
                texts.append(txt or ""); last_dates.append(d)
    count = len(texts)
    if last_dates:
        last_comm = max(last_dates)
        ds = (ref_date - last_comm).days
    else:
        ds = 999
    kf = {"has_cancellation":0,"has_positive":0,"has_frustration":0}
    for t in texts:
        f = kw_flags(t)
        for k in kf: kf[k]=max(kf[k], f[k])
    return dict(has_communication=int(count>0), communication_count=count,
                days_since_last_comm=ds, **kf)

# ───────────────────────── 5. Assemble dataset ───────────────────────────
records = []
# churners
for cid, last_end in churners.items():
    sid = None
    # find name from plan rows
    nm = by_client[cid][0]["name"]
    sid = name2sid.get(norm(nm)) or name2sid.get((norm(nm).split()[0]+" "+norm(nm).split()[-1]) if len(norm(nm).split())>=2 else norm(nm))
    if sid is None: continue
    lf = lesson_features(sid, last_end)
    cf = comm_features(sid, last_end)
    if lf["n_win"] == 0 and lf["total_lessons_lifetime"] == 0:
        continue  # no lesson history at all — can't featurize
    records.append({"key":cid, "name":nm, "label":1, "ref":last_end.isoformat(),
                    **lf, **cf, "_n_win":lf["n_win"]})
# actives (negatives) — must NOT be a known churner
act_added = 0
for sid in active_sids:
    records.append(None)  # placeholder to keep loop; real add below
records = [r for r in records if r is not None]
for sid in active_sids:
    # skip if this student is a known churner (by name)
    cur.execute("SELECT student_name FROM students WHERE student_id=?", (sid,))
    nm = cur.fetchone()
    nm = norm(nm[0]) if nm and nm[0] else ""
    if nm in churner_names:
        continue
    lf = lesson_features(sid, TODAY)
    cf = comm_features(sid, TODAY)
    records.append({"key":f"active_{sid}", "name":nm, "label":0, "ref":TODAY.isoformat(),
                    **lf, **cf, "_n_win":lf["n_win"]})
    act_added += 1
print(f"[assemble] churn records: {sum(r['label'] for r in records)} | active records: {act_added}")

df = pd.DataFrame(records)
FEATURES = ["avg_note_score","membership_days","total_lessons_lifetime","teacher_consistency",
            "has_communication","communication_count","days_since_last_comm",
            "has_cancellation","has_positive","has_frustration"]
df = df.dropna(subset=FEATURES)
print(f"[assemble] final usable rows: {len(df)} (churn={int(df['label'].sum())}, active={len(df)-int(df['label'].sum())})")

# ───────────────────────── 6. Train + validate ────────────────────────────
print("\n[train] Stratified 5-fold CV AUC:")
X = df[FEATURES].values; y = df["label"].values
Xs = StandardScaler().fit_transform(X)
clf = LogisticRegression(max_iter=2000, class_weight="balanced")
cv = StratifiedKFold(5, shuffle=True, random_state=42)
aucs = cross_val_score(clf, Xs, y, cv=cv, scoring="roc_auc")
print(f"  CV AUC: {aucs.mean():.3f} +/- {aucs.std():.3f}")

clf.fit(Xs, y)
coefs = pd.DataFrame({"feature":FEATURES,"coef":clf.coef_[0],"abs":np.abs(clf.coef_[0])}).sort_values("abs",ascending=False)
print("\n[coefs]  (expected: avg_note_score<0, teacher_consistency<0, days_since_last_comm>0, has_cancellation>0)")
print(coefs.to_string(index=False))

Xtr,Xte,ytr,yte = train_test_split(Xs,y,test_size=0.25,stratify=y,random_state=42)
clf2 = LogisticRegression(max_iter=2000,class_weight="balanced").fit(Xtr,ytr)
p = clf2.predict_proba(Xte)[:,1]
print(f"\n[holdout] AUC={roc_auc_score(yte,p):.3f}")
print(classification_report(yte,(p>0.5).astype(int),digits=3))
print("confusion:", confusion_matrix(yte,(p>0.5).astype(int)).tolist())

# save
OUT = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models")
df.to_csv(OUT/"v13_labeled_dataset.csv", index=False)
coefs.to_csv(OUT/"v13_coefficients.csv", index=False)
import pickle; pickle.dump(clf, open(OUT/"v13_model.pkl","wb"))
print(f"\n[saved] {OUT/'v13_labeled_dataset.csv'} | {OUT/'v13_coefficients.csv'} | {OUT/'v13_model.pkl'}")
conn.close(); print("DONE")
