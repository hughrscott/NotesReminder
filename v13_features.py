#!/usr/bin/env python3
"""v13_features.py — Shared feature engineering for the v13 churn model.

This module is imported by BOTH churn_model_v13_build.py (training) and
score_v13.py (production scoring) so the feature logic is guaranteed identical.
Do not fork the logic — change it here only.

Feature window: [ref_date - 60d, ref_date].
All features are person-specific (no global leakage).
"""
import sqlite3, re, unicodedata
from datetime import date, datetime, timedelta
from collections import defaultdict
import numpy as np

DB_PATH = None  # set by caller via set_db(path)
_conn = None

KEYWORDS = {
    "cancellation": ["cancel","stop lessons","stopping lessons","not coming back","last lesson",
        "final lesson","quit","quitting","drop out","dropping out","no longer","discontinue",
        "end lessons","not continue","pulling him out","pulling her out","pulling them out",
        "won't be attending","not going to continue"],
    "positive": ["great","love","thank","awesome","excited","happy","wonderful","fantastic","amazing","appreciate"],
    "frustration": ["not happy","disappointed","frustrated","unacceptable","ridiculous","fed up",
        "upset","problem","issue with","complaint","not satisfied","waste of time","waste of money","broken"],
}
FEATURES = ["avg_note_score","membership_days","total_lessons_lifetime","teacher_consistency",
            "has_communication","communication_count","days_since_last_comm",
            "has_cancellation","has_positive","has_frustration"]

def set_db(path):
    global DB_PATH, _conn
    DB_PATH = path
    _conn = sqlite3.connect(str(path))

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

def kw_flags(text):
    t = (text or "").lower()
    return {
        "has_cancellation": int(any(x in t for x in KEYWORDS["cancellation"])),
        "has_positive": int(any(x in t for x in KEYWORDS["positive"])),
        "has_frustration": int(any(x in t for x in KEYWORDS["frustration"])),
    }

def build_phone2sid(conn):
    """phone (last 10 digits) -> student_id, via pike13_clients name -> students name bridge."""
    cur = conn.cursor()
    name2sid = {}
    cur.execute("SELECT student_id, student_name FROM students")
    for sid, sname in cur.fetchall():
        if not sname: continue
        n = norm(sname)
        name2sid.setdefault(n, sid)
        parts = n.split()
        if len(parts) >= 2:
            name2sid.setdefault(parts[0]+" "+parts[-1], sid)
    phone2sid = {}
    cur.execute('SELECT "Client", "Phone", "Mobile Phone" FROM pike13_clients')
    for name, ph, mob in cur.fetchall():
        for raw in (ph, mob):
            d = digits(raw)
            if len(d) >= 10:
                sid = name2sid.get(norm(name))
                if sid: phone2sid[d[-10:]] = sid
    return phone2sid

def lesson_features(conn, sid, ref_date):
    cur = conn.cursor()
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

def comm_features(conn, phone2sid, sid, ref_date):
    cur = conn.cursor()
    lo = (ref_date - timedelta(days=60)).isoformat()
    hi = ref_date.isoformat()
    texts = []
    last_dates = []
    for ph10 in set(v for v in phone2sid.values() if v == sid):
        cur.execute("SELECT transcription_text, date FROM dialpad_voicemails WHERE external_number LIKE ?", (f"%{ph10}",))
        for txt, dt in cur.fetchall():
            d = parse_d(dt) if dt else None
            if d and lo <= d.isoformat() <= hi:
                texts.append(txt or ""); last_dates.append(d)
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

def featurize(conn, phone2sid, sid, ref_date):
    """Return ordered feature vector (FIXED order == FEATURES) for one student."""
    lf = lesson_features(conn, sid, ref_date)
    cf = comm_features(conn, phone2sid, sid, ref_date)
    row = {**lf, **cf}
    return [float(row[f]) for f in FEATURES], row
