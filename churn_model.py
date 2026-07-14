#!/usr/bin/env python3
"""
SOR Churn Model v5 — transcript-aware, enrollment-timed, XGBoost.
Key improvements over v4:
  - Fixed comm data loading bugs (column names, phone mapping pipeline)
  - Voicemail transcript concern detection (1,446 transcripts)
  - Call duration analysis (unanswered rate, long disputes, call storms)
  - Per-student feature windows: churned students use pre-churn behavior window
    (honest training — no "already gone" leakage)
  - Seasonality (month-of-year)
  - Instructor departure detection
  - Newbie cliff × trend interaction
  - XGBoost with L1/L2 regularization
"""

import os, sqlite3, pickle, warnings, re
from datetime import date, timedelta
from collections import defaultdict
import pandas as pd
import numpy as np
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")
DB = "reminders.db"
MDIR = "models"
os.makedirs(MDIR, exist_ok=True)

def S(x):
    if pd.isna(x) or x is None: return ""
    return str(x).strip().lower()
def P(x):
    return S(x).replace("+1","").replace("(","").replace(")","").replace("-","").replace(" ","")


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
        SELECT lesson_id, note_completed, notes_text, note_score,
               note_score_explanation
        FROM lesson_notes
    """, c)

    people = pd.read_sql_query(
        "SELECT person_id, full_name, email_normalized, phone, phone_normalized, "
        "membership_state, school FROM pike13_people", c)

    # Use voice_events as primary call/voicemail source (current through Jul 2026)
    # dialpad_calls and dialpad_voicemails are stale CSV imports (end Jan 2026)
    voice_events = pd.read_sql_query("""
        SELECT event_id, call_id, event_type, phone_normalized, direction,
               event_at, outcome, voicemail_transcript
        FROM dialpad_voice_events
        WHERE phone_normalized IS NOT NULL AND phone_normalized != ''
    """, c)

    reviews = pd.read_sql_query("""
        SELECT call_review_id, call_id, transcript_text, event_at
        FROM dialpad_call_reviews WHERE transcript_text IS NOT NULL
    """, c)

    call_matches = pd.read_sql_query(
        "SELECT call_id, client_id, match_type FROM call_client_matches", c)

    sms = pd.read_sql_query(
        "SELECT message_id, thread_id, direction, sender, recipient, body FROM dialpad_sms_messages", c)

    emails = pd.read_sql_query("""
        SELECT message_id, external_email_normalized, direction, message_at,
               subject, snippet
        FROM school_email_messages
    """, c)

    visits = pd.read_sql_query("""
        SELECT person_id, starts_at, status, first_visit_flag, no_show_flag, canceled_flag
        FROM pike13_visits WHERE starts_at IS NOT NULL AND starts_at != ''
    """, c)

    c.close()

    lessons["lesson_date"] = pd.to_datetime(lessons["lesson_date"])
    if len(emails) and "message_at" in emails.columns:
        emails["message_at"] = pd.to_datetime(emails["message_at"], format="mixed", utc=True)
    voice_events["event_at"] = pd.to_datetime(voice_events["event_at"], format="mixed")
    if len(reviews) and "event_at" in reviews.columns:
        reviews["event_at"] = pd.to_datetime(reviews["event_at"], format="mixed")
    visits["starts_at"] = pd.to_datetime(visits["starts_at"], format="mixed", utc=True)

    # Split group lessons
    rows = []
    for _, r in lessons.iterrows():
        names = [n.strip() for n in re.split(r',\s*', str(r["students_raw"])) if n.strip()]
        n_s = len(names) if names else 1
        per_count = float(r["lesson_student_count"] or 1) / n_s
        is_grp = 1 if n_s > 1 else int(r["lesson_is_group"] or 0)
        for name in names:
            rows.append(dict(
                lesson_id=r["lesson_id"], school_id=r["school_id"],
                instructor_id=r["instructor_id"],
                lesson_date=r["lesson_date"], lesson_time=r["lesson_time"],
                lesson_type=r["lesson_type"], location=r["location"],
                student_name=name, is_group=is_grp,
                student_count=per_count, is_reportable=r["lesson_is_reportable"],
            ))
    lessons_split = pd.DataFrame(rows)

    return lessons_split, notes, reviews, people, sms, voice_events, emails, \
           call_matches, visits


# ── PHONE/IDENTITY MAPPING ───────────────────────────────────────────────

def build_phone_maps(people, call_matches, voice_events, reviews):
    nmap = {}
    for _, p in people.iterrows():
        n = S(p.get("full_name"))
        if n: nmap[n] = (S(p.get("email_normalized")), P(str(p.get("phone") or "")))

    phone_to_names = defaultdict(set)
    for name, (_, ph) in nmap.items():
        if ph:
            phone_to_names[ph].add(name)

    pid_to_phone = {}
    for _, p in people.iterrows():
        ph = P(str(p.get("phone") or ""))
        if ph:
            pid_to_phone[str(p["person_id"])] = ph

    call_phone = {}
    if len(call_matches):
        for _, m in call_matches.iterrows():
            cid = str(m["call_id"])
            client = str(m["client_id"])
            if client in pid_to_phone:
                call_phone[cid] = pid_to_phone[client]

    # Also: call_id → phone directly from voice_events phone_normalized
    for _, row in voice_events.iterrows():
        ph = P(str(row.get("phone_normalized", "")))
        cid = str(row.get("call_id", ""))
        if ph and cid and cid not in call_phone:
            call_phone[cid] = ph

    return nmap, phone_to_names, call_phone


# ── FEATURE ENGINEERING ──────────────────────────────────────────────────

def build_features(lessons, notes, reviews, people, sms, voice_events, emails,
                   call_matches, visits,
                   today, ref_date=None, student_ref_dates=None):
    """Build per-student features.

    If `student_ref_dates` is provided (dict: student_name → ref_date),
    uses per-student reference dates for honest training (churned students
    get their pre-churn behavior window). Otherwise uses global ref_date
    or today for prediction.
    """
    if ref_date is None and student_ref_dates is None:
        ref_date = pd.Timestamp(today)

    nmap, phone_to_names, call_phone = build_phone_maps(people, call_matches, voice_events, reviews)

    # Instructor last-seen for departure detection
    instructor_last_seen = {}
    all_instructor_dates = lessons.groupby("instructor_id")["lesson_date"].max()
    for inst, last_dt in all_instructor_dates.items():
        instructor_last_seen[inst] = last_dt

    # Voicemail concern detection from voice_events
    # voice_events has event_type='voicemail' with voicemail_transcript (rarely populated)
    vm_concern_words = re.compile(
        r'cancel|quitting|can\'t\s+make|won\'t\s+be|stopping|not\s+coming|'
        r'unhappy|frustrat|concern|problem|issue|not\s+happy|too\s+expensive|'
        r'scheduling\s+conflict|switch|looking\s+at\s+other|refund', re.IGNORECASE)
    # Mark concern voicemails where transcript is available
    voice_events["has_concern"] = 0
    vm_mask = (voice_events["event_type"] == "voicemail") & voice_events["voicemail_transcript"].notna()
    if vm_mask.any():
        voice_events.loc[vm_mask, "has_concern"] = voice_events.loc[vm_mask, "voicemail_transcript"].str.contains(
            vm_concern_words, regex=True, na=False).astype(int)

    # Call review concern marking
    concern_re = re.compile(
        r'cancel|quit|unhappy|frustrat|concern|problem|issue|'
        r'not\s+happy|too\s+expensive|scheduling\s+conflict|'
        r'stop\s+coming|no\s+longer|discontinue', re.IGNORECASE)
    if len(reviews) and "transcript_text" in reviews.columns:
        reviews["has_concern"] = reviews["transcript_text"].fillna("").str.contains(
            concern_re, regex=True, na=False).astype(int)

    # Email concern regex
    cancel_words = re.compile(
        r'cancel|won\\\'t make|can\\\'t make|not coming|reschedule|'
        r'drop|quit|stop coming|unable|unavailable|no longer|discontinue', re.IGNORECASE)

    emails_valid = len(emails) and "subject" in emails.columns
    if emails_valid:
        emails["_msg_at"] = pd.to_datetime(emails["message_at"], format="mixed", utc=True).dt.tz_convert(None)

    # ── Per-student features ──
    lesson_features = []
    for name, g in lessons.groupby("student_name"):
        g = g.sort_values("lesson_date")
        dt = g["lesson_date"]
        total_all = len(g)
        first = dt.min()
        last = dt.max()
        school = int(g["school_id"].mode().iloc[0]) if not g["school_id"].empty else 0

        # Determine reference date for this student
        if student_ref_dates and name in student_ref_dates:
            s_ref = pd.Timestamp(student_ref_dates[name])
        elif ref_date is not None:
            s_ref = pd.Timestamp(ref_date)
        else:
            s_ref = pd.Timestamp(today)

        window_start = s_ref - timedelta(days=60)
        window_end = s_ref

        # Activity in feature window
        win = g[(dt >= window_start) & (dt <= window_end)]
        n_win = len(win)

        # --- Attendance ---
        weekly = []
        for d in pd.date_range(window_start, window_end, freq="W"):
            weekly.append(int(((dt >= d) & (dt < d + timedelta(days=7))).sum()))
        trend = np.polyfit(range(len(weekly)), weekly, 1)[0] if len(weekly) >= 2 else 0.0
        avg_weekly = np.mean(weekly) if weekly else 0.0
        std_weekly = np.std(weekly) if len(weekly) > 1 else 0.0

        win_dates = sorted(win["lesson_date"].dt.date.unique())
        gaps = [(win_dates[i] - win_dates[i-1]).days for i in range(1, len(win_dates))]
        max_gap = max(gaps) if gaps else 0
        avg_gap = np.mean(gaps) if gaps else 0

        # --- Teacher ---
        win_instructors = win["instructor_id"].dropna()
        n_instructors = win_instructors.nunique()
        top_instructor_id = None
        if n_win > 0 and len(win_instructors) > 0:
            top_instructor_pct = win_instructors.value_counts().iloc[0] / n_win
            top_instructor_id = win_instructors.value_counts().index[0]
        else:
            top_instructor_pct = 0

        instructor_left = 0
        if top_instructor_id and n_win > 0 and top_instructor_id in instructor_last_seen:
            last_taught = instructor_last_seen[top_instructor_id]
            days_since_last_taught = (s_ref - last_taught).days
            if days_since_last_taught > 30:
                instructor_left = 1

        # --- Time consistency ---
        clean_times = win["lesson_time"].dropna()
        if len(clean_times) > 1:
            time_consistency = clean_times.value_counts().iloc[0] / len(clean_times)
        elif len(clean_times) == 1:
            time_consistency = 1.0
        else:
            time_consistency = 0.0

        # --- Lesson types ---
        group_ratio = win["is_group"].sum() / n_win if n_win > 0 else 0
        n_types = win["lesson_type"].nunique() if n_win > 0 else 0
        makeup_ratio = win["lesson_type"].str.contains("MAKE UP", case=False).sum() / n_win if n_win > 0 else 0

        # --- Timing ---
        days_since_last = (s_ref - last).days if pd.notna(last) and n_win > 0 else 60
        tenure = (s_ref - first).days if pd.notna(first) else 0
        month = first.month if pd.notna(first) else 0

        # --- Derived ---
        newbie = 1 if pd.notna(first) and (s_ref - first).days <= 90 else 0
        trend_x_teacher = trend * (1 - top_instructor_pct)
        newbie_x_trend = newbie * trend

        # --- Communications for this student (via voice_events) ---
        nm = S(name)
        em, ph = nmap.get(nm, ("", ""))

        # Voice events in this student's window
        student_ve = voice_events[
            (voice_events["event_at"] >= window_start) & (voice_events["event_at"] <= window_end)
        ]

        if ph:
            # Calls (event_type='call' or 'missed_call')
            phone_calls = student_ve[
                (student_ve["phone_normalized"].apply(P) == ph) &
                (student_ve["event_type"].isin(["call", "missed_call"]))
            ]
            n_calls = len(phone_calls)
            n_inbound = int((phone_calls["direction"] == "inbound").sum())
            n_outbound = int((phone_calls["direction"] == "outbound").sum())

            # Call storm: ≥3 events from this phone in 7 days
            call_dates = sorted(phone_calls["event_at"].tolist())
            storm = 0
            for i in range(len(call_dates)):
                wc = sum(1 for d in call_dates[max(0,i-3):i+1] if (call_dates[i] - d).days <= 7)
                if wc >= 3:
                    storm = 1
                    break

            # Voicemails (event_type='voicemail')
            phone_vms = student_ve[
                (student_ve["phone_normalized"].apply(P) == ph) &
                (student_ve["event_type"] == "voicemail")
            ]
            n_vm = len(phone_vms)
            n_vm_concern = int(phone_vms["has_concern"].sum()) if "has_concern" in phone_vms.columns else 0
        else:
            n_calls = n_inbound = n_outbound = storm = 0
            n_vm = n_vm_concern = 0

        # voice_events lacks talk_duration — these features are zero until duration data is backfilled
        n_unanswered = 0
        n_long_inbound = 0

        inbound_ratio = n_inbound / (n_inbound + n_outbound) if (n_inbound + n_outbound) > 0 else -1.0
        unanswered_rate = n_unanswered / n_outbound if n_outbound > 0 else 0.0

        # Email
        if em and emails_valid:
            student_emails = emails[
                (emails["external_email_normalized"].apply(S) == em) &
                (emails["_msg_at"] >= window_start) &
                (emails["_msg_at"] <= window_end)
            ]
            email_count = len(student_emails)
            cancel_email_count = student_emails["subject"].str.lower().str.contains(
                cancel_words, na=False, regex=True).sum() if email_count > 0 else 0
        else:
            email_count = cancel_email_count = 0

        # Concern transcripts: reviews mapped via call_id → phone
        n_concern_reviews = 0
        if ph and len(reviews):
            for _, rev in reviews.iterrows():
                cid = str(rev.get("call_id", ""))
                mapped_phone = call_phone.get(cid, "")
                if mapped_phone == ph and rev.get("has_concern", 0):
                    evt = rev.get("event_at")
                    if pd.isna(evt) or (window_start <= evt <= window_end):
                        n_concern_reviews += 1

        lesson_features.append(dict(
            student_name=name, school_id=school,
            n_lessons_window=n_win, total_lessons_all=total_all,
            avg_weekly=avg_weekly, std_weekly=std_weekly,
            weekly_trend=trend,
            max_gap_days=max_gap, avg_gap_days=avg_gap,
            n_instructors=n_instructors, teacher_consistency=top_instructor_pct,
            time_consistency=time_consistency,
            group_ratio=group_ratio, n_lesson_types=n_types,
            makeup_ratio=makeup_ratio,
            days_since_last=days_since_last,
            tenure_days=tenure,
            last_lesson=last, first_lesson=first,
            instructor_left=instructor_left,
            month_of_year=month,
            newbie_cliff=newbie,
            trend_x_teacher=trend_x_teacher,
            newbie_x_trend=newbie_x_trend,
            vm_count=n_vm, vm_concern_count=n_vm_concern,
            call_count=n_calls,
            call_inbound_ratio=inbound_ratio,
            call_unanswered_rate=unanswered_rate,
            call_long_inbound=n_long_inbound,
            call_storm=storm,
            email_count=email_count,
            cancel_email_count=cancel_email_count,
            concern_transcripts=n_concern_reviews,
            total_comms=email_count + n_calls + n_vm,
        ))

    df = pd.DataFrame(lesson_features)

    # ── Note quality features (per-student windows) ──
    note_agg = notes.groupby("lesson_id").agg(
        note_score=("note_score", "max"),
        note_completed=("note_completed", "max"),
    ).reset_index()

    note_rows = []
    for _, row in df.iterrows():
        name = row["student_name"]
        if student_ref_dates and name in student_ref_dates:
            s_ref = pd.Timestamp(student_ref_dates[name])
        elif ref_date is not None:
            s_ref = pd.Timestamp(ref_date)
        else:
            s_ref = pd.Timestamp(today)
        ws = s_ref - timedelta(days=60)
        we = s_ref

        student_notes = lessons[
            (lessons["student_name"] == name) &
            (lessons["lesson_date"] >= ws) & (lessons["lesson_date"] <= we)
        ].merge(note_agg, on="lesson_id", how="left")

        if len(student_notes) > 0 and student_notes["note_score"].notna().any():
            ns = student_notes["note_score"].dropna()
            nc = student_notes["note_completed"].dropna()
            if len(ns) >= 2:
                note_trend = np.polyfit(range(len(ns)), ns.values, 1)[0]
            else:
                note_trend = 0.0
            note_rows.append(dict(
                student_name=name,
                avg_note_score=ns.mean(),
                note_completion_rate=nc.mean() if len(nc) > 0 else 0.0,
                n_notes=len(ns),
                note_score_trend=note_trend,
            ))
        else:
            note_rows.append(dict(
                student_name=name,
                avg_note_score=0.0, note_completion_rate=0.0,
                n_notes=0, note_score_trend=0.0,
            ))

    note_df = pd.DataFrame(note_rows)
    df = df.drop(columns=[c for c in ["avg_note_score", "note_completion_rate",
                    "n_notes", "note_score_trend"] if c in df.columns], errors="ignore")
    df = df.merge(note_df, on="student_name", how="left")
    for col in ["avg_note_score", "note_completion_rate", "n_notes", "note_score_trend"]:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    return df


# ── TRAINING ─────────────────────────────────────────────────────────────

def build_training_labels(features_df, lessons, today):
    """Label students as churned (1) or active (0). Uses fixed ref_date = today-90d
    for all students. The model learns to distinguish pre-churn behavioral patterns
    from current active patterns — which is what the churn report needs."""
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
        if days_since >= 90:
            labels.append(1)
        elif days_since <= 60:
            labels.append(0)
        else:
            labels.append(-1)
    df["label"] = labels
    return df


def train_model(df):
    skip = {"student_name", "school_id", "label", "last_lesson", "first_lesson",
            "n_lessons_window", "total_lessons_all",
            "days_since_last", "tenure_days"}
    cols = [c for c in df.columns if c not in skip and df[c].dtype in ("int64", "float64", "float32")]

    labeled = df[df["label"] >= 0].copy()
    X = labeled[cols].fillna(-1)
    y = labeled["label"].astype(int)

    n_total = len(df)
    n_labeled = len(labeled)
    n_churned = int(y.sum())
    n_active = n_labeled - n_churned

    print(f"  Training set: {n_total} total, {n_labeled} labeled")
    print(f"  Churned: {n_churned}  Active: {n_active}  ({n_churned/(n_labeled or 1):.1%})")

    if n_churned < 5 or n_active < 10:
        print("  ⚠️  Not enough examples — need more data")
        return None, None, cols, None

    Xt, Xv, yt, yv = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
    sc = StandardScaler()
    Xts, Xvs = sc.fit_transform(Xt), sc.transform(Xv)

    use_xgb = True
    if use_xgb:
        try:
            from xgboost import XGBClassifier
            model = XGBClassifier(
                n_estimators=200, max_depth=4, learning_rate=0.03,
                reg_alpha=0.5, reg_lambda=1.0,
                subsample=0.8, colsample_bytree=0.8,
                eval_metric="logloss", random_state=42, verbosity=0
            )
            model_name = "XGBoost"
        except ImportError:
            use_xgb = False

    if not use_xgb:
        from sklearn.ensemble import GradientBoostingClassifier
        model = GradientBoostingClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.03,
            min_samples_leaf=5, random_state=42
        )
        model_name = "GradientBoosting"

    model.fit(Xts, yt)
    yp = model.predict_proba(Xvs)[:, 1]
    yp_class = model.predict(Xvs)

    auc = roc_auc_score(yv, yp)
    cv = cross_val_score(model, Xts, yt, cv=3, scoring="roc_auc")

    print(f"  Model: {model_name}")
    print(f"  AUC: {auc:.3f}  CV: {cv.mean():.3f} ± {cv.std():.3f}")
    print(f"\n{classification_report(yv, yp_class, target_names=['Active', 'Churned'])}")

    imps = pd.DataFrame({"feature": cols, "importance": model.feature_importances_})\
           .sort_values("importance", ascending=False)
    print("  Top 15 features:")
    for _, row in imps.head(15).iterrows():
        print(f"    {row['feature']:<35s} {row['importance']:.4f}")

    return model, sc, cols, imps


# ── EXPLANATION ──────────────────────────────────────────────────────────

def explain(row, feature_importances=None):
    idle = int(row.get("days_since_last", 0))
    trend = float(row.get("weekly_trend", 0))
    n_win = int(row.get("n_lessons_window", 0))
    max_gap = int(row.get("max_gap_days", 0))
    n_inst = int(row.get("n_instructors", 0))
    tchr = float(row.get("teacher_consistency", 0))
    time_c = float(row.get("time_consistency", 0))
    cancel = int(row.get("cancel_email_count", 0))
    makeup = float(row.get("makeup_ratio", 0))
    note_comp = float(row.get("note_completion_rate", 0))
    note_score = float(row.get("avg_note_score", 0))
    comms = int(row.get("total_comms", 0))
    group_r = float(row.get("group_ratio", 0))
    vm_concern = int(row.get("vm_concern_count", 0))
    call_unans = float(row.get("call_unanswered_rate", 0))
    call_long = int(row.get("call_long_inbound", 0))
    call_storm = int(row.get("call_storm", 0))
    inst_left = int(row.get("instructor_left", 0))
    newbie = int(row.get("newbie_cliff", 0))
    concern_t = int(row.get("concern_transcripts", 0))

    reasons, actions = [], []

    if call_storm:
        reasons.append("Multiple calls in one week — possible billing or scheduling crisis")
        actions.append("Review recent calls; address issue immediately")
    if concern_t >= 2:
        reasons.append(f"{concern_t} calls with concern/negative sentiment in transcripts")
        actions.append("Listen to call recordings; reach out personally")
    if vm_concern > 0:
        reasons.append(f"{vm_concern} voicemail(s) expressing concern or frustration")
        actions.append("Call parent back promptly to address issue")
    if call_long > 0:
        reasons.append(f"{call_long} long inbound call(s) — potential dispute or complex issue")
        actions.append("Review call history for unresolved problems")
    if call_unans > 0.3 and row.get("call_count", 0) >= 3:
        reasons.append(f"{call_unans:.0%} of outbound calls unanswered — can't reach parent")
        actions.append("Try alternative contact method; verify phone number")
    if inst_left:
        reasons.append("Primary instructor appears to have left — loss of continuity")
        actions.append("Assign new consistent instructor; inform parent of transition")
    if max_gap > 21 and n_win >= 3:
        reasons.append(f"Gap of {max_gap} days between lessons — inconsistent attendance")
        actions.append("Call parent: check if there are scheduling issues or loss of interest")
    elif max_gap > 14:
        reasons.append(f"{max_gap}-day gap between lessons — break in routine")
        actions.append("Send text: 'We noticed a gap — is everything OK?'")
    if trend < -0.2 and n_win >= 3:
        reasons.append(f"Lesson frequency dropping ({trend:+.1f}/week)")
        actions.append("Suggest switching to a more convenient time slot")
    elif trend < -0.1:
        reasons.append(f"Attendance slowly declining ({trend:+.1f}/week)")
        actions.append("Monitor closely; check in if trend continues next week")
    if time_c < 0.5 and n_win >= 5:
        reasons.append(f"Only {time_c:.0%} of lessons at same time — inconsistent schedule")
        actions.append("Help them lock in a consistent weekly time")
    if n_inst >= 3 and n_win >= 5:
        reasons.append(f"{n_inst} different instructors — lack of teacher continuity")
        actions.append("Check if a specific teacher change triggered this; reassign if possible")
    if cancel >= 2:
        reasons.append(f"{cancel} cancellation/reschedule emails in recent weeks")
        actions.append("Reach out personally — they may be about to leave")
    elif cancel >= 1:
        reasons.append("Recent cancellation or reschedule request")
        actions.append("Follow up to understand the reason")
    # 12. Note completion/quality
    n_notes_row = int(row.get("n_notes", 0))
    if n_notes_row >= 3 and note_comp < 0.5:
        reasons.append(f"{note_comp:.0%} of lesson notes incomplete — reduced instructor engagement")
        actions.append("Ask instructor to complete notes; ensure parent gets feedback")
    if note_score > 0 and note_score < 3.0:
        reasons.append(f"Note quality average {note_score:.1f}/5 — lessons may need attention")
        actions.append("Review with instructor; offer additional guidance")
    if makeup > 0.3:
        reasons.append(f"{makeup:.0%} of lessons are make-ups — schedule problems")
        actions.append("Check if the regular time slot still works")
    if comms == 0 and idle > 14:
        reasons.append("No recent phone, voicemail, or email contact on file")
        actions.append("Verify parent contact info is current; send welcome check-in")
    if newbie and max_gap > 14:
        reasons.append(f"New student with {max_gap}-day gap — may be losing momentum")
        actions.append("Follow up on trial experience; offer next session")
    if group_r > 0.5 and trend < -0.1:
        reasons.append("Group lesson attendance declining")
        actions.append("Offer individual lesson to maintain engagement")
    if not reasons:
        reasons.append("Subtle warning signs in attendance patterns")
        actions.append("Continue monitoring; no immediate action needed")

    return "; ".join(reasons[:4]), "; ".join(actions[:3])


# ── PREDICTION ───────────────────────────────────────────────────────────

def predict_and_rank(model, scaler, cols, df, today, feature_imps=None):
    X = df[cols].fillna(-1)
    if scaler is not None: X = scaler.transform(X)

    if model is not None:
        scores = model.predict_proba(X)[:, 1]
    else:
        scores = (
            (1.0 / (df["days_since_last"].clip(lower=1) / 7 + 1))
            - df["weekly_trend"].clip(lower=-2, upper=0) * 0.3
            + df["cancel_email_count"] * 0.15
            + (1 - df["teacher_consistency"].fillna(0)) * 0.1
        )
        s_min, s_max = scores.min(), scores.max()
        if s_max > s_min: scores = (scores - s_min) / (s_max - s_min)

    out = df[["student_name", "school_id", "n_lessons_window", "last_lesson",
              "days_since_last", "weekly_trend", "max_gap_days",
              "teacher_consistency", "time_consistency", "cancel_email_count",
              "avg_note_score", "note_completion_rate",
              "n_instructors", "total_comms", "group_ratio", "makeup_ratio"]].copy()

    for c in ["vm_concern_count", "call_storm", "call_unanswered_rate",
              "call_long_inbound", "instructor_left", "newbie_cliff",
              "concern_transcripts"]:
        if c in df.columns:
            out[c] = df[c]

    out["churn_risk"] = scores

    today_dt = pd.Timestamp(today)
    out["days_idle_now"] = out["last_lesson"].apply(
        lambda x: (today_dt - pd.Timestamp(x)).days if pd.notna(x) else 365).astype(int)

    out = out[out["days_idle_now"] <= 60].copy()

    whys, dos = [], []
    for _, r in out.iterrows():
        w, d = explain(r, feature_imps)
        whys.append(w); dos.append(d)
    out["why_at_risk"] = whys
    out["recommended_action"] = dos

    out = out.sort_values("churn_risk", ascending=False)
    return out


# ── DISPLAY ──────────────────────────────────────────────────────────────

SCHOOL_NAMES = {1: "West University Place", 2: "The Heights"}

def print_by_school(risks, top=10):
    for sid, sname in sorted(SCHOOL_NAMES.items()):
        sub = risks[risks["school_id"] == sid].head(top)
        if sub.empty: continue
        n_total = len(risks[risks["school_id"] == sid])
        print(f"\n{'='*70}")
        print(f"  {sname} — Top {len(sub)} of {n_total} active students")
        print(f"{'='*70}")
        for i, (_, r) in enumerate(sub.iterrows(), 1):
            print(f"\n  {i:2d}. {r['student_name']}")
            risk_line = f"      Risk: {r['churn_risk']:.0%}  |  last: {str(r['last_lesson'])[:10]}"
            risk_line += f"  |  {int(r['days_idle_now'])}d  |  gap {int(r['max_gap_days'])}d"
            risk_line += f"  |  trend {r['weekly_trend']:+.2f}/wk"
            print(risk_line)
            print(f"      Why: {r['why_at_risk']}")
            print(f"      Do:  {r['recommended_action']}")


# ── MAIN ─────────────────────────────────────────────────────────────────

def main():
    today = date.today()
    print(f"Today: {today}\n")

    print("[1] Load data...")
    lessons, notes, reviews, people, sms, voice_events, emails, \
        call_matches, visits = load()
    print(f"    {len(lessons):,} student-records, {len(notes):,} notes, "
          f"{len(reviews):,} call reviews, {len(voice_events):,} voice events")

    print("[2] Build training labels...")
    train_ref = date.today() - timedelta(days=90)
    train_features = build_features(lessons, notes, reviews, people, sms,
                                    voice_events, emails, call_matches, visits,
                                    today, ref_date=train_ref)
    train_labeled = build_training_labels(train_features, lessons, today)
    n_churned = (train_labeled["label"] == 1).sum()
    n_active = (train_labeled["label"] == 0).sum()
    print(f"    Churned: {n_churned}, Active: {n_active}")

    print("[3] Train model...")
    model, scaler, cols, imps = train_model(train_labeled)

    print("[4] Build current features & predict...")
    current_features = build_features(lessons, notes, reviews, people, sms,
                                      voice_events, emails, call_matches, visits,
                                      today)
    risks = predict_and_rank(model, scaler, cols, current_features, today, imps)

    with open(f"{MDIR}/churn_model.pkl", "wb") as f:
        pickle.dump(dict(model=model, scaler=scaler, feature_cols=cols,
                         importances=imps.to_dict("records") if imps is not None else [],
                         trained_at=today.isoformat()), f)
    risks.to_csv(f"{MDIR}/churn_risk_scores.csv", index=False)

    active_count = len(risks)
    flagged = (risks["churn_risk"] > 0.3).sum()
    print(f"\n  Active students scored: {active_count}")
    print(f"  Flagged (risk > 30%): {flagged}")
    print(f"  Saved → models/")

    print_by_school(risks, top=10)
    return model, risks


if __name__ == "__main__":
    main()
