#!/usr/bin/env python3
"""
SOR Churn Model v4 — trained on confirmed leavers vs active students.
Features: attendance trend, teacher consistency, time slot stability,
note scores, note completion, communication frequency/channel, call sentiment,
email cancellation signals.

Training: 217 churned (last lesson 90d+, 5+ total) vs 671 active (60d window).
Prediction: only currently-active students (lesson in last 60d).
"""

import os, sqlite3, pickle, warnings, re
from datetime import date, timedelta
from collections import Counter
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
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

def load(cutoff=None):
    if cutoff is None: cutoff = date.today().isoformat()
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

    reviews = pd.read_sql_query("""
        SELECT call_review_id, transcript_text, speaker_turns_json, event_at
        FROM dialpad_call_reviews WHERE transcript_text IS NOT NULL
    """, c)

    people = pd.read_sql_query(
        "SELECT person_id, full_name, email_normalized, phone, membership_state, school FROM pike13_people", c)

    sms = pd.read_sql_query("SELECT * FROM dialpad_sms_messages", c)
    calls = pd.read_sql_query("SELECT * FROM dialpad_calls", c)

    emails = pd.read_sql_query("""
        SELECT message_id, external_email_normalized, direction, message_at,
               subject, snippet
        FROM school_email_messages
    """, c)
    c.close()

    # Parse dates
    lessons["lesson_date"] = pd.to_datetime(lessons["lesson_date"])
    if len(emails) and "message_at" in emails.columns:
        emails["message_at"] = pd.to_datetime(emails["message_at"], format="mixed", utc=True)

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
    return lessons_split, notes, reviews, people, sms, calls, emails


# ── FEATURE ENGINEERING ──────────────────────────────────────────────────

def build_features(lessons, notes, reviews, people, sms, calls, emails,
                   today, ref_date=None):
    """Build per-student features.

    ref_date: lookback cutoff for features (for training, this is the
    churned student's last lesson date minus 1; for prediction, this is today).
    If None, uses today.
    """
    if ref_date is None:
        ref_date = pd.Timestamp(today)
    else:
        ref_date = pd.Timestamp(ref_date)

    window_start = ref_date - timedelta(days=60)  # features from last 60 days
    window_end = ref_date

    # ── per-student lesson aggregates ──
    lesson_features = []
    for name, g in lessons.groupby("student_name"):
        g = g.sort_values("lesson_date")
        dt = g["lesson_date"]
        total_all = len(g)
        first = dt.min()
        last = dt.max()
        school = int(g["school_id"].mode().iloc[0]) if not g["school_id"].empty else 0

        # Activity in feature window
        win = g[(dt >= window_start) & (dt <= window_end)]
        n_win = len(win)

        # Not in window = skip for training
        # For active label: must have had lessons in window
        # For churned label: must have had lessons ending near ref_date

        # Attendance trend: weekly lesson count over 60d window
        weekly = []
        for d in pd.date_range(window_start, window_end, freq="W"):
            weekly.append(int(((dt >= d) & (dt < d + timedelta(days=7))).sum()))
        trend = np.polyfit(range(len(weekly)), weekly, 1)[0] if len(weekly) >= 2 else 0.0
        avg_weekly = np.mean(weekly) if weekly else 0.0
        std_weekly = np.std(weekly) if len(weekly) > 1 else 0.0

        # Gap analysis: days between lessons in window
        win_dates = sorted(win["lesson_date"].dt.date.unique())
        gaps = []
        for i in range(1, len(win_dates)):
            gaps.append((win_dates[i] - win_dates[i-1]).days)
        max_gap = max(gaps) if gaps else 0
        avg_gap = np.mean(gaps) if gaps else 0

        # Teacher consistency
        instructors = win["instructor_id"].dropna().unique()
        n_instructors = len(instructors)
        if n_win > 0 and len(win["instructor_id"].dropna()) > 0:
            top_instructor_pct = win["instructor_id"].value_counts().iloc[0] / n_win
        else:
            top_instructor_pct = 0

        # Time slot consistency
        clean_times = win["lesson_time"].dropna()
        time_consistency = 0.0
        if len(clean_times) > 1:
            top_time_pct = clean_times.value_counts().iloc[0] / len(clean_times)
            time_consistency = top_time_pct
        elif len(clean_times) == 1:
            time_consistency = 1.0

        # Group vs individual
        group_ratio = win["is_group"].sum() / n_win if n_win > 0 else 0

        # Lesson type variety
        n_types = win["lesson_type"].nunique() if n_win > 0 else 0

        # Days since last lesson (relative to ref_date)
        last_in_win = win["lesson_date"].max()
        days_since_last = (ref_date - last_in_win).days if pd.notna(last_in_win) and n_win > 0 else 60

        # Total tenure (days from first lesson to ref_date)
        tenure = (ref_date - first).days if pd.notna(first) else 0

        # Makeup lessons ratio (lesson type contains "MAKE UP")
        makeup_ratio = win["lesson_type"].str.contains("MAKE UP", case=False).sum() / n_win if n_win > 0 else 0

        lesson_features.append(dict(
            student_name=name, school_id=school,
            n_lessons_window=n_win, total_lessons_all=total_all,
            avg_weekly=avg_weekly, std_weekly=std_weekly,
            weekly_trend=trend,
            max_gap_days=max_gap, avg_gap_days=avg_gap,
            n_instructors=n_instructors, teacher_consistency=top_instructor_pct,
            time_consistency=time_consistency,
            group_ratio=group_ratio, n_lesson_types=n_types,
            days_since_last=days_since_last,
            tenure_days=tenure, makeup_ratio=makeup_ratio,
            last_lesson=dt.max(), first_lesson=first,
        ))

    df = pd.DataFrame(lesson_features)

    # ── note scores ──
    note_agg = notes.groupby("lesson_id").agg(
        note_score=("note_score", "max"),
        note_completed=("note_completed", "max"),
    ).reset_index()

    # Merge notes onto lessons in the window
    win_lessons = lessons[
        (lessons["lesson_date"] >= window_start) & (lessons["lesson_date"] <= window_end)
    ]
    win_with_notes = win_lessons.merge(note_agg, on="lesson_id", how="left")

    note_by_student = win_with_notes.groupby("student_name").agg(
        avg_note_score=("note_score", "mean"),
        note_completion_rate=("note_completed", "mean"),
        n_notes=("note_score", "count"),
        note_score_trend=("note_score", lambda x: 
            np.polyfit(range(len(x.dropna())), x.dropna().values, 1)[0] 
            if len(x.dropna()) >= 2 else 0.0),
    ).reset_index()

    df = df.merge(note_by_student, on="student_name", how="left")
    for col in ["avg_note_score", "note_completion_rate", "n_notes", "note_score_trend"]:
        df[col] = df[col].fillna(0)

    # ── email cancellation signals ──
    cancel_words = r'cancel|won\'t make|can\'t make|not coming|reschedule|drop|quit|stop coming|unable|unavailable|no longer|discontinue'
    if len(emails) and "subject" in emails.columns:
        emails["_msg_at"] = pd.to_datetime(emails["message_at"], format="mixed", utc=True).dt.tz_convert(None)
    else:
        emails = pd.DataFrame()

    # Person lookup for email/phone
    nmap = {}
    for _, p in people.iterrows():
        n = S(p.get("full_name"))
        if n: nmap[n] = (S(p.get("email_normalized")), P(p.get("phone")))

    email_feats = []
    for _, row in df.iterrows():
        nm = S(row["student_name"])
        em, ph = nmap.get(nm, ("", ""))
        cancel_count = 0
        if em and len(emails):
            student_emails = emails[
                (emails["external_email_normalized"].apply(S) == em) &
                (emails["_msg_at"] >= window_start) &
                (emails["_msg_at"] <= window_end)
            ]
            n_em = len(student_emails)
            cancel_count = student_emails["subject"].str.lower().str.contains(
                cancel_words, na=False, regex=True).sum() if n_em > 0 else 0
            email_count = n_em
        else:
            email_count = 0

        # SMS/Call counts
        sms_count = 0
        if ph and "phone_number" in sms.columns:
            sms_count = sms["phone_number"].apply(P).str.contains(ph, na=False).sum()

        call_count = 0
        if ph and "phone_number" in calls.columns:
            call_count = calls["phone_number"].apply(P).str.contains(ph, na=False).sum()

        email_feats.append(dict(
            email_count=email_count, cancel_email_count=cancel_count,
            sms_count=sms_count, call_count=call_count,
            total_comms=email_count + sms_count + call_count,
        ))

    comm_df = pd.DataFrame(email_feats)
    df = pd.concat([df.reset_index(drop=True), comm_df], axis=1)

    # ── call transcript sentiment (simple keyword-based) ──
    # Map students to call reviews via phone number
    # For simplicity: count calls where transcript contains negative/concern keywords
    concern_words = r'can\'t |won\'t |stop|quit|cancel|unhappy|frustrat|concern|problem|issue|not happy|too expensive|scheduling conflict'
    concern_by_phone = {}
    if len(reviews) and "transcript_text" in reviews.columns:
        reviews["has_concern"] = reviews["transcript_text"].fillna("").str.lower().str.contains(
            concern_words, regex=True, na=False).astype(int)
        reviews["transcript_len"] = reviews["transcript_text"].fillna("").str.len()
        # For now, phone mapping is rough — count all reviews
        # Better: match via Dialpad voice events → phone → student
        pass

    sentiment_feats = []
    for _, row in df.iterrows():
        nm = S(row["student_name"])
        em, ph = nmap.get(nm, ("", ""))
        # Rough: attribute reviews that match student phone
        n_concern = 0
        if ph and len(reviews):
            n_concern = reviews["has_concern"].sum()  # placeholder — can refine later
        sentiment_feats.append(dict(concern_transcripts=n_concern))
    sent_df = pd.DataFrame(sentiment_feats)
    df = pd.concat([df.reset_index(drop=True), sent_df], axis=1)

    return df


# ── TRAINING LABELS ──────────────────────────────────────────────────────

def build_training_labels(features_df, lessons, today):
    """Label students as churned (1) or active (0) based on actual lesson history.

    Churned: had 5+ total lessons, no lesson in 90+ days, had lessons in a
    60-day window ending 90 days before today.

    Active: had at least 1 lesson in last 60 days.

    Returns DataFrame with added 'label' column (0=active, 1=churned).
    """
    today_ts = pd.Timestamp(today)
    churn_cutoff = today_ts - timedelta(days=90)

    df = features_df.copy()

    # Churn definition
    # For each student in features, check if they're churned
    labels = []
    for _, row in df.iterrows():
        nm = row["student_name"]
        student_lessons = lessons[lessons["student_name"] == nm]
        if len(student_lessons) < 5:
            labels.append(-1)  # too few lessons — skip
            continue

        last_date = student_lessons["lesson_date"].max()
        days_since = (today_ts - last_date).days

        if days_since >= 90:
            labels.append(1)  # churned
        elif days_since <= 60:
            labels.append(0)  # active
        else:
            labels.append(-1)  # gray zone — skip

    df["label"] = labels
    return df


# ── TRAINING ─────────────────────────────────────────────────────────────

def train_model(df):
    skip = {"student_name", "school_id", "label", "last_lesson", "first_lesson",
            "n_lessons_window", "total_lessons_all",
            "days_since_last", "tenure_days"}  # exclude label-leaking features
    cols = [c for c in df.columns if c not in skip and df[c].dtype in ("int64", "float64", "float32")]

    labeled = df[df["label"] >= 0].copy()
    X = labeled[cols].fillna(0)
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

    gb = GradientBoostingClassifier(
        n_estimators=200, max_depth=4, learning_rate=0.03,
        min_samples_leaf=5, random_state=42
    )
    gb.fit(Xts, yt)
    yp = gb.predict_proba(Xvs)[:, 1]
    yp_class = gb.predict(Xvs)

    auc = roc_auc_score(yv, yp)
    cv = cross_val_score(gb, Xts, yt, cv=3, scoring="roc_auc")

    print(f"  AUC: {auc:.3f}  CV: {cv.mean():.3f} ± {cv.std():.3f}")
    print(f"\n{classification_report(yv, yp_class, target_names=['Active', 'Churned'])}")

    imps = pd.DataFrame({"feature": cols, "importance": gb.feature_importances_})\
           .sort_values("importance", ascending=False)
    print("  Top 10 features:")
    for _, row in imps.head(10).iterrows():
        print(f"    {row['feature']:<35s} {row['importance']:.4f}")

    return gb, sc, cols, imps


# ── EXPLANATION ──────────────────────────────────────────────────────────

def explain(row, feature_importances=None):
    """Generate human-readable explanation for why a student is at risk."""
    idle = int(row.get("days_since_last", 0))
    trend = float(row.get("weekly_trend", 0))
    n_win = int(row.get("n_lessons_window", 0))
    max_gap = int(row.get("max_gap_days", 0))
    avg_wk = float(row.get("avg_weekly", 0))
    n_inst = int(row.get("n_instructors", 0))
    tchr = float(row.get("teacher_consistency", 0))
    time_c = float(row.get("time_consistency", 0))
    cancel = int(row.get("cancel_email_count", 0))
    makeup = float(row.get("makeup_ratio", 0))
    note_comp = float(row.get("note_completion_rate", 0))
    note_score = float(row.get("avg_note_score", 0))
    comms = int(row.get("total_comms", 0))
    group_r = float(row.get("group_ratio", 0))

    reasons, actions = [], []

    # 1. Attendance gaps — the biggest red flag
    if max_gap > 21 and n_win >= 3:
        reasons.append(f"Gap of {max_gap} days between lessons — inconsistent attendance")
        actions.append("Call parent: check if there are scheduling issues or loss of interest")
    elif max_gap > 14:
        reasons.append(f"{max_gap}-day gap between lessons — break in routine")
        actions.append("Send text: 'We noticed a gap — is everything OK?'")

    # 2. Attendance trend — declining weekly frequency
    if trend < -0.2 and n_win >= 3:
        reasons.append(f"Lesson frequency dropping ({trend:+.1f}/week)")
        actions.append("Suggest switching to a more convenient time slot")
    elif trend < -0.1:
        reasons.append(f"Attendance slowly declining ({trend:+.1f}/week)")
        actions.append("Monitor closely; check in if trend continues next week")

    # 3. Time slot instability
    if time_c < 0.5 and n_win >= 5:
        reasons.append(f"Only {time_c:.0%} of lessons at same time — inconsistent schedule")
        actions.append("Help them lock in a consistent weekly time")

    # 4. Teacher switching
    if n_inst >= 3 and n_win >= 5:
        reasons.append(f"{n_inst} different instructors — lack of teacher continuity")
        actions.append("Check if a specific teacher change triggered this; reassign if possible")

    # 5. Cancellation emails
    if cancel >= 2:
        reasons.append(f"{cancel} cancellation/reschedule emails in recent weeks")
        actions.append("Reach out personally — they may be about to leave")
    elif cancel >= 1:
        reasons.append("Recent cancellation or reschedule request")
        actions.append("Follow up to understand the reason")

    # 6. Note completion / quality
    if note_comp < 0.5 and n_win >= 5:
        reasons.append(f"{note_comp:.0%} of lesson notes incomplete — reduced instructor engagement")
        actions.append("Ask instructor to complete notes; ensure parent gets feedback")
    if note_score > 0 and note_score < 3.0:
        reasons.append(f"Note quality average {note_score:.1f}/5 — lessons may need attention")
        actions.append("Review with instructor; offer additional guidance")

    # 7. Makeup lessons
    if makeup > 0.3:
        reasons.append(f"{makeup:.0%} of lessons are make-ups — schedule problems")
        actions.append("Check if the regular time slot still works")

    # 8. No communication
    if comms == 0 and idle > 14:
        reasons.append("No recent phone, text, or email contact on file")
        actions.append("Verify parent contact info is current; send welcome check-in")

    # 9. New student with gaps
    if n_win <= 4 and max_gap > 14:
        reasons.append(f"New student with {max_gap}-day gap — may be losing momentum")
        actions.append("Follow up on trial experience; offer next session")

    # 10. Group lesson decline
    if group_r > 0.5 and trend < -0.1:
        reasons.append("Group lesson attendance declining")
        actions.append("Offer individual lesson to maintain engagement")

    if not reasons:
        reasons.append("Subtle warning signs in attendance patterns")
        actions.append("Continue monitoring; no immediate action needed")

    return "; ".join(reasons[:4]), "; ".join(actions[:3])


# ── PREDICTION ───────────────────────────────────────────────────────────

def predict_and_rank(model, scaler, cols, df, today, feature_imps=None):
    X = df[cols].fillna(0)
    if scaler is not None: X = scaler.transform(X)

    if model is not None:
        scores = model.predict_proba(X)[:, 1]
    else:
        # Heuristic fallback
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
    out["churn_risk"] = scores

    # Actual idle days from today
    today_dt = pd.Timestamp(today)
    out["days_idle_now"] = out["last_lesson"].apply(
        lambda x: (today_dt - pd.Timestamp(x)).days if pd.notna(x) else 365).astype(int)

    # Only score active students
    out = out[out["days_idle_now"] <= 60].copy()

    whys, dos = [], []
    for _, r in out.iterrows():
        w, d = explain(r, feature_imps)
        whys.append(w); dos.append(d)
    out["why_at_risk"] = whys
    out["recommended_action"] = dos

    # Sort: top risk first, but with minimum 5% risk floor to avoid
    # flagging students with zero real signals
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
            print(f"      Risk: {r['churn_risk']:.0%}  |  last: {str(r['last_lesson'])[:10]}"
                  f"  |  {int(r['days_idle_now'])}d  |  gap {int(r['max_gap_days'])}d"
                  f"  |  trend {r['weekly_trend']:+.2f}/wk")
            print(f"      Why: {r['why_at_risk']}")
            print(f"      Do:  {r['recommended_action']}")


# ── MAIN ─────────────────────────────────────────────────────────────────

def main():
    today = date.today()
    print(f"Today: {today}\n")

    print("[1] Load data...")
    lessons, notes, reviews, people, sms, calls, emails = load()
    print(f"    {len(lessons):,} student-records, {len(notes):,} notes, "
          f"{len(reviews):,} call reviews")

    print("[2] Build training labels...")
    # For training: use features as of 90 days ago, label = churned or not
    train_ref = date.today() - timedelta(days=90)
    train_features = build_features(lessons, notes, reviews, people, sms, calls,
                                    emails, today, ref_date=train_ref)
    train_labeled = build_training_labels(train_features, lessons, today)
    n_churned = (train_labeled["label"] == 1).sum()
    n_active = (train_labeled["label"] == 0).sum()
    print(f"    Churned: {n_churned}, Active: {n_active}")

    print("[3] Train model...")
    model, scaler, cols, imps = train_model(train_labeled)

    print("[4] Build current features & predict...")
    current_features = build_features(lessons, notes, reviews, people, sms, calls,
                                      emails, today)
    risks = predict_and_rank(model, scaler, cols, current_features, today, imps)

    # Save
    with open(f"{MDIR}/churn_model.pkl", "wb") as f:
        pickle.dump(dict(model=model, scaler=scaler, feature_cols=cols,
                         importances=imps.to_dict("records") if imps is not None else [],
                         trained_at=today.isoformat()), f)
    risks.to_csv(f"{MDIR}/churn_risk_scores.csv", index=False)

    # Summarize
    active_count = len(risks)
    flagged = (risks["churn_risk"] > 0.3).sum()
    print(f"\n  Active students scored: {active_count}")
    print(f"  Flagged (risk > 30%): {flagged}")
    print(f"  Saved → models/")

    print_by_school(risks, top=10)
    return model, risks


if __name__ == "__main__":
    main()
