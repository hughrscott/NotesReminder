#!/usr/bin/env python3
"""
generate_weekly_churn_email.py — Weekly churn prevention intelligence email.

Produces a markdown email covering:
    1. Students likely to churn (reasons + personalized actions)
    2. Students returning from on-hold (talking points)
    3. Churn score changes week-over-week
    4. Follow-up on last week's recommendations
    5. Actions personalized from lesson notes, comms, Pike13, HubSpot

Usage:
    python3 generate_weekly_churn_email.py                    # Generate + print
    python3 generate_weekly_churn_email.py --send             # Generate + send
    python3 generate_weekly_churn_email.py --output email.md  # Save to file
"""
import sqlite3, json, pickle, csv, re
from pathlib import Path
from datetime import date, datetime, timedelta
from collections import defaultdict
import numpy as np
import pandas as pd

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"
MODEL_PATH = MODELS_DIR / "churn_model_v14_final_enhanced.pkl"
LEAVERS_PATH = MODELS_DIR / "pike13_leavers.json"
ENGAGEMENT_PATH = MODELS_DIR / "comms_engagement_features.csv"
TRACKING_PATH = MODELS_DIR / "weekly_recommendations.json"

TODAY = date.today()
LOOKBACK = 90
TOP_N_CHURN = 10   # How many at-risk students to highlight
TOP_N_RETURN = 5   # How many returning students to highlight


# ─── 1. DATA LOADING ───

def load_model():
    with open(MODEL_PATH, "rb") as f:
        return pickle.load(f)


def load_all_data():
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    
    # Students with lessons
    lessons = pd.read_sql_query("""
        SELECT lesson_id, lesson_date, students_raw, school_id, instructor_id
        FROM lessons WHERE students_raw IS NOT NULL AND students_raw != ''
    """, con)
    lessons["lesson_date"] = pd.to_datetime(lessons["lesson_date"])
    
    # Notes
    notes = pd.read_sql_query(
        "SELECT lesson_id, note_score, note_score_explanation FROM lesson_notes WHERE note_score IS NOT NULL",
        con
    )
    
    # Pike13 people
    people = {}
    for row in con.execute("""
        SELECT full_name, first_name, last_name, email_normalized, phone_normalized, 
               membership_state, school
        FROM pike13_people WHERE full_name IS NOT NULL AND full_name != ''
    """):
        key = (row["full_name"] or "").strip().lower()
        if key == "loading":
            continue
        people[key] = {
            "full_name": row["full_name"], "first_name": row["first_name"] or "",
            "email": row["email_normalized"] or "", "phone": row["phone_normalized"] or "",
            "membership": row["membership_state"] or "", "school": row["school"] or "",
        }
    
    # Engagement
    engagement = {}
    if ENGAGEMENT_PATH.exists():
        with open(ENGAGEMENT_PATH) as f:
            for row in csv.DictReader(f):
                engagement[row["student"]] = row
    
    # Leavers
    with open(LEAVERS_PATH) as f:
        leavers_raw = json.load(f)
    leavers = {k.lower(): v for k, v in leavers_raw.items()}
    
    # Previous week recommendations
    prev_recs = {}
    if TRACKING_PATH.exists():
        with open(TRACKING_PATH) as f:
            prev_recs = json.load(f)
    
    con.close()
    return lessons, notes, people, engagement, leavers, prev_recs


# ─── 2. FEATURE COMPUTATION ───

def expand_students(lessons_df):
    rows = []
    for _, r in lessons_df.iterrows():
        for n in re.split(r',\s*', str(r["students_raw"])):
            if n.strip():
                rows.append({
                    "student": n.strip().lower(),
                    "lesson_id": r["lesson_id"],
                    "lesson_date": r["lesson_date"],
                    "school_id": r["school_id"],
                    "instructor_id": r["instructor_id"],
                })
    return pd.DataFrame(rows)


def compute_features(student_name, group, notes_df, engagement, ref_date):
    ref_ts = pd.Timestamp(ref_date)
    pre = group[group["lesson_date"] <= ref_ts]
    if len(pre) < 5:
        return None

    all_dates = pre["lesson_date"].sort_values()
    d30, d60, d90 = ref_ts - timedelta(days=30), ref_ts - timedelta(days=60), ref_ts - timedelta(days=90)

    total_lessons = len(pre)
    lessons_30d = len(pre[pre["lesson_date"] >= d30])
    lessons_60d = len(pre[pre["lesson_date"] >= d60])
    lessons_90d = len(pre[pre["lesson_date"] >= d90])
    
    older = len(pre[(pre["lesson_date"] >= d60) & (pre["lesson_date"] < d30)])
    freq_decline = lessons_30d / max(older, 1)
    
    days_since_last = (ref_ts - all_dates.max()).days
    tenure_days = (ref_ts - all_dates.min()).days
    
    gaps = all_dates.diff().dropna().dt.days
    max_gap = gaps.max() if len(gaps) > 0 else 0
    avg_gap = gaps.mean() if len(gaps) > 0 else 999
    gap_std = gaps.std() if len(gaps) > 1 else 0
    
    recent = pre[pre["lesson_date"] >= d90]
    inst_counts = recent["instructor_id"].value_counts()
    teacher_cons = inst_counts.iloc[0] / len(recent) if len(recent) > 0 and len(inst_counts) > 0 else 0
    
    merged = pre.merge(notes_df, on="lesson_id", how="left")
    ns = merged["note_score"].dropna()
    avg_note = ns.mean() if len(ns) > 0 else 0.0

    row = {
        "total_lessons": total_lessons, "lessons_30d": lessons_30d,
        "lessons_60d": lessons_60d, "lessons_90d": lessons_90d,
        "freq_decline_ratio": freq_decline, "days_since_last": days_since_last,
        "max_gap_days": max_gap, "avg_gap_days": avg_gap, "gap_std": gap_std,
        "tenure_days": tenure_days, "teacher_consistency": teacher_cons,
        "avg_note_score": avg_note,
    }

    e = engagement.get(student_name, {})
    eng_keys = [
        "comms_engagement_avg_risk", "comms_engagement_total",
        "comms_engagement_cancellation_rate", "comms_engagement_praise_rate",
        "comms_engagement_inquiry_rate", "comms_engagement_positive_ratio",
        "comms_engagement_negative_ratio", "comms_engagement_risk_volatility",
    ]
    for k in eng_keys:
        row[k] = float(e.get(k, 0))

    return row


# ─── 3. CHURN PREDICTION ───

def predict_churn(expanded, notes_df, people, engagement, model_artifact):
    """Compute churn probabilities for all active students."""
    model = model_artifact["model"]
    scaler = model_artifact["scaler"]
    features = model_artifact["features"]
    today_ts = pd.Timestamp(TODAY)
    
    predictions = {}
    for name, group in expanded.groupby("student"):
        name_l = name.strip().lower()
        last = group["lesson_date"].max()
        if (today_ts - last).days > 90:
            continue  # Inactive
        
        feat = compute_features(name_l, group, notes_df, engagement, TODAY)
        if feat is None:
            continue
        
        # Build feature vector in model's expected order
        X = []
        for f in features:
            X.append(feat.get(f, 0))
        X = np.array(X).reshape(1, -1)
        Xs = scaler.transform(X)
        prob = model.predict_proba(Xs)[0, 1]
        
        info = people.get(name_l, {})
        predictions[name_l] = {
            "name": info.get("full_name", name_l),
            "churn_probability": round(prob, 3),
            "features": feat,
            "school": info.get("school", "Unknown"),
            "membership": info.get("membership", "Unknown"),
            "plans": info.get("plans", ""),
            "phone": info.get("phone", ""),
            "email": info.get("email", ""),
        }
    
    return predictions


# ─── 4. REASON GENERATION ───

def generate_reasons(feat, student_name, con):
    """Generate human-readable churn reasons from features."""
    reasons = []
    
    if feat["days_since_last"] > 14:
        reasons.append(f"No lessons in {feat['days_since_last']} days")
    if feat["freq_decline_ratio"] < 0.5:
        reasons.append(f"Attendance declining (last 30d: {feat['lessons_30d']} vs prior: {feat['freq_decline_ratio']:.1f}x)")
    if feat["max_gap_days"] > 30:
        reasons.append(f"Longest gap: {feat['max_gap_days']} days between lessons")
    if feat["gap_std"] > 15:
        reasons.append("Inconsistent attendance pattern")
    if feat["comms_engagement_total"] == 0:
        reasons.append("No parent communication — 'quiet quit' risk")
    if feat["comms_engagement_avg_risk"] > 0.3:
        reasons.append(f"Recent comms show disengagement (risk: {feat['comms_engagement_avg_risk']:.1f})")
    if feat["comms_engagement_cancellation_rate"] > 0:
        reasons.append(f"Cancellation calls: {feat['comms_engagement_cancellation_rate']:.0%} of comms")
    if feat["lessons_30d"] == 0 and feat["lessons_60d"] > 3:
        reasons.append("Recent attendance drop-off")
    
    return reasons


def generate_actions(feat, student_info, student_name, con):
    """Generate personalized action recommendations."""
    actions = []
    
    # Contact-based actions
    phone = student_info.get("phone", "")
    email = student_info.get("email", "")
    
    if feat["days_since_last"] > 14:
        if phone:
            actions.append(f"📞 Call parent at {phone} — check on attendance gap")
        elif email:
            actions.append(f"📧 Email {email} — check on attendance gap")
    
    if feat["comms_engagement_cancellation_rate"] > 0:
        actions.append("Discuss schedule flexibility — recent cancellations suggest scheduling conflict")
    
    if feat["comms_engagement_total"] == 0:
        if phone:
            actions.append(f"📱 Send personalized SMS to {phone} — no communication in record, re-engage")
        elif email:
            actions.append(f"📧 Send re-engagement email to {email}")
    
    if feat["freq_decline_ratio"] < 0.5:
        actions.append("Offer make-up lesson or schedule adjustment")
    
    # Look for recent instructor notes for personalization
    notes_text = ""
    notes_rows = con.execute("""
        SELECT ln.note_score_explanation, l.lesson_date
        FROM lesson_notes ln JOIN lessons l ON ln.lesson_id = l.lesson_id
        WHERE l.students_raw LIKE ? AND ln.note_score_explanation IS NOT NULL
        ORDER BY l.lesson_date DESC LIMIT 3
    """, (f"%{student_info.get('full_name', student_name)}%",)).fetchall()
    
    for nr in notes_rows:
        if nr[0]:
            notes_text += nr[0][:200] + " "
    
    if notes_text:
        actions.append(f"📝 Recent note insight: \"{notes_text[:150].strip()}...\"")
    
    # Look for recent comms for personalization
    comms_rows = con.execute("""
        SELECT body, message_at FROM dialpad_sms_messages
        WHERE body IS NOT NULL AND body != '' AND (body LIKE ? OR body LIKE ?)
        ORDER BY message_at DESC LIMIT 2
    """, (f"%{student_info.get('first_name', '')}%", f"%{student_name.split('_')[0]}%")).fetchall()
    
    if comms_rows:
        for cr in comms_rows:
            actions.append(f"💬 Recent SMS: \"{cr[0][:120]}...\"")
    
    return actions


# ─── 5. RETURNING STUDENTS ───

def find_returning_students(expanded, leavers):
    """Find students on hold with return dates in next 14 days."""
    today_ts = pd.Timestamp(TODAY)
    returning = []
    
    for name, info in leavers.items():
        end_str = info.get("end_date", "")
        try:
            end_date = pd.Timestamp(end_str).date()
        except:
            continue
        
        days_since_end = (TODAY - end_date).days
        
        # Students who ended 30-90 days ago (on hold, not permanent churn)
        if 30 <= days_since_end <= 90:
            # Check if they had lessons before leaving
            student_lessons = expanded[expanded["student"] == name.lower()]
            if len(student_lessons) > 5:
                last_lesson = student_lessons["lesson_date"].max()
                lesson_gap = (today_ts - last_lesson).days
                
                returning.append({
                    "name": info.get("name", name),
                    "end_date": end_date,
                    "days_on_hold": days_since_end,
                    "last_lesson": last_lesson,
                    "lesson_gap_days": lesson_gap,
                    "total_lessons": len(student_lessons),
                    "school": info.get("school", "Unknown"),
                })
    
    returning.sort(key=lambda x: x["days_on_hold"])
    return returning[:TOP_N_RETURN]


def generate_return_talking_points(student, con):
    """Generate personalized talking points for returning students."""
    points = []
    name = student["name"]
    
    # Get last lesson details
    last_lesson = con.execute("""
        SELECT l.lesson_type, l.lesson_date, i.instructor_name
        FROM lessons l LEFT JOIN instructors i ON l.instructor_id = i.instructor_id
        WHERE l.students_raw LIKE ? 
        ORDER BY l.lesson_date DESC LIMIT 1
    """, (f"%{name}%",)).fetchone()
    
    if last_lesson:
        points.append(f"Last lesson: {last_lesson[0]} with {last_lesson[2] or 'Unknown'} on {last_lesson[1]}")
    
    # Get last note
    last_note = con.execute("""
        SELECT ln.note_score_explanation, l.lesson_date
        FROM lesson_notes ln JOIN lessons l ON ln.lesson_id = l.lesson_id
        WHERE l.students_raw LIKE ? AND ln.note_score_explanation IS NOT NULL
        ORDER BY l.lesson_date DESC LIMIT 1
    """, (f"%{name}%",)).fetchone()
    
    if last_note:
        points.append(f'Note: "{last_note[0][:200]}"')
    
    points.append(f"Welcome back after {student['days_on_hold']} days — check schedule availability")
    points.append("Ask: 'What brought you back?' and listen for commitment signals")
    
    return points


# ─── 6. FOLLOW-UP TRACKING ───

def check_followups(prev_recs, con):
    """Check whether last week's recommendations were acted on."""
    if not prev_recs:
        return []
    
    followups = []
    last_week = prev_recs.get("week", "")
    recs = prev_recs.get("recommendations", {})
    
    for student, rec in recs.items():
        actions = rec.get("actions", [])
        statuses = []
        
        for action in actions:
            # Check if phone call was made
            if "call" in action.lower() or "📞" in action:
                calls = con.execute("""
                    SELECT COUNT(*) FROM dialpad_calls
                    WHERE date >= ? AND date <= ?
                """, (last_week, str(TODAY))).fetchone()[0]
                statuses.append(f"{'✓' if calls > 0 else '✗'} Phone call {'made' if calls > 0 else 'NOT made'}")
            
            # Check if SMS was sent
            if "sms" in action.lower() or "📱" in action:
                sms_count = con.execute("""
                    SELECT COUNT(*) FROM dialpad_sms_messages
                    WHERE message_at >= ? AND message_at <= ?
                """, (last_week, str(TODAY))).fetchone()[0]
                statuses.append(f"{'✓' if sms_count > 0 else '✗'} SMS {'sent' if sms_count > 0 else 'NOT sent'}")
        
        if statuses:
            followups.append({
                "student": rec.get("name", student),
                "statuses": statuses,
                "all_done": all("✓" in s for s in statuses),
            })
    
    return followups


# ─── 7. EMAIL GENERATION ───

def format_email(at_risk, returning, score_changes, followups, total_students):
    """Format the weekly churn intelligence email as markdown."""
    lines = []
    
    # Subject
    lines.append(f"# 🎸 Weekly Churn Intelligence — {TODAY.strftime('%B %d, %Y')}")
    lines.append("")
    
    # Executive Summary
    at_risk_count = len(at_risk)
    returning_count = len(returning)
    total_at_risk = sum(1 for _, s in score_changes.items() if s["churn_probability"] > 0.3)
    followup_incomplete = sum(1 for f in followups if not f["all_done"])
    
    lines.append("## 📊 Executive Summary")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|---|---|")
    lines.append(f"| Students monitored | {total_students} |")
    lines.append(f"| At risk (score > 0.3) | **{total_at_risk}** |")
    lines.append(f"| Top churn risks | {at_risk_count} highlighted below |")
    lines.append(f"| Returning from hold | {returning_count} |")
    lines.append(f"| Follow-ups incomplete | {followup_incomplete} |")
    lines.append("")
    
    # Section 1: Likely to Churn
    lines.append("---")
    lines.append(f"## 🚨 Top {at_risk_count} Students Likely to Churn")
    lines.append("")
    
    for i, student in enumerate(at_risk, 1):
        s = student
        lines.append(f"### {i}. {s['name']} — Churn Risk: **{s['churn']:.0%}**")
        if s.get("score_change"):
            delta = s["score_change"]
            arrow = "↑" if delta > 0.05 else "↓" if delta < -0.05 else "→"
            lines.append(f"*Score change: {arrow} {delta:+.0%}*")
        lines.append("")
        lines.append(f"**School:** {s['school']} | **Membership:** {s['membership']}")
        lines.append("")
        lines.append("**Why at risk:**")
        for reason in s["reasons"]:
            lines.append(f"- {reason}")
        lines.append("")
        lines.append("**Recommended actions:**")
        for action in s["actions"]:
            lines.append(f"- {action}")
        lines.append("")
    
    # Section 2: Returning from Hold
    if returning:
        lines.append("---")
        lines.append(f"## 🔙 Returning from On-Hold — Next 2 Weeks")
        lines.append("")
        
        for i, student in enumerate(returning, 1):
            s = student
            lines.append(f"### {i}. {s['name']} — Back after {s['days_on_hold']} days")
            lines.append(f"*On hold since: {s['end_date']} | Last lesson: {s['last_lesson'].date()} ({s['lesson_gap_days']} days ago)*")
            lines.append("")
            lines.append("**Talking points:**")
            for point in s["talking_points"]:
                lines.append(f"- {point}")
            lines.append("")
    
    # Section 3: Score Changes
    lines.append("---")
    lines.append(f"## 📈 Churn Score Changes (Week-over-Week)")
    lines.append("")
    
    if score_changes:
        changes = sorted(score_changes.items(), key=lambda x: abs(x[1].get("change", 0)), reverse=True)[:5]
        lines.append("| Student | Current Score | Change | Trend |")
        lines.append("|---|---|---|---|")
        for name, info in changes:
            curr = info["churn_probability"]
            change = info.get("change", 0) if info.get("change") is not None else 0
            arrow = "🔺" if change > 0.02 else "🔻" if change < -0.02 else "➖"
            lines.append(f"| {info.get('name', name)} | {curr:.0%} | {change:+.0%} | {arrow} |")
        lines.append("")
    else:
        lines.append("*No prior week data for comparison — baseline scores established this week.*")
        lines.append("")
    
    # Section 4: Follow-Ups
    if followups:
        lines.append("---")
        lines.append(f"## ✅ Follow-Up: Last Week's Recommendations")
        lines.append("")
        
        for f in followups:
            status_icon = "✅" if f["all_done"] else "⚠️"
            lines.append(f"### {status_icon} {f['student']}")
            for s in f["statuses"]:
                lines.append(f"- {s}")
            lines.append("")
    
    lines.append("---")
    lines.append(f"*Generated by Hermes Churn Intelligence | {TODAY.strftime('%Y-%m-%d %H:%M')}*")
    
    return "\n".join(lines)


# ─── MAIN ───

def main():
    print("🔍 Loading data...")
    lessons, notes, people, engagement, leavers, prev_recs = load_all_data()
    expanded = expand_students(lessons)
    
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    
    print("📊 Computing churn predictions...")
    model = load_model()
    predictions = predict_churn(expanded, notes, people, engagement, model)
    
    print(f"  {len(predictions)} active students scored")
    
    # Top at-risk
    scored = sorted(predictions.items(), key=lambda x: -x[1]["churn_probability"])
    at_risk = []
    
    for name, info in scored[:TOP_N_CHURN]:
        if info["churn_probability"] < 0.15:
            break
        reasons = generate_reasons(info["features"], name, con)
        actions = generate_actions(info["features"], info, name, con)
        
        # Check prior score
        prev = prev_recs.get("scores", {}).get(name, {})
        score_change = None
        if prev:
            score_change = info["churn_probability"] - prev.get("churn_probability", info["churn_probability"])
        
        at_risk.append({
            "name": info["name"], "churn": info["churn_probability"],
            "score_change": score_change, "school": info["school"],
            "membership": info["membership"], "reasons": reasons, "actions": actions,
        })
    
    print(f"  {len(at_risk)} students at elevated risk")
    
    # Returning students
    returning = find_returning_students(expanded, leavers)
    for student in returning:
        student["talking_points"] = generate_return_talking_points(student, con)
    
    print(f"  {len(returning)} students returning from hold")
    
    # Score changes
    score_changes = {}
    for name, info in predictions.items():
        prev = prev_recs.get("scores", {}).get(name, {})
        if prev:
            score_changes[name] = {
                "name": info["name"],
                "churn_probability": info["churn_probability"],
                "change": info["churn_probability"] - prev.get("churn_probability", info["churn_probability"]),
            }
    
    # Follow-ups
    followups = check_followups(prev_recs, con)
    
    # Generate email
    email = format_email(at_risk, returning, score_changes, followups, len(predictions))
    
    # Save recommendations for next week
    recs = {
        "week": str(TODAY),
        "generated": datetime.now().isoformat(),
        "recommendations": {},
        "scores": {name: {"churn_probability": info["churn_probability"]} for name, info in predictions.items()},
    }
    for student in at_risk:
        recs["recommendations"][student["name"].lower()] = {
            "name": student["name"],
            "churn": student["churn"],
            "actions": student["actions"],
        }
    
    with open(TRACKING_PATH, "w") as f:
        json.dump(recs, f, indent=2, default=str)
    
    con.close()
    
    # Output
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", help="Save to file")
    parser.add_argument("--send", action="store_true", help="Send email (not implemented)")
    args = parser.parse_args()
    
    if args.output:
        with open(args.output, "w") as f:
            f.write(email)
        print(f"📧 Saved to {args.output}")
    else:
        print("\n" + "=" * 70)
        print(email)
        print("=" * 70)
        print(f"\nStored {len(recs['scores'])} scores for next week's comparison")


if __name__ == "__main__":
    main()
