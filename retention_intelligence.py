#!/usr/bin/env python3
"""retention_intelligence.py v4 — Action tracking + Hold return system.

  • Tracks every suggestion made to the GM
  • On next report, checks whether actions were taken:
    - Did the GM call? (new call reviews with student name)
    - Constant instructor found? (future lessons show same teacher)
    - Hold ending soon? (2-week advance reminder to reach out)
    - Hold outreach detected? (did someone contact the parent?)
    - Hold return confirmed? (student back after hold end)
    - Parent responded to outreach? (new comms from parent)
    - No action? (flagged as stale — escalating urgency)
"""

import sqlite3, re, json, pickle, warnings
from pathlib import Path
from datetime import date, timedelta
from collections import defaultdict, Counter
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"
MATCHES_PATH = MODELS_DIR / "comms_name_matches.json"
HOLDS_PATH_WU = MODELS_DIR / "pike13_holds_westu-sor.json"
HOLDS_PATH_TH = MODELS_DIR / "pike13_holds_theheights-sor.json"
TRACKER_PATH = MODELS_DIR / "action_tracker.json"
LEAVERS_PATH = MODELS_DIR / "pike13_leavers.json"
TODAY = date.today()
SCHOOL_NAMES = {1: "West U", 2: "The Heights"}

# ═══════════════════════════════════════════════════════════
# ACTION TRACKER
# ═══════════════════════════════════════════════════════════

def load_action_tracker():
    if TRACKER_PATH.exists():
        return json.load(open(TRACKER_PATH))
    return {"reports": [], "students": {}}

def save_action_tracker(tracker):
    MODELS_DIR.mkdir(exist_ok=True)
    json.dump(tracker, open(TRACKER_PATH, "w"), indent=2)

def check_action_status(student_name, profile, tracker, data):
    """Check whether actions suggested last time were taken."""
    prev = tracker.get("students", {}).get(student_name.lower(), {})
    if not prev or not prev.get("last_suggested"):
        return []
    
    last_date = prev.get("last_suggested", "")
    suggestions = prev.get("suggestions", [])
    statuses = []
    
    try:
        last_dt = date.fromisoformat(last_date)
        days_ago = (TODAY - last_dt).days
    except:
        return []
    
    if days_ago < 3:
        return []
    
    # ── Check 1: Did someone call/reach out? ──
    need_outreach = any(s["type"] in ("action", "red_flag") for s in suggestions)
    if need_outreach:
        con = sqlite3.connect(str(DB_PATH))
        recent_reviews = pd.read_sql_query(f"""
            SELECT transcript_text, event_at FROM dialpad_call_reviews
            WHERE event_at >= '{last_date}' AND transcript_text IS NOT NULL
        """, con)
        con.close()
        
        contacted = any(student_name.lower() in str(r.get("transcript_text","") or "").lower()
                       for _, r in recent_reviews.iterrows())
        
        if contacted:
            statuses.append({"type":"status_ok","text":"✅ ACTION TAKEN: Student discussed in staff call since last report"})
        else:
            student_comms_since = sum(1 for c in profile.get("recent_comms",[]) if c["date"] >= last_date)
            if student_comms_since > 0:
                statuses.append({"type":"status_ok","text":f"✅ ACTION LIKELY: {student_comms_since} new parent communications since last report"})
            else:
                statuses.append({"type":"status_stale","text":f"⚠️ NO ACTION DETECTED — {days_ago}d since suggested outreach. Escalating urgency."})
    
    # ── Check 2: Instructor consistency improved? ──
    prev_inst = prev.get("instructor_consistency", 0)
    curr_inst = profile.get("instructor_consistency", 0)
    if prev_inst < 0.5 and curr_inst >= 0.5:
        statuses.append({"type":"status_ok","text":"✅ INSTRUCTOR STABILIZED: Consistency improved since last report"})
    
    # ── Check 3: Hold return tracking ──
    hold_info = profile.get("hold_info", {})
    hold_end_str = hold_info.get("hold_end", "")
    if hold_end_str:
        try:
            hold_end_dt = date.fromisoformat(hold_end_str)
        except:
            try:
                hold_end_dt = pd.Timestamp(hold_end_str).date()
            except:
                hold_end_dt = None
        
        if hold_end_dt:
            days_until = (hold_end_dt - TODAY).days
            
            if -7 <= days_until <= 14:
                # Hold ending soon or just ended — check outreach
                con = sqlite3.connect(str(DB_PATH))
                recent_reviews = pd.read_sql_query(f"""
                    SELECT transcript_text, event_at FROM dialpad_call_reviews
                    WHERE event_at >= '{last_date}' AND transcript_text IS NOT NULL
                """, con)
                con.close()
                student_mentioned = any(student_name.lower() in str(r.get("transcript_text","") or "").lower()
                                       for _, r in recent_reviews.iterrows())
                parent_comms_since = sum(1 for c in profile.get("recent_comms",[]) if c["date"] >= last_date)
                
                if student_mentioned or parent_comms_since > 0:
                    who = "Staff call" if student_mentioned else "Parent communication"
                    statuses.append({"type":"status_ok","text":f"✅ HOLD OUTREACH DETECTED: {who} since hold end reminder. {'Parent responded — confirm return.' if parent_comms_since > 0 else 'Check for parent response.'}"})
                else:
                    statuses.append({"type":"status_stale","text":f"⚠️ NO HOLD OUTREACH: {days_ago}d since reminder. Student returns in {days_until}d — reach out NOW."})
            
            elif days_until < -7:
                if profile.get("days_idle", 0) < 30:
                    statuses.append({"type":"status_ok","text":f"✅ HOLD RETURN CONFIRMED: Student back within 30d of hold ending {hold_end_str}"})
                else:
                    parent_comms_since = sum(1 for c in profile.get("recent_comms",[]) if c["date"] >= last_date)
                    if parent_comms_since > 0:
                        statuses.append({"type":"status_warn","text":f"⚠️ HOLD EXPIRED, PARENT CONTACTED BUT NOT RETURNED: {profile['days_idle']}d idle. Follow up."})
                    else:
                        statuses.append({"type":"status_stale","text":f"⚠️ HOLD EXPIRED, NO OUTREACH, NO RETURN: Hold ended {hold_end_str}. Student may be lost."})
    
    # ── Check 4: Risk changed? ──
    prev_risk = prev.get("risk", 0)
    curr_risk = profile.get("v12_risk", 0)
    if curr_risk < prev_risk - 0.1:
        statuses.append({"type":"status_ok","text":f"✅ IMPROVING: Risk dropped from {prev_risk:.0%} → {curr_risk:.0%}"})
    elif curr_risk > prev_risk + 0.1:
        statuses.append({"type":"status_stale","text":f"⚠️ WORSENING: Risk increased from {prev_risk:.0%} → {curr_risk:.0%}"})
    
    return statuses

def update_tracker(student_name, profile, suggestions, tracker):
    key = student_name.lower()
    tracker["students"][key] = {
        "last_suggested": str(TODAY),
        "risk": profile.get("v12_risk", 0),
        "archetype": profile.get("archetypes", [{}])[0].get("archetype", "?"),
        "instructor_consistency": profile.get("instructor_consistency", 0),
        "days_idle": profile.get("days_idle", 0),
        "suggestions": [{"type": s["type"], "text": s["text"][:200]} for s in suggestions],
    }
    if not tracker.get("reports"):
        tracker["reports"] = []
    if str(TODAY) not in [r["date"] for r in tracker.get("reports", [])]:
        tracker["reports"].append({"date": str(TODAY), "students_flagged": 0})

# ═══════════════════════════════════════════════════════════
# SENTIMENT KEYWORDS
# ═══════════════════════════════════════════════════════════

KEYWORD_CATEGORIES = {
    "cancellation": {"keywords": ["cancel", "stop lessons", "stopping lessons", "not coming back",
        "quit", "drop out", "no longer", "discontinue", "end lessons", "pulling him out", "pulling her out"]},
    "frustration": {"keywords": ["not happy", "disappointed", "frustrated", "unacceptable",
        "ridiculous", "fed up", "upset", "problem", "complaint", "not satisfied"]},
    "financial": {"keywords": ["bill", "billing", "charged", "credit card", "payment",
        "too expensive", "can't afford", "cost", "price", "invoice", "overdue"]},
    "scheduling_stress": {"keywords": ["completely forgot", "double booked", "conflict",
        "schedule conflict", "running late", "have to miss", "need to reschedule", "different time"]},
    "positive": {"keywords": ["love", "great", "amazing", "thank you so much", "excited",
        "fantastic", "wonderful", "awesome", "loves it", "really enjoys", "doing great",
        "making progress", "improving", "can't wait", "looking forward"]},
}

def categorize_text(text):
    hits = {}
    for cat, cfg in KEYWORD_CATEGORIES.items():
        count = sum(1 for kw in cfg["keywords"] if kw in text.lower())
        if count > 0: hits[cat] = count
    return hits

# ═══════════════════════════════════════════════════════════
# PHASE A: DATA SYNTHESIS
# ═══════════════════════════════════════════════════════════

def load_all_data():
    con = sqlite3.connect(str(DB_PATH))
    lessons = pd.read_sql_query("""
        SELECT l.*, ln.note_score, ln.notes_text, ln.note_completed
        FROM lessons l LEFT JOIN lesson_notes ln ON l.lesson_id = ln.lesson_id
        WHERE l.lesson_date IS NOT NULL ORDER BY l.lesson_date DESC
    """, con)
    lessons["lesson_date"] = pd.to_datetime(lessons["lesson_date"])
    
    phone_student = {}
    matches = json.load(open(MATCHES_PATH))
    for k, v in matches["matches"].items():
        if not k.startswith("call_") and "@" not in k and not k.startswith("sms_"):
            phone_student[k] = v["student"]
    ppl = pd.read_sql_query("SELECT full_name, phone, phone_normalized FROM pike13_people WHERE phone IS NOT NULL OR phone_normalized IS NOT NULL", con)
    for _, r in ppl.iterrows():
        n = str(r["full_name"]).strip()
        for c in ["phone", "phone_normalized"]:
            p = re.sub(r"\D", "", str(r.get(c, "") or ""))
            if p and n: phone_student[p] = n
    
    call_student = {k.replace("call_", ""): v["student"] for k, v in matches["matches"].items() if k.startswith("call_")}
    
    vms_by_phone = defaultdict(list)
    vms = pd.read_sql_query("SELECT external_number, transcription_text, date as created_at FROM dialpad_voicemails WHERE transcription_text IS NOT NULL", con)
    for _, r in vms.iterrows():
        phone = re.sub(r"\D", "", str(r["external_number"]))
        if phone: vms_by_phone[phone].append({"text": str(r["transcription_text"])[:1000], "date": str(r.get("created_at", ""))[:10]})
    
    sms_threads = pd.read_sql_query("SELECT thread_id, phone FROM dialpad_sms_threads WHERE phone IS NOT NULL", con)
    thread_phone = {str(r["thread_id"]): str(r["phone"]).strip() for _, r in sms_threads.iterrows()}
    sms_by_thread = defaultdict(list)
    sms = pd.read_sql_query("SELECT thread_id, body, message_at FROM dialpad_sms_messages WHERE body IS NOT NULL AND body != ''", con)
    for _, r in sms.iterrows(): sms_by_thread[str(r["thread_id"])].append({"text": str(r["body"])[:500], "date": str(r.get("message_at", ""))[:10]})
    
    reviews_by_call = {}
    reviews = pd.read_sql_query("SELECT call_id, transcript_text, recap_text, event_at FROM dialpad_call_reviews WHERE transcript_text IS NOT NULL OR recap_text IS NOT NULL", con)
    for _, r in reviews.iterrows(): reviews_by_call[str(r["call_id"])] = {"text": (str(r.get("transcript_text", "") or "") + " " + str(r.get("recap_text", "") or ""))[:1000], "date": str(r.get("event_at", ""))[:10]}
    
    holds = {}
    for path in [HOLDS_PATH_WU, HOLDS_PATH_TH]:
        if path.exists():
            for r in json.load(open(path)):
                c = r.get("Client", "").strip()
                if c: holds[c.lower()] = {"on_hold": r.get("On Hold?", "") == "Yes", "hold_start": r.get("Last Hold Start Date", ""), "hold_end": r.get("Last Hold End Date", ""), "hold_by": r.get("Last Hold By", ""), "plan": r.get("Plan Name", ""), "base_price": r.get("Base Price", ""), "account_managers": r.get("Account Managers", ""), "account_emails": r.get("Account Manager Emails", ""), "account_phones": r.get("Account Manager Phones", "")}
    
    con.close()
    print(f"  Lessons: {len(lessons)} | Phone→student: {len(phone_student)} | Holds: {len(holds)}")
    
    # Load Pike13 membership states for trial/enrolled detection
    con = sqlite3.connect(str(DB_PATH))
    ppl_states = pd.read_sql_query("SELECT full_name, membership_state FROM pike13_people WHERE full_name IS NOT NULL", con)
    con.close()
    pike13_states = {}
    for _, r in ppl_states.iterrows():
        name = str(r["full_name"]).strip().lower()
        if name not in pike13_states:
            pike13_states[name] = str(r.get("membership_state", "") or "")
    
    # Load Pike13 leaver data (last_membership_end dates)
    leavers = {}
    if LEAVERS_PATH.exists():
        leavers = json.load(open(LEAVERS_PATH))
    
    return {"lessons": lessons, "phone_student": phone_student, "call_student": call_student, "vms_by_phone": vms_by_phone, "sms_thread_phone": thread_phone, "sms_by_thread": sms_by_thread, "reviews_by_call": reviews_by_call, "holds": holds, "pike13_states": pike13_states, "leavers": leavers}

def build_student_profile(student_name, data):
    lessons = data["lessons"]
    mask = lessons["students_raw"].str.contains(student_name, na=False, case=False)
    sl = lessons[mask].sort_values("lesson_date", ascending=False)
    if len(sl) == 0: return None
    
    last_date = sl["lesson_date"].max()
    days_idle = (TODAY - last_date.date()).days if pd.notna(last_date) else 999
    
    scored = sl.dropna(subset=["note_score"])
    avg_score = float(scored["note_score"].mean()) if len(scored) > 0 else None
    scores_list = scored["note_score"].tolist()[:12]
    
    score_trend = "stable"
    if len(scores_list) >= 6:
        recent = np.mean(scores_list[:3]); older = np.mean(scores_list[3:6])
        if recent < older - 0.5: score_trend = "declining"
        elif recent > older + 0.5: score_trend = "improving"
    
    note_samples = []
    for _, r in sl.head(10).iterrows():
        txt = str(r.get("notes_text", "") or "").strip()
        if txt: note_samples.append({"date": str(r["lesson_date"].date()), "score": float(r["note_score"]) if pd.notna(r.get("note_score")) else None, "text": txt[:200]})
    
    all_dates = sl["lesson_date"].dropna().sort_values()
    avg_gap = None; is_weekly = is_biweekly = is_irregular = False
    if len(all_dates) >= 3:
        gaps = all_dates.diff().dropna().dt.days
        avg_gap = float(gaps.median())
        is_weekly = 5 <= avg_gap <= 9; is_biweekly = 10 <= avg_gap <= 18; is_irregular = gaps.std() > 10
    
    no_shows = sum(1 for _, r in sl.iterrows() if "no show" in str(r.get("notes_text", "") or "").lower() or "did not attend" in str(r.get("notes_text", "") or "").lower())
    inst_counts = sl["instructor_id"].value_counts()
    instructor_consistency = float(inst_counts.iloc[0] / len(sl)) if len(inst_counts) > 0 else 0.0
    instructor_changes = len(inst_counts) - 1
    
    all_comms = []; keyword_hits = defaultdict(int)
    student_phones = {phone for phone, s in data["phone_student"].items() if s.lower() == student_name.lower()}
    for phone in student_phones:
        for vm in data["vms_by_phone"].get(phone, []):
            all_comms.append({**vm, "source": "voicemail"})
            for cat, count in categorize_text(vm["text"]).items(): keyword_hits[cat] += count
    sms_tids = {tid for tid, phone in data["sms_thread_phone"].items() if re.sub(r"\D", "", phone) in student_phones}
    for tid in sms_tids:
        for m in data["sms_by_thread"].get(tid, []):
            all_comms.append({**m, "source": "sms"})
            for cat, count in categorize_text(m["text"]).items(): keyword_hits[cat] += count
    for cid, s in data["call_student"].items():
        if s.lower() == student_name.lower() and cid in data["reviews_by_call"]:
            cr = data["reviews_by_call"][cid]; all_comms.append({**cr, "source": "call_review"})
            for cat, count in categorize_text(cr["text"]).items(): keyword_hits[cat] += count
    
    all_comms.sort(key=lambda x: x.get("date", ""), reverse=True)
    hold_info = data["holds"].get(student_name.lower(), {})
    school_id = int(sl["school_id"].mode().iloc[0]) if len(sl) > 0 else 0
    
    # ── Check Pike13 membership data: enrolled? left? ──
    leavers = data.get("leavers", {})
    has_left = student_name.lower() in leavers
    left_date = leavers.get(student_name.lower(), {}).get("end_date", "") if has_left else ""
    
    membership_state = data.get("pike13_states", {}).get(student_name.lower(), "")
    state_lower = membership_state.lower()
    is_trial = any(t in state_lower for t in ["trial", "free trial", "camp", "workshop", "parent orientation", "immersion pass", "none", ""])
    is_enrolled = not is_trial and state_lower and membership_state.strip()
    
    best_note = None
    for ns in note_samples:
        if ns["text"] and len(ns["text"]) > 30 and "no show" not in ns["text"].lower() and "see you next time" not in ns["text"].lower():
            if ns.get("score") and ns["score"] >= 5:
                best_note = ns; break
    if not best_note:
        for ns in note_samples:
            if ns["text"] and len(ns["text"]) > 20 and "no show" not in ns["text"].lower():
                best_note = ns; break
    
    def _is_real_sms(text):
        bad = ["dialpad", "redacted", "web list snippet", "api message", "sms message", "sms web", "web snippet"]
        return not any(b in text.lower() for b in bad) and len(text) > 5
    
    sms_for_student = [c for c in all_comms if c["source"] == "sms" and _is_real_sms(c["text"])]
    recent_sms = [c["text"][:120] for c in sms_for_student[:5]]
    
    return {
        "student": student_name, "school_id": school_id, "school": SCHOOL_NAMES.get(school_id, "Unknown"),
        "total_lessons": len(sl), "days_idle": days_idle, "last_lesson": str(last_date.date()) if pd.notna(last_date) else "",
        "avg_score": avg_score, "score_trend": score_trend, "scores_recent": scores_list[:6],
        "note_samples": note_samples[:5], "best_note": best_note,
        "avg_lesson_gap": avg_gap, "is_weekly": is_weekly, "is_biweekly": is_biweekly, "is_irregular": is_irregular,
        "no_shows": no_shows, "instructor_consistency": instructor_consistency, "instructor_changes": instructor_changes,
        "comm_count": len(all_comms), "recent_comms": all_comms[:10], "recent_sms": recent_sms,
        "keyword_hits": dict(keyword_hits),
        "has_cancellation": keyword_hits.get("cancellation", 0) > 0,
        "has_frustration": keyword_hits.get("frustration", 0) > 0,
        "has_financial": keyword_hits.get("financial", 0) > 0,
        "has_scheduling": keyword_hits.get("scheduling_stress", 0) > 0,
        "has_positive": keyword_hits.get("positive", 0) > 0,
        "is_on_hold": hold_info.get("on_hold", False), "hold_info": hold_info,
        "membership_state": membership_state, "is_trial": is_trial, "is_enrolled": is_enrolled,
        "has_left": has_left, "left_date": left_date,
    }

# ═══════════════════════════════════════════════════════════
# PHASE B: DYNAMIC ADVICE ENGINE
# ═══════════════════════════════════════════════════════════

def generate_advice(profile):
    p = profile
    advice = []
    
    if p.get("best_note"):
        bn = p["best_note"]
        score_str = f" (scored {bn['score']:.0f})" if bn.get("score") is not None else ""
        advice.append({"type": "hook", "text": f"Their last strong session was {bn['date']}{score_str}. The instructor wrote: \"{bn['text'][:150]}\""})
    
    if p.get("score_trend") == "declining" and p.get("avg_score") is not None:
        advice.append({"type": "concern", "text": f"Scores have been declining (current avg: {p['avg_score']:.1f}). They may be hitting a plateau or losing motivation."})
    elif p.get("score_trend") == "improving" and p.get("avg_score") is not None:
        advice.append({"type": "positive", "text": f"Scores were improving before they stopped (avg: {p['avg_score']:.1f}). They were on an upward trajectory — remind them of this."})
    
    if p.get("has_cancellation"):
        advice.append({"type": "red_flag", "text": "⚠️ Parent has used cancel/discontinue language in communications. This is the most urgent signal — direct outreach needed."})
    if p.get("has_frustration"):
        advice.append({"type": "red_flag", "text": "⚠️ Parent has expressed frustration in voicemails or messages. Acknowledge the issue directly — don't deflect."})
    if p.get("has_financial"):
        advice.append({"type": "context", "text": "Financial/billing topics have come up in parent communications. Consider whether pricing is a factor."})
    if p.get("has_scheduling"):
        advice.append({"type": "context", "text": "Parent has mentioned scheduling challenges. Their issue may be logistics, not interest — try offering a different time or format."})
    
    if p["comm_count"] == 0:
        advice.append({"type": "gap", "text": "No parent communications on file. Collect phone/email — there's no way to reach them currently."})
    elif p["comm_count"] > 20:
        advice.append({"type": "positive", "text": f"Parent has been very communicative ({p['comm_count']} messages/calls). They're engaged — use that relationship."})
    
    if p.get("recent_sms"):
        first_sms = p["recent_sms"][0]
        if not any(b in first_sms.lower() for b in ["dialpad", "redacted", "web list", "api"]):
            advice.append({"type": "context", "text": f"Recent parent message: \"{first_sms[:100]}\""})
    
    if p["total_lessons"] < 10:
        advice.append({"type": "context", "text": f"Only {p['total_lessons']} lessons on record — still in onboarding. Standard welcome outreach, confirm they understand the program."})
    
    if p.get("instructor_changes", 0) >= 2:
        advice.append({"type": "concern", "text": f"They've had {p['instructor_changes']} different instructors — low consistency. Ask if they've connected with any particular teacher."})
    
    if p.get("no_shows", 0) >= 2:
        advice.append({"type": "concern", "text": f"{p['no_shows']} no-shows in recent history. Ask about schedule conflicts or communication breakdowns."})
    
    # Hold-specific advice
    hold_info = p.get("hold_info", {})
    if hold_info.get("hold_end"):
        try:
            hold_end_dt = date.fromisoformat(hold_info["hold_end"])
        except:
            try:
                hold_end_dt = pd.Timestamp(hold_info["hold_end"]).date()
            except:
                hold_end_dt = None
        if hold_end_dt:
            days_until = (hold_end_dt - TODAY).days
            if 0 <= days_until <= 14:
                advice.append({"type": "hold_action", "text": f"HOLD: Return date in {days_until}d ({hold_info['hold_end']}). Confirm lesson time with parent, remind them to come back. Check for response by next report."})
            elif days_until < 0 and p["days_idle"] < 30:
                advice.append({"type": "hold_returned", "text": f"HOLD: Student returned from hold ending {hold_info['hold_end']}. Verify schedule is consistent."})
            elif days_until < 0:
                advice.append({"type": "hold_expired", "text": f"HOLD: Hold ended {hold_info['hold_end']} but student not back ({p['days_idle']}d). Escalate — did anyone reach out? What happened?"})
    
    if p["days_idle"] > 45:
        if p.get("has_positive") and not p.get("has_cancellation"):
            advice.append({"type": "action", "text": "They were doing well before they stopped and parent is positive. This is likely a life/schedule issue, not dissatisfaction. Reach out with warmth, not urgency."})
        elif p.get("has_cancellation"):
            advice.append({"type": "action", "text": "DIRECT OUTREACH. Parent has signaled intent to leave. GM should call personally. Reference their progress, acknowledge any concerns, offer a solution."})
        elif p["comm_count"] == 0:
            advice.append({"type": "action", "text": "Silent disengagement — no comms, no notes indicating why they stopped. Try to reach parent, but don't push if unresponsive."})
        else:
            advice.append({"type": "action", "text": f"No contact for {p['days_idle']} days. Reach out directly — ask if there was a specific reason they stopped. Reference what they were working on."})
    elif p["days_idle"] > 14:
        advice.append({"type": "action", "text": f"{p['days_idle']} days since last lesson. Gentle check-in — 'just wanted to make sure everything's okay, we'd love to see you back.'"})
    else:
        advice.append({"type": "action", "text": "Still attending but flagged as at-risk by model. Review note quality and instructor consistency before reaching out."})
    
    return advice

def classify_archetypes(profile):
    if profile["is_on_hold"]:
        hi = profile.get("hold_info", {})
        end = hi.get("hold_end", "check Pike13")
        return [{"archetype": "On Hold", "speed": f"ends {end}", "confidence": "high",
                 "playbook": generate_advice(profile)}]
    
    signals = []
    if profile["days_idle"] > 30 and profile["total_lessons"] >= 5:
        speed = "rapid" if profile["days_idle"] < 60 and profile.get("is_weekly") else "gradual"
        signals.append(("Disengagement", speed, "high" if profile["days_idle"] > 45 else "medium"))
    if profile.get("has_cancellation") or profile.get("has_frustration"):
        signals.append(("Comm Red Flags", "urgent", "high"))
    if profile.get("has_financial"):
        signals.append(("Financial Stress", "threshold", "high" if profile.get("has_financial") else "medium"))
    if profile.get("is_irregular") or profile.get("has_scheduling") or profile.get("no_shows", 0) >= 2:
        signals.append(("Schedule Conflict", "ongoing", "high" if profile.get("has_scheduling") else "medium"))
    if profile.get("instructor_changes", 0) >= 2 and profile["total_lessons"] >= 10:
        signals.append(("Instructor Relationship", "threshold", "high" if profile["instructor_changes"] >= 3 else "medium"))
    if profile.get("score_trend") == "declining" and profile.get("avg_score") is not None and profile["avg_score"] < 5:
        signals.append(("Quality Fade", "gradual", "high" if profile["avg_score"] < 3.5 else "medium"))
    if profile["total_lessons"] < 10 and profile["days_idle"] > 14:
        signals.append(("New Student Risk", "early", "medium"))
    
    priority = {"Comm Red Flags": 0, "Disengagement": 1, "Financial Stress": 2, "Schedule Conflict": 3, "Instructor Relationship": 4, "Quality Fade": 5, "New Student Risk": 6}
    signals.sort(key=lambda x: (priority.get(x[0], 99), -{"high":3,"medium":2}.get(x[2],1)))
    
    matches = []
    for name, speed, conf in signals[:2]:
        matches.append({"archetype": name, "speed": speed, "confidence": conf, "playbook": generate_advice(profile)})
    
    if not matches:
        matches.append({"archetype": "Review Needed", "speed": "n/a", "confidence": "low", "playbook": generate_advice(profile)})
    
    return matches

# ═══════════════════════════════════════════════════════════
# REPORT GENERATION
# ═══════════════════════════════════════════════════════════

def generate_reports(profiles, risk_scores, data):
    tracker = load_action_tracker()
    
    for school_id, school_name in SCHOOL_NAMES.items():
        school_profiles = [p for p in profiles if p["school_id"] == school_id]
        if not school_profiles: continue
        
        for p in school_profiles:
            match = risk_scores[risk_scores["student_name"].str.lower() == p["student"].lower()]
            p["v12_risk"] = float(match["risk"].values[0]) if len(match) > 0 else 0.0
            p["archetypes"] = classify_archetypes(p)
        
        school_profiles.sort(key=lambda x: x["v12_risk"], reverse=True)
        
        on_hold = [p for p in school_profiles if p["is_on_hold"]]
        critical = [p for p in school_profiles if p["v12_risk"] >= 0.70 and not p["is_on_hold"]]
        high = [p for p in school_profiles if 0.50 <= p["v12_risk"] < 0.70 and not p["is_on_hold"]]
        watch = [p for p in school_profiles if 0.30 <= p["v12_risk"] < 0.50 and not p["is_on_hold"]]
        
        active_critical = [p for p in critical if p["days_idle"] < 120]
        historical = [p for p in critical if p["days_idle"] >= 120]
        
        # ── Separate known leavers, trial, and true actionable ──
        confirmed_left = [p for p in active_critical if p.get("has_left", False)]
        trial_inactive = [p for p in active_critical if p.get("is_trial", False) and p["days_idle"] > 60 and not p.get("has_left")]
        active_critical = [p for p in active_critical if not p.get("has_left") and not (p.get("is_trial", False) and p["days_idle"] > 60)]
        
        # ── Classify holds BEFORE summary ──
        holds_ending_soon = []
        holds_future = []
        holds_expired = []
        holds_returned = []
        
        for p in on_hold:
            hi = p.get("hold_info", {})
            end_str = hi.get("hold_end", "")
            if not end_str:
                holds_future.append(p)
                continue
            try:
                end_dt = date.fromisoformat(end_str)
            except:
                try:
                    end_dt = pd.Timestamp(end_str).date()
                except:
                    holds_future.append(p)
                    continue
            
            days_until = (end_dt - TODAY).days
            if days_until < 0:
                if p["days_idle"] < 30:
                    holds_returned.append(p)
                else:
                    holds_expired.append(p)
            elif days_until <= 14:
                holds_ending_soon.append((days_until, p))
            else:
                holds_future.append(p)
        
        holds_ending_soon.sort(key=lambda x: x[0])
        
        active_critical.sort(key=lambda x: (
            0 if any(a["archetype"] != "Disengagement" for a in x.get("archetypes", [])) else 1,
            -x["v12_risk"]
        ))
        
        lines = [f"Subject: 🎸 {school_name} Retention Intelligence — {TODAY.strftime('%B %d, %Y')}", "",
                 f"Hi Hugh,", "",
                 f"Every recommendation below is drawn from actual lesson notes, parent communications,",
                 f"and attendance data — not a template. Each student's advice is unique to their situation.", ""]
        
        lines.append("📋 SUMMARY")
        lines.append("")
        lines.append(f"   ⏰ Ending soon: {len(holds_ending_soon)}  |  ⚠️ Expired: {len(holds_expired)}  |  ⏸️  Future: {len(holds_future)}")
        lines.append(f"   🔴 Actionable:  {len(active_critical)}  |  🟠 High:  {len(high)}  |  🟡 Watch:  {len(watch)}")
        lines.append(f"   ✅ Confirmed Left: {len(confirmed_left)}  |  📦 Trial: {len(trial_inactive)}  |  📦 Historical: {len(historical)}")
        lines.append("")
        
        # ── HOLD ENDING SOON (highest priority — above actionable) ──
        if holds_ending_soon:
            lines.append("─" * 72)
            lines.append(f"⏰ HOLD ENDING SOON — {len(holds_ending_soon)} students (reach out now)")
            lines.append("─" * 72)
            lines.append("")
            for days_left, p in holds_ending_soon[:10]:
                hi = p.get("hold_info", {})
                plan = hi.get("plan", "")[:50]
                urgency = f"ends in {days_left}d" if days_left > 0 else "ends TODAY"
                lines.append(f"  • {p['student']} — {urgency}  |  {plan}")
                lines.append(f"    → GM/contact: reach out NOW. Confirm return date, schedule first lesson back.")
                lines.append(f"    → HOLD ACTION: Check for parent response by next report.")
                # Action status
                action_status = check_action_status(p["student"], p, tracker, data)
                for s in action_status:
                    lines.append(f"     {s['text']}")
                if p.get("archetypes") and p["archetypes"][0].get("playbook"):
                    update_tracker(p["student"], p, p["archetypes"][0]["playbook"], tracker)
                lines.append("")
        
        # ── HOLD EXPIRED (urgent — students who didn't return) ──
        if holds_expired:
            lines.append("─" * 72)
            lines.append(f"⚠️ HOLD EXPIRED — {len(holds_expired)} students not yet returned")
            lines.append("─" * 72)
            lines.append("")
            for p in holds_expired[:10]:
                hi = p.get("hold_info", {})
                end_str = hi.get("hold_end", "?")
                lines.append(f"  • {p['student']} — hold ended {end_str}  |  {p['days_idle']}d since last lesson")
                lines.append(f"    → Escalate: did GM reach out? Did parent respond? If not, student may be lost.")
                action_status = check_action_status(p["student"], p, tracker, data)
                for s in action_status:
                    lines.append(f"     {s['text']}")
                if p.get("archetypes") and p["archetypes"][0].get("playbook"):
                    update_tracker(p["student"], p, p["archetypes"][0]["playbook"], tracker)
                lines.append("")
        
        # ── HOLD RETURNED (good news) ──
        if holds_returned:
            lines.append(f"✅ Hold returns confirmed: {len(holds_returned)} students back from hold. No action needed.")
            lines.append("")
        
        # ── HOLD FUTURE (not urgent) ──
        if holds_future:
            lines.append("─" * 72)
            lines.append(f"⏸️  ON HOLD (future) — {len(holds_future)} students")
            lines.append("─" * 72)
            lines.append("")
            for p in holds_future[:10]:
                hi = p.get("hold_info", {})
                lines.append(f"  • {p['student']} — hold ends {hi.get('hold_end', '?')}  |  {hi.get('plan', '')[:50]}")
            lines.append("")
        
        # ── ACTIONABLE CRITICAL ──
        if active_critical:
            lines.append("─" * 72)
            lines.append(f"🔴 ACTIONABLE — {len(active_critical)} students (contact this week)")
            lines.append("─" * 72)
            lines.append("")
            
            # Batch dedup
            deduped = []
            skip_idx = set()
            for i, p in enumerate(active_critical):
                if i in skip_idx: continue
                cluster = [p]
                for j, q in enumerate(active_critical):
                    if j <= i or j in skip_idx: continue
                    if (p["days_idle"] == q["days_idle"] and 
                        p["total_lessons"] == q["total_lessons"] and
                        p["v12_risk"] == q["v12_risk"] and
                        p.get("comm_count", 0) == 0 and q.get("comm_count", 0) == 0 and
                        not p.get("best_note") and not q.get("best_note") and
                        p["total_lessons"] < 10):
                        cluster.append(q)
                        skip_idx.add(j)
                
                if len(cluster) >= 3:
                    names = [c["student"] for c in cluster]
                    arch = cluster[0].get("archetypes", [])
                    deduped.append({"batch": True, "names": names, "count": len(names),
                                    "profile": cluster[0],
                                    "primary": arch[0] if arch else {},
                                    "secondary": arch[1] if len(arch) > 1 else None})
                else:
                    deduped.append({"batch": False, "profile": p,
                                    "primary": p.get("archetypes", [{}])[0],
                                    "secondary": p.get("archetypes", [{}])[1] if len(p.get("archetypes", [])) > 1 else None})
            
            for item in deduped[:15]:
                if item["batch"]:
                    p = item["profile"]
                    primary = item["primary"]
                    badge = f"⚠️ {p['days_idle']}d idle" if p['days_idle'] > 30 else f"📝 {p['total_lessons']} lessons"
                    lines.append(f"  📦 {item['count']} STUDENTS — {p['v12_risk']:.0%} risk each | {badge}")
                    lines.append(f"  🏷️  {primary.get('archetype', '?')} ({primary.get('speed', '?')}) — {primary.get('confidence', '?')} confidence")
                    name_str = ", ".join(item["names"][:4])
                    if len(item["names"]) > 4: name_str += f" +{len(item['names'])-4} more"
                    lines.append(f"     Students: {name_str}")
                    for advice_item in primary.get("playbook", []):
                        prefix = {"hook": "💬", "red_flag": "🚩", "action": "→", "concern": "⚠️", "positive": "✅", "context": "📋", "gap": "❓", "hold_action": "⏰", "hold_returned": "✅", "hold_expired": "⚠️"}.get(advice_item["type"], "•")
                        lines.append(f"     {prefix} {advice_item['text']}")
                    if primary.get("playbook"):
                        update_tracker(item["names"][0], p, primary["playbook"], tracker)
                    lines.append("")
                    continue
                
                p = item["profile"]
                primary = item["primary"]
                secondary = item["secondary"]
                
                badge = f"⚠️ {p['days_idle']}d idle" if p['days_idle'] > 30 else f"📝 {p['total_lessons']} lessons"
                lines.append(f"  {p['student']} — {p['v12_risk']:.0%} risk | {badge}")
                lines.append(f"  🏷️  {primary.get('archetype', '?')} ({primary.get('speed', '?')}) — {primary.get('confidence', '?')} confidence")
                if secondary: lines.append(f"     Also: {secondary['archetype']}")
                
                for advice_item in primary.get("playbook", []):
                    prefix = {"hook": "💬", "red_flag": "🚩", "action": "→", "concern": "⚠️", "positive": "✅", "context": "📋", "gap": "❓", "hold_action": "⏰", "hold_returned": "✅", "hold_expired": "⚠️"}.get(advice_item["type"], "•")
                    lines.append(f"     {prefix} {advice_item['text']}")
                
                hi = p.get("hold_info", {})
                contact = []
                if hi.get("account_emails"): contact.append(f"✉️ {hi['account_emails']}")
                if hi.get("account_phones"): contact.append(f"📞 {hi['account_phones']}")
                if contact: lines.append(f"     Contact: {' | '.join(contact)}")
                
                action_status = check_action_status(p["student"], p, tracker, data)
                for s in action_status:
                    lines.append(f"     {s['text']}")
                if primary.get("playbook"):
                    update_tracker(p["student"], p, primary["playbook"], tracker)
                
                lines.append("")
        
        # ── CONFIRMED LEFT (Pike13 last_membership_end) ──
        if confirmed_left:
            lines.append("─" * 72)
            lines.append(f"✅ CONFIRMED LEFT — {len(confirmed_left)} students with Pike13 end dates")
            lines.append("─" * 72)
            lines.append("")
            for p in confirmed_left[:10]:
                lines.append(f"  • {p['student']} — membership ended {p.get('left_date', '?')}  |  {p['total_lessons']} lessons | {p['days_idle']}d idle")
            if len(confirmed_left) > 10:
                lines.append(f"     +{len(confirmed_left)-10} more")
            lines.append(f"     → Students with confirmed Pike13 membership end dates. No outreach needed.")
            lines.append("")
        
        # ── TRIAL / NON-CONVERTED ──
        if trial_inactive:
            lines.append("─" * 72)
            lines.append(f"📦 TRIAL / NON-CONVERTED — {len(trial_inactive)} students (never enrolled, idle 60+d)")
            lines.append("─" * 72)
            lines.append("")
            for p in trial_inactive[:8]:
                state = p.get("membership_state", "Unknown")
                lines.append(f"  • {p['student']} — {state} | {p['total_lessons']} lessons | {p['days_idle']}d idle")
            lines.append(f"     → These students were trial/camp/workshop — never paying customers.")
            lines.append(f"     → Review quarterly. If any had good scores/notes, flag for re-engagement campaign.")
            lines.append("")
        
        # ── HISTORICAL ──
        if historical:
            lines.append("─" * 72)
            lines.append(f"📦 HISTORICAL CHURN — {len(historical)} students idle 120+ days")
            lines.append("─" * 72)
            lines.append("")
            hist_by_arch = defaultdict(int)
            for p in historical:
                arch = p.get("archetypes", [])
                hist_by_arch[arch[0]["archetype"] if arch else "Unknown"] += 1
            for arch, count in sorted(hist_by_arch.items(), key=lambda x: -x[1]):
                lines.append(f"  {arch}: {count}")
            lines.append("")
        
        # ── HIGH ──
        if high:
            lines.append("─" * 72)
            lines.append(f"🟠 HIGH — {len(high)} students")
            lines.append("─" * 72)
            lines.append("")
            for p in high[:8]:
                arch = p.get("archetypes", [])
                primary = arch[0] if arch else {}
                lines.append(f"  • {p['student']} — {p['v12_risk']:.0%} | {primary.get('archetype', '?')} ({primary.get('speed', '?')})")
            lines.append("")
        
        out_path = MODELS_DIR / f"retention_intel_{school_name.replace(' ', '_')}.txt"
        open(out_path, "w").write("\n".join(lines))
        print(f"  {school_name}: {out_path} ({len(lines)} lines)")
    
    if tracker.get("reports"):
        tracker["reports"][-1]["students_flagged"] = sum(1 for s in tracker.get("students", {}).values() if s.get("last_suggested") == str(TODAY))
    save_action_tracker(tracker)
    
    json_out = [{"student": p["student"], "school": p["school"], "v12_risk": p.get("v12_risk", 0),
                 "archetypes": p.get("archetypes", []), "days_idle": p["days_idle"],
                 "avg_score": p["avg_score"], "score_trend": p["score_trend"],
                 "keyword_hits": p["keyword_hits"], "is_on_hold": p["is_on_hold"]} for p in profiles]
    json.dump(json_out, open(MODELS_DIR / "retention_intelligence.json", "w"), indent=2)
    print(f"  JSON: {MODELS_DIR / 'retention_intelligence.json'} ({len(json_out)} students)")

def main():
    print("retention_intelligence.py v4 — Action tracking + Hold return system")
    print()
    data = load_all_data()
    risk_scores = pd.read_csv(MODELS_DIR / "v11_risk_scores.csv")
    active = risk_scores[risk_scores["risk"] >= 0.10]
    profiles = [p for name in active["student_name"] if (p := build_student_profile(str(name), data))]
    print(f"  Profiles: {len(profiles)}")
    generate_reports(profiles, risk_scores, data)
    print("\nDone.")

if __name__ == "__main__":
    main()
