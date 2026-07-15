#!/usr/bin/env python3
"""retention_intelligence.py — Phase A + B: Unified profiles + archetype classification.

Phase A: Data Synthesis
  Merges all data sources into a unified student profile:
    • v12 risk scores
    • Lesson history (scores, notes text, attendance patterns, instructor)
    • Communication history (voicemails, SMS, call reviews — phone + name matched)
    • Keyword sentiment (cancellation, financial, frustration, positive, etc.)
    • Pike13 hold data with dates
    • Plan details (type, price, account manager)

Phase B: Archetype Classification
  For each flagged student, detects which of 7 churn patterns is present,
  assigns primary + secondary archetypes, and generates an intervention playbook.

Output: models/retention_intelligence.json + per-school reports
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
V12_MODEL_PATH = MODELS_DIR / "v12_model.pkl"
TODAY = date.today()

SCHOOL_NAMES = {1: "West U", 2: "The Heights"}

# ═══════════════════════════════════════════════════════════
# SENTIMENT KEYWORD CATEGORIES
# ═══════════════════════════════════════════════════════════

KEYWORD_CATEGORIES = {
    "cancellation": {
        "keywords": ["cancel", "stop lessons", "stopping lessons", "not coming back",
            "quit", "quitting", "drop out", "dropping out", "no longer", "discontinue",
            "end lessons", "not continue", "pulling him out", "pulling her out",
            "won't be attending anymore", "not going to continue"],
        "archetype": "Comm Red Flags",
    },
    "frustration": {
        "keywords": ["not happy", "disappointed", "frustrated", "unacceptable",
            "ridiculous", "fed up", "upset", "problem", "issue with", "complaint",
            "not satisfied", "waste of time", "waste of money"],
        "archetype": "Comm Red Flags",
    },
    "financial": {
        "keywords": ["bill", "billing", "charged", "charge", "credit card",
            "payment", "too expensive", "can't afford", "cost", "price", "pricing",
            "invoice", "overdue", "past due", "how much"],
        "archetype": "Financial Stress",
    },
    "scheduling_stress": {
        "keywords": ["completely forgot", "so sorry", "double booked", "conflict",
            "schedule conflict", "can't make", "running late", "have to miss",
            "need to reschedule", "need to move", "different time", "another day",
            "won't be able to make", "missed"],
        "archetype": "Schedule Conflict",
    },
    "positive": {
        "keywords": ["love", "great", "amazing", "thank you so much", "excited",
            "fantastic", "wonderful", "awesome", "incredible", "best", "so happy",
            "thrilled", "loves it", "really enjoys", "having a blast", "doing great",
            "making progress", "improving", "can't wait", "looking forward"],
        "archetype": "Retention Signal",
    },
}


def categorize_text(text):
    hits = {}
    text_lower = text.lower()
    for cat, cfg in KEYWORD_CATEGORIES.items():
        count = sum(1 for kw in cfg["keywords"] if kw in text_lower)
        if count > 0:
            hits[cat] = count
    return hits


# ═══════════════════════════════════════════════════════════
# PHASE A: DATA SYNTHESIS
# ═══════════════════════════════════════════════════════════

def load_all_data():
    """Load and cross-reference all data sources into memory."""
    con = sqlite3.connect(str(DB_PATH))
    
    # ── Lessons with notes ──
    lessons = pd.read_sql_query("""
        SELECT l.*, ln.note_score, ln.notes_text, ln.note_completed
        FROM lessons l
        LEFT JOIN lesson_notes ln ON l.lesson_id = ln.lesson_id
        WHERE l.lesson_date IS NOT NULL
        ORDER BY l.lesson_date DESC
    """, con)
    lessons["lesson_date"] = pd.to_datetime(lessons["lesson_date"])
    print(f"  Lessons: {len(lessons)}")
    
    # ── Phone → student mappings ──
    phone_student = {}
    matches = json.load(open(MATCHES_PATH))
    for k, v in matches["matches"].items():
        if not k.startswith("call_") and "@" not in k and not k.startswith("sms_"):
            phone_student[k] = v["student"]
    
    ppl = pd.read_sql_query("""
        SELECT full_name, phone, phone_normalized FROM pike13_people
        WHERE phone IS NOT NULL OR phone_normalized IS NOT NULL
    """, con)
    for _, r in ppl.iterrows():
        name = str(r["full_name"]).strip()
        for col in ["phone", "phone_normalized"]:
            p = re.sub(r"\D", "", str(r.get(col, "") or ""))
            if p and name:
                phone_student[p] = name
    print(f"  Phone→student: {len(phone_student)}")
    
    # ── Call ID → student (from name matches) ──
    call_student = {}
    for k, v in matches["matches"].items():
        if k.startswith("call_"):
            call_student[k.replace("call_", "")] = v["student"]
    
    # ── Voicemails indexed by phone ──
    vms_by_phone = defaultdict(list)
    vms = pd.read_sql_query("""
        SELECT external_number, transcription_text, date as created_at
        FROM dialpad_voicemails WHERE transcription_text IS NOT NULL
    """, con)
    for _, r in vms.iterrows():
        phone = re.sub(r"\D", "", str(r["external_number"]))
        if phone:
            vms_by_phone[phone].append({
                "text": str(r["transcription_text"])[:1000],
                "date": str(r.get("created_at", ""))[:10],
            })
    print(f"  Voicemails indexed: {sum(len(v) for v in vms_by_phone.values())}")
    
    # ── SMS by thread phone ──
    sms_threads = pd.read_sql_query(
        "SELECT thread_id, phone FROM dialpad_sms_threads WHERE phone IS NOT NULL", con)
    thread_phone = {str(r["thread_id"]): str(r["phone"]).strip()
                    for _, r in sms_threads.iterrows()}
    
    sms_by_thread = defaultdict(list)
    sms = pd.read_sql_query("""
        SELECT thread_id, body, message_at FROM dialpad_sms_messages
        WHERE body IS NOT NULL AND body != ''
    """, con)
    for _, r in sms.iterrows():
        sms_by_thread[str(r["thread_id"])].append({
            "text": str(r["body"])[:500],
            "date": str(r.get("message_at", ""))[:10],
        })
    
    # ── Call reviews by call_id ──
    reviews_by_call = {}
    reviews = pd.read_sql_query("""
        SELECT call_id, transcript_text, recap_text, event_at
        FROM dialpad_call_reviews
        WHERE transcript_text IS NOT NULL OR recap_text IS NOT NULL
    """, con)
    for _, r in reviews.iterrows():
        reviews_by_call[str(r["call_id"])] = {
            "text": (str(r.get("transcript_text", "") or "") + " " +
                     str(r.get("recap_text", "") or ""))[:1000],
            "date": str(r.get("event_at", ""))[:10],
        }
    print(f"  Call reviews indexed: {len(reviews_by_call)}")
    
    # ── Pike13 hold data ──
    holds = {}
    for path, school in [(HOLDS_PATH_WU, "West U"), (HOLDS_PATH_TH, "The Heights")]:
        if path.exists():
            data = json.load(open(path))
            for r in data:
                client = r.get("Client", "").strip()
                if client:
                    holds[client.lower()] = {
                        "on_hold": r.get("On Hold?", "") == "Yes",
                        "hold_start": r.get("Last Hold Start Date", ""),
                        "hold_end": r.get("Last Hold End Date", ""),
                        "hold_by": r.get("Last Hold By", ""),
                        "plan": r.get("Plan Name", ""),
                        "base_price": r.get("Base Price", ""),
                        "account_managers": r.get("Account Managers", ""),
                        "account_emails": r.get("Account Manager Emails", ""),
                        "account_phones": r.get("Account Manager Phones", ""),
                        "school": school,
                    }
        print(f"  {school} holds: {len([h for h in holds.values() if h['on_hold']])}")
    
    con.close()
    
    return {
        "lessons": lessons,
        "phone_student": phone_student,
        "call_student": call_student,
        "vms_by_phone": vms_by_phone,
        "sms_thread_phone": thread_phone,
        "sms_by_thread": sms_by_thread,
        "reviews_by_call": reviews_by_call,
        "holds": holds,
    }


def build_student_profile(student_name, data):
    """Build a unified profile for one student."""
    lessons = data["lessons"]
    mask = lessons["students_raw"].str.contains(student_name, na=False, case=False)
    sl = lessons[mask].sort_values("lesson_date", ascending=False)
    
    if len(sl) == 0:
        return None
    
    last_date = sl["lesson_date"].max()
    days_idle = (TODAY - last_date.date()).days if pd.notna(last_date) else 999
    
    # Notes
    scored = sl.dropna(subset=["note_score"])
    avg_score = float(scored["note_score"].mean()) if len(scored) > 0 else None
    scores_list = scored["note_score"].tolist()[:12] if len(scored) > 0 else []
    
    # Score trend: last 3 vs previous 3
    score_trend = "stable"
    if len(scores_list) >= 6:
        recent = np.mean(scores_list[:3])
        older = np.mean(scores_list[3:6])
        if recent < older - 0.5: score_trend = "declining"
        elif recent > older + 0.5: score_trend = "improving"
    
    # Note text samples
    note_samples = []
    for _, r in sl.head(10).iterrows():
        txt = str(r.get("notes_text", "") or "").strip()
        if txt:
            note_samples.append({
                "date": str(r["lesson_date"].date()),
                "score": float(r["note_score"]) if pd.notna(r.get("note_score")) else None,
                "text": txt[:200],
            })
    
    # Attendance pattern
    all_dates = sl["lesson_date"].dropna().sort_values()
    avg_gap = None
    is_weekly = is_biweekly = is_irregular = False
    if len(all_dates) >= 3:
        gaps = all_dates.diff().dropna().dt.days
        avg_gap = float(gaps.median())
        is_weekly = 5 <= avg_gap <= 9
        is_biweekly = 10 <= avg_gap <= 18
        is_irregular = gaps.std() > 10 if len(gaps) >= 3 else False
    
    # No-shows (lessons with no notes or "no show" in notes)
    no_shows = sum(1 for _, r in sl.iterrows()
                   if "no show" in str(r.get("notes_text", "") or "").lower()
                   or "did not attend" in str(r.get("notes_text", "") or "").lower())
    
    # Instructor consistency
    inst_counts = sl["instructor_id"].value_counts()
    instructor_consistency = float(inst_counts.iloc[0] / len(sl)) if len(inst_counts) > 0 else 0.0
    instructor_changes = len(inst_counts) - 1  # how many different instructors
    
    # ── Communications ──
    all_comms = []
    keyword_hits = defaultdict(int)
    
    # Find phones for this student
    student_phones = set()
    for phone, s in data["phone_student"].items():
        if s.lower() == student_name.lower():
            student_phones.add(phone)
    
    # Voicemails
    for phone in student_phones:
        for vm in data["vms_by_phone"].get(phone, []):
            all_comms.append({**vm, "source": "voicemail"})
            hits = categorize_text(vm["text"])
            for cat, count in hits.items():
                keyword_hits[cat] += count
    
    # SMS
    sms_phones_for_student = set()
    for tid, phone in data["sms_thread_phone"].items():
        norm = re.sub(r"\D", "", phone)
        if norm in student_phones:
            sms_phones_for_student.add(tid)
    for tid in sms_phones_for_student:
        for sms_msg in data["sms_by_thread"].get(tid, []):
            all_comms.append({**sms_msg, "source": "sms"})
            hits = categorize_text(sms_msg["text"])
            for cat, count in hits.items():
                keyword_hits[cat] += count
    
    # Call reviews
    for cid, student in data["call_student"].items():
        if student.lower() == student_name.lower() and cid in data["reviews_by_call"]:
            cr = data["reviews_by_call"][cid]
            all_comms.append({**cr, "source": "call_review"})
            hits = categorize_text(cr["text"])
            for cat, count in hits.items():
                keyword_hits[cat] += count
    
    # Sort comms by date
    all_comms.sort(key=lambda x: x.get("date", ""), reverse=True)
    
    # ── Hold status ──
    hold_info = data["holds"].get(student_name.lower(), {})
    is_on_hold = hold_info.get("on_hold", False)
    
    # ── School ──
    school_id = int(sl["school_id"].mode().iloc[0]) if len(sl) > 0 else 0
    
    return {
        "student": student_name,
        "school_id": school_id,
        "school": SCHOOL_NAMES.get(school_id, "Unknown"),
        "total_lessons": len(sl),
        "days_idle": days_idle,
        "last_lesson": str(last_date.date()) if pd.notna(last_date) else "",
        "avg_score": avg_score,
        "score_trend": score_trend,
        "scores_recent": scores_list[:6],
        "note_samples": note_samples[:5],
        "avg_lesson_gap": avg_gap,
        "is_weekly": is_weekly,
        "is_biweekly": is_biweekly,
        "is_irregular": is_irregular,
        "no_shows": no_shows,
        "instructor_consistency": instructor_consistency,
        "instructor_changes": instructor_changes,
        "comm_count": len(all_comms),
        "comms_by_source": Counter(c["source"] for c in all_comms),
        "recent_comms": all_comms[:10],
        "keyword_hits": dict(keyword_hits),
        "has_cancellation": keyword_hits.get("cancellation", 0) > 0,
        "has_frustration": keyword_hits.get("frustration", 0) > 0,
        "has_financial": keyword_hits.get("financial", 0) > 0,
        "has_scheduling": keyword_hits.get("scheduling_stress", 0) > 0,
        "has_positive": keyword_hits.get("positive", 0) > 0,
        "is_on_hold": is_on_hold,
        "hold_info": hold_info,
    }


# ═══════════════════════════════════════════════════════════
# PHASE B: ARCHETYPE CLASSIFICATION
# ═══════════════════════════════════════════════════════════

ARCHETYPES = {
    "Disengagement": {
        "signals": lambda p: (
            p["days_idle"] > 30 and p["total_lessons"] >= 5
            and not p["is_on_hold"]
        ),
        "speed": lambda p: "rapid" if p["days_idle"] < 60 and p.get("is_weekly") else "gradual",
        "playbook": [
            "Reach out directly to parent — this student has stopped attending.",
            "Reference their best recent progress (see note hooks below).",
            "Ask about schedule, life changes, or loss of interest.",
            "Offer flexibility: different time, biweekly, or temporary pause with return date.",
        ],
    },
    "Schedule Conflict": {
        "signals": lambda p: (
            p.get("is_irregular") or p.get("has_scheduling") or
            (p.get("no_shows", 0) >= 2) or
            (p.get("avg_lesson_gap") and p["avg_lesson_gap"] > 12)
        ),
        "speed": lambda p: "ongoing",
        "playbook": [
            "Ask parent about schedule challenges — this isn't about interest, it's logistics.",
            "Offer alternative time slots, online option, or biweekly schedule.",
            "Check if a different instructor at a better time is available.",
            "Goal: keep them enrolled even if format changes.",
        ],
    },
    "Quality Fade": {
        "signals": lambda p: (
            p.get("score_trend") == "declining"
            and p.get("avg_score") is not None and p["avg_score"] < 4.0
            and p["total_lessons"] >= 10
        ),
        "speed": lambda p: "gradual",
        "playbook": [
            "Student's scores are declining — they may be hitting a plateau or losing motivation.",
            "Talk to instructor about what's changed. Different approach? New material?",
            "Consider a fresh start: different instructor, new genre, performance goal.",
            "Ask the student directly what would make lessons fun again.",
        ],
    },
    "Instructor Relationship": {
        "signals": lambda p: (
            p.get("instructor_changes", 0) >= 2
            and p["total_lessons"] >= 10
            and p["instructor_consistency"] < 0.4
        ),
        "speed": lambda p: "threshold",
        "playbook": [
            "Multiple instructor changes or low consistency — student may not have found their fit.",
            "Review which instructor they did best with (highest scores, most notes).",
            "Ask parent: 'Has [student] connected with any particular teacher?'",
            "Try matching them back to their best-fit instructor.",
        ],
    },
    "Financial Stress": {
        "signals": lambda p: (
            p.get("has_financial") or
            (p.get("hold_info", {}).get("hold_start", "") and
             not p.get("has_cancellation"))
        ),
        "speed": lambda p: "threshold",
        "playbook": [
            "Financial concerns detected in parent communications.",
            "Offer lower-tier plan, sibling discount, or payment plan.",
            "Better to keep them at reduced rate than lose them entirely.",
            "Check if auto-bill failed — sometimes it's a card issue, not a decision.",
        ],
    },
    "Comm Red Flags": {
        "signals": lambda p: (
            p.get("has_cancellation") or p.get("has_frustration")
        ),
        "speed": lambda p: "urgent",
        "playbook": [
            "THIS IS PRIORITY 1 — parent is frustrated or talking about leaving.",
            "Direct GM call, not instructor delegation.",
            "Acknowledge the issue, apologize if appropriate, offer a solution.",
            "Document the resolution and follow up in 2 weeks.",
        ],
    },
    "New Student Risk": {
        "signals": lambda p: (
            p["total_lessons"] < 10 and p.get("days_idle", 0) > 14
        ),
        "speed": lambda p: "early",
        "playbook": [
            "New student with limited lesson history — standard onboarding check.",
            "Confirm they understand the program structure and expectations.",
            "Schedule their first performance or group activity to build excitement.",
            "Make sure parent has all contact info and knows how to reach the studio.",
        ],
    },
}


def classify_archetypes(profile):
    """Determine which archetypes apply to a student, with primary + secondary."""
    if profile["is_on_hold"]:
        return [{
            "archetype": "On Hold",
            "speed": profile.get("hold_info", {}).get("hold_end", "unknown"),
            "confidence": "high",
            "playbook": [
                f"⏸️ PLAN ON HOLD. Hold ends: {profile.get('hold_info', {}).get('hold_end', 'check Pike13')}.",
                "No outreach needed until hold ends.",
                "Flag for re-activation check when hold expires.",
            ],
        }]
    
    matches = []
    for name, cfg in ARCHETYPES.items():
        try:
            if cfg["signals"](profile):
                matches.append({
                    "archetype": name,
                    "speed": cfg["speed"](profile),
                    "confidence": _confidence_for(profile, name),
                    "playbook": cfg["playbook"],
                })
        except Exception as e:
            continue
    
    # Sort: Comm Red Flags always first, then by confidence
    matches.sort(key=lambda x: (
        0 if x["archetype"] == "Comm Red Flags" else 1,
        -{"high": 3, "medium": 2, "low": 1}.get(x["confidence"], 1)
    ))
    
    return matches[:2]  # primary + secondary


def _confidence_for(profile, archetype):
    """Estimate confidence based on signal strength and data quality."""
    if archetype == "Comm Red Flags":
        return "high" if profile.get("has_cancellation") else "medium"
    if archetype == "Disengagement":
        return "high" if profile["days_idle"] > 60 else "medium"
    if archetype == "Quality Fade":
        return "high" if profile.get("score_trend") == "declining" else "medium"
    if archetype == "Schedule Conflict":
        return "high" if profile.get("has_scheduling") and profile.get("is_irregular") else "medium"
    if archetype == "Instructor Relationship":
        return "high" if profile.get("instructor_changes", 0) >= 3 else "medium"
    if archetype == "Financial Stress":
        return "high" if profile.get("has_financial") else "medium"
    return "medium"


# ═══════════════════════════════════════════════════════════
# REPORT GENERATION
# ═══════════════════════════════════════════════════════════

def generate_reports(profiles, risk_scores):
    """Generate per-school retention reports."""
    
    for school_id, school_name in SCHOOL_NAMES.items():
        school_profiles = [p for p in profiles if p["school_id"] == school_id]
        if not school_profiles:
            continue
        
        # Merge risk scores
        for p in school_profiles:
            match = risk_scores[risk_scores["student_name"].str.lower() == p["student"].lower()]
            p["v12_risk"] = float(match["risk"].values[0]) if len(match) > 0 else 0.0
        
        # Classify archetypes
        for p in school_profiles:
            p["archetypes"] = classify_archetypes(p)
        
        # Sort by risk
        school_profiles.sort(key=lambda x: x["v12_risk"], reverse=True)
        
        # Generate report
        lines = []
        lines.append(f"Subject: 🎸 {school_name} Retention Intelligence — {TODAY.strftime('%B %d, %Y')}")
        lines.append("")
        lines.append(f"Hi Hugh,")
        lines.append("")
        lines.append(f"Retention intelligence report for {school_name}. Each flagged student below")
        lines.append(f"has been classified into a churn archetype with a specific action plan.")
        lines.append("")
        
        # Summary
        on_hold = [p for p in school_profiles if p["is_on_hold"]]
        critical = [p for p in school_profiles if p["v12_risk"] >= 0.70 and not p["is_on_hold"]]
        high = [p for p in school_profiles if 0.50 <= p["v12_risk"] < 0.70 and not p["is_on_hold"]]
        watch = [p for p in school_profiles if 0.30 <= p["v12_risk"] < 0.50 and not p["is_on_hold"]]
        
        archetype_counts = Counter()
        for p in school_profiles:
            for a in p.get("archetypes", []):
                archetype_counts[a["archetype"]] += 1
        
        lines.append("📋 SUMMARY")
        lines.append("")
        lines.append(f"   ⏸️  On Hold (no action):            {len(on_hold)}")
        lines.append(f"   🔴 Critical (≥70% risk):            {len(critical)}")
        lines.append(f"   🟠 High (50-69%):                    {len(high)}")
        lines.append(f"   🟡 Watch (30-49%):                   {len(watch)}")
        lines.append("")
        lines.append("   Archetype breakdown:")
        for arch, count in archetype_counts.most_common():
            lines.append(f"     {arch}: {count}")
        lines.append("")
        
        # ── On Hold section ──
        if on_hold:
            lines.append("─" * 72)
            lines.append(f"⏸️  ON HOLD — {len(on_hold)} students (no outreach needed)")
            lines.append("─" * 72)
            lines.append("")
            for p in on_hold[:10]:
                hi = p.get("hold_info", {})
                end = hi.get("hold_end", "?")
                lines.append(f"  • {p['student']} — hold ends {end}")
                if hi.get("plan"):
                    lines.append(f"    Plan: {hi['plan'][:60]}")
            lines.append("")
        
        # Filter: show only actionable students in critical (idle < 120 days)
        active_critical = [p for p in critical if p["days_idle"] < 120]
        historical = [p for p in critical if p["days_idle"] >= 120]
        
        # Sort: non-Disengagement first (more interesting), then by risk
        active_critical.sort(key=lambda x: (
            0 if any(a["archetype"] != "Disengagement" for a in x.get("archetypes", [])) else 1,
            -x["v12_risk"]
        ))
        
        # ── Critical section (actionable) ──
        if active_critical:
            lines.append("─" * 72)
            lines.append(f"🔴 CRITICAL — {len(active_critical)} actionable students (contact this week)")
            lines.append("─" * 72)
            lines.append("")
            
            for p in active_critical[:15]:
                arch = p.get("archetypes", [])
                primary = arch[0] if arch else {"archetype": "Unknown", "speed": "n/a", "confidence": "low"}
                secondary = arch[1] if len(arch) > 1 else None
                
                badge = f"⚠️ {p['days_idle']}d idle" if p['days_idle'] > 30 else f"📝 {p['total_lessons']} lessons"
                lines.append(f"  {p['student']} — {p['v12_risk']:.0%} risk | {badge}")
                lines.append(f"  🏷️  {primary['archetype']} ({primary['speed']}) — {primary['confidence']} confidence")
                if secondary:
                    lines.append(f"     Also: {secondary['archetype']}")
                
                for action in primary.get("playbook", []):
                    lines.append(f"     → {action}")
                
                # Note hooks
                for ns in p.get("note_samples", [])[:2]:
                    if ns["text"]:
                        score_str = f" (score {ns['score']:.0f})" if ns["score"] is not None else ""
                        lines.append(f"     💬 {ns['date']}{score_str}: \"{ns['text'][:120]}…\"")
                
                # Contact info
                hi = p.get("hold_info", {})
                contact = []
                if hi.get("account_emails"):
                    contact.append(f"✉️ {hi['account_emails']}")
                if hi.get("account_phones"):
                    contact.append(f"📞 {hi['account_phones']}")
                if contact:
                    lines.append(f"     Contact: {' | '.join(contact)}")
                
                lines.append("")
        
        # ── Historical Churn (idle 120+ days, low priority) ──
        if historical:
            lines.append("─" * 72)
            lines.append(f"📦 HISTORICAL CHURN — {len(historical)} students idle 120+ days (review quarterly)")
            lines.append("─" * 72)
            lines.append("")
            hist_by_arch = defaultdict(list)
            for p in historical:
                arch = p.get("archetypes", [])
                primary = arch[0]["archetype"] if arch else "Unknown"
                hist_by_arch[primary].append(p["student"])
            for arch, names in sorted(hist_by_arch.items()):
                lines.append(f"  {arch}: {len(names)} students")
            lines.append(f"  → Consider removing from active monitoring. Prioritize <120d idle.")
            lines.append("")
        
        # ── High section ──
        if high:
            lines.append("─" * 72)
            lines.append(f"🟠 HIGH — {len(high)} students (instructor check-in)")
            lines.append("─" * 72)
            lines.append("")
            for p in high[:8]:
                arch = p.get("archetypes", [])
                primary = arch[0] if arch else {}
                lines.append(f"  • {p['student']} — {p['v12_risk']:.0%} | {primary.get('archetype', '?')} ({primary.get('speed', '?')})")
            lines.append("")
        
        # ── Save ──
        out_path = MODELS_DIR / f"retention_intel_{school_name.replace(' ', '_')}.txt"
        open(out_path, "w").write("\n".join(lines))
        print(f"\n  Saved: {out_path} ({len(lines)} lines)")
    
    # Save full JSON
    json_out = []
    for p in profiles:
        json_out.append({
            "student": p["student"],
            "school": p["school"],
            "v12_risk": p.get("v12_risk", 0),
            "archetypes": p.get("archetypes", []),
            "days_idle": p["days_idle"],
            "avg_score": p["avg_score"],
            "score_trend": p["score_trend"],
            "keyword_hits": p["keyword_hits"],
            "is_on_hold": p["is_on_hold"],
        })
    
    json_path = MODELS_DIR / "retention_intelligence.json"
    json.dump(json_out, open(json_path, "w"), indent=2)
    print(f"  Saved: {json_path} ({len(json_out)} students)")


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    print("retention_intelligence.py — Phase A + B")
    print()
    
    # Load data
    print("Phase A: Data Synthesis")
    print("-" * 40)
    data = load_all_data()
    
    # Load v12 risk scores
    risk_scores = pd.read_csv(MODELS_DIR / "v11_risk_scores.csv")
    print(f"  Risk scores loaded: {len(risk_scores)} students")
    
    # Get active students (from risk scores)
    active = risk_scores[risk_scores["risk"] >= 0.10]  # flag anyone with any risk
    print(f"  Active students to profile: {len(active)}")
    
    # Build profiles
    profiles = []
    for _, row in active.iterrows():
        name = str(row["student_name"])
        profile = build_student_profile(name, data)
        if profile:
            profiles.append(profile)
    
    print(f"  Profiles built: {len(profiles)}")
    
    # Quick stats
    on_hold = sum(1 for p in profiles if p["is_on_hold"])
    has_cancel = sum(1 for p in profiles if p.get("has_cancellation"))
    has_frust = sum(1 for p in profiles if p.get("has_frustration"))
    has_fin = sum(1 for p in profiles if p.get("has_financial"))
    has_pos = sum(1 for p in profiles if p.get("has_positive"))
    has_sched = sum(1 for p in profiles if p.get("has_scheduling"))
    has_comms = sum(1 for p in profiles if p["comm_count"] > 0)
    
    print(f"\n  Profile signals:")
    print(f"    On hold: {on_hold}")
    print(f"    Has comms: {has_comms}")
    print(f"    Cancellation hits: {has_cancel}")
    print(f"    Frustration hits: {has_frust}")
    print(f"    Financial hits: {has_fin}")
    print(f"    Positive hits: {has_pos}")
    print(f"    Scheduling hits: {has_sched}")
    
    # Phase B: Classify
    print(f"\nPhase B: Archetype Classification")
    print("-" * 40)
    
    generate_reports(profiles, risk_scores)
    
    print(f"\nDone. Reports at models/retention_intel_*.txt and models/retention_intelligence.json")


if __name__ == "__main__":
    main()
