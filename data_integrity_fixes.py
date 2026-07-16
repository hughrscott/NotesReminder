#!/usr/bin/env python3
"""
data_integrity_fixes.py — Consolidated fixes for SOR data pipeline.

Fixes applied:
  1. Extract membership from Pike13 raw_text (Service: field)
  2. Map call_review transcripts → students via:
     a. Direct person_id → Pike13
     b. Phone bridge: voice_event phone → Pike13 phone
     c. HubSpot phone bridge: voice_event phone → HubSpot phone → HubSpot full_name
  3. Score all linked transcripts for engagement + sentiment
  4. Merge with existing comms_engagement_features.csv

Author: Hermes
Date: 2026-07-16
"""
import sqlite3, json, csv, re
from pathlib import Path
from datetime import date
from collections import defaultdict
import numpy as np

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"

try:
    from transformers import pipeline
    HAS_TRANSFORMERS = True
except ImportError:
    HAS_TRANSFORMERS = False


# ── Keyword patterns ──
ENGAGEMENT_KEYWORDS = [
    ("cancellation", [r"\bcancel", r"can'?t\s+(?:make|come|be\s+there|do)", r"won'?t\s+(?:be|make|come)", r"not\s+going\s+to\s+(?:make|be|come)", r"skip", r"miss(?:ing)?\s+(?:lesson|class|today)", r"not\s+(?:be|come|there|attend)", r"out\s+(?:sick|of\s+town)", r"unable\s+to"]),
    ("reschedule", [r"\breschedule", r"move\s+(?:lesson|class|time)", r"change\s+(?:time|day|schedule)", r"switch", r"different\s+(?:time|day)", r"another\s+(?:time|day)", r"make\s*up\s+(?:lesson|class)", r"rain\s*check"]),
    ("scheduling_issue", [r"\bproblem", r"\bissue", r"\bconflict", r"double\s*book", r"doesn'?t\s+work", r"not\s+working", r"\bwrong\b", r"\bmistake"]),
    ("inquiry", [r"interested", r"learn\s+more", r"find\s+out", r"\binfo", r"\btrial\b", r"sign\s*up", r"enroll", r"how\s+much"]),
    ("praise", [r"\bgreat\b", r"\bamazing\b", r"\bawesome\b", r"\blove", r"\bexcellent\b", r"\bwonderful", r"\bfantastic", r"doing\s+(?:well|great)", r"\bthank", r"\bhappy", r"\benjoy", r"\bimpressed"]),
    ("complaint", [r"\bdisappointed", r"\bunhappy", r"\bfrustrated", r"\bupset", r"not\s+happy", r"\bterrible", r"\bhorrible", r"\brude"]),
]

ENGAGEMENT_WEIGHTS = {"cancellation": +0.8, "reschedule": +0.1, "scheduling_issue": +0.5, "inquiry": -0.3, "praise": -0.4, "complaint": +0.7}
SENTIMENT_WEIGHTS = {"positive": -0.2, "neutral": 0.0, "negative": +0.4}


def classify_engagement(text):
    text_lower = text.lower()
    for category, patterns in ENGAGEMENT_KEYWORDS:
        for pattern in patterns:
            if re.search(pattern, text_lower):
                return category
    return "other"


# ═══ 1. EXTRACT MEMBERSHIP FROM PIKE13 RAW_TEXT ═══

def extract_membership_from_raw():
    """Extract membership from Pike13 raw_text Service: field."""
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row

    nulls = con.execute("""
        SELECT person_id, full_name, raw_text FROM pike13_people
        WHERE (membership_state IS NULL OR membership_state = '')
          AND raw_text IS NOT NULL AND raw_text != ''
    """).fetchall()

    fixed = 0
    for row in nulls:
        m = re.search(r'^Service:\s*(.+?)$', row["raw_text"] or "", re.MULTILINE)
        if m:
            service = m.group(1).strip()
            con.execute("""
                UPDATE pike13_people SET membership_state = ?
                WHERE person_id = ?
            """, (service, row["person_id"]))
            fixed += 1
            print(f"    {row['full_name']}: {service}")

    con.commit()
    con.close()

    print(f"  Membership from raw_text: {fixed}/{len(nulls)} fixed")
    return fixed


# ═══ 2. MAP CALL TRANSCRIPTS ═══

def map_call_transcripts():
    """Map call_review transcripts → students via all available bridges."""
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row

    transcript_rows = list(con.execute("""
        SELECT cr.call_review_id, cr.transcript_text, cr.recap_text,
               cr.action_items_json, cr.event_at, ve.person_id,
               ve.phone_normalized, ve.contact_name, ve.event_id
        FROM dialpad_call_reviews cr
        JOIN dialpad_voice_events ve ON cr.call_id = ve.call_id
        WHERE cr.transcript_text IS NOT NULL AND cr.transcript_text != ''
    """))

    # Route A: person_id → Pike13
    person_to_name = {}
    for row in con.execute("""
        SELECT person_identity_id, full_name FROM pike13_people
        WHERE person_identity_id IS NOT NULL AND full_name IS NOT NULL
    """):
        person_to_name[row["person_identity_id"]] = row["full_name"]

    # Route B: phone → Pike13
    phone_to_name = {}
    for row in con.execute("""
        SELECT phone_normalized, full_name FROM pike13_people
        WHERE phone_normalized IS NOT NULL AND phone_normalized != ''
          AND full_name IS NOT NULL
    """):
        phone_to_name[row["phone_normalized"]] = row["full_name"]

    # Route C: phone → HubSpot (for contacts not in Pike13)
    hs_phone_to_name = {}
    for row in con.execute("""
        SELECT phone_normalized, full_name FROM hubspot_contacts
        WHERE phone_normalized IS NOT NULL AND phone_normalized != ''
          AND full_name IS NOT NULL
          AND phone_normalized NOT IN (
              SELECT phone_normalized FROM pike13_people
              WHERE phone_normalized IS NOT NULL
          )
    """):
        if row["phone_normalized"] not in phone_to_name:
            hs_phone_to_name[row["phone_normalized"]] = row["full_name"]

    transcripts = []
    direct_count = 0
    phone_count = 0
    hs_phone_count = 0

    for r in transcript_rows:
        student = None
        pid = r["person_id"]
        phone = r["phone_normalized"]

        if pid:
            student = person_to_name.get(pid)
            if student:
                direct_count += 1
        if not student and phone:
            student = phone_to_name.get(phone)
            if student:
                phone_count += 1
        if not student and phone:
            student = hs_phone_to_name.get(phone)
            if student:
                hs_phone_count += 1

        if student:
            transcripts.append({
                "id": r["call_review_id"],
                "text": (r["transcript_text"] or "")[:1000],
                "student": student.lower(),
                "date": r["event_at"] or "",
                "recap": (r["recap_text"] or "")[:500],
                "actions": r["action_items_json"] or "",
            })

    con.close()
    print(f"  Transcripts: {len(transcript_rows)} reviewed, {len(transcripts)} linked")
    print(f"    Direct person: {direct_count}")
    print(f"    Pike13 phone: {phone_count}")
    print(f"    HubSpot phone: {hs_phone_count}")
    return transcripts


# ═══ 3. SCORE ═══

def score_transcripts(transcripts):
    if not transcripts:
        return {}
    if not HAS_TRANSFORMERS:
        print("  Skipping sentiment — transformers not available")
        return {}

    print("  Loading RoBERTa sentiment...")
    sentiment = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest", device=-1, max_length=512, truncation=True)

    results = {}
    print(f"  Scoring {len(transcripts)} transcripts...")
    for i in range(0, len(transcripts), 32):
        batch = transcripts[i:i+32]
        texts = [t["text"][:512] for t in batch]
        sent_results = sentiment(texts)
        for j, t in enumerate(batch):
            eng_label = classify_engagement(t["text"])
            sr = sent_results[j]
            sent_label = sr["label"].lower()
            if sent_label not in ("positive", "neutral", "negative"):
                sent_label = "neutral"
            eng_w = ENGAGEMENT_WEIGHTS.get(eng_label, 0)
            sent_w = SENTIMENT_WEIGHTS.get(sent_label, 0)
            results[t["id"]] = {"student": t["student"], "channel": "call_transcript", "engagement": eng_label, "sentiment": sent_label, "risk_score": round(eng_w + sent_w, 3)}
        if i % (32 * 5) == 0:
            print(f"    {i}/{len(transcripts)}")
    print(f"  Scored {len(results)} call transcripts")
    return results


# ═══ 4. MERGE ═══

def merge_and_save(call_results):
    eng_path = MODELS_DIR / "comms_engagement_features.csv"
    existing = defaultdict(lambda: dict.fromkeys(["comms_engagement_total", "comms_engagement_channels", "comms_engagement_avg_risk", "comms_engagement_risk_volatility", "comms_engagement_cancellation_rate", "comms_engagement_reschedule_rate", "comms_engagement_scheduling_issue_rate", "comms_engagement_inquiry_rate", "comms_engagement_no_show_rate", "comms_engagement_praise_rate", "comms_engagement_complaint_rate", "comms_engagement_positive_ratio", "comms_engagement_negative_ratio"], 0.0))

    if eng_path.exists():
        with open(eng_path) as f:
            for row in csv.DictReader(f):
                s = row["student"]
                for k in existing[s]:
                    existing[s][k] = float(row.get(k, 0))

    student_calls = defaultdict(list)
    for cid, info in call_results.items():
        student_calls[info["student"]].append(info)

    new_students = 0
    for student, calls in student_calls.items():
        e = existing[student]
        had_signal = e["comms_engagement_total"] > 0
        n_calls = len(calls)
        e["comms_engagement_total"] += n_calls
        risks = [c["risk_score"] for c in calls]
        cur_avg = e["comms_engagement_avg_risk"]
        cur_n = e["comms_engagement_total"] - n_calls
        e["comms_engagement_avg_risk"] = round((cur_avg * cur_n + sum(risks)) / max(e["comms_engagement_total"], 1), 3)
        if len(risks) > 1:
            e["comms_engagement_risk_volatility"] = round(np.std(risks), 3)
        for c in calls:
            if c["engagement"] == "cancellation": e["comms_engagement_cancellation_rate"] += 1
            elif c["engagement"] == "reschedule": e["comms_engagement_reschedule_rate"] += 1
            elif c["engagement"] == "scheduling_issue": e["comms_engagement_scheduling_issue_rate"] += 1
            elif c["engagement"] == "inquiry": e["comms_engagement_inquiry_rate"] += 1
            elif c["engagement"] == "praise": e["comms_engagement_praise_rate"] += 1
            elif c["engagement"] == "complaint": e["comms_engagement_complaint_rate"] += 1
            if c["sentiment"] == "positive": e["comms_engagement_positive_ratio"] += 1
            elif c["sentiment"] == "negative": e["comms_engagement_negative_ratio"] += 1
        total = e["comms_engagement_total"] or 1
        for key in ["cancellation_rate", "reschedule_rate", "scheduling_issue_rate", "inquiry_rate", "praise_rate", "complaint_rate", "positive_ratio", "negative_ratio"]:
            e[f"comms_engagement_{key}"] /= total
        if not had_signal and e["comms_engagement_total"] > 0:
            new_students += 1

    out_path = MODELS_DIR / "comms_engagement_features.csv"
    with open(out_path, "w", newline="") as f:
        fieldnames = ["student"] + list(next(iter(existing.values())).keys())
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for student in sorted(existing.keys()):
            row = {"student": student, **{k: round(v, 4) if isinstance(v, float) else v for k, v in existing[student].items()}}
            w.writerow(row)

    print(f"  Saved: {out_path} ({len(existing)} students, {new_students} new with call data)")
    return len(student_calls)


def main():
    print("=" * 60)
    print("Data Integrity Fixes — v2 (comprehensive)")
    print("=" * 60)

    print("\n[1] Extracting membership from Pike13 raw_text...")
    n_fixed = extract_membership_from_raw()

    print("\n[2] Mapping call review transcripts...")
    transcripts = map_call_transcripts()

    print("\n[3] Scoring call transcripts...")
    call_results = score_transcripts(transcripts)

    print("\n[4] Merging with engagement features...")
    n_enriched = merge_and_save(call_results)

    print("\n" + "=" * 60)
    print(f"Fixed {n_fixed} memberships from raw_text")
    print(f"Linked {len(transcripts)} call transcripts to students")
    print(f"Scored {len(call_results)} transcripts, enriched {n_enriched} students")
    print("=" * 60)


if __name__ == "__main__":
    main()
