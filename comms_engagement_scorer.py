#!/usr/bin/env python3
"""
comms_engagement_scorer.py — Combined engagement classification + sentiment
for all matched student comms. Uses transformer models locally.

Engagement categories: cancellation, reschedule, scheduling_issue, 
  inquiry, payment, routine_admin, no_show, other
Sentiment: positive / neutral / negative

Output: per-student engagement features for churn model.
"""
import sqlite3, json, csv, re, sys
from pathlib import Path
from datetime import date, timedelta
from collections import defaultdict
import numpy as np

# ── Try loading transformers ──
try:
    from transformers import pipeline
    HAS_TRANSFORMERS = True
except ImportError:
    print("⚠️  transformers not installed. Install with: pip install transformers torch")
    HAS_TRANSFORMERS = False

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"
BATCH_SIZE = 32


# Engagement categories with descriptions for zero-shot
# Engagement patterns: (category, [keyword patterns])
ENGAGEMENT_KEYWORDS = [
    ("cancellation", [
        r"\bcancel", r"can'?t\s+(?:make|come|be\s+there|do)", r"won'?t\s+(?:be|make|come)",
        r"not\s+going\s+to\s+(?:make|be|come)", r"skip", r"miss(?:ing)?\s+(?:lesson|class|today)",
        r"not\s+(?:be|come|there|attend)", r"out\s+(?:sick|of\s+town)", r"unable\s+to",
    ]),
    ("reschedule", [
        r"\breschedule", r"move\s+(?:lesson|class|time)", r"change\s+(?:time|day|schedule)",
        r"switch", r"different\s+(?:time|day)", r"another\s+(?:time|day)",
        r"make\s*up\s+(?:lesson|class)", r"rain\s*check",
    ]),
    ("scheduling_issue", [
        r"\bproblem", r"\bissue", r"\bconflict", r"double\s*book",
        r"doesn'?t\s+work", r"not\s+working", r"\bwrong\b",
        r"\bmistake", r"\berror",
    ]),
    ("inquiry", [
        r"interested", r"learn\s+more", r"find\s+out", r"\binfo", r"\binformation",
        r"\btrial\b", r"sign\s*up", r"enroll", r"new\s+student", r"how\s+much",
        r"\bprice", r"\bcost", r"\bprogram", r"\boffer", r"\bavailab",
    ]),
    ("payment", [
        r"\bbill", r"\binvoice", r"\bpay", r"\bcharge", r"\bcredit\b",
        r"\brefund", r"\bowe", r"\bcost\b", r"\bdollar", r"\$\d+",
    ]),
    ("routine_admin", [
        r"running\s+late", r"on\s+(?:my|our)\s+way", r"be\s+there\s+soon",
        r"confirm", r"checking\s+(?:in|on)", r"just\s+wanted\s+to\s+(?:check|see|let)",
        r"letting\s+you\s+know", r"heads?\s*up", r"friendly\s+reminder",
    ]),
    ("no_show", [
        r"no\s*show", r"didn'?t\s+show", r"forgot", r"missed\s+(?:lesson|class)",
        r"didn'?t\s+make\s+it", r"never\s+showed",
    ]),
    ("praise", [
        r"\bgreat\b", r"\bamazing\b", r"\bawesome\b", r"\blove", r"\bexcellent\b",
        r"\bwonderful", r"\bfantastic", r"doing\s+(?:well|great)", r"\bthank",
        r"\bhappy", r"\benjoy", r"\bimpressed",
    ]),
    ("complaint", [
        r"\bdisappointed", r"\bunhappy", r"\bfrustrated", r"\bupset",
        r"not\s+happy", r"\bterrible", r"\bhorrible", r"\bawful",
        r"\brude", r"\bunacceptable", r"not\s+ok", r"\bcomplaint",
    ]),
    ("spam", [
        r"\bwine\b", r"\bsoda\s*rock", r"\bcomcast\b", r"\brealtor",
        r"verification\s*code", r"\buplive", r"\bdialpad",
        r"download\s+the\s+(?:android|iphone|app)",
    ]),
]

# ── Engagement weights (impact on churn risk) ──
ENGAGEMENT_WEIGHTS = {
    "cancellation":      +0.8,   # high churn signal
    "reschedule":        +0.1,   # mild churn signal (still engaged enough to reschedule)
    "scheduling_issue":  +0.5,   # friction → churn
    "inquiry":           -0.3,   # new interest → negative churn risk
    "payment":           +0.2,   # slightly churn-correlated
    "routine_admin":      0.0,   # neutral
    "no_show":           +0.9,   # strong churn signal
    "praise":            -0.4,   # happy → negative churn risk
    "complaint":         +0.7,   # unhappy → churn
    "other":              0.0,
}

# ── Sentiment weights ──
SENTIMENT_WEIGHTS = {
    "positive":  -0.2,
    "neutral":    0.0,
    "negative":  +0.4,
}


def load_matched_comms():
    """Build student→comms ID mappings using reverse matching logic.
    Same approach as reverse_matcher.py but produces ID lists."""
    import re
    
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row

    def norm_phone(raw):
        if not raw: return None
        digits = re.sub(r'\D', '', str(raw))
        return digits[-10:] if len(digits) >= 10 else (digits if len(digits) >= 7 else None)

    # Build student profiles with search terms
    profiles = {}
    for row in con.execute("""
        SELECT person_id, full_name, first_name, last_name, 
               email_normalized, phone_normalized, school
        FROM pike13_people WHERE full_name IS NOT NULL AND full_name != ''
    """):
        full = (row['full_name'] or '').strip().lower()
        if full == 'loading':
            continue
        first = (row['first_name'] or '').strip().lower()
        last = (row['last_name'] or '').strip().lower()
        phone = norm_phone(row['phone_normalized'])
        email = (row['email_normalized'] or '').strip().lower()
        
        terms = set()
        if full: terms.add(full)
        if first: terms.add(first)
        if last: terms.add(last)
        if first and last: terms.add(f"{first} {last}")
        if email: terms.add(email)
        if phone: terms.add(phone)
        
        profiles[full] = {'name': full, 'terms': terms, 'phone': phone, 'email': email,
                         'first': first, 'last': last}

    # Also add HubSpot phones
    for row in con.execute("""
        SELECT phone_normalized, full_name FROM hubspot_contacts 
        WHERE phone_normalized IS NOT NULL AND phone_normalized != ''
    """):
        phone = norm_phone(row['phone_normalized'])
        if phone:
            for key in profiles:
                profiles[key]['terms'].add(phone)

    print(f"  {len(profiles)} student profiles")

    # ── Match SMS by name/phone in content ──
    sms_map = {}  # message_id → student
    thread_phones = {}
    for row in con.execute("SELECT thread_id, phone FROM dialpad_sms_threads"):
        thread_phones[str(row['thread_id'])] = norm_phone(row['phone'])

    for row in con.execute("""
        SELECT message_id, body, thread_id FROM dialpad_sms_messages
        WHERE body IS NOT NULL AND body != ''
    """):
        text = (row['body'] or '').lower()
        phone = thread_phones.get(str(row['thread_id']))
        for key, p in profiles.items():
            if any(t in text for t in p['terms'] if len(t) >= 3):
                sms_map[row['message_id']] = key
                break
            elif phone and p.get('phone') and phone == p['phone']:
                sms_map[row['message_id']] = key
                break

    # ── Match voicemails by name/phone in transcript ──
    vm_map = {}
    for row in con.execute("""
        SELECT call_id, transcription_text, external_number FROM dialpad_voicemails
        WHERE transcription_text IS NOT NULL AND transcription_text != ''
    """):
        text = (row['transcription_text'] or '').lower()
        phone = norm_phone(row['external_number'])
        for key, p in profiles.items():
            if any(t in text for t in p['terms'] if len(t) >= 3):
                vm_map[row['call_id']] = key
                break
            elif phone and p.get('phone') and phone == p['phone']:
                vm_map[row['call_id']] = key
                break

    # ── Match emails by name/email in subject/snippet ──
    email_map = {}
    for row in con.execute("""
        SELECT message_id, subject, snippet, body, external_email_normalized FROM school_email_messages
        WHERE (subject NOT LIKE '%HubSpot%' OR subject IS NULL)
          AND (snippet NOT LIKE '%HubSpot%' OR snippet IS NULL)
    """):
        text = " ".join(str(v or "") for v in [row['subject'], row['snippet']]).lower()
        ext = (row['external_email_normalized'] or '').strip().lower()
        for key, p in profiles.items():
            if ext and p.get('email') and ext == p['email']:
                email_map[row['message_id']] = key
                break
            elif any(t in text for t in p['terms'] if len(t) >= 3):
                email_map[row['message_id']] = key
                break

    print(f"  Matched: {len(sms_map)} SMS, {len(vm_map)} VM, {len(email_map)} email")

    # ── Build comms list ──
    comms = []
    for row in con.execute("SELECT message_id, body, message_at FROM dialpad_sms_messages WHERE body IS NOT NULL AND body != ''"):
        mid = row['message_id']
        if mid in sms_map:
            comms.append({'id': mid, 'channel': 'sms', 'text': row['body'].strip(),
                         'student': sms_map[mid], 'date': row['message_at'] or ''})

    for row in con.execute("SELECT call_id, transcription_text, date FROM dialpad_voicemails WHERE transcription_text IS NOT NULL AND transcription_text != ''"):
        cid = row['call_id']
        if cid in vm_map:
            comms.append({'id': cid, 'channel': 'voicemail', 'text': (row['transcription_text'] or '').strip(),
                         'student': vm_map[cid], 'date': row['date'] or ''})

    for row in con.execute("SELECT message_id, subject, snippet, body, message_at FROM school_email_messages WHERE (subject NOT LIKE '%HubSpot%' OR subject IS NULL) AND (snippet NOT LIKE '%HubSpot%' OR snippet IS NULL)"):
        mid = row['message_id']
        if mid in email_map:
            text = " ".join(str(v or "") for v in [row['subject'], row['snippet'], (row['body'] or '')[:200]])
            comms.append({'id': mid, 'channel': 'email', 'text': text.strip(),
                         'student': email_map[mid], 'date': row['message_at'] or ''})

    con.close()
    print(f"  Total comms to score: {len(comms)}")
    return comms


def classify_engagement(text):
    """Fast keyword-based engagement classification. Returns (category, confidence)."""
    text_lower = text.lower()
    for category, patterns in ENGAGEMENT_KEYWORDS:
        for pattern in patterns:
            if re.search(pattern, text_lower):
                return category, 1.0
    return "other", 0.0


def score_comms_fast(comms):
    """Score comms using keyword engagement + RoBERTa sentiment. Fast: ~30s for 2.5K comms."""
    if not HAS_TRANSFORMERS:
        print("  ⚠️  Skipping sentiment — transformers not installed")
        return {}

    print("  Loading RoBERTa sentiment model...")
    try:
        sentiment = pipeline(
            "sentiment-analysis",
            model="cardiffnlp/twitter-roberta-base-sentiment-latest",
            device=-1, max_length=256, truncation=True,
        )
    except Exception:
        try:
            sentiment = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english",
                               device=-1, max_length=256, truncation=True)
        except Exception as e:
            print(f"  ⚠️  Could not load sentiment model: {e}")
            return {}

    print(f"  Scoring {len(comms)} comms (keyword engagement + RoBERTa sentiment)...")
    results = {}
    
    for i in range(0, len(comms), BATCH_SIZE):
        batch = comms[i:i + BATCH_SIZE]
        texts = [c["text"][:256] for c in batch]
        
        # Fast keyword engagement classification
        eng_results = [classify_engagement(t) for t in texts]
        
        # RoBERTa sentiment
        sent_results = sentiment(texts)
        
        for j, c in enumerate(batch):
            eng_label, _ = eng_results[j]
            sr = sent_results[j]
            sent_label = sr["label"].lower()
            
            # Map RoBERTa labels
            if sent_label == "label_0":
                sent_label = "negative"
            elif sent_label == "label_1":
                sent_label = "neutral"
            elif sent_label == "label_2":
                sent_label = "positive"
            elif sent_label not in ("positive", "neutral", "negative"):
                sent_label = "neutral"
            
            # Skip spam
            if eng_label == "spam":
                continue
            
            # Combined risk score
            eng_w = ENGAGEMENT_WEIGHTS.get(eng_label, 0)
            sent_w = SENTIMENT_WEIGHTS.get(sent_label, 0)
            risk_score = eng_w + sent_w
            
            results[c["id"]] = {
                "student": c["student"],
                "channel": c["channel"],
                "text": c["text"][:200],
                "engagement": eng_label,
                "engagement_confidence": 1.0,
                "sentiment": sent_label,
                "risk_score": round(risk_score, 3),
            }
        
        if i % (BATCH_SIZE * 10) == 0 and i > 0:
            print(f"    {i}/{len(comms)} ({i/len(comms)*100:.0f}%)")
    
    print(f"  Scored {len(results)} comms (filtered spam)")
    return results


def aggregate_to_students(results):
    """Roll up comm-level scores to per-student features."""
    students = defaultdict(lambda: {
        "total_comms": 0,
        "cancellation_count": 0,
        "reschedule_count": 0,
        "scheduling_issue_count": 0,
        "inquiry_count": 0,
        "no_show_count": 0,
        "praise_count": 0,
        "complaint_count": 0,
        "avg_risk_score": 0,
        "risk_scores": [],
        "positive_ratio": 0,
        "negative_ratio": 0,
        "sentiments": [],
        "channels": set(),
    })

    for cid, info in results.items():
        s = students[info["student"]]
        s["total_comms"] += 1
        s["channels"].add(info["channel"])
        s["risk_scores"].append(info["risk_score"])

        eng = info["engagement"]
        if eng in s:
            s[f"{eng}_count"] += 1

        sent = info["sentiment"]
        s["sentiments"].append(sent)

    features = {}
    for student, s in students.items():
        n = s["total_comms"] or 1
        s["risk_scores"] = [r for r in s["risk_scores"]]
        avg_risk = np.mean(s["risk_scores"]) if s["risk_scores"] else 0
        risk_volatility = np.std(s["risk_scores"]) if len(s["risk_scores"]) > 1 else 0

        pos_count = sum(1 for sent in s["sentiments"] if sent == "positive")
        neg_count = sum(1 for sent in s["sentiments"] if sent == "negative")

        features[student] = {
            "comms_engagement_total": n,
            "comms_engagement_channels": len(s["channels"]),
            "comms_engagement_avg_risk": round(avg_risk, 3),
            "comms_engagement_risk_volatility": round(risk_volatility, 3),
            "comms_engagement_cancellation_rate": round(s["cancellation_count"] / n, 3),
            "comms_engagement_reschedule_rate": round(s["reschedule_count"] / n, 3),
            "comms_engagement_scheduling_issue_rate": round(s["scheduling_issue_count"] / n, 3),
            "comms_engagement_inquiry_rate": round(s["inquiry_count"] / n, 3),
            "comms_engagement_no_show_rate": round(s["no_show_count"] / n, 3),
            "comms_engagement_praise_rate": round(s["praise_count"] / n, 3),
            "comms_engagement_complaint_rate": round(s["complaint_count"] / n, 3),
            "comms_engagement_positive_ratio": round(pos_count / n, 3),
            "comms_engagement_negative_ratio": round(neg_count / n, 3),
        }

    print(f"  Aggregated to {len(features)} students")
    return features


def main():
    print("=" * 60)
    print("Comms Engagement + Sentiment Scorer")
    print("=" * 60)

    print("\n[1] Loading matched comms...")
    comms = load_matched_comms()
    if not comms:
        print("  No matched comms found. Run comms_matcher_v3.py first.")
        return

    print(f"\n[2] Scoring engagement + sentiment...")
    results = score_comms_fast(comms)

    if not results:
        print("  ⚠️  Install transformers: pip install transformers torch")
        return

    print(f"\n[3] Aggregating to student features...")
    features = aggregate_to_students(results)

    # Save
    out_path = MODELS_DIR / "comms_engagement_features.csv"
    with open(out_path, "w", newline="") as f:
        fieldnames = list(next(iter(features.values())).keys())
        w = csv.DictWriter(f, fieldnames=["student"] + fieldnames)
        w.writeheader()
        for student, feat in sorted(features.items()):
            w.writerow({"student": student, **feat})

    print(f"\n  Saved: {out_path}")

    # Sample stats
    print(f"\n[4] Sample engagement distribution:")
    for label in ["cancellation", "reschedule", "complaint", "praise", "no_show", "inquiry"]:
        count = sum(1 for r in results.values() if r["engagement"] == label)
        print(f"  {label:<20s}: {count}")

    print(f"\n[5] Sentiment distribution:")
    for label in ["positive", "neutral", "negative"]:
        count = sum(1 for r in results.values() if r["sentiment"] == label)
        print(f"  {label:<10s}: {count}")


if __name__ == "__main__":
    main()
