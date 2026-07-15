#!/usr/bin/env python3
"""comm_sentiment_v2.py — Sentiment + frequency features from all matched communications.

Uses 333 name-matched + phone-matched communications to compute per-student features:
  Sentiment: avg_compound, neg_ratio, sentiment_trend, volatility
  Frequency: comm_count, recent_spike_ratio, days_since_last, longest_gap, cv
  Content:   reschedule_count, question_count

Output: models/comm_features_v2.csv — one row per student with all features.
"""

import sqlite3, re, json
from pathlib import Path
from datetime import date, timedelta
from collections import defaultdict
import pandas as pd
import numpy as np

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "vaderSentiment", "-q"])
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"
MATCHES_PATH = MODELS_DIR / "comms_name_matches.json"
TODAY = date.today()
LOOKBACK = 180  # days

con = sqlite3.connect(str(DB_PATH))
sia = SentimentIntensityAnalyzer()

# ═══════════════════════════════════════════════════════════
# 1. LOAD ALL MATCHED COMMUNICATIONS
# ═══════════════════════════════════════════════════════════

def load_matches():
    """Build student_name → list of (source, text, date, sentiment) from all matched comms."""
    matches = json.load(open(MATCHES_PATH))
    student_comms = defaultdict(list)
    
    # Phone → student lookup from name matches
    phone_to_student = {}
    for key, info in matches["matches"].items():
        if not key.startswith("call_") and "@" not in key and not key.startswith("sms_"):
            phone_to_student[key] = info["student"]
    
    # Also load phone-matched students from identity_matches + pike13_people
    phone_matched = {}
    ppl = pd.read_sql_query("""
        SELECT full_name, phone, phone_normalized FROM pike13_people 
        WHERE phone IS NOT NULL OR phone_normalized IS NOT NULL
    """, con)
    for _, r in ppl.iterrows():
        name = str(r["full_name"]).strip()
        for col in ["phone", "phone_normalized"]:
            p = str(r.get(col, "") or "")
            if p and name:
                phone_matched[re.sub(r"\D", "", p)] = name
    
    # Combine: name-matched takes precedence
    all_phones = {**phone_matched, **phone_to_student}
    print(f"  Phone→student mappings: {len(all_phones)}")
    
    # ── Voicemails ──
    vms = pd.read_sql_query("""
        SELECT external_number, transcription_text, date as created_at
        FROM dialpad_voicemails
        WHERE transcription_text IS NOT NULL
    """, con)
    
    vm_count = 0
    for _, row in vms.iterrows():
        phone = re.sub(r"\D", "", str(row["external_number"]))
        student = all_phones.get(phone)
        if not student:
            continue
        txt = str(row["transcription_text"])[:1000]
        dt = str(row.get("created_at", ""))
        sentiment = sia.polarity_scores(txt)
        student_comms[student].append({
            "source": "voicemail",
            "text": txt,
            "date": dt[:10] if dt else "",
            "compound": sentiment["compound"],
            "neg": sentiment["neg"],
            "pos": sentiment["pos"],
            "neu": sentiment["neu"],
            "phone": phone,
        })
        vm_count += 1
    print(f"  Voicemails matched: {vm_count}")
    
    # ── SMS (via thread phone) ──
    sms_threads = pd.read_sql_query("""
        SELECT thread_id, phone FROM dialpad_sms_threads WHERE phone IS NOT NULL
    """, con)
    thread_phone = {}
    for _, r in sms_threads.iterrows():
        phone = re.sub(r"\D", "", str(r["phone"]))
        if phone:
            thread_phone[str(r["thread_id"])] = phone
    
    sms = pd.read_sql_query("""
        SELECT thread_id, body, message_at FROM dialpad_sms_messages
        WHERE body IS NOT NULL AND body != ''
    """, con)
    
    sms_count = 0
    for _, row in sms.iterrows():
        phone = thread_phone.get(str(row["thread_id"]))
        student = all_phones.get(phone, "") if phone else ""
        if not student:
            continue
        txt = str(row["body"])[:500]
        dt = str(row.get("message_at", ""))
        sentiment = sia.polarity_scores(txt)
        student_comms[student].append({
            "source": "sms",
            "text": txt,
            "date": dt[:10] if dt else "",
            "compound": sentiment["compound"],
            "neg": sentiment["neg"],
            "pos": sentiment["pos"],
            "neu": sentiment["neu"],
            "phone": phone,
        })
        sms_count += 1
    print(f"  SMS matched: {sms_count}")
    
    # ── Call reviews (via call_id from name matches) ──
    call_students = {}
    for key, info in matches["matches"].items():
        if key.startswith("call_"):
            call_students[key.replace("call_", "")] = info["student"]
    
    reviews = pd.read_sql_query("""
        SELECT call_id, transcript_text, recap_text, event_at
        FROM dialpad_call_reviews
        WHERE transcript_text IS NOT NULL OR recap_text IS NOT NULL
    """, con)
    
    call_count = 0
    for _, row in reviews.iterrows():
        cid = str(row["call_id"])
        student = call_students.get(cid)
        if not student:
            continue
        txt = (str(row.get("transcript_text", "") or "") + " " + str(row.get("recap_text", "") or ""))[:1000]
        dt = str(row.get("event_at", ""))
        sentiment = sia.polarity_scores(txt)
        student_comms[student].append({
            "source": "call_review",
            "text": txt,
            "date": dt[:10] if dt else "",
            "compound": sentiment["compound"],
            "neg": sentiment["neg"],
            "pos": sentiment["pos"],
            "neu": sentiment["neu"],
        })
        call_count += 1
    print(f"  Call reviews matched: {call_count}")
    
    print(f"  Unique students with comms: {len(student_comms)}")
    return student_comms


# ═══════════════════════════════════════════════════════════
# 2. FEATURE ENGINEERING
# ═══════════════════════════════════════════════════════════

def compute_features(student_comms):
    """For each student, compute sentiment + frequency features."""
    rows = []
    
    for student, comms in student_comms.items():
        if not comms:
            continue
        
        # Sort by date
        dated = [c for c in comms if c["date"]]
        dated.sort(key=lambda x: x["date"])
        
        # Filter to last LOOKBACK days
        cutoff = (TODAY - timedelta(days=LOOKBACK)).isoformat()
        recent = [c for c in dated if c["date"] >= cutoff]
        all_comms = recent if recent else dated  # fallback to all
        
        if not all_comms:
            continue
        
        # ── Sentiment features ──
        compounds = [c["compound"] for c in all_comms]
        negs = [c["neg"] for c in all_comms]
        poss = [c["pos"] for c in all_comms]
        
        avg_compound = float(np.mean(compounds))
        avg_neg = float(np.mean(negs))
        avg_pos = float(np.mean(poss))
        neg_ratio = sum(1 for c in compounds if c < -0.1) / max(len(compounds), 1)
        pos_ratio = sum(1 for c in compounds if c > 0.1) / max(len(compounds), 1)
        
        # Sentiment volatility
        sent_vol = float(np.std(compounds)) if len(compounds) >= 3 else 0.0
        
        # Sentiment trend: slope of compound over time
        sent_trend = 0.0
        if len(compounds) >= 3:
            xs = np.arange(len(compounds))
            if np.std(xs) > 0:
                sent_trend = float(np.polyfit(xs, compounds, 1)[0])
        
        # Most extreme sentiment
        min_compound = float(min(compounds))
        max_compound = float(max(compounds))
        
        # ── Frequency features ──
        n = len(all_comms)
        
        # Days since last communication
        last_date_str = all_comms[-1]["date"]
        try:
            last_date = date.fromisoformat(last_date_str)
            days_since_last = (TODAY - last_date).days
        except:
            days_since_last = 999
        
        # Recent spike: ratio of last 30d vs total
        cutoff_30 = (TODAY - timedelta(days=30)).isoformat()
        recent_30 = [c for c in all_comms if c["date"] >= cutoff_30]
        spike_ratio = len(recent_30) / max(n, 1)
        
        # Longest gap between communications
        longest_gap = 0
        if len(dated) >= 2:
            gaps = []
            for i in range(1, len(dated)):
                try:
                    d1 = date.fromisoformat(dated[i-1]["date"])
                    d2 = date.fromisoformat(dated[i]["date"])
                    gaps.append((d2 - d1).days)
                except:
                    pass
            longest_gap = max(gaps) if gaps else 0
        
        # Communication frequency CV (coefficient of variation)
        comm_cv = 0.0
        if len(dated) >= 3:
            try:
                dts = [date.fromisoformat(c["date"]) for c in dated if c["date"]]
                gaps = [(dts[i] - dts[i-1]).days for i in range(1, len(dts))]
                if np.mean(gaps) > 0:
                    comm_cv = float(np.std(gaps) / np.mean(gaps))
            except:
                pass
        
        # ── Content features ──
        all_text = " ".join(c["text"] for c in all_comms).lower()
        reschedule_count = len(re.findall(r'\b(?:reschedule|move|cancel|change)\s+(?:lesson|appointment|time|day|schedule)', all_text))
        reschedule_count += len(re.findall(r'\bcan\'t\s+make\b', all_text))
        reschedule_count += len(re.findall(r'\brunning\s+late\b', all_text))
        
        # Question count (engaged parent)
        question_count = len(re.findall(r'\?', all_text))
        
        # ── Source diversity ──
        sources = set(c["source"] for c in all_comms)
        
        # ── Per-source breakdown ──
        for src in ["voicemail", "sms", "call_review"]:
            src_comms = [c for c in all_comms if c["source"] == src]
            if src_comms:
                src_compounds = [c["compound"] for c in src_comms]
                src_count = len(src_comms)
            else:
                src_compounds = [0]
                src_count = 0
        
        row = {
            "student": student,
            "comm_count": n,
            "comm_count_voicemail": sum(1 for c in all_comms if c["source"] == "voicemail"),
            "comm_count_sms": sum(1 for c in all_comms if c["source"] == "sms"),
            "comm_count_call_review": sum(1 for c in all_comms if c["source"] == "call_review"),
            "comm_sources": len(sources),
            "days_since_last_comm": days_since_last,
            "recent_spike_ratio": round(spike_ratio, 3),
            "longest_comm_gap_days": longest_gap,
            "comm_frequency_cv": round(comm_cv, 3),
            "avg_sentiment_compound": round(avg_compound, 3),
            "avg_sentiment_neg": round(avg_neg, 3),
            "avg_sentiment_pos": round(avg_pos, 3),
            "sentiment_volatility": round(sent_vol, 3),
            "sentiment_trend": round(sent_trend, 4),
            "min_sentiment": round(min_compound, 3),
            "max_sentiment": round(max_compound, 3),
            "neg_ratio": round(neg_ratio, 3),
            "pos_ratio": round(pos_ratio, 3),
            "reschedule_count": reschedule_count,
            "question_count": question_count,
        }
        rows.append(row)
    
    df = pd.DataFrame(rows)
    print(f"\n  Students with features: {len(df)}")
    print(f"  Columns: {list(df.columns)}")
    return df


# ═══════════════════════════════════════════════════════════
# 3. MAIN
# ═══════════════════════════════════════════════════════════

def main():
    print("comm_sentiment_v2.py")
    print(f"  Lookback: {LOOKBACK} days")
    
    student_comms = load_matches()
    features = compute_features(student_comms)
    
    # Save
    MODELS_DIR.mkdir(exist_ok=True)
    out_path = MODELS_DIR / "comm_features_v2.csv"
    features.to_csv(out_path, index=False)
    print(f"\nSaved: {out_path} ({len(features)} rows × {len(features.columns)} columns)")
    
    # Summary stats
    print(f"\nFeature summary:")
    for col in ["comm_count", "days_since_last_comm", "avg_sentiment_compound", 
                "sentiment_trend", "neg_ratio", "reschedule_count"]:
        if col in features.columns:
            vals = features[col].dropna()
            print(f"  {col}: mean={vals.mean():.3f}, median={vals.median():.3f}, std={vals.std():.3f}")
    
    # Top/bottom sentiment
    print(f"\nTop 5 most negative sentiment:")
    top_neg = features.nsmallest(5, "avg_sentiment_compound")
    for _, r in top_neg.iterrows():
        print(f"  {r['student']}: {r['avg_sentiment_compound']:.3f} ({r['comm_count']} comms)")
    
    print(f"\nTop 5 most communicative:")
    top_comm = features.nlargest(5, "comm_count")
    for _, r in top_comm.iterrows():
        print(f"  {r['student']}: {r['comm_count']} comms, sentiment={r['avg_sentiment_compound']:.3f}")

if __name__ == "__main__":
    main()
