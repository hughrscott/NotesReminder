#!/usr/bin/env python3
"""match_comms_by_name.py — Link unmatched communications to students via name extraction.

4-pass approach (Gemini-reviewed):
  Pass 1: Exact full-name match (regex, word boundary) — high precision
  Pass 2: LLM extraction (handles nicknames, misspellings, implicit mentions)
  Pass 3: First-name + context heuristic (weighted disambiguation)
  Pass 4: Email thread deduction

Output: models/comms_name_matches.json — phone/email → (student_name, confidence, pass)
"""

import sqlite3, re, json, os, sys
from pathlib import Path
from datetime import date, timedelta
from collections import Counter, defaultdict
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"
OUTPUT = MODELS_DIR / "comms_name_matches.json"

# ── Load student name DB ──
con = sqlite3.connect(str(DB_PATH))

def load_student_names():
    """Load all student names from Pike13, plus lesson history for context."""
    ppl = pd.read_sql_query("SELECT full_name FROM pike13_people", con)
    all_names = [str(n).strip() for n in ppl["full_name"].tolist() if str(n).strip()]
    
    # Build: normalized name → list of (original name, school_id)
    name_to_schools = defaultdict(list)
    name_to_full = {}
    
    # Use lessons table to determine school
    lessons = pd.read_sql_query("SELECT students_raw, school_id FROM lessons", con)
    
    for n in all_names:
        norm = n.lower().strip()
        name_to_full[norm] = n
        # Find school
        mask = lessons["students_raw"].str.contains(re.escape(n), na=False, case=False)
        if mask.any():
            for sid in lessons[mask]["school_id"].dropna().unique():
                name_to_schools[norm].append(int(sid))
        else:
            name_to_schools[norm] = [1, 2]  # unknown — assign to both
    
    # First-name → list of full names (for disambiguation)
    first_to_full = defaultdict(list)
    for n in all_names:
        parts = n.lower().split()
        if parts:
            first_to_full[parts[0]].append(n)
    
    # Build set of valid first names (appearing ≥2 times, ≥3 chars)
    first_counts = Counter()
    for n in all_names:
        parts = n.lower().split()
        if parts:
            first_counts[parts[0]] += 1
    
    common_words = {'the','and','for','van','de','la','da','of','in','to','a','i','you',
        'he','she','it','we','they','is','are','was','were','be','been','have','has',
        'had','do','does','did','will','would','could','should','not','no','yes',
        'good','bad','great','new','old','big','small','high','low','long','short',
        'thank','thanks','please','sorry','hello','hi','hey','ok','okay','well',
        'just','like','know','think','want','need','get','got','go','come','came',
        'school','rock','music','lesson','time','day','week','today','tomorrow',
        'call','called','calling','leave','left','message','phone','number',
        'student','child','kid','son','daughter','parent','mom','dad',
        'hank','hool','read','mark','will','may','hope','joy','grace','rose',
        'summer','fall','winter','spring','morning','evening','afternoon',
        'right','left','back','next','last','first','second','third',
        'also','much','many','more','most','some','any','each','every',
        'here','there','where','when','what','which','who','how'}
    
    valid_first = {fn for fn, cnt in first_counts.items() 
                   if cnt >= 2 and len(fn) >= 3 and fn not in common_words}
    
    # Instructor names — skip (not available directly, use IDs)
    instructor_names = set()  # Could map instructor_id → name later
    print(f"  Loaded {len(all_names)} students, {len(valid_first)} valid first names")
    
    return {
        "all_names": all_names,
        "name_to_full": name_to_full,
        "name_to_schools": name_to_schools,
        "first_to_full": first_to_full,
        "valid_first": valid_first,
        "instructor_names": instructor_names,
        "lessons": lessons,
    }

NAMES = load_student_names()

# ── Load existing phone matches (so we don't duplicate) ──
def load_existing_matches():
    """Phones already linked to students via Pike13 or identity_matches."""
    matched = set()
    
    # Pike13 people phones
    for col in ["phone", "phone_normalized"]:
        try:
            df = pd.read_sql_query(f"SELECT {col} FROM pike13_people WHERE {col} IS NOT NULL", con)
            for p in df[col]:
                normalized = re.sub(r'\D', '', str(p))
                if normalized:
                    matched.add(normalized)
        except:
            pass
    
    # Identity matches
    try:
        df = pd.read_sql_query("SELECT phone FROM identity_matches WHERE phone IS NOT NULL", con)
        for p in df["phone"]:
            matched.add(re.sub(r'\D', '', str(p)))
    except:
        pass
    
    # Pike13 clients
    try:
        df = pd.read_sql_query("""SELECT "Account Manager Phones" as phones FROM pike13_clients 
                                   WHERE "Account Manager Phones" IS NOT NULL""", con)
        for phones in df["phones"]:
            for p in str(phones).split(","):
                normalized = re.sub(r'\D', '', p.strip())
                if normalized:
                    matched.add(normalized)
    except:
        pass
    
    print(f"  {len(matched)} phones already matched")
    return matched

EXISTING = load_existing_matches()

def normalize_phone(phone):
    return re.sub(r'\D', '', str(phone))


# ═══════════════════════════════════════════════════════════════════
# PASS 1: Exact full-name match
# ═══════════════════════════════════════════════════════════════════

def pass1_exact_match():
    """Match full first+last name with word boundaries in transcripts."""
    results = {}  # phone → [(name, confidence, pass)]
    
    # Voicemails
    vms = pd.read_sql_query("""
        SELECT DISTINCT external_number, LOWER(transcription_text) as txt
        FROM dialpad_voicemails 
        WHERE transcription_text IS NOT NULL AND transcription_text != ''
    """, con)
    
    for _, row in vms.iterrows():
        phone = normalize_phone(row['external_number'])
        if phone in EXISTING or not phone:
            continue
        txt = str(row['txt']) if row['txt'] else ''
        
        for name in NAMES["all_names"]:
            pattern = r'\b' + re.escape(name.lower()) + r'\b'
            if re.search(pattern, txt):
                if phone not in results:
                    results[phone] = []
                results[phone].append((name, 0.95, "pass1"))
    
    # Call reviews
    reviews = pd.read_sql_query("""
        SELECT call_id, LOWER(transcript_text) as txt
        FROM dialpad_call_reviews
        WHERE transcript_text IS NOT NULL
    """, con)
    
    for _, row in reviews.iterrows():
        txt = str(row['txt']) if row['txt'] else ''
        for name in NAMES["all_names"]:
            pattern = r'\b' + re.escape(name.lower()) + r'\b'
            if re.search(pattern, txt):
                # Call reviews don't have phone — store by call_id
                key = f"call_{row['call_id']}"
                if key not in results:
                    results[key] = []
                results[key].append((name, 0.95, "pass1"))
    
    print(f"  Pass 1 (exact name): {len(results)} matches")
    return results


# ═══════════════════════════════════════════════════════════════════
# PASS 2: LLM extraction 
# ═══════════════════════════════════════════════════════════════════

def pass2_llm_extraction(already_matched):
    """Use a cheap LLM to extract student names from transcript text."""
    # Try to use local model or API
    results = {}
    
    # Get unmatched voicemails (not in Pass 1, not already matched)
    already_phones = set(already_matched.keys()) | {p for p in already_matched if not p.startswith("call_")}
    
    vms = pd.read_sql_query("""
        SELECT external_number, transcription_text
        FROM dialpad_voicemails 
        WHERE transcription_text IS NOT NULL AND transcription_text != ''
        ORDER BY RANDOM() LIMIT 200
    """, con)
    
    unmatched_vms = []
    for _, row in vms.iterrows():
        phone = normalize_phone(row['external_number'])
        if phone in EXISTING or phone in already_phones or not phone:
            continue
        unmatched_vms.append((phone, str(row['transcription_text'])[:500]))
    
    print(f"  Pass 2 (LLM): {len(unmatched_vms)} unmatched voicemails to scan")
    
    if not unmatched_vms:
        return results
    
    # Build prompt with name list
    name_list = "\n".join(sorted(NAMES["all_names"])[:300])  # Top 300 to stay in context
    batch_size = 20
    
    for i in range(0, len(unmatched_vms), batch_size):
        batch = unmatched_vms[i:i+batch_size]
        
        prompt = f"""You are analyzing voicemail transcripts for a music school called School of Rock.
Your ONLY job is to extract student names mentioned in these transcripts.
The school's students include (but are not limited to):

{name_list}

For each transcript below, output ONLY the student's full name if mentioned, or "none" if no student name is found.
Output format: one line per transcript, "INDEX: name" or "INDEX: none"

--- TRANSCRIPTS ---
"""
        for j, (phone, txt) in enumerate(batch):
            prompt += f"\n[{j}] {txt[:400]}\n"
        
        try:
            import openai
            # Read API key from Hermes .env if not in os.environ
            api_key = os.environ.get("DEEPSEEK_API_KEY", "")
            if not api_key:
                env_file = Path.home() / ".hermes" / ".env"
                if env_file.exists():
                    for line in open(env_file):
                        if line.startswith("DEEPSEEK_API_KEY="):
                            api_key = line.strip().split("=", 1)[1].strip('"').strip("'")
                            break
            if not api_key:
                print("  No DEEPSEEK_API_KEY found")
                continue
            client = openai.OpenAI(
                api_key=api_key,
                base_url="https://api.deepseek.com/v1"
            )
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=200,
                temperature=0
            )
            output = response.choices[0].message.content
            
            # Parse output
            for j, (phone, _) in enumerate(batch):
                # Find line matching [j]
                pattern = re.compile(rf'\[{j}\]\s*(.+)', re.IGNORECASE)
                match = pattern.search(output)
                if match:
                    name = match.group(1).strip().lower()
                    if name != "none" and name in NAMES["name_to_full"]:
                        results[phone] = [(NAMES["name_to_full"][name], 0.85, "pass2")]
            
        except Exception as e:
            print(f"  LLM batch {i} failed: {e}")
            continue
    
    print(f"  Pass 2 (LLM): {len(results)} matches")
    return results


# ═══════════════════════════════════════════════════════════════════
# PASS 3: First name + context heuristic
# ═══════════════════════════════════════════════════════════════════

def pass3_context_heuristic(already_matched):
    """Match first names, disambiguate with weighted scoring."""
    results = {}
    already_phones = set()
    for k in already_matched:
        if not k.startswith("call_"):
            already_phones.add(k)
    
    # Load full lessons for recency lookup
    full_lessons = pd.read_sql_query("SELECT students_raw, lesson_date FROM lessons", con)
    
    vms = pd.read_sql_query("""
        SELECT external_number, LOWER(transcription_text) as txt
        FROM dialpad_voicemails 
        WHERE transcription_text IS NOT NULL AND transcription_text != ''
    """, con)
    
    for _, row in vms.iterrows():
        phone = normalize_phone(row['external_number'])
        if phone in EXISTING or phone in already_phones or not phone:
            continue
        
        txt = str(row['txt']) if row['txt'] else ''
        words = set(re.findall(r'\b[a-z]{3,}\b', txt))
        
        # Find candidates: first names that appear in transcript
        candidates = []
        for fn in NAMES["valid_first"]:
            if fn in words:
                for full in NAMES["first_to_full"][fn]:
                    candidates.append(full)
        
        if not candidates:
            continue
        
        unique = list(set(candidates))
        
        if len(unique) == 1:
            results[phone] = [(unique[0], 0.75, "pass3")]
        else:
            # Weighted disambiguation
            best = None
            best_score = 0
            
            for cand in unique:
                score = 0.0
                
                # School weight (40%) — prefer candidate whose school peers are fewer
                cand_schools = set(NAMES["name_to_schools"].get(cand.lower(), [1,2]))
                mates = sum(1 for c in unique 
                           if set(NAMES["name_to_schools"].get(c.lower(), [1,2])) & cand_schools)
                if mates == 1:
                    score += 0.40
                else:
                    score += 0.40 / mates
                
                # Recency weight (20%) — prefer recently active candidate
                mask = full_lessons["students_raw"].str.contains(re.escape(cand), na=False, case=False)
                cand_dates = full_lessons[mask]
                if len(cand_dates) > 0:
                    last_date = pd.to_datetime(cand_dates["lesson_date"]).max()
                    days_ago = (pd.Timestamp(date.today()) - last_date).days
                    score += 0.20 * max(0, 1 - days_ago/365)
                
                if score > best_score:
                    best_score = score
                    best = cand
            
            if best and best_score > 0.20:
                conf = min(0.55 + best_score * 0.3, 0.85)
                results[phone] = [(best, conf, "pass3")]
    
    print(f"  Pass 3 (context): {len(results)} matches")
    return results


# ═══════════════════════════════════════════════════════════════════
# PASS 4: Email thread deduction
# ═══════════════════════════════════════════════════════════════════

def pass4_email_deduction(already_matched):
    """Extract student names from school email subjects and bodies."""
    results = {}
    
    emails = pd.read_sql_query("""
        SELECT message_id, subject, body, from_email, direction
        FROM school_email_messages
        WHERE subject IS NOT NULL AND body IS NOT NULL
        ORDER BY message_at DESC
        LIMIT 500
    """, con)
    
    print(f"  Pass 4 (email): scanning {len(emails)} emails")
    
    # Pre-compile name patterns for speed
    name_pattern = re.compile(
        r'\b(' + '|'.join(re.escape(n.lower()) for n in sorted(NAMES["all_names"], key=len, reverse=True)[:200]) + r')\b',
        re.IGNORECASE
    )
    
    for _, row in emails.iterrows():
        subject = str(row['subject'] or '')[:200]
        body_text = str(row['body'] or '')[:500]
        combined = subject + " " + body_text
        
        matches = name_pattern.findall(combined.lower())
        if matches:
            sender = str(row['from_email'] or '').strip().lower()
            if not sender:
                continue
            # Take most specific (longest) match
            best = max(set(matches), key=len)
            # Look up full name
            best_norm = best.lower()
            if best_norm in NAMES["name_to_full"]:
                full_name = NAMES["name_to_full"][best_norm]
            else:
                full_name = best.title()
            
            if sender not in results:
                results[sender] = []
            results[sender].append((full_name, 0.80, "pass4"))
    
    print(f"  Pass 4 (email): {len(results)} matches")
    return results


# ═══════════════════════════════════════════════════════════════════
# MERGE & SAVE
# ═══════════════════════════════════════════════════════════════════

def merge_results(*pass_results):
    """Merge all passes, keeping highest confidence match per phone."""
    merged = {}
    
    for pass_num, results in enumerate(pass_results, 1):
        for key, matches in results.items():
            if not matches:
                continue
            # Take highest confidence match
            best = max(matches, key=lambda x: x[1])
            if key not in merged or best[1] > merged[key][1]:
                merged[key] = {
                    "student": best[0],
                    "confidence": round(best[1], 2),
                    "pass": best[2],
                    "matches_considered": [m[0] for m in matches],
                }
    
    return merged


def main():
    print("match_comms_by_name.py")
    print(f"  Students: {len(NAMES['all_names'])}")
    print(f"  Already matched phones: {len(EXISTING)}")
    print()
    
    # Run passes
    print("--- Pass 1: Exact Name Match ---")
    p1 = pass1_exact_match()
    
    all_matched = p1
    
    print("\n--- Pass 2: LLM Extraction ---")
    p2 = pass2_llm_extraction(all_matched)
    all_matched = {**all_matched, **p2}
    
    print("\n--- Pass 3: Context Heuristic ---")
    p3 = pass3_context_heuristic(all_matched)
    all_matched = {**all_matched, **p3}
    
    print("\n--- Pass 4: Email Deduction ---")
    p4 = pass4_email_deduction(all_matched)
    
    # Merge
    merged = merge_results(p1, p2, p3, p4)
    
    # Stats
    total_new = len(merged)
    voicemail_matches = sum(1 for k in merged if not k.startswith("call_") and "@" not in k)
    email_matches = sum(1 for k in merged if "@" in k)
    call_matches = sum(1 for k in merged if k.startswith("call_"))
    
    pass1_count = sum(1 for v in merged.values() if v["pass"] == "pass1")
    pass2_count = sum(1 for v in merged.values() if v["pass"] == "pass2")
    pass3_count = sum(1 for v in merged.values() if v["pass"] == "pass3")
    pass4_count = sum(1 for v in merged.values() if v["pass"] == "pass4")
    
    high_conf = sum(1 for v in merged.values() if v["confidence"] >= 0.80)
    med_conf = sum(1 for v in merged.values() if 0.60 <= v["confidence"] < 0.80)
    low_conf = sum(1 for v in merged.values() if v["confidence"] < 0.60)
    
    # Save
    MODELS_DIR.mkdir(exist_ok=True)
    output = {
        "generated": str(date.today()),
        "total_new_matches": total_new,
        "by_type": {"voicemail_phone": voicemail_matches, "email": email_matches, "call_id": call_matches},
        "by_pass": {"pass1": pass1_count, "pass2": pass2_count, "pass3": pass3_count, "pass4": pass4_count},
        "by_confidence": {"high": high_conf, "medium": med_conf, "low": low_conf},
        "matches": merged,
    }
    
    json.dump(output, open(OUTPUT, "w"), indent=2)
    
    print(f"\n{'='*60}")
    print(f"RESULTS: {total_new} new matches")
    print(f"  By source: {voicemail_matches} phone, {email_matches} email, {call_matches} call review")
    print(f"  By pass: P1={pass1_count} P2={pass2_count} P3={pass3_count} P4={pass4_count}")
    print(f"  By confidence: {high_conf} high, {med_conf} medium, {low_conf} low")
    print(f"  Saved to: {OUTPUT}")
    
    # Sample
    print(f"\nSample matches:")
    for i, (key, v) in enumerate(list(merged.items())[:8]):
        name = v["student"]
        conf = v["confidence"]
        p = v["pass"]
        print(f"  {name} ({conf:.0%} conf, {p}) — key: {key[:30]}...")

if __name__ == "__main__":
    main()
