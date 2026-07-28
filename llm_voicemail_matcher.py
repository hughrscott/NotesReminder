#!/usr/bin/env python3
"""
llm_voicemail_matcher.py — Use LLM to extract student names from unmatched voicemail transcripts.
Batch process all unmatched VMs, match extracted names to pike13_people.
"""
import sqlite3, json, csv, re, time, requests
from pathlib import Path
from collections import defaultdict

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"
BATCH_SIZE = 15

env = {}
for line in (Path.home() / ".hermes" / ".env").read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")

SYSTEM = """You are a name extraction specialist for a music school's voicemail system.
Given a voicemail transcript, extract the STUDENT'S name (the person taking lessons).
Parents often call on behalf of their children. Look for patterns like:
- "my son/daughter/child NAME" → extract NAME
- "NAME's mom/dad" → extract NAME
- "calling about NAME" → extract NAME
- "this is PARENT, NAME's mom" → extract NAME (the child), not the parent
- "I'm calling for NAME" → extract NAME
- Sometimes the caller IS the student: "Hi, my name is NAME, I have a lesson..."
- Multiple students: "X and Y's mom" → extract both

Return JSON array with one object per transcript:
[{"idx": 0, "student_name": "Luke Woods", "confidence": "high"},
 {"idx": 1, "student_name": null, "confidence": "none"}]

Use null if no student name is mentioned. Be precise — don't guess."""

def get_unmatched_vms(con):
    """Get voicemails that haven't been matched yet."""
    # Reuse v3 matching logic to identify unmatched
    from comms_matcher_v3 import build_indices, match_all_comms
    _, _, name_idx, first_idx, _, instructors, _, _ = build_indices(con)
    results = match_all_comms(con)
    
    # Get all VM rows
    all_vms = con.execute("""
        SELECT call_id, external_number, transcription_text, date
        FROM dialpad_voicemails
        WHERE transcription_text IS NOT NULL AND transcription_text != ''
        ORDER BY date
    """).fetchall()
    
    matched_ids = set()
    for comms in results["voicemail"]["student_comms"].values():
        for c in comms:
            matched_ids.add(c["call_id"])
    
    unmatched = [dict(r) for r in all_vms if r["call_id"] not in matched_ids]
    print(f"  {len(unmatched)} unmatched VMs out of {len(all_vms)}")
    return unmatched


def extract_names_llm(vms):
    """Extract student names from voicemail transcripts using LLM."""
    all_names = {}
    
    for b in range(0, len(vms), BATCH_SIZE):
        batch = vms[b:b+BATCH_SIZE]
        snippets = []
        for i, vm in enumerate(batch):
            text = (vm["transcription_text"] or "")[:300].replace("\n", " ")
            snippets.append(f"[{i}] {text}")
        
        prompt = "Extract student names from these voicemail transcripts:\n\n" + "\n\n".join(snippets)
        
        try:
            r = requests.post("https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {env.get('OPENAI_API_KEY','')}", "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini", "messages": [
                    {"role": "system", "content": SYSTEM},
                    {"role": "user", "content": prompt}
                ], "temperature": 0.1, "max_tokens": 2000, "response_format": {"type": "json_object"}},
                timeout=60)
            
            if r.status_code == 200:
                data = r.json()
                content = data["choices"][0]["message"]["content"]
                results = json.loads(content)
                if isinstance(results, dict):
                    for k in results:
                        if isinstance(results[k], list):
                            results = results[k]
                            break
                
                for item in results:
                    idx = item.get("idx", -1)
                    name = item.get("student_name")
                    if isinstance(idx, int) and 0 <= idx < len(batch) and name:
                        all_names[batch[idx]["call_id"]] = {
                            "student_name": name,
                            "confidence": item.get("confidence", "unknown"),
                            "transcript": batch[idx]["transcription_text"][:200]
                        }
                
                print(f"  Batch {b//BATCH_SIZE+1}/{(len(vms)-1)//BATCH_SIZE+1}: {len(all_names)} names found so far")
        except Exception as e:
            print(f"  Batch error: {e}")
        
        time.sleep(0.5)
    
    return all_names


def match_to_students(con, extracted_names):
    """Match extracted names to pike13_people using fuzzy matching."""
    from comms_matcher_v3 import fuzzy_name_match, expand_names
    
    # Build name index
    name_candidates = {}
    first_name_map = defaultdict(list)
    for row in con.execute("""
        SELECT person_id, full_name, first_name, last_name
        FROM pike13_people WHERE full_name IS NOT NULL AND full_name != ''
    """):
        full = (row["full_name"] or "").strip()
        first = (row["first_name"] or "").strip()
        last = (row["last_name"] or "").strip()
        entry = {"name": full, "person_id": row["person_id"], "first": first, "last": last}
        name_candidates[full.lower()] = entry
        if first: first_name_map[first.lower()].append(entry)
    
    expand_names(name_candidates, first_name_map)
    
    matches = {}
    for call_id, info in extracted_names.items():
        raw_names = info["student_name"]
        if isinstance(raw_names, str):
            raw_names = [raw_names]
        if not isinstance(raw_names, list):
            continue
        
        for raw_name in raw_names:
            raw_name = str(raw_name).lower().strip()
            if not raw_name or raw_name == "none" or raw_name == "null":
                continue
            match, score = fuzzy_name_match(raw_name, name_candidates, threshold=0.6)
            if match:
                key = f"{call_id}_{raw_name[:20]}"
                matches[key] = {
                    **info,
                    "extracted_name": raw_name,
                    "matched_name": match[0],
                    "match_score": score,
                    "person_id": match[1]["person_id"]
                }
    
    return matches


def main():
    print("=" * 60)
    print("LLM Voicemail Name Extractor")
    print("=" * 60)
    
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    
    print("\n[1] Finding unmatched voicemails...")
    unmatched = get_unmatched_vms(con)
    
    if not unmatched:
        print("  All matched!")
        return
    
    print(f"\n[2] Extracting names via LLM from {len(unmatched)} VMs...")
    extracted = extract_names_llm(unmatched)
    print(f"  Extracted {len(extracted)} names")
    
    print("\n[3] Matching to students...")
    matched = match_to_students(con, extracted)
    print(f"  Matched {len(matched)} to students")
    
    # Show examples
    print("\n[4] Sample matches:")
    for call_id, info in list(matched.items())[:10]:
        print(f"  LLM: '{info['student_name']}' → DB: '{info['matched_name']}' (score: {info['match_score']:.2f})")
    
    # Save
    out = {
        "total_unmatched": len(unmatched),
        "names_extracted": len(extracted),
        "students_matched": len(matched),
        "matches": {k: {kk: str(vv) for kk, vv in v.items()} for k, v in matched.items()}
    }
    out_path = MODELS_DIR / "llm_vm_name_matches.json"
    json.dump(out, open(out_path, "w"), indent=2, default=str)
    print(f"\n  Saved: {out_path}")
    
    con.close()


if __name__ == "__main__":
    main()
