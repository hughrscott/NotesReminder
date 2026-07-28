#!/usr/bin/env python3
"""
comms_universal_matcher.py — Comprehensive student matching across all comm channels.
Strategies: phone (normalized), email (normalized), name (fuzzy + regex + content extraction),
cross-channel propagation. Target: >90% match rate.
"""
import sqlite3, json, csv, re
from pathlib import Path
from collections import defaultdict

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"


def norm_phone(raw):
    """Normalize phone: strip all non-digits, take last 10 (handles +1, formatting)."""
    if not raw:
        return None
    digits = re.sub(r'\D', '', str(raw))
    if len(digits) >= 10:
        return digits[-10:]
    return digits if len(digits) >= 7 else None


def norm_email(raw):
    """Normalize email: lowercase, strip whitespace."""
    if not raw:
        return None
    return str(raw).strip().lower()


def fuzzy_name_match(query, candidates, threshold=0.85):
    """Match a name query against candidates using token-based fuzzy matching.
    Returns best match and score, or (None, 0)."""
    if not query or len(query) < 3:
        return None, 0

    query_tokens = set(query.lower().split())
    best_score = 0
    best_match = None

    for cand_name, cand_data in candidates.items():
        cand_tokens = set(cand_name.lower().split())
        if not cand_tokens:
            continue
        # Jaccard similarity on tokens
        intersection = query_tokens & cand_tokens
        union = query_tokens | cand_tokens
        score = len(intersection) / len(union) if union else 0

        # Bonus for exact substring
        if query.lower() in cand_name.lower() or cand_name.lower() in query.lower():
            score = max(score, 0.9)

        if score > best_score:
            best_score = score
            best_match = (cand_name, cand_data)

    if best_score >= threshold:
        return best_match, best_score
    return None, 0


def extract_names_from_text(text):
    """Extract potential student/parent name references from text."""
    if not text:
        return []
    text = str(text).lower()
    names = []

    # Pattern: "my name is X" / "this is X calling" / "I'm X"
    intro_patterns = [
        r"(?:my\s+name\s+is\s+)([\w\s]+?)(?:\.|,|\s+i\s|\s+and\s|$)",
        r"(?:this\s+is\s+)([\w\s]+?)(?:\s+(?:calling|and|i|i'm|i\s+am)|\.|,|$)",
        r"(?:i'?m\s+)([\w\s]+?)(?:\s+(?:and|calling|call|just)|\.|,|$)",
    ]
    for pat in intro_patterns:
        for m in re.finditer(pat, text):
            name = m.group(1).strip()
            if len(name) >= 2 and name not in ('the', 'a', 'an', 'my', 'his', 'her', 'just', 'also', 'not', 'calling'):
                names.append(name)

    # Pattern: "my son/daughter/child/kid X" or "X's mom/dad/parent"
    family_patterns = [
        r"(?:my\s+(?:son|daughter|child|kid)s?\s+)(\w+(?:\s+\w+)?)",
        r"(\w+(?:\s+\w+)?)'s\s+(?:mom|dad|parent|mother|father)",
        r"(?:the\s+)?(?:mom|dad|parent)\s+of\s+(\w+(?:\s+\w+)?)",
        r"(?:i\s+(?:am|i'm)\s+)(?:calling\s+)?(?:for|about)\s+(\w+(?:\s+\w+)?)",
        r"(?:calling\s+(?:for|about|regarding)\s+)(\w+(?:\s+\w+)?)",
    ]
    for pat in family_patterns:
        for m in re.finditer(pat, text):
            name = m.group(1).strip()
            if len(name) >= 2 and name not in ('the', 'a', 'an', 'my', 'his', 'her', 'our', 'your'):
                names.append(name)

    # Also try to find capitalized proper names (2+ word sequences)
    proper = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+))\b', str(text))
    names.extend([n.strip().lower() for n in proper if len(n) > 2])

    return names


def build_indices(con):
    """Build all lookup indices from pike13_people AND hubspot_contacts."""
    phone_to_student = {}  # normalized phone -> {name, person_id}
    email_to_student = {}  # normalized email -> {name, person_id}
    name_candidates = {}   # full name -> {name, person_id, phones, emails, first, last}
    first_name_map = defaultdict(list)
    shared_phones = defaultdict(list)  # phone -> [student entries] for sibling propagation

    # ── Pike13 people ──
    rows = con.execute("""
        SELECT person_id, full_name, first_name, last_name, 
               email_normalized, phone_normalized, school
        FROM pike13_people
        WHERE full_name IS NOT NULL AND full_name != ''
    """).fetchall()

    for row in rows:
        full = (row["full_name"] or "").strip()
        first = (row["first_name"] or "").strip()
        last = (row["last_name"] or "").strip()
        email = norm_email(row["email_normalized"])
        phone = norm_phone(row["phone_normalized"])
        person_id = row["person_id"]

        entry = {"name": full, "person_id": person_id, "first": first, "last": last}
        name_candidates[full.lower()] = entry

        if phone:
            if phone not in phone_to_student:
                phone_to_student[phone] = entry
            shared_phones[phone].append(entry)
        if email:
            email_to_student[email] = entry
        if first:
            first_name_map[first.lower()].append(entry)

    # ── HubSpot contacts (additional phone universe) ──
    hs_rows = con.execute("""
        SELECT hc.phone_normalized, hc.full_name, hc.person_id,
               pp.full_name as p13_name
        FROM hubspot_contacts hc
        LEFT JOIN pike13_people pp ON hc.person_id = pp.person_identity_id
        WHERE hc.phone_normalized IS NOT NULL AND hc.phone_normalized != ''
          AND hc.person_id IS NOT NULL
    """).fetchall()

    hs_added = 0
    for row in hs_rows:
        phone = norm_phone(row["phone_normalized"])
        if not phone or phone in phone_to_student:
            continue
        name = row["p13_name"] or row["full_name"] or ""
        entry = {"name": name.strip(), "person_id": row["person_id"], "first": "", "last": ""}
        phone_to_student[phone] = entry
        shared_phones[phone].append(entry)
        hs_added += 1

    return phone_to_student, email_to_student, name_candidates, first_name_map, shared_phones, hs_added


def match_phone(phone_raw, phone_index):
    """Match a phone number to a student."""
    phone = norm_phone(phone_raw)
    if phone and phone in phone_index:
        return phone_index[phone]
    return None


def match_email(email_raw, email_index):
    """Match an email to a student."""
    email = norm_email(email_raw)
    if email and email in email_index:
        return email_index[email]
    return None


def match_name_in_text(text, name_candidates, first_name_map):
    """Match by finding student names in arbitrary text using fuzzy matching."""
    if not text:
        return None, 0

    # Strategy 1: try exact full names first (fast, most reliable)
    text_lower = str(text).lower()
    for name, data in sorted(name_candidates.items(), key=lambda x: -len(x[0])):
        if len(name) >= 4 and name in text_lower:
            return data, 1.0

    # Strategy 2: extract candidate names from text, then fuzzy match
    extracted = extract_names_from_text(str(text))
    best_score = 0
    best_match = None

    for name_fragment in extracted:
        # Try fuzzy match against full names
        match, score = fuzzy_name_match(name_fragment, name_candidates, threshold=0.7)
        if match and score > best_score:
            best_score = score
            best_match = match[1]

        # Try first name only
        if name_fragment in first_name_map:
            candidates = first_name_map[name_fragment]
            if len(candidates) == 1:
                return candidates[0], 0.85
            # Multiple candidates with same first name — try to disambiguate
            # For now, return the first (imperfect but better than nothing)
            return candidates[0], 0.7

    if best_match:
        return best_match, best_score
    return None, 0


def match_all_comms(con):
    """Match ALL communications to students. Returns per-channel stats and student-indexed comms."""
    phone_idx, email_idx, name_idx, first_idx, shared_phones, _ = build_indices(con)

    results = {
        "email": {"total": 0, "matched": 0, "strategies": defaultdict(int), "student_comms": defaultdict(list)},
        "sms": {"total": 0, "matched": 0, "strategies": defaultdict(int), "student_comms": defaultdict(list)},
        "voicemail": {"total": 0, "matched": 0, "strategies": defaultdict(int), "student_comms": defaultdict(list)},
    }

    # ─── 1. SCHOOL EMAILS (filter out HubSpot timeline noise) ───
    emails = con.execute("""
        SELECT message_id, thread_id, subject, snippet, body,
               external_email_normalized, from_email, to_emails, cc_emails,
               direction, school, message_at
        FROM school_email_messages
        WHERE (body IS NOT NULL AND body != '' OR subject IS NOT NULL)
          AND (subject NOT LIKE '%HubSpot%' OR subject IS NULL)
          AND (snippet NOT LIKE '%HubSpot%' OR snippet IS NULL)
        ORDER BY message_at
    """).fetchall()
    results["email"]["total"] = len(emails)

    # Build thread context for propagation
    thread_students = {}  # thread_id -> {student_name, person_id}

    for row in emails:
        mid = row["message_id"]
        tid = row["thread_id"]
        matched = None
        strategy = None

        # A: Thread already matched
        if tid and tid in thread_students:
            matched = thread_students[tid]
            strategy = "thread_propagation"

        # B: Email exact match
        if not matched:
            matched = match_email(row["external_email_normalized"], email_idx)
            if matched:
                strategy = "email_exact"

        # C: Try all email fields (from, to, cc)
        if not matched:
            for field in ["from_email", "to_emails", "cc_emails"]:
                val = row[field]
                if val:
                    for addr in re.split(r'[,;]\s*', str(val)):
                        matched = match_email(addr, email_idx)
                        if matched:
                            strategy = f"email_{field}"
                            break
                if matched:
                    break

        # D: Name in content (subject + snippet + body)
        if not matched:
            text = " ".join(str(v or "") for v in [row["subject"], row["snippet"], (row["body"] or "")[:1000]])
            matched, score = match_name_in_text(text, name_idx, first_idx)
            if matched:
                strategy = f"name_in_content_{score:.2f}"

        # E: Name in subject line specifically (e.g. "Re: Dominique's Lessons")
        if not matched and row["subject"]:
            subj = str(row["subject"])
            subj_names = re.findall(r'(?:Re:|for-?|about|regarding)\s+(\w+)', subj, re.IGNORECASE)
            for name in subj_names:
                if len(name) >= 3:
                    fmatch, fscore = fuzzy_name_match(name.lower(), name_idx, threshold=0.7)
                    if fmatch:
                        matched = fmatch[1]  # (name, data) -> data dict
                        strategy = f"name_in_subject_{fscore:.2f}"
                        break

        if matched:
            results["email"]["matched"] += 1
            results["email"]["strategies"][strategy] += 1
            results["email"]["student_comms"][matched["name"]].append(dict(row))
            if tid:
                thread_students[tid] = matched

    # ─── 2. VOICEMAILS (run before SMS for cross-channel phone discovery) ───
    discovered_phones = {}  # phone -> student (cross-channel discovery)

    vm_rows = con.execute("""
        SELECT call_id, external_number, transcription_text, date, name as vm_name
        FROM dialpad_voicemails
        WHERE transcription_text IS NOT NULL AND transcription_text != ''
        ORDER BY date
    """).fetchall()
    results["voicemail"]["total"] = len(vm_rows)

    for row in vm_rows:
        matched = None
        strategy = None

        # A: Phone match (with normalized comparison)
        matched = match_phone(row["external_number"], phone_idx)
        if matched:
            strategy = "phone_exact"

        # B: Name in transcription
        if not matched:
            text = str(row["transcription_text"] or "")[:1000]
            matched, score = match_name_in_text(text, name_idx, first_idx)
            if matched:
                strategy = f"name_in_transcript_{score:.2f}"
                # Cross-channel: remember this phone→student mapping
                phone_norm = norm_phone(row["external_number"])
                if phone_norm:
                    discovered_phones[phone_norm] = matched

        if matched:
            results["voicemail"]["matched"] += 1
            results["voicemail"]["strategies"][strategy] += 1
            results["voicemail"]["student_comms"][matched["name"]].append(dict(row))

    # ─── 3. SMS MESSAGES (uses discovered_phones from voicemails) ───
    # Load thread→phone mapping
    thread_phones = {}
    for row in con.execute("SELECT thread_id, phone FROM dialpad_sms_threads WHERE phone IS NOT NULL"):
        thread_phones[str(row["thread_id"])] = row["phone"]

    sms_rows = con.execute("""
        SELECT message_id, thread_id, body, message_at, direction, sender, recipient
        FROM dialpad_sms_messages
        WHERE body IS NOT NULL AND body != ''
        ORDER BY message_at
    """).fetchall()
    results["sms"]["total"] = len(sms_rows)

    sms_thread_students = {}

    for row in sms_rows:
        mid = row["message_id"]
        tid = str(row["thread_id"])
        matched = None
        strategy = None

        # A: Thread already matched
        if tid in sms_thread_students:
            matched = sms_thread_students[tid]
            strategy = "thread_propagation"

        # B: Phone match via thread
        if not matched and tid in thread_phones:
            matched = match_phone(thread_phones[tid], phone_idx)
            if matched:
                strategy = "phone_exact"

        # C: Name in body
        if not matched:
            text = str(row["body"] or "")[:500]
            matched, score = match_name_in_text(text, name_idx, first_idx)
            if matched:
                strategy = f"name_in_sms_{score:.2f}"

        # D: Cross-channel phone (discovered from voicemail transcript)
        if not matched and tid in thread_phones:
            phone_norm = norm_phone(thread_phones[tid])
            if phone_norm and phone_norm in discovered_phones:
                matched = discovered_phones[phone_norm]
                strategy = "cross_channel_phone"

        if matched:
            results["sms"]["matched"] += 1
            results["sms"]["strategies"][strategy] += 1
            results["sms"]["student_comms"][matched["name"]].append(dict(row))
            sms_thread_students[tid] = matched

    # ─── 4. POST-PROCESSING: Shared-phone sibling propagation ───
    # For every matched phone, also match all siblings sharing that phone
    extra_matches = {"email": 0, "sms": 0, "voicemail": 0}
    
    for channel in ["email", "sms", "voicemail"]:
        student_comms = results[channel]["student_comms"]
        new_entries = defaultdict(list)
        
        for student_name, comms in list(student_comms.items()):
            # Find this student's phone(s)
            student_phones = set()
            for phone, entries in shared_phones.items():
                for entry in entries:
                    if entry["name"].lower() == student_name.lower():
                        student_phones.add(phone)
            
            if not student_phones:
                continue
            
            # For each sibling sharing this phone, give them the same comms
            for phone in student_phones:
                for entry in shared_phones.get(phone, []):
                    sibling_name = entry["name"]
                    if sibling_name.lower() != student_name.lower():
                        if sibling_name not in student_comms:
                            new_entries[sibling_name].extend(comms)
                            extra_matches[channel] += len(comms)
        
        # Merge new entries
        for name, comms in new_entries.items():
            if name not in student_comms:
                student_comms[name] = comms
            results[channel]["student_comms"] = student_comms
    
    print(f"  Shared-phone propagation: +{extra_matches['email']} email, +{extra_matches['sms']} SMS, +{extra_matches['voicemail']} VM assignments")

    return results


def main():
    print("=" * 60)
    print("Universal Comms Matcher — Multi-strategy")
    print("=" * 60)

    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row

    print("\n[1] Building indices...")
    phone_idx, email_idx, name_idx, first_idx, shared_phones, hs_added = build_indices(con)
    print(f"  {len(phone_idx)} phones ({hs_added} from HubSpot), {len(email_idx)} emails, {len(name_idx)} names, {len(shared_phones)} shared-phone groups")

    print("\n[2] Matching all communications...")
    results = match_all_comms(con)

    total_matched_students = set()
    for channel in ["email", "sms", "voicemail"]:
        r = results[channel]
        rate = r["matched"] / max(r["total"], 1) * 100
        students = len(r["student_comms"])
        total_matched_students.update(r["student_comms"].keys())
        print(f"\n  {channel.upper()}: {r['matched']}/{r['total']} ({rate:.1f}%) — {students} students")
        for strat, count in sorted(r["strategies"].items(), key=lambda x: -x[1]):
            print(f"    {strat}: {count}")

    print(f"\n  TOTAL unique students across all channels: {len(total_matched_students)}")

    # Save per-channel student-indexed comms
    out = {}
    for channel in ["email", "sms", "voicemail"]:
        r = results[channel]
        out[channel] = {
            "total": r["total"],
            "matched": r["matched"],
            "rate": r["matched"] / max(r["total"], 1) * 100,
            "students": len(r["student_comms"]),
            "strategies": dict(r["strategies"]),
            "student_comms": {k: len(v) for k, v in r["student_comms"].items()},
        }

    out_path = MODELS_DIR / "comms_match_results.json"
    json.dump(out, open(out_path, "w"), indent=2, default=str)
    print(f"\n  Saved: {out_path}")

    # Also save student-comm counts CSV for model
    csv_path = MODELS_DIR / "comms_student_counts.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["student", "email_count", "sms_count", "voicemail_count", "total_comms"])
        for student in sorted(total_matched_students):
            ec = len(results["email"]["student_comms"].get(student, []))
            sc = len(results["sms"]["student_comms"].get(student, []))
            vc = len(results["voicemail"]["student_comms"].get(student, []))
            w.writerow([student, ec, sc, vc, ec + sc + vc])

    print(f"  Saved: {csv_path}")

    con.close()


if __name__ == "__main__":
    main()
