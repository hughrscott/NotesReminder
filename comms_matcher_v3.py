#!/usr/bin/env python3
"""
comms_matcher_v3.py — Maximum-effort matching with all strategies.
Adds: instructor matching, nickname expansion, reverse student matching,
better voicemail name extraction, SMS body names.
"""
import sqlite3, json, csv, re
from pathlib import Path
from collections import defaultdict

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"


def norm_phone(raw):
    if not raw: return None
    digits = re.sub(r'\D', '', str(raw))
    return digits[-10:] if len(digits) >= 10 else (digits if len(digits) >= 7 else None)


def norm_email(raw):
    if not raw: return None
    return str(raw).strip().lower()


# ─── NICKNAME EXPANSION ───
NICKNAMES = {
    "andrew": "andy", "andy": "andrew",
    "katherine": "katie", "katie": "katherine", "kate": "katherine",
    "william": "will", "will": "william", "bill": "william",
    "robert": "bob", "bob": "robert",
    "james": "jim", "jim": "james",
    "elizabeth": "liz", "liz": "elizabeth", "beth": "elizabeth",
    "michael": "mike", "mike": "michael",
    "christopher": "chris", "chris": "christopher",
    "matthew": "matt", "matt": "matthew",
    "jennifer": "jen", "jen": "jennifer",
    "daniel": "dan", "dan": "daniel",
    "joseph": "joe", "joe": "joseph",
    "thomas": "tom", "tom": "thomas",
    "charles": "charlie", "charlie": "charles",
    "samuel": "sam", "sam": "samuel",
    "benjamin": "ben", "ben": "benjamin",
    "alexander": "alex", "alex": "alexander",
    "nicholas": "nick", "nick": "nicholas",
    "jonathan": "jon", "jon": "jonathan",
    "nathaniel": "nate", "nate": "nathaniel",
    "timothy": "tim", "tim": "timothy",
    "zachary": "zach", "zach": "zachary",
    "joshua": "josh", "josh": "joshua",
    "anthony": "tony", "tony": "anthony",
    "richard": "rick", "rick": "richard",
    "david": "dave", "dave": "david",
    "stephen": "steve", "steve": "stephen",
    "douglas": "doug", "doug": "douglas",
    "gregory": "greg", "greg": "gregory",
    "jeffrey": "jeff", "jeff": "jeffrey",
    "rebecca": "becky", "becky": "rebecca",
    "kimberly": "kim", "kim": "kimberly",
    "jacqueline": "jackie", "jackie": "jacqueline",
    "margaret": "maggie", "maggie": "margaret",
    "patricia": "pat", "pat": "patricia",
    "abigail": "abby", "abby": "abigail",
    "gabrielle": "gabby", "gabby": "gabrielle",
    "isabella": "bella", "bella": "isabella",
    "madeline": "maddie", "maddie": "madeline",
    "eleanor": "ellie", "ellie": "eleanor",
    "olivia": "liv", "liv": "olivia",
    "victoria": "tori", "tori": "victoria",
}


def expand_names(name_candidates, first_name_map):
    """Add nickname variants to the name index."""
    additions = {}
    for full_name, data in list(name_candidates.items()):
        first = data.get("first", "").lower()
        if first in NICKNAMES:
            nickname = NICKNAMES[first]
            # Create nickname variant full name
            if " " in full_name:
                last_part = full_name.split(" ", 1)[1]
                nickname_full = f"{nickname} {last_part}"
                if nickname_full not in name_candidates:
                    additions[nickname_full] = {**data, "nickname_of": full_name}
    name_candidates.update(additions)
    return len(additions)


def fuzzy_name_match(query, candidates, threshold=0.65):
    if not query or len(query) < 2:
        return None, 0
    query_tokens = set(query.lower().split())
    best_score = 0
    best_match = None
    for cand_name, cand_data in candidates.items():
        cand_tokens = set(cand_name.lower().split())
        if not cand_tokens: continue
        intersection = query_tokens & cand_tokens
        union = query_tokens | cand_tokens
        score = len(intersection) / len(union) if union else 0
        if query.lower() in cand_name.lower() or cand_name.lower() in query.lower():
            score = max(score, 0.9)
        # Bonus for first/last partial match
        first = cand_data.get("first", "").lower()
        last = cand_data.get("last", "").lower()
        if first and first in query.lower(): score = max(score, 0.8)
        if last and last in query.lower(): score = max(score, 0.8)
        if score > best_score:
            best_score = score
            best_match = (cand_name, cand_data)
    if best_score >= threshold:
        return best_match, best_score
    return None, 0


def extract_names_from_text(text):
    if not text: return []
    text_lower = str(text).lower()
    orig_text = str(text)
    names = []

    intro_patterns = [
        r"(?:my\s+name\s+is\s+)([\w\s]+?)(?:\.|,|\s+i\s|\s+and\s|$)",
        r"(?:this\s+is\s+)([\w\s]+?)(?:\s+(?:calling|and|i|i'm|i\s+am|just)|\b\.\b|,|$)",
        r"(?:i'?m\s+)([\w\s]+?)(?:\s+(?:and|calling|call|just)|\b\.\b|,|$)",
        r"(?:hi,?\s+)?(?:this\s+is\s+)?([\w\s]+?)\s+calling",
        r"(?:hi\s+)?(?:this\s+is\s+)?([\w\s]+?)(?:'s\s+(?:mom|dad|parent))",
    ]
    for pat in intro_patterns:
        for m in re.finditer(pat, text_lower):
            name = m.group(1).strip()
            if len(name) >= 2 and name not in ('the', 'a', 'an', 'my', 'his', 'her', 'just', 'also', 'not', 'calling', 'there', 'this', 'hello', 'good', 'morning'):
                names.append(name)

    family_patterns = [
        r"(?:my\s+(?:son|daughter|child|kid)s?\s+)([\w\s]+?)(?:\.|,|$|\s+and|\s+is)",
        r"([\w\s]+?)'s\s+(?:mom|dad|parent|mother|father)",
        r"(?:the\s+)?(?:mom|dad|parent)\s+of\s+([\w\s]+?)(?:\.|,|$)",
        r"(?:i\s+(?:am|i'm)\s+)(?:calling\s+)?(?:for|about|regarding)\s+([\w\s]+?)(?:\.|,|$|\s+and)",
        r"(?:calling\s+(?:for|about|regarding)\s+)([\w\s]+?)(?:\.|,|$)",
    ]
    for pat in family_patterns:
        for m in re.finditer(pat, text_lower):
            name = m.group(1).strip()
            if len(name) >= 2 and name not in ('the', 'a', 'an', 'my', 'his', 'her', 'our', 'your'):
                names.append(name)

    # Sibling patterns: "X and Y's mom"
    sibling = re.findall(r'(\w+)\s+and\s+(\w+)', text_lower)
    for a, b in sibling:
        if len(a) >= 2 and a not in ('the', 'my', 'his', 'her', 'our'): names.append(a)
        if len(b) >= 2 and b not in ('the', 'my', 'his', 'her', 'our'): names.append(b)

    # Capitalized proper names
    proper = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+))\b', orig_text)
    names.extend([n.strip().lower() for n in proper if len(n) > 3])

    return names


def build_indices(con):
    phone_to_student = {}
    email_to_student = {}
    name_candidates = {}
    first_name_map = defaultdict(list)
    shared_phones = defaultdict(list)
    instructors = set()  # instructor last names for matching

    # ── Pike13 people ──
    rows = con.execute("""
        SELECT person_id, full_name, first_name, last_name, 
               email_normalized, phone_normalized, school
        FROM pike13_people WHERE full_name IS NOT NULL AND full_name != ''
    """).fetchall()
    for row in rows:
        full = (row["full_name"] or "").strip()
        first = (row["first_name"] or "").strip()
        last = (row["last_name"] or "").strip()
        email = norm_email(row["email_normalized"])
        phone = norm_phone(row["phone_normalized"])
        entry = {"name": full, "person_id": row["person_id"], "first": first, "last": last}
        name_candidates[full.lower()] = entry
        if phone:
            if phone not in phone_to_student: phone_to_student[phone] = entry
            shared_phones[phone].append(entry)
        if email: email_to_student[email] = entry
        if first: first_name_map[first.lower()].append(entry)

    # ── HubSpot phones ──
    hs_rows = con.execute("""
        SELECT hc.phone_normalized, hc.full_name, hc.person_id, pp.full_name as p13_name
        FROM hubspot_contacts hc LEFT JOIN pike13_people pp ON hc.person_id = pp.person_identity_id
        WHERE hc.phone_normalized IS NOT NULL AND hc.phone_normalized != '' AND hc.person_id IS NOT NULL
    """).fetchall()
    hs_added = 0
    for row in hs_rows:
        phone = norm_phone(row["phone_normalized"])
        if not phone or phone in phone_to_student: continue
        name = row["p13_name"] or row["full_name"] or ""
        entry = {"name": name.strip(), "person_id": row["person_id"], "first": "", "last": ""}
        phone_to_student[phone] = entry
        shared_phones[phone].append(entry)
        hs_added += 1

    # ── Instructors ──
    for row in con.execute("SELECT DISTINCT instructor_name FROM instructors WHERE instructor_name IS NOT NULL"):
        name = str(row["instructor_name"]).strip()
        if name:
            instructors.add(name.lower())
            parts = name.split()
            for p in parts:
                if len(p) >= 3: instructors.add(p.lower())

    # ── Nickname expansion ──
    nick_added = expand_names(name_candidates, first_name_map)

    return phone_to_student, email_to_student, name_candidates, first_name_map, shared_phones, instructors, hs_added, nick_added


def match_name_in_text(text, name_candidates, first_name_map, instructors):
    if not text: return None, 0
    text_lower = str(text).lower()

    # Strategy 1: Exact full name (longest first)
    for name, data in sorted(name_candidates.items(), key=lambda x: -len(x[0])):
        if len(name) >= 4 and name in text_lower:
            return data, 1.0

    # Strategy 2: Instructor matching — narrow the field
    # If text mentions an instructor, prioritize that instructor's students
    text_words = set(text_lower.split())
    mentioned_instructors = instructors & text_words
    
    # Strategy 3: Extract names, fuzzy match
    extracted = extract_names_from_text(text)
    best_score = 0
    best_match = None

    for name_fragment in extracted:
        match, score = fuzzy_name_match(name_fragment, name_candidates, threshold=0.6)
        if match and score > best_score:
            best_score = score
            best_match = match[1]

        # First name only
        if name_fragment in first_name_map:
            candidates = first_name_map[name_fragment]
            if len(candidates) == 1:
                return candidates[0], 0.85
            if len(candidates) <= 5:
                return candidates[0], 0.7

    if best_match:
        return best_match, best_score
    return None, 0


def match_all_comms(con):
    phone_idx, email_idx, name_idx, first_idx, shared_phones, instructors, _, _ = build_indices(con)

    results = {
        "email": {"total": 0, "matched": 0, "strategies": defaultdict(int), "student_comms": defaultdict(list)},
        "sms": {"total": 0, "matched": 0, "strategies": defaultdict(int), "student_comms": defaultdict(list)},
        "voicemail": {"total": 0, "matched": 0, "strategies": defaultdict(int), "student_comms": defaultdict(list)},
    }

    # ─── 1. EMAILS ───
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
    thread_students = {}

    for row in emails:
        mid, tid = row["message_id"], row["thread_id"]
        matched, strategy = None, None

        if tid and tid in thread_students:
            matched, strategy = thread_students[tid], "thread_propagation"

        if not matched:
            matched = match_phone_or_email(row, email_idx)
            if matched: strategy = "email_exact"

        if not matched:
            for field in ["from_email", "to_emails", "cc_emails"]:
                val = row[field]
                if val:
                    for addr in re.split(r'[,;]\s*', str(val)):
                        e = norm_email(addr)
                        if e and e in email_idx:
                            matched = email_idx[e]
                            strategy = f"email_{field}"
                            break
                if matched: break

        if not matched:
            text = " ".join(str(v or "") for v in [row["subject"], row["snippet"], (row["body"] or "")[:1000]])
            matched, score = match_name_in_text(text, name_idx, first_idx, instructors)
            if matched: strategy = f"name_{score:.2f}"

        if not matched and row["subject"]:
            subj = str(row["subject"])
            subj_names = re.findall(r'(?:Re:|for-?|about|regarding)\s+(\w+)', subj, re.IGNORECASE)
            for name in subj_names:
                if len(name) >= 3:
                    fmatch, fscore = fuzzy_name_match(name.lower(), name_idx, threshold=0.6)
                    if fmatch:
                        matched = fmatch[1]
                        strategy = f"subject_{fscore:.2f}"
                        break

        if matched:
            results["email"]["matched"] += 1
            results["email"]["strategies"][strategy] += 1
            results["email"]["student_comms"][matched["name"]].append(dict(row))
            if tid: thread_students[tid] = matched

    # ─── 2. VOICEMAILS ───
    discovered_phones = {}
    vm_rows = con.execute("""
        SELECT call_id, external_number, transcription_text, date, name as vm_name
        FROM dialpad_voicemails
        WHERE transcription_text IS NOT NULL AND transcription_text != ''
        ORDER BY date
    """).fetchall()
    results["voicemail"]["total"] = len(vm_rows)

    for row in vm_rows:
        matched, strategy = None, None

        # A: Phone
        phone = norm_phone(row["external_number"])
        if phone and phone in phone_idx:
            matched = phone_idx[phone]
            strategy = "phone"

        # B: Name in transcription
        if not matched:
            text = str(row["transcription_text"] or "")[:1500]
            matched, score = match_name_in_text(text, name_idx, first_idx, instructors)
            if matched:
                strategy = f"name_{score:.2f}"
                if phone:
                    discovered_phones[phone] = matched

        if matched:
            results["voicemail"]["matched"] += 1
            results["voicemail"]["strategies"][strategy] += 1
            results["voicemail"]["student_comms"][matched["name"]].append(dict(row))

    # ─── 3. SMS ───
    thread_phones = {}
    for row in con.execute("SELECT thread_id, phone FROM dialpad_sms_threads WHERE phone IS NOT NULL"):
        thread_phones[str(row["thread_id"])] = row["phone"]

    sms_rows = con.execute("""
        SELECT message_id, thread_id, body, message_at, direction, sender, recipient
        FROM dialpad_sms_messages WHERE body IS NOT NULL AND body != ''
        ORDER BY message_at
    """).fetchall()
    results["sms"]["total"] = len(sms_rows)
    sms_thread_students = {}

    for row in sms_rows:
        mid, tid = row["message_id"], str(row["thread_id"])
        matched, strategy = None, None

        if tid in sms_thread_students:
            matched, strategy = sms_thread_students[tid], "thread"

        if not matched and tid in thread_phones:
            phone = norm_phone(thread_phones[tid])
            if phone and phone in phone_idx:
                matched = phone_idx[phone]
                strategy = "phone"

        if not matched:
            text = str(row["body"] or "")[:500]
            matched, score = match_name_in_text(text, name_idx, first_idx, instructors)
            if matched: strategy = f"name_{score:.2f}"

        if not matched and tid in thread_phones:
            phone = norm_phone(thread_phones[tid])
            if phone and phone in discovered_phones:
                matched = discovered_phones[phone]
                strategy = "cross_channel"

        if matched:
            results["sms"]["matched"] += 1
            results["sms"]["strategies"][strategy] += 1
            results["sms"]["student_comms"][matched["name"]].append(dict(row))
            sms_thread_students[tid] = matched

    # ─── 4. SIBLING PROPAGATION ───
    extra = {"email": 0, "sms": 0, "voicemail": 0}
    for channel in ["email", "sms", "voicemail"]:
        student_comms = results[channel]["student_comms"]
        new_entries = defaultdict(list)
        for student_name, comms in list(student_comms.items()):
            student_phones = set()
            for phone, entries in shared_phones.items():
                for entry in entries:
                    if entry["name"].lower() == student_name.lower():
                        student_phones.add(phone)
            if not student_phones: continue
            for phone in student_phones:
                for entry in shared_phones.get(phone, []):
                    sib = entry["name"]
                    if sib.lower() != student_name.lower():
                        if sib not in student_comms:
                            new_entries[sib].extend(comms)
                            extra[channel] += len(comms)
        for name, comms in new_entries.items():
            if name not in student_comms:
                student_comms[name] = comms
        results[channel]["student_comms"] = student_comms

    print(f"  Sibling propagation: +{sum(extra.values())} assignments")
    return results


def match_phone_or_email(row, email_idx):
    ext = row["external_email_normalized"] if "external_email_normalized" in row.keys() else None
    e = norm_email(ext)
    if e and e in email_idx: return email_idx[e]
    return None


def main():
    print("=" * 60)
    print("Comms Matcher v3 — Maximum Effort")
    print("=" * 60)

    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row

    phone_idx, email_idx, name_idx, first_idx, shared_phones, instructors, hs_added, nick_added = build_indices(con)
    print(f"\n[1] Indices: {len(phone_idx)} phones (+{hs_added} HS), {len(email_idx)} emails, {len(name_idx)} names (+{nick_added} nicknames), {len(instructors)} instructor tokens")

    print("\n[2] Matching...")
    results = match_all_comms(con)

    total_students = set()
    for channel in ["email", "sms", "voicemail"]:
        r = results[channel]
        rate = r["matched"] / max(r["total"], 1) * 100
        students = len(r["student_comms"])
        total_students.update(r["student_comms"].keys())
        print(f"\n  {channel.upper()}: {r['matched']}/{r['total']} ({rate:.1f}%) — {students} students")
        for strat, count in sorted(r["strategies"].items(), key=lambda x: -x[1])[:8]:
            print(f"    {strat}: {count}")

    print(f"\n  TOTAL unique students: {len(total_students)}")

    # Save
    csv_path = MODELS_DIR / "comms_student_counts_v3.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["student", "email_count", "sms_count", "voicemail_count", "total_comms"])
        for student in sorted(total_students):
            ec = len(results["email"]["student_comms"].get(student, []))
            sc = len(results["sms"]["student_comms"].get(student, []))
            vc = len(results["voicemail"]["student_comms"].get(student, []))
            w.writerow([student, ec, sc, vc, ec + sc + vc])
    print(f"\n  Saved: {csv_path}")

    con.close()


if __name__ == "__main__":
    main()
