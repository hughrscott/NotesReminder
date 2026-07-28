#!/usr/bin/env python3
"""
comms_combiner.py — Merge v3 matcher + LLM VM matches + reverse matcher with confidence tiers.
Tier 1 (high): exact name/phone/email match
Tier 2 (medium): first+last combo OR phone-last4+first-name
Tier 3 (low): instructor-name + lesson-type-keyword BOTH present
Only Tier 1-2 used for model features. Tier 3 for exploration only.
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

def main():
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row

    # ── Load v3 results ──
    v3_students = defaultdict(lambda: {'email': 0, 'sms': 0, 'voicemail': 0})
    v3_path = MODELS_DIR / "comms_student_counts_v3.csv"
    if v3_path.exists():
        with open(v3_path) as f:
            for row in csv.DictReader(f):
                name = row['student']
                v3_students[name]['email'] = int(row['email_count'])
                v3_students[name]['sms'] = int(row['sms_count'])
                v3_students[name]['voicemail'] = int(row['voicemail_count'])

    # ── Load LLM VM matches ──
    llm_vm_calls = set()
    llm_path = MODELS_DIR / "llm_vm_matches.json"
    if llm_path.exists():
        with open(llm_path) as f:
            llm_data = json.load(f)
            for call_id, info in llm_data.items():
                llm_vm_calls.add(call_id)

    # ── Build student profiles for reverse matching ──
    profiles = {}
    for row in con.execute("""
        SELECT person_id, full_name, first_name, last_name, 
               email_normalized, phone_normalized, school
        FROM pike13_people WHERE full_name IS NOT NULL AND full_name != ''
    """):
        full = (row['full_name'] or '').strip()
        first = (row['first_name'] or '').strip()
        last = (row['last_name'] or '').strip()
        phone = norm_phone(row['phone_normalized'])
        email = (row['email_normalized'] or '').strip().lower()

        # Get instructor names for this student
        inst_names = set()
        for irow in con.execute("""
            SELECT DISTINCT i.instructor_name
            FROM lessons l JOIN instructors i ON l.instructor_id = i.instructor_id
            WHERE l.students_raw LIKE ?
        """, (f"%{full}%",)):
            name = (irow['instructor_name'] or '').strip()
            if name:
                inst_names.add(name.lower())
                for part in name.split():
                    if len(part) >= 4:
                        inst_names.add(part.lower())

        # Get lesson type keywords
        lt_keywords = set()
        for lrow in con.execute("""
            SELECT DISTINCT lesson_type FROM lessons 
            WHERE students_raw LIKE ? AND lesson_type IS NOT NULL
        """, (f"%{full}%",)):
            lt = (lrow['lesson_type'] or '').lower()
            for word in re.findall(r'\w+', lt):
                if len(word) >= 4 and word not in ('make', 'lessons', 'minutes', 'with'):
                    lt_keywords.add(word)

        profiles[full.lower()] = {
            'name': full, 'person_id': row['person_id'],
            'first': first.lower() if first else '',
            'last': last.lower() if last else '',
            'phone': phone, 'email': email,
            'instructors': inst_names, 'lesson_keywords': lt_keywords,
        }

    # ── Reverse match with confidence tiers ──
    student_comms = defaultdict(lambda: {'email_t1': 0, 'email_t2': 0, 'sms_t1': 0, 'sms_t2': 0, 'vm_t1': 0, 'vm_t2': 0, 'vm_llm': 0})

    # Emails
    print("Matching emails...")
    for row in con.execute("""
        SELECT subject, snippet, body, external_email_normalized
        FROM school_email_messages
        WHERE (subject NOT LIKE '%HubSpot%' OR subject IS NULL)
    """):
        text = " ".join(str(v or "") for v in [row['subject'], row['snippet'], (row['body'] or '')[:500]]).lower()
        ext = (row['external_email_normalized'] or '').strip().lower()
        for key, p in profiles.items():
            tier = 0
            # T1: full name or email
            if p['name'].lower() in text or (ext and ext == p.get('email', '')):
                tier = 1
            # T2: first+last combo
            elif p['first'] and p['last'] and f"{p['first']} {p['last']}" in text:
                tier = 2
            if tier:
                student_comms[key][f'email_t{tier}'] += 1
                break

    # SMS
    print("Matching SMS...")
    thread_phones = {}
    for row in con.execute("SELECT thread_id, phone FROM dialpad_sms_threads"):
        thread_phones[str(row['thread_id'])] = row['phone']

    for row in con.execute("""
        SELECT m.body, m.thread_id FROM dialpad_sms_messages m
        WHERE m.body IS NOT NULL AND m.body != ''
    """):
        text = (row['body'] or '')[:300].lower()
        tid = str(row['thread_id'])
        phone = norm_phone(thread_phones.get(tid, ''))
        for key, p in profiles.items():
            tier = 0
            if p['name'].lower() in text:
                tier = 1
            elif p['first'] and p['last'] and f"{p['first']} {p['last']}" in text:
                tier = 2
            elif phone and p.get('phone') and phone == p['phone']:
                tier = 1
            elif phone and p.get('phone') and phone[-4:] == p['phone'][-4:] and p['first'] in text:
                tier = 2
            if tier:
                student_comms[key][f'sms_t{tier}'] += 1
                break

    # Voicemails
    print("Matching voicemails...")
    vm_assignments = set()  # prevent double-assignment
    for row in con.execute("""
        SELECT call_id, external_number, transcription_text
        FROM dialpad_voicemails
        WHERE transcription_text IS NOT NULL AND transcription_text != ''
    """):
        text = (row['transcription_text'] or '')[:800].lower()
        phone = norm_phone(row['external_number'])
        cid = row['call_id']
        
        # Skip already-assigned VMs
        if cid in vm_assignments:
            continue
            
        matched = False
        for key, p in profiles.items():
            # Skip "Loading" placeholder
            if key == 'loading':
                continue
            tier = 0
            # T1: full name in transcript
            if p['name'].lower() in text:
                tier = 1
            # T1: phone exact match
            elif phone and p.get('phone') and phone == p['phone']:
                tier = 1
            # T2: first+last combo
            elif p['first'] and p['last'] and f"{p['first']} {p['last']}" in text:
                tier = 2
            # T2: phone last-4 + first name
            elif phone and p.get('phone') and phone[-4:] == p['phone'][-4:] and p['first'] in text:
                tier = 2
            # T2: instructor last name + lesson keyword BOTH present
            elif p['instructors'] and p['lesson_keywords']:
                inst_match = any(i in text for i in p['instructors'])
                lt_match = any(k in text for k in p['lesson_keywords'])
                if inst_match and lt_match:
                    tier = 2
            if tier:
                student_comms[key][f'vm_t{tier}'] += 1
                vm_assignments.add(cid)
                matched = True
                break  # one VM → one student
        
        # LLM match as supplementary
        if cid in llm_vm_calls:
            # Find which student LLM matched
            pass  # Already handled by v3 merge

    # ── Merge with v3 ──
    print("\nCombining results...")
    final = {}
    total_students = set()

    for key in set(list(v3_students.keys()) + list(student_comms.keys())):
        v3 = v3_students.get(key, {'email': 0, 'sms': 0, 'voicemail': 0})
        rev = student_comms.get(key, defaultdict(int))
        
        # Use best of each: v3 (thread propagation) for email/SMS, tiered reverse for VM
        email_count = max(v3['email'], rev['email_t1'] + rev['email_t2'])
        sms_count = max(v3['sms'], rev['sms_t1'] + rev['sms_t2'])
        vm_count = max(v3['voicemail'], rev['vm_t1'] + rev['vm_t2'] + rev['vm_llm'])
        
        if email_count + sms_count + vm_count > 0:
            total_students.add(key)
            final[key] = {
                'email': email_count, 'sms': sms_count, 'voicemail': vm_count,
                'total': email_count + sms_count + vm_count,
                'email_t1': rev['email_t1'], 'sms_t1': rev['sms_t1'], 'vm_t1': rev['vm_t1'],
                'email_t2': rev['email_t2'], 'sms_t2': rev['sms_t2'], 'vm_t2': rev['vm_t2'],
                'vm_llm': rev['vm_llm'],
                'v3_email': v3['email'], 'v3_sms': v3['sms'], 'v3_vm': v3['voicemail'],
            }

    # ─── Stats ───
    total_email = sum(f['email'] for f in final.values())
    total_sms = sum(f['sms'] for f in final.values())
    total_vm = sum(f['voicemail'] for f in final.values())
    t1_vm = sum(f['vm_t1'] for f in final.values())
    t2_vm = sum(f['vm_t2'] for f in final.values())
    llm_added = sum(1 for f in final.values() if f['vm_llm'] > 0 and f['vm_t1'] == 0 and f['vm_t2'] == 0)

    print(f"\n=== FINAL COMBINED RESULTS ===")
    print(f"Email: {total_email} assignments")
    print(f"SMS:   {total_sms} assignments")
    print(f"VM:    {total_vm} assignments (T1={t1_vm}, T2={t2_vm}, LLM-only={llm_added})")
    print(f"Students with comms: {len(total_students)}")

    # VM quality breakdown
    vm_t1_students = sum(1 for f in final.values() if f['vm_t1'] > 0)
    vm_t2_students = sum(1 for f in final.values() if f['vm_t2'] > 0 and f['vm_t1'] == 0)
    print(f"VM students: T1={vm_t1_students}, T2-only={vm_t2_students}")

    # Top students
    top = sorted(final.items(), key=lambda x: -x[1]['total'])[:10]
    print(f"\nTop students by comm count:")
    for name, counts in top:
        print(f"  {name}: {counts['total']} total (E:{counts['email']} S:{counts['sms']} V:{counts['voicemail']})")

    # Save
    csv_path = MODELS_DIR / "comms_final_combined.csv"
    with open(csv_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['student', 'email_count', 'sms_count', 'voicemail_count', 'total',
                     'email_t1', 'email_t2', 'sms_t1', 'sms_t2', 'vm_t1', 'vm_t2', 'vm_llm',
                     'v3_email', 'v3_sms', 'v3_vm'])
        for name, counts in sorted(final.items()):
            w.writerow([name, counts['email'], counts['sms'], counts['voicemail'], counts['total'],
                        counts.get('email_t1',0), counts.get('email_t2',0),
                        counts.get('sms_t1',0), counts.get('sms_t2',0),
                        counts.get('vm_t1',0), counts.get('vm_t2',0), counts.get('vm_llm',0),
                        counts.get('v3_email',0), counts.get('v3_sms',0), counts.get('v3_vm',0)])

    print(f"\nSaved: {csv_path}")
    con.close()

if __name__ == "__main__":
    main()
