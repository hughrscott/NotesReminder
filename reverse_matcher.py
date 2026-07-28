#!/usr/bin/env python3
"""
reverse_matcher.py — Start from students, search ALL comms content for their identifiers.
Uses: full name, first/last name, phone (partial), email, instructor names, lesson types.
Also matches lesson context: times, programs, trial/makeup keywords.
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


def build_student_profiles(con):
    """Build per-student profiles with all matchable identifiers."""
    profiles = {}

    # Get all pike13 people
    students = con.execute("""
        SELECT person_id, full_name, first_name, last_name, 
               email_normalized, phone_normalized, membership_state, school
        FROM pike13_people WHERE full_name IS NOT NULL AND full_name != ''
    """).fetchall()

    # Get instructor assignments per student
    student_instructors = defaultdict(set)
    for row in con.execute("""
        SELECT DISTINCT students_raw, instructor_id 
        FROM lessons WHERE students_raw IS NOT NULL AND instructor_id IS NOT NULL
    """):
        for name in re.split(r',\s*', str(row['students_raw'])):
            if name.strip():
                student_instructors[name.strip().lower()].add(row['instructor_id'])

    # Get instructor names
    instructor_names = {}
    for row in con.execute("SELECT instructor_id, instructor_name FROM instructors"):
        instructor_names[row['instructor_id']] = (row['instructor_name'] or '').strip()

    # Get lesson types per student
    student_lesson_types = defaultdict(set)
    for row in con.execute("""
        SELECT DISTINCT students_raw, lesson_type 
        FROM lessons WHERE students_raw IS NOT NULL AND lesson_type IS NOT NULL
    """):
        for name in re.split(r',\s*', str(row['students_raw'])):
            if name.strip():
                student_lesson_types[name.strip().lower()].add(row['lesson_type'])

    for s in students:
        full = (s['full_name'] or '').strip()
        first = (s['first_name'] or '').strip()
        last = (s['last_name'] or '').strip()
        phone = norm_phone(s['phone_normalized'])
        email = (s['email_normalized'] or '').strip().lower()
        school = (s['school'] or '').strip()

        key = full.lower()
        if key in profiles:
            continue

        search_terms = set()
        # Full name and variants
        if full:
            search_terms.add(full.lower())
            search_terms.add(full.lower().replace(' ', ''))
        if first:
            search_terms.add(first.lower())
        if last:
            search_terms.add(last.lower())
        if first and last:
            search_terms.add(f"{first} {last}".lower())
        # Phone partials
        if phone:
            search_terms.add(phone)
            search_terms.add(phone[-4:])  # last 4 digits
        # Email
        if email:
            search_terms.add(email)
            search_terms.add(email.split('@')[0])  # email username
        # Instructor names they've had
        for inst_id in student_instructors.get(key, set()):
            inst_name = instructor_names.get(inst_id, '')
            if inst_name:
                for part in inst_name.split():
                    if len(part) >= 3:
                        search_terms.add(part.lower())
        # Lesson type keywords
        for lt in student_lesson_types.get(key, set()):
            for word in lt.lower().split():
                if len(word) >= 3 and word not in ('the', 'and', 'for', 'make', 'one', 'up'):
                    search_terms.add(word)
        # School
        if school:
            search_terms.add(school.lower())

        profiles[key] = {
            'name': full,
            'person_id': s['person_id'],
            'first': first,
            'last': last,
            'phone': phone,
            'email': email,
            'search_terms': search_terms,
            'instructors': [instructor_names[i] for i in student_instructors.get(key, set()) if i in instructor_names],
            'lesson_types': list(student_lesson_types.get(key, set())),
        }

    return profiles


def reverse_match(con, profiles):
    """For each student, search all comms for their identifiers."""
    student_comms = defaultdict(lambda: {'email': [], 'sms': [], 'voicemail': []})
    matches = defaultdict(int)

    # Load all comms content
    emails = con.execute("""
        SELECT message_id, subject, snippet, body, external_email_normalized, thread_id
        FROM school_email_messages
        WHERE (subject NOT LIKE '%HubSpot%' OR subject IS NULL)
          AND (snippet NOT LIKE '%HubSpot%' OR snippet IS NULL)
    """).fetchall()

    sms_msgs = con.execute("""
        SELECT m.message_id, m.body, m.thread_id, t.phone
        FROM dialpad_sms_messages m
        JOIN dialpad_sms_threads t ON m.thread_id = t.thread_id
        WHERE m.body IS NOT NULL AND m.body != ''
    """).fetchall()

    voicemails = con.execute("""
        SELECT call_id, external_number, transcription_text
        FROM dialpad_voicemails
        WHERE transcription_text IS NOT NULL AND transcription_text != ''
    """).fetchall()

    print(f"  Searching: {len(emails)} emails, {len(sms_msgs)} SMS, {len(voicemails)} VMs")
    print(f"  Against {len(profiles)} student profiles")

    total_comms = len(emails) + len(sms_msgs) + len(voicemails)

    # ─── EMAILS ───
    for row in emails:
        text = " ".join(str(v or "") for v in [row['subject'], row['snippet'], (row['body'] or '')[:500]]).lower()
        ext_email = (row['external_email_normalized'] or '').strip().lower()
        for key, profile in profiles.items():
            terms = profile['search_terms']
            # Fast check: does the text contain any search term?
            if any(t in text for t in terms if len(t) >= 3):
                student_comms[key]['email'].append(dict(row))
                matches['email'] += 1
                break  # match to first student found
            # Email exact match
            elif ext_email and ext_email == profile.get('email', ''):
                student_comms[key]['email'].append(dict(row))
                matches['email'] += 1
                break

    # ─── SMS ───
    for row in sms_msgs:
        text = (row['body'] or '')[:300].lower()
        phone = norm_phone(row['phone'])
        for key, profile in profiles.items():
            terms = profile['search_terms']
            if any(t in text for t in terms if len(t) >= 3):
                student_comms[key]['sms'].append(dict(row))
                matches['sms'] += 1
                break
            elif phone and profile.get('phone') and (phone == profile['phone'] or phone[-4:] == profile['phone'][-4:]):
                student_comms[key]['sms'].append(dict(row))
                matches['sms'] += 1
                break

    # ─── VOICEMAILS ───
    for row in voicemails:
        text = (row['transcription_text'] or '')[:800].lower()
        phone = norm_phone(row['external_number'])
        for key, profile in profiles.items():
            terms = profile['search_terms']
            if any(t in text for t in terms if len(t) >= 3):
                student_comms[key]['voicemail'].append(dict(row))
                matches['voicemail'] += 1
                break
            elif phone and profile.get('phone') and (phone == profile['phone'] or phone[-4:] == profile['phone'][-4:]):
                student_comms[key]['voicemail'].append(dict(row))
                matches['voicemail'] += 1
                break

    return student_comms, matches


def main():
    print("=" * 60)
    print("Reverse Matcher — Student → Comms search")
    print("=" * 60)

    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row

    print("\n[1] Building student profiles...")
    profiles = build_student_profiles(con)
    # Show sample search terms
    for key, p in list(profiles.items())[:3]:
        print(f"  {p['name']}: {len(p['search_terms'])} terms ({', '.join(sorted(list(p['search_terms']))[:8])}...)")
    print(f"  {len(profiles)} student profiles built")

    print("\n[2] Reverse matching...")
    student_comms, match_counts = reverse_match(con, profiles)

    # Stats
    student_count = sum(1 for s, comms in student_comms.items() if any(comms.values()))
    total_email = match_counts['email']
    total_sms = match_counts['sms']
    total_vm = match_counts['voicemail']

    print(f"\n[3] Results:")
    print(f"  Email matches: {total_email}")
    print(f"  SMS matches: {total_sms}")
    print(f"  VM matches: {total_vm}")
    print(f"  Students with comms: {student_count}")

    # Save
    csv_path = MODELS_DIR / "comms_student_counts_reverse.csv"
    with open(csv_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['student', 'email_count', 'sms_count', 'voicemail_count', 'total'])
        for student in sorted(student_comms.keys()):
            ec = len(student_comms[student]['email'])
            sc = len(student_comms[student]['sms'])
            vc = len(student_comms[student]['voicemail'])
            if ec + sc + vc > 0:
                w.writerow([student, ec, sc, vc, ec + sc + vc])

    print(f"\n  Saved: {csv_path}")
    con.close()


if __name__ == "__main__":
    main()
