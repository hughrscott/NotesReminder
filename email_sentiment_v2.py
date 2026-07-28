#!/usr/bin/env python3
"""
email_sentiment_v2.py — Smart student matching for school emails.
Three strategies: (1) email exact match, (2) name in subject/body/snippet,
(3) thread propagation. Then scores sentiment and computes per-student features.
"""
import sqlite3, json, csv, requests, re, time
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"

env_path = Path.home() / ".hermes" / ".env"
env = {}
for line in env_path.read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")
OPENAI_KEY = env.get("OPENAI_API_KEY", "")


def build_name_index(con):
    """Build lookup: known student name -> person_id, and email->student."""
    name_to_student = {}
    email_to_student = {}

    rows = con.execute("""
        SELECT person_id, full_name, first_name, last_name, email_normalized, phone_normalized, school
        FROM pike13_people
        WHERE full_name IS NOT NULL AND full_name != ''
    """).fetchall()

    for row in rows:
        full = (row["full_name"] or "").strip()
        first = (row["first_name"] or "").strip()
        last = (row["last_name"] or "").strip()
        email = (row["email_normalized"] or "").strip().lower()

        # Full name
        if full:
            name_to_student[full.lower()] = {"name": full, "person_id": row["person_id"]}

        # First + Last combo
        if first and last:
            combo = f"{first} {last}".lower()
            if combo not in name_to_student:
                name_to_student[combo] = {"name": full, "person_id": row["person_id"]}

        # Email
        if email:
            email_to_student[email] = {"name": full, "person_id": row["person_id"]}

    return name_to_student, email_to_student


def match_emails_to_students(con, name_index, email_index):
    """Match all school emails to students using multi-strategy approach."""
    rows = con.execute("""
        SELECT message_id, thread_id, subject, snippet, body,
               external_email_normalized, direction, school, message_at
        FROM school_email_messages
        WHERE body IS NOT NULL AND body != ''
        ORDER BY message_at
    """).fetchall()

    # Strategy 1: email exact match
    email_matched = {}
    for row in rows:
        ext = (row["external_email_normalized"] or "").strip().lower()
        if ext in email_index:
            email_matched[row["message_id"]] = {
                "strategy": "email_exact",
                "student": email_index[ext]["name"],
                "person_id": email_index[ext]["person_id"],
            }

    # Strategy 2: name in subject/snippet/body
    # Build a regex-friendly name list: sort by length descending to match "Ved Agrawal" before "Ved"
    names_by_len = sorted(name_index.keys(), key=len, reverse=True)
    name_pattern = re.compile(
        r'\b(' + '|'.join(re.escape(n) for n in names_by_len if len(n) >= 5) + r')\b',
        re.IGNORECASE
    )

    name_matched = {}
    for row in rows:
        if row["message_id"] in email_matched:
            name_matched[row["message_id"]] = email_matched[row["message_id"]]
            continue

        text = " ".join(str(v or "") for v in [row["subject"], row["snippet"], (row["body"] or "")[:500]])
        found = name_pattern.findall(text.lower())
        if found:
            # Take longest match
            best = max(found, key=len)
            student = name_index.get(best)
            if student:
                name_matched[row["message_id"]] = {
                    "strategy": "name_in_content",
                    "student": student["name"],
                    "person_id": student["person_id"],
                }

    # Strategy 3: thread propagation
    # Build thread->message mapping
    thread_msgs = defaultdict(list)
    for row in rows:
        if row["thread_id"]:
            thread_msgs[row["thread_id"]].append(row["message_id"])

    # If any message in thread is matched, match all in that thread
    final = dict(name_matched)
    for thread_id, msg_ids in thread_msgs.items():
        matched_students = set()
        for mid in msg_ids:
            if mid in final:
                matched_students.add((final[mid]["student"], final[mid]["person_id"]))
        if len(matched_students) == 1:
            student_name, person_id = list(matched_students)[0]
            for mid in msg_ids:
                if mid not in final:
                    final[mid] = {
                        "strategy": "thread_propagation",
                        "student": student_name,
                        "person_id": person_id,
                    }

    print(f"  Email exact: {sum(1 for v in final.values() if v['strategy']=='email_exact')}")
    print(f"  Name in content: {sum(1 for v in final.values() if v['strategy']=='name_in_content')}")
    print(f"  Thread propagation: {sum(1 for v in final.values() if v['strategy']=='thread_propagation')}")
    print(f"  Total matched: {len(final)}/{len(rows)} emails")
    print(f"  Unique students: {len(set(v['student'] for v in final.values()))}")

    # Build student->emails index
    student_emails = defaultdict(list)
    for row in rows:
        mid = row["message_id"]
        if mid in final:
            student_emails[final[mid]["student"]].append(dict(row))

    print(f"  Students with emails: {len(student_emails)}")
    return final, student_emails


def score_emails(emails, batch_size=20):
    """Score a batch of emails for sentiment + intent."""
    SYSTEM = """You are analyzing school-to-parent communications for churn signals.
For each email snippet, return:
- sentiment: "positive" (praise, progress, excitement), "neutral" (routine info),
  or "negative" (concern, complaint, cancellation, frustration)
- intent: "scheduling", "progress_update", "concern", "cancellation", "billing", 
  "info_only", or "other"
Return JSON array: [{"idx": 0, "sentiment": "neutral", "intent": "info_only"}]"""

    results = []
    for b in range(0, len(emails), batch_size):
        batch = emails[b:b+batch_size]
        snippets = []
        for i, e in enumerate(batch):
            body = (e.get("body") or "")[:300].replace("\n", " ")
            subj = (e.get("subject") or "")[:80]
            snippets.append(f"[{i}] {subj} | {body}")

        prompt = "Classify:\n\n" + "\n\n".join(snippets)

        headers = {"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"}
        body_req = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.2, "max_tokens": 1500,
            "response_format": {"type": "json_object"}
        }

        try:
            r = requests.post("https://api.openai.com/v1/chat/completions",
                            headers=headers, json=body_req, timeout=120)
            if r.status_code == 200:
                data = r.json()
                content = data["choices"][0]["message"]["content"]
                scores = json.loads(content)
                if isinstance(scores, dict):
                    for k in scores:
                        if isinstance(scores[k], list):
                            scores = scores[k]
                            break
                for s in scores:
                    idx = s.get("idx", -1)
                    if isinstance(idx, int) and 0 <= idx < len(batch):
                        results.append({**batch[idx], "sentiment": s.get("sentiment", "neutral"),
                                       "intent": s.get("intent", "info_only")})
            else:
                print(f"    Batch error: {r.status_code}")
        except Exception as ex:
            print(f"    Batch error: {ex}")
        time.sleep(0.3)

    return results


def main():
    print("=" * 60)
    print("Email Sentiment v2 — Smart multi-strategy matching")
    print("=" * 60)

    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row

    print("\n[1] Building name index...")
    name_index, email_index = build_name_index(con)
    print(f"  {len(name_index)} names, {len(email_index)} emails indexed")

    print("\n[2] Matching emails to students...")
    matches, student_emails = match_emails_to_students(con, name_index, email_index)

    # Score a sample: all emails for first 50 students
    print("\n[3] Scoring sample emails...")
    sample_students = sorted(student_emails.keys(), key=lambda s: len(student_emails[s]), reverse=True)[:50]
    to_score = []
    for s in sample_students:
        to_score.extend(student_emails[s][:10])  # max 10 per student

    print(f"  Scoring {len(to_score)} emails for {len(sample_students)} top students...")
    scored = score_emails(to_score)

    # Compute per-student features
    student_features = defaultdict(lambda: {"emails": [], "sentiments": [], "intents": []})
    for e in scored:
        s = matches[e["message_id"]]["student"]
        student_features[s]["emails"].append(e)
        student_features[s]["sentiments"].append(e.get("sentiment", "neutral"))
        student_features[s]["intents"].append(e.get("intent", "info_only"))

    print("\n[4] Summary:")
    all_s = [e.get("sentiment") for e in scored]
    all_i = [e.get("intent") for e in scored]
    print(f"  Sentiments: {dict(Counter(all_s))}")
    print(f"  Intents: {dict(Counter(all_i))}")

    # Per-student features
    rows = []
    for student, data in student_features.items():
        sents = data["sentiments"]
        ints = data["intents"]
        n = len(sents)
        rows.append({
            "student": student,
            "email_count": n,
            "email_pos_ratio": sum(1 for s in sents if s == "positive") / n,
            "email_neg_ratio": sum(1 for s in sents if s == "negative") / n,
            "email_cancel_count": sum(1 for i in ints if i == "cancellation"),
            "email_concern_count": sum(1 for i in ints if i == "concern"),
            "email_progress_count": sum(1 for i in ints if i == "progress_update"),
        })

    out_path = MODELS_DIR / "email_sentiment_features_v2.csv"
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["student", "email_count", "email_pos_ratio",
                                           "email_neg_ratio", "email_cancel_count",
                                           "email_concern_count", "email_progress_count"])
        w.writeheader()
        w.writerows(rows)

    print(f"\n  Saved features: {out_path} ({len(rows)} students)")
    total_emails = con.execute("SELECT COUNT(*) FROM school_email_messages WHERE body IS NOT NULL AND body != ''").fetchone()[0]
    match_rate = len(matches) / max(total_emails, 1) * 100
    print(f"\n  TOTAL match rate: {len(matches)}/{total_emails} emails ({match_rate:.1f}%)")

    con.close()


if __name__ == "__main__":
    main()
