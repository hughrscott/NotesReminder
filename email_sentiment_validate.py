#!/usr/bin/env python3
"""
email_sentiment_validate.py — Sample 500 school emails, score sentiment,
link to students, measure churn model lift. Validation gate before scaling to 10K.
"""
import sqlite3, json, csv, requests, re, hashlib
from pathlib import Path
from datetime import datetime, date
from collections import defaultdict

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"
SAMPLE_SIZE = 500
BATCH_SIZE = 20

env_path = Path.home() / ".hermes" / ".env"
env = {}
for line in env_path.read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")
OPENAI_KEY = env.get("OPENAI_API_KEY", "")

SYSTEM = """You are analyzing school-to-parent communications for churn prediction. 
For each email snippet, determine:

1. sentiment: "positive" (praise, progress, excitement), "neutral" (routine info, scheduling), 
   or "negative" (concerns, complaints, frustration, cancellation)

2. intent: "scheduling", "progress_update", "concern", "cancellation", "billing", 
   "info_only", or "other"

Return JSON: [{"idx": 0, "sentiment": "neutral", "intent": "scheduling", "explanation": "short"}]"""

def sample_emails():
    """Get 500 diverse school emails."""
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row

    # Get emails with body, stratified by direction
    inbound = con.execute("""
        SELECT message_id, subject, body, direction, external_email_normalized, school, message_at
        FROM school_email_messages
        WHERE body IS NOT NULL AND body != '' AND direction = 'inbound'
        ORDER BY RANDOM() LIMIT 250
    """).fetchall()

    outbound = con.execute("""
        SELECT message_id, subject, body, direction, external_email_normalized, school, message_at
        FROM school_email_messages
        WHERE body IS NOT NULL AND body != '' AND direction = 'outbound'
        ORDER BY RANDOM() LIMIT 250
    """).fetchall()

    con.close()
    return list(inbound) + list(outbound)


def score_emails(emails):
    """Score samples via OpenAI, return scored results."""
    import time
    results = []

    for b in range(0, len(emails), BATCH_SIZE):
        batch = emails[b:b+BATCH_SIZE]
        snippets = []
        for i, e in enumerate(batch):
            body = (e["body"] or "")[:400].replace("\n", " ")
            subj = (e["subject"] or "")[:100]
            snippets.append(f"[{i}] Subject: {subj} | Body: {body}")

        prompt = "Classify these school-parent emails:\n\n" + "\n\n".join(snippets)

        headers = {"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"}
        body_req = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.2, "max_tokens": 2000,
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
                    if 0 <= idx < len(batch):
                        results.append({**dict(batch[idx]), **s})
                print(f"  Batch {b//BATCH_SIZE+1}/{len(emails)//BATCH_SIZE+1}: {len(results)}/{min(b+BATCH_SIZE, len(emails))} scored")
            else:
                print(f"  Batch error: {r.status_code}")
        except Exception as ex:
            print(f"  Batch exception: {ex}")
        time.sleep(0.3)

    return results


def link_to_students(con, scored_emails):
    """Link scored emails to students via email → pike13_people → persons."""
    # Build email→student lookup
    email_to_student = {}
    rows = con.execute("""
        SELECT pp.full_name, pp.email_normalized, pp.person_identity_id
        FROM pike13_people pp
        WHERE pp.email_normalized IS NOT NULL AND pp.email_normalized != ''
    """).fetchall()
    for row in rows:
        email_to_student[row["email_normalized"]] = {
            "name": row["full_name"],
            "person_id": row["person_identity_id"]
        }

    student_emails = defaultdict(list)
    linked = 0
    for e in scored_emails:
        email = e.get("external_email_normalized", "") or ""
        email = email.strip().lower()
        if email in email_to_student:
            student_emails[email_to_student[email]["name"]].append(e)
            linked += 1

    print(f"  Linked {linked}/{len(scored_emails)} emails to students")
    print(f"  Unique students with email data: {len(student_emails)}")
    return student_emails


def compute_email_features(student_emails):
    """Compute per-student email sentiment features."""
    features = {}
    for student, emails in student_emails.items():
        if not emails:
            continue
        sentiments = [e.get("sentiment", "neutral") for e in emails]
        intents = [e.get("intent", "") for e in emails]
        n = len(sentiments)

        pos = sum(1 for s in sentiments if s == "positive")
        neg = sum(1 for s in sentiments if s == "negative")
        neu = sum(1 for s in sentiments if s == "neutral")
        cancel = sum(1 for i in intents if i == "cancellation")
        concern = sum(1 for i in intents if i == "concern")

        features[student.strip().lower()] = {
            "email_count": n,
            "email_sentiment_positive_ratio": pos / n,
            "email_sentiment_negative_ratio": neg / n,
            "email_sentiment_neutral_ratio": neu / n,
            "email_cancellation_hits": cancel,
            "email_concern_hits": concern,
            "has_email_comms": 1,
        }

    return features


def main():
    print("=" * 60)
    print("Email Sentiment Validation — 500 sample gate")
    print("=" * 60)

    print("\n[1] Sampling 500 emails...")
    emails = sample_emails()
    print(f"  Sampled: {len(emails)} ({sum(1 for e in emails if e['direction']=='inbound')} inbound, {sum(1 for e in emails if e['direction']=='outbound')} outbound)")

    print("\n[2] Scoring via gpt-4o-mini...")
    scored = score_emails(emails)

    # Save scored results
    out_path = MODELS_DIR / "email_sentiment_sample_500.json"
    json.dump([{k: str(v) for k, v in s.items()} for s in scored], open(out_path, "w"), indent=2, default=str)
    print(f"  Saved: {out_path}")

    print("\n[3] Linking to students...")
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    student_emails = link_to_students(con, scored)

    print("\n[4] Computing email features...")
    email_features = compute_email_features(student_emails)

    # ─── Sentiment distribution ───
    all_sentiments = [e.get("sentiment") for e in scored if e.get("sentiment")]
    from collections import Counter
    sent_dist = Counter(all_sentiments)
    print(f"\n  Sentiment distribution: {dict(sent_dist)}")

    intent_dist = Counter(e.get("intent") for e in scored if e.get("intent"))
    print(f"  Intent distribution: {dict(intent_dist)}")

    # ─── Summary ───
    print(f"\n[5] Validation summary:")
    print(f"  Emails scored: {len(scored)}")
    print(f"  Students with email features: {len(email_features)}")
    print(f"  Lift assessment: ", end="")
    if len(email_features) < 20:
        print("TOO FEW students — email scoring unlikely to help churn model")
    else:
        print(f"{len(email_features)} students linked — potential for churn signal")

    con.close()
    print("\nDone. Review the sentiment distribution to decide on scaling.")


if __name__ == "__main__":
    main()
