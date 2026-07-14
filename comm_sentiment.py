#!/usr/bin/env python3
"""
SOR Communication Sentiment Pipeline.

Scores EVERY customer communication across all channels:
  - Voicemail transcripts (dialpad_voicemails)
  - SMS messages (dialpad_sms_messages + dialpad_sms_threads)
  - Emails (school_email_messages)
  - Call reviews (dialpad_call_reviews)

For each message: VADER sentiment (compound/pos/neg/neu) + phrase mining.
Aggregated per student → feature table for churn model.

Requires: pip install nltk (vader_lexicon bundled)
"""

import sqlite3, re, os, hashlib
import pandas as pd
import numpy as np
from collections import defaultdict

DB = "reminders.db"
MDIR = "models"

# ── Phrase categories ─────────────────────────────────────────

CHURN_PHRASES = {
    "cancel_intent": [
        "cancel", "canceling", "cancelled", "cancellation",
        "quit", "quitting", "stop", "stopping", "stopped",
        "not coming", "won't be back", "no longer", "discontinue",
        "last lesson", "last class", "final lesson", "final class",
        "done with", "taking a break", "pause", "pausing",
        "withdraw", "withdrawing", "withdrawn",
    ],
    "dissatisfaction": [
        "unhappy", "not happy", "disappointed", "frustrated", "frustrating",
        "concern", "concerned", "worried", "not satisfied",
        "complaint", "complaining", "issue", "problem", "problems",
        "not working", "doesn't work", "isn't working",
    ],
    "scheduling": [
        "reschedule", "rescheduling", "change time", "change day",
        "can't make", "cannot make", "won't be able", "conflict",
        "doesn't work for", "too busy", "schedule change",
    ],
    "financial": [
        "too expensive", "can't afford", "too much", "cost", "costs",
        "price", "pricing", "bill", "billing", "charge", "charged",
        "payment", "paying", "refund", "credit",
    ],
    "positive_engagement": [
        "love", "loves", "loving", "great", "amazing", "wonderful",
        "thank", "thanks", "appreciate", "enjoy", "enjoys", "enjoying",
        "progress", "improving", "excited", "happy",
    ],
}

# Compile all phrases as regex patterns (word-boundary aware)
PHRASE_REGEX = {}
for category, phrases in CHURN_PHRASES.items():
    patterns = []
    for phrase in phrases:
        # Escape and add word boundaries where appropriate
        escaped = re.escape(phrase)
        patterns.append(r'\b' + escaped + r'\b')
    PHRASE_REGEX[category] = re.compile('|'.join(patterns), re.IGNORECASE)


# ── Sentiment engine ──────────────────────────────────────────

def get_sentiment(text):
    """VADER sentiment analysis. Returns dict with compound/pos/neg/neu."""
    if not text or not isinstance(text, str) or len(text.strip()) < 5:
        return {"compound": 0.0, "pos": 0.0, "neg": 0.0, "neu": 0.0}

    from nltk.sentiment import SentimentIntensityAnalyzer
    sia = SentimentIntensityAnalyzer()
    scores = sia.polarity_scores(str(text).strip())
    return scores


def mine_phrases(text):
    """Scan text for churn-related phrases. Returns dict of category → count."""
    if not text or not isinstance(text, str):
        return {}
    text_lower = str(text).lower()
    hits = {}
    for category, pattern in PHRASE_REGEX.items():
        matches = pattern.findall(text_lower)
        if matches:
            hits[category] = len(matches)
    return hits


# ── Phone normalization ───────────────────────────────────────

def normalize_phone(phone):
    """Strip to digits. Handle +1 prefix, spaces, dashes, parens.
    Dialpad uses +1XXXXXXXXXX (11 digits after strip).
    Pike13 uses XXXXXXXXXX (10 digits). Normalize both to 10-digit."""
    if not phone:
        return ""
    digits = re.sub(r'\D', '', str(phone))
    # Strip leading '1' if 11 digits (US country code)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    return digits


# ── Student matching ──────────────────────────────────────────

def build_student_phone_map():
    """Build mapping: normalized_phone → list of student names from Pike13."""
    c = sqlite3.connect(DB)
    people = pd.read_sql_query("""
        SELECT full_name, phone_normalized
        FROM pike13_people
        WHERE phone_normalized IS NOT NULL AND phone_normalized != ''
    """, c)
    c.close()
    phone_map = defaultdict(list)
    for _, row in people.iterrows():
        phone = normalize_phone(row["phone_normalized"])
        if phone:
            phone_map[phone].append(row["full_name"].strip().lower())
    return phone_map


def build_student_email_map():
    """Build mapping: email_normalized → list of student names."""
    c = sqlite3.connect(DB)
    people = pd.read_sql_query("""
        SELECT full_name, email_normalized
        FROM pike13_people
        WHERE email_normalized IS NOT NULL AND email_normalized != ''
    """, c)
    c.close()
    email_map = defaultdict(list)
    for _, row in people.iterrows():
        email = str(row["email_normalized"]).strip().lower()
        if email:
            email_map[email].append(row["full_name"].strip().lower())
    return email_map


# ── Channel processors ────────────────────────────────────────

def process_voicemails(phone_map):
    """Voicemail transcripts: match by external_number → phone → student."""
    c = sqlite3.connect(DB)
    vms = pd.read_sql_query("""
        SELECT call_id, external_number, name, email, date, transcription_text
        FROM dialpad_voicemails
        WHERE transcription_text IS NOT NULL AND transcription_text != ''
    """, c)
    c.close()
    print(f"  Voicemails: {len(vms)} with transcripts")

    results = []
    matched = 0
    for _, row in vms.iterrows():
        phone = normalize_phone(row.get("external_number", ""))
        text = str(row.get("transcription_text", ""))
        if not phone or not text:
            continue

        sentiment = get_sentiment(text)
        phrases = mine_phrases(text)

        students = phone_map.get(phone, [])
        if students:
            matched += 1
        for student in (students if students else ["__unmatched__"]):
            results.append(dict(
                channel="voicemail",
                student_name=student,
                phone=phone,
                message_at=str(row.get("date", "")),
                compound=sentiment["compound"],
                pos=sentiment["pos"],
                neg=sentiment["neg"],
                neu=sentiment["neu"],
                text_length=len(text),
                cancel_hits=phrases.get("cancel_intent", 0),
                dissat_hits=phrases.get("dissatisfaction", 0),
                schedule_hits=phrases.get("scheduling", 0),
                financial_hits=phrases.get("financial", 0),
                positive_hits=phrases.get("positive_engagement", 0),
            ))
    print(f"    Matched to students: {matched}/{len(vms)}")
    return results


def process_sms(phone_map):
    """SMS messages: join threads for phone → match to students."""
    c = sqlite3.connect(DB)
    msgs = pd.read_sql_query("""
        SELECT m.message_id, m.message_at, m.direction, m.body,
               t.phone, t.contact_name, t.person_id
        FROM dialpad_sms_messages m
        JOIN dialpad_sms_threads t ON m.thread_id = t.thread_id
        WHERE m.body IS NOT NULL AND m.body != ''
          AND t.phone IS NOT NULL AND t.phone != ''
    """, c)
    c.close()
    print(f"  SMS: {len(msgs)} joined messages with phone numbers")

    results = []
    matched = 0
    for _, row in msgs.iterrows():
        phone = normalize_phone(row.get("phone", ""))
        text = str(row.get("body", ""))
        if not phone or not text:
            continue

        sentiment = get_sentiment(text)
        phrases = mine_phrases(text)

        students = phone_map.get(phone, [])
        if students:
            matched += 1
        for student in (students if students else ["__unmatched__"]):
            results.append(dict(
                channel="sms",
                student_name=student,
                phone=phone,
                message_at=str(row.get("message_at", "")),
                compound=sentiment["compound"],
                pos=sentiment["pos"],
                neg=sentiment["neg"],
                neu=sentiment["neu"],
                text_length=len(text),
                cancel_hits=phrases.get("cancel_intent", 0),
                dissat_hits=phrases.get("dissatisfaction", 0),
                schedule_hits=phrases.get("scheduling", 0),
                financial_hits=phrases.get("financial", 0),
                positive_hits=phrases.get("positive_engagement", 0),
            ))
    print(f"    Matched to students: {matched}/{len(msgs)}")
    return results


def process_emails():
    """Emails: match by person_id AND by external_email → student."""
    c = sqlite3.connect(DB)
    emails = pd.read_sql_query("""
        SELECT e.message_id, e.message_at, e.direction, e.subject, e.body,
               e.external_email_normalized, e.person_id,
               p.full_name as person_name
        FROM school_email_messages e
        LEFT JOIN pike13_people p ON e.person_id = p.person_id
        WHERE e.body IS NOT NULL AND e.body != ''
    """, c)
    c.close()
    print(f"  Emails: {len(emails)} with body text")

    email_map = build_student_email_map()

    results = []
    matched_person = 0
    matched_email = 0
    for _, row in emails.iterrows():
        text = str(row.get("body", ""))
        subject = str(row.get("subject", ""))
        full_text = subject + " " + text

        sentiment = get_sentiment(full_text)
        phrases = mine_phrases(full_text)

        # Try person_id first, then email matching
        person_name = row.get("person_name")
        external_email = str(row.get("external_email_normalized", "")).strip().lower()

        students = []
        if person_name and str(person_name) != "nan" and str(person_name) != "None":
            students = [str(person_name).strip().lower()]
            matched_person += 1
        elif external_email:
            students = email_map.get(external_email, [])
            if students:
                matched_email += 1

        for student in (students if students else ["__unmatched__"]):
            results.append(dict(
                channel="email",
                student_name=student,
                phone="",  # emails don't use phone
                message_at=str(row.get("message_at", "")),
                compound=sentiment["compound"],
                pos=sentiment["pos"],
                neg=sentiment["neg"],
                neu=sentiment["neu"],
                text_length=len(full_text),
                cancel_hits=phrases.get("cancel_intent", 0),
                dissat_hits=phrases.get("dissatisfaction", 0),
                schedule_hits=phrases.get("scheduling", 0),
                financial_hits=phrases.get("financial", 0),
                positive_hits=phrases.get("positive_engagement", 0),
            ))
    print(f"    Matched via person_id: {matched_person}, via email: {matched_email}")
    return results


def process_call_reviews(phone_map):
    """Call reviews: match via call_client_matches → pike13_people."""
    c = sqlite3.connect(DB)
    reviews = pd.read_sql_query("""
        SELECT r.call_id, r.transcript_text, r.recap_text, r.event_at,
               m.client_id, p.full_name
        FROM dialpad_call_reviews r
        LEFT JOIN call_client_matches m ON r.call_id = m.call_id
        LEFT JOIN pike13_people p ON m.client_id = p.person_id
        WHERE (r.transcript_text IS NOT NULL AND r.transcript_text != '')
           OR (r.recap_text IS NOT NULL AND r.recap_text != '')
    """, c)
    c.close()
    print(f"  Call reviews: {len(reviews)} with text")

    results = []
    matched = 0
    for _, row in reviews.iterrows():
        text = str(row.get("transcript_text", "") or "") + " " + str(row.get("recap_text", "") or "")
        if not text.strip():
            continue

        sentiment = get_sentiment(text)
        phrases = mine_phrases(text)

        person_name = row.get("full_name")
        students = []
        if person_name and str(person_name) != "nan" and str(person_name) != "None":
            students = [str(person_name).strip().lower()]
            matched += 1

        for student in (students if students else ["__unmatched__"]):
            results.append(dict(
                channel="call_review",
                student_name=student,
                phone="",
                message_at=str(row.get("event_at", "")),
                compound=sentiment["compound"],
                pos=sentiment["pos"],
                neg=sentiment["neg"],
                neu=sentiment["neu"],
                text_length=len(text),
                cancel_hits=phrases.get("cancel_intent", 0),
                dissat_hits=phrases.get("dissatisfaction", 0),
                schedule_hits=phrases.get("scheduling", 0),
                financial_hits=phrases.get("financial", 0),
                positive_hits=phrases.get("positive_engagement", 0),
            ))
    print(f"    Matched to students: {matched}")
    return results


# ── Aggregation ───────────────────────────────────────────────

def aggregate_per_student(all_results):
    """Roll up all messages into per-student features."""
    df = pd.DataFrame(all_results)

    # Only matched students
    matched = df[df["student_name"] != "__unmatched__"]

    agg = matched.groupby("student_name").agg(
        # Message counts per channel
        total_messages=("channel", "count"),
        voicemail_count=("channel", lambda x: (x == "voicemail").sum()),
        sms_count=("channel", lambda x: (x == "sms").sum()),
        email_count=("channel", lambda x: (x == "email").sum()),
        call_review_count=("channel", lambda x: (x == "call_review").sum()),

        # Sentiment aggregates
        avg_compound=("compound", "mean"),
        min_compound=("compound", "min"),
        max_compound=("compound", "max"),
        pct_negative=("neg", lambda x: (x > 0.3).mean()),  # % of messages that are notably negative

        # Phrase hit totals
        total_cancel_hits=("cancel_hits", "sum"),
        total_dissat_hits=("dissat_hits", "sum"),
        total_schedule_hits=("schedule_hits", "sum"),
        total_financial_hits=("financial_hits", "sum"),
        total_positive_hits=("positive_hits", "sum"),

        # Per-channel sentiment
        voicemail_sentiment=("compound", lambda x: x[matched["channel"] == "voicemail"].mean()),
        sms_sentiment=("compound", lambda x: x[matched["channel"] == "sms"].mean()),
        email_sentiment=("compound", lambda x: x[matched["channel"] == "email"].mean()),
    ).reset_index()

    # Fill NaN channel sentiments with 0 (no messages = neutral)
    for col in ["voicemail_sentiment", "sms_sentiment", "email_sentiment"]:
        agg[col] = agg[col].fillna(0.0)

    return agg


# ── Main ───────────────────────────────────────────────────────

def main():
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SOR Communication Sentiment Pipeline                   ║")
    print("║  VADER sentiment + phrase mining across all channels    ║")
    print("╚══════════════════════════════════════════════════════════╝\n")

    # Download VADER lexicon if needed
    try:
        import nltk
        nltk.data.find('sentiment/vader_lexicon.zip')
    except LookupError:
        print("(Downloading VADER lexicon...)")
        nltk.download('vader_lexicon', quiet=True)

    # Build matching maps
    print("[0] Building student matching maps...")
    phone_map = build_student_phone_map()
    print(f"    {len(phone_map)} unique phones → {sum(len(v) for v in phone_map.values())} student links")

    # Process each channel
    all_results = []

    print("\n[1] Voicemails...")
    all_results.extend(process_voicemails(phone_map))

    print("\n[2] SMS messages...")
    all_results.extend(process_sms(phone_map))

    print("\n[3] Emails...")
    all_results.extend(process_emails())

    print("\n[4] Call reviews...")
    all_results.extend(process_call_reviews(phone_map))

    # Aggregate
    print(f"\n[5] Aggregating {len(all_results):,} total messages...")
    agg = aggregate_per_student(all_results)

    print(f"\n  ── Coverage ──")
    print(f"  Students with communication data: {len(agg)}")
    print(f"  Total messages analyzed:          {agg['total_messages'].sum():,}")
    print(f"  Voicemails:                       {agg['voicemail_count'].sum():,}")
    print(f"  SMS:                              {agg['sms_count'].sum():,}")
    print(f"  Emails:                           {agg['email_count'].sum():,}")
    print(f"  Call reviews:                     {agg['call_review_count'].sum():,}")

    # Phrase hit summary
    print(f"\n  ── Phrase Mining Hits ──")
    for col, label in [
        ("total_cancel_hits", "Cancel/quitting"),
        ("total_dissat_hits", "Dissatisfaction"),
        ("total_schedule_hits", "Scheduling issues"),
        ("total_financial_hits", "Financial concerns"),
        ("total_positive_hits", "Positive engagement"),
    ]:
        total = int(agg[col].sum())
        students = int((agg[col] > 0).sum())
        print(f"    {label:<25s} {total:>6,} hits across {students:>4} students")

    # Sentiment distribution
    print(f"\n  ── Sentiment Distribution (avg compound per student) ──")
    for label, pct in [
        ("Strongly negative (<-0.5)", (agg["avg_compound"] < -0.5).mean()),
        ("Negative (-0.5 to -0.1)", ((agg["avg_compound"] >= -0.5) & (agg["avg_compound"] < -0.1)).mean()),
        ("Neutral (-0.1 to 0.1)", ((agg["avg_compound"] >= -0.1) & (agg["avg_compound"] <= 0.1)).mean()),
        ("Positive (0.1 to 0.5)", ((agg["avg_compound"] > 0.1) & (agg["avg_compound"] <= 0.5)).mean()),
        ("Strongly positive (>0.5)", (agg["avg_compound"] > 0.5).mean()),
    ]:
        n = int(pct * len(agg))
        print(f"    {label:<30s} {n:>4} students ({pct:.0%})")

    # Save
    os.makedirs(MDIR, exist_ok=True)
    agg.to_csv(f"{MDIR}/comm_sentiment.csv", index=False)
    pd.DataFrame(all_results).to_csv(f"{MDIR}/comm_sentiment_raw.csv", index=False)
    print(f"\n  💾 Saved → {MDIR}/comm_sentiment.csv ({len(agg)} students)")
    print(f"  💾 Raw   → {MDIR}/comm_sentiment_raw.csv ({len(all_results)} messages)")

    # Top findings
    print(f"\n{'='*70}")
    print(f"  TOP NEGATIVE STUDENTS (by avg compound)")
    print(f"{'='*70}")
    for _, r in agg.nsmallest(10, "avg_compound").iterrows():
        print(f"  {r['avg_compound']:+.3f}  {r['student_name']:<30s}  "
              f"{int(r['total_messages'])} msgs  "
              f"cancel:{int(r['total_cancel_hits'])} "
              f"dissat:{int(r['total_dissat_hits'])}")

    print(f"\n{'='*70}")
    print(f"  TOP CANCEL-INTENT STUDENTS (by cancel phrase hits)")
    print(f"{'='*70}")
    for _, r in agg.nlargest(10, "total_cancel_hits").iterrows():
        print(f"  cancel:{int(r['total_cancel_hits'])}  {r['student_name']:<30s}  "
              f"{int(r['total_messages'])} msgs  sentiment:{r['avg_compound']:+.3f}")


if __name__ == "__main__":
    main()
