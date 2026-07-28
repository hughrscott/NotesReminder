#!/usr/bin/env python3
"""
score_lesson_notes.py — Score unscored lesson notes with enhanced rubric.
Uses OpenAI gpt-4o-mini for quality (0-10) + engagement signal (positive/neutral/negative).
Batches 20 notes per API call to minimize cost. Saves to lesson_notes table.
"""
import sqlite3, json, time, hashlib, requests
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "reminders.db"
BATCH_SIZE = 20
MODEL = "gpt-4o-mini"
VERSION = "v2-quality-engagement"

env_path = Path.home() / ".hermes" / ".env"
env = {}
for line in env_path.read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")
OPENAI_KEY = env.get("OPENAI_API_KEY", "")

SYSTEM = """You are a music education quality assessor. Score each lesson note on two dimensions:

1. quality (0-10): How substantive and useful is this note?
   - 0-2: Empty, placeholder, or single word
   - 3-5: Basic mention of what was covered
   - 6-8: Specific skills, progress, or areas to work on
   - 9-10: Detailed, personalized, with clear next steps

2. engagement: Student's emotional/behavioral state
   - "positive": Excited, making progress, engaged, having fun
   - "neutral": Routine lesson, no clear signal
   - "negative": Struggling, frustrated, plateauing, disengaged

Return JSON array with one object per note:
[{"note_index": 0, "quality": 7, "engagement": "positive", "explanation": "Good progress on fills"}]

Be honest — don't inflate scores for thin notes. A 3-line "worked on song" is a 3-4."""

def get_unscored_notes():
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    rows = con.execute("""
        SELECT lesson_id, notes_text, note_timestamp
        FROM lesson_notes
        WHERE notes_text IS NOT NULL AND notes_text != ''
          AND note_score IS NULL
        ORDER BY lesson_id
    """).fetchall()
    con.close()
    return rows


def score_batch(batch):
    """Score a batch of notes via OpenAI."""
    notes_list = []
    for i, row in enumerate(batch):
        notes_list.append(f"[{i}] {row['notes_text'][:300]}")

    prompt = "Score these lesson notes:\n\n" + "\n\n".join(notes_list)

    headers = {
        "Authorization": f"Bearer {OPENAI_KEY}",
        "Content-Type": "application/json"
    }
    body = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 2000,
        "response_format": {"type": "json_object"}
    }

    r = requests.post("https://api.openai.com/v1/chat/completions",
                      headers=headers, json=body, timeout=120)
    if r.status_code != 200:
        print(f"  API error {r.status_code}: {r.text[:200]}")
        return None

    try:
        data = r.json()
        content = data["choices"][0]["message"]["content"]
        result = json.loads(content)
        # Handle both array and object wrappers
        if isinstance(result, dict):
            for key in result:
                if isinstance(result[key], list):
                    result = result[key]
                    break
        return result
    except Exception as e:
        print(f"  Parse error: {e}")
        print(f"  Raw: {content[:200]}")
        return None


def save_scores(con, batch, scores):
    """Save scores back to lesson_notes table."""
    now = datetime.utcnow().isoformat()
    score_map = {}
    for s in scores:
        idx = s.get("note_index", -1)
        if 0 <= idx < len(batch):
            score_map[idx] = s

    for i, row in enumerate(batch):
        s = score_map.get(i, {})
        quality = s.get("quality")
        engagement = s.get("engagement", "")
        explanation = s.get("explanation", "")
        note_hash = hashlib.md5(row["notes_text"][:200].encode()).hexdigest()[:16]

        con.execute("""
            UPDATE lesson_notes
            SET note_score = ?, note_score_explanation = ?,
                note_score_model = ?, note_score_version = ?,
                note_score_updated_at = ?, note_score_hash = ?
            WHERE lesson_id = ?
        """, (quality, f"{engagement}; {explanation}", MODEL, VERSION, now, note_hash, row["lesson_id"]))


def main():
    print("score_lesson_notes.py — Enhanced rubric scoring")
    notes = get_unscored_notes()
    total = len(notes)
    print(f"  Unscored notes with text: {total}")

    if total == 0:
        print("  Nothing to score!")
        return

    con = sqlite3.connect(str(DB_PATH))
    scored = 0
    batches = total // BATCH_SIZE + (1 if total % BATCH_SIZE else 0)

    for b in range(batches):
        start = b * BATCH_SIZE
        end = min(start + BATCH_SIZE, total)
        batch = notes[start:end]

        print(f"  Batch {b+1}/{batches}: notes {start+1}-{end}...", end=" ", flush=True)

        scores = score_batch(batch)
        if scores is None:
            print("FAILED, retrying after delay...")
            time.sleep(5)
            scores = score_batch(batch)
            if scores is None:
                print("SKIPPING batch")
                continue

        save_scores(con, batch, scores)
        con.commit()
        scored += len(batch)
        print(f"✓ ({scored}/{total})")

        if b < batches - 1:
            time.sleep(0.5)  # rate limit

    con.close()
    print(f"\n  Done! Scored {scored}/{total} notes with {MODEL} {VERSION}")


if __name__ == "__main__":
    main()
