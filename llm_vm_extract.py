#!/usr/bin/env python3
"""LLM-based voicemail name extraction + fuzzy matching to students."""
import sqlite3, json, requests, time, re
from pathlib import Path
from collections import defaultdict

env = {}
for line in (Path.home() / '.hermes' / '.env').read_text().splitlines():
    if '=' in line and not line.startswith('#'):
        k, _, v = line.partition('=')
        env[k.strip()] = v.strip().strip('"').strip("'")

con = sqlite3.connect('reminders.db')
con.row_factory = sqlite3.Row

vms = con.execute("""
    SELECT call_id, external_number, transcription_text 
    FROM dialpad_voicemails 
    WHERE transcription_text IS NOT NULL AND transcription_text != ''
    ORDER BY date
""").fetchall()
print(f'Total VMs: {len(vms)}')

# Build name index
name_candidates = {}
for row in con.execute("""
    SELECT person_id, full_name, first_name, last_name 
    FROM pike13_people WHERE full_name IS NOT NULL
"""):
    full = (row['full_name'] or '').strip()
    first = (row['first_name'] or '').strip()
    last = (row['last_name'] or '').strip()
    name_candidates[full.lower()] = {
        'name': full, 'person_id': row['person_id'], 'first': first, 'last': last
    }

def fuzzy_match(query, candidates, thr=0.55):
    if not query or len(query) < 2:
        return None, 0
    qt = set(query.lower().split())
    best_s, best_m = 0, None
    for cn, cd in candidates.items():
        ct = set(cn.lower().split())
        if not ct:
            continue
        inter = qt & ct
        union = qt | ct
        s = len(inter) / len(union) if union else 0
        if query.lower() in cn.lower() or cn.lower() in query.lower():
            s = max(s, 0.85)
        first = cd.get('first', '').lower()
        last = cd.get('last', '').lower()
        if first and first in query.lower():
            s = max(s, 0.75)
        if last and last in query.lower():
            s = max(s, 0.75)
        if s > best_s:
            best_s, best_m = s, (cn, cd)
    if best_s >= thr:
        return best_m, best_s
    return None, 0

SYSTEM = (
    "Extract the STUDENT name (the person taking lessons) from voicemail transcripts. "
    "Parents often call for their kids. Look for patterns like: "
    "\"my son NAME\", \"my daughter NAME\", \"NAME's mom\", \"NAME's dad\", "
    "\"calling about NAME\", \"this is PARENT, NAME's mom\", "
    "\"I'm calling for NAME\". "
    "If the caller IS the student: \"my name is NAME, I have a lesson...\" "
    "Multiple students? Return all. Return JSON array: "
    "[{\"idx\":0,\"student_name\":\"First Last\"},{\"idx\":1,\"student_name\":null}]"
)

all_matches = {}
for b in range(0, len(vms), 15):
    batch = vms[b:b+15]
    snips = [f'[{i}] {(r["transcription_text"] or "")[:250]}' for i, r in enumerate(batch)]
    prompt = 'Extract student names:\n\n' + '\n\n'.join(snips)

    try:
        r = requests.post('https://api.openai.com/v1/chat/completions',
            headers={'Authorization': f'Bearer {env["OPENAI_API_KEY"]}', 'Content-Type': 'application/json'},
            json={'model': 'gpt-4o-mini', 'messages': [
                {'role': 'system', 'content': SYSTEM},
                {'role': 'user', 'content': prompt}
            ], 'temperature': 0.1, 'max_tokens': 1500, 'response_format': {'type': 'json_object'}},
            timeout=60)
        if r.status_code == 200:
            data = r.json()
            content = data['choices'][0]['message']['content']
            results = json.loads(content)
            if isinstance(results, dict):
                for k in results:
                    if isinstance(results[k], list):
                        results = results[k]
                        break

            for item in results:
                idx = item.get('idx', -1)
                names = item.get('student_name')
                if isinstance(idx, int) and 0 <= idx < len(batch) and names:
                    if isinstance(names, str):
                        names = [names]
                    for name in names:
                        name = str(name).lower().strip()
                        if not name or len(name) < 2:
                            continue
                        match, score = fuzzy_match(name, name_candidates)
                        if match:
                            all_matches[batch[idx]['call_id']] = {
                                'extracted': name,
                                'matched': match[0],
                                'score': score,
                                'person_id': match[1]['person_id']
                            }

            if (b // 15) % 15 == 0:
                print(f'  Batch {b//15+1}/{(len(vms)-1)//15+1}: {len(all_matches)} matches')
    except Exception as e:
        print(f'  Batch {b//15+1} error: {e}')
    time.sleep(0.3)

print(f'\nLLM extraction + matching: {len(all_matches)}/{len(vms)} VMs ({len(all_matches)/len(vms)*100:.1f}%)')

# Sample matches
print('\nSample:')
for i, (call_id, info) in enumerate(list(all_matches.items())[:15]):
    print(f"  '{info['extracted']}' -> '{info['matched']}' ({info['score']:.2f})")

# Save
with open('models/llm_vm_matches.json', 'w') as f:
    json.dump(all_matches, f, indent=2, default=str)
print(f'\nSaved {len(all_matches)} LLM-matched VMs')

con.close()
