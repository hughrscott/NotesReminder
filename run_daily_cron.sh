#!/bin/bash
# run_daily_cron.sh — Daily Pike13 lesson notes pipeline
# Called by cron at 10pm CT (3am UTC) Mon-Sat for both schools

set -uo pipefail

# Load credentials using Python (handles special chars in keys)
eval "$(python3 << 'PYEOF'
import os, shlex
env = {}
for line in open('/home/ubuntu/.hermes/SOR/.sorenv'):
    line = line.strip()
    if not line or line.startswith('#'):
        continue
    if '=' in line:
        k, v = line.split('=', 1)
        v = v.strip().strip('"').strip("'")
        env[k] = v
for line in open(os.path.expanduser('~/.hermes/.env')):
    line = line.strip()
    if line.startswith('SOR_APP_PASSWORD='):
        env['SOR_APP_PASSWORD'] = line.split('=',1)[1].strip()
        break
for k, v in env.items():
    print(f'export {k}={shlex.quote(v)}')
PYEOF
)"

cd ~/projects/hughrscott/NotesReminder
source ~/.hermes/env/bin/activate 2>/dev/null

YESTERDAY=$(date -d yesterday +%Y-%m-%d)

echo "Daily cron run: ${YESTERDAY}"
echo "Started: $(date)"

# Recipients per school (Hugh + Vivian on both; school director added per school)
WESTU_TO="hughrscott@mac.com vivian@schoolofrock.com cabarnhill@schoolofrock.com"
HEIGHTS_TO="hughrscott@mac.com vivian@schoolofrock.com ndees@schoolofrock.com"

for SCHOOL in westu-sor theheights-sor; do
    echo ""
    echo "Processing: ${SCHOOL}"
    if [ "$SCHOOL" = "westu-sor" ]; then
        SCHOOL_TO="$WESTU_TO"
    else
        SCHOOL_TO="$HEIGHTS_TO"
    fi
    python3 run_daily.py \
        --school "$SCHOOL" \
        --start-date "$YESTERDAY" \
        --end-date "$YESTERDAY" \
        --verbose \
        --pike13-profile-dir ~/.hermes/SOR/pike13_profile \
        --to $SCHOOL_TO \
        --cc hugh.scott@gmail.com || echo "FAILED: ${SCHOOL} ${YESTERDAY}"
done

echo ""
echo "Daily cron complete: $(date)"