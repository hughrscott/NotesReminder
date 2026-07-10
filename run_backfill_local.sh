#!/bin/bash
# run_backfill_local.sh — Backfill using S3 sync (now that AWS keys are fixed)
set -uo pipefail

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

DATES="2026-07-06 2026-07-07 2026-07-08 2026-07-09"

for SCHOOL in westu-sor theheights-sor; do
    echo ""
    echo "============================================"
    echo "Processing school: ${SCHOOL}"
    echo "============================================"
    for DATE in $DATES; do
        echo ""
        echo "--- ${SCHOOL} | ${DATE} ---"
        python3 run_daily.py \
            --school "$SCHOOL" \
            --start-date "$DATE" \
            --end-date "$DATE" \
            --verbose \
            --no-email \
            --to hughrscott@mac.com \
            --cc hugh.scott@gmail.com || echo "FAILED: ${SCHOOL} ${DATE}"
    done
done
echo ""
echo "Backfill complete!"