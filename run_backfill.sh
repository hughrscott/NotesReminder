#!/bin/bash
# run_backfill.sh — Backfill Pike13 lesson notes from last_date to yesterday
# Excludes Sundays. Runs run_daily.py for each non-Sunday day.
#
# Usage: run_backfill.sh [START_DATE] [END_DATE]
# If dates not provided, defaults to last note date in DB → yesterday

set -uo pipefail

# Load credentials via helper script
eval "$(python3 scripts/load_creds.py)"

cd ~/projects/hughrscott/NotesReminder
source ~/.hermes/env/bin/activate 2>/dev/null

# Get dates
START_DATE="${1:-}"
END_DATE="${2:-$(date -d yesterday +%Y-%m-%d)}"

if [ -z "$START_DATE" ]; then
    python3 -c "
import boto3, sqlite3, os
s3 = boto3.client('s3')
s3.download_file('notesreminder-db', 'reminders.db', '/tmp/reminders_last.db')
conn = sqlite3.connect('/tmp/reminders_last.db')
result = conn.execute('SELECT MAX(l.lesson_date) FROM lessons l JOIN lesson_notes ln ON l.lesson_id = ln.lesson_id WHERE ln.notes_text IS NOT NULL AND TRIM(ln.notes_text) != \"\"').fetchone()
print(result[0])
conn.close()
os.remove('/tmp/reminders_last.db')
" > /tmp/last_note_date.txt
    START_DATE=$(cat /tmp/last_note_date.txt)
fi

echo "Backfill: ${START_DATE} to ${END_DATE}"
echo "Skipping Sundays"

# Generate list of non-Sunday dates
python3 -c "
from datetime import datetime, timedelta
start = datetime.strptime('${START_DATE}', '%Y-%m-%d')
end = datetime.strptime('${END_DATE}', '%Y-%m-%d')
current = start
while current <= end:
    if current.weekday() != 6:  # 6 = Sunday
        print(current.strftime('%Y-%m-%d'))
    current += timedelta(days=1)
" > /tmp/backfill_dates.txt

DATES=$(cat /tmp/backfill_dates.txt)
echo "Dates to process: $(echo "$DATES" | wc -l) days"

# Run for both schools
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