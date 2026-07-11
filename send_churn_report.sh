#!/bin/bash
# Runs churn model and emails results to SOR management.
# Called by cron at 10pm CT daily (3am UTC).
set -euo pipefail

PROJECT="/home/ubuntu/projects/hughrscott/NotesReminder"
VENV="/home/ubuntu/.hermes/env/bin/activate"
REPORT="/tmp/churn_report_$(date +%Y%m%d).txt"
RECIPIENTS="vscott@schoolofrock.com huscott@schoolofrock.com"
SUBJECT="SOR Churn Report – $(date '+%b %d, %Y')"

cd "$PROJECT"
source "$VENV"

# Run the model
python churn_model.py > "$REPORT" 2>&1

# Email to each recipient using Himalaya raw MIME format
for addr in $RECIPIENTS; do
    {
        printf 'From: huscott@schoolofrock.com\n'
        printf 'To: %s\n' "$addr"
        printf 'Subject: %s\n' "$SUBJECT"
        printf '\n'
        cat "$REPORT"
    } | himalaya message send -a sor - 2>&1 || echo "-- send to $addr failed --" >> "$REPORT"
done

# Keep last 30 reports
ls -t /tmp/churn_report_*.txt 2>/dev/null | tail -n +31 | xargs rm -f 2>/dev/null || true
