#!/bin/sh
set -euo pipefail

SRC="/Users/hughscott/Documents/Coding/NotesReminder/reminders.db"
DST="/Users/hughscott/Documents/Coding/NotesReminder/reminders_mcp.db"

if [ ! -f "$SRC" ]; then
  echo "Source DB not found: $SRC" >&2
  exit 1
fi

cp "$SRC" "$DST"
echo "Copied $SRC -> $DST"
