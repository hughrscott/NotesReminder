#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

PROFILE_DIR="${PIKE13_PROFILE_DIR:-browser_profiles/pike13}"
OUTPUT="pike13_cookies.json"
ORACLE_HOST="${ORACLE_HOST:-}"

echo "=== Step 1: Extract cookies from browser profile ==="
python3 scripts/extract_pike13_cookies.py \
  --profile-dir "$PROFILE_DIR" \
  --output "$OUTPUT"

echo ""
echo "=== Step 2: Copy cookies to Oracle Cloud server ==="
if [ -n "$ORACLE_HOST" ]; then
  scp "$OUTPUT" "ubuntu@${ORACLE_HOST}:/home/ubuntu/projects/hughrscott/NotesReminder/pike13_cookies.json"
  echo "Cookies copied to $ORACLE_HOST"
  echo ""
  echo "=== Step 3: Restart MCP server to pick up new cookies ==="
  ssh "ubuntu@${ORACLE_HOST}" "sudo systemctl restart notesreminder-mcp"
  echo "MCP server restarted"
else
  echo "Set ORACLE_HOST env var to auto-copy cookies to the server."
  echo "Example: ORACLE_HOST=192.168.1.100 ./scripts/refresh_pike13_cookies.sh"
fi
