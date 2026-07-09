#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Load environment
if [ -f ".env" ]; then
  set -a
  . ./.env
  set +a
fi

# Use venv if available
if [ -x "venv/bin/python" ]; then
  PYTHON_BIN="venv/bin/python"
else
  PYTHON_BIN="python3"
fi

exec "$PYTHON_BIN" mcp_server.py "$@"
