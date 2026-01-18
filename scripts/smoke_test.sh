#!/bin/sh
set -euo pipefail

if [ -z "${PYTHON_BIN:-}" ]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN=python3
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN=python
  else
    echo "python not found in PATH (set PYTHON_BIN to override)." >&2
    exit 1
  fi
fi

if [ ! -f .env ]; then
  echo "Missing .env. Copy .env.example to .env and fill in credentials." >&2
  exit 1
fi

missing=0
for key in PIKE13_USER PIKE13_PASSWORD AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION SENDER_EMAIL SENDER_PASSWORD; do
  value=$(grep -E "^${key}=" .env | tail -n 1 | cut -d= -f2-)
  if [ -z "${value}" ]; then
    echo "Missing value for ${key} in .env" >&2
    missing=1
  fi
done

if [ "$missing" -ne 0 ]; then
  exit 1
fi

"$PYTHON_BIN" - <<'PY'
import importlib
modules = ["boto3", "pandas", "dotenv", "playwright.async_api"]
missing = []
for name in modules:
    try:
        importlib.import_module(name)
    except Exception as exc:
        missing.append((name, str(exc)))
if missing:
    print("Missing Python deps:")
    for name, exc in missing:
        print(f"- {name}: {exc}")
    raise SystemExit(1)
print("✅ Environment and Python deps look OK")
PY
