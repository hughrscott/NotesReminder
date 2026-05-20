#!/usr/bin/env python3
"""CLI shim for notes pipeline health reporting."""

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from notesreminder.reports.notes_pipeline_health import main


if __name__ == "__main__":
    raise SystemExit(main())
