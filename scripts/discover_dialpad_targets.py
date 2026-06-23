#!/usr/bin/env python3
"""Compatibility wrapper for the Dialpad target discovery extractor."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from notesreminder.extractors.dialpad_discovery import *  # noqa: F401,F403
from notesreminder.extractors.dialpad_discovery import main


if __name__ == "__main__":
    main()
