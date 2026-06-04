#!/usr/bin/env python3
"""Compatibility wrapper for the Dialpad target discovery extractor."""

from notesreminder.extractors.dialpad_discovery import *  # noqa: F401,F403
from notesreminder.extractors.dialpad_discovery import main


if __name__ == "__main__":
    main()
