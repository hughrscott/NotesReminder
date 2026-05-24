"""Compatibility shim for Playwright recording downloads."""

from notesreminder.transcription.download_recordings_playwright import *  # noqa: F401,F403
from notesreminder.transcription.download_recordings_playwright import main


if __name__ == "__main__":
    main()
