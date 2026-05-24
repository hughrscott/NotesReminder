"""Compatibility shim for transcript analysis."""

from notesreminder.transcription.analyze_transcripts_openai import *  # noqa: F401,F403
from notesreminder.transcription.analyze_transcripts_openai import main


if __name__ == "__main__":
    main()
