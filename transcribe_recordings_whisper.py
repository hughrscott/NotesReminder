"""Compatibility shim for local Whisper recording transcription."""

from notesreminder.transcription.whisper_transcribe_recordings import *  # noqa: F401,F403
from notesreminder.transcription.whisper_transcribe_recordings import main


if __name__ == "__main__":
    main()
