"""Compatibility shim for AWS Transcribe recording processing."""

from notesreminder.transcription.aws_transcribe_recordings import *  # noqa: F401,F403
from notesreminder.transcription.aws_transcribe_recordings import main


if __name__ == "__main__":
    main()
