"""Compatibility shim for Dialpad/Pike13 call-data import."""

from notesreminder.extractors.call_data import *  # noqa: F401,F403
from notesreminder.extractors.call_data import main


if __name__ == "__main__":
    main()
