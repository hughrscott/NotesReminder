"""Compatibility shim for the multi-school notes backfill entry point."""

from notesreminder.orchestration.backfill import *  # noqa: F401,F403
from notesreminder.orchestration.backfill import main


if __name__ == "__main__":
    main()
