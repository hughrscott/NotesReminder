"""Compatibility shim for reporting schema backfill."""

from notesreminder.schema.reporting import *  # noqa: F401,F403
from notesreminder.schema.reporting import main


if __name__ == "__main__":
    main()
