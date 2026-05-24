"""Compatibility shim for source completeness reporting."""

from notesreminder.reports.source_completeness import *  # noqa: F401,F403
from notesreminder.reports.source_completeness import main


if __name__ == "__main__":
    main()
