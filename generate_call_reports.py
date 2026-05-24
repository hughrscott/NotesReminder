"""Compatibility shim for call report generation."""

from notesreminder.reports.call_reports import *  # noqa: F401,F403
from notesreminder.reports.call_reports import main


if __name__ == "__main__":
    main()
