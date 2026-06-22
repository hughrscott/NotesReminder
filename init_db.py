"""Compatibility shim for reminders database initialization."""

from notesreminder.schema.init_db import *  # noqa: F401,F403
from notesreminder.schema.init_db import main


if __name__ == "__main__":
    main()
