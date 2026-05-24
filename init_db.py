"""Compatibility shim for reminders database initialization."""

from notesreminder.schema.init_db import *  # noqa: F401,F403
from notesreminder.schema.init_db import initialize_db


if __name__ == "__main__":
    initialize_db()
