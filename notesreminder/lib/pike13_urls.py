"""
Shared Pike13 URL helpers.

Centralizes lesson-ID and note-URL construction so all tools
(noteschecker, verify_notes, backfill, cron) use the same logic.

Rule: Pike13's /desk/e/{id}/notes endpoint expects the NUMERIC
occurrence ID, not the composite DB key ("westu-sor-295349613").
"""

import re


def numeric_pike13_lesson_id(lesson_id: str) -> str:
    """Extract the numeric Pike13 occurrence ID from any form.

    >>> numeric_pike13_lesson_id("westu-sor-295349613")
    '295349613'
    >>> numeric_pike13_lesson_id("295349613")
    '295349613'

    Raises ValueError on non-numeric IDs that can't be a valid Pike13 URL.
    """
    if lesson_id.isdigit():
        return lesson_id
    m = re.search(r"(\d{6,})$", lesson_id)
    if m:
        return m.group(1)
    raise ValueError(f"Cannot extract numeric Pike13 ID from: {lesson_id}")


def pike13_note_url(school: str, lesson_id: str) -> str:
    """Build the correct Pike13 note page URL.

    >>> pike13_note_url("westu-sor", "westu-sor-295349613")
    'https://westu-sor.pike13.com/desk/e/295349613/notes'
    """
    numeric_id = numeric_pike13_lesson_id(lesson_id)
    return f"https://{school}.pike13.com/desk/e/{numeric_id}/notes"


def pike13_lesson_url(school: str, lesson_id: str) -> str:
    """Build the correct Pike13 lesson detail URL."""
    numeric_id = numeric_pike13_lesson_id(lesson_id)
    return f"https://{school}.pike13.com/e/{numeric_id}"


def normalize_lesson_id(school: str, lesson_id: str) -> str:
    """Normalize a lesson ID for use as a database key.

    Ensures the ID has the school prefix, used for deduplication
    across schools in shared tables.
    """
    if lesson_id.startswith(f"{school}-"):
        return lesson_id
    return f"{school}-{lesson_id}"
