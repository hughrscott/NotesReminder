"""
Note page classification — definitive Pike13 note-existence detection.

Replaces the old pattern-matching approach with explicit statuses:
  extracted   — note text successfully captured
  exists      — note exists but text extraction failed
  no_note     — "No notes have been created" on page
  no_note_page — Pike13 returned 404 (no note page for this event)
  auth_failed  — redirected to login during probe
  error       — network/timeout/unhandled failure

Never generates "empty" or "unknown" — those are historical artifacts.
"""

import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class NoteProbeResult:
    """Result of probing a Pike13 note page."""
    status: str          # one of the 6 definitive statuses
    notes_text: Optional[str] = None
    http_status: Optional[int] = None
    error_message: Optional[str] = None
    has_delete_button: bool = False
    has_no_notes_text: bool = False
    raw_page_text: str = ""


# Valid statuses — anything else is a bug
VALID_STATUSES = frozenset({
    "extracted", "exists", "no_note", "no_note_page",
    "auth_failed", "error",
})

# Chrome patterns to strip from extracted text
CHROME_PATTERNS = frozenset({
    "link to a website", "finish", "cancel", "no notes",
})


def classify_note_page(
    http_status: int,
    page_text: str,
    has_delete_button: bool,
    has_no_notes_text: bool,
    extracted_text: Optional[str] = None,
) -> NoteProbeResult:
    """Classify a Pike13 note page into one of the 6 definitive statuses.

    Args:
        http_status: HTTP status code from navigation
        page_text: Full page text content
        has_delete_button: True if Delete button found
        has_no_notes_text: True if "No notes have been created" found
        extracted_text: Note text extracted from richtext div (if any)

    Returns:
        NoteProbeResult with definitive status
    """
    result = NoteProbeResult(status="error")

    # HTTP-level classification
    if http_status == 404:
        result.status = "no_note_page"
        result.http_status = 404
        return result

    if http_status in (401, 403):
        result.status = "auth_failed"
        result.http_status = http_status
        return result

    if http_status != 200:
        result.status = "error"
        result.http_status = http_status
        result.error_message = f"HTTP {http_status}"
        return result

    # Page-level classification (200 OK)
    result.http_status = 200
    result.has_delete_button = has_delete_button
    result.has_no_notes_text = has_no_notes_text
    result.raw_page_text = page_text

    if has_no_notes_text:
        result.status = "no_note"
        return result

    if has_delete_button:
        if extracted_text and extracted_text.strip():
            cleaned = strip_editor_chrome(extracted_text)
            if cleaned:
                result.status = "extracted"
                result.notes_text = cleaned
                return result
        result.status = "exists"
        return result

    # Neither signal found — shouldn't happen on a valid 200 page,
    # but if it does, treat as error rather than guessing
    result.status = "error"
    result.error_message = "No definitive signal found on 200 page"
    return result


def strip_editor_chrome(text: str) -> str:
    """Remove Pike13 editor toolbar text from extracted content.

    The note editor page includes toolbar buttons ('Finish', 'Cancel',
    'Link to a website') that appear alongside real notes.  This strips
    those out, leaving only the instructor-written text.
    """
    lines = []
    for line in text.split('\n'):
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.lower() in CHROME_PATTERNS:
            continue
        lines.append(stripped)
    return '\n'.join(lines).strip()


def note_completed_for_status(status: str) -> int:
    """Map a note status to the note_completed boolean flag.

    'exists' means a note was written even if we couldn't read it —
    this counts as completed for compliance purposes.
    """
    return 1 if status in ("extracted", "exists") else 0
