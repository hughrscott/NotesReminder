import re


EXTENSION_RE = re.compile(r"(?:ext\.?|extension|x)\s*\d+\s*$", re.IGNORECASE)


def normalize_phone(value):
    if not value:
        return None
    text = EXTENSION_RE.sub("", str(value).strip())
    digits = re.sub(r"\D", "", text)
    if not digits:
        return None
    if len(digits) == 11 and digits.startswith("1"):
        return digits[-10:]
    if len(digits) == 10:
        return digits
    return digits
