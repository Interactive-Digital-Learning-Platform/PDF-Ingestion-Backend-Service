import re

_HEADING_PATTERNS = [
    # "1.2.3 Title" / "Chapter 3" / "Section 4.1"
    re.compile(r"^(chapter|section|part)\s+\d[\d.]*\b", re.IGNORECASE),
    re.compile(r"^\d+(\.\d+)*\s+[A-Z][^\n]{3,60}$", re.MULTILINE),
    # ALL CAPS short line  (common textbook heading style)
    re.compile(r"^[A-Z][A-Z\s\-:]{4,50}$"),
    # Title Case short line not ending with punctuation
    re.compile(r"^([A-Z][a-z]+\s){1,6}[A-Z][a-z]+$"),
]