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

_CHAPTER_HEADING = re.compile(r"^(chapter|part|unit|lesson)\s+\d+\b", re.IGNORECASE)
_SECTION_HEADING = re.compile(r"^section\s+(\d+(?:\.\d+)*)\b", re.IGNORECASE)
_NUMBERED_HEADING = re.compile(r"^(\d+(?:\.\d+)*)\s+([A-Z][^\n]{2,80})$")
_TRAILING_SENTENCE_PUNCTUATION = re.compile(r"[.,;:?!]$")
_SENTENCE_BOUNDARY = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9(\"'])")
_DOCUMENT_ID_SUFFIX = re.compile(r"[-_ ]?[0-9a-f]{8,}$", re.IGNORECASE)
_PAGE_NUMBER = re.compile(
    r"(page\s*)?(\d{1,4}|m{0,3}(cm|cd|d?c{0,3})(xc|xl|l?x{0,3})(ix|iv|v?i{0,3}))",
    re.IGNORECASE,
)
