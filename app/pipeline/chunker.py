import hashlib
import re
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from app.constants.patterns import (
    _CHAPTER_HEADING,
    _DOCUMENT_ID_SUFFIX,
    _HEADING_PATTERNS,
    _NUMBERED_HEADING,
    _PAGE_NUMBER,
    _SECTION_HEADING,
    _SENTENCE_BOUNDARY,
    _TRAILING_SENTENCE_PUNCTUATION,
)
from app.pipeline.extractor import PageContent

_LEAF_LEVEL = 9
_MAX_SECTION_DEPTH = 4
_MAX_HEADING_WORDS = 12
_MAX_HEADING_CHARS = 120
_EDGE_LINES = 2
_RUNNING_LINE_MIN_PAGES = 3
_PATH_SEPARATOR = " › "


@dataclass
class Chunk:
    chunk_id: str
    text: str
    token_estimate: int
    metadata: dict = field(default_factory=dict)

    @property
    def contextual_text(self) -> str:
        header = self.metadata.get("context_header")
        return f"{header}\n\n{self.text}" if header else self.text


@dataclass
class _Paragraph:
    text: str
    page: int
    words: int
    is_heading: bool = False
    is_overlap: bool = False


def _word_count(text: str) -> int:
    return len(text.split())


def _estimate_tokens(text: str) -> int:
    return int(len(text.split()) * 1.3)


def _is_heading(line: str) -> bool:
    line = line.strip()

    if not line or len(line) > _MAX_HEADING_CHARS:
        return False

    for pattern in _HEADING_PATTERNS:
        if pattern.match(line):
            return True

    return False


def heading_level(line: str) -> int | None:
    line = line.strip()

    if not line or len(line) > _MAX_HEADING_CHARS or _word_count(line) > _MAX_HEADING_WORDS:
        return None

    if _CHAPTER_HEADING.match(line):
        return 1

    section = _SECTION_HEADING.match(line)
    if section:
        return section.group(1).count(".") + 2

    if not _is_heading(line) or _TRAILING_SENTENCE_PUNCTUATION.search(line):
        return None

    numbered = _NUMBERED_HEADING.match(line)
    if numbered:
        return numbered.group(1).count(".") + 2

    return _LEAF_LEVEL


def document_title(filename: str, override: str | None = None) -> str:
    if override and override.strip():
        return override.strip()

    stem = _DOCUMENT_ID_SUFFIX.sub("", Path(filename).stem)
    words = [w for w in re.split(r"[\s_-]+", stem) if w]

    return " ".join(w[:1].upper() + w[1:] if w.islower() else w for w in words)


def _normalise_edge_line(line: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"\d+", "", line)).strip().lower()


def _is_structural_heading(line: str) -> bool:
    return bool(
        _CHAPTER_HEADING.match(line)
        or _SECTION_HEADING.match(line)
        or _NUMBERED_HEADING.match(line)
    )


class DocumentChunkingSession:

    def __init__(
        self,
        filename: str,
        title: str,
        chunk_words: int,
        overlap_words: int,
        min_words: int,
    ):
        self.filename = filename
        self.title = title
        self.chunk_words = chunk_words
        self.overlap_words = overlap_words
        self.min_words = min_words

        self._stack: list[tuple[int, str]] = []
        self._buffer: list[_Paragraph] = []
        self._last_was_heading = False
        self._next_index = 0
        self._fingerprints: set[str] = set()
        self._edge_line_pages: dict[str, set[int]] = defaultdict(set)

    @property
    def chunks_emitted(self) -> int:
        return self._next_index

    def add_pages(self, pages: list[PageContent]) -> list[Chunk]:
        for page in pages:
            self._register_edge_lines(page)

        emitted: list[Chunk] = []

        for page in pages:
            for paragraph, level in self._page_blocks(page):
                if level is None:
                    emitted.extend(self._add_paragraph(paragraph))
                else:
                    emitted.extend(self._start_section(paragraph, level))

        return emitted

    def finish(self) -> list[Chunk]:
        if not any(not p.is_overlap for p in self._buffer):
            self._buffer = []
            return []

        chunk = self._emit()
        self._buffer = []
        return [chunk] if chunk else []

    def _register_edge_lines(self, page: PageContent) -> None:
        lines = [line.strip() for line in page.text.splitlines() if line.strip()]

        for line in lines[:_EDGE_LINES] + lines[-_EDGE_LINES:]:
            if _is_structural_heading(line):
                continue

            key = _normalise_edge_line(line)
            if key:
                self._edge_line_pages[key].add(page.page_count)

    def _is_page_furniture(self, line: str) -> bool:
        if _PAGE_NUMBER.fullmatch(line):
            return True

        if _is_structural_heading(line):
            return False

        key = _normalise_edge_line(line)
        return len(self._edge_line_pages.get(key, ())) >= _RUNNING_LINE_MIN_PAGES

    def _page_blocks(self, page: PageContent):
        lines = page.text.splitlines()
        content_indices = [i for i, line in enumerate(lines) if line.strip()]
        edge_indices = set(content_indices[:_EDGE_LINES] + content_indices[-_EDGE_LINES:])

        current: list[str] = []

        def paragraph_from(parts: list[str]) -> _Paragraph:
            text = "\n".join(parts)
            return _Paragraph(text=text, page=page.page_count, words=_word_count(text))

        for i, raw in enumerate(lines):
            line = raw.strip()

            if not line:
                if current:
                    yield paragraph_from(current), None
                    current = []
                continue

            if i in edge_indices and self._is_page_furniture(line):
                continue

            level = heading_level(line)

            if level is None:
                current.append(line)
                continue

            if current:
                yield paragraph_from(current), None
                current = []

            yield paragraph_from([line]), level

        if current:
            yield paragraph_from(current), None

    def _start_section(self, heading: _Paragraph, level: int) -> list[Chunk]:
        emitted: list[Chunk] = []

        new_words = sum(p.words for p in self._buffer if not p.is_overlap)
        has_overlap = any(p.is_overlap for p in self._buffer)

        if new_words == 0:
            self._buffer = []
        elif new_words >= self.min_words or has_overlap:
            chunk = self._emit()
            if chunk:
                emitted.append(chunk)
            self._buffer = []

        title = heading.text.strip()

        if self._last_was_heading and level == _LEAF_LEVEL and self._stack:
            parent_level, parent_title = self._stack[-1]
            separator = ": " if parent_level < _LEAF_LEVEL else " "
            merged = f"{parent_title}{separator}{title}"[:_MAX_HEADING_CHARS]
            self._stack[-1] = (parent_level, merged)
        else:
            while self._stack and self._stack[-1][0] >= level:
                self._stack.pop()
            self._stack.append((level, title[:_MAX_HEADING_CHARS]))
            self._stack = self._stack[-_MAX_SECTION_DEPTH:]

        heading.is_heading = True
        self._buffer.append(heading)
        self._last_was_heading = True

        return emitted

    def _add_paragraph(self, paragraph: _Paragraph) -> list[Chunk]:
        emitted: list[Chunk] = []

        pieces = (
            self._split_long(paragraph)
            if paragraph.words > self.chunk_words
            else [paragraph]
        )

        for piece in pieces:
            buffered = sum(p.words for p in self._buffer)
            has_new_content = any(not p.is_overlap for p in self._buffer)

            if buffered + piece.words > self.chunk_words and has_new_content:
                chunk = self._emit()
                if chunk:
                    emitted.append(chunk)
                self._buffer = self._overlap_tail()

            self._buffer.append(piece)

        self._last_was_heading = False
        return emitted

    def _split_long(self, paragraph: _Paragraph) -> list[_Paragraph]:
        sentences = [s for s in _SENTENCE_BOUNDARY.split(paragraph.text) if s.strip()]
        pieces: list[str] = []
        current: list[str] = []
        current_words = 0

        for sentence in sentences:
            sentence_words = _word_count(sentence)

            if sentence_words > self.chunk_words:
                if current:
                    pieces.append(" ".join(current))
                    current, current_words = [], 0

                words = sentence.split()
                for start in range(0, len(words), self.chunk_words):
                    pieces.append(" ".join(words[start : start + self.chunk_words]))
                continue

            if current_words + sentence_words > self.chunk_words and current:
                pieces.append(" ".join(current))
                current, current_words = [], 0

            current.append(sentence)
            current_words += sentence_words

        if current:
            pieces.append(" ".join(current))

        return [
            _Paragraph(text=piece, page=paragraph.page, words=_word_count(piece))
            for piece in pieces
        ]

    def _overlap_tail(self) -> list[_Paragraph]:
        last = next(
            (p for p in reversed(self._buffer) if not p.is_heading and not p.is_overlap),
            None,
        )

        if last is None or self.overlap_words <= 0:
            return []

        sentences = [s for s in _SENTENCE_BOUNDARY.split(last.text) if s.strip()]
        tail: list[str] = []
        tail_words = 0

        for sentence in reversed(sentences):
            sentence_words = _word_count(sentence)
            if tail_words + sentence_words > self.overlap_words:
                break
            tail.insert(0, sentence)
            tail_words += sentence_words

        if not tail:
            return []

        text = " ".join(tail)
        return [_Paragraph(text=text, page=last.page, words=tail_words, is_overlap=True)]

    def _emit(self) -> Chunk | None:
        text = "\n\n".join(p.text for p in self._buffer).strip()

        if not text:
            return None

        fingerprint = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if fingerprint in self._fingerprints:
            return None
        self._fingerprints.add(fingerprint)

        pages = sorted({p.page for p in self._buffer})
        section_path = [title for _, title in self._stack]
        chapter = next((title for level, title in self._stack if level < _LEAF_LEVEL), "")
        context_header = _PATH_SEPARATOR.join(
            part for part in [self.title, *section_path] if part
        )

        chunk = Chunk(
            chunk_id=str(uuid.uuid4()),
            text=text,
            token_estimate=_estimate_tokens(text),
            metadata={
                "source": self.filename,
                "filename": self.filename,
                "pages": pages,
                "page_start": pages[0],
                "page_end": pages[-1],
                "chunk_index": self._next_index,
                "doc_title": self.title,
                "section_path": section_path,
                "section": section_path[-1] if section_path else "",
                "chapter": chapter,
                "context_header": context_header,
                "word_count": _word_count(text),
                "token_estimate": _estimate_tokens(text),
            },
        )

        self._next_index += 1
        return chunk


class HierarchicalChunker:

    def __init__(self, chunk_words: int = 260, overlap_words: int = 40, min_words: int = 60):
        self.chunk_words = chunk_words
        self.overlap_words = overlap_words
        self.min_words = min_words

    def start_document(self, filename: str, title: str | None = None) -> DocumentChunkingSession:
        return DocumentChunkingSession(
            filename=filename,
            title=document_title(filename, title),
            chunk_words=self.chunk_words,
            overlap_words=self.overlap_words,
            min_words=self.min_words,
        )
