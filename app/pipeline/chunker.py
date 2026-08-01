import re
import uuid
from dataclasses import dataclass, field
from typing import List

from app.constants.patterns import _HEADING_PATTERNS
from app.pipeline.extractor import PageContent


@dataclass
class Chunk:
    chunk_id: str
    text: str
    token_estimate: int
    metadata: dict = field(default_factory=dict)


def _is_heading(line: str) -> bool:
    line = line.strip()

    if not line or len(line) > 120:
        return False

    for pattern in _HEADING_PATTERNS:
        if pattern.match(line):
            return True

    return False


def _estimate_tokens(text: str) -> int:
    return int(len(text.split()) * 1.3)


class HierarchicalChunker:

    def __init__(
        self, chunk_size: int = 512, chunk_overlap: int = 64, min_chunk_size: int = 80
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_size = min_chunk_size

    def chunk_pages(self, pages: list[PageContent]) -> list[Chunk]:

        if not pages:
            return []

        doc_text, page_map = self._merge_pages(pages)
        sections = self._split_into_sections(doc_text)

        raw_chunks: list[str] = []
        for section in sections:
            raw_chunks.extend(self._chunk_section(section))

        raw_chunks = self._merge_small_chunks(raw_chunks)

        source_meta = pages[0].metadata
        chunks = self._build_chunks(raw_chunks, page_map, source_meta)

        return chunks

    def _merge_pages(self, pages: list[PageContent]):
        parts = []
        page_map = []
        cursor = 0

        for page in pages:
            start = cursor
            parts.append(page.text)
            cursor += len(page.text)
            page_map.append((start, cursor, page.page_count))

            parts.append("\n\n")
            cursor += 2

        return "".join(parts), page_map

    def _split_into_sections(self, doc_text: str) -> List[str]:

        lines = doc_text.splitlines(keepends=True)
        sections: List[str] = []
        current: List[str] = []

        for line in lines:
            if _is_heading(line.rstrip()) and current:
                section_text = "".join(current).strip()
                if section_text:
                    sections.append(section_text)
                current = [line]
            else:
                current.append(line)

        if current:
            section_text = "".join(current).strip()

            if section_text:
                sections.append(section_text)

        return sections if sections else [doc_text]

    def _chunk_section(self, section: str) -> List[str]:

        if _estimate_tokens(section) <= self.chunk_size:
            return [section]

        paragraphs = re.split(r"\n{2,}", section)
        paragraphs = [p.strip() for p in paragraphs if p.strip()]

        chunks: List[str] = []
        current_parts: List[str] = []
        current_tokens = 0

        for para in paragraphs:
            para_tokens = _estimate_tokens(para)

            if para_tokens > self.chunk_size:
                if current_parts:
                    chunks.append("\n\n".join(current_parts))
                    current_parts = []
                    current_tokens = 0

                chunks.extend(self._sliding_window(para))
                continue

            if current_tokens + para_tokens > self.chunk_size and current_parts:
                chunks.append("\n\n".join(current_parts))

                overlap_para = current_parts[-1] if current_parts else ""
                current_parts = [overlap_para] if overlap_para else []
                current_tokens = _estimate_tokens(overlap_para)

            current_parts.append(para)
            current_tokens += para_tokens

        if current_parts:
            chunks.append("\n\n".join(current_parts))

        return chunks

    def _sliding_window(self, text: str) -> List[str]:
        words = text.split()

        step = max(1, self.chunk_size - self.chunk_overlap)

        step_w = int(step / 1.3)
        size_w = int(self.chunk_size / 1.3)

        chunks = []
        for i in range(0, len(words), step_w):
            window = words[i : i + size_w]
            if len(window) < int(self.min_chunk_size / 1.3):
                if window:
                    chunks.append(" ".join(window))
                break
            chunks.append(" ".join(window))

        return chunks

    def _merge_small_chunks(self, chunks: List[str]) -> List[str]:
        if not chunks:
            return []

        merged: List[str] = []
        pending = chunks[0]

        for next_chunk in chunks[1:]:
            if _estimate_tokens(pending) < self.min_chunk_size:
                pending = pending + "\n\n" + next_chunk
            else:
                merged.append(pending)
                pending = next_chunk

        merged.append(pending)
        return merged

    def _build_chunks(
        self,
        raw_chunks: List[str],
        page_map: List[tuple],
        source_meta: dict,
    ) -> List[Chunk]:

        chunks = []
        char_cursor = 0

        for idx, text in enumerate(raw_chunks):

            start_char = char_cursor
            end_char = start_char + len(text)
            source_pages = self._resolve_pages(start_char, end_char, page_map)

            chunk = Chunk(
                chunk_id=str(uuid.uuid4()),
                text=text,
                token_estimate=_estimate_tokens(text),
                metadata={
                    "source": source_meta.get("source", ""),
                    "filename": source_meta.get("filename", ""),
                    "pages": source_pages,
                    "page_start": source_pages[0] if source_pages else 0,
                    "chunk_index": idx,
                    "total_chunks": len(raw_chunks),
                    "token_estimate": _estimate_tokens(text),
                },
            )
            chunks.append(chunk)
            char_cursor = end_char + 2

        for chunk in chunks:
            chunk.metadata["total_chunks"] = len(chunks)

        return chunks

    @staticmethod
    def _resolve_pages(
        start_char: int,
        end_char: int,
        page_map: List[tuple],
    ) -> List[int]:

        pages = []
        for p_start, p_end, page_num in page_map:
            if p_start <= end_char and p_end >= start_char:
                pages.append(page_num)
        return pages if pages else [0]
