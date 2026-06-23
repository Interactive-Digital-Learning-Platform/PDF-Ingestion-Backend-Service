import fitz
import re
from dataclasses import dataclass, field
from typing import Generator
from pathlib import Path


@dataclass
class PageContent:
    """Single extracted page content"""

    page_count: int
    text: str
    char_count: int
    metadata: dict = field(default_factory=dict)


class PDFExtractor:

    def __init__(
        self,
        min_chars: int = 80,
        ocr_fallback: bool = False,
        extract_images_meta: bool = True,
    ):
        self.min_chars = min_chars
        self.ocr_fallback = ocr_fallback
        self.extract_images_meta = extract_images_meta

    def extract(self, pdf_path: str) -> Generator[PageContent, None, None]:

        file_path = Path(pdf_path)

        if not file_path.exists():
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")

        doc = fitz.open(str(file_path))
        total_pages = len(doc)
        filename = file_path.name

        skipped = 0
        extracted = 0

        try:
            for page_index in range(total_pages):
                page = doc[page_index]
                page_number = page_index + 1

                raw_text = self._extract_text(page)

                is_ocr = False

                if len(raw_text.strip()) < self.min_chars and self.ocr_fallback:
                    raw_text = self._ocr_page(page, document=doc)
                    is_ocr = True

                clean_text = self._clean(raw_text)

                if len(clean_text) < self.min_chars:
                    skipped += 1
                    continue

                metadata = {
                    "source": str(file_path),
                    "filename": filename,
                    "page": page_number,
                    "total_pages": total_pages,
                    "is_ocr_fallback": is_ocr,
                    "word_count": len(clean_text.split()),
                }

                if self.extract_images_meta:
                    metadata["image_count"] = len(page.get_images())

                extracted += 1

                yield PageContent(
                    page_count=page_number,
                    text=clean_text,
                    char_count=len(clean_text),
                    metadata=metadata,
                )

                page = None

        finally:
            doc.close()

    def inspect(self, pdf_path: str) -> dict:
        
        path = Path(pdf_path)
        doc = fitz.open(str(path))

        first_page_text = doc[0].get_text("text") if len(doc) > 0 else ""
        is_scanned = len(first_page_text.strip()) < 50

        info = {
            "filename": path.name,
            "total_pages": len(doc),
            "file_size_mb": round(path.stat().st_size / 1_048_576, 2),
            "is_scanned": is_scanned,
            "pdf_version": doc.metadata.get("format", "N/A"),
            "metadata": doc.metadata,
        }
        doc.close()
        return info

    def _extract_text(self, page: fitz.Page) -> str:

        blocks = page.get_text("blocks", sort=True)

        text_parts = [b[4] for b in blocks if b[6] == 0 and b[4].strip()]

        return "\n".join(text_parts)

    def _ocr_page(self, page: fitz.Page, document: fitz.Document) -> str:
        try:
            import pytesseract
            from PIL import Image
            import io

            mat = fitz.Matrix(200 / 72, 200 / 72)
            pix = page.get_pixmap(matrix=mat, colorspace=fitz.csRGB)
            img_data = pix.tobytes("png")

            image = Image.open(io.BytesIO(img_data))
            text = pytesseract.image_to_string(image, lang="eng")

            return text

        except ImportError:

            return ""

    @staticmethod
    def _clean(text: str) -> str:
        if not text:
            return ""

        replacements = {
            "\ufb00": "ff",  # ﬀ
            "\ufb01": "fi",  # ﬁ
            "\ufb02": "fl",  # ﬂ
            "\ufb03": "ffi",  # ﬃ
            "\ufb04": "ffl",  # ﬄ
            "\u00ad": "",  # soft hyphen
        }

        for char, replacement in replacements.items():
            text = text.replace(char, replacement)

        text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)

        lines = text.splitlines()
        lines = [re.sub(r"[ \t]+", " ", line).strip() for line in lines]

        text = "\n".join(lines)
        text = re.sub(r"\n{3,}", "\n\n", text)

        return text.strip()
