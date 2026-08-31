"""Backfill grade/subject metadata onto pdf_knowledge_base so Personalized-Quiz-Service can filter
retrieval to the right curriculum content — see qdrant-migrations/_lib/runner_support.py.

pdf_knowledge_base already holds the ingested Grade 10/11 textbook PDFs (used to ground Quiz
Service's AI question generation), but points were indexed without structured `grade`/`subject`
payload fields — only `filename`/`source`. This derives them from the known textbook filenames so
retrieval_service.py's grade+subject filter has something to match on, without needing a separate
quiz_knowledge_base collection or re-ingesting anything.
"""

import logging

from qdrant_client.models import Filter

REVISION = "003"
DESCRIPTION = "backfill grade/subject metadata on pdf_knowledge_base textbook chunks"

_COLLECTION = "pdf_knowledge_base"

logger = logging.getLogger(__name__)

# Known textbook filenames -> (grade, canonical subject). Canonical subject strings must match
# Personalized-Quiz-Service's app/data/curriculum/grade_{10,11}.json keys exactly, since
# curriculum_service.canonical_subject() is what quiz requests are normalized through before
# reaching retrieval.
_FILENAME_METADATA: dict[str, tuple[int, str]] = {
    "ICT grade 10.pdf": (10, "ICT"),
    "ICT grade 11.pdf": (11, "ICT"),
    "english grade 10 work book.pdf": (10, "English"),
    "english grade 11 work book.pdf": (11, "English"),
    "geography grade 10.pdf": (10, "Geography"),
    "geography grade 11.pdf": (11, "Geography"),
    "grade-10-science-text-book-6200ec8f089d4.pdf": (10, "Science"),
    "grade-11-geography-text-book-6200f4747a893.pdf": (11, "Geography"),
    "health science grade 10.pdf": (10, "Health and Physical Education"),
    "health science grade 11.pdf": (11, "Health and Physical Education"),
    "histoy grade 10.pdf": (10, "History"),
    "histoy grade 11.pdf": (11, "History"),
    "mathematics grade 10 part 1.pdf": (10, "Mathematics"),
    "mathematics grade 10 part 2.pdf": (10, "Mathematics"),
    "mathematics grade 11 part 1.pdf": (11, "Mathematics"),
    "mathematics grade 11 part 2.pdf": (11, "Mathematics"),
    "mathematics grade 11 part 3.pdf": (11, "Mathematics"),
    "science grade 10 part 1.pdf": (10, "Science"),
    "science grade 10 part 2.pdf": (10, "Science"),
    "science grade 11 part 1.pdf": (11, "Science"),
    "science grade 11 part 2.pdf": (11, "Science"),
}


def upgrade(client) -> None:
    client.create_payload_index(_COLLECTION, field_name="grade", field_schema="integer")
    client.create_payload_index(_COLLECTION, field_name="subject", field_schema="keyword")

    updated, skipped = 0, 0
    offset = None
    while True:
        points, offset = client.scroll(
            _COLLECTION,
            limit=200,
            offset=offset,
            with_payload=["filename", "source"],
            with_vectors=False,
        )
        if not points:
            break

        by_meta: dict[tuple[int, str], list] = {}
        for point in points:
            payload = point.payload or {}
            filename = payload.get("filename") or payload.get("source")
            meta = _FILENAME_METADATA.get(filename)
            if meta is None:
                skipped += 1
                logger.warning(
                    "No grade/subject mapping for filename=%r (point %s) — skipping", filename, point.id
                )
                continue
            by_meta.setdefault(meta, []).append(point.id)

        for (grade, subject), ids in by_meta.items():
            client.set_payload(_COLLECTION, payload={"grade": grade, "subject": subject}, points=ids)
            updated += len(ids)

        if offset is None:
            break

    logger.info("Backfilled grade/subject on %d points (%d skipped, no filename mapping)", updated, skipped)


def downgrade(client) -> None:
    client.delete_payload(_COLLECTION, keys=["grade", "subject"], points_selector=Filter(must=[]))
