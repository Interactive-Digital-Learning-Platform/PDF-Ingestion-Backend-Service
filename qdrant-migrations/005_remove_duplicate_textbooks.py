"""Remove two textbooks that were indexed into pdf_knowledge_base twice under different filenames —
see qdrant-migrations/_lib/runner_support.py.

- grade-10-science-text-book-6200ec8f089d4.pdf duplicates "science grade 10 part 1.pdf" +
  "science grade 10 part 2.pdf" (the two parts are kept).
- grade-11-geography-text-book-6200f4747a893.pdf duplicates "geography grade 11.pdf" (that one is kept;
  all 97 of its chunks also appear in the duplicate).

Duplicates made retrieval return the same passage twice, crowding out other results for both
Personalized-Quiz-Service and the AI Learning Assistant. Neither file has an ingestion job row (so the
DELETE /ingest/{job_id} endpoint can't remove them); their chunks are deleted here by filename.

downgrade() can't restore deleted vectors — re-ingest the PDFs (or restore the collection snapshot
taken before applying this) to bring them back.
"""

import logging

from qdrant_client.models import FieldCondition, Filter, FilterSelector, MatchValue

REVISION = "005"
DESCRIPTION = "remove duplicate Grade 10 Science / Grade 11 Geography textbook chunks from pdf_knowledge_base"

_COLLECTION = "pdf_knowledge_base"
_DUPLICATE_FILENAMES = [
    "grade-10-science-text-book-6200ec8f089d4.pdf",
    "grade-11-geography-text-book-6200f4747a893.pdf",
]

logger = logging.getLogger(__name__)


def upgrade(client) -> None:
    # Qdrant Cloud's strict mode rejects filtering on an unindexed payload field.
    client.create_payload_index(_COLLECTION, field_name="filename", field_schema="keyword")

    for filename in _DUPLICATE_FILENAMES:
        selector = Filter(must=[FieldCondition(key="filename", match=MatchValue(value=filename))])
        count = client.count(_COLLECTION, count_filter=selector, exact=True).count
        client.delete(_COLLECTION, points_selector=FilterSelector(filter=selector))
        logger.info("Deleted %d duplicate chunks of %r", count, filename)


def downgrade(client) -> None:
    logger.warning(
        "005 can't be reverted automatically: re-ingest %s or restore the pre-005 collection snapshot.",
        ", ".join(_DUPLICATE_FILENAMES),
    )
