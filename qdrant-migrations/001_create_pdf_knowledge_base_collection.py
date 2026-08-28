"""Create the pdf_knowledge_base collection — the existing admin-curated KB, used by the
existing web-upload flow (source_service="pdf-ingestion-web-application").
"""

from qdrant_client.models import Distance, VectorParams

from app.core.config import settings

REVISION = "001"
DESCRIPTION = "create pdf_knowledge_base collection"

_COLLECTION = "pdf_knowledge_base"


def upgrade(client) -> None:
    if not client.collection_exists(_COLLECTION):
        client.create_collection(
            collection_name=_COLLECTION,
            vectors_config=VectorParams(size=settings.EMBEDDING_DIM, distance=Distance.COSINE),
        )

    # Enables an efficient filtered delete for DELETE /ingest/{job_id} — see
    # dev-docs/generic-pdf-processing-service.md §14.
    client.create_payload_index(_COLLECTION, field_name="job_id", field_schema="keyword")


def downgrade(client) -> None:
    client.delete_collection(_COLLECTION)
