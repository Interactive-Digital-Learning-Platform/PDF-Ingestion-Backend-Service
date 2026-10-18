"""Drop the original pdf_knowledge_base collection — pdf_knowledge_base_v2 (006) replaces it as the
one curriculum knowledge base. See dev-docs/knowledge-base-v2-migration-guide.md.

v2 adds a sparse BM25 vector next to the dense one (hybrid retrieval), so the two can't share a
collection, and keeping both would split the knowledge base. Every service reading the knowledge
base must point at pdf_knowledge_base_v2 before this is applied.

upgrade() deletes every point still in pdf_knowledge_base — snapshot it first if it isn't empty.
downgrade() recreates it empty, as 001 creates it; the deleted points can only come back from that
snapshot or by re-ingesting the PDFs.
"""

from qdrant_client.models import Distance, VectorParams

from app.core.config import settings

REVISION = "008"
DESCRIPTION = "drop pdf_knowledge_base, replaced by pdf_knowledge_base_v2"

_COLLECTION = "pdf_knowledge_base"


def upgrade(client) -> None:
    if client.collection_exists(_COLLECTION):
        client.delete_collection(_COLLECTION)


def downgrade(client) -> None:
    if not client.collection_exists(_COLLECTION):
        client.create_collection(
            collection_name=_COLLECTION,
            vectors_config=VectorParams(size=settings.EMBEDDING_DIM, distance=Distance.COSINE),
        )

    client.create_payload_index(_COLLECTION, field_name="job_id", field_schema="keyword")
