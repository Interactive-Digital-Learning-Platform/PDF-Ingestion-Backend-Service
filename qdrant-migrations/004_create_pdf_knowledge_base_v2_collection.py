from qdrant_client.models import Distance, Modifier, SparseVectorParams, VectorParams

from app.core.config import settings

REVISION = "004"
DESCRIPTION = "create pdf_knowledge_base_v2 hybrid collection"

_COLLECTION = "pdf_knowledge_base_v2"


def upgrade(client) -> None:
    if not client.collection_exists(_COLLECTION):
        client.create_collection(
            collection_name=_COLLECTION,
            vectors_config={
                settings.DENSE_VECTOR_NAME: VectorParams(
                    size=settings.EMBEDDING_DIM, distance=Distance.COSINE
                )
            },
            sparse_vectors_config={
                settings.SPARSE_VECTOR_NAME: SparseVectorParams(modifier=Modifier.IDF)
            },
        )

    client.create_payload_index(_COLLECTION, field_name="job_id", field_schema="keyword")
    client.create_payload_index(_COLLECTION, field_name="grade", field_schema="integer")
    client.create_payload_index(_COLLECTION, field_name="subject", field_schema="keyword")


def downgrade(client) -> None:
    client.delete_collection(_COLLECTION)
