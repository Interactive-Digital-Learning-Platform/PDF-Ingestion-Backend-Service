"""Create the collection AI-Learning-Assistant-Service targets for large PDF chat attachments.

Collection name matches that service's own ATTACHMENT_QDRANT_COLLECTION setting as of writing —
confirm with that team before relying on it; if it changes, add a new numbered migration rather
than editing this one after it's been applied anywhere.
"""

from qdrant_client.models import Distance, VectorParams

from app.core.config import settings

REVISION = "002"
DESCRIPTION = "create attachment collection for AI-Learning-Assistant-Service"

_COLLECTION = "user_uploads"


def upgrade(client) -> None:
    if not client.collection_exists(_COLLECTION):
        client.create_collection(
            collection_name=_COLLECTION,
            vectors_config=VectorParams(size=settings.EMBEDDING_DIM, distance=Distance.COSINE),
        )

    client.create_payload_index(_COLLECTION, field_name="job_id", field_schema="keyword")
    # AI-Learning-Assistant-Service scopes every attachment search to one conversation via an
    # ExactMatchFilter on this field — see that service's dev-docs/file-upload-multimodal-rag.md §8.1.
    client.create_payload_index(_COLLECTION, field_name="conversation_id", field_schema="keyword")


def downgrade(client) -> None:
    client.delete_collection(_COLLECTION)
