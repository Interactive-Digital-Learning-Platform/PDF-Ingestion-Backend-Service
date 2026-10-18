REVISION = "007"
DESCRIPTION = "index the chapter payload field on pdf_knowledge_base_v2"

_COLLECTION = "pdf_knowledge_base_v2"


def upgrade(client) -> None:
    client.create_payload_index(_COLLECTION, field_name="chapter", field_schema="keyword")


def downgrade(client) -> None:
    client.delete_payload_index(_COLLECTION, field_name="chapter")
