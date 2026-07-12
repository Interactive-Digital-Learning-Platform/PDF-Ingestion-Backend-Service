import os

from dotenv import load_dotenv

load_dotenv()


DB_URL = os.getenv("DB_URL")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text-v1.5")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

MAX_TOKENS_PER_INPUT = int(os.getenv("MAX_TOKENS_PER_INPUT", 8192))
MAX_INPUTS_PER_BATCH = int(os.getenv("MAX_INPUTS_PER_BATCH", 32))

MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", 1.0))

# Qdrant configurations
DEFAULT_COLLECTION = os.getenv("DEFAULT_COLLECTION", "pdf_knowledge_base")
UPSERT_BATCH_SIZE = 100
QDRANT_DB_URL = os.getenv("QDRANT_DB_URL")

# Minio configurations
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

# Redis configurations
REDIS_BROKER_URL = os.getenv("REDIS_BROKER_URL", "redis://localhost:6379/0")
REDIS_BACKEND_URL = os.getenv("REDIS_BACKEND_URL", "redis://localhost:6379/1")


# Pipeline batch configs
PAGE_BATCH_SIZE = 20
PAGE_OVERLAP = 2
CHUNK_SIZE = 512
CHUNK_OVERLAP = 64

_allowed_content_types_raw = os.getenv("ALLOWED_CONTENT_TYPES", "")
ALLOWED_CONTENT_TYPES = {
    content_type.strip()
    for content_type in _allowed_content_types_raw.split(",")
    if content_type.strip()
}
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", 100))

EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", 768))
