
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    DB_URL: str
    EMBEDDING_MODEL: str = "nomic-ai/nomic-embed-text-v1.5"
    # OPENAI_API_KEY:str
    
    MAX_TOKENS_PER_INPUT: int = 8192
    MAX_INPUTS_PER_BATCH: int = 32
    
    MAX_RETRIES: int = 3
    RETRY_BASE_DELAY: float = 1.0
    
    DEFAULT_COLLECTION: str = "pdf_knowledge_base"
    UPSERT_BATCH_SIZE: int = 100
    QDRANT_DB_URL:str
    
    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_BUCKET: str
    
    REDIS_BROKER_URL: str
    REDIS_BACKEND_URL: str
    
    PAGE_BATCH_SIZE: int = 20
    PAGE_OVERLAP: int = 2
    CHUNK_SIZE: int = 512
    CHUNK_OVERLAP: int = 64
    
    ALLOWED_CONTENT_TYPES: set[str]
    
    MAX_FILE_SIZE_MB: int = 100
    
    EMBEDDING_DIM: int = 768

    MULTIPART_CHUNK_SIZE: int = 8 * 1024 * 1024
    DB_POOL_SIZE:int = 1
    DB_MAX_OVERFLOW: int = 0

    INTERNAL_SERVICE_KEY: str

    AI_LEARNING_ASSISTANT_BASE_URL: str
    AI_LEARNING_ASSISTANT_WEBHOOK_PATH: str = "/internal/webhooks/pdf-ingestion"
    WEBHOOK_TIMEOUT_SECONDS: float = 10.0
    WEBHOOK_MAX_RETRIES: int = 5
    WEBHOOK_RETRY_BASE_DELAY: float = 2.0


settings = Settings()



