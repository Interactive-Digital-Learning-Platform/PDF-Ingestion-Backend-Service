from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from app.services.storage_service import StorageService
from app.pipeline.indexer import VectorIndexer
from app.pipeline.embedder import EmbeddingGenerator
from app.routes.ingestion_routes import ingestion_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    storage_service = StorageService()
    await storage_service.ensure_bucket()

    embedder = EmbeddingGenerator()
    indexer = VectorIndexer()
    await indexer.ensure_collection(
        vector_dim=embedder.embedding_dimension()
    )

    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ingestion_router)


@app.get("/", tags=["health"])
async def root():
    """Liveness ping — returns 200 immediately."""
    return {
        "service": "pdf_ingestion",
        "status": "running",
        "docs": "/docs",
    }
