from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routes.ingestion_routes import ingestion_router
from app.routes.internal_ingestion_routes import internal_ingestion_router
from app.services.storage_service import StorageService


@asynccontextmanager
async def lifespan(app: FastAPI):
    storage_service = StorageService()
    await storage_service.open()
    await storage_service.ensure_bucket()
    app.state.storage_service = storage_service

    try:
        yield
    finally:
        await storage_service.close()



app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ingestion_router)
app.include_router(internal_ingestion_router)


@app.get("/", tags=["health"])
async def root():
    """Liveness ping — returns 200 immediately."""
    return {
        "service": "pdf_ingestion",
        "status": "running",
        "docs": "/docs",
    }
