import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Annotated

import redis.asyncio as aioredis
from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_async_session
from app.models.job_model import Job
from app.schemas.job_schema import JobStatus
from app.services.storage_service import StorageService, get_storage_service
from app.workers.tasks import process_pdf_task

logger = logging.getLogger(__name__)

ingestion_router = APIRouter(prefix="/ingest", tags=["ingestion"])

UPLOAD_READ_CHUNK_SIZE = 1024 * 1024

async def _iter_upload(file: UploadFile, chunk_size: int = UPLOAD_READ_CHUNK_SIZE):
    while True:
        chunk = await file.read(chunk_size)

        if not chunk:
            break

        yield chunk


@ingestion_router.post("/upload")
async def upload_pdfs(
    files: list[UploadFile] = File(...),
    user_id: str = Form(...),
    db: AsyncSession = Depends(get_async_session),
    storage_service: StorageService = Depends(get_storage_service),
):

    if not user_id or not user_id.strip():
        raise HTTPException(status_code=400, detail="user_id is required")

    if not files:
        raise HTTPException(status_code=400, detail="At least one file is required")

    created_jobs = []
    max_bytes = settings.MAX_FILE_SIZE_MB * 1024 * 1024

    for file in files:

        if file.content_type not in settings.ALLOWED_CONTENT_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"'{file.filename}' is not a PDF (got {file.content_type})",
            )

        job_id = str(uuid.uuid4())
        filename = file.filename or f"upload_{job_id}.pdf"

        object_key = StorageService.make_object_key(job_id, filename)

        try:
            file_size = await storage_service.upload_stream(
                object_key=object_key,
                chunks=_iter_upload(file),
                content_type=file.content_type or "application/pdf",
                max_bytes=max_bytes
            )
        except ValueError:
            raise HTTPException(
                status_code=413,
                detail=f"'{filename}' exceeds {settings.MAX_FILE_SIZE_MB}MB limit"
            )
        
        logger.info(f"Uploaded {filename} ({file_size} bytes) to Minio storage:{object_key}")

        job = Job(
            job_id=uuid.UUID(job_id),
            user_id=user_id,
            filename=filename,
            minio_object_key=object_key,
            qdrant_collection=settings.DEFAULT_COLLECTION,
            status=JobStatus.QUEUED.value,
            progress=0,
            started_at=datetime.now(UTC),
        )
        
        db.add(job)
        await db.commit()
        await db.refresh(job)

        process_pdf_task.delay(
            job_id=job_id,
            filename=filename,
            minio_object_key=object_key,
        )

        logger.info(f"Job {job_id} queued for {filename} (user={user_id})")
        created_jobs.append(
            {
                "job_id": job_id,
                "filename": filename,
                "status": JobStatus.QUEUED.value,
            }
        )

    return {"jobs": created_jobs}


@ingestion_router.websocket("/ws/{job_id}")
async def ingestion_progress_ws(
    websocket: WebSocket,
    job_id: str,
    db: Annotated[AsyncSession, Depends(get_async_session)],
):

    await websocket.accept()
    logger.info(f"WebSocket connected for job {job_id}")

    channel = f"job:{job_id}"
    redis_client = aioredis.from_url(settings.REDIS_BROKER_URL, decode_responses=True)
    pubsub = redis_client.pubsub()

    try:

        job = await db.get(Job, uuid.UUID(job_id))
        if job:
            await websocket.send_json(job.to_dict())

            if job.status in (JobStatus.DONE.value, JobStatus.FAILED.value):
                logger.info(f"Job {job_id} already terminal — closing WS")
                return

        await pubsub.subscribe(channel)
        logger.info(f"Subscribed to Redis channel: {channel}")

        async for message in pubsub.listen():
            if message["type"] != "message":
                continue

            try:
                data = json.loads(message["data"])
            except (json.JSONDecodeError, TypeError):
                continue

            await websocket.send_json(data)

            if data.get("status") in ("done", "failed"):
                logger.info(f"Job {job_id} reached terminal state — closing WS")
                break

    except WebSocketDisconnect:
        logger.info(f"WebSocket client disconnected for job {job_id}")

    except Exception as e:
        logger.error(f"WebSocket error for job {job_id}: {e}")
        try:
            await websocket.send_json(
                {
                    "job_id": job_id,
                    "status": "error",
                    "message": f"WebSocket error: {e}",
                }
            )
        except Exception:
            pass

    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await redis_client.aclose()
        logger.info(f"WebSocket cleaned up for job {job_id}")


async def _get_job_or_404(job_id: str, db: AsyncSession) -> Job:
    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job_id format")

    job = await db.get(Job, job_uuid)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job


@ingestion_router.get("/status/{job_id}")
async def get_job_status(
    job_id: str,
    db: AsyncSession = Depends(get_async_session),
):
    """
    Return the current state of an ingestion job.
    Use this for polling when WebSocket is not available.
    """
    job = await _get_job_or_404(job_id, db)
    return job.to_dict()


@ingestion_router.delete("/{job_id}")
async def delete_job(
    job_id: str,
    db: AsyncSession = Depends(get_async_session),
    storage: StorageService = Depends(get_storage_service),
):

    job = await _get_job_or_404(job_id, db)

    await storage.delete(job.minio_object_key)

    from app.pipeline.indexer import VectorIndexer

    indexer = VectorIndexer(
        url=settings.QDRANT_DB_URL,
        collection_name=job.qdrant_collection,
    )
    deleted = await indexer.delete_document(job.filename)
    logger.info(f"Deleted {deleted} Qdrant points for job {job_id}")

    await db.delete(job)
    await db.commit()

    return {
        "message": f"Job {job_id} deleted",
        "qdrant_deleted": deleted,
    }
