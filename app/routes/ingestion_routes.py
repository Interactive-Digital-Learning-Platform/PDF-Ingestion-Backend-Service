import json
import logging
import uuid
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
from pydantic import ValidationError
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.constants.sources import WEB_SOURCE_SERVICE
from app.core.config import settings
from app.core.database import get_async_session
from app.models.job_model import Job
from app.schemas.document_tags import DocumentTags
from app.schemas.job_schema import JobStatus
from app.services.job_service import (
    JobService,
    get_job_service,
    validate_and_upload_pdf,
)
from app.services.storage_service import StorageService, get_storage_service

logger = logging.getLogger(__name__)

ingestion_router = APIRouter(prefix="/ingest", tags=["ingestion"])


def _parse_file_tags(metadata: str, file_count: int) -> list[DocumentTags]:
    try:
        entries = json.loads(metadata) if metadata.strip() else []
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="metadata must be valid JSON")

    if not isinstance(entries, list) or len(entries) > file_count:
        raise HTTPException(
            status_code=400,
            detail="metadata must be a JSON array with at most one entry per file",
        )

    tags: list[DocumentTags] = []

    for position, entry in enumerate(entries, 1):
        try:
            tags.append(DocumentTags.model_validate(entry or {}))
        except ValidationError as exc:
            message = "; ".join(error["msg"] for error in exc.errors())
            raise HTTPException(status_code=422, detail=f"File {position}: {message}")

    return tags + [DocumentTags()] * (file_count - len(tags))


@ingestion_router.get("/curriculum")
async def get_curriculum():
    return {
        "grades": list(range(settings.KB_MIN_GRADE, settings.KB_MAX_GRADE + 1)),
        "subjects": settings.KB_SUBJECTS,
    }


@ingestion_router.post("/upload")
async def upload_pdfs(
    files: list[UploadFile] = File(...),
    user_id: str = Form(...),
    metadata: str = Form("[]"),
    storage_service: StorageService = Depends(get_storage_service),
    job_service: JobService = Depends(get_job_service),
):

    if not user_id or not user_id.strip():
        raise HTTPException(status_code=400, detail="user_id is required")

    if not files:
        raise HTTPException(status_code=400, detail="At least one file is required")

    file_tags = _parse_file_tags(metadata, len(files))
    created_jobs = []

    for file, tags in zip(files, file_tags):
        job_id = uuid.uuid4()

        object_key, filename, file_size = await validate_and_upload_pdf(
            file, storage_service, str(job_id)
        )

        logger.info(f"Uploaded {filename} ({file_size} bytes) to Minio storage:{object_key}")

        job = await job_service.create_job(
            job_id=job_id,
            object_key=object_key,
            filename=filename,
            user_id=user_id,
            source_service=WEB_SOURCE_SERVICE,
            qdrant_collection=settings.DEFAULT_COLLECTION,
            caller_metadata={"user_id": user_id, **tags.payload()},
        )

        logger.info(
            f"Job {job.job_id} queued for {filename} "
            f"(user={user_id}, grade={tags.grade}, subject={tags.subject})"
        )
        created_jobs.append(
            {
                "job_id": str(job.job_id),
                "filename": filename,
                "status": JobStatus.QUEUED.value,
                "caller_metadata": job.caller_metadata,
            }
        )

    return {"jobs": created_jobs}


@ingestion_router.get("/jobs")
async def list_jobs(
    job_service: JobService = Depends(get_job_service),
):
    jobs = await job_service.get_all_jobs()
    return {"jobs": [job.to_dict() for job in jobs]}


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
    job = await _get_job_or_404(job_id, db)
    return job.to_dict()


@ingestion_router.patch("/{job_id}/metadata")
async def update_job_metadata(
    job_id: str,
    tags: DocumentTags,
    db: AsyncSession = Depends(get_async_session),
):
    job = await _get_job_or_404(job_id, db)

    if job.status not in (JobStatus.DONE.value, JobStatus.FAILED.value):
        raise HTTPException(
            status_code=409,
            detail="Tags can be changed once ingestion has finished",
        )

    changes = tags.model_dump(include=tags.model_fields_set)
    if not changes:
        return job.to_dict()

    to_set = {key: value for key, value in changes.items() if value is not None}
    to_clear = [key for key, value in changes.items() if value is None]

    if job.status == JobStatus.DONE.value:
        from app.pipeline.indexer import VectorIndexer

        indexer = VectorIndexer(
            url=settings.QDRANT_DB_URL,
            collection_name=job.qdrant_collection,
        )

        try:
            if to_set:
                await indexer.set_document_payload(str(job.job_id), to_set)
            if to_clear:
                await indexer.delete_document_payload(str(job.job_id), to_clear)
        except (UnexpectedResponse, ResponseHandlingException, OSError) as exc:
            logger.error(f"Could not update Qdrant tags for job {job_id}: {exc}")
            raise HTTPException(
                status_code=502, detail="Could not update the knowledge base tags"
            )
        finally:
            await indexer.client.close()

    metadata = {
        key: value
        for key, value in {**(job.caller_metadata or {}), **to_set}.items()
        if key not in to_clear
    }
    job.caller_metadata = metadata
    await db.commit()
    await db.refresh(job)

    logger.info(f"Updated tags for job {job_id}: set={to_set} cleared={to_clear}")
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
    deleted = await indexer.delete_document(str(job.job_id))
    logger.info(f"Deleted {deleted} Qdrant points for job {job_id}")

    await db.delete(job)
    await db.commit()

    return {
        "message": f"Job {job_id} deleted",
        "qdrant_deleted": deleted,
    }
