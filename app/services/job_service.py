import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

from fastapi import Depends, HTTPException, UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.registry import WEBHOOK_CLIENT_REGISTRY
from app.core.config import settings
from app.core.database import get_async_session
from app.models.job_model import Job
from app.schemas.job_schema import CallbackStatus, JobStatus
from app.services.storage_service import StorageService
from app.workers.tasks import process_pdf_task

UPLOAD_READ_CHUNK_SIZE = 1024 * 1024


async def _iter_upload(file: UploadFile, chunk_size: int = UPLOAD_READ_CHUNK_SIZE) -> AsyncIterator[bytes]:
    while True:
        chunk = await file.read(chunk_size)

        if not chunk:
            break

        yield chunk


async def validate_and_upload_pdf(
    file: UploadFile,
    storage_service: StorageService,
    job_id: str,
) -> tuple[str, str, int]:
    
    if file.content_type not in settings.ALLOWED_CONTENT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"'{file.filename}' is not a PDF (got {file.content_type})",
        )

    filename = file.filename or f"upload_{job_id}.pdf"
    object_key = StorageService.make_object_key(job_id, filename)
    max_bytes = settings.MAX_FILE_SIZE_MB * 1024 * 1024

    try:
        file_size = await storage_service.upload_stream(
            object_key=object_key,
            chunks=_iter_upload(file),
            content_type=file.content_type or "application/pdf",
            max_bytes=max_bytes,
        )
    except ValueError:
        raise HTTPException(
            status_code=413,
            detail=f"'{filename}' exceeds {settings.MAX_FILE_SIZE_MB}MB limit",
        )

    return object_key, filename, file_size


class JobService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all_jobs(self) -> list[Job]:

        result = await self.session.execute(
            select(Job).order_by(Job.started_at.desc())
        )

        return list(result.scalars().all())

    async def create_job(
        self,
        *,
        job_id: uuid.UUID,
        object_key: str,
        filename: str,
        user_id: str,
        source_service: str,
        qdrant_collection: str,
        caller_metadata: dict | None = None,
        external_reference_id: str | None = None,
        max_pages: int | None = None,
        callback_required: bool = False,
    ) -> Job:
        
        if callback_required and source_service not in WEBHOOK_CLIENT_REGISTRY:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"callback_requested=true but source_service='{source_service}' has no "
                    f"registered webhook client"
                ),
            )

        job = Job(
            job_id=job_id,
            user_id=user_id,
            filename=filename,
            minio_object_key=object_key,
            qdrant_collection=qdrant_collection,
            status=JobStatus.QUEUED.value,
            progress=0,
            started_at=datetime.now(UTC),
            source_service=source_service,
            external_reference_id=external_reference_id,
            caller_metadata=caller_metadata,
            max_pages=max_pages,
            callback_required=callback_required,
            callback_status=CallbackStatus.PENDING.value if callback_required else None,
        )

        self.session.add(job)
        await self.session.commit()
        await self.session.refresh(job)

        process_pdf_task.delay(job_id=str(job.job_id))

        return job


def get_job_service(session: AsyncSession = Depends(get_async_session)) -> JobService:
    return JobService(session)
