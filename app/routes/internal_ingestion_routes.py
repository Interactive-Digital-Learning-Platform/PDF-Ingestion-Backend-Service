import json
import logging
import uuid

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from app.dependencies.internal_auth import require_internal_service
from app.services.job_service import (
    JobService,
    get_job_service,
    validate_and_upload_pdf,
)
from app.services.storage_service import StorageService, get_storage_service

logger = logging.getLogger(__name__)

internal_ingestion_router = APIRouter(
    prefix="/ingest/internal",
    tags=["internal-ingestion"],
    dependencies=[Depends(require_internal_service)],
)


@internal_ingestion_router.post("/documents", status_code=202)
async def submit_document(
    file: UploadFile = File(...),
    source_service: str = Form(...),
    user_id: str = Form(...),
    external_reference_id: str = Form(...),
    qdrant_collection: str = Form(...),
    metadata: str = Form("{}"),
    max_pages: int | None = Form(None),
    callback_requested: bool = Form(True),
    storage_service: StorageService = Depends(get_storage_service),
    job_service: JobService = Depends(get_job_service),
):
    if not source_service or not source_service.strip():
        raise HTTPException(status_code=400, detail="source_service is required")

    if not qdrant_collection or not qdrant_collection.strip():
        raise HTTPException(status_code=400, detail="qdrant_collection is required")

    try:
        caller_metadata = json.loads(metadata) if metadata else {}
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="metadata must be valid JSON")

    if not isinstance(caller_metadata, dict):
        raise HTTPException(status_code=400, detail="metadata must be a JSON object")

    job_id = uuid.uuid4()

    object_key, filename, file_size = await validate_and_upload_pdf(
        file, storage_service, str(job_id)
    )

    logger.info(
        f"Internal upload — {filename} ({file_size} bytes) "
        f"from source_service={source_service}"
    )

    job = await job_service.create_job(
        job_id=job_id,
        object_key=object_key,
        filename=filename,
        user_id=user_id,
        source_service=source_service,
        qdrant_collection=qdrant_collection,
        caller_metadata=caller_metadata,
        external_reference_id=external_reference_id,
        max_pages=max_pages,
        callback_required=callback_requested,
    )

    logger.info(
        f"Job {job.job_id} queued for {filename} "
        f"(source_service={source_service}, external_reference_id={external_reference_id})"
    )

    return {"job_id": str(job.job_id), "status": "queued"}
