import asyncio
import hashlib
import json
import logging
import traceback
from datetime import UTC, datetime
from pathlib import Path

from celery import Task
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from app.core.config import settings
from app.pipeline.extractor import PDFExtractor
from app.schemas.job_schema import JobStatus
from app.services.celery_storage_service import CeleryStorageService
from app.workers.celery_app import celery
from app.workers.celery_worker_resources import (
    get_chunker,
    get_db_engine,
    get_embedder,
    get_event_loop,
    get_indexer,
    get_redis_client,
)

logger = logging.getLogger(__name__)


class ProgressPublisher:

    def __init__(self):
        self._redis = get_redis_client()

    def publish(
        self,
        job_id: str,
        stage: str,
        progress: int,
        message: str,
        status: str = "processing",
        **extra,
    ) -> None:
        channel = f"job:{job_id}"
        payload = json.dumps(
            {
                "job_id": job_id,
                "status": status,
                "stage": stage,
                "progress": progress,
                "message": message,
                **extra,
            }
        )
        self._redis.publish(channel, payload)
        logger.debug(f"Published to {channel}: stage={stage} progress={progress}%")

    def close(self):
        pass


def _update_job_sync(job_id: str, **fields) -> None:

    engine = get_db_engine()
    Session = sessionmaker(bind=engine)

    with Session() as session:
        set_clauses = ", ".join(f"{k} = :{k}" for k in fields)
        params = {"job_id": job_id, **fields}
        session.execute(
            text(f"UPDATE jobs SET {set_clauses} WHERE job_id = :job_id"),
            params,
        )
        session.commit()



def _process_batch(page_buffer, chunker, embedder):

    chunks = chunker.chunk_pages(page_buffer)
    embeddings = embedder.embed_chunks(chunks)
    return chunks, embeddings


def _chunk_fingerprint(chunk) -> str:
    text = chunk.get("text") if isinstance(chunk, dict) else getattr(chunk, "text", None)

    if text is None:
        text = str(chunk)

    return hashlib.sha256(text.encode("utf-8")).hexdigest()

    

async def _run_pipeline(
    job_id: str,
    filename: str,
    minio_object_key: str,
    publisher: ProgressPublisher
) -> dict:

    tmp_path = None

    try:
        _update_job_sync(
            job_id,
            status=JobStatus.PROCESSING.value,
            current_stage="downloading",
            progress=0,
        )
        
        publisher.publish(
            job_id,
            stage="downloading",
            progress=0,
            message="Downloading PDF from storage",
        )

        storage = CeleryStorageService(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            bucket=settings.MINIO_BUCKET,
        )
        tmp_path = await storage.download_to_temp(minio_object_key)
        logger.info(f"[{job_id}] PDF downloaded to {tmp_path}")

        publisher.publish(
            job_id,
            stage="extracting",
            progress=5,
            message="Starting text extraction",
        )

        extractor = PDFExtractor(min_chars=80)
        chunker = get_chunker()
        embedder = get_embedder()
        indexer = get_indexer()

        pdf_info = extractor.inspect(tmp_path)
        total_pages = pdf_info["total_pages"]
        logger.info(f"[{job_id}] {filename}: {total_pages} pages")

        all_chunks_count = 0
        pages_extracted = 0
        page_buffer: list = []
        seen_fingerprints: set[str] = set()


        async def flush(buffer: list) -> None:
            nonlocal all_chunks_count

            batch_chunks, batch_embeddings = _process_batch(buffer, chunker, embedder)

            new_chunks, new_embeddings = [], []

            for chunk, embedding in zip(batch_chunks, batch_embeddings):
                fp = _chunk_fingerprint(chunk)

                if fp in seen_fingerprints:
                    continue

                seen_fingerprints.add(fp)
                new_chunks.append(chunk)
                new_embeddings.append(embedding)

            if new_chunks:
                await indexer.index_chunks(
                    new_chunks,
                    new_embeddings,
                    job_id
                )

                all_chunks_count += len(new_chunks)

        for page in extractor.extract(tmp_path):
            page_buffer.append(page)
            pages_extracted += 1

            if len(page_buffer) >= settings.PAGE_BATCH_SIZE:
                await flush(page_buffer)
                page_buffer = page_buffer[-settings.PAGE_OVERLAP:]

                pct = min(5 + int((pages_extracted / total_pages) * 85), 90)

                _update_job_sync(
                    job_id,
                    progress=pct,
                    current_stage="indexing",
                )
                publisher.publish(
                    job_id,
                    stage="indexing",
                    progress=pct,
                    message=(
                        f"Processed {pages_extracted}/{total_pages} pages — "
                        f"{all_chunks_count} chunks indexed"
                    ),
                )

        if page_buffer:
            await flush(page_buffer)

        _update_job_sync(
            job_id,
            status=JobStatus.DONE.value,
            progress=100,
            current_stage="done",
            chunks_created=all_chunks_count,
            pages_processed=pages_extracted,
            completed_at=datetime.now(UTC).isoformat(),
        )
        publisher.publish(
            job_id,
            stage="done",
            progress=100,
            status="done",
            message=(
                f"Ingestion complete — {all_chunks_count} chunks from "
                f"{pages_extracted} pages"
            ),
            chunks_created=all_chunks_count,
            pages_processed=pages_extracted,
        )

        logger.info(
            f"[{job_id}] Done — {all_chunks_count} chunks, " f"{pages_extracted} pages"
        )
        return {
            "job_id": job_id,
            "chunks_created": all_chunks_count,
            "pages_processed": pages_extracted,
        }

    finally:
        if tmp_path:
            Path(tmp_path).unlink(missing_ok=True)
            logger.debug(f"[{job_id}] Temp file removed: {tmp_path}")
        publisher.close()


@celery.task(
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    name="tasks.process_pdf",
)
def process_pdf_task(
    self: Task,
    job_id: str,
    filename: str,
    minio_object_key: str
) -> dict:

    publisher = ProgressPublisher()

    try:
        loop = get_event_loop()
        future = asyncio.run_coroutine_threadsafe(
            _run_pipeline(job_id, filename, minio_object_key, publisher),
            loop
        )

        return future.result()

    except Exception as exc:
        logger.error(f"[{job_id}] Pipeline failed: {exc}\n{traceback.format_exc()}")
        error_msg = str(exc)

        retries_left = self.max_retries - self.request.retries

        if retries_left > 0:
            _update_job_sync(
                job_id,
                current_stage="retrying",
                error_message=error_msg
            )

            publisher.publish(
                job_id,
                stage="retrying",
                progress=0,
                status="processing",
                message=(
                    f"Ingestion failed, retrying "
                    f"({self.request.retries + 1}/{self.max_retries}): {error_msg}"
                )
            )
        else:
            _update_job_sync(
                job_id,
                status=JobStatus.FAILED.value,
                current_stage="failed",
                error_message=error_msg,
                completed_at=datetime.now(UTC).isoformat()
            )

            publisher.publish(
                job_id,
                stage="failed",
                progress=0,
                status="failed",
                message=f"Ingestion failed: {error_msg}"
            )

        raise self.retry(exc=exc)


    finally:
        publisher.close()