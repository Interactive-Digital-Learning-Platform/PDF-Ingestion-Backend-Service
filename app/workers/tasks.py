import asyncio
import json
import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path

import redis
from celery import Task

from app.workers.celery_app import celery
from app.pipeline.extractor import PDFExtractor
from app.pipeline.chunker import HierarchicalChunker
from app.pipeline.embedder import EmbeddingGenerator
from app.pipeline.indexer import VectorIndexer
from app.services.storage_service import StorageService
from app.schemas.job_schema import JobStatus
from app.core.config import (
    REDIS_BROKER_URL,
    DB_URL,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    QDRANT_DB_URL,
    DEFAULT_COLLECTION,
    CHUNK_OVERLAP,
    CHUNK_SIZE,
    PAGE_BATCH_SIZE,
    PAGE_OVERLAP,
)

logger = logging.getLogger(__name__)


class ProgressPublisher:

    def __init__(self):
        self._redis = redis.from_url(REDIS_BROKER_URL, decode_responses=True)

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
        self._redis.close()


def _update_job_sync(job_id: str, **fields) -> None:

    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    sync_url = DB_URL.replace("+asyncpg", "")

    engine = create_engine(sync_url, pool_pre_ping=True)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        set_clauses = ", ".join(f"{k} = :{k}" for k in fields)
        params = {"job_id": job_id, **fields}
        session.execute(
            text(f"UPDATE jobs SET {set_clauses} WHERE job_id = :job_id"),
            params,
        )
        session.commit()

    engine.dispose()


def _process_batch(page_buffer, chunker, embedder):

    chunks = chunker.chunk_pages(page_buffer)
    embeddings = embedder.embed_chunks(chunks)
    return chunks, embeddings


@celery.task(
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    name="tasks.process_pdf",
)
def process_pdf_task(
    self: Task,
    job_id: str,
    user_id: str,
    filename: str,
    minio_object_key: str,
) -> dict:

    publisher = ProgressPublisher()
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

        storage = StorageService(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            bucket=MINIO_BUCKET,
        )
        tmp_path = asyncio.run(storage.download_to_temp(minio_object_key))
        logger.info(f"[{job_id}] PDF downloaded to {tmp_path}")

        publisher.publish(
            job_id,
            stage="extracting",
            progress=5,
            message="Starting text extraction",
        )

        extractor = PDFExtractor(min_chars=80)
        chunker = HierarchicalChunker(
            chunk_size=CHUNK_SIZE,
            chunk_overlap=CHUNK_OVERLAP,
        )
        embedder = EmbeddingGenerator()
        indexer = VectorIndexer(
            url=QDRANT_DB_URL,
            collection_name=DEFAULT_COLLECTION,
        )

        asyncio.run(
            indexer.ensure_collection(vector_dim=embedder.embedding_dimension())
        )

        pdf_info = extractor.inspect(tmp_path)
        total_pages = pdf_info["total_pages"]
        logger.info(f"[{job_id}] {filename}: {total_pages} pages")

        all_chunks_count = 0
        pages_processed = 0
        page_buffer: list = []

        for page in extractor.extract(tmp_path):
            page_buffer.append(page)

            if len(page_buffer) >= PAGE_BATCH_SIZE:
                batch_chunks, batch_embeddings = _process_batch(
                    page_buffer, chunker, embedder
                )

                asyncio.run(
                    indexer.index_chunks(
                        batch_chunks,
                        batch_embeddings,
                        user_id=user_id,
                        job_id=job_id,
                    )
                )

                pages_processed += len(page_buffer) - PAGE_OVERLAP
                all_chunks_count += len(batch_chunks)
                page_buffer = page_buffer[-PAGE_OVERLAP:]

                pct = 5 + int((pages_processed / total_pages) * 85)
                pct = min(pct, 90)

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
                        f"Processed {pages_processed}/{total_pages} pages — "
                        f"{all_chunks_count} chunks indexed"
                    ),
                )

        if page_buffer:
            batch_chunks, batch_embeddings = _process_batch(
                page_buffer, chunker, embedder
            )
            asyncio.run(
                indexer.index_chunks(
                    batch_chunks,
                    batch_embeddings,
                    user_id=user_id,
                    job_id=job_id,
                )
            )
            pages_processed += len(page_buffer)
            all_chunks_count += len(batch_chunks)

        _update_job_sync(
            job_id,
            status=JobStatus.DONE.value,
            progress=100,
            current_stage="done",
            chunks_created=all_chunks_count,
            pages_processed=pages_processed,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
        publisher.publish(
            job_id,
            stage="done",
            progress=100,
            status="done",
            message=(
                f"Ingestion complete — {all_chunks_count} chunks from "
                f"{pages_processed} pages"
            ),
            chunks_created=all_chunks_count,
            pages_processed=pages_processed,
        )

        logger.info(
            f"[{job_id}] Done — {all_chunks_count} chunks, " f"{pages_processed} pages"
        )
        return {
            "job_id": job_id,
            "chunks_created": all_chunks_count,
            "pages_processed": pages_processed,
        }

    except Exception as exc:
        logger.error(f"[{job_id}] Pipeline failed: {exc}\n{traceback.format_exc()}")

        error_msg = str(exc)

        _update_job_sync(
            job_id,
            status=JobStatus.FAILED.value,
            current_stage="failed",
            error_message=error_msg,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
        publisher.publish(
            job_id,
            stage="failed",
            progress=0,
            status="failed",
            message=f"Ingestion failed: {error_msg}",
        )

        raise self.retry(exc=exc)

    finally:
        if tmp_path:
            Path(tmp_path).unlink(missing_ok=True)
            logger.debug(f"[{job_id}] Temp file removed: {tmp_path}")
        publisher.close()
