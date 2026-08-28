import asyncio
import logging
import uuid
from datetime import UTC, datetime

from celery import Task
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from app.clients.registry import WEBHOOK_CLIENT_REGISTRY
from app.core.config import settings
from app.schemas.job_schema import CallbackStatus
from app.workers.celery_app import celery
from app.workers.celery_worker_resources import (
    get_db_engine,
    get_event_loop,
    get_http_client,
)

logger = logging.getLogger(__name__)

_WEBHOOK_EVENT_NAMESPACE = uuid.UUID("d3f9a1b2-4e5c-4a6d-9f1e-2b3c4d5e6f7a")


def _load_job_sync(job_id: str) -> dict:
    engine = get_db_engine()
    Session = sessionmaker(bind=engine)

    with Session() as session:
        row = session.execute(
            text("SELECT * FROM jobs WHERE job_id = :job_id"), {"job_id": job_id}
        ).mappings().first()

    if row is None:
        raise ValueError(f"Job {job_id} not found")

    return dict(row)


def _update_job_callback_sync(job_id: str, **fields) -> None:
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


def build_webhook_payload(job: dict) -> dict:
    job_id = str(job["job_id"])
    status = job["status"]
    event_id = str(uuid.uuid5(_WEBHOOK_EVENT_NAMESPACE, f"{job_id}:{status}"))

    payload = {
        "event_id": event_id,
        "job_id": job_id,
        "external_reference_id": job.get("external_reference_id"),
        "source_service": job.get("source_service"),
        "status": status,
    }

    if status == "done":
        completed_at = job.get("completed_at")
        payload.update(
            {
                "collection": job.get("qdrant_collection"),
                "chunks_created": job.get("chunks_created"),
                "pages_processed": job.get("pages_processed"),
                "completed_at": completed_at.isoformat() if hasattr(completed_at, "isoformat") else completed_at,
            }
        )
    else:
        payload["error"] = {
            "code": job.get("error_code"),
            "message": job.get("error_message"),
        }

    return payload


async def _deliver(job: dict) -> None:
    client = WEBHOOK_CLIENT_REGISTRY.get(job["source_service"])
    if client is None:
        raise RuntimeError(
            f"No registered webhook client for source_service='{job['source_service']}'"
        )

    payload = build_webhook_payload(job)
    http_client = get_http_client()
    await client.notify_job_terminal(http_client, payload)


@celery.task(
    bind=True,
    max_retries=settings.WEBHOOK_MAX_RETRIES,
    name="tasks.deliver_job_webhook",
)
def deliver_job_webhook(self: Task, job_id: str) -> None:
    job = _load_job_sync(job_id)

    try:
        loop = get_event_loop()
        future = asyncio.run_coroutine_threadsafe(_deliver(job), loop)
        future.result()

    except Exception as exc:
        logger.warning(f"[{job_id}] Webhook delivery attempt failed: {exc}")

        retries_left = self.max_retries - self.request.retries
        next_status = CallbackStatus.RETRYING.value if retries_left > 0 else CallbackStatus.FAILED.value

        _update_job_callback_sync(
            job_id,
            callback_attempts=job.get("callback_attempts", 0) + 1,
            callback_last_error=str(exc)[:1000],
            callback_status=next_status,
        )

        if retries_left > 0:
            backoff = settings.WEBHOOK_RETRY_BASE_DELAY * (2 ** self.request.retries)
            raise self.retry(exc=exc, countdown=backoff)

        logger.error(
            f"[{job_id}] Webhook delivery permanently failed after {self.max_retries} attempts "
            f"— job.status is unaffected, only callback_status."
        )
        return

    _update_job_callback_sync(
        job_id,
        callback_status=CallbackStatus.DELIVERED.value,
        callback_attempts=job.get("callback_attempts", 0) + 1,
        callback_delivered_at=datetime.now(UTC).isoformat(),
    )
    logger.info(f"[{job_id}] Webhook delivered")
