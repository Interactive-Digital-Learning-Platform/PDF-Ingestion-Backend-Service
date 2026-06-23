from celery import Celery
from app.core.config import REDIS_BROKER_URL, REDIS_BACKEND_URL

celery = Celery("pdf_ingestion", broker=REDIS_BROKER_URL, backend=REDIS_BACKEND_URL)

celery.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    result_expires=86_400,
    worker_prefetch_multiplier=1,
    task_max_retries=3,
    task_default_retry_delay=60,
    task_routes={
        "tasks.process_pdf": {"queue": "ingestion"},
    },
)

celery.autodiscover_tasks(["app.workers"])
