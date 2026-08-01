import asyncio
import logging
import threading

import redis
from celery.signals import worker_process_init, worker_process_shutdown
from sqlalchemy import create_engine

from app.core.config import settings
from app.pipeline.chunker import HierarchicalChunker
from app.pipeline.embedder import EmbeddingGenerator
from app.pipeline.indexer import VectorIndexer

logger = logging.getLogger(__name__)


_resources: dict = {}

_loop: asyncio.AbstractEventLoop | None = None
_loop_thread: threading.Thread | None = None
_loop_ready = threading.Event()


def _sync_db_url() -> str:
    return settings.DB_URL.replace(
        "postgresql+asyncpg://",
        "postgresql+psycopg://",
        1,
    )


def _run_event_loop_forever() -> None:
    global _loop

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _loop = loop
    _loop_ready.set()
    loop.run_forever()


def get_event_loop() -> asyncio.AbstractEventLoop:
    if _loop is None:
        raise RuntimeError(
             "Worker event loop not started — worker_process_init hasn't run yet"
        )

    return _loop


@worker_process_init.connect
def init_worker_resources(**kwargs) -> None:
        
    logger.info("Worker process starting — initializing per-process resources...")

    _resources["embedder"] = EmbeddingGenerator()
    _resources["chunker"] = HierarchicalChunker(
        chunk_size=settings.CHUNK_SIZE, chunk_overlap=settings.CHUNK_OVERLAP
    )
    _resources["db_engine"] = create_engine(
        _sync_db_url(), pool_pre_ping=True, pool_size=settings.DB_POOL_SIZE, max_overflow=settings.DB_MAX_OVERFLOW
    )
    _resources["redis_client"] = redis.from_url(settings.REDIS_BROKER_URL, decode_responses=True)


    global _loop_thread
    _loop_thread = threading.Thread(
        target=_run_event_loop_forever, daemon=True, name="worker-event-loop"
    )

    _loop_thread.start()
    if not _loop_ready.wait(timeout=10):
        raise RuntimeError("Worker event loop failed to start within 10s")


    indexer = VectorIndexer(
        url=settings.QDRANT_DB_URL, collection_name=settings.DEFAULT_COLLECTION
    )

    fut = asyncio.run_coroutine_threadsafe(
        indexer.ensure_collection(_resources["embedder"].embedding_dimension()),
        _loop
    )

    fut.result()
    _resources["indexer"] = indexer

    logger.info("Worker resources ready")


@worker_process_shutdown.connect
def shutdown_worker_resources(**kwargs) -> None:
    global _loop, _loop_thread

    indexer = _resources.pop("indexer", None)
    if indexer is not None and _loop is not None:
        try:
            fut = asyncio.run_coroutine_threadsafe(indexer.close(), _loop)
            fut.result(timeout=5)

        except Exception:
            logger.warning("Failed to close VectorIndexer cleanly on shutdown")


    if _loop is not None:
        _loop.call_soon_threadsafe(_loop.stop)

    if _loop_thread is not None:
        _loop_thread.join(timeout=5)

    _loop = None
    _loop_thread = None

    engine = _resources.pop("db_engine", None)
    if engine is not None:
        engine.dispose()

    redis_client = _resources.pop("redis_client", None)
    if redis_client is not None:
        redis_client.close()

    _resources.pop("embedder", None)
    _resources.pop("chunker", None)
    logger.info("Worker resources released")


def get_embedder() -> EmbeddingGenerator:
    if "embedder" not in _resources:
        logger.warning(
            "EmbeddingGenerator requested before worker_process_init ran"
        )
        _resources["embedder"] = EmbeddingGenerator()

    return _resources["embedder"]


def get_chunker() -> HierarchicalChunker:
    if "chunker" not in _resources:
        _resources["chunker"] = HierarchicalChunker(
            chunk_size=settings.CHUNK_SIZE, chunk_overlap=settings.CHUNK_OVERLAP
        )

    return _resources["chunker"]


def get_db_engine():
    if "db_engine" not in _resources:
        _resources["db_engine"] = create_engine(
            _sync_db_url(), pool_pre_ping=True, pool_size=settings.DB_POOL_SIZE, max_overflow=settings.DB_MAX_OVERFLOW
        )

    return _resources["db_engine"]


def get_redis_client():
    if "redis_client" not in _resources:
        _resources["redis_client"] = redis.from_url(settings.REDIS_BROKER_URL, decode_responses=True)

    return _resources["redis_client"]


def get_indexer() -> VectorIndexer:
    if "indexer" not in _resources:
        logger.warning(
            "VectorIndexer requested before worker_process_init ran — "
        )
        indexer = VectorIndexer(
            url=settings.QDRANT_DB_URL, collection_name=settings.DEFAULT_COLLECTION
        )
        loop = get_event_loop()
        fut = asyncio.run_coroutine_threadsafe(
            indexer.ensure_collection(get_embedder().embedding_dimension()),
            loop
        )
        fut.result()
        _resources["indexer"] = indexer

    return _resources["indexer"]