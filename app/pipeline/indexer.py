import asyncio
import logging
import time
import uuid
from typing import List

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import (
    Condition,
    Distance,
    FieldCondition,
    Filter,
    FilterSelector,
    MatchValue,
    PointStruct,
    VectorParams,
)

from app.constants.namespaces import _QDRANT_POINT_ID_NAMESPACE_VALUE
from app.core.config import settings
from app.pipeline.chunker import Chunk

logger = logging.getLogger(__name__)

def _deterministic_point_id(job_id: str, chunk_text: str) -> str:
    return str(uuid.uuid5(_QDRANT_POINT_ID_NAMESPACE_VALUE, f"{job_id}:{chunk_text}"))


    
class VectorIndexer:
    def __init__(
        self,
        url: str = settings.QDRANT_DB_URL or "",
        collection_name: str = settings.DEFAULT_COLLECTION,
        api_key: str | None = None,
    ):
        self.collection_name = collection_name
        self.client = AsyncQdrantClient(url=url, api_key=api_key)
        logger.info(
            f"VectorIndexer initialised — collection={collection_name}, url={url}"
        )

    async def ensure_collection(self, vector_dim: int = settings.EMBEDDING_DIM) -> None:

        existing = await self.client.get_collections()
        names = [c.name for c in existing.collections]

        if self.collection_name in names:
            info = await self.client.get_collection(self.collection_name)
            vectors_config = info.config.params.vectors

            if vectors_config is None:
                raise RuntimeError(
                    f"Collection '{self.collection_name}' has no vector config."
                )

            existing_dim = (
                next(iter(vectors_config.values())).size
                if isinstance(vectors_config, dict)
                else vectors_config.size
            )

            if existing_dim != vector_dim:
                raise RuntimeError(
                    f"Collection '{self.collection_name}' already exists with "
                    f"dim={existing_dim}, but current model requires dim={vector_dim}. "
                    f"Drop the collection or update EMBEDDING_DIM in .env."
                )
            logger.info(
                f"Collection '{self.collection_name}' exists — skipping creation"
            )
            return

        await self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(
                size=vector_dim,
                distance=Distance.COSINE,
            ),
        )
        logger.info(
            f"Collection '{self.collection_name}' created "
            f"(dim={vector_dim}, distance=COSINE)"
        )

    async def index_chunks(
        self,
        chunks: List[Chunk],
        embeddings: List[List[float]],
        job_id: str = "",
    ) -> int:

        if len(chunks) != len(embeddings):
            raise ValueError(
                f"chunks ({len(chunks)}) and embeddings ({len(embeddings)}) "
                f"must be the same length"
            )
        if not chunks:
            logger.warning("index_chunks called with empty list — nothing to do")
            return 0

        logger.info(f"Indexing {len(chunks)} chunks into '{self.collection_name}' ")
        t0 = time.perf_counter()

        points = [
            PointStruct(
                id=_deterministic_point_id(job_id, chunks[i].text),
                vector=embeddings[i],
                payload={
                    "text": chunks[i].text,
                    "job_id": job_id,
                    "original_chunk_id": chunks[i].chunk_id,
                    **chunks[i].metadata,
                },
            )
            for i in range(len(chunks))
        ]

        total_upserted = 0
        batches = [
            points[i : i + settings.UPSERT_BATCH_SIZE]
            for i in range(0, len(points), settings.UPSERT_BATCH_SIZE)
        ]

        for batch_idx, batch in enumerate(batches):
            await self._upsert_with_retry(batch, batch_idx, len(batches))
            total_upserted += len(batch)
            logger.debug(
                f"  Batch {batch_idx + 1}/{len(batches)} — {len(batch)} points upserted"
            )

        elapsed = time.perf_counter() - t0
        logger.info(f"Indexing complete — {total_upserted} points in {elapsed:.2f}s")
        return total_upserted

    async def delete_document(self, job_id: str) -> int:

        must_conditions: List[Condition] = [
            FieldCondition(key="job_id", match=MatchValue(value=job_id))
        ]

        count_filter = Filter(must=must_conditions)

        count_result = await self.client.count(
            collection_name=self.collection_name,
            count_filter=count_filter,
            exact=True,
        )
        count = count_result.count

        if count == 0:
            logger.info(f"No points found for job_id='{job_id}'")
            return 0

        await self.client.delete(
            collection_name=self.collection_name,
            points_selector=FilterSelector(filter=count_filter),
        )

        logger.info(f"Deleted {count} points for filename='{job_id}'")
        return count

    async def collection_info(self) -> dict:

        info = await self.client.get_collection(self.collection_name)

        vectors_config = info.config.params.vectors

        if vectors_config is None:
            raise RuntimeError(
                f"Collection '{self.collection_name}' has no vector config."
            )

        if isinstance(vectors_config, dict):
            first = next(iter(vectors_config.values()))
            dim = first.size
            distance = str(first.distance)
        else:
            dim = vectors_config.size
            distance = str(vectors_config.distance)

        return {
            "collection": self.collection_name,
            "total_points": info.points_count,
            "vector_dim": dim,
            "distance": distance,
            "status": str(info.status),
        }

    async def _upsert_with_retry(
        self,
        batch: List[PointStruct],
        batch_idx: int,
        total_batches: int,
        max_retries: int = settings.MAX_RETRIES,
    ) -> None:
        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                await self.client.upsert(
                    collection_name=self.collection_name,
                    points=batch,
                    wait=True,
                )
                return
            except Exception as e:
                delay = settings.RETRY_BASE_DELAY * (2 ** (attempt - 1))
                logger.warning(
                    f"  Upsert batch {batch_idx + 1}/{total_batches} failed "
                    f"(attempt {attempt}/{max_retries}) — "
                    f"retrying in {delay:.1f}s: {e}"
                )
                last_error = e
                await asyncio.sleep(delay)

        raise RuntimeError(
            f"Upsert batch {batch_idx + 1} failed after {max_retries} retries. "
            f"Last error: {last_error}"
        )

async def close(self) -> None:
    await self.client.close()