import asyncio
import logging
import time
from typing import List

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchValue,
    PointStruct,
    VectorParams,
    FilterSelector,
)

from app.pipeline.chunker import Chunk
from app.core.config import DEFAULT_COLLECTION, UPSERT_BATCH_SIZE, QDRANT_DB_URL

logger = logging.getLogger(__name__)


class VectorIndexer:

    def __init__(
        self,
        url: str = QDRANT_DB_URL,
        collection_name: str = DEFAULT_COLLECTION,
        api_key: str | None = None,
    ):
        self.collection_name = collection_name
        self.client = AsyncQdrantClient(url=url, api_key=api_key)
        logger.info(
            f"VectorIndexer initialised — " f"collection={collection_name}, url={url}"
        )

    async def ensure_collection(self, vector_dim: int = 384) -> None:

        existing = await self.client.get_collections()
        names = [c.name for c in existing.collections]

        if self.collection_name in names:
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
        user_id: str = "",
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

        logger.info(
            f"Indexing {len(chunks)} chunks into '{self.collection_name}' "
            f"(user={user_id}, job={job_id})"
        )
        t0 = time.perf_counter()

        points = [
            PointStruct(
                id=chunks[i].chunk_id,
                vector=embeddings[i],
                payload={
                    "text": chunks[i].text,
                    "user_id": user_id,
                    "job_id": job_id,
                    **chunks[i].metadata,
                },
            )
            for i in range(len(chunks))
        ]

        total_upserted = 0
        batches = [
            points[i : i + UPSERT_BATCH_SIZE]
            for i in range(0, len(points), UPSERT_BATCH_SIZE)
        ]

        for batch_idx, batch in enumerate(batches):
            await self._upsert_with_retry(batch, batch_idx, len(batches))
            total_upserted += len(batch)
            logger.debug(
                f"  Batch {batch_idx + 1}/{len(batches)} — "
                f"{len(batch)} points upserted"
            )

        elapsed = time.perf_counter() - t0
        logger.info(f"Indexing complete — {total_upserted} points in {elapsed:.2f}s")
        return total_upserted

    async def delete_document(self, filename: str, user_id: str = "") -> int:

        must_conditions = [
            FieldCondition(key="filename", match=MatchValue(value=filename))
        ]
        if user_id:
            must_conditions.append(
                FieldCondition(key="user_id", match=MatchValue(value=user_id))
            )

        count_filter = Filter(must=must_conditions)

        count_result = await self.client.count(
            collection_name=self.collection_name,
            count_filter=count_filter,
            exact=True,
        )
        count = count_result.count

        if count == 0:
            logger.info(f"No points found for filename='{filename}'")
            return 0

        await self.client.delete(
            collection_name=self.collection_name,
            points_selector=FilterSelector(filter=count_filter),
        )

        logger.info(
            f"Deleted {count} points for filename='{filename}' " f"user='{user_id}'"
        )
        return count

    async def collection_info(self) -> dict:

        info = await self.client.get_collection(self.collection_name)
        return {
            "collection": self.collection_name,
            "total_points": info.points_count,
            "vector_dim": info.config.params.vectors.size,
            "distance": str(info.config.params.vectors.distance),
            "status": str(info.status),
        }

    async def _upsert_with_retry(
        self,
        batch: List[PointStruct],
        batch_idx: int,
        total_batches: int,
        max_retries: int = 3,
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
                delay = 1.0 * (2 ** (attempt - 1))
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
