import logging
import time

from fastembed import SparseTextEmbedding
from qdrant_client.models import SparseVector

from app.core.config import settings
from app.pipeline.chunker import Chunk

logger = logging.getLogger(__name__)


class SparseEmbeddingGenerator:
    def __init__(
        self,
        model_name: str = settings.SPARSE_MODEL,
        cache_dir: str | None = settings.FASTEMBED_CACHE_DIR,
        batch_size: int = settings.MAX_INPUTS_PER_BATCH,
    ):
        self.model_name = model_name
        self.batch_size = batch_size

        logger.info(f"Loading sparse model '{model_name}'...")
        self.model = SparseTextEmbedding(model_name=model_name, cache_dir=cache_dir)
        logger.info("Sparse model ready")

    def embed_chunks(self, chunks: list[Chunk]) -> list[SparseVector]:
        if not chunks:
            return []

        t0 = time.perf_counter()

        vectors = [
            SparseVector(indices=embedding.indices.tolist(), values=embedding.values.tolist())
            for embedding in self.model.embed(
                [chunk.contextual_text for chunk in chunks], batch_size=self.batch_size
            )
        ]

        if len(vectors) != len(chunks):
            raise RuntimeError(
                f"Sparse embedding count mismatch: got {len(vectors)}, expected {len(chunks)}"
            )

        logger.info(
            f"Sparse embedding complete — {len(vectors)} vectors in "
            f"{time.perf_counter() - t0:.2f}s"
        )

        return vectors
