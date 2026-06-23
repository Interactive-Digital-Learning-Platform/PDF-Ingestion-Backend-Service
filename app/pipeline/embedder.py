import logging
import time
from typing import List

from sentence_transformers import SentenceTransformer

from app.pipeline.chunker import Chunk

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "BAAI/bge-small-en-v1.5"
EMBEDDING_DIM = 384
MAX_TOKENS    = 512
BATCH_SIZE    = 32


class EmbeddingGenerator:

    def __init__(
        self,
        model_name: str = DEFAULT_MODEL,
        device:     str = "cpu",
        batch_size: int = BATCH_SIZE,
    ):
        self.model_name = model_name
        self.batch_size = batch_size
        self.device     = device

        logger.info(
            f"Loading embedding model '{model_name}' on {device}... "
            f"(first run downloads ~130MB)"
        )

        self.model = SentenceTransformer(model_name, device=device)

        logger.info(
            f"Model ready — dim={self.embedding_dimension()}, "
            f"max_tokens={MAX_TOKENS}, device={device}"
        )

    def embed_chunks(self, chunks: List[Chunk]) -> List[List[float]]:
       
        if not chunks:
            return []

        logger.info(f"Embedding {len(chunks)} chunks locally with {self.model_name}")
        t0 = time.perf_counter()

        texts = self._prepare_texts(chunks)

        embeddings_np = self.model.encode(
            texts,
            batch_size           = self.batch_size,
            show_progress_bar    = False,
            normalize_embeddings = True,
            convert_to_numpy     = True,
        )

        embeddings = [emb.tolist() for emb in embeddings_np]

        elapsed = time.perf_counter() - t0
        logger.info(
            f"Embedding complete — {len(embeddings)} vectors in "
            f"{elapsed:.2f}s "
            f"({elapsed / len(chunks) * 1000:.1f}ms per chunk)"
        )

        assert len(embeddings) == len(chunks), (
            f"Embedding count mismatch: got {len(embeddings)}, "
            f"expected {len(chunks)}"
        )

        return embeddings

    def embed_single(self, text: str) -> List[float]:
        
        query_text = f"Represent this sentence for searching relevant passages: {text}"
        query_text = self._truncate_if_needed(query_text, label="query")

        embedding_np = self.model.encode(
            query_text,
            normalize_embeddings = True,
            convert_to_numpy     = True,
        )
        return embedding_np.tolist()

    def embedding_dimension(self) -> int:
        
        return self.model.get_embedding_dimension()

    def _prepare_texts(self, chunks: List[Chunk]) -> List[str]:
        texts = []
        for chunk in chunks:
            text = chunk.text.replace("\n", " ").strip()
            text = self._truncate_if_needed(
                text, label=f"chunk {chunk.metadata.get('chunk_index', '?')}"
            )
            texts.append(text)
        return texts

    def _truncate_if_needed(self, text: str, label: str = "text") -> str:
     
        estimated_tokens = int(len(text.split()) * 1.3)

        if estimated_tokens > MAX_TOKENS:
            max_words = int(MAX_TOKENS / 1.3)
            words     = text.split()[:max_words]
            text      = " ".join(words)
            logger.warning(
                f"  {label} exceeded ~{MAX_TOKENS} tokens "
                f"(est. {estimated_tokens}) — truncated to {max_words} words"
            )
        return text