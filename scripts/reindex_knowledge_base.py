import argparse
import importlib.util
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from qdrant_client import QdrantClient
from sqlalchemy import create_engine, text

from app.core.config import settings
from app.schemas.job_schema import JobStatus

_TEXTBOOK_METADATA_MIGRATION = (
    _REPO_ROOT / "qdrant-migrations" / "003_create_quiz_knowledge_base_collection.py"
)


def _textbook_metadata() -> dict[str, tuple[int, str]]:
    spec = importlib.util.spec_from_file_location("textbook_metadata", _TEXTBOOK_METADATA_MIGRATION)

    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {_TEXTBOOK_METADATA_MIGRATION}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module._FILENAME_METADATA


def _assert_hybrid_collection(collection: str) -> None:
    client = QdrantClient(url=settings.QDRANT_DB_URL, api_key=settings.QDRANT_API_KEY)

    try:
        if not client.collection_exists(collection):
            sys.exit(f"Target collection '{collection}' does not exist — run the Qdrant migrations first.")

        params = client.get_collection(collection).config.params
        vectors = params.vectors
        sparse_vectors = params.sparse_vectors or {}

        if not (
            isinstance(vectors, dict)
            and settings.DENSE_VECTOR_NAME in vectors
            and settings.SPARSE_VECTOR_NAME in sparse_vectors
        ):
            sys.exit(
                f"Target collection '{collection}' is not a hybrid collection "
                f"('{settings.DENSE_VECTOR_NAME}' + sparse '{settings.SPARSE_VECTOR_NAME}')."
            )
    finally:
        client.close()


def _sync_db_url() -> str:
    return settings.DB_URL.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Re-ingest every finished knowledge-base job into the hybrid collection."
    )
    parser.add_argument("--source", default="pdf_knowledge_base")
    parser.add_argument("--target", default="pdf_knowledge_base_v2")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    _assert_hybrid_collection(args.target)

    engine = create_engine(_sync_db_url(), pool_pre_ping=True)
    textbook_metadata = _textbook_metadata()

    with engine.connect() as conn:
        jobs = conn.execute(
            text(
                "SELECT job_id, filename, caller_metadata FROM jobs "
                "WHERE qdrant_collection = :source AND status = :done "
                "ORDER BY started_at"
            ),
            {"source": args.source, "done": JobStatus.DONE.value},
        ).mappings().all()

    print(f"{len(jobs)} finished job(s) in '{args.source}' → '{args.target}'")

    planned: list[tuple[str, dict]] = []

    for job in jobs:
        metadata = dict(job["caller_metadata"] or {})
        known = textbook_metadata.get(job["filename"])

        if known and "grade" not in metadata and "subject" not in metadata:
            metadata["grade"], metadata["subject"] = known

        planned.append((str(job["job_id"]), metadata))
        tags = f"grade={metadata.get('grade')} subject={metadata.get('subject')}"
        print(f"  {job['job_id']}  {job['filename']}  ({tags})")

    if args.dry_run or not planned:
        return

    from app.workers.tasks import process_pdf_task

    for job_id, metadata in planned:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE jobs SET qdrant_collection = :target, status = :queued, progress = 0, "
                    "current_stage = NULL, error_message = NULL, error_code = NULL, "
                    "chunks_created = NULL, pages_processed = NULL, completed_at = NULL, "
                    "caller_metadata = CAST(:metadata AS JSONB) "
                    "WHERE job_id = :job_id"
                ),
                {
                    "target": args.target,
                    "queued": JobStatus.QUEUED.value,
                    "metadata": json.dumps(metadata),
                    "job_id": job_id,
                },
            )

        process_pdf_task.delay(job_id=job_id)

    print(f"Queued {len(planned)} job(s). Track progress in the jobs table or the web app.")


if __name__ == "__main__":
    main()
