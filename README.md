# PDF Ingestion Backend Service

## Setup

Requirements: Python 3.13, [uv](https://docs.astral.sh/uv/), Docker, a
PostgreSQL database, and Tesseract OCR.

Start Redis, Qdrant, MinIO, and the API gateway from the repository root:

```bash
docker compose -f deployment-infrastructure/docker-compose.yaml up -d
```

Install the service and create its environment file:

```bash
cd PDF-Ingestion-Backend-Service
uv sync
cp .env.example .env
```

Fill in `.env`, including the following host-run service values and your real
database/API credentials:

```env
DB_URL=postgresql+asyncpg://USER:PASSWORD@HOST:5432/DATABASE
OPENAI_API_KEY=YOUR_KEY
QDRANT_DB_URL=http://localhost:6333
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=pdf-documents
REDIS_BROKER_URL=redis://localhost:6379/0
REDIS_BACKEND_URL=redis://localhost:6379/1
ALLOWED_CONTENT_TYPES=["application/pdf"]
INTERNAL_SERVICE_KEY=YOUR_SHARED_SECRET
AI_LEARNING_ASSISTANT_BASE_URL=http://localhost:8002
```

`INTERNAL_SERVICE_KEY` must be the exact same value configured in
`AI-Learning-Assistant-Service`'s own `.env` — see
`dev-docs/generic-pdf-processing-service.md` §7.

Apply Postgres migrations:

```bash
uv run alembic upgrade head
```

Apply Qdrant migrations — collections are never created automatically by the API or worker (see
`dev-docs/generic-pdf-processing-service.md` §9), so this must be run before any job can target
a given collection:

```bash
uv run python qdrant-migrations/run_migrations.py upgrade
```

Start the API on the port expected by Nginx:

```bash
uv run uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

In a second terminal, start the Celery worker. It must consume both queues — `ingestion` (PDF
processing) and `webhooks` (terminal-state callback delivery, see
`dev-docs/generic-pdf-processing-service.md` §12) — or jobs from one of them will never be picked
up:

```bash
uv run celery -A app.workers.celery_app worker   --loglevel=info   --concurrency=1   -Q ingestion,webhooks
```

Or in Windows:
uv run celery -A app.workers.celery_app worker `
  --loglevel=info `
  --concurrency=1 `
  --pool=solo `
  -Q ingestion,webhooks


The API is then available directly at `http://localhost:8001` and through the
gateway at `http://localhost:8080/api/pdf/`.
