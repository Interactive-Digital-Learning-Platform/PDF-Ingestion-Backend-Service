###############################################################################
# PDF Ingestion Backend Service
#
# Multi-stage image intended for:
#   - build/push:  az acr build (or docker build) -> Azure Container Registry
#   - deploy:      Azure Container Apps
#
# One image, two workloads (override the command per Container App):
#   API     : uvicorn app.main:app --host 0.0.0.0 --port 8001   (default CMD)
#   Worker  : celery -A app.workers.celery_app worker \
#                    --loglevel=info --concurrency=1 -Q ingestion,webhooks
#
# Builds with the classic Docker builder (no BuildKit features used), so it
# works as-is with `az acr build`.
###############################################################################

ARG PYTHON_VERSION=3.13

###############################################################################
# Stage 1 — builder: resolve and install dependencies into a self-contained venv
###############################################################################
FROM ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm-slim AS builder

# - bytecode-compile installed packages for faster cold starts
# - copy (don't hardlink) from the uv cache so the venv is relocatable
# - never let uv fetch its own Python; use the interpreter in this image
# - raise the HTTP timeout so the ~200 MB torch CPU wheel doesn't hit the 30s default
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    UV_HTTP_TIMEOUT=180

WORKDIR /app

# Dependency manifests only — this layer is rebuilt only when they change.
COPY pyproject.toml uv.lock ./

# Install third-party dependencies into /app/.venv (project itself not installed).
RUN uv sync --frozen --no-dev --no-install-project

###############################################################################
# Stage 2 — runtime: slim image with only what's needed to run the service
###############################################################################
FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime

# System packages:
#   tesseract-ocr(+eng) : OCR fallback in app/pipeline/extractor.py (pytesseract)
#   libgomp1            : OpenMP runtime for onnxruntime / torch (fastembed,
#                         sentence-transformers) wheels
#   curl               : container HEALTHCHECK
RUN apt-get update && apt-get install -y --no-install-recommends \
        tesseract-ocr \
        tesseract-ocr-eng \
        libgomp1 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Run as an unprivileged user
RUN groupadd --system app && useradd --system --gid app --home-dir /app app

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Dependencies (built in stage 1) then application code — most-volatile last.
COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --chown=app:app app ./app
COPY --chown=app:app alembic ./alembic
COPY --chown=app:app alembic.ini ./alembic.ini
COPY --chown=app:app qdrant-migrations ./qdrant-migrations

USER app

EXPOSE 8001

# Hits the "/" liveness ping in app/main.py.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -fsS http://localhost:8001/ || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]

###############################################################################
# NOTE — image size / CUDA
#
# torch is pinned to the CPU-only wheel via [tool.uv.sources] in pyproject.toml
# (index "pytorch-cpu" -> https://download.pytorch.org/whl/cpu). That keeps the
# ~6-8 GB of nvidia-* CUDA packages out of the image; Azure Container Apps has
# no GPU. If you ever remove that pin, this image balloons accordingly.
###############################################################################
