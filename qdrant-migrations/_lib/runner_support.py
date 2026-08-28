"""Shared helpers for the manual Qdrant migration runner.

Deliberately outside `app/` (see dev-docs/generic-pdf-processing-service.md §9) so nothing in the
FastAPI or Celery process can import this at runtime — collection creation only ever happens
through `run_migrations.py`, run by hand.
"""

import datetime as dt
import importlib.util
import re
import sys
from pathlib import Path

from qdrant_client import QdrantClient
from sqlalchemy import create_engine, text

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent
_REPO_ROOT = MIGRATIONS_DIR.parent
_FILENAME_RE = re.compile(r"^(\d{3})_.*\.py$")

# So `import app.core.config` resolves when this is run as a standalone script
# (`uv run python qdrant-migrations/run_migrations.py`), not as part of the `app` package.
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.core.config import settings


def get_qdrant_client() -> QdrantClient:
    return QdrantClient(url=settings.QDRANT_DB_URL)


def _sync_db_url() -> str:
    # Same swap `app/workers/celery_worker_resources.py::_sync_db_url` already uses — this
    # runner needs a plain sync connection, same as the Celery worker does.
    return settings.DB_URL.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)


def get_pg_engine():
    return create_engine(_sync_db_url(), pool_pre_ping=True)


def discover_migrations() -> list[dict]:
    """Load every NNN_*.py file in qdrant-migrations/, sorted by revision number."""
    migrations = []

    for path in sorted(MIGRATIONS_DIR.glob("*.py")):
        match = _FILENAME_RE.match(path.name)
        if not match:
            continue

        spec = importlib.util.spec_from_file_location(path.stem, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        migrations.append(
            {
                "revision": module.REVISION,
                "description": module.DESCRIPTION,
                "upgrade": module.upgrade,
                "downgrade": module.downgrade,
                "path": path,
            }
        )

    migrations.sort(key=lambda m: m["revision"])
    return migrations


def get_applied_revisions(engine) -> set[str]:
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT revision FROM qdrant_migrations")).fetchall()
    except Exception as e:
        raise RuntimeError(
            "Could not read the qdrant_migrations tracking table — has "
            "`uv run alembic upgrade head` been run yet? "
            f"Original error: {e}"
        ) from e

    return {row[0] for row in rows}


def record_applied(engine, revision: str, description: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO qdrant_migrations (revision, description, applied_at) "
                "VALUES (:revision, :description, :applied_at)"
            ),
            {
                "revision": revision,
                "description": description,
                "applied_at": dt.datetime.now(dt.UTC),
            },
        )


def remove_applied(engine, revision: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM qdrant_migrations WHERE revision = :revision"),
            {"revision": revision},
        )
