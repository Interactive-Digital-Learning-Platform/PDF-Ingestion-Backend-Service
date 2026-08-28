import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base
from app.schemas.job_schema import JobStatus


class Job(Base):

    __tablename__ = "jobs"

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    user_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
    )

    filename: Mapped[str] = mapped_column(
        String(512),
        nullable=False,
    )

    minio_object_key: Mapped[str] = mapped_column(
        String(1024),
        nullable=False,
    )

    qdrant_collection: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        default="pdf_knowledge_base",
    )

    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default=JobStatus.QUEUED.value,
        index=True,
    )

    progress: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )

    current_stage: Mapped[str] = mapped_column(
        String(64),
        nullable=True,
    )

    chunks_created: Mapped[int] = mapped_column(
        Integer,
        nullable=True,
    )

    pages_processed: Mapped[int] = mapped_column(
        Integer,
        nullable=True,
    )

    error_message: Mapped[str] = mapped_column(
        Text,
        nullable=True,
    )

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    completed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    source_service: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        server_default="pdf-ingestion-web-application",
        index=True,
    )

    external_reference_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
    )

    caller_metadata: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
    )

    max_pages: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )

    callback_required: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )

    callback_status: Mapped[str | None] = mapped_column(
        String(20),
        nullable=True,
    )

    callback_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )

    callback_last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    callback_delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    error_code: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
    )

    def __repr__(self):
        return (
            f"<Job job_id={self.job_id} user_id={self.user_id} "
            f"filename={self.filename} status={self.status}>"
        )

    def to_dict(self) -> dict:
        return {
            "job_id": str(self.job_id),
            "user_id": self.user_id,
            "filename": self.filename,
            "minio_object_key": self.minio_object_key,
            "qdrant_collection": self.qdrant_collection,
            "status": self.status,
            "progress": self.progress,
            "current_stage": self.current_stage,
            "chunks_created": self.chunks_created,
            "pages_processed": self.pages_processed,
            "error_message": self.error_message,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
            "source_service": self.source_service,
            "external_reference_id": self.external_reference_id,
            "caller_metadata": self.caller_metadata,
            "max_pages": self.max_pages,
            "callback_required": self.callback_required,
            "callback_status": self.callback_status,
            "callback_attempts": self.callback_attempts,
            "callback_last_error": self.callback_last_error,
            "callback_delivered_at": (
                self.callback_delivered_at.isoformat() if self.callback_delivered_at else None
            ),
            "error_code": self.error_code,
        }
