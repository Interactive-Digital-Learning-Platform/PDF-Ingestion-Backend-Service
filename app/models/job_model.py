from app.core.database import Base
from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID
from app.schemas.job_schema import JobStatus
from datetime import datetime, timezone
import uuid


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
        }
