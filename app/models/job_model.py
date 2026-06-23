from app.core.database import Base
from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID
from app.schemas.job_schema import JobStatus
from datetime import datetime, timezone
import uuid


class Job(Base):

    __tablename__ = "jobs"

    job_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    user_id = Column(
        String(255),
        nullable=False,
        index=True,
    )

    filename = Column(
        String(512),
        nullable=False,
    )

    minio_object_key = Column(
        String(1024),
        nullable=False,
    )

    qdrant_collection = Column(
        String(255),
        nullable=False,
        default="pdf_knowledge_base",
    )

    status = Column(
        String(50),
        nullable=False,
        default=JobStatus.QUEUED.value,
        index=True,
    )

    progress = Column(
        Integer,
        nullable=False,
        default=0,
    )

    current_stage = Column(
        String(64),
        nullable=True,
    )

    chunks_created = Column(
        Integer,
        nullable=True,
    )

    pages_processed = Column(
        Integer,
        nullable=True,
    )

    error_message = Column(
        Text,
        nullable=True,
    )

    started_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    completed_at = Column(
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
