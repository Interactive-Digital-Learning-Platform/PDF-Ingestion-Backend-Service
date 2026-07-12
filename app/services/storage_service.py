import asyncio
import logging
import tempfile
from pathlib import Path
from typing import BinaryIO

from minio import Minio
from minio.error import S3Error

from app.core.config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)

logger = logging.getLogger(__name__)


class StorageService:
    def __init__(
        self,
        endpoint: str = MINIO_ENDPOINT,
        access_key: str = MINIO_ACCESS_KEY,
        secret_key: str = MINIO_SECRET_KEY,
        bucket: str = MINIO_BUCKET,
        secure: bool = False,
    ):
        self.bucket = bucket
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        logger.info(
            f"StorageService initialised — endpoint={endpoint}, bucket={bucket}"
        )

    async def ensure_bucket(self) -> None:

        def _ensure():
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info(f"MinIO bucket '{self.bucket}' created")
            else:
                logger.info(f"MinIO bucket '{self.bucket}' already exists")

        await asyncio.to_thread(_ensure)

    @staticmethod
    def make_object_key(job_id: str, filename: str) -> str:

        safe_filename = Path(filename).name
        return f"{job_id}/{safe_filename}"

    async def upload(
        self,
        file_obj: BinaryIO,
        object_key: str,
        content_type: str = "application/pdf",
        file_size: int = -1,
    ) -> str:

        def _upload():
            self.client.put_object(
                bucket_name=self.bucket,
                object_name=object_key,
                data=file_obj,
                length=file_size,
                content_type=content_type,
            )
            logger.info(f"Uploaded '{object_key}' to MinIO bucket '{self.bucket}'")

        await asyncio.to_thread(_upload)
        return object_key

    async def download_to_temp(self, object_key: str) -> str:

        def _download():
            suffix = Path(object_key).suffix or ".pdf"
            tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
            try:
                response = self.client.get_object(self.bucket, object_key)
                for chunk in response.stream(32 * 1024):
                    tmp.write(chunk)
                tmp.flush()
                logger.info(f"Downloaded '{object_key}' → '{tmp.name}'")
                return tmp.name
            except S3Error as e:
                tmp.close()
                Path(tmp.name).unlink(missing_ok=True)
                raise RuntimeError(f"Failed to download '{object_key}' from MinIO: {e}")
            finally:
                response.close()
                response.release_conn()
                tmp.close()

        return await asyncio.to_thread(_download)

    async def delete(self, object_key: str) -> None:

        def _delete():
            try:
                self.client.remove_object(self.bucket, object_key)
                logger.info(f"Deleted '{object_key}' from MinIO")
            except S3Error as e:
                logger.warning(f"Could not delete '{object_key}': {e}")

        await asyncio.to_thread(_delete)

    async def exists(self, object_key: str) -> bool:
        """Check whether an object exists in the bucket."""

        def _exists():
            try:
                self.client.stat_object(self.bucket, object_key)
                return True
            except S3Error:
                return False

        return await asyncio.to_thread(_exists)


def get_storage_service() -> StorageService:
    return StorageService(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket=MINIO_BUCKET,
    )
