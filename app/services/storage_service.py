import logging
import tempfile
from pathlib import Path
from typing import AsyncIterator, Optional

import aioboto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from fastapi import Request

from app.core.config import settings

logger = logging.getLogger(__name__)


class StorageService:
    def __init__(
        self,
        bucket: str = settings.MINIO_BUCKET,
        secure: bool = False,
    ):
        self.bucket = bucket
        self._secure = secure
        self._session = aioboto3.Session()
        self._client_ctx = None
        self._client = None
        

    async def open(self) -> None:

        scheme = "https" if self._secure else "http"
        endpoint_url = f"{scheme}://{settings.MINIO_ENDPOINT}"

        boto_config = BotoConfig(
            s3={"addressing_style" : "path"},
            retries={"max_attempts" : 3, "mode": "standard"},
            connect_timeout=5,
            read_timeout=30,
            max_pool_connections=5
        )

        self._client_ctx = self._session.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=settings.MINIO_ACCESS_KEY,
            aws_secret_access_key=settings.MINIO_SECRET_KEY,
            region_name="us-east-1",
            config=boto_config
        )

        self._client = await self._client_ctx.__aenter__()

        logger.info(
            f"aioboto3 S3 client opened — endpoint={endpoint_url}, bucket={self.bucket}"
        )

    async def close(self) -> None:
        if self._client_ctx is not None:
            await self._client_ctx.__aexit__(None, None, None)
            logger.info("aioboto3 S3 client closed")


    @property
    def client(self):
        if self._client is None:
            raise RuntimeError(
                 "StorageService.open() was not called — no active S3 client. "
            )

        return self._client

        
    async def ensure_bucket(self) -> None:
        try:
            await self.client.head_bucket(Bucket=self.bucket)
            logger.info(f"Bucket '{self.bucket}' already exists")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")

            if code in ("404", "NoSuchBucket"):
                await self.client.create_bucket(Bucket=self.bucket)
                logger.info(f"Bucket '{self.bucket}' created")
            else:
                raise

    @staticmethod
    def make_object_key(job_id: str, filename: str) -> str:

        safe_filename = Path(filename).name
        return f"{job_id}/{safe_filename}"
        

    async def upload_stream(
        self,
        object_key: str,
        chunks:AsyncIterator[bytes],
        content_type: str = "application/pdf",
        max_bytes: Optional[int] = None
    ) -> int:

        buffer = bytearray()
        total = 0

        chunk_iter = chunks.__aiter__()

        async def _next_chunk():
            try:
                return await chunk_iter.__anext__()

            except StopAsyncIteration:
                return None

        def _check_limit():
            if max_bytes is not None and total > max_bytes:
                raise ValueError(f"Object exceeds max allowed size of {max_bytes} bytes")

        first = await _next_chunk()
        if first is None:
            await self.client.put_object(Bucket=self.bucket, Key=object_key, Body=b"", ContentType=content_type)
            return 0


        buffer.extend(first)
        total += len(first)
        _check_limit()

        second = await _next_chunk()
        if second is None:
            await self.client.put_object(
                Bucket=self.bucket, Key=object_key, Body=bytes(buffer), ContentType=content_type
            )
            logger.info(f"Uploaded '{object_key}' via single PutObject ({total} bytes)")
            return total

        buffer.extend(first)
        total += len(first)
        _check_limit()


        mpu = await self.client.create_multipart_upload(
            Bucket=self.bucket, Key=object_key, ContentType=content_type
        )
        upload_id = mpu["UploadId"]
        parts = []
        part_number = 1

        try:
            while True:
                chunk = await _next_chunk()
                if chunk is None:
                    break

                buffer.extend(chunk)
                total += len(chunk)
                _check_limit()

                while len(buffer) >= settings.MULTIPART_CHUNK_SIZE:
                    part_bytes = bytes(buffer[:settings.MULTIPART_CHUNK_SIZE])
                    del buffer[:settings.MULTIPART_CHUNK_SIZE]

                    resp = await self.client.upload_part(
                        Bucket=self.bucket,
                        Key=object_key,
                        UploadId=upload_id,
                        PartNumber=part_number,
                        Body=part_bytes
                    )
                    parts.append({"ETag": resp["ETag"], "PartNumber": part_number})
                    part_number += 1

            if buffer:
                resp = await self.client.upload_part(
                    Bucket=self.bucket,
                    Key=object_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=bytes(buffer)
                )
                parts.append({"ETag": resp["ETag"], "PartNumber": part_number})

            await self.client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts}
            )
            logger.info(f"Uploaded '{object_key}' via multipart ({total} bytes, {len(parts)} parts)")
            return total

        except Exception as e:
            logger.error(f"Aborting multipart upload for '{object_key}' after failure: {e}")
            await self.client.abort_multipart_upload(
                Bucket=self.bucket,
                Key=object_key,
                UploadId=upload_id
            )
            raise

    async def download_to_temp(self, object_key: str) -> str:

        suffix = Path(object_key).suffix or ".pdf"
        tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)

        try:
            resp = await self.client.get_object(Bucket=self.bucket, Key=object_key)
            body = resp["Body"]

            async with body:
                while True:
                    chunk = await body.read(32 * 1024)

                    if not chunk:
                        break

                    tmp.write(chunk)

            tmp.flush()
            logger.info(f"Downloaded '{object_key}' → '{tmp.name}'")
            return tmp.name
        except ClientError as e:
            Path(tmp.name).unlink(missing_ok=True)
            raise RuntimeError(f"Failed to download '{object_key}' from storage: {e}") from e

        finally:
            tmp.close()
        

    async def delete(self, object_key: str) -> None:
        try:
            await self.client.delete_object(Bucket=self.bucket, Key=object_key)
            logger.info(f"Deleted '{object_key}'")

        except ClientError as e:
            logger.warning(f"Could not delete '{object_key}': {e}")

    async def exists(self, object_key: str) -> bool:
        try:
            await self.client.head_object(Bucket=self.bucket, Key=object_key)
            return True
        except ClientError:
            return False


def get_storage_service(request:Request) -> StorageService:
    return request.app.state.storage_service