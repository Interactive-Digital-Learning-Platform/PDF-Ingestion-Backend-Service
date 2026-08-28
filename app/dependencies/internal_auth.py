import secrets

from fastapi import Header, HTTPException

from app.core.config import settings


async def require_internal_service(
    x_internal_key: str | None = Header(default=None, alias="X-Internal-Key"),
) -> None:
    if x_internal_key is None or not secrets.compare_digest(
        x_internal_key, settings.INTERNAL_SERVICE_KEY
    ):
        
        raise HTTPException(status_code=401, detail="Invalid or missing internal service key")
