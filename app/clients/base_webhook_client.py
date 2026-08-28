import logging

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)


class WebhookDeliveryError(Exception):
    pass

class BaseWebhookClient:
    
    base_url: str
    path: str

    async def notify_job_terminal(self, http_client: httpx.AsyncClient, payload: dict) -> None:
        url = f"{self.base_url.rstrip('/')}{self.path}"

        logger.info(
            "Delivering webhook — job_id=%s status=%s url=%s",
            payload.get("job_id"), payload.get("status"), url,
        )

        try:
            response = await http_client.post(
                url,
                json=payload,
                headers={"X-Internal-Key": settings.INTERNAL_SERVICE_KEY},
                timeout=settings.WEBHOOK_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning(
                "Webhook delivery failed — job_id=%s url=%s: %s",
                payload.get("job_id"), url, e,
            )
            raise WebhookDeliveryError(str(e)) from e

        logger.info("Webhook delivered — job_id=%s url=%s", payload.get("job_id"), url)
