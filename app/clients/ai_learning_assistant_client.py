from app.clients.base_webhook_client import BaseWebhookClient
from app.core.config import settings


class AILearningAssistantClient(BaseWebhookClient):
    @property
    def base_url(self) -> str:
        return settings.AI_LEARNING_ASSISTANT_BASE_URL

    @property
    def path(self) -> str:
        return settings.AI_LEARNING_ASSISTANT_WEBHOOK_PATH


ai_learning_assistant_client = AILearningAssistantClient()
