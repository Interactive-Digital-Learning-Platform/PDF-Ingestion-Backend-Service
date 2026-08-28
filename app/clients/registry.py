from app.clients.ai_learning_assistant_client import ai_learning_assistant_client
from app.clients.base_webhook_client import BaseWebhookClient

WEBHOOK_CLIENT_REGISTRY: dict[str, BaseWebhookClient] = {
    "ai-learning-assistant-service": ai_learning_assistant_client,
}
