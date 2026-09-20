import random
import uuid

from celery_app import app
from schemas import DeliveryResult, Message, SendResult


class WhatsAppAPIError(Exception):
    """Raised when the simulated WhatsApp API call fails."""


@app.task(
    autoretry_for=(WhatsAppAPIError,),
    retry_backoff=True,
    retry_backoff_max=30,
    retry_jitter=True,
    max_retries=5,
)
def send_whatsapp_message(message: Message) -> SendResult:
    if random.random() < 0.4:
        raise WhatsAppAPIError(f"Simulated failure sending to {message['recipient']}")
    return SendResult(message_id=str(uuid.uuid4()), status="sent")


def _is_delivered() -> bool:
    return random.random() < 0.3


@app.task(bind=True, max_retries=10)
def check_delivery_status(self, message_id: str) -> DeliveryResult:
    if not _is_delivered():
        raise self.retry(countdown=5)
    return DeliveryResult(message_id=message_id, delivered=True)
