import random
import uuid

from celery_app import app
from schemas import Message, SendResult


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
