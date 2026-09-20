from celery.utils.log import get_task_logger

from celery_app import app
from schemas import DeliveryResult, Message, SendResult
from tasks.messaging import check_delivery_status, send_whatsapp_message

logger = get_task_logger(__name__)


@app.task
def send_and_wait(messages: list[Message]) -> None:
    """Start of one loop iteration: send the first message, then hand off.

    The whole chain is a loop built from task callbacks (link=), where each
    task schedules the next one only when it succeeds:

        send_and_wait ──> send_whatsapp_message ──link──> wait_for_delivery
              ^                                                  │
              │                                                  v
        advance_after_delivery <──link── check_delivery_status <─┘

    `remaining` (the messages still to send) is carried along in each link's
    arguments. Message N+1 is only sent after N is delivered because
    advance_after_delivery runs only when check_delivery_status succeeds.
    An empty list ends the loop.
    """
    if not messages:
        logger.info("chain finished: all messages delivered")
        return
    current, *remaining = messages
    logger.info("sending to %s (%d left after this)", current["recipient"], len(remaining))
    send_whatsapp_message.apply_async(args=[current], link=wait_for_delivery.s(remaining))


@app.task
def wait_for_delivery(send_result: SendResult, remaining: list[Message]) -> None:
    """Runs after a successful send. Celery passes the send's return value as
    `send_result`; `remaining` comes from the `.s(remaining)` in send_and_wait.
    Starts the delivery polling for this message."""
    logger.info("sent %s, now polling delivery", send_result["message_id"])
    check_delivery_status.apply_async(
        args=[send_result["message_id"]],
        link=advance_after_delivery.s(remaining),
    )


@app.task
def advance_after_delivery(delivery_result: DeliveryResult, remaining: list[Message]) -> None:
    """Runs only once check_delivery_status succeeded (message delivered).
    Recurses into send_and_wait with the rest of the list."""
    logger.info("delivered %s, advancing", delivery_result["message_id"])
    send_and_wait.delay(remaining)
