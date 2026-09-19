from typing import TypedDict


class Message(TypedDict):
    recipient: str
    text: str


class SendResult(TypedDict):
    message_id: str
    status: str


class DeliveryResult(TypedDict):
    message_id: str
    delivered: bool
