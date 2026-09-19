from celery import Celery

app = Celery(
    "whatsapp_pipeline",
    broker="amqp://guest:guest@localhost:5672//",
    backend="rpc://",
    include=["tasks.images", "tasks.messaging"],
)


@app.task
def ping():
    return "pong"
