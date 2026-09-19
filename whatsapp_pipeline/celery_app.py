from celery import Celery

app = Celery(
    "whatsapp_pipeline",
    broker="amqp://guest:guest@localhost:5672//",
    backend="rpc://",
)


@app.task
def ping():
    return "pong"
