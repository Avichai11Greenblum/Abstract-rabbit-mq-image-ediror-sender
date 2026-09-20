# rabbitmq_practice

A small learning project for practicing **Celery + RabbitMQ (as docker container server)** (managed with **uv**).

> **Disclaimer:** this is an abstract architecture exercise, not a real WhatsApp sender. The "WhatsApp API" and the delivery status are random stubs, and the image tasks only sleep. Some patterns (sequential sending, self-retry as polling) are deliberately naive, chosen to practice specific Celery primitives rather than as production design.

## What it demonstrates

| Pattern | Where | How |
|---|---|---|
| Fan-out | `tasks/images.py` | `resize_image`, `watermark_image` and `extract_metadata` run in parallel via `group()` |
| Flaky call + retry | `tasks/messaging.py` | `send_whatsapp_message` fails ~40% of the time; `autoretry_for` with exponential backoff, jitter (max 30s) and `max_retries=5` |
| Self-retry as polling | `tasks/messaging.py` | `check_delivery_status` calls `self.retry(countdown=5)` until the stubbed delivery check passes (no Celery Beat) |
| Manual sequential chain | `tasks/chain.py` | Messages are sent one at a time using `link=` callbacks: send, then wait, then poll delivery, then recurse with the remaining messages |

Typed payloads (`Message`, `SendResult`, `DeliveryResult`) live in `schemas.py`.

## Layout

```
docker-compose.yml        RabbitMQ (with management UI)
whatsapp_pipeline/
  celery_app.py           Celery app (RabbitMQ broker, rpc:// result backend)
  schemas.py              TypedDicts
  tasks/                  images.py, messaging.py, chain.py
  cli.py                  demo entrypoint, one subcommand per pattern
  tests/                  pytest suite (tasks run eagerly, no broker needed)
```

One shared virtualenv and `pyproject.toml` at the repo root serve the whole project.

## Run it

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
docker compose up -d        # RabbitMQ; management UI at http://localhost:15672 (guest/guest)
uv sync

cd whatsapp_pipeline
uv run celery -A celery_app worker --loglevel=info      # terminal 1
```

In a second terminal, from `whatsapp_pipeline/`:

```bash
uv run python cli.py fanout photo.jpg    # parallel image tasks
uv run python cli.py flaky 5             # watch retries/backoff in the worker log
uv run python cli.py poll msg-1          # watch the task re-schedule itself
uv run python cli.py chain 3             # strictly one message at a time
uv run python cli.py load 50 0.5         # steady fire-and-forget load, watch the queue in the UI
```

## Tests

```bash
uv run pytest -v
```

Tests run tasks eagerly in-process, so they check each pattern's logic and wiring, not real parallelism or timing.
