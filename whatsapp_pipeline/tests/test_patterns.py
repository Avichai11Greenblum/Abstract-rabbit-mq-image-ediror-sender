"""Runs tasks eagerly in-process (no broker or worker), so it checks the logic
and wiring of each pattern, not real parallelism or timing."""

import itertools
from types import SimpleNamespace

import pytest
from celery import group
from celery.exceptions import MaxRetriesExceededError

from celery_app import app
from tasks import chain, images, messaging

MESSAGE = {"recipient": "user0", "text": "hi"}


@pytest.fixture(autouse=True)
def eager_celery():
    old = (app.conf.task_always_eager, app.conf.task_eager_propagates)
    app.conf.task_always_eager = True
    app.conf.task_eager_propagates = True
    yield
    app.conf.task_always_eager, app.conf.task_eager_propagates = old


@pytest.fixture
def api_always_up(monkeypatch):
    monkeypatch.setattr(messaging, "random", SimpleNamespace(random=lambda: 0.9))


def fail_then_succeed(monkeypatch, rolls):
    """Make send_whatsapp_message consume `rolls` (0.1 = API failure, 0.9 = success)."""
    calls = []
    rolls = iter(rolls)

    def fake_random():
        calls.append(1)
        return next(rolls)

    monkeypatch.setattr(messaging, "random", SimpleNamespace(random=fake_random))
    return calls


def delivery_sequence(monkeypatch, answers):
    """Make _is_delivered return `answers` in order (an iterator, or a callable)."""
    calls = []
    answers = iter(answers)

    def fake_is_delivered():
        calls.append(1)
        return next(answers)

    monkeypatch.setattr(messaging, "_is_delivered", fake_is_delivered)
    return calls


# --- Fan-out via group() ---


def test_fanout_runs_all_three_tasks(monkeypatch):
    monkeypatch.setattr(images, "time", SimpleNamespace(sleep=lambda s: None))

    job = group(
        images.resize_image.s("a.jpg"),
        images.watermark_image.s("a.jpg"),
        images.extract_metadata.s("a.jpg"),
    )

    assert job.apply_async().get() == [
        "resized:a.jpg",
        "watermarked:a.jpg",
        "metadata:a.jpg",
    ]


# --- Flaky call + autoretry ---


def test_send_retry_config():
    task = messaging.send_whatsapp_message
    assert task.autoretry_for == (messaging.WhatsAppAPIError,)
    assert task.retry_backoff is True
    assert task.retry_backoff_max == 30
    assert task.retry_jitter is True
    assert task.max_retries == 5


def test_send_retries_until_success(monkeypatch):
    calls = fail_then_succeed(monkeypatch, [0.1, 0.1, 0.9])

    result = messaging.send_whatsapp_message.delay(MESSAGE).get()

    assert result["status"] == "sent"
    assert len(calls) == 3


def test_send_gives_up_after_max_retries(monkeypatch):
    calls = fail_then_succeed(monkeypatch, itertools.repeat(0.1))

    with pytest.raises(messaging.WhatsAppAPIError):
        messaging.send_whatsapp_message.delay(MESSAGE).get()

    assert len(calls) == messaging.send_whatsapp_message.max_retries + 1


# --- Self-retry as polling ---


def test_check_delivery_polls_until_delivered(monkeypatch):
    calls = delivery_sequence(monkeypatch, [False, False, True])

    result = messaging.check_delivery_status.delay("m1").get()

    assert result == {"message_id": "m1", "delivered": True}
    assert len(calls) == 3


def test_check_delivery_gives_up_after_max_retries(monkeypatch):
    calls = delivery_sequence(monkeypatch, itertools.repeat(False))

    with pytest.raises(MaxRetriesExceededError):
        messaging.check_delivery_status.delay("m1").get()

    assert len(calls) == messaging.check_delivery_status.max_retries + 1


# --- Manual link= chain ---


def test_chain_sends_one_message_at_a_time(monkeypatch, api_always_up):
    events = []
    original_apply_async = chain.send_whatsapp_message.apply_async

    def spy_apply_async(*args, **kwargs):
        events.append(("send", kwargs["args"][0]["recipient"]))
        return original_apply_async(*args, **kwargs)

    monkeypatch.setattr(chain.send_whatsapp_message, "apply_async", spy_apply_async)

    checks = itertools.count(1)

    def fake_is_delivered():
        events.append("check")
        return next(checks) % 3 == 0  # each message needs 3 checks to be delivered

    monkeypatch.setattr(messaging, "_is_delivered", fake_is_delivered)

    messages = [{"recipient": f"user{i}", "text": "hi"} for i in range(3)]
    chain.send_and_wait.delay(messages)

    assert events == [
        ("send", "user0"), "check", "check", "check",
        ("send", "user1"), "check", "check", "check",
        ("send", "user2"), "check", "check", "check",
    ]


def test_chain_with_no_messages_does_nothing(monkeypatch):
    calls = fail_then_succeed(monkeypatch, [])

    chain.send_and_wait.delay([])

    assert calls == []
