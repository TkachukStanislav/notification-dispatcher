import json
from unittest.mock import AsyncMock

import pytest

from app.workers import consumer


class FakeMessage:
    def __init__(self, body: bytes):
        self.body = body
        self.ack = AsyncMock()
        self.reject = AsyncMock()


@pytest.fixture
def handoff(monkeypatch):
    """Підміняє Redis-замок і Celery: запам'ятовує, що передано в Celery."""
    state = {"lock": True, "queued": []}

    async def fake_lock(redis, key):
        return state["lock"]

    class FakeTask:
        @staticmethod
        def delay(notification_id):
            state["queued"].append(notification_id)

    monkeypatch.setattr(consumer, "acquire_lock", fake_lock)
    monkeypatch.setattr(consumer, "send_notification", FakeTask)
    return state


def event(**overrides):
    data = {"id": 7, "recipient_id": 1, "message": "hi", "idempotency_key": "k-7"}
    data.update(overrides)
    return FakeMessage(json.dumps(data).encode())


async def test_valid_event_goes_to_celery(handoff):
    msg = event()

    await consumer.handle_message(msg)

    assert handoff["queued"] == [7]
    msg.ack.assert_awaited_once()
    msg.reject.assert_not_awaited()


async def test_broken_event_goes_to_dlq(handoff):
    msg = FakeMessage(b'{"broken": ')

    await consumer.handle_message(msg)

    msg.reject.assert_awaited_once_with(requeue=False)
    msg.ack.assert_not_awaited()
    assert handoff["queued"] == []


async def test_main_subscribes_and_closes_connection(monkeypatch):
    import asyncio
    from unittest.mock import MagicMock

    queue = MagicMock(consume=AsyncMock())
    channel = MagicMock(set_qos=AsyncMock(), get_queue=AsyncMock(return_value=queue))
    connection = MagicMock(close=AsyncMock())

    async def fake_connect():
        return connection, channel

    monkeypatch.setattr(consumer, "connect", fake_connect)
    monkeypatch.setattr(consumer, "setup_queues", AsyncMock())

    task = asyncio.create_task(consumer.main())
    await asyncio.sleep(0.05)  # дали main() підписатися на чергу
    task.cancel()  # те саме, що Ctrl+C
    with pytest.raises(asyncio.CancelledError):
        await task

    channel.set_qos.assert_awaited_once_with(prefetch_count=10)
    queue.consume.assert_awaited_once_with(consumer.handle_message)
    connection.close.assert_awaited_once()  # finally закрив з'єднання


async def test_duplicate_is_acked_but_not_queued(handoff):
    handoff["lock"] = False
    msg = event()

    await consumer.handle_message(msg)

    msg.ack.assert_awaited_once()
    assert handoff["queued"] == []
