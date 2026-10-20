import json
from unittest.mock import AsyncMock, MagicMock

import aio_pika

from app import main
from app.services import broker


async def test_connect_opens_robust_connection_and_channel(monkeypatch):
    channel = object()
    connection = MagicMock(channel=AsyncMock(return_value=channel))
    connect_robust = AsyncMock(return_value=connection)
    monkeypatch.setattr(aio_pika, "connect_robust", connect_robust)

    conn, ch = await broker.connect()

    assert (conn, ch) == (connection, channel)
    connect_robust.assert_awaited_once()


async def test_setup_queues_declares_dlq_topology():
    queue = MagicMock(bind=AsyncMock())
    channel = MagicMock(
        declare_exchange=AsyncMock(return_value="exchange"),
        declare_queue=AsyncMock(return_value=queue),
    )

    await broker.setup_queues(channel)

    exchanges = [c.args[0] for c in channel.declare_exchange.await_args_list]
    assert exchanges == ["notifications.dlx", "notifications.direct"]
    main_queue = channel.declare_queue.await_args_list[1]
    assert main_queue.args[0] == "notifications.queue"
    assert main_queue.kwargs["arguments"] == {
        "x-dead-letter-exchange": "notifications.dlx",
        "x-dead-letter-routing-key": "dlq",
    }
    assert all(c.kwargs["durable"] for c in channel.declare_queue.await_args_list)


async def test_publish_sends_persistent_json():
    exchange = MagicMock(publish=AsyncMock())
    channel = MagicMock(get_exchange=AsyncMock(return_value=exchange))

    await broker.publish_notification(channel, {"id": 1, "message": "hi"})

    message = exchange.publish.await_args.args[0]
    assert json.loads(message.body) == {"id": 1, "message": "hi"}
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert exchange.publish.await_args.kwargs["routing_key"] == "notifications"


async def test_lifespan_connects_on_start_and_cleans_up_on_stop(monkeypatch):
    channel = object()
    connection = MagicMock(close=AsyncMock())
    monkeypatch.setattr(main, "connect", AsyncMock(return_value=(connection, channel)))
    monkeypatch.setattr(main, "setup_queues", AsyncMock())
    redis = MagicMock(aclose=AsyncMock())
    engine = MagicMock(dispose=AsyncMock())
    monkeypatch.setattr(main, "redis_client", redis)
    monkeypatch.setattr(main, "engine", engine)

    async with main.lifespan(main.app):
        assert main.app.state.rabbit_channel is channel  # канал поклали в «шухляду»

    connection.close.assert_awaited_once()
    redis.aclose.assert_awaited_once()
    engine.dispose.assert_awaited_once()
