import asyncio

from pydantic import ValidationError

from app.api.deps import redis_client
from app.schemas.notification import NotificationMessage
from app.services.broker import connect, setup_queues
from app.services.idempotency import acquire_lock
from app.tasks import send_notification


async def handle_message(message):
    try:
        data = NotificationMessage.model_validate_json(message.body)
    except ValidationError:
        print("Зламане повідомлення → DLQ:", message.body)
        await message.reject(requeue=False)
        return

    if not await acquire_lock(redis_client, data.idempotency_key):
        print("Дубль, пропускаю:", data.idempotency_key)
        await message.ack()
        return

    # Саму доставку виконує Celery. .delay() — звичайний синхронний виклик,
    # тому запускаємо його в окремому потоці, щоб не блокувати event loop
    await asyncio.to_thread(send_notification.delay, data.id)
    print("Передано в Celery:", data.id)
    await message.ack()


async def main():
    connection, channel = await connect()
    await channel.set_qos(prefetch_count=10)  # брати з черги не більше 10 повідомлень одночасно
    await setup_queues(channel)
    queue = await channel.get_queue("notifications.queue")
    await queue.consume(handle_message)
    print("Консьюмер слухає чергу... (Ctrl+C, щоб зупинити)")
    try:
        await asyncio.Future()
    finally:
        await connection.close()  # при зупинці закриваємо з'єднання з RabbitMQ


if __name__ == "__main__":
    asyncio.run(main())
