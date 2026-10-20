import asyncio
import random

from celery import Celery
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.models import DeliveryLog, Notification
from app.services.rate_limiter import acquire_token
from app.services.sender import send_message

MAX_RETRIES = 2  # 1 спроба + 2 повтори = 3 спроби разом

celery_app = Celery("dispatcher", broker=settings.RABBITMQ_URL)
celery_app.conf.update(
    task_default_queue="notifications.tasks",
    task_acks_late=True,  # ack після виконання задачі, а не при отриманні
    task_reject_on_worker_lost=True,  # воркер помер посеред задачі → повернути її в чергу
    worker_prefetch_multiplier=1,  # брати по одній задачі, щоб довгі retry не тримали інші
)

# Кожен asyncio.run() створює новий event loop, а пул з'єднань прив'язаний до одного loop.
# Тому в Celery окремий двигун без пулу: кожна задача відкриває свіже з'єднання.
engine = create_async_engine(settings.database_url, poolclass=NullPool)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def deliver(notification_id: int, attempt: int) -> int | None:
    """Одна спроба: надіслати і записати її в delivery_logs. Повертає код відповіді."""
    async with SessionLocal() as session:
        notification = await session.get(Notification, notification_id)
        if notification is None:
            return 404
        if notification.status == "sent":  # повторна доставка задачі: вже надіслано
            return 200

        # Спільний для всіх воркерів ліміт частоти (token bucket у Redis).
        # acquire_token синхронний і може чекати, тому виносимо його в окремий потік.
        await asyncio.to_thread(acquire_token)
        status_code = await send_message(notification.message)
        session.add(
            DeliveryLog(notification_id=notification_id, attempt=attempt, status_code=status_code)
        )
        await session.commit()
        return status_code


async def set_status(notification_id: int, status: str) -> None:
    async with SessionLocal() as session:
        notification = await session.get(Notification, notification_id)
        if notification is not None:
            notification.status = status
            await session.commit()


@celery_app.task(bind=True, max_retries=MAX_RETRIES)
def send_notification(self, notification_id: int) -> str:
    attempt = self.request.retries + 1
    status_code = asyncio.run(deliver(notification_id, attempt))
    print(f"Нотифікація {notification_id}, спроба {attempt}: {status_code}")

    if status_code == 200:
        asyncio.run(set_status(notification_id, "sent"))
        return "sent"

    retryable = status_code is None or status_code >= 500  # тиша або 5xx — тимчасовий збій
    if retryable and self.request.retries < self.max_retries:
        raise self.retry(countdown=2**attempt + random.uniform(0.1, 0.5))  # 2 с, потім 4 с

    asyncio.run(set_status(notification_id, "failed"))  # 4xx або спроби закінчились
    return "failed"
