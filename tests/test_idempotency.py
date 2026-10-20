import uuid

import pytest
from redis.asyncio import Redis

from app.core.config import settings
from app.services.idempotency import acquire_lock


@pytest.fixture
async def redis():
    client = Redis.from_url(settings.REDIS_URL)
    yield client
    await client.aclose()


async def test_only_first_acquire_wins(redis):
    key = f"test-{uuid.uuid4().hex}"

    first = await acquire_lock(redis, key)
    second = await acquire_lock(redis, key)

    assert first is True
    assert second is False  # той самий ключ удруге — дубль
    assert 0 < await redis.ttl(f"lock:{key}") <= 300  # замок сам зникне
    await redis.delete(f"lock:{key}")
