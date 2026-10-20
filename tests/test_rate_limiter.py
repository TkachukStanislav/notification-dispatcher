import time
import uuid

import pytest

from app.services import rate_limiter


@pytest.fixture
def bucket_key():
    key = f"rate_limit:test:{uuid.uuid4().hex}"  # окреме відро на кожен тест
    yield key
    rate_limiter.redis.delete(key)


def test_burst_is_capped_by_capacity(bucket_key):
    allowed = sum(rate_limiter.try_acquire(bucket_key) for _ in range(40))

    # 30 жетонів у повному відрі + кілька, що встигли нарости під час циклу
    assert rate_limiter.CAPACITY <= allowed < 40


def test_empty_bucket_refills_over_time(bucket_key):
    while rate_limiter.try_acquire(bucket_key):  # спорожнили відро
        pass

    start = time.monotonic()
    rate_limiter.acquire_token(bucket_key)  # чекає, поки наросте жетон
    waited = time.monotonic() - start

    assert waited < 0.5  # при 30 жетонах/с новий з'являється приблизно за 0,03 с
