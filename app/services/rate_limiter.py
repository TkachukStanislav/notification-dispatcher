import time
from pathlib import Path

from redis import Redis

from app.core.config import settings

CAPACITY = 30  # скільки жетонів влазить у відро (дозволений «залп»)
RATE = 30  # скільки жетонів додається за секунду
BUCKET_KEY = "rate_limit:telegram"

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "token_bucket.lua"

# Celery-задачі синхронні, тому тут синхронний клієнт Redis.
# Відро одне на всіх воркерів, тож ліміт спільний для будь-якої кількості реплік.
redis = Redis.from_url(settings.REDIS_URL, socket_connect_timeout=2)
token_bucket = redis.register_script(SCRIPT_PATH.read_text(encoding="utf-8"))


def try_acquire(key: str = BUCKET_KEY) -> bool:
    """Одна спроба взяти жетон. Атомарність забезпечує Lua-скрипт у Redis."""
    return token_bucket(keys=[key], args=[CAPACITY, RATE]) == 1


def acquire_token(key: str = BUCKET_KEY) -> None:
    """Чекає, поки у відрі з'явиться жетон."""
    while not try_acquire(key):
        time.sleep(1 / RATE)  # приблизно стільки наростає один жетон
