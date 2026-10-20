from httpx import ASGITransport, AsyncClient

from app.api.deps import get_redis
from app.main import app


async def test_live():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


class FakeRedis:
    def __init__(self, alive: bool):
        self.alive = alive

    async def ping(self):
        if not self.alive:
            raise ConnectionError("redis is down")
        return True


async def test_ready_ok(client):
    app.dependency_overrides[get_redis] = lambda: FakeRedis(alive=True)

    response = await client.get("/health/ready")

    assert response.status_code == 200
    assert response.json()["checks"] == {"postgres": "ok", "redis": "ok"}


async def test_ready_returns_503_when_postgres_down(client):
    from app.api.deps import get_db

    class BrokenSession:
        async def execute(self, *args):
            raise ConnectionError("postgres is down")

    async def broken_db():
        yield BrokenSession()

    app.dependency_overrides[get_db] = broken_db
    app.dependency_overrides[get_redis] = lambda: FakeRedis(alive=True)

    response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["postgres"] == "error"


async def test_root(client):
    response = await client.get("/")

    assert response.status_code == 200


async def test_dependencies_provide_session_and_redis():
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.api.deps import get_db, redis_client

    gen = get_db()
    session = await anext(gen)
    assert isinstance(session, AsyncSession)
    await gen.aclose()
    assert get_redis() is redis_client


async def test_ready_returns_503_when_redis_down(client):
    app.dependency_overrides[get_redis] = lambda: FakeRedis(alive=False)

    response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["redis"] == "error"
