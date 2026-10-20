from contextlib import asynccontextmanager

import httpx
import pytest
import respx
from sqlalchemy import select

from app import tasks
from app.core.config import settings
from app.models import DeliveryLog, Notification


@pytest.fixture
def task_db(db_session, monkeypatch):
    """deliver() і set_status() працюють через «олівцеву» сесію, яка відкочується після тесту."""

    @asynccontextmanager
    async def session_factory():
        yield db_session

    monkeypatch.setattr(tasks, "SessionLocal", session_factory)
    return db_session


async def make_notification(session, status="pending"):
    n = Notification(
        recipient_id=1, message="hi", idempotency_key=f"deliver-{status}", status=status
    )
    session.add(n)
    await session.commit()
    return n


@respx.mock
async def test_deliver_sends_and_logs_attempt(task_db):
    route = respx.post(settings.TELEGRAM_URL).respond(502)
    n = await make_notification(task_db)

    code = await tasks.deliver(n.id, attempt=2)

    assert code == 502
    assert route.call_count == 1
    log = (
        await task_db.scalars(select(DeliveryLog).where(DeliveryLog.notification_id == n.id))
    ).one()
    assert (log.attempt, log.status_code) == (2, 502)


@respx.mock
async def test_deliver_skips_already_sent(task_db):
    route = respx.post(settings.TELEGRAM_URL).respond(200)
    n = await make_notification(task_db, status="sent")

    code = await tasks.deliver(n.id, attempt=1)

    assert code == 200
    assert route.call_count == 0  # повторна задача не надсилає вдруге


async def test_deliver_unknown_notification(task_db):
    assert await tasks.deliver(999999, attempt=1) == 404


@respx.mock
async def test_deliver_connection_error_returns_none(task_db):
    respx.post(settings.TELEGRAM_URL).mock(side_effect=httpx.ConnectError("down"))
    n = await make_notification(task_db)

    assert await tasks.deliver(n.id, attempt=1) is None


async def test_set_status(task_db):
    n = await make_notification(task_db)

    await tasks.set_status(n.id, "sent")
    await tasks.set_status(999999, "sent")  # неіснуючий id не ламає задачу

    await task_db.refresh(n)
    assert n.status == "sent"
