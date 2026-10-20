import pytest

from app import tasks


@pytest.fixture
def fake_delivery(monkeypatch):
    """Підміняє роботу з базою і HTTP: deliver повертає коди по черзі, set_status запам'ятовує статус."""
    state = {"codes": [], "attempts": [], "status": None}

    async def fake_deliver(notification_id, attempt):
        state["attempts"].append(attempt)
        return state["codes"].pop(0)

    async def fake_set_status(notification_id, status):
        state["status"] = status

    monkeypatch.setattr(tasks, "deliver", fake_deliver)
    monkeypatch.setattr(tasks, "set_status", fake_set_status)
    return state


def test_sent_on_first_attempt(fake_delivery):
    fake_delivery["codes"] = [200]

    tasks.send_notification.apply(args=[1])

    assert fake_delivery["attempts"] == [1]
    assert fake_delivery["status"] == "sent"


def test_retries_5xx_then_sent(fake_delivery):
    fake_delivery["codes"] = [502, None, 200]  # збій, тиша, успіх

    tasks.send_notification.apply(args=[1])

    assert fake_delivery["attempts"] == [1, 2, 3]
    assert fake_delivery["status"] == "sent"


def test_failed_after_all_retries(fake_delivery):
    fake_delivery["codes"] = [502, 502, 502]

    tasks.send_notification.apply(args=[1])

    assert fake_delivery["attempts"] == [1, 2, 3]
    assert fake_delivery["status"] == "failed"


def test_4xx_not_retried(fake_delivery):
    fake_delivery["codes"] = [404]

    tasks.send_notification.apply(args=[1])

    assert fake_delivery["attempts"] == [1]
    assert fake_delivery["status"] == "failed"
