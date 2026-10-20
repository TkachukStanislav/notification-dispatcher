# Notification Dispatcher

[![CI](https://github.com/TkachukStanislav/notification-dispatcher/actions/workflows/ci.yml/badge.svg)](https://github.com/TkachukStanislav/notification-dispatcher/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.12-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-async-009688)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-DLQ-FF6600)
![Celery](https://img.shields.io/badge/Celery-retries-37814A)
![Redis](https://img.shields.io/badge/Redis-idempotency-DC382D)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)

**Reliable asynchronous notification delivery.** The API accepts a notification and responds
instantly; an event consumer validates and deduplicates it, and Celery workers deliver it with
retries — so a slow or failing provider never blocks clients and no message is lost or sent twice.

## Architecture

```mermaid
flowchart LR
    C[Client] -->|POST /api/v1/notifications| API[FastAPI]
    API -->|status: pending| DB[(PostgreSQL)]
    API -->|publish event| Q[[RabbitMQ<br/>notifications.queue]]
    Q --> CON[Event consumer<br/>aio-pika]
    Q -.->|invalid event| DLQ[[Dead-letter queue]]
    CON -->|lock idempotency_key| R[(Redis)]
    CON -->|send_notification.delay| TQ[[RabbitMQ<br/>notifications.tasks]]
    TQ --> W[Celery worker]
    W -->|send, retry with backoff| T[Telegram API<br/>mock]
    W -->|sent / failed + delivery_logs| DB
```

Two kinds of workers, each with one job:

- **Event consumer (aio-pika)** reads raw events in their own format — as it would from an external
  system like an ERP — validates them, drops duplicates and hands each one to Celery. It stays fast,
  so the event queue never backs up while a provider is slow.
- **Celery worker** does the slow part: sends the notification, retries on failure and records
  the result.

## Features

| | |
|---|---|
| **Async API** | FastAPI + SQLAlchemy 2.0 (asyncpg) with an explicit connection pool |
| **Event queue** | RabbitMQ with a direct exchange and a dead-letter queue for invalid events |
| **Task queue** | Celery on RabbitMQ with late acks, one task per worker slot |
| **Idempotency** | Unique key in PostgreSQL + atomic Redis `SET NX EX` lock in the consumer; the task skips already-sent notifications |
| **Retries** | Up to 3 attempts on 5xx / connection errors via Celery `retry` with exponential backoff and jitter |
| **Delivery history** | Every attempt stored in `delivery_logs` |
| **Health checks** | `/health/live` (process) and `/health/ready` (PostgreSQL + Redis, `503` on failure) |
| **Zero-downtime migrations** | Alembic indexes built `CONCURRENTLY` |
| **Tests** | 100% line coverage enforced in CI; isolated per-test DB transactions, dependency overrides, HTTP mocking, Celery tasks run eagerly — no real network |
| **Docker** | Multi-stage, non-root image, ~68 MB compressed; dev tools stay out of it |
| **CI** | GitHub Actions: lint, tests against PostgreSQL, Docker build |

**Stack:** Python 3.12 · FastAPI · Pydantic v2 · SQLAlchemy 2.0 · asyncpg · Alembic ·
RabbitMQ (aio-pika) · Celery · Redis · httpx · pytest · respx · ruff · Docker · GitHub Actions

## Key design decisions

- **Consumer for events, Celery for work.** A Celery worker only understands messages created by
  Celery itself, so raw events are read by a plain aio-pika consumer. It turns each valid event into
  a Celery task, which gives built-in retries, countdowns and separate scaling for delivery.
- **Ack after processing, not on receive.** The consumer acks only after the task is queued;
  Celery runs with `task_acks_late=True` and `task_reject_on_worker_lost=True`, so a crash mid-task
  returns it to the queue instead of losing it (at-least-once delivery).
- **Idempotency on two levels.** A unique `idempotency_key` in PostgreSQL rejects repeated API
  requests; a Redis `SET NX EX` lock stops a redelivered event from being processed twice. `NX`
  makes check-and-set a single atomic step, so two consumers cannot both win. A redelivered task
  checks the status and does not resend an already-sent notification.
- **Poison messages go to a DLQ.** Invalid payloads are rejected with `requeue=False` and routed to
  `notifications.dlq` by RabbitMQ, instead of being redelivered forever and blocking the queue.
- **Retry only what can succeed.** 5xx and connection errors are retried with
  `2^attempt + random jitter` seconds of delay; 4xx responses fail immediately, because repeating
  a bad request will not fix it. Jitter prevents all failed messages from retrying at once. The
  delay is a Celery countdown, so the worker is free for other tasks while waiting.
- **Async code inside sync Celery tasks.** Tasks are synchronous, the delivery code is async. Each
  task runs it with `asyncio.run()` on a separate engine with `NullPool`, because a pooled
  connection is bound to the event loop that created it and every `asyncio.run()` starts a new one.
- **Bounded concurrency.** `prefetch_count=10` on the consumer and `worker_prefetch_multiplier=1`
  on Celery keep a large backlog from exhausting database or Redis connections.
- **Indexes without locking writes.** Indexes are created in a separate migration with
  `CREATE INDEX CONCURRENTLY` inside Alembic's `autocommit_block()`.
- **Tests never touch real data or network.** Each test runs in a transaction that is rolled back;
  the DB session is swapped via `dependency_overrides`, RabbitMQ via `monkeypatch`, the Telegram
  API via respx, and Celery tasks run eagerly with `apply()`.

## Known limitations & next steps

Honest list of what a production version would still need:

- **Transactional outbox.** The notification is committed before it is published; if RabbitMQ is
  down at that moment, the record stays `pending` with no message in the queue.
- **Lock is taken before handing off.** If the consumer crashes after acquiring the Redis lock but
  before queuing the task, the redelivered event is treated as a duplicate until the lock expires
  (5 min).
- **Duplicate API requests return `500`.** Should return the existing notification or `409`.
- **Rate limiting** towards the provider is not implemented. Next step: a Redis fixed-window
  counter shared by all Celery workers (`INCR` + `EXPIRE` in one `MULTI/EXEC`), deferring the task
  with `self.retry(countdown=...)` when the window is full. Celery's own `rate_limit` is not enough,
  because it counts per worker, not across replicas.
- **Real Telegram Bot API** instead of the local mock, and Flower for monitoring Celery.

## Running locally

1. Start PostgreSQL, Redis and RabbitMQ:
   ```bash
   docker compose up -d
   ```
2. Install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate        # Windows: .venv\Scripts\activate
   pip install -r requirements-dev.txt   # runtime + test and lint tools
   ```
3. Create `.env`:
   ```
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=postgres
   POSTGRES_DB=notification_db
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   REDIS_URL=redis://localhost:6379/0
   RABBITMQ_URL=amqp://guest:guest@localhost:5672/
   TELEGRAM_URL=http://localhost:9000/send
   ```
4. Apply migrations:
   ```bash
   alembic upgrade head
   ```
5. Run each process in its own terminal:
   ```bash
   uvicorn mock_telegram:app --port 9000      # mock Telegram API
   uvicorn app.main:app --reload              # API → http://localhost:8000/docs
   python -m app.workers.consumer             # event consumer
   celery -A app.tasks worker --loglevel=info # Celery worker (on Windows add --pool=solo)
   ```

### Try it

```bash
curl -X POST http://localhost:8000/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{"recipient_id": 42, "message": "Your order has shipped", "idempotency_key": "order-123"}'
```

```json
{"id": 1, "status": "pending", "recipient_id": 42, "message": "Your order has shipped", "idempotency_key": "order-123", "created_at": "2026-10-07T12:00:00Z"}
```

Within a second the consumer hands it to Celery, and the worker sets the status to `sent` (or
`failed` after 3 attempts).

## Tests

```bash
pytest -v --cov
```

30 tests, 100% line coverage of `app/` (CI fails below 100%). Needs the PostgreSQL and Redis
containers with migrations applied; RabbitMQ, the Celery broker and the Telegram API are mocked.

## Project structure

```
app/
├── api/            # endpoints (notifications, health) and dependencies
├── core/           # settings, database engine
├── models/         # Notification, DeliveryLog
├── schemas/        # Pydantic schemas
├── services/       # RabbitMQ broker, idempotency lock, HTTP sender
├── workers/        # aio-pika event consumer
├── tasks.py        # Celery app and the send_notification task
└── main.py         # FastAPI app and lifespan
alembic/            # migrations
tests/              # pytest suite
mock_telegram.py    # local stand-in for the Telegram API
```

## Docker

```bash
docker build -t notification-dispatcher .
```

The image installs only `requirements.txt` (runtime); test and lint tools live in
`requirements-dev.txt`. Size: ~68 MB compressed (what a registry stores), ~225 MB unpacked, of which
125 MB is the `python:3.12-slim` base.

The container reads the same environment variables as `.env`. Inside a container `localhost` is
the container itself, so point hosts to the services (e.g. `POSTGRES_HOST=host.docker.internal`
on Docker Desktop). The same image runs every process with a different command:
`python -m app.workers.consumer` for the consumer and `celery -A app.tasks worker` for Celery.
