# Harmoniq Scheduler — POC / Architecture Document

Version 0.2.0 · Lambda + EventBridge style generic scheduler service.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Solution — What This Service Does](#2-solution--what-this-service-does)
3. [High-Level Architecture](#3-high-level-architecture)
4. [Components — What & Why](#4-components--what--why)
5. [Data Flow — End-to-End Lifecycle](#5-data-flow--end-to-end-lifecycle)
6. [Storage Strategy — Postgres vs Redis](#6-storage-strategy--postgres-vs-redis)
7. [API Reference](#7-api-reference)
8. [Failure Handling & Reliability](#8-failure-handling--reliability)
9. [Configuration](#9-configuration)
10. [Setup & Running Locally](#10-setup--running-locally)
11. [Testing Guide](#11-testing-guide)
12. [Comparison: AWS vs This Service](#12-comparison-aws-vs-this-service)
13. [Trade-offs & Future Work](#13-trade-offs--future-work)

---

## 1. Problem Statement

We need a **generic scheduler service** that can fire HTTP webhooks at specific times or on recurring cron schedules — much like AWS EventBridge Scheduler firing Lambda functions.

**Use cases:**
- Trigger a report generation every Monday at 9 AM
- Send a reminder 3 days from now
- Run a batch job every 15 minutes
- Fire a template execution once at 2026-08-15 14:00 UTC

**Requirements:**
- Consumer service (Harmoniq) provides a webhook URL + payload
- Scheduler stores it, fires at the right time
- Scheduler doesn't need to know what the webhook does
- Must survive restarts (pod restart, Redis wipe, deployment)
- Must handle transient failures (retry)

---

## 2. Solution — What This Service Does

A standalone service with 3 processes:

- **API (FastAPI)** — exposes `/schedules` CRUD endpoints
- **Worker (Celery)** — executes the webhook when a schedule fires
- **Beat (Celery + RedBeat)** — the scheduling engine (fires cron entries)

**Consumer contract:**
```
POST /schedules
{
  "id": "my-unique-id",
  "path": "/task/status",             // relative path
  "payload": {"user": "u1"},          // opaque JSON
  "schedule_type": "onetime",         // or "cron"
  "run_at": "2026-08-15T09:00:00Z"    // for onetime
  // OR "cron_expression": "0 9 * * MON-FRI"  // for cron
}
```

When the schedule fires, the worker POSTs `payload` to `COMMON_SERVICE_URL + path`.

---

## 3. High-Level Architecture

```
                                                       
                       ┌──────────────────────────┐
                       │  Consumer Service (BFF)  │
                       │  http://localhost:5000   │
                       │  /task/status endpoint   │
                       └────────────▲─────────────┘
                                    │  POST
                                    │  (fires here)
     ┌──────────────────────────────┼────────────────────────────────────────┐
     │                              │                                        │
     │              ┌───────────────┴────────────────┐                       │
     │              │      Celery Worker             │                       │
     │              │   dispatch_webhook task        │                       │
     │              │   (POSTs to consumer)          │                       │
     │              └───────────────▲────────────────┘                       │
     │                              │ dequeue                                │
     │              ┌───────────────┴────────────────┐                       │
     │              │        Redis DB 3              │                       │
     │              │      (Celery Broker)           │                       │
     │              │   pending + ETA tasks          │                       │
     │              └───────────────▲────────────────┘                       │
     │                              │ enqueue                                │
     │              ┌───────────────┴────────────────┐                       │
     │              │        Celery Beat             │                       │
     │              │   reads cron schedules         │                       │
     │              │   from Redis DB 5              │                       │
     │              └───────────────▲────────────────┘                       │
     │                              │ reads                                  │
     │              ┌───────────────┴────────────────┐                       │
     │              │        Redis DB 5              │                       │
     │              │     (RedBeat store)            │                       │
     │              │    cron schedule entries       │                       │
     │              └───────────────▲────────────────┘                       │
     │                              │                                        │
     │              ┌───────────────┴────────────────┐                       │
     │              │      FastAPI  (/schedules)     │                       │
     │              │  create / read / update /      │◄──── API calls        │
     │              │  delete / pause / resume       │                       │
     │              └───┬────────────────────────┬───┘                       │
     │                  │                        │                           │
     │                  │ writes                 │ reads on boot             │
     │                  ▼                        │ (rehydrate)               │
     │        ┌──────────────────┐               │                           │
     │        │   Postgres       │───────────────┘                           │
     │        │   schedules table │                                          │
     │        │   (SOURCE OF     │                                           │
     │        │    TRUTH)        │                                           │
     │        └──────────────────┘                                           │
     │                                                                        │
     │  ┌────────────────────────────────────────────────────────────────┐   │
     │  │  Redis DB 4 (result backend)                                   │   │
     │  │  Every completed task's return value stored here 24h           │   │
     │  └────────────────────────────────────────────────────────────────┘   │
     │                                                                        │
     └────────────────────────── Harmoniq Scheduler ─────────────────────────┘
```

---

## 4. Components — What & Why

### 4.1 FastAPI (`src/main.py`, `src/schedules/router.py`)

**What:** HTTP API server on port 8080 exposing `/schedules/*` endpoints.

**Why FastAPI:**
- Async-native → handles many concurrent schedule creates efficiently
- Automatic OpenAPI/Swagger docs at `/docs` (great for demos)
- Pydantic validation built-in (schema + validators in one place)
- Modern Python typing support

**Key behavior:**
- Every mutation writes to Postgres AND registers with the Scheduler class
- Startup runs `rehydrate_schedules` (see §5)

### 4.2 Celery Worker (`src/celery/tasks.py`)

**What:** Long-running process that consumes tasks from Redis broker and executes `dispatch_webhook`.

**Why Celery:**
- Battle-tested distributed task queue for Python
- Built-in retry with exponential backoff
- Horizontally scalable (multiple worker replicas)
- Result backend integration
- Prefetch + acknowledgment control (no task loss on crash)

**Key behavior:**
- One task defined: `dispatch_webhook(schedule_id, path, payload)`
- Auto-retries on `httpx.HTTPError` (3 attempts, exponential backoff)
- After success → updates Postgres `last_run`; marks onetime as `ended`
- Fast-fails on permanent errors (invalid URL, missing COMMON_SERVICE_URL)

### 4.3 Celery Beat + RedBeat (`beat` process)

**What:** Scheduling engine that reads cron entries from Redis and enqueues tasks at the right time.

**Why RedBeat (instead of plain Celery Beat):**
- Plain Beat uses a local file for schedules → not shared, not dynamic
- RedBeat stores schedules in Redis → **dynamic updates** (create/delete without restart), **shared across replicas** (only one is active via Redis lock)

**Constraint:** Only 1 Beat replica active at a time (RedBeat handles this via a Redis lock).

### 4.4 Scheduler Class (`src/scheduler.py`)

**What:** Thin wrapper around Celery + RedBeat with a clean CRUD API.

**Why a wrapper:**
- Handlers stay decoupled from Celery/RedBeat internals
- Cron expression parsing centralized
- Same interface for onetime vs cron (`create_onetime_schedule` vs `create_cron_schedule`)
- Delete/pause/resume abstracted

### 4.5 Postgres (`src/database.py`)

**What:** Relational DB storing the `schedules` table (source of truth).

**Why Postgres:**
- ACID guarantees for schedule persistence
- Complex query support (list, filter by status, etc.)
- Same DB used by other Harmoniq services — easier ops
- Survives Redis wipes and pod restarts

**Schema:**
```sql
CREATE TABLE schedules (
  id                VARCHAR PRIMARY KEY,
  path              VARCHAR NOT NULL,
  payload           JSON,
  schedule_type     VARCHAR NOT NULL,      -- 'onetime' | 'cron'
  run_at            TIMESTAMPTZ,           -- for onetime
  cron_expression   VARCHAR,               -- for cron
  tags              JSON,
  status            VARCHAR NOT NULL,      -- 'active' | 'paused' | 'ended'
  last_run          TIMESTAMPTZ,
  next_run          TIMESTAMPTZ,
  is_active         BOOLEAN NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL,
  updated_at        TIMESTAMPTZ NOT NULL
);
```

### 4.6 Redis — 3 DBs

**Why 3 separate DBs on same Redis instance:**
- Isolation for debugging (`redis-cli -n 3 KEYS "*"` sees only broker)
- Independent flushing (can wipe broker without losing schedules)
- Different retention semantics per DB

| DB | Purpose | What lives here |
|----|---------|-----------------|
| 3  | Celery broker | Pending tasks, ETA-queued tasks (`unacked`, `celery`) |
| 4  | Result backend | `celery-task-meta-<id>` — task return values (24h TTL) |
| 5  | RedBeat schedule store | `redbeat:<schedule_id>` — cron entries (permanent) |

---

## 5. Data Flow — End-to-End Lifecycle

### 5.1 Create schedule (onetime, fires in 5 min)

```
Consumer
   │
   │  POST /schedules/  { id, path, payload, schedule_type: "onetime", run_at }
   ▼
FastAPI (create_schedule_handler)
   │
   │  1. Validate: run_at > now, path starts with /
   │  2. Check duplicate id → 409 if exists
   │
   ├──▶ Scheduler.create_onetime_schedule(...)
   │         │
   │         └──▶ Celery send_task(task_name, eta=run_at, task_id=schedule_id)
   │                    │
   │                    └──▶ Redis DB 3: task pushed with ETA
   │
   └──▶ Postgres: INSERT INTO schedules
             │
             └──▶ Row persisted (source of truth)

Response: 201 with ScheduleResponse
```

### 5.2 Create schedule (cron, every 15 min)

Same as above but:
- Uses `Scheduler.create_cron_schedule(cron_expression, ...)`
- Writes to **Redis DB 5** (RedBeat entry) instead of DB 3
- Beat process picks it up on next poll (~5s)

### 5.3 Schedule fires (5 min later)

```
For onetime:
   Redis DB 3 → Worker fetches ETA task when eta arrives

For cron:
   Beat reads DB 5 → enqueues to DB 3 at cron time → Worker fetches

Worker
   │
   │  dispatch_webhook(schedule_id, path, payload)
   │
   ├──▶ HTTP POST to: {COMMON_SERVICE_URL}{path}
   │         │
   │         │  Retries on network errors (3x, exponential backoff)
   │         │  Fast-fails on invalid URL
   │         │
   │         └──▶ Consumer service (e.g. BFF localhost:5000/task/status)
   │
   ├──▶ _mark_schedule_fired(schedule_id) — updates Postgres
   │         │
   │         │  onetime → status='ended', is_active=False, last_run=now
   │         │  cron    → last_run=now (stays active for next fire)
   │
   └──▶ Return {status_code, url} → stored in Redis DB 4 (result backend)
```

### 5.4 Rehydrate on startup

```
FastAPI starts (lifespan hook)
   │
   ├──▶ init_db()  — create tables if not exist
   │
   └──▶ rehydrate_schedules(session)
             │
             │  Reads active schedules from Postgres
             │
             │  For each schedule:
             │    - onetime with run_at in future → re-queue to Redis DB 3
             │    - onetime with run_at past      → mark ended (skip)
             │    - cron                          → re-register in Redis DB 5
```

**Why rehydrate matters:** If Redis is wiped (pod restart, deploy, flushdb), schedules are recovered from Postgres. Otherwise long-ETA one-time schedules would silently disappear.

---

## 6. Storage Strategy — Postgres vs Redis

### The principle

**Postgres = source of truth. Redis = runtime cache.**

This is critical because:
- Redis can be wiped for many reasons (pod restart, deploy, manual `FLUSHDB`, crash without persistence)
- Postgres is the durable, backed-up, ACID-compliant store
- Any Redis loss can be rebuilt from Postgres via `rehydrate_schedules`

### Why not just Redis?

Redis alone would mean:
- Any Redis wipe → all schedules lost forever
- No relational query support for UI (`SELECT * FROM schedules WHERE tags @> ...`)
- No history/audit trail
- Weak durability guarantees (unless AOF everysecond)

### Why not just Postgres?

Postgres alone would mean:
- Need to poll DB every second to find "which schedules should fire now?" — inefficient
- No native scheduling primitives (cron, ETA)
- Higher latency than Redis for the fire-time decision

**The hybrid solves both:** Postgres for durability + query, Redis + Celery for fast scheduling primitives.

---

## 7. API Reference

Base URL: `http://localhost:8080`

### Create schedule
```
POST /schedules/
Body: CreateScheduleRequest
Response: 201 ScheduleResponse
Errors: 400 (validation), 409 (duplicate id)
```

### List schedules
```
GET /schedules/
Response: 200 [ScheduleResponse, ...]
```

### Get schedule
```
GET /schedules/{id}
Response: 200 ScheduleResponse | 404
```

### Update schedule
```
PATCH /schedules/{id}
Body: UpdateScheduleRequest  (partial — only fields to change)
Response: 200 ScheduleResponse | 404
```

### Delete schedule
```
DELETE /schedules/{id}
Response: 200 {"message": "Schedule deleted."} | 404
```

### Pause / Resume
```
POST /schedules/{id}/pause   → sets status='paused', disables Redis entry
POST /schedules/{id}/resume  → re-enables

Response: 200 ScheduleResponse | 404 | 409 (if past onetime schedule)
```

### Schema — `CreateScheduleRequest`

| Field           | Type    | Required          | Notes |
|-----------------|---------|-------------------|-------|
| id              | str     | ✅                | Caller-provided unique identifier |
| path            | str     | ✅                | Must start with `/` (auto-prefixed) |
| payload         | Any     | ❌                | JSON body forwarded to webhook |
| schedule_type   | str     | ✅                | `"onetime"` or `"cron"` |
| run_at          | ISO dt  | onetime only     | UTC, must be in future |
| cron_expression | str     | cron only         | 5-field cron: `"0 9 * * MON-FRI"` |
| tags            | dict    | ❌                | Opaque metadata |

---

## 8. Failure Handling & Reliability

### 8.1 Transient network errors

`dispatch_webhook` decorated with:
```python
autoretry_for=(httpx.HTTPError,),
retry_backoff=True,           # exponential delays
retry_backoff_max=60,         # cap at 60s
retry_jitter=True,            # random jitter — avoid thundering herd
max_retries=3,                # total attempts
```

Timeline for a failing endpoint:
```
t=0     → attempt 1 → fail
t=~2s   → attempt 2 → fail
t=~8s   → attempt 3 → fail
t=~24s  → attempt 4 (final) → fail → task marked FAILURE
```

Result FAILURE state stored in Redis DB 4 — visible in Flower + `celery-task-meta-*`.

### 8.2 Permanent errors (fast-fail)

Task returns immediately without retry when:
- `webhook_url` missing scheme (no http:// or https://)
- `COMMON_SERVICE_URL` not configured
- `path` doesn't start with `/`

These are caller/config errors — retrying would burn attempts and pollute logs.

### 8.3 Pod restarts

| What restarts | Impact | Recovery |
|---------------|--------|----------|
| FastAPI       | Zero — schedules already in Redis+Postgres | rehydrate runs on boot (idempotent) |
| Worker        | In-memory prefetched tasks lost — redelivered from broker after visibility_timeout | Automatic |
| Beat          | Missed cron fires during downtime (usually seconds) | Automatic — reads Redis DB 5 on start |
| Redis         | Schedules lost from Redis | Postgres row still exists; rehydrate on next FastAPI restart |
| Postgres      | Service degraded — Redis-only schedules still fire but rehydrate can't run | Restore Postgres from backup |

### 8.4 Validation

Multiple layers of validation:
- **Pydantic** (schema.py) — type + shape validation, cron field validators
- **Handler-level** (handler.py) — business rules (run_at > now, duplicate id)
- **Scheduler-level** (scheduler.py) — cron expression parsing
- **Task-level** (tasks.py) — final defensive checks

---

## 9. Configuration

All config via environment variables (`.env` file loaded automatically).

| Variable                | Default                        | Purpose |
|-------------------------|--------------------------------|---------|
| `DATABASE_URL`          | (required)                     | Postgres async URL |
| `REDIS_URL`             | `redis://localhost:6379`       | Base Redis URL (db number added per role) |
| `REDIS_BROKER_DB`       | `3`                            | Celery broker DB |
| `REDIS_RESULT_DB`       | `4`                            | Celery result backend DB |
| `REDIS_BEAT_DB`         | `5`                            | RedBeat schedule store DB |
| `SCHEDULER_APP_NAME`    | `harmoniq_scheduler`           | Celery app name (for inspect commands) |
| `COMMON_SERVICE_URL`    | *(empty)*                      | Base URL prepended to schedule `path` |
| `WEBHOOK_HTTP_TIMEOUT`  | `30`                           | HTTP timeout for webhook POST (seconds) |
| `LOG_LEVEL`             | `INFO`                         | Python logging level |

---

## 10. Setup & Running Locally

### Prerequisites
- Python 3.10+
- PostgreSQL 14+ (with a `docker` database or your own name)
- Redis 6+

### Install
```bash
python3 -m venv scheduler
source scheduler/bin/activate
pip install -r requirements.txt
```

### Create `.env`
```
DATABASE_URL=postgresql+asyncpg://postgres:12345@localhost:5432/docker
REDIS_URL=redis://localhost:6379
REDIS_BROKER_DB=3
REDIS_RESULT_DB=4
REDIS_BEAT_DB=5
SCHEDULER_APP_NAME=harmoniq_scheduler
COMMON_SERVICE_URL=http://localhost:5000
WEBHOOK_HTTP_TIMEOUT=30
LOG_LEVEL=INFO
```

### Run 4 processes (each in own terminal, venv activated)

**Terminal 1 — API:**
```bash
uvicorn src.main:app --host 0.0.0.0 --port 8080 --reload
```

**Terminal 2 — Celery Worker:**
```bash
celery -A src.celery.celery_instance worker --loglevel=info --concurrency=4
```

**Terminal 3 — Celery Beat:**
```bash
celery -A src.celery.celery_instance beat --loglevel=info
```

**Terminal 4 — Flower (monitoring UI):**
```bash
celery -A src.celery.celery_instance flower --port=5555 --broker=redis://localhost:6379/3
```

**Access:**
- API docs: http://localhost:8080/docs
- Flower: http://localhost:5555

---

## 11. Testing Guide

### 11.1 Onetime schedule (fires in 2 min)

```bash
curl -X POST http://localhost:8080/schedules/ \
  -H "Content-Type: application/json" \
  -d '{
    "id": "test_onetime_001",
    "path": "/task/status",
    "payload": {"test": "hello"},
    "schedule_type": "onetime",
    "run_at": "'$(date -u -d '+2 minutes' +'%Y-%m-%dT%H:%M:%SZ')'"
  }'
```

Watch Terminal 2 (worker) for `dispatch_webhook succeeded` after ~2 min.

Verify:
```bash
curl http://localhost:8080/schedules/test_onetime_001
# Expect: last_run populated, status="ended", is_active=false
```

### 11.2 Cron schedule (every 2 min)

```bash
curl -X POST http://localhost:8080/schedules/ \
  -H "Content-Type: application/json" \
  -d '{
    "id": "test_cron_001",
    "path": "/task/status",
    "payload": {"test": "cron"},
    "schedule_type": "cron",
    "cron_expression": "*/2 * * * *"
  }'
```

Check Redis DB 5:
```bash
redis-cli -n 5 KEYS "redbeat:*"
# Expect: redbeat:test_cron_001
```

Every 2 min, worker logs will show fire. Results in Redis DB 4:
```bash
redis-cli -n 4 KEYS "celery-task-meta-*"
```

### 11.3 Pause / Resume

```bash
curl -X POST http://localhost:8080/schedules/test_cron_001/pause
# → cron entry disabled, no more fires

curl -X POST http://localhost:8080/schedules/test_cron_001/resume
# → cron entry re-enabled, fires resume
```

### 11.4 Rehydrate

Stop everything (kill uvicorn, worker, beat).
Wipe Redis:
```bash
redis-cli -n 3 FLUSHDB
redis-cli -n 5 FLUSHDB
```
Restart uvicorn. Startup logs will show:
```
INFO src.schedules.handler rehydrate complete
INFO src.main Rehydrate summary: {'restored': N, 'skipped': M, 'failed': 0}
```
Redis will be repopulated from Postgres.

---

## 12. Comparison: AWS vs This Service

| AWS EventBridge + Lambda | Harmoniq Scheduler |
|---|---|
| EventBridge Scheduler rule | Row in Postgres `schedules` table |
| Cron / rate / at() expression | `cron_expression` or `run_at` |
| Target Lambda ARN | `COMMON_SERVICE_URL` + `path` |
| Target input (JSON) | `payload` |
| Schedule state (ENABLED/DISABLED) | `status` (`active` / `paused` / `ended`) |
| EventBridge scheduling engine | RedBeat (cron) + Celery ETA (onetime) |
| Lambda invocation | HTTP POST via `dispatch_webhook` |
| Lambda function code | Consumer's endpoint (opaque to scheduler) |
| CloudWatch retry policy | Celery `autoretry_for` + backoff |
| CloudWatch task history | Celery result backend (Redis DB 4) + Flower UI |
| IAM policies | *(not implemented — future work)* |
| Auto-scaling | Manual (add worker replicas) |

**Bottom line:** conceptually equivalent, self-hosted, no AWS dependency.

---

## 13. Trade-offs & Future Work

### Current limitations

| Limitation | Impact | Fix |
|---|---|---|
| No API authentication | Anyone can hit `/schedules/*` | Add API key middleware or JWT auth |
| No pagination on list | Large lists blow up memory | Add `?limit=&offset=` params |
| `next_run` field never populated | UI can't show "next fire time" | Compute via `croniter` on write |
| Default `visibility_timeout` (1h) | Long-ETA tasks may duplicate-deliver | Set `broker_transport_options={"visibility_timeout": 60*60*24*60}` for 60-day ETAs |
| No webhook HMAC signatures | Consumer can't verify request origin | Add HMAC header signing |
| `init_db()` uses `create_all` | Not production-safe for migrations | Add Alembic |
| No dead-letter queue | Failed tasks after max_retries just log | Route to a DLQ / notification |
| Zero tests | Refactors are risky | Add pytest coverage for CRUD + rehydrate |
| No health check for deps | `/health` returns OK even if Redis/DB down | Add dep pings in `/health` |

### Architectural strengths

- **Generic / consumer-agnostic** — same scheduler can serve many services
- **Idempotent operations** — safe to retry create/update
- **Recoverable state** — Postgres → Redis rehydrate covers wipes
- **Fast-fail permanent errors** — no infinite retry loops
- **Clean layering** — schema / router / handler / scheduler / task separation
- **Observable** — Flower for tasks, logs at every stage, DB inspection possible

### Production-readiness checklist

1. ☐ Enable Postgres backups (AWS RDS automated if hosted)
2. ☐ Enable Redis persistence (AOF everysecond)
3. ☐ Set proper `visibility_timeout` in Celery config
4. ☐ Add API authentication
5. ☐ Add rate limiting on `/schedules/*`
6. ☐ Migrate to Alembic
7. ☐ Add Prometheus metrics
8. ☐ Deploy with Kubernetes (`k8s/` folder has manifests)
9. ☐ Managed Redis (ElastiCache) instead of self-hosted
10. ☐ Beat: `replicas: 1` with `strategy: Recreate` (already in k8s config)

---

## File Structure Reference

```
harmoniq_scheduler/
├── .env                       # env vars (Postgres, Redis, common URL)
├── docker-compose.yml         # Full local stack (postgres, redis, api, worker, beat, flower)
├── Dockerfile                 # Same image for api/worker/beat
├── k8s/                       # Kubernetes deployment manifests
├── pyproject.toml             # Package metadata + deps
├── requirements.txt           # Pinned dependencies
├── POC.md                     # This document
└── src/
    ├── __init__.py            # Package entry; loads .env; re-exports SchedulerConfig etc.
    ├── main.py                # FastAPI entrypoint; lifespan hook runs init_db + rehydrate
    ├── config.py              # SchedulerConfig dataclass + config_from_env
    ├── base.py                # SQLAlchemy DeclarativeBase
    ├── database.py            # Schedule ORM model
    ├── db_session.py          # Async engine + get_db dependency
    ├── app.py                 # create_celery_app factory
    ├── scheduler.py           # Scheduler class (CRUD over RedBeat + Celery)
    ├── celery/
    │   ├── __init__.py
    │   ├── celery_instance.py # Module-level singleton celery_app
    │   └── tasks.py           # dispatch_webhook task + _mark_schedule_fired
    └── schedules/
        ├── __init__.py
        ├── schema.py          # Pydantic Create/Update/Response models + validators
        ├── router.py          # FastAPI routes (POST/GET/PATCH/DELETE/pause/resume)
        └── handler.py         # Business logic + rehydrate_schedules
```
