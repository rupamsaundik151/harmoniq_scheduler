# harmoniq-scheduler

A Celery + RedBeat based scheduling service that replaces the AWS Lambda +
EventBridge trigger backend. It fires HTTP webhooks on a cron expression or at a
specific one-time timestamp, persists schedules in PostgreSQL, and dispatches
them through Celery workers backed by Redis.

> **Private package** — install access is granted per-user. If you don't have
> access to the GitHub repo, ask the maintainer for a personal access token or
> SSH key access.

---

## What it does

- **Cron schedules** — recurring jobs defined by a 5-field cron expression
  (`minute hour day-of-month month day-of-week`), optionally bounded by
  `starts_at` / `ends_at` windows.
- **One-time schedules** — a job that fires exactly once at a future timestamp.
- **HTTP webhook dispatch** — when a schedule fires, the worker POSTs a JSON
  payload to `<COMMON_SERVICE_URL><path>` with automatic retries on transient
  HTTP errors.
- **CRUD REST API** — FastAPI endpoints to create, list, get, update, delete,
  reschedule, pause, and resume schedules.
- **Rehydration on startup** — on API boot the DB is scanned and any active
  schedules missing from RedBeat are re-registered, so a redis flush or fresh
  deploy never loses schedules.

---

## Architecture

```
    ┌──────────────┐    HTTP     ┌──────────────┐
    │  Your app    │ ──────────▶ │  FastAPI API │ ──▶ PostgreSQL (schedules)
    └──────────────┘   /schedules└──────┬───────┘
                                        │ register
                                        ▼
                                ┌──────────────┐
                                │    Redis     │◀── RedBeat (schedule store)
                                │  (broker +   │
                                │   backend)   │
                                └──────┬───────┘
                                       │ enqueue at fire time
                          ┌────────────┴────────────┐
                          ▼                         ▼
                  ┌──────────────┐          ┌──────────────┐
                  │ Celery Beat  │          │Celery Worker │
                  │ (1 replica)  │          │(N replicas)  │
                  └──────────────┘          └──────┬───────┘
                                                   │ POST
                                                   ▼
                                          ┌──────────────┐
                                          │ Common       │
                                          │ Service      │
                                          └──────────────┘
```

- **API** ([main.py](src/harmoniq_scheduler/main.py)) — FastAPI app exposing
  `/schedules/*` and `/health`.
- **Beat** — a single Celery Beat process running `RedBeatScheduler`, which
  reads schedule entries from Redis and enqueues tasks when they're due. Must
  stay at 1 replica (RedBeat serializes via a Redis lock; more than one is safe
  but wasted).
- **Worker** ([celery/tasks.py](src/harmoniq_scheduler/celery/tasks.py)) —
  horizontally scalable Celery workers that execute `dispatch_webhook`, POST
  the payload, and update the DB row (`last_run`, `status`).
- **PostgreSQL** — source of truth for schedule metadata.
- **Redis** — Celery broker, result backend, and RedBeat schedule store
  (separate DB indexes: broker=3, result=4, beat=5 by default).

---

## Project structure

```
harmoniq_scheduler/
├── pyproject.toml               ← package metadata + dependencies (install recipe)
├── README.md
├── requirements.txt             ← dev / deployment pins (not used at install)
├── k8s/                         ← reference Kubernetes manifests for deployment
│   ├── 00-namespace.yaml
│   ├── 10-configmap.yaml
│   ├── 11-secret.yaml
│   ├── 20-postgres.yaml
│   ├── 21-redis.yaml
│   ├── 30-api.yaml              ← FastAPI deployment + service
│   ├── 31-worker.yaml           ← Celery worker deployment + HPA
│   ├── 32-beat.yaml             ← Celery beat (Recreate strategy, 1 replica)
│   └── 40-flower.yaml           ← Flower monitoring UI
└── src/
    └── harmoniq_scheduler/      ← the installable package (pip installs this)
        ├── __init__.py          ← public exports: Scheduler, SchedulerConfig, create_celery_app
        ├── app.py               ← create_celery_app() — Celery factory
        ├── config.py            ← SchedulerConfig + config_from_env()
        ├── scheduler.py         ← Scheduler class — cron / one-time / update / delete
        ├── main.py              ← FastAPI entrypoint (uvicorn harmoniq_scheduler.main:app)
        ├── base.py              ← SQLAlchemy declarative Base
        ├── database.py          ← Schedule ORM model
        ├── db_session.py        ← async engine + session factory + init_db()
        ├── celery/
        │   ├── celery_instance.py   ← the celery_app instance workers/beat load
        │   └── tasks.py             ← dispatch_webhook task
        └── schedules/
            ├── router.py        ← FastAPI /schedules routes
            ├── handler.py       ← business logic + rehydrate_schedules()
            └── schema.py        ← pydantic request / response models
```

The `src/` folder is a Python packaging convention (src-layout). Only the
`harmoniq_scheduler/` directory inside it is what gets installed — the
`src/` wrapper, `k8s/`, and repo-root files are NOT shipped to consumers.

---

## Install

The package is hosted in a private GitHub repository, so `pip install
harmoniq-scheduler` from public PyPI will not work. Use one of the two options
below.

### Option 1 — HTTPS with a personal access token

Create a GitHub PAT with read access to the `harmoniq_scheduler` repo
(Settings → Developer settings → Personal access tokens → Fine-grained tokens),
then:

```bash
pip install git+https://<GITHUB_TOKEN>@github.com/rupamsaundik151/harmoniq_scheduler.git
```

Pin to a specific branch / tag / commit:

```bash
pip install git+https://<GITHUB_TOKEN>@github.com/rupamsaundik151/harmoniq_scheduler.git@main
pip install git+https://<GITHUB_TOKEN>@github.com/rupamsaundik151/harmoniq_scheduler.git@v0.1.0
```

### Option 2 — SSH

If your GitHub account has SSH access to the repo:

```bash
pip install git+ssh://git@github.com/rupamsaundik151/harmoniq_scheduler.git
```

### In `requirements.txt`

```
harmoniq-scheduler @ git+https://<GITHUB_TOKEN>@github.com/rupamsaundik151/harmoniq_scheduler.git@main
```

### In `pyproject.toml`

```toml
[project]
dependencies = [
    "harmoniq-scheduler @ git+https://<GITHUB_TOKEN>@github.com/rupamsaundik151/harmoniq_scheduler.git@main",
]
```

---

## Usage

### As a library

```python
from harmoniq_scheduler import Scheduler, SchedulerConfig, create_celery_app

config = SchedulerConfig(redis_url="redis://localhost:6379")
celery_app = create_celery_app(config)
scheduler = Scheduler(celery_app)

scheduler.create_cron_schedule(
    schedule_id="daily-report",
    task_name="myapp.tasks.send_report",
    cron_expression="0 9 * * *",
)
```

### As a service (running the bundled API + worker + beat)

Once installed, the three processes are launched as:

```bash
# API
uvicorn harmoniq_scheduler.main:app --host 0.0.0.0 --port 8080

# Worker (scale horizontally)
celery -A harmoniq_scheduler.celery.celery_instance worker --loglevel=info --concurrency=4

# Beat (exactly 1 replica)
celery -A harmoniq_scheduler.celery.celery_instance beat --loglevel=info
```

Reference Kubernetes manifests are in [k8s/](k8s/).

---

## Configuration

Environment variables read by `config_from_env()`:

| Variable                | Default                      | Purpose                                     |
| ----------------------- | ---------------------------- | ------------------------------------------- |
| `REDIS_URL`             | `redis://localhost:6379`     | Redis endpoint (broker + backend + RedBeat) |
| `DATABASE_URL`          | —                            | PostgreSQL async URL (asyncpg)              |
| `SCHEDULER_APP_NAME`    | `harmoniq_scheduler`         | Celery app name                             |
| `REDIS_BROKER_DB`       | `3`                          | Redis DB index for broker                   |
| `REDIS_RESULT_DB`       | `4`                          | Redis DB index for results                  |
| `REDIS_BEAT_DB`         | `5`                          | Redis DB index for RedBeat                  |
| `WEBHOOK_HTTP_TIMEOUT`  | `30`                         | HTTP timeout for webhook dispatch (seconds) |
| `COMMON_SERVICE_URL`    | —                            | Base URL prepended to each schedule's path  |
| `LOG_LEVEL`             | `INFO`                       | stdlib logging level                        |

---

## Requirements

- Python 3.10+
- Redis (broker + RedBeat schedule store)
- PostgreSQL (for the bundled schedules API)
