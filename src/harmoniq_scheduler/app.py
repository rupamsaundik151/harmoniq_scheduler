from celery import Celery

from .config import SchedulerConfig


def _url_with_db(base: str, db: int) -> str:
    base = base.rstrip("/")
    parts = base.rsplit("/", 1)
    if len(parts) == 2 and parts[1].isdigit():
        base = parts[0]
    return f"{base}/{db}"


def create_celery_app(config: SchedulerConfig) -> Celery:
    broker_url = _url_with_db(config.redis_url, config.broker_db)
    result_url = _url_with_db(config.redis_url, config.result_db)
    beat_url = _url_with_db(config.redis_url, config.beat_db)

    app = Celery(
        config.app_name,
        broker=broker_url,
        backend=result_url,
        include=list(config.task_modules),
    )

    app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        timezone=config.timezone,
        enable_utc=True,
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        task_reject_on_worker_lost=False,
        result_expires=config.result_expires,
        beat_scheduler="redbeat.RedBeatScheduler",
        redbeat_redis_url=beat_url,
        redbeat_lock_timeout=config.redbeat_lock_timeout,
        beat_max_loop_interval=config.beat_max_loop_interval,
    )
    return app
