import os
from dataclasses import dataclass, field


@dataclass
class SchedulerConfig:
    redis_url: str
    app_name: str = "harmoniq_scheduler"
    broker_db: int = 3
    result_db: int = 4
    beat_db: int = 5
    task_modules: list[str] = field(default_factory=list)
    timezone: str = "UTC"
    result_expires: int = 86400
    beat_max_loop_interval: int = 5
    redbeat_lock_timeout: int = 90
    webhook_http_timeout: float = 30.0
    common_service_url: str = ""


def config_from_env() -> SchedulerConfig:
    return SchedulerConfig(
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"),
        app_name=os.getenv("SCHEDULER_APP_NAME", "harmoniq_scheduler"),
        broker_db=int(os.getenv("REDIS_BROKER_DB", "3")),
        result_db=int(os.getenv("REDIS_RESULT_DB", "4")),
        beat_db=int(os.getenv("REDIS_BEAT_DB", "5")),
        task_modules=["src.celery.tasks"],
        webhook_http_timeout=float(os.getenv("WEBHOOK_HTTP_TIMEOUT", "30")),
        common_service_url=os.getenv("COMMON_SERVICE_URL", ""),
    )
