import asyncio
import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy import select

from ..config import config_from_env
from ..database import Schedule
from ..db_session import AsyncSessionLocal
from .celery_instance import celery_app

logger = logging.getLogger(__name__)

_config = config_from_env()

_FIXED_HEADERS = {
    "accept": "application/json",
    "Content-Type": "application/json",
}


async def _mark_schedule_fired(schedule_id: str) -> None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Schedule).where(Schedule.id == schedule_id)
        )
        schedule = result.scalar_one_or_none()
        if schedule is None:
            logger.warning(
                "mark_schedule_fired: schedule row missing",
                extra={"schedule_id": schedule_id},
            )
            return
        schedule.last_run = datetime.now(timezone.utc)
        if schedule.schedule_type == "onetime":
            schedule.status = "ended"
            schedule.is_active = False
        await session.commit()


@celery_app.task(
    name="src.celery.tasks.dispatch_webhook",
    bind=True,
    autoretry_for=(httpx.HTTPError,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=3,
)
def dispatch_webhook(
    self, schedule_id: str, path: str, payload=None
) -> dict:
    base = (_config.common_service_url or "").strip().rstrip("/")
    if not base:
        logger.error(
            "dispatch_webhook: COMMON_SERVICE_URL not configured — skipping (no retry)",
            extra={"schedule_id": schedule_id, "path": path},
        )
        return {
            "status_code": None,
            "path": path,
            "error": "common_service_url_not_configured",
        }

    if not path or not path.startswith("/"):
        logger.error(
            "dispatch_webhook: invalid path — skipping (no retry)",
            extra={"schedule_id": schedule_id, "path": path},
        )
        return {
            "status_code": None,
            "path": path,
            "error": "invalid_path",
        }

    url = f"{base}{path}"

    logger.info(
        "dispatch_webhook firing",
        extra={
            "schedule_id": schedule_id,
            "url": url,
            "attempt": self.request.retries + 1,
        },
    )

    with httpx.Client(timeout=_config.webhook_http_timeout) as client:
        response = client.post(url, json=payload, headers=_FIXED_HEADERS)
        response.raise_for_status()

    try:
        asyncio.run(_mark_schedule_fired(schedule_id))
    except Exception:
        logger.exception(
            "dispatch_webhook: post-fire DB update failed",
            extra={"schedule_id": schedule_id},
        )

    logger.info(
        "dispatch_webhook succeeded",
        extra={
            "schedule_id": schedule_id,
            "url": url,
            "status": response.status_code,
        },
    )
    return {"status_code": response.status_code, "url": url}
