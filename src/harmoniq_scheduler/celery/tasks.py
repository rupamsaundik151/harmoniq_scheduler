import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from celery.signals import worker_process_init
from redbeat import RedBeatSchedulerEntry
from sqlalchemy import select

from ..config import config_from_env
from ..database import Schedule
from ..db_session import AsyncSessionLocal, engine
from .celery_instance import celery_app

logger = logging.getLogger(__name__)

_config = config_from_env()


@worker_process_init.connect
def _reset_db_engine_after_fork(**_: object) -> None:
    try:
        asyncio.run(engine.dispose())
    except Exception:
        logger.exception("worker_process_init: engine.dispose failed")

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


async def _mark_schedule_ended(schedule_id: str) -> None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Schedule).where(Schedule.id == schedule_id)
        )
        schedule = result.scalar_one_or_none()
        if schedule is None:
            return
        schedule.status = "ended"
        schedule.is_active = False
        await session.commit()


def _parse_window_bound(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _delete_beat_entry(schedule_id: str) -> None:
    try:
        entry = RedBeatSchedulerEntry.from_key(
            f"redbeat:{schedule_id}", app=celery_app
        )
        entry.delete()
    except KeyError:
        pass
    except Exception:
        logger.exception(
            "dispatch_webhook: failed to delete RedBeat entry after ends_at",
            extra={"schedule_id": schedule_id},
        )


@celery_app.task(
    name="harmoniq_scheduler.celery.tasks.dispatch_webhook",
    bind=True,
    autoretry_for=(httpx.HTTPError,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=3,
)
def dispatch_webhook(
    self,
    schedule_id: str,
    path: str,
    payload=None,
    starts_at: Optional[str] = None,
    ends_at: Optional[str] = None,
) -> dict:
    now = datetime.now(timezone.utc)
    starts_at_dt = _parse_window_bound(starts_at)
    ends_at_dt = _parse_window_bound(ends_at)

    if starts_at_dt is not None and now < starts_at_dt:
        logger.info(
            "dispatch_webhook: before starts_at — skipping (no retry)",
            extra={
                "schedule_id": schedule_id,
                "path": path,
                "starts_at": starts_at,
            },
        )
        return {
            "status_code": None,
            "path": path,
            "skipped": "before_starts_at",
        }

    if ends_at_dt is not None and now > ends_at_dt:
        logger.info(
            "dispatch_webhook: after ends_at — skipping + ending schedule",
            extra={
                "schedule_id": schedule_id,
                "path": path,
                "ends_at": ends_at,
            },
        )
        _delete_beat_entry(schedule_id)
        try:
            asyncio.run(_mark_schedule_ended(schedule_id))
        except Exception:
            logger.exception(
                "dispatch_webhook: mark_schedule_ended failed",
                extra={"schedule_id": schedule_id},
            )
        return {
            "status_code": None,
            "path": path,
            "skipped": "after_ends_at",
        }

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
