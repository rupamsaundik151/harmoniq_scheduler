import logging
from datetime import datetime, timezone

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..celery.celery_instance import celery_app
from ..database import Schedule
from ..scheduler import Scheduler
from .schema import (
    CreateScheduleRequest,
    ScheduleResponse,
    UpdateScheduleRequest,
)

logger = logging.getLogger(__name__)

_RUNNER_TASK_NAME = "src.celery.tasks.dispatch_webhook"


def _get_scheduler() -> Scheduler:
    return Scheduler(celery_app)


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _runner_kwargs(schedule: Schedule) -> dict:
    return {
        "schedule_id": schedule.id,
        "path": schedule.path,
        "payload": schedule.payload,
    }


def _register_with_scheduler(
    scheduler: Scheduler, schedule: Schedule, enabled: bool = True
) -> None:
    kwargs = _runner_kwargs(schedule)
    if schedule.schedule_type == "onetime":
        if enabled:
            scheduler.create_onetime_schedule(
                schedule_id=schedule.id,
                task_name=_RUNNER_TASK_NAME,
                run_at=_ensure_utc(schedule.run_at),
                kwargs=kwargs,
            )
    elif schedule.schedule_type == "cron":
        scheduler.create_cron_schedule(
            schedule_id=schedule.id,
            task_name=_RUNNER_TASK_NAME,
            cron_expression=schedule.cron_expression,
            kwargs=kwargs,
            enabled=enabled,
        )
    else:
        raise ValueError(f"Unknown schedule_type: {schedule.schedule_type!r}")


async def create_schedule_handler(
    request: CreateScheduleRequest, db: AsyncSession
) -> ScheduleResponse:
    schedule_id = request.id

    existing = await db.get(Schedule, schedule_id)
    if existing is not None:
        raise HTTPException(
            status_code=409,
            detail=f"Schedule with id '{schedule_id}' already exists.",
        )

    if request.schedule_type == "onetime":
        run_at = _ensure_utc(request.run_at)
        if run_at <= datetime.now(timezone.utc):
            raise HTTPException(
                status_code=400,
                detail="`run_at` must be in the future.",
            )
    else:
        run_at = None

    scheduler = _get_scheduler()
    if request.schedule_type == "cron":
        try:
            scheduler._crontab_from_expression(request.cron_expression)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    schedule = Schedule(
        id=schedule_id,
        path=request.path,
        payload=request.payload,
        schedule_type=request.schedule_type,
        run_at=run_at,
        cron_expression=request.cron_expression,
        tags=request.tags,
        status="active",
        is_active=True,
    )

    try:
        _register_with_scheduler(scheduler, schedule, enabled=True)
    except Exception as e:
        logger.exception(
            "create_schedule: scheduler register failed",
            extra={"schedule_id": schedule_id},
        )
        raise HTTPException(
            status_code=500, detail=f"Failed to register schedule: {e}"
        ) from e

    try:
        db.add(schedule)
        await db.commit()
        await db.refresh(schedule)
    except Exception as e:
        await db.rollback()
        scheduler.delete_schedule(schedule_id)
        logger.exception(
            "create_schedule: DB save failed — rolled back schedule",
            extra={"schedule_id": schedule_id},
        )
        raise HTTPException(
            status_code=500, detail=f"Failed to save schedule: {e}"
        ) from e

    return ScheduleResponse.model_validate(schedule)


async def list_schedules_handler(db: AsyncSession) -> list[ScheduleResponse]:
    stmt = (
        select(Schedule)
        .where(Schedule.is_active.is_(True))
        .order_by(Schedule.created_at.desc())
    )
    result = await db.execute(stmt)
    return [ScheduleResponse.model_validate(s) for s in result.scalars().all()]


async def get_schedule_handler(
    schedule_id: str, db: AsyncSession
) -> ScheduleResponse:
    schedule = await db.get(Schedule, schedule_id)
    if schedule is None:
        raise HTTPException(status_code=404, detail="Schedule not found.")
    return ScheduleResponse.model_validate(schedule)


async def update_schedule_handler(
    schedule_id: str, request: UpdateScheduleRequest, db: AsyncSession
) -> ScheduleResponse:
    schedule = await db.get(Schedule, schedule_id)
    if schedule is None:
        raise HTTPException(status_code=404, detail="Schedule not found.")

    update_data = request.model_dump(exclude_unset=True)

    for field_name, value in update_data.items():
        setattr(schedule, field_name, value)

    if schedule.schedule_type == "onetime":
        if schedule.run_at is None:
            raise HTTPException(
                status_code=400,
                detail="`run_at` is required for onetime schedules.",
            )
        schedule.cron_expression = None
        schedule.run_at = _ensure_utc(schedule.run_at)
    elif schedule.schedule_type == "cron":
        if not schedule.cron_expression:
            raise HTTPException(
                status_code=400,
                detail="`cron_expression` is required for cron schedules.",
            )
        schedule.run_at = None

    scheduler = _get_scheduler()
    enabled = schedule.status != "paused"

    try:
        if schedule.schedule_type == "onetime":
            scheduler.update_onetime_schedule(
                schedule_id=schedule.id,
                task_name=_RUNNER_TASK_NAME,
                run_at=schedule.run_at,
                kwargs=_runner_kwargs(schedule),
                enabled=enabled,
            )
        else:
            scheduler.update_cron_schedule(
                schedule_id=schedule.id,
                task_name=_RUNNER_TASK_NAME,
                cron_expression=schedule.cron_expression,
                kwargs=_runner_kwargs(schedule),
                enabled=enabled,
            )
    except Exception as e:
        logger.exception(
            "update_schedule: scheduler update failed",
            extra={"schedule_id": schedule_id},
        )
        raise HTTPException(
            status_code=500, detail=f"Failed to update schedule: {e}"
        ) from e

    try:
        await db.commit()
        await db.refresh(schedule)
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=500, detail=f"Failed to save schedule: {e}"
        ) from e

    return ScheduleResponse.model_validate(schedule)


async def delete_schedule_handler(schedule_id: str, db: AsyncSession) -> dict:
    schedule = await db.get(Schedule, schedule_id)
    if schedule is None:
        raise HTTPException(status_code=404, detail="Schedule not found.")

    try:
        await db.delete(schedule)
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=500, detail=f"Failed to remove schedule row: {e}"
        ) from e

    scheduler = _get_scheduler()
    try:
        scheduler.delete_schedule(schedule_id)
    except Exception:
        logger.exception(
            "delete_schedule: scheduler cleanup failed after DB delete "
            "— task-level guard will suppress any stray fires",
            extra={"schedule_id": schedule_id},
        )

    return {"message": "Schedule deleted."}


async def pause_schedule_handler(
    schedule_id: str, db: AsyncSession
) -> ScheduleResponse:
    schedule = await db.get(Schedule, schedule_id)
    if schedule is None:
        raise HTTPException(status_code=404, detail="Schedule not found.")
    if schedule.status == "paused":
        return ScheduleResponse.model_validate(schedule)

    scheduler = _get_scheduler()
    try:
        scheduler.set_schedule_state(schedule_id, enabled=False)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to pause schedule: {e}"
        ) from e

    schedule.status = "paused"
    await db.commit()
    await db.refresh(schedule)
    return ScheduleResponse.model_validate(schedule)


async def resume_schedule_handler(
    schedule_id: str, db: AsyncSession
) -> ScheduleResponse:
    schedule = await db.get(Schedule, schedule_id)
    if schedule is None:
        raise HTTPException(status_code=404, detail="Schedule not found.")
    if schedule.status == "active":
        return ScheduleResponse.model_validate(schedule)

    scheduler = _get_scheduler()
    success = scheduler.set_schedule_state(schedule_id, enabled=True)
    if not success:
        if schedule.schedule_type == "onetime" and schedule.run_at:
            run_at = _ensure_utc(schedule.run_at)
            if run_at <= datetime.now(timezone.utc):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "One-time schedule's run_at has already passed. "
                        "Update run_at to a future time first."
                    ),
                )
            scheduler.create_onetime_schedule(
                schedule_id=schedule.id,
                task_name=_RUNNER_TASK_NAME,
                run_at=run_at,
                kwargs=_runner_kwargs(schedule),
            )
        else:
            raise HTTPException(
                status_code=409,
                detail="Cannot resume schedule — re-create it instead.",
            )

    schedule.status = "active"
    await db.commit()
    await db.refresh(schedule)
    return ScheduleResponse.model_validate(schedule)


async def rehydrate_schedules(db: AsyncSession) -> dict:
    result = await db.execute(
        select(Schedule).where(Schedule.is_active.is_(True))
    )
    schedules = result.scalars().all()

    scheduler = _get_scheduler()
    now = datetime.now(timezone.utc)
    restored = 0
    skipped = 0
    failed = 0

    for schedule in schedules:
        enabled = schedule.status == "active"
        try:
            if schedule.schedule_type == "onetime":
                if not schedule.run_at:
                    skipped += 1
                    continue
                run_at = _ensure_utc(schedule.run_at)
                if run_at <= now:
                    schedule.status = "ended"
                    skipped += 1
                    continue
                if enabled:
                    scheduler.create_onetime_schedule(
                        schedule_id=schedule.id,
                        task_name=_RUNNER_TASK_NAME,
                        run_at=run_at,
                        kwargs=_runner_kwargs(schedule),
                    )
                    restored += 1
                else:
                    skipped += 1
            elif schedule.schedule_type == "cron":
                if not schedule.cron_expression:
                    skipped += 1
                    continue
                scheduler.update_cron_schedule(
                    schedule_id=schedule.id,
                    task_name=_RUNNER_TASK_NAME,
                    cron_expression=schedule.cron_expression,
                    kwargs=_runner_kwargs(schedule),
                    enabled=enabled,
                )
                restored += 1
            else:
                skipped += 1
        except Exception:
            failed += 1
            logger.exception(
                "rehydrate: failed to restore schedule",
                extra={"schedule_id": schedule.id},
            )

    await db.commit()
    logger.info(
        "rehydrate complete",
        extra={"restored": restored, "skipped": skipped, "failed": failed},
    )
    return {"restored": restored, "skipped": skipped, "failed": failed}
