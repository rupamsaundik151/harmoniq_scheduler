from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ..db_session import get_db
from .handler import (
    create_schedule_handler,
    delete_schedule_handler,
    get_schedule_handler,
    list_schedules_handler,
    pause_schedule_handler,
    resume_schedule_handler,
    update_schedule_handler,
)
from .schema import (
    CreateScheduleRequest,
    ScheduleResponse,
    UpdateScheduleRequest,
)

router = APIRouter()


@router.post("/", response_model=ScheduleResponse)
async def create_schedule(
    body: CreateScheduleRequest, db: AsyncSession = Depends(get_db)
) -> ScheduleResponse:
    return await create_schedule_handler(body, db)


@router.get("/", response_model=list[ScheduleResponse])
async def list_schedules(
    db: AsyncSession = Depends(get_db),
) -> list[ScheduleResponse]:
    return await list_schedules_handler(db)


@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule(
    schedule_id: str, db: AsyncSession = Depends(get_db)
) -> ScheduleResponse:
    return await get_schedule_handler(schedule_id, db)


@router.patch("/{schedule_id}", response_model=ScheduleResponse)
async def update_schedule(
    schedule_id: str,
    body: UpdateScheduleRequest,
    db: AsyncSession = Depends(get_db),
) -> ScheduleResponse:
    return await update_schedule_handler(schedule_id, body, db)


@router.delete("/{schedule_id}")
async def delete_schedule(schedule_id: str, db: AsyncSession = Depends(get_db)):
    return await delete_schedule_handler(schedule_id, db)


@router.post("/{schedule_id}/pause", response_model=ScheduleResponse)
async def pause_schedule(
    schedule_id: str, db: AsyncSession = Depends(get_db)
) -> ScheduleResponse:
    return await pause_schedule_handler(schedule_id, db)


@router.post("/{schedule_id}/resume", response_model=ScheduleResponse)
async def resume_schedule(
    schedule_id: str, db: AsyncSession = Depends(get_db)
) -> ScheduleResponse:
    return await resume_schedule_handler(schedule_id, db)
