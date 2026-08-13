from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ScheduleType = Literal["onetime", "cron"]
ScheduleStatus = Literal["active", "paused", "ended"]


class CreateScheduleRequest(BaseModel):
    id: str = Field(...)
    path: str = Field(...)
    payload: Optional[Any] = None
    schedule_type: ScheduleType
    run_at: Optional[datetime] = None
    cron_expression: Optional[str] = None
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None
    tags: Optional[dict[str, Any]] = None

    @field_validator("path")
    @classmethod
    def _normalize_path(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("path must not be empty.")
        if not v.startswith("/"):
            v = "/" + v
        return v

    @model_validator(mode="after")
    def _validate_type_fields(self) -> "CreateScheduleRequest":
        if self.schedule_type == "onetime":
            if self.run_at is None:
                raise ValueError("`run_at` is required when schedule_type='onetime'.")
            if self.cron_expression is not None:
                raise ValueError(
                    "`cron_expression` must be omitted when schedule_type='onetime'."
                )
            if self.starts_at is not None or self.ends_at is not None:
                raise ValueError(
                    "`starts_at`/`ends_at` are only for schedule_type='cron'."
                )
        elif self.schedule_type == "cron":
            if not self.cron_expression:
                raise ValueError(
                    "`cron_expression` is required when schedule_type='cron'."
                )
            if self.run_at is not None:
                raise ValueError(
                    "`run_at` must be omitted when schedule_type='cron'."
                )
            if (
                self.starts_at is not None
                and self.ends_at is not None
                and self.ends_at <= self.starts_at
            ):
                raise ValueError("`ends_at` must be after `starts_at`.")
        return self


class UpdateScheduleRequest(BaseModel):
    path: Optional[str] = None
    payload: Optional[Any] = None
    schedule_type: Optional[ScheduleType] = None
    run_at: Optional[datetime] = None
    cron_expression: Optional[str] = None
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None
    tags: Optional[dict[str, Any]] = None

    @field_validator("path")
    @classmethod
    def _normalize_path(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip()
        if not v:
            raise ValueError("path must not be empty.")
        if not v.startswith("/"):
            v = "/" + v
        return v


class ScheduleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    path: str
    payload: Optional[Any] = None
    schedule_type: ScheduleType
    run_at: Optional[datetime] = None
    cron_expression: Optional[str] = None
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None
    tags: Optional[dict[str, Any]] = None
    status: ScheduleStatus
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    is_active: bool
    created_at: datetime
    updated_at: datetime
