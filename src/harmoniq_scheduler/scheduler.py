from datetime import datetime, timezone as _tz
from typing import Any

from celery import Celery
from celery.schedules import crontab
from redbeat import RedBeatSchedulerEntry


class Scheduler:
    def __init__(self, celery_app: Celery) -> None:
        self.app = celery_app

    def _redbeat_key(self, schedule_id: str) -> str:
        return f"redbeat:{schedule_id}"

    def _coerce_to_utc(self, dt: datetime | str) -> datetime:
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_tz.utc)
        return dt

    def _crontab_from_expression(self, expression: str) -> crontab:
        parts = expression.strip().split()
        if len(parts) != 5:
            raise ValueError(
                f"Cron expression must have 5 fields "
                f"(minute hour day_of_month month day_of_week), got: {expression!r}"
            )
        minute, hour, day_of_month, month, day_of_week = parts
        return crontab(
            minute=minute,
            hour=hour,
            day_of_month=day_of_month,
            month_of_year=month,
            day_of_week=day_of_week,
        )

    def create_onetime_schedule(
        self,
        schedule_id: str,
        task_name: str,
        run_at: datetime | str,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> str:
        eta = self._coerce_to_utc(run_at)
        self.app.send_task(
            task_name,
            args=list(args or []),
            kwargs=dict(kwargs or {}),
            eta=eta,
            task_id=schedule_id,
        )
        return schedule_id

    def create_cron_schedule(
        self,
        schedule_id: str,
        task_name: str,
        cron_expression: str,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        enabled: bool = True,
    ) -> str:
        entry = RedBeatSchedulerEntry(
            name=schedule_id,
            task=task_name,
            schedule=self._crontab_from_expression(cron_expression),
            args=list(args or []),
            kwargs=dict(kwargs or {}),
            app=self.app,
        )
        entry.enabled = enabled
        entry.save()
        return schedule_id

    def set_schedule_state(self, schedule_id: str, enabled: bool) -> bool:
        try:
            entry = RedBeatSchedulerEntry.from_key(
                self._redbeat_key(schedule_id), app=self.app
            )
            entry.enabled = enabled
            entry.save()
            return True
        except KeyError:
            if not enabled:
                self.app.control.revoke(schedule_id, terminate=False)
                return True
            return False

    def update_onetime_schedule(
        self,
        schedule_id: str,
        task_name: str,
        run_at: datetime | str,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        enabled: bool = True,
    ) -> str:
        self.app.control.revoke(schedule_id, terminate=False)
        if enabled:
            return self.create_onetime_schedule(
                schedule_id, task_name, run_at, args, kwargs
            )
        return schedule_id

    def update_cron_schedule(
        self,
        schedule_id: str,
        task_name: str,
        cron_expression: str,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        enabled: bool = True,
    ) -> str:
        cron = self._crontab_from_expression(cron_expression)
        try:
            entry = RedBeatSchedulerEntry.from_key(
                self._redbeat_key(schedule_id), app=self.app
            )
            entry.task = task_name
            entry.schedule = cron
            entry.args = list(args or [])
            entry.kwargs = dict(kwargs or {})
            entry.enabled = enabled
        except KeyError:
            entry = RedBeatSchedulerEntry(
                name=schedule_id,
                task=task_name,
                schedule=cron,
                args=list(args or []),
                kwargs=dict(kwargs or {}),
                app=self.app,
            )
            entry.enabled = enabled
        entry.save()
        return schedule_id

    def delete_schedule(self, schedule_id: str) -> bool:
        try:
            entry = RedBeatSchedulerEntry.from_key(
                self._redbeat_key(schedule_id), app=self.app
            )
            entry.delete()
        except KeyError:
            pass
        try:
            self.app.AsyncResult(schedule_id).forget()
        except Exception:
            pass
        return True
