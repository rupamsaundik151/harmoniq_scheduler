import uuid

from sqlalchemy import Boolean, Column, DateTime, JSON, String, func

from .base import Base


class Schedule(Base):
    __tablename__ = "schedules"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))

    path = Column(String, nullable=False)
    payload = Column(JSON, nullable=True)

    schedule_type = Column(String, nullable=False)
    run_at = Column(DateTime(timezone=True), nullable=True)
    cron_expression = Column(String, nullable=True)

    tags = Column(JSON, nullable=True)

    status = Column(String, nullable=False, server_default="active")
    last_run = Column(DateTime(timezone=True), nullable=True)
    next_run = Column(DateTime(timezone=True), nullable=True)

    is_active = Column(Boolean, nullable=False, server_default="true")

    created_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
