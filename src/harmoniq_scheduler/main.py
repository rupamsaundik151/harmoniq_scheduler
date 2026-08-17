import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .db_session import AsyncSessionLocal, init_db
from .schedules.handler import rehydrate_schedules
from .schedules.router import router as schedules_router

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_db()
    async with AsyncSessionLocal() as session:
        try:
            summary = await rehydrate_schedules(session)
            logger.info("Rehydrate summary: %s", summary)
        except Exception:
            logger.exception("Rehydrate failed at startup")
    yield


app = FastAPI(
    title="harmoniq-scheduler",
    version="0.2.0",
    lifespan=lifespan,
)

app.include_router(schedules_router, prefix="/schedules", tags=["schedules"])


@app.get("/health", tags=["ops"])
async def health() -> dict:
    return {"status": "ok"}
