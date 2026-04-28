from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import router
from app.core.minio_client import ensure_minio_ready


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_minio_ready()
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(router, prefix="/api/v1")


@app.get("/health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}

