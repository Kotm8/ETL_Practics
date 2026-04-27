from contextlib import asynccontextmanager
from app.api.v1.router import router
from fastapi import FastAPI

app = FastAPI()
app.include_router(router, prefix="/api/v1")


@app.get("/health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}

