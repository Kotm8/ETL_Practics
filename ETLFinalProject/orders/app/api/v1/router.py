from fastapi import APIRouter
from app.api.v1.endpoints import orders, users

router = APIRouter()

router.include_router(orders.router, prefix="/orders", tags=["orders"])
router.include_router(users.router, prefix="/users", tags=["users"])
