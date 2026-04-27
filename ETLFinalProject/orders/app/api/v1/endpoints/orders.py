from fastapi import APIRouter, Query, status

from app.services.order_service import OrderService


router = APIRouter()


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_orders(
     order_amount: int = Query(..., gt=0, le=5000)
):
    return await OrderService.generate_and_store_orders(order_amount)