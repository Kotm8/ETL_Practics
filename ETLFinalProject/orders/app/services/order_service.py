from fastapi import HTTPException, status

from app.services.user_service import UserService
from app.repositories.counters import CounterRepository
from app.core.minio_client import get_minio_client
from app.core.config import MINIO_BUCKET_NAME
from datetime import datetime, time
from io import BytesIO, StringIO
import csv
import random


class OrderService:
    @staticmethod
    async def generate_and_store_orders(order_amount: int):
        now = datetime.now()
        date = now.date()

        users_db = await UserService.get_users()
        if len(users_db) == 0:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Cannot generate orders because no users exist",
            )
        client = get_minio_client()

        counter = await CounterRepository.increment("order_id", order_amount)
        first_order_id = counter.value - order_amount + 1

        folder = f"etl-raw/orders/load_date={date}"
        file_name = f"orders_{date}.csv"
        object_name = f"{folder}/{file_name}"

        orders = OrderService.generate_orders(
            order_amount,
            users_db,
            date,
            first_order_id,
        )

        csv_buffer = StringIO()
        writer = csv.DictWriter(
            csv_buffer,
            fieldnames=[
                "order_id",
                "customer_id",
                "order_ts",
                "revenue",
                "delivery_quality",
                "product_category",
                "customer_satisfaction",
            ],
        )

        writer.writeheader()
        writer.writerows(orders)

        data = csv_buffer.getvalue().encode("utf-8")

        client.put_object(
            MINIO_BUCKET_NAME,
            object_name,
            BytesIO(data),
            length=len(data),
            content_type="text/csv",
        )

        return {
            "file": object_name,
            "first_order_id": first_order_id,
            "last_order_id": counter.value,
            "orders_created": order_amount,
        }

    @staticmethod
    def generate_orders(order_amount: int, users_db, date, first_order_id: int):
        orders = []

        for i in range(order_amount):
            order_id = first_order_id + i
            user = random.choice(users_db)

            order_datetime = datetime.combine(
                date,
                time(
                    hour=random.randint(0, 23),
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59),
                ),
            )

            orders.append(
                {
                    "order_id": order_id,
                    "customer_id": user.id,
                    "order_ts": order_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                    "revenue": round(random.uniform(100, 10000), 2),
                    "delivery_quality": random.randint(1, 5),
                    "product_category": 3,
                    "customer_satisfaction": random.randint(1, 5),
                }
            )

        return orders
