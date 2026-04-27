from fastapi import HTTPException, status
from pymongo import ReturnDocument

from app.db.db import db
from app.models.counters import CounterInDB


class CounterRepository:
    collection = db["counters"]

    @staticmethod
    def _serialize_counter(counter: dict) -> CounterInDB:
        counter_data = counter.copy()
        counter_data["id"] = str(counter_data.pop("_id"))
        return CounterInDB(**counter_data)

    @classmethod
    async def get_by_id(cls, id: str) -> CounterInDB | None:
        counter = await cls.collection.find_one({"_id": id})
        if not counter:
            return None

        return cls._serialize_counter(counter)

    @classmethod
    async def increment(cls, id: str, amount: int) -> CounterInDB:
        counter = await cls.collection.find_one_and_update(
            {"_id": id},
            {"$inc": {"value": amount}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )

        if counter is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to increment counter",
            )

        return cls._serialize_counter(counter)
