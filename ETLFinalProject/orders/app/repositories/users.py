from datetime import datetime, timezone

from bson import ObjectId
from bson.errors import InvalidId

from app.db.db import db
from app.models.users import UserCreate, UserInDB


class UserRepository:
    collection = db["users"]

    @staticmethod
    def _serialize_user(user: dict) -> UserInDB:
        user_data = user.copy()
        user_data["id"] = str(user_data.pop("_id"))
        return UserInDB(**user_data)

    @staticmethod
    def _to_object_id(id: str) -> ObjectId | None:
        try:
            return ObjectId(id)
        except InvalidId:
            return None

    @classmethod
    async def create(cls, user: UserCreate) -> UserInDB | None:
        user_doc = user.model_dump()
        user_doc["created_at"] = datetime.now(timezone.utc)

        result = await cls.collection.insert_one(user_doc)
        created_user = await cls.collection.find_one({"_id": result.inserted_id})
        if not created_user:
            return None
        return cls._serialize_user(created_user)

    @classmethod
    async def list(cls) -> list[UserInDB]:
        users = []
        async for user in cls.collection.find():
            users.append(cls._serialize_user(user))
        return users

    @classmethod
    async def get_by_id(cls, id: str) -> UserInDB | None:
        object_id = cls._to_object_id(id)
        if not object_id:
            return None

        user = await cls.collection.find_one({"_id": object_id})
        if not user:
            return None
        return cls._serialize_user(user)

    @classmethod
    async def email_exists(cls, email: str, exclude_id: str | None = None) -> bool:
        query: dict = {"email": email}
        if exclude_id is not None:
            object_id = cls._to_object_id(exclude_id)
            if object_id:
                query["_id"] = {"$ne": object_id}

        return await cls.collection.find_one(query) is not None

    @classmethod
    async def update(cls, id: str, update_data: dict) -> UserInDB | None:
        object_id = cls._to_object_id(id)
        if not object_id:
            return None

        result = await cls.collection.update_one({"_id": object_id}, {"$set": update_data})
        if result.matched_count == 0:
            return None
        return await cls.get_by_id(id)

    @classmethod
    async def delete(cls, id: str) -> bool:
        object_id = cls._to_object_id(id)
        if not object_id:
            return False

        result = await cls.collection.delete_one({"_id": object_id})
        return result.deleted_count > 0
