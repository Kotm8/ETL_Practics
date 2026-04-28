from fastapi import HTTPException, status
from app.models.users import UserCreate, UserInDB, UserUpdate
from app.repositories.users import UserRepository


class UserService:
    @staticmethod
    async def create_user(user: UserCreate) -> UserInDB:
        if await UserRepository.email_exists(user.email):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="User with this email already exists",
            )

        created_user = await UserRepository.create(user)
        if not created_user:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="User was created but could not be retrieved",
            )
        return created_user

    @staticmethod
    async def get_users() -> list[UserInDB]:
        return await UserRepository.list()

    @staticmethod
    async def get_user(id: str) -> UserInDB:
        user = await UserRepository.get_by_id(id)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found",
            )
        return user

    @staticmethod
    async def update_user(id: str, user_update: UserUpdate) -> UserInDB:
        existing_user = await UserRepository.get_by_id(id)
        if not existing_user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found",
            )

        update_data = user_update.model_dump(exclude_unset=True)
        if not update_data:
            return existing_user

        if "email" in update_data and await UserRepository.email_exists(
            update_data["email"], exclude_id=id
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="User with this email already exists",
            )

        updated_user = await UserRepository.update(id, update_data)
        if not updated_user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found",
            )
        return updated_user

    @staticmethod
    async def delete_user(id: str) -> None:
        if not await UserRepository.delete(id):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found",
            )
