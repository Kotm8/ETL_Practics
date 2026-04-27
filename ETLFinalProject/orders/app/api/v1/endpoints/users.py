from fastapi import APIRouter, status

from app.models.users import UserCreate, UserInDB, UserUpdate
from app.services.user_service import UserService


router = APIRouter()


@router.post("/", response_model=UserInDB, status_code=status.HTTP_201_CREATED)
async def create_user(user: UserCreate) -> UserInDB:
    return await UserService.create_user(user)


@router.get("/", response_model=list[UserInDB])
async def get_users() -> list[UserInDB]:
    return await UserService.get_users()


@router.get("/{id}", response_model=UserInDB)
async def get_user(id: str) -> UserInDB:
    return await UserService.get_user(id)


@router.patch("/{id}", response_model=UserInDB)
async def update_user(id: str, user_update: UserUpdate) -> UserInDB:
    return await UserService.update_user(id, user_update)


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(id: str) -> None:
    await UserService.delete_user(id)
