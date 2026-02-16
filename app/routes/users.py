from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..schemas import KafkaUser
from ..storage import delete_user, list_users, upsert_user

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/")
async def users_list():
    return list_users()


@router.post("/")
async def users_upsert(user: KafkaUser):
    upsert_user(user)
    return {"status": "saved"}


@router.delete("/{username}")
async def users_delete(username: str):
    if not delete_user(username):
        raise HTTPException(status_code=404, detail="user not found")
    return {"status": "deleted"}
