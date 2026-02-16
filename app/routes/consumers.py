from __future__ import annotations

from fastapi import APIRouter

from ..schemas import KafkaConnection
from ..services import consumers as consumer_service

router = APIRouter(prefix="/consumers", tags=["consumers"])


@router.post("/groups")
async def list_groups(connection: KafkaConnection):
    return await consumer_service.list_consumer_groups(connection)


@router.post("/groups/{group_id}/lags")
async def group_lags(connection: KafkaConnection, group_id: str):
    return await consumer_service.get_consumer_lag(connection, group_id)
