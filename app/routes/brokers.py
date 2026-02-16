from __future__ import annotations

from fastapi import APIRouter

from ..schemas import KafkaConnection
from ..services import brokers as broker_service

router = APIRouter(prefix="/brokers", tags=["brokers"])


@router.post("/cluster")
async def cluster_info(connection: KafkaConnection):
    return await broker_service.get_cluster_info(connection)
