from __future__ import annotations

from fastapi import APIRouter

from ..schemas import KafkaConnection
from ..services import topics as topic_service

router = APIRouter(prefix="/retention", tags=["retention"])


@router.post("/{topic}")
async def get_retention(connection: KafkaConnection, topic: str):
    config = await topic_service.describe_topic_config(connection, topic)
    return {"topic": topic, "retention_ms": config.configs.get("retention.ms")}
