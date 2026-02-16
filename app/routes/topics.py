from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from ..schemas import (
    KafkaConnection,
    LiveMessageRequest,
    MessageDecodeOptions,
    TailRequest,
    TopicCreate,
)
from ..services import topics as topic_service

router = APIRouter(prefix="/topics", tags=["topics"])


@router.post("/list")
async def list_topics(connection: KafkaConnection):
    return await topic_service.list_topics(connection)


@router.post("/create")
async def create_topic(connection: KafkaConnection, payload: TopicCreate):
    await topic_service.create_topic(connection, payload)
    return {"status": "created"}


@router.post("/{topic}/delete")
async def delete_topic(connection: KafkaConnection, topic: str):
    await topic_service.delete_topic(connection, topic)
    return {"status": "deleted"}


@router.post("/{topic}/configs")
async def topic_configs(connection: KafkaConnection, topic: str):
    return await topic_service.describe_topic_config(connection, topic)


@router.post("/{topic}/tail")
async def tail_messages(connection: KafkaConnection, topic: str, payload: TailRequest):
    if payload.limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be positive")
    return await topic_service.tail_messages(
        connection,
        topic,
        payload.limit,
        decode_options=MessageDecodeOptions(),
    )


@router.post("/{topic}/live")
async def live_messages(
    connection: KafkaConnection, topic: str, payload: LiveMessageRequest
):
    if payload.max_messages <= 0:
        raise HTTPException(status_code=400, detail="max_messages must be positive")
    generator = topic_service.stream_live_messages(
        connection,
        topic,
        payload.max_messages,
        payload.duration_seconds,
        payload.poll_interval_ms,
        decode_options=MessageDecodeOptions(),
    )
    return StreamingResponse(generator, media_type="text/event-stream")
