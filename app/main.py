from __future__ import annotations

from fastmcp import FastMCP
from fastmcp.settings import ServerSettings
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.responses import Response
from starlette.routing import Mount, Route
import uvicorn
from .schemas import (
    KafkaConnection,
    KafkaUser,
    LiveMessageRequest,
    MessageDecodeOptions,
    TailRequest,
    TopicCreate,
)
from .services import brokers, consumers, topics
from .storage import delete_user, list_users, upsert_user

mcp = FastMCP("kafka-mcp-server")


@mcp.tool()
async def health() -> dict:
    return {"status": "ok"}


@mcp.tool()
async def list_topics(connection: KafkaConnection):
    return await topics.list_topics(connection)


@mcp.tool()
async def create_topic(connection: KafkaConnection, payload: TopicCreate):
    await topics.create_topic(connection, payload)
    return {"status": "created"}


@mcp.tool()
async def delete_topic(connection: KafkaConnection, name: str):
    await topics.delete_topic(connection, name)
    return {"status": "deleted"}


@mcp.tool()
async def topic_configs(connection: KafkaConnection, name: str):
    return await topics.describe_topic_config(connection, name)


@mcp.tool()
async def topic_retention(connection: KafkaConnection, name: str):
    config = await topics.describe_topic_config(connection, name)
    return {"topic": name, "retention_ms": config.configs.get("retention.ms")}


@mcp.tool()
async def tail_messages(connection: KafkaConnection, name: str, payload: TailRequest):
    return await topics.tail_messages(
        connection,
        name,
        payload.limit,
        decode_options=MessageDecodeOptions(),
    )


@mcp.tool()
async def live_messages(
    connection: KafkaConnection, name: str, payload: LiveMessageRequest
):
    return await topics.collect_live_messages(
        connection,
        name,
        payload.max_messages,
        payload.duration_seconds,
        payload.poll_interval_ms,
        decode_options=MessageDecodeOptions(),
    )


@mcp.tool()
async def list_consumer_groups(connection: KafkaConnection):
    return await consumers.list_consumer_groups(connection)


@mcp.tool()
async def consumer_group_lag(connection: KafkaConnection, group_id: str):
    return await consumers.get_consumer_lag(connection, group_id)


@mcp.tool()
async def cluster_info(connection: KafkaConnection):
    return await brokers.get_cluster_info(connection)


@mcp.tool()
async def list_kafka_users():
    return list_users()


@mcp.tool()
async def upsert_kafka_user(user: KafkaUser):
    upsert_user(user)
    return {"status": "saved"}


@mcp.tool()
async def delete_kafka_user(username: str):
    if not delete_user(username):
        return {"status": "not_found"}
    return {"status": "deleted"}


def run() -> None:
    settings = ServerSettings()
    sse = SseServerTransport(settings.message_path)

    async def handle_sse(request):
        async with sse.connect_sse(
            request.scope,
            request.receive,
            request._send,  # type: ignore[reportPrivateUsage]
        ) as streams:
            await mcp._mcp_server.run(
                streams[0],
                streams[1],
                mcp._mcp_server.create_initialization_options(),
            )
        return Response()

    app = Starlette(
        debug=settings.debug,
        routes=[
            Route(settings.sse_path, endpoint=handle_sse),
            Mount(settings.message_path, app=sse.handle_post_message),
        ],
    )

    config = uvicorn.Config(
        app,
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
    )
    server = uvicorn.Server(config)
    server.run()


if __name__ == "__main__":
    run()
