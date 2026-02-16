from __future__ import annotations

import asyncio
import os
from typing import Any, Dict

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from mcp import ClientSession
from mcp.client.sse import sse_client


MCP_URL = os.getenv("MCP_URL", "http://localhost:8000/mcp")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TEST_TOPIC", "mcp_smoke")
GROUP = os.getenv("KAFKA_TEST_GROUP", "mcp_smoke_group")


def _connection() -> Dict[str, Any]:
    return {
        "bootstrap_servers": BOOTSTRAP,
        "security_protocol": "PLAINTEXT",
        "sasl_mechanism": None,
        "sasl_username": None,
        "sasl_password": None,
        "ssl_cafile": None,
        "ssl_certfile": None,
        "ssl_keyfile": None,
        "oauth_token": None,
    }


async def _call_tool(session: ClientSession, name: str, arguments: Dict[str, Any]):
    result = await session.call_tool(name, arguments)
    return getattr(result, "content", result)


async def _produce_messages(messages: list[bytes]) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
    await producer.start()
    try:
        for msg in messages:
            await producer.send_and_wait(TOPIC, msg)
    finally:
        await producer.stop()


async def _consume_one_and_commit() -> None:
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        msg = await consumer.getone()
        await consumer.commit()
        _ = msg.offset
    finally:
        await consumer.stop()


async def _test_tools() -> None:
    async with sse_client(MCP_URL) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            connection = _connection()

            print("list_topics")
            print(await _call_tool(session, "list_topics", {"connection": connection}))

            print("create_topic")
            print(
                await _call_tool(
                    session,
                    "create_topic",
                    {
                        "connection": connection,
                        "payload": {
                            "name": TOPIC,
                            "num_partitions": 1,
                            "replication_factor": 1,
                            "configs": {},
                        },
                    },
                )
            )

            print("topic_configs")
            print(
                await _call_tool(
                    session, "topic_configs", {"connection": connection, "name": TOPIC}
                )
            )

            print("topic_retention")
            print(
                await _call_tool(
                    session,
                    "topic_retention",
                    {"connection": connection, "name": TOPIC},
                )
            )

            await _produce_messages([b"smoke-1", b"smoke-2", b"smoke-3"])

            print("tail_messages")
            print(
                await _call_tool(
                    session,
                    "tail_messages",
                    {
                        "connection": connection,
                        "name": TOPIC,
                        "payload": {"limit": 5},
                    },
                )
            )

            await _consume_one_and_commit()
            await _produce_messages([b"smoke-4", b"smoke-5"])

            print("list_consumer_groups")
            print(
                await _call_tool(
                    session, "list_consumer_groups", {"connection": connection}
                )
            )

            print("consumer_group_lag")
            print(
                await _call_tool(
                    session,
                    "consumer_group_lag",
                    {"connection": connection, "group_id": GROUP},
                )
            )

            async def produce_for_live() -> None:
                await asyncio.sleep(1)
                await _produce_messages([b"live-1", b"live-2"])

            print("live_messages")
            live_task = asyncio.create_task(produce_for_live())
            print(
                await _call_tool(
                    session,
                    "live_messages",
                    {
                        "connection": connection,
                        "name": TOPIC,
                        "payload": {
                            "max_messages": 5,
                            "duration_seconds": 5,
                            "poll_interval_ms": 200,
                        },
                    },
                )
            )
            await live_task

            print("cluster_info")
            print(await _call_tool(session, "cluster_info", {"connection": connection}))

            print("list_kafka_users")
            print(await _call_tool(session, "list_kafka_users", {}))

            print("upsert_kafka_user")
            print(
                await _call_tool(
                    session,
                    "upsert_kafka_user",
                    {
                        "user": {
                            "username": "smoke",
                            "sasl_mechanism": None,
                            "note": "smoke",
                        }
                    },
                )
            )

            print("list_kafka_users")
            print(await _call_tool(session, "list_kafka_users", {}))

            print("delete_kafka_user")
            print(await _call_tool(session, "delete_kafka_user", {"username": "smoke"}))

            print("delete_topic")
            print(
                await _call_tool(
                    session, "delete_topic", {"connection": connection, "name": TOPIC}
                )
            )


def main() -> None:
    asyncio.run(_test_tools())


if __name__ == "__main__":
    main()
