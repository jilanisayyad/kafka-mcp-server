from __future__ import annotations

import asyncio
from typing import Dict, List

from aiokafka import TopicPartition
from aiokafka.admin import NewTopic

from ..kafka_client import create_admin_client, create_consumer
from ..schemas import (
    KafkaConnection,
    MessageRecord,
    MessageDecodeOptions,
    TopicConfigResponse,
    TopicCreate,
    TopicSummary,
)


def _decode(value: bytes | None, encoding: str, errors: str) -> str | None:
    if value is None:
        return None
    return value.decode(encoding, errors=errors)


async def list_topics(connection: KafkaConnection) -> List[TopicSummary]:
    consumer = create_consumer(connection)
    await consumer.start()
    try:
        topics = await consumer.topics()
        metadata = consumer._client.cluster
        summaries = []
        for topic in sorted(topics):
            partitions = metadata.partitions_for_topic(topic) or set()
            replication_factor = 0
            for partition_id in partitions:
                partition = metadata._partitions.get(
                    TopicPartition(topic, partition_id)
                )
                if partition and partition.replicas:
                    replication_factor = len(partition.replicas)
                    break
            summaries.append(
                TopicSummary(
                    name=topic,
                    partitions=len(partitions),
                    replication_factor=replication_factor,
                )
            )
        return summaries
    finally:
        await consumer.stop()


async def create_topic(connection: KafkaConnection, data: TopicCreate) -> None:
    admin = create_admin_client(connection)
    await admin.start()
    try:
        topic = NewTopic(
            name=data.name,
            num_partitions=data.num_partitions,
            replication_factor=data.replication_factor,
            topic_configs=data.configs,
        )
        await admin.create_topics([topic])
    finally:
        await admin.close()


async def delete_topic(connection: KafkaConnection, name: str) -> None:
    admin = create_admin_client(connection)
    await admin.start()
    try:
        await admin.delete_topics([name])
    finally:
        await admin.close()


async def describe_topic_config(
    connection: KafkaConnection, name: str
) -> TopicConfigResponse:
    admin = create_admin_client(connection)
    await admin.start()
    try:
        resource = ("topic", name)
        configs = await admin.describe_configs([resource])
        cfg = configs[resource]
        return TopicConfigResponse(
            name=name,
            configs={key: value.value for key, value in cfg.items()},
        )
    finally:
        await admin.close()


async def tail_messages(
    connection: KafkaConnection,
    topic: str,
    limit: int,
    decode_options: MessageDecodeOptions,
) -> List[MessageRecord]:
    consumer = create_consumer(connection, group_id=None, auto_offset_reset="latest")
    await consumer.start()
    try:
        partitions = consumer.partitions_for_topic(topic) or set()
        if not partitions:
            return []

        tps = [TopicPartition(topic, p) for p in partitions]
        await consumer.assign(tps)
        end_offsets = await consumer.end_offsets(tps)
        records: List[MessageRecord] = []
        for tp in tps:
            end = end_offsets.get(tp, 0)
            start = max(0, end - limit)
            await consumer.seek(tp, start)

        while len(records) < limit:
            batch = await consumer.getmany(timeout_ms=1000)
            if not batch:
                break
            for tp, messages in batch.items():
                for message in messages:
                    records.append(
                        MessageRecord(
                            partition=tp.partition,
                            offset=message.offset,
                            timestamp=message.timestamp,
                            key=_decode(
                                message.key,
                                decode_options.key_encoding,
                                decode_options.errors,
                            ),
                            value=_decode(
                                message.value,
                                decode_options.value_encoding,
                                decode_options.errors,
                            ),
                        )
                    )
                    if len(records) >= limit:
                        break
                if len(records) >= limit:
                    break

        return records
    finally:
        await consumer.stop()


async def stream_live_messages(
    connection: KafkaConnection,
    topic: str,
    max_messages: int,
    duration_seconds: int,
    poll_interval_ms: int,
    decode_options: MessageDecodeOptions,
):
    consumer = create_consumer(connection, group_id=None, auto_offset_reset="latest")
    await consumer.start()
    try:
        await consumer.subscribe([topic])
        count = 0
        end_time = asyncio.get_event_loop().time() + duration_seconds
        while count < max_messages and asyncio.get_event_loop().time() < end_time:
            batch = await consumer.getmany(timeout_ms=poll_interval_ms)
            for tp, messages in batch.items():
                for message in messages:
                    payload = MessageRecord(
                        partition=tp.partition,
                        offset=message.offset,
                        timestamp=message.timestamp,
                        key=_decode(
                            message.key,
                            decode_options.key_encoding,
                            decode_options.errors,
                        ),
                        value=_decode(
                            message.value,
                            decode_options.value_encoding,
                            decode_options.errors,
                        ),
                    )
                    yield payload
                    count += 1
                    if count >= max_messages:
                        break
                if count >= max_messages:
                    break
    finally:
        await consumer.stop()


async def collect_live_messages(
    connection: KafkaConnection,
    topic: str,
    max_messages: int,
    duration_seconds: int,
    poll_interval_ms: int,
    decode_options: MessageDecodeOptions,
) -> List[MessageRecord]:
    messages: List[MessageRecord] = []
    async for message in stream_live_messages(
        connection,
        topic,
        max_messages,
        duration_seconds,
        poll_interval_ms,
        decode_options,
    ):
        messages.append(message)
    return messages
