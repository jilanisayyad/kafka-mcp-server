from __future__ import annotations

from typing import List

from aiokafka import TopicPartition

from ..kafka_client import create_admin_client, create_consumer
from ..schemas import (
    ConsumerGroupLag,
    ConsumerGroupSummary,
    KafkaConnection,
    PartitionLag,
)


async def list_consumer_groups(
    connection: KafkaConnection,
) -> List[ConsumerGroupSummary]:
    admin = create_admin_client(connection)
    await admin.start()
    try:
        groups = await admin.list_consumer_groups()
        group_ids = [group[0] for group in groups]
        descriptions = await admin.describe_consumer_groups(group_ids)
        summaries: List[ConsumerGroupSummary] = []
        for description in descriptions:
            summaries.append(
                ConsumerGroupSummary(
                    group_id=description.group_id,
                    state=description.state,
                    protocol=description.protocol_type,
                    members=len(description.members),
                )
            )
        return summaries
    finally:
        await admin.close()


async def get_consumer_lag(
    connection: KafkaConnection, group_id: str
) -> ConsumerGroupLag:
    admin = create_admin_client(connection)
    await admin.start()
    try:
        offsets = await admin.list_consumer_group_offsets(group_id)
        tps = list(offsets.keys())
        if not tps:
            return ConsumerGroupLag(group_id=group_id, lags=[])
    finally:
        await admin.close()

    consumer = create_consumer(connection, group_id=None)
    await consumer.start()
    try:
        end_offsets = await consumer.end_offsets(tps)
        lags: List[PartitionLag] = []
        for tp, offset_meta in offsets.items():
            end = end_offsets.get(tp)
            current = offset_meta.offset if offset_meta else None
            lag = None
            if end is not None and current is not None:
                lag = max(0, end - current)
            lags.append(
                PartitionLag(
                    topic=tp.topic,
                    partition=tp.partition,
                    current_offset=current,
                    end_offset=end,
                    lag=lag,
                )
            )
        return ConsumerGroupLag(group_id=group_id, lags=lags)
    finally:
        await consumer.stop()
