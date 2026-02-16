from __future__ import annotations

from typing import List

from ..kafka_client import create_consumer
from ..schemas import BrokerInfo, ClusterInfo, KafkaConnection


async def get_cluster_info(connection: KafkaConnection) -> ClusterInfo:
    consumer = create_consumer(connection)
    await consumer.start()
    try:
        cluster = consumer._client.cluster
        brokers: List[BrokerInfo] = []
        for broker in cluster.brokers():
            brokers.append(
                BrokerInfo(
                    node_id=broker.nodeId,
                    host=broker.host,
                    port=broker.port,
                    rack=broker.rack,
                )
            )
        return ClusterInfo(
            cluster_id=cluster.cluster_id,
            controller_id=cluster.controller_id,
            brokers=brokers,
        )
    finally:
        await consumer.stop()
