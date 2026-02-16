from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class KafkaConnection(BaseModel):
    bootstrap_servers: str = Field(..., min_length=1)
    security_protocol: str = Field("PLAINTEXT")
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    oauth_token: Optional[str] = None


class TopicCreate(BaseModel):
    name: str
    num_partitions: int = 1
    replication_factor: int = 1
    configs: Dict[str, str] = Field(default_factory=dict)


class TopicSummary(BaseModel):
    name: str
    partitions: int
    replication_factor: int


class TopicConfigResponse(BaseModel):
    name: str
    configs: Dict[str, str]


class MessageRecord(BaseModel):
    partition: int
    offset: int
    timestamp: Optional[int] = None
    key: Optional[str] = None
    value: Optional[str] = None


class ConsumerGroupSummary(BaseModel):
    group_id: str
    state: Optional[str] = None
    protocol: Optional[str] = None
    members: int = 0


class PartitionLag(BaseModel):
    topic: str
    partition: int
    current_offset: Optional[int] = None
    end_offset: Optional[int] = None
    lag: Optional[int] = None


class ConsumerGroupLag(BaseModel):
    group_id: str
    lags: List[PartitionLag]


class BrokerInfo(BaseModel):
    node_id: int
    host: str
    port: int
    rack: Optional[str] = None


class ClusterInfo(BaseModel):
    cluster_id: Optional[str] = None
    controller_id: Optional[int] = None
    brokers: List[BrokerInfo]


class KafkaUser(BaseModel):
    username: str
    sasl_mechanism: Optional[str] = None
    note: Optional[str] = None


@dataclass
class MessageDecodeOptions:
    key_encoding: str = "utf-8"
    value_encoding: str = "utf-8"
    errors: str = "replace"


class LiveMessageRequest(BaseModel):
    max_messages: int = 50
    duration_seconds: int = 10
    poll_interval_ms: int = 500


class TailRequest(BaseModel):
    limit: int = 50
