from __future__ import annotations

from typing import Any, Dict, Optional

from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient

from .schemas import KafkaConnection


class StaticOAuthTokenProvider:
    def __init__(self, token: str) -> None:
        self._token = token

    def token(self) -> str:
        return self._token


def _build_kafka_kwargs(connection: KafkaConnection) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {
        "bootstrap_servers": connection.bootstrap_servers,
        "security_protocol": connection.security_protocol,
    }

    if connection.sasl_mechanism:
        kwargs["sasl_mechanism"] = connection.sasl_mechanism
    if connection.sasl_username:
        kwargs["sasl_plain_username"] = connection.sasl_username
    if connection.sasl_password:
        kwargs["sasl_plain_password"] = connection.sasl_password

    if connection.ssl_cafile:
        kwargs["ssl_cafile"] = connection.ssl_cafile
    if connection.ssl_certfile:
        kwargs["ssl_certfile"] = connection.ssl_certfile
    if connection.ssl_keyfile:
        kwargs["ssl_keyfile"] = connection.ssl_keyfile

    if connection.sasl_mechanism == "OAUTHBEARER":
        if not connection.oauth_token:
            raise ValueError("oauth_token is required for OAUTHBEARER")
        kwargs["sasl_oauth_token_provider"] = StaticOAuthTokenProvider(
            connection.oauth_token
        )

    return kwargs


def create_admin_client(connection: KafkaConnection) -> AIOKafkaAdminClient:
    kwargs = _build_kafka_kwargs(connection)
    return AIOKafkaAdminClient(**kwargs)


def create_consumer(
    connection: KafkaConnection,
    *,
    group_id: Optional[str] = None,
    auto_offset_reset: str = "latest",
) -> AIOKafkaConsumer:
    kwargs = _build_kafka_kwargs(connection)
    return AIOKafkaConsumer(
        **kwargs,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset=auto_offset_reset,
    )
