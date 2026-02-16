from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


@dataclass
class KafkaClusterConfig:
    name: str
    bootstrap_servers: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    oauth_token: Optional[str] = None


@dataclass
class AppSettings:
    host: str = "0.0.0.0"
    port: int = 8000
    default_cluster: Optional[KafkaClusterConfig] = None


def load_settings() -> AppSettings:
    load_dotenv()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    default_cluster = None
    if bootstrap:
        default_cluster = KafkaClusterConfig(
            name="default",
            bootstrap_servers=bootstrap,
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip()
            or "PLAINTEXT",
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM") or None,
            sasl_username=os.getenv("KAFKA_SASL_USERNAME") or None,
            sasl_password=os.getenv("KAFKA_SASL_PASSWORD") or None,
            ssl_cafile=os.getenv("KAFKA_SSL_CAFILE") or None,
            ssl_certfile=os.getenv("KAFKA_SSL_CERTFILE") or None,
            ssl_keyfile=os.getenv("KAFKA_SSL_KEYFILE") or None,
            oauth_token=os.getenv("KAFKA_OAUTH_TOKEN") or None,
        )

    return AppSettings(
        host=os.getenv("APP_HOST", "0.0.0.0"),
        port=int(os.getenv("APP_PORT", "8000")),
        default_cluster=default_cluster,
    )
