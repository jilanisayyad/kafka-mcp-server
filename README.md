Kafka MCP Server

Overview

FastMCP-based MCP server that connects to Kafka and exposes MCP tools for common
broker, topic, consumer, and retention operations.

Features

- List/create/delete topics and inspect topic configs.
- Tail recent messages or collect a short live stream.
- List consumer groups and compute group lag.
- Inspect cluster broker metadata.
- Store and manage named Kafka users locally.

Requirements

- Python 3.10+ (Makefile defaults to 3.13).
- uv (recommended) or any Python environment manager.
- A Kafka broker (local via Podman or your own cluster).

Quick start (uv)

1. Create and activate a virtual environment:
   - uv python install 3.13
   - uv venv .venv --python 3.13
   - source .venv/bin/activate
2. Install dependencies:
   - uv sync
3. Run the server:
   - uv run python -m app.main

Default MCP SSE endpoint: http://localhost:8000/mcp

Makefile shortcuts

- Start server: make start
- Start Kafka (Podman): make kafka-start
- Stop Kafka: make kafka-stop
- Kafka logs: make kafka-logs
- Run smoke test: make test-smoke

Configuration

Server settings come from FastMCP ServerSettings. The defaults are:

- Host: 0.0.0.0
- Port: 8000

FastMCP supports environment overrides such as:

- FASTMCP_SERVER_HOST
- FASTMCP_SERVER_PORT

Smoke tests and clients can use:

- MCP_URL (default http://localhost:8000/mcp)
- KAFKA_BOOTSTRAP_SERVERS (default localhost:9092)
- KAFKA_TEST_TOPIC (default mcp_smoke)
- KAFKA_TEST_GROUP (default mcp_smoke_group)

Kafka connection payload

Most tools require a Kafka connection object with the following fields:

- bootstrap_servers (required)
- security_protocol (default PLAINTEXT)
- sasl_mechanism, sasl_username, sasl_password (optional)
- ssl_cafile, ssl_certfile, ssl_keyfile (optional)
- oauth_token (required when sasl_mechanism is OAUTHBEARER)

MCP tool catalog

Each tool name below is the MCP command. Parameter shapes match the schemas in
app/schemas.py.

- `health` - Returns `{"status":"ok"}`.
  - Params: none
- `list_topics` - Lists topics with partition and replication info.
  - Params: `connection`
- `create_topic` - Creates a topic with optional configs.
  - Params: `connection`, `payload` (`name`, `num_partitions`, `replication_factor`, `configs`)
- `delete_topic` - Deletes a topic by name.
  - Params: `connection`, `name`
- `topic_configs` - Fetches topic configuration values.
  - Params: `connection`, `name`
- `topic_retention` - Returns `retention.ms` for a topic.
  - Params: `connection`, `name`
- `tail_messages` - Reads the most recent messages for a topic.
  - Params: `connection`, `name`, `payload` (`limit`)
- `live_messages` - Collects a short live stream of messages.
  - Params: `connection`, `name`, `payload` (`max_messages`, `duration_seconds`, `poll_interval_ms`)
- `list_consumer_groups` - Lists consumer groups with state and member count.
  - Params: `connection`
- `consumer_group_lag` - Computes lag per partition for a group.
  - Params: `connection`, `group_id`
- `cluster_info` - Returns broker and controller metadata.
  - Params: `connection`
- `list_kafka_users` - Lists locally stored user entries.
  - Params: none
- `upsert_kafka_user` - Creates or updates a local user entry.
  - Params: `user` (`username`, `sasl_mechanism`, `note`)
- `delete_kafka_user` - Deletes a local user entry.
  - Params: `username`

MCP command examples (Python)

```python
from mcp import ClientSession
from mcp.client.sse import sse_client

MCP_URL = "http://localhost:8000/mcp"

connection = {
      "bootstrap_servers": "localhost:9092",
      "security_protocol": "PLAINTEXT",
      "sasl_mechanism": None,
      "sasl_username": None,
      "sasl_password": None,
      "ssl_cafile": None,
      "ssl_certfile": None,
      "ssl_keyfile": None,
      "oauth_token": None,
}

async with sse_client(MCP_URL) as (read, write):
      async with ClientSession(read, write) as session:
            await session.initialize()

            await session.call_tool("list_topics", {"connection": connection})

            await session.call_tool(
                  "create_topic",
                  {
                        "connection": connection,
                        "payload": {
                              "name": "example-topic",
                              "num_partitions": 1,
                              "replication_factor": 1,
                              "configs": {},
                        },
                  },
            )
```

Local storage

Kafka user entries are stored at app/data/users.json.

Testing

Smoke test all MCP tools end-to-end:

1. Start Kafka: make kafka-start
2. Start the server: make start
3. In a new terminal: make test-smoke

LocalAI (optional)

LocalAI is not required for Kafka MCP usage, but this repo includes helper
targets for running models locally:

1. Install LocalAI and a model: make localai-install && make localai-model
2. Start LocalAI: make localai-start
