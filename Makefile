PYTHON_VERSION ?= 3.13
KAFKA_IMAGE ?= apache/kafka:latest
KAFKA_CONTAINER ?= kafka-mcp
KAFKA_NETWORK ?= kafka-net
KAFKA_BROKER_PORT ?= 9092
MCP_URL ?= http://localhost:8000/mcp
SMOKE_TOPIC ?= mcp_smoke
SMOKE_GROUP ?= mcp_smoke_group
LOCALAI_PORT ?= 8080
MODEL_DIR ?= $(HOME)/models
LOCALAI_MODEL ?= localai@llama-3.2-sun-2.5b-chat
LOCALAI_DOWNLOAD_TIMEOUT ?= 300
LOCALAI_MODEL_NAME ?= tinyllama
LOCALAI_MODEL_FILE ?= tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf

.PHONY: start kafka-start kafka-stop kafka-logs test-smoke localai-install localai-backend localai-model localai-model-gguf localai-start

start:
	uv python install $(PYTHON_VERSION)
	uv venv .venv --python $(PYTHON_VERSION)
	. .venv/bin/activate && uv sync
	. .venv/bin/activate && uv run python -m app.main

kafka-start:
	@podman network inspect $(KAFKA_NETWORK) >/dev/null 2>&1 || podman network create $(KAFKA_NETWORK)
	@podman rm -f $(KAFKA_CONTAINER) >/dev/null 2>&1 || true
	podman run --name $(KAFKA_CONTAINER) \
		--network $(KAFKA_NETWORK) \
		-p $(KAFKA_BROKER_PORT):9092 \
		-p 9093:9093 \
		-e KAFKA_NODE_ID=1 \
		-e KAFKA_PROCESS_ROLES=broker,controller \
		-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@$(KAFKA_CONTAINER):9093 \
		-e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
		-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
		-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
		-e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
		-e KAFKA_LOG_DIRS=/var/lib/kafka/data \
		$(KAFKA_IMAGE)

kafka-stop:
	@podman rm -f $(KAFKA_CONTAINER) >/dev/null 2>&1 || true

kafka-logs:
	@podman logs -f $(KAFKA_CONTAINER)

test-smoke:
	. .venv/bin/activate && \
		MCP_URL=$(MCP_URL) \
		KAFKA_BOOTSTRAP_SERVERS=localhost:$(KAFKA_BROKER_PORT) \
		KAFKA_TEST_TOPIC=$(SMOKE_TOPIC) \
		KAFKA_TEST_GROUP=$(SMOKE_GROUP) \
		uv run python scripts/smoke_test.py

localai-install:
	brew install local-ai huggingface-cli
	local-ai backends install llama-cpp

localai-backend:
	local-ai backends install llama-cpp

localai-model:
	LOCALAI_DOWNLOAD_TIMEOUT=$(LOCALAI_DOWNLOAD_TIMEOUT) local-ai models install $(LOCALAI_MODEL)

localai-model-gguf:
	@if [ ! -f "$(MODEL_DIR)/$(LOCALAI_MODEL_FILE)" ]; then \
		echo "Missing GGUF: $(MODEL_DIR)/$(LOCALAI_MODEL_FILE)"; \
		exit 1; \
	fi
	@mkdir -p $(MODEL_DIR)
	@printf "name: %s\nbackend: llama-cpp\nparameters:\n  model: %s\n" \
		"$(LOCALAI_MODEL_NAME)" "$(LOCALAI_MODEL_FILE)" \
		> "$(MODEL_DIR)/$(LOCALAI_MODEL_NAME).yaml"

localai-start:
	local-ai run --models-path $(MODEL_DIR)
