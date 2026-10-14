.DEFAULT_GOAL := help
.PHONY: help up demo down clean ps logs test lint lint-local format replay-dlq

COMPOSE := docker compose
ALL_PROFILES := --profile demo --profile test

help:  ## Список команд
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

up:  ## Поднять postgres, rabbitmq, api, consumer
	$(COMPOSE) up -d --build --wait

demo:  ## То же + демо-приёмник webhook'ов (localhost:8090)
	$(COMPOSE) --profile demo up -d --build --wait

down:  ## Остановить всё (включая demo)
	$(COMPOSE) $(ALL_PROFILES) down

clean:  ## Остановить всё и удалить данные (volumes)
	$(COMPOSE) $(ALL_PROFILES) down -v

ps:  ## Состояние сервисов
	$(COMPOSE) $(ALL_PROFILES) ps

logs:  ## Логи api и consumer
	$(COMPOSE) logs -f api consumer

test:  ## Тесты против настоящих PostgreSQL и RabbitMQ (в Docker)
	$(COMPOSE) --profile test run --rm --build tests

lint:  ## ruff + mypy --strict (в Docker, локальный uv не нужен)
	$(COMPOSE) --profile test run --rm --build --no-deps tests sh -c "ruff check . && ruff format --check . && mypy app tools tests"

lint-local:  ## То же локально через uv
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy app tools tests

format:  ## Автоформатирование (локально через uv)
	uv run ruff check --fix .
	uv run ruff format .

replay-dlq:  ## Вернуть сообщения из DLQ в обработку (LIMIT=100 по умолчанию)
	$(COMPOSE) exec -T consumer python -m app.messaging.dlq_replay --limit $(or $(LIMIT),100)
