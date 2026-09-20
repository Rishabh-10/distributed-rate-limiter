.PHONY: help build test race integration cover lint fmt vet run up down redis-up redis-down clean

REDIS_CONTAINER ?= ratelimiter-redis
# Its own port by default, so a Redis you already run for something else is
# left alone: make integration REDIS_PORT=6379 to reuse a standard one.
REDIS_PORT ?= 6380
REDIS_ADDR ?= localhost:$(REDIS_PORT)
COMPOSE ?= docker compose -f deploy/docker-compose.yml

help: ## List the available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## Build both binaries into bin/
	go build -o bin/ ./cmd/...

test: ## Run unit tests (no Docker needed; Redis is faked by miniredis)
	go test ./...

race: ## Run unit tests under the race detector (needs a C toolchain; CI always runs this)
	go test -race ./...

integration: redis-up ## Run integration tests against a real Redis in Docker
	REDIS_ADDR=$(REDIS_ADDR) go test -tags integration -count=1 -timeout 5m ./test/integration/...

cover: ## Report unit test coverage
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

fmt: ## Format the source
	gofmt -w .

vet: ## Run go vet
	go vet ./...

lint: fmt vet ## Format and vet

run: ## Run the limiter locally against $(REDIS_ADDR)
	go run ./cmd/ratelimiterd -config configs/config.yaml

redis-up: ## Start a local Redis container if one is not already running
	@docker inspect -f '{{.State.Running}}' $(REDIS_CONTAINER) 2>/dev/null | grep -q true || \
		docker run -d --rm --name $(REDIS_CONTAINER) -p $(REDIS_PORT):6379 redis:7-alpine \
			redis-server --save '' --appendonly no >/dev/null
	@echo "redis listening on $(REDIS_ADDR)"

redis-down: ## Stop the local Redis container
	-@docker rm -f $(REDIS_CONTAINER) >/dev/null 2>&1 || true

up: ## Start Redis, the limiter and the demo app with docker compose
	$(COMPOSE) up --build

down: ## Tear the compose stack down
	$(COMPOSE) down -v

clean: redis-down ## Remove build artefacts and the local Redis container
	-@rm -rf bin coverage.out
