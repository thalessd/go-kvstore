SHELL := /bin/bash
.DEFAULT_GOAL := help

GO ?= go

# Matches compose.yaml. Override to run the integration tier against another
# server: make test-integration KVSTORE_TEST_DSN=...
KVSTORE_TEST_DSN ?= postgres://kvstore:kvstore@localhost:55433/kvstore?sslmode=disable

.PHONY: help fmt check test test-race test-integration tidy db-up db-down

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

fmt: ## Format Go
	gofmt -s -w .

check: ## Vet and check formatting without rewriting files
	$(GO) vet ./...
	$(GO) vet -tags=integration ./...
	test -z "$$(gofmt -s -l .)"

test: ## Unit tests with cover. No container runtime needed
	$(GO) test ./... -cover

test-race: ## Unit tests with the race detector
	$(GO) test -race ./...

test-integration: ## Contract tests against a real PostgreSQL (needs db-up)
	KVSTORE_TEST_DSN="$(KVSTORE_TEST_DSN)" \
		$(GO) test -tags=integration -count=1 -timeout=10m ./...

tidy: ## Tidy the module and fail if it was not already tidy
	$(GO) mod tidy
	git diff --exit-code go.mod go.sum

db-up: ## Start PostgreSQL for the integration tier and wait for it
	docker compose up -d --wait postgres

db-down: ## Stop PostgreSQL, keeping the volume
	docker compose down
