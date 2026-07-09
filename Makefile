# Conduit CDC — Makefile

# ---- Config ----
BINARY      := conduit-example
CMD_PKG     := ./cmd/example
BIN_DIR     := bin
GO          := go
GOFLAGS     ?=

# DB URL used by `make run` (overridable: `make run DB_URL=...`)
DB_URL      ?= postgres://conduit:conduit@localhost:1234/conduit_db?replication=database

.DEFAULT_GOAL := help

# ---- Meta ----
.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ---- Build & run ----
.PHONY: build
build: ## Compile the example binary into ./bin
	$(GO) build $(GOFLAGS) -o $(BIN_DIR)/$(BINARY) $(CMD_PKG)

.PHONY: run
run: ## Run the example CDC consumer (honours DB_URL)
	DB_URL="$(DB_URL)" $(GO) run $(GOFLAGS) $(CMD_PKG)

.PHONY: install
install: ## Install the example binary into GOBIN
	$(GO) install $(GOFLAGS) $(CMD_PKG)

# ---- Quality ----
.PHONY: fmt
fmt: ## Format all Go source
	$(GO) fmt ./...

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: test
test: ## Run tests
	$(GO) test $(GOFLAGS) ./...

.PHONY: test-race
test-race: ## Run tests with the race detector
	$(GO) test $(GOFLAGS) -race ./...

.PHONY: tidy
tidy: ## Sync go.mod / go.sum
	$(GO) mod tidy

.PHONY: check
check: fmt vet test ## Format, vet and test

# ---- Local Postgres (docker compose) ----
.PHONY: db-up
db-up: ## Start the local Postgres (logical replication enabled)
	docker compose up -d

.PHONY: db-down
db-down: ## Stop the local Postgres

	docker compose down

.PHONY: db-reset
db-reset: ## Wipe the Postgres volume and recreate
	docker compose down -v
	docker compose up -d

.PHONY: db-logs
db-logs: ## Tail Postgres logs
	docker compose logs -f postgres

# ---- Housekeeping ----
.PHONY: clean
clean: ## Remove build artifacts
	rm -rf $(BIN_DIR)
	$(GO) clean
