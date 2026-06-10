# Boltrope — developer & CI task runner.
#
# This Makefile is a thin, documented wrapper around the underlying Go / buf /
# golangci-lint toolchain. Every target simply runs the tool command it
# documents — there is no hidden logic — so you can always run the underlying
# command directly.
#
# ──────────────────────────────────────────────────────────────────────────
# Windows developers
# ──────────────────────────────────────────────────────────────────────────
# `make` is not always present on Windows. Every recipe below is a single
# tool invocation; run the command shown under each target directly in
# PowerShell. For example, instead of `make lint`:
#
#     golangci-lint run ./...
#
# instead of `make test`:
#
#     go test -short ./...
#
# and instead of `make gen`:
#
#     buf generate
#
# The tools (go, buf, golangci-lint, protoc-gen-*, migrate) must be on PATH.
# See the `tools` target for the pinned versions and `go install` lines.
#
# ──────────────────────────────────────────────────────────────────────────
# Pinned tool versions (keep in sync with the `tools` target and CI)
# ──────────────────────────────────────────────────────────────────────────
BUF_VERSION              ?= v1.70.0
PROTOC_GEN_GO_VERSION    ?= v1.36.11
PROTOC_GEN_GO_GRPC_VERSION ?= v1.6.2
GOLANGCI_LINT_VERSION    ?= v2.12.2
MIGRATE_VERSION          ?= v4.18.3

# Import path prefix used for goimports local grouping (matches .golangci.yml).
LOCAL_PREFIX             := github.com/boltrope/boltrope

# Branch buf compares against for breaking-change detection. Override on a
# release/maintenance branch, e.g. `make proto-breaking BREAKING_AGAINST=.git#branch=release-1.0`.
BREAKING_AGAINST         ?= .git#branch=main

# GOTESTFLAGS lets CI inject extra flags (e.g. -coverprofile) without editing
# recipes: `make test GOTESTFLAGS="-coverprofile=cover.out"`.
GOTESTFLAGS              ?=

.DEFAULT_GOAL := help

# ──────────────────────────────────────────────────────────────────────────
# Meta
# ──────────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## List available targets.
	@echo "Boltrope make targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ──────────────────────────────────────────────────────────────────────────
# Dependencies & formatting
# ──────────────────────────────────────────────────────────────────────────

.PHONY: tidy
tidy: ## Sync go.mod/go.sum with the source (go mod tidy).
	go mod tidy

.PHONY: fmt
fmt: ## Format Go code with gofumpt + goimports (via golangci-lint formatters).
	golangci-lint fmt ./...

# ──────────────────────────────────────────────────────────────────────────
# Protobuf (buf): generate, lint, breaking-change detection
# ──────────────────────────────────────────────────────────────────────────

.PHONY: gen
gen: ## Regenerate committed protobuf stubs in gen/ (buf generate).
	buf generate

.PHONY: proto-lint
proto-lint: ## Lint the .proto contracts (buf lint).
	buf lint

.PHONY: proto-breaking
proto-breaking: ## Detect breaking proto changes vs. main (buf breaking).
	buf breaking --against "$(BREAKING_AGAINST)"

# ──────────────────────────────────────────────────────────────────────────
# Lint & build
# ──────────────────────────────────────────────────────────────────────────

.PHONY: lint
lint: ## Run golangci-lint over the module (golangci-lint run ./...).
	golangci-lint run ./...

.PHONY: build
build: ## Compile all packages (go build ./...).
	go build ./...

# ──────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────

.PHONY: test
test: ## Run fast unit tests (no integration, no Docker/network).
	go test -short $(GOTESTFLAGS) ./...

.PHONY: test-race
test-race: ## Run all tests under the race detector.
	go test -race $(GOTESTFLAGS) ./...

.PHONY: test-integration
test-integration: ## Run integration tests (//go:build integration; needs Docker).
	go test -tags integration -race $(GOTESTFLAGS) ./...

.PHONY: cover
cover: ## Run unit tests with a coverage profile and print the summary.
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out

# ──────────────────────────────────────────────────────────────────────────
# Tooling install (pinned). Installs into $(go env GOPATH)/bin.
# Run once on a fresh checkout, or after bumping a pinned version above.
# ──────────────────────────────────────────────────────────────────────────

.PHONY: tools
tools: ## Install pinned dev tools (buf, protoc-gen-*, golangci-lint, migrate).
	go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@$(MIGRATE_VERSION)

# ──────────────────────────────────────────────────────────────────────────
# Local stack (deploy/docker-compose.yml)
# ──────────────────────────────────────────────────────────────────────────
#
# The compose file lives under deploy/, so `docker compose` auto-loads
# `deploy/.env` (its project directory is the compose file's parent). We bootstrap
# that file from the repo-root .env.example template. See deploy/README.md.

COMPOSE_FILE              ?= deploy/docker-compose.yml
COMPOSE                   := docker compose -f $(COMPOSE_FILE)

# deploy/.env is git-ignored; create it from the template on first use so the
# stack targets work out of the box. (compose-config below deliberately does NOT
# require it, since every value has an inline default.)
deploy/.env:
	cp .env.example deploy/.env

.PHONY: run-compose
run-compose: deploy/.env ## Build + bring up the local stack (Postgres → migrate → grant → services), waiting for readiness.
	$(COMPOSE) up --build --wait

.PHONY: compose-config
compose-config: ## Validate and render the compose file (no .env needed; defaults are inline).
	$(COMPOSE) config

.PHONY: compose-logs
compose-logs: ## Tail logs from the running stack.
	$(COMPOSE) logs -f

.PHONY: down
down: ## Stop the local stack and remove its containers + network (keeps volumes).
	$(COMPOSE) down

.PHONY: down-volumes
down-volumes: ## Stop the local stack and ALSO delete its volumes (pgdata + blobs — destructive).
	$(COMPOSE) down --volumes
