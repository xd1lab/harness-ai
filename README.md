<!-- SPDX-License-Identifier: Apache-2.0 -->

# Boltrope

**A provider-portable, event-sourced AI agent harness in Go.**

[![CI](https://github.com/boltrope/boltrope/actions/workflows/ci.yml/badge.svg)](https://github.com/boltrope/boltrope/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/boltrope/boltrope.svg)](https://pkg.go.dev/github.com/boltrope/boltrope)
[![Go Report Card](https://goreportcard.com/badge/github.com/boltrope/boltrope)](https://goreportcard.com/report/github.com/boltrope/boltrope)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/boltrope/boltrope/badge)](https://securityscorecards.dev/viewer/?uri=github.com/boltrope/boltrope)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Boltrope turns a stateless LLM completion API into a **stateful, tool-using, self-correcting agent** that you run on your own infrastructure, against any supported hosted or self-hosted model. It is backend-only (no frontend, no proprietary cloud dependency) and built on one idea: the single source of truth is an **append-only, event-sourced log in PostgreSQL**. Session resume, fork, replay, cost accounting, and observability all derive from that log.

**Why another harness?** Most agent frameworks couple the loop to one vendor's SDK, keep session state in process memory, and run tools with unrestricted host access. Boltrope draws three hard boundaries instead: a normalized `Provider` interface so the loop never imports a vendor SDK; a durable event log so a crashed agent resumes deterministically (and is never silently re-billed); and a single sandboxed tool-runtime with deny-by-default egress as the only place model-influenced code runs. Context is treated as a finite resource — actively managed via token accounting, automatic compaction, and prompt caching.

> **Status:** v1, backend-only. The agent loop, the four-family model gateway, the event store, the sandboxed tool-runtime, permissions, MCP client, and OpenTelemetry observability are implemented. See [Feature overview](#feature-overview) for what ships today and [Roadmap](#roadmap--deferred) for what is deliberately deferred.

---

## Quickstart

Bring up the full stack (PostgreSQL, schema migration, the three services, and `projectord`) and run one task — **keyless, no model API key required**. Bringing up the stack needs only **Docker** with the Compose plugin; the `harnessctl` client in step 4 additionally needs **Go** to build/run (`go run ./cmd/harnessctl …`, or `go build -o bin/ ./cmd/harnessctl` once). The model-gateway defaults to the built-in `stub` provider (a deterministic, network-free provider), so a clean `docker compose up` runs an end-to-end task out of the box. Point it at a real model in [Use a real model](#use-a-real-model) below.

```bash
# 1. Clone.
git clone https://github.com/boltrope/boltrope.git
cd boltrope

# 2. Up — keyless. Postgres healthy -> migrate completes -> grant -> the four
#    services start. The model-gateway defaults to BOLTROPE_MODELGW_PROVIDER=stub,
#    so no .env and no API key are needed. --wait blocks until every service's
#    /readyz reports ready (each gates on its real downstream deps; see Notes).
docker compose -f deploy/docker-compose.yml up --build --wait

# 3. Confirm the whole stack is ready (each returns HTTP 200). The orchestrator's
#    HTTP edge is published on host port 8080, the gRPC edge on 9000.
curl -fsS http://localhost:8080/readyz && echo   # orchestrator: ready

# 4. Run a hello-world task. harnessctl is the gRPC client CLI; BOLTROPE_DEV_INSECURE=1
#    selects its shared-seed dev mTLS dial path so it handshakes the compose edge
#    (see the note below). No --tenant is needed: under dev-insecure the orchestrator
#    scopes the call to its fixed dev tenant (the authenticated principal), and the
#    CLI sends no seed so it derives the same dev CA as the stack. It runs the real
#    agent loop against the stub provider, which replies with one deterministic text
#    turn, so the run streams the assistant TextDelta and then a terminal
#    [result] subtype=success frame — keyless, with no approval step. This proves the
#    full distributed pipeline end-to-end: client → orchestrator (mTLS + event-sourced
#    log) → model-gateway, plus the orchestrator → tool-runtime tool advertisement and
#    the resumable streaming relay. (Tool EXECUTION in the sandbox is covered by the
#    tool-runtime integration suite, not this network-free demo provider. Point at a
#    real model below to drive actual tool calls: under the default permission mode a
#    model's tool call pauses at an [approval required] prompt printing a call_id —
#    approve it from a second terminal with `harnessctl ... --session <id> approve
#    <call-id>`. The standing mode is chosen when the CLI CREATES the session:
#    --permission-mode default|acceptEdits|plan, env BOLTROPE_CTL_PERMISSION_MODE.)
#    Reconnect to a dropped session with --session <id> --after-seq <n>;
#    fork a trajectory with `harnessctl ... fork --at-seq <n>`.
BOLTROPE_DEV_INSECURE=1 go run ./cmd/harnessctl --endpoint localhost:9000 \
    run "Write a hello-world Go program."
```

> **`harnessctl` over the dev edge.** Under `BOLTROPE_DEV_INSECURE=1` the orchestrator's gRPC edge speaks **static-cert mTLS** (it has no plaintext listener). Setting the same `BOLTROPE_DEV_INSECURE=1` on `harnessctl` (as in step 4) makes it dial over the **shared-seed dev CA**: the CLI presents the `spiffe://boltrope.local/edge` identity the orchestrator's RBAC admits and pins `spiffe://boltrope.local/orchestrator`, completing mutual TLS against the compose edge. (Override the trust domain / pinned id with `--trust-domain` / `--server-id`, or set `BOLTROPE_DEV_CA_SEED` consistently across the stack.) The bare `--insecure` flag is plaintext-only and is for a local orchestrator started **without** mTLS — it cannot handshake the compose dev edge. Production uses SPIFFE/SPIRE SVIDs and OIDC at the edge.

> **Notes.** The Compose stack lives at [`deploy/docker-compose.yml`](deploy/docker-compose.yml); it orders Postgres → migrate → grant → services with `depends_on` + healthchecks per [docs/architecture/00-architecture.md §10](docs/architecture/00-architecture.md). `--wait` returns only once every service's `/readyz` is green, and each service's readiness gates on its real downstream dependencies — the orchestrator on a Postgres ping **and a `grpc.health.v1` probe of the model-gateway and tool-runtime over the same inter-service mTLS channel it serves on**, the tool-runtime additionally on `docker version` — so a green `--wait` means the stack is wired up and the inter-service mTLS actually handshakes (a shared-CA mismatch fails `--wait` here, not on the first turn), not merely that the processes started. It is the **dev / single-tenant / trusted-code** stack: it runs with `BOLTROPE_DEV_INSECURE=1` (the static-cert mTLS fallback, with one shared dev-CA seed across all services) and mounts the host Docker socket into the tool-runtime (docker-out-of-docker, root-equivalent on the host — read the comments in that file). Production deployments use SPIFFE/SPIRE-issued SVIDs for inter-service mTLS, OIDC/bearer auth at the client edge, and a socket-proxied or microVM sandbox backend (see [Security](#security)).

### Use a real model

The `stub` provider proves the wiring; swap in a real model when you want real output. Provider selection is a **deployment concern of the model-gateway**, read from its environment. The gateway stores only the **name** of the env var holding the key — the value is resolved at a trusted boundary and never lands in config, the event log, or any response body. Set these in `deploy/.env` (git-ignored; compose reads it automatically) and re-run the `up --wait` above:

```bash
# deploy/.env — Anthropic Claude (swap for gemini / openai / openaicompat):
cat > deploy/.env <<'EOF'
BOLTROPE_MODELGW_PROVIDER=anthropic
BOLTROPE_MODELGW_API_KEY_ENV=ANTHROPIC_API_KEY
ANTHROPIC_API_KEY=sk-ant-...
EOF
```

| Provider | `BOLTROPE_MODELGW_PROVIDER` | Key env (name → in `BOLTROPE_MODELGW_API_KEY_ENV`) |
|---|---|---|
| Anthropic Claude | `anthropic` | `ANTHROPIC_API_KEY` |
| Google Gemini | `gemini` | `GEMINI_API_KEY` |
| OpenAI (Responses API) | `openai` | `OPENAI_API_KEY` |
| Self-hosted / OpenAI-compatible (Ollama, vLLM, LM Studio, …) | `openaicompat` | optional — set only if the endpoint requires one; point `BOLTROPE_MODELGW_OPENAI_BASE_URL` at the `/v1` URL |

See [Configuring a provider](#configuring-a-provider) for the full matrix and per-`(endpoint, model)` capability resolution.

---

## Feature overview

Everything below is implemented in v1 unless explicitly marked _roadmap_.

- **Agent loop** — a single-threaded gather → act → verify (ReAct-style) loop with turns, `max_turns` / `max_budget_usd` caps, and typed termination subtypes (`success`, `error_max_turns`, `error_max_budget_usd`, `error_during_execution`, `error_max_structured_output_retries`). Cooperative cancellation, doom-loop (stuck-loop) detection, and depth-limited sub-agents-as-tools.
- **Multi-LLM, provider-portable** — one normalized `Provider` interface (Generate / Stream / CountTokens / Capabilities) behind the model-gateway, with adapters for **Anthropic Claude**, **Google Gemini**, **OpenAI** (Responses API primary, Chat Completions sub-flag), and an **OpenAI-compatible** adapter covering **self-hosted** endpoints (vLLM, Ollama, LM Studio, llama.cpp, TGI, LiteLLM). Capability flags resolve per `(endpoint, model)`, not per provider family. The loop holds **zero** vendor-SDK imports — adding a provider touches only an adapter package plus a capabilities-table entry.
- **Event-sourced sessions with resume & fork** — an append-only PostgreSQL log is the single source of truth. Appends are **optimistic** (compare `expected_seq`), **fenced** (lease epoch), and **idempotent** (a re-sent `request_id` is a no-op, not a conflict). Resume folds the log and adjudicates open turns/tool-executions explicitly — a crashed run is never silently re-billed. Fork branches a session at any sequence without touching the parent.
- **Sandboxed tools** — core native tools (`read`, `edit`, `write`, `glob`, `grep`, `bash`, `webfetch`, `websearch`) run inside per-session containers behind a `Workspace`/`Runtime` port. Tool inputs are JSON-Schema-validated before execution; errors surface as an `Observation`, never a panic. On cancellation the process group is killed at the cgroup/PID-namespace boundary. A durable dedup ledger makes mutating tools at-most-once across restarts.
- **Permissions & human-in-the-loop** — a layered `deny → mode → allow → tool` policy pipeline with `default` / `acceptEdits` / `plan` / `bypass` modes, a taint-tracking egress gate for the lethal-trifecta risk, and approval decisions persisted as events (re-checkable on replay). A session's standing mode is set at creation: `harnessctl --permission-mode default|acceptEdits|plan` (env `BOLTROPE_CTL_PERMISSION_MODE`) applies when the CLI creates the session; `bypass` is operator-only and a client-supplied bypass is rejected server-side (ADR-0019).
- **MCP (client)** — connect Model Context Protocol servers over **stdio or HTTP** with lazy schema loading; each server runs in its own confined sandbox; first-use registration requires explicit human approval and MCP tool descriptions are treated as untrusted input.
- **Hooks / middleware** — `PreToolUse`, `PostToolUse`, `Stop`, and `PreCompact` hooks run as host subprocesses behind a `CommandRunner` port; a `PreToolUse` block prevents dispatch.
- **Context management** — running token accounting, automatic compaction before the budget threshold, append-only tool-result clearing (stubs in the window, full content retained in the log/blob store), and tenant-scoped prompt-cache prefixes.
- **Observability** — OpenTelemetry GenAI spans (`invoke_agent` / `chat` / `execute_tool`) with `gen_ai.*` attributes and trace-context propagation over gRPC; RED metrics per RPC (errors broken down by termination subtype) and USE/saturation gauges (worker-pool, live sandboxes, PG pool, blob bytes, projection lag); `slog` JSON logs with `LogValuer` secret redaction; gRPC health + HTTP `/livez` / `/readyz` with dependency-gated readiness.
- **Client API** — a resumable `Run` server-stream (Last-Event-ID semantics) plus a unary `Control` RPC (approve / deny / interrupt / reattach). The client API is **gRPC-only in v1**; a REST/JSON facade (grpc-gateway, `Run` over SSE + `Control` POST, enforcing identical auth) is _roadmap_, planned as `google.api.http` annotations over the same protos.
- **Deterministic eval harness** — golden scenarios drive the real loop against a scripted fake provider and fake clock, with **no network**; wired into CI as a required gate.

---

## Configuring a provider

Boltrope is provider-portable: the agent loop is identical regardless of which model serves a turn. Provider selection is a **deployment concern** of the model-gateway, read from the environment. The gateway stores only the **name** of the env var that holds the API key — the secret value is resolved at a trusted boundary and never lands in config, the event log, or any response body (per [ADR-0013](docs/decisions/0013-security-model.md)).

Set these on the `boltrope-modelgwd` service (in `.env` / Compose, or the process environment):

| Variable | Meaning |
|---|---|
| `BOLTROPE_MODELGW_PROVIDER` | `anthropic` \| `gemini` \| `openai` \| `openaicompat` \| `stub` (gateway binary default: `openaicompat`; the Compose stack defaults to `stub` for the keyless path) |
| `BOLTROPE_MODELGW_API_KEY_ENV` | the **name** of the env var holding the upstream API key (e.g. `ANTHROPIC_API_KEY`); unused by `stub` / keyless `openaicompat` |
| `BOLTROPE_MODELGW_OPENAI_BASE_URL` | base URL for `openai` / `openaicompat` (default `http://localhost:11434/v1`, Ollama) |

The `stub` provider is a built-in deterministic, network-free provider for local demo and CI smoke tests (it streams a scripted response and needs no key); it is the Compose default so the stack runs keyless. It is never for production.

**Anthropic Claude**

```bash
export ANTHROPIC_API_KEY=sk-ant-...
export BOLTROPE_MODELGW_PROVIDER=anthropic
export BOLTROPE_MODELGW_API_KEY_ENV=ANTHROPIC_API_KEY
```

**Google Gemini** (uses `google.golang.org/genai`)

```bash
export GEMINI_API_KEY=...
export BOLTROPE_MODELGW_PROVIDER=gemini
export BOLTROPE_MODELGW_API_KEY_ENV=GEMINI_API_KEY
```

**OpenAI** (Responses API by default)

```bash
export OPENAI_API_KEY=sk-...
export BOLTROPE_MODELGW_PROVIDER=openai
export BOLTROPE_MODELGW_API_KEY_ENV=OPENAI_API_KEY
```

**Self-hosted / OpenAI-compatible** (Ollama, vLLM, LM Studio, llama.cpp, TGI, LiteLLM) — point at the `/v1` base URL; a key is optional for keyless local endpoints:

```bash
export BOLTROPE_MODELGW_PROVIDER=openaicompat
export BOLTROPE_MODELGW_OPENAI_BASE_URL=http://localhost:11434/v1   # Ollama
# export BOLTROPE_MODELGW_API_KEY_ENV=MY_GATEWAY_KEY                # if the endpoint requires one
```

Capability flags (streaming tool calls, parallel tool calls, vision, thinking, server-side token counting, max output tokens) are resolved per `(endpoint, model)` and can be overridden per endpoint. When an endpoint lacks streaming tool-call support (e.g. LM Studio), the gateway buffers and emits complete tool calls. See the [Multi-LLM support matrix](docs/spec/00-system-specification.md#6-multi-llm-support-matrix) for per-family defaults, and [ADR-0004](docs/decisions/0004-multi-llm-provider-strategy.md) / [ADR-0016](docs/decisions/0016-provider-abstraction.md) for the rationale.

Shared service configuration follows `flags > env > file > defaults` via `knadh/koanf` and fails fast on a missing or invalid required field. Environment variables are `BOLTROPE_`-prefixed, with `__` as the nesting separator — e.g. `BOLTROPE_POSTGRES__DSN`, `BOLTROPE_SERVER__GRPC_ADDR`, `BOLTROPE_OTLP__ENDPOINT`, `BOLTROPE_LOG_LEVEL`, `BOLTROPE_DEV_INSECURE`.

---

## Architecture

Boltrope is **three long-lived services plus one projection worker**, all over a single PostgreSQL instance (the durable spine). The event store is an **in-process package inside the orchestrator**, not a separate service — PostgreSQL already provides the data-gravity, backup, and ordering guarantees a Go shim would only add a network hop to.

| Service (`cmd/`) | Responsibility |
|---|---|
| **orchestrator** (`boltrope-orchestratord`) | The brain: the agent loop, turns, permissions, hooks, context/token budget, sub-agents — and the embedded event store (append/load/fork/subscribe over `pgx`). |
| **model-gateway** (`boltrope-modelgwd`) | Stateless provider abstraction: normalizes the internal message/tool model to/from each LLM SDK, streams deltas, counts tokens, resolves capabilities, centralizes provider retry + error normalization. |
| **tool-runtime** (`boltrope-toolruntimed`) | The trust boundary for model-influenced code: tool registry (native + MCP), JSON-Schema validation, per-session sandboxes, MCP client, and the deny-by-default egress broker. |
| **projectord** (`boltrope-projectord`) | Read-side worker (off the request path): tails the event log and runs cost-rollup and OTel-export projections with an xmin-bounded safe-advance cursor. Lag never blocks a turn. |

Plus `boltrope-migrate` (runs DDL and exits — a release gate) and `harnessctl` (the client CLI/SDK). Services talk gRPC + protobuf with mTLS; the client edge is gRPC (a REST facade is planned — see [Roadmap](#roadmap--deferred)).

```
Client ──gRPC (resumable Run / Control)──> Orchestrator ──┬─ gRPC ─> Model Gateway ──> LLM APIs / self-hosted
                                            (agent loop +  │                            (Anthropic/Gemini/OpenAI)
                                             event store)  ├─ gRPC ─> Tool Runtime ──> Sandbox (per session)
                                                  │        │                            + egress broker
                                                  ▼        └──────────────────────────> External MCP servers
                                             PostgreSQL  <── projectord (cost-rollup, OTel export)
                                             (event log = single source of truth)
```

Read the details:

- [docs/architecture/00-architecture.md](docs/architecture/00-architecture.md) — full v1 architecture: service decomposition, the PostgreSQL event-store schema, durability & exactly-once side effects, the security model, the concurrency/cancellation model, and provider streaming across four families.
- [docs/decisions/](docs/decisions/) — the Architecture Decision Records (ADR index) recording every significant choice and its rejected alternatives.
- [ARCHITECTURE.md](ARCHITECTURE.md) — a short orientation map into the above.

---

## Security

- **Service-to-service mTLS** via SPIFFE/SPIRE workload identity, with deny-by-default per-RPC verb gates. A dev-only static-cert fallback is env-gated behind `BOLTROPE_DEV_INSECURE=1` and logs a loud warning — it is present in the binary but inert unless explicitly enabled; release images build with `-tags spire` to enable the SPIRE path.
- **Client-edge auth** validates OIDC/bearer tokens (`iss`/`aud`/`exp`, `alg=none` rejected, JWKS rotation) and verifies session ownership on every call.
- **Tenant isolation** at the database layer via PostgreSQL Row-Level Security (non-owner role, `SET LOCAL` GUC from the verified token, `FORCE ROW LEVEL SECURITY`).
- **Deny-by-default egress** — every per-session sandbox runs with `--network none` by default, so all model-influenced tools (sandbox `bash`, `webfetch`, `websearch`, MCP HTTP) have **no external network** — the network namespace is the v1 containment, and there is no unrestricted egress path. A per-session egress **broker** is the deny-by-default allowlist *policy* layer (configure allowed hosts via `BOLTROPE_TOOLRT_EGRESS_ALLOWLIST`; empty ⇒ deny-all); combined with a forward egress-proxy data path it will gate allowlisted egress per connection — the proxy is a [roadmap](#roadmap--deferred) item, so in v1 `webfetch`/`websearch` are effectively disabled unless an egress path is configured. Provider-native/server-side tools are disabled in v1.
- **Secrets** live only in model-gateway configuration (env), never in the log or any response; secret-bearing types redact via `slog.LogValuer`.

Found a vulnerability? Please report it privately — see [SECURITY.md](SECURITY.md). The [ADR-0013 security model](docs/decisions/0013-security-model.md) has the full picture.

---

## Roadmap / Deferred

v1 is a deliberately focused, irreducible harness. The `Provider`, `Workspace`/`Runtime`, `EventLogPort`, and MCP abstractions are shaped so these slot in **without re-architecture** ([ADR-0003](docs/decisions/0003-v1-scope.md)):

- **MCP server mode** (and A2A interoperability) — v1 ships the MCP *client* only.
- **microVM / gVisor / OS-native sandbox backends** — v1 is containers-only behind the `Workspace`/`Runtime` port; multi-tenant execution of mutually-untrusted code is therefore out of scope for v1.
- **Egress-proxy data path** — v1 contains egress with the sandbox network namespace (`--network none`) and ships the egress broker as the deny-by-default allowlist *policy* layer. The forward proxy that turns an allowlisted host into a live, per-connection-gated network path (re-enabling `webfetch`/`websearch`/MCP-HTTP for allowlisted hosts) is deferred; the `EgressBroker` port and the `--network` seam are shaped to slot it in without re-architecture.
- **Model routing** and advanced multi-agent topologies.
- **REST/JSON facade** — the v1 client API is gRPC-only. The facade is planned as grpc-gateway `google.api.http` annotations over the same protos — `Run` (over SSE) + `Control` POST first, a full REST mapping for every RPC later.
- **Native-Ollama NDJSON adapter** — the OpenAI-compatible `/v1` path is used instead.
- **Semantic codebase indexing** / tree-sitter repo map; **LLM-based risk classifier**; non-native function-calling fallback / constrained decoding.
- **Durable workspace snapshots** (consistent filesystem resume after crash); virtual-filesystem context mounts; interactive workspace access.
- **SWE-bench / SWE-bench-Lite** as a CI gate — the deterministic bespoke eval suite is the v1 gate; SWE-bench is a post-v1 external target.

---

## Documentation

- [docs/README.md](docs/README.md) — documentation index.
- [docs/spec/00-system-specification.md](docs/spec/00-system-specification.md) — the v1 system specification (functional + non-functional requirements, support matrix, definition of done).
- [docs/architecture/00-architecture.md](docs/architecture/00-architecture.md) — the v1 architecture.
- [docs/architecture/02-implementation-plan.md](docs/architecture/02-implementation-plan.md) — the test-first implementation plan.
- [docs/decisions/](docs/decisions/) — Architecture Decision Records.
- [CONTRIBUTING.md](CONTRIBUTING.md) · [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) · [SECURITY.md](SECURITY.md)

---

## Development

```bash
make tools          # install pinned dev tools (buf, protoc-gen-*, golangci-lint, migrate)
make lint           # golangci-lint run ./...
make test           # fast unit tests (no Docker/network)
make test-integration  # //go:build integration tests (needs Docker)
make gen            # regenerate protobuf stubs in gen/ (buf generate)
```

On Windows, run the underlying tool commands directly (each Make recipe is a single invocation — see the [Makefile](Makefile) header).

---

## License

Licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE). Contributions require a Developer Certificate of Origin sign-off (`git commit -s`); see [CONTRIBUTING.md](CONTRIBUTING.md) and [ADR-0002](docs/decisions/0002-license-apache-2.0.md).

> **Before publishing:** the module path `github.com/boltrope/boltrope` uses a **placeholder owner segment**. Rename the `boltrope` owner (the GitHub org/user) to your own across `go.mod`, all import paths, the badge URLs above, and CI/release configuration before pushing to a public remote.
