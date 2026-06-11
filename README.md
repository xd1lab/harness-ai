<!-- SPDX-License-Identifier: Apache-2.0 -->

# Boltrope

**A provider-portable, event-sourced AI agent harness in Go.**

[![CI](https://github.com/xd1lab/harness-ai/actions/workflows/ci.yml/badge.svg)](https://github.com/xd1lab/harness-ai/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/xd1lab/harness-ai.svg)](https://pkg.go.dev/github.com/xd1lab/harness-ai)
[![Go Report Card](https://goreportcard.com/badge/github.com/xd1lab/harness-ai)](https://goreportcard.com/report/github.com/xd1lab/harness-ai)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/xd1lab/harness-ai/badge)](https://securityscorecards.dev/viewer/?uri=github.com/xd1lab/harness-ai)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**English** · [繁體中文](README.zh-Hant.md)

Boltrope turns a stateless LLM completion API into a **stateful, tool-using, self-correcting agent** that you run on your own infrastructure, against any supported hosted or self-hosted model. It is backend-only (no frontend, no proprietary cloud dependency) and built on one idea: the single source of truth is an **append-only, event-sourced log in PostgreSQL**. Session resume, fork, replay, cost accounting, and observability all derive from that log.

**Why another harness?** Most agent frameworks couple the loop to one vendor's SDK, keep session state in process memory, and run tools with unrestricted host access. Boltrope draws three hard boundaries instead: a normalized `Provider` interface so the loop never imports a vendor SDK; a durable event log so a crashed agent resumes deterministically (and is never silently re-billed); and a single sandboxed tool-runtime with deny-by-default egress as the only place model-influenced code runs. Context is treated as a finite resource — actively managed via token accounting, automatic compaction, and prompt caching.

> **Status:** v1, backend-only. The agent loop, the four-family model gateway, the event store, the sandboxed tool-runtime, permissions, MCP client, and OpenTelemetry observability are implemented. See [Feature overview](#feature-overview) for what ships today and [Roadmap](#roadmap--deferred) for what is deliberately deferred.

---

## Contents

- [Quickstart](#quickstart) · [Use a real model](#use-a-real-model)
- [Install — binaries & container images](#install)
- [REST API (SSE)](#rest-api-sse) — drive it from Python/curl, no SDK
- [Examples](#examples) · [How Boltrope compares](docs/comparison.md)
- [Feature overview](#feature-overview)
- [Configuring a provider](#configuring-a-provider)
- [Architecture](#architecture)
- [Security](#security)
- [Roadmap / Deferred](#roadmap--deferred)
- [Documentation](#documentation)
- [Contributing](#contributing) · [Community & support](#community--support)
- [Development](#development) · [License](#license)

---

## Quickstart

Bring up the full stack (PostgreSQL, schema migration, and the four services — orchestrator, model-gateway, tool-runtime, `projectord`) and run one task — **keyless, no model API key required**. Bringing up the stack needs only **Docker** with the Compose plugin; the `harnessctl` client in step 4 additionally needs **Go** to build/run (`go run ./cmd/harnessctl …`, or `go build -o bin/ ./cmd/harnessctl` once). The model-gateway defaults to the built-in `stub` provider (a deterministic, network-free provider), so a clean `docker compose up` runs an end-to-end task out of the box. Point it at a real model in [Use a real model](#use-a-real-model) below.

```bash
# 1. Clone.
git clone https://github.com/xd1lab/harness-ai.git
cd harness-ai

# 2. Up — keyless. Postgres healthy -> migrate completes -> grant -> the four
#    services start. The model-gateway defaults to BOLTROPE_MODELGW_PROVIDER=stub,
#    so no .env and no API key are needed. --wait blocks until every service's
#    /readyz reports ready (each gates on its real downstream deps; see Notes).
docker compose -f deploy/docker-compose.yml up --build --wait

# 3. Confirm the whole stack is ready (each returns HTTP 200). The orchestrator's
#    HTTP edge is published on host port 8080, the gRPC edge on 9000.
curl -fsS http://localhost:8080/readyz && echo   # orchestrator: ready

# 4. Run a task. harnessctl is the gRPC client CLI; BOLTROPE_DEV_INSECURE=1 makes it
#    dial the compose edge over the shared-seed dev mTLS path (details in the note
#    below). The keyless stub replies with one deterministic text turn, so this
#    streams the assistant text and a terminal success frame.
BOLTROPE_DEV_INSECURE=1 go run ./cmd/harnessctl --endpoint localhost:9000 \
    run "Write a hello-world Go program."
# => session: 019eb1...
#    I received your task and I am working on it.
#    [result] subtype=success turns=1 cost=0.000000 USD
```

> **What the keyless run proves, and the commands beyond it.** It exercises the full distributed pipeline end-to-end — client → orchestrator (mTLS + event-sourced log) → model-gateway, plus the orchestrator → tool-runtime tool advertisement and the resumable streaming relay. (Tool *execution* in the sandbox is covered by the tool-runtime integration suite, not this network-free demo provider — point at a [real model](#use-a-real-model) to drive actual tool calls.) Useful flags: a session's standing permission mode is chosen at creation with `--permission-mode default|acceptEdits|plan` (env `BOLTROPE_CTL_PERMISSION_MODE`); under the `default` mode a real model's tool call pauses at an `[approval required]` prompt printing a `call_id` — approve it from a second terminal with `harnessctl … --session <id> approve <call-id>`. Reconnect to a dropped session with `--session <id> --after-seq <n>`; fork a trajectory with `harnessctl … fork --at-seq <n>`.

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

<details>
<summary><b>Example</b> — a real (self-hosted) model driving a tool through the sandbox</summary>

With `BOLTROPE_MODELGW_PROVIDER=openaicompat` pointed at a local Ollama serving `gemma4:26b` and a session created in `acceptEdits` mode, asking the agent to write a file:

```text
$ harnessctl --endpoint localhost:9000 --session <id> \
    run "Write a hello-world Go program to hello.go, then confirm."

[tool] wrote 77 bytes to hello.go
[result] subtype=success turns=2 cost=0.000000 USD
File hello.go has been created successfully.
```

The model issued a real `write` tool call; the policy pipeline auto-approved it (`acceptEdits` mode), the tool executed inside the per-session Docker sandbox, and the result was fed back for a second turn — the complete gather → act → verify loop. The event log records the whole trajectory:

```text
seq  event                  detail
1    SessionStarted
2    MessageAppended        user task
3    TurnStarted            model=gemma4:26b
4    AssistantMessage       tool_call: write(hello.go)
5    PermissionDecided      allow — "acceptEdits mode: file edit auto-approved"
6    ToolExecutionStarted   durable intent (idempotency key)
7    ToolResult             "wrote 77 bytes to hello.go"
8    MessageAppended        tool result fed back
9    TurnStarted            model=gemma4:26b
10   AssistantMessage       confirmation text
11   TurnFinished           success — usage 1670 in / 63 out tokens
```

Cost is `$0` because the model is self-hosted; against a metered provider the same run rolls up real token usage and USD cost on the `TurnFinished` event.

</details>

---

## Install

The Quickstart above runs everything from source. For a real deployment, use the **released artifacts** — produced by [GoReleaser](.goreleaser.yaml) on every tagged release: cross-compiled, checksummed, SBOM'd, and keyless-signed with cosign (Sigstore).

**Container images** (multi-arch `linux/amd64` + `arm64`, on GHCR) — one per service, the same images `deploy/docker-compose.yml` can pin:

```bash
docker pull ghcr.io/xd1lab/boltrope-orchestratord:latest
docker pull ghcr.io/xd1lab/boltrope-modelgwd:latest
docker pull ghcr.io/xd1lab/boltrope-toolruntimed:latest
docker pull ghcr.io/xd1lab/boltrope-projectord:latest
docker pull ghcr.io/xd1lab/boltrope-migrate:latest      # one-shot schema migration
```

**Binaries** — each GitHub release attaches two kinds of archive (cosign-signed via `checksums.txt`, verify before use):

- `boltrope_<version>_linux_<arch>.tar.gz` — the **server bundle**: the four daemons + `boltrope-migrate`, for `linux/{amd64,arm64}`.
- `harnessctl_<version>_<os>_<arch>` — the **client CLI** on its own, for `linux`, **macOS**, and **Windows** (`.tar.gz`, or `.zip` on Windows).

**From source** (Go 1.25+):

```bash
go install github.com/xd1lab/harness-ai/cmd/harnessctl@latest   # the client CLI
# daemons build with the `spire` tag for production SPIFFE/SPIRE identity:
go build -tags spire ./cmd/...
```

> Releases are cut by a maintainer pushing a `vX.Y.Z` tag. The `ghcr.io/xd1lab/…` images and the `go install` path resolve once a release is published (`v0.1.0` onward); before that, build from source as in the Quickstart.

---

## REST API (SSE)

You don't need Go or gRPC to drive Boltrope: the orchestrator's HTTP listener (port 8080 in the compose stack, next to `/readyz` and `/metrics`) serves a minimal REST/JSON facade over the **same server** the gRPC edge uses — identical auth (the shared OIDC validator; the dev stack needs no token), identical ownership checks, identical event stream.

```bash
# 1. Create a session.
SESSION=$(curl -fsS -X POST localhost:8080/v1/sessions \
    -d '{"mode":"default"}' | jq -r .sessionId)

# 2. Run a task — the response is a live Server-Sent-Events stream.
curl -NfsS -X POST "localhost:8080/v1/sessions/$SESSION/run" \
    -d '{"text":"Write a hello-world Go program."}'
# id: 2
# event: text_delta
# data: {"seq":"2","textDelta":{"text":"I received your task..."}}
#
# event: result
# data: {"result":{"subtype":"TERMINATION_SUBTYPE_SUCCESS","numTurns":"1",...}}

# 3. Out-of-band control (approve a pending tool call, interrupt, reattach):
curl -fsS -X POST "localhost:8080/v1/sessions/$SESSION/control" \
    -d '{"action":"approve","call_id":"<call-id from the approval_request frame>"}'
```

Every SSE frame carries its durable event seq as the `id:` field, so reconnecting with the standard `Last-Event-ID` header (or `{"after_seq": N}`) resumes exactly — no duplicates, no gaps. Frame payloads are canonical protojson of the `boltrope.v1.RunEvent` message; in production send `Authorization: Bearer <OIDC access token>` and terminate TLS at your ingress.

**Python, zero SDK:** [examples/python/run_task.py](examples/python/run_task.py) is a complete ~100-line client (`pip install requests`) that creates a session, streams a run, and answers approval prompts interactively.

Routes: `POST /v1/sessions` · `GET /v1/sessions/{id}` · `POST /v1/sessions/{id}/run` (SSE) · `POST /v1/sessions/{id}/control` · `POST /v1/sessions/{id}/fork`.

---

## Examples

Runnable walkthroughs in [examples/](examples/) — the first three run end-to-end
against the keyless dev stack (no API key, no client tooling):

- **[curl/](examples/curl/)** — drive a full session with nothing but `curl`: create, run, stream SSE, resume on `Last-Event-ID`.
- **[durable-resume/](examples/durable-resume/)** — inspect the per-session Postgres event log, then watch a session **survive an orchestrator restart** (the projection is rebuilt from the durable log, headSeq intact).
- **[python/](examples/python/)** — a ~100-line `requests`-only client with interactive approvals.
- **[web-research/](examples/web-research/)** — enable `webfetch`/`websearch` through the deny-by-default egress data path (needs a real model + an allowlisted host).

New here and weighing the options? [**How Boltrope compares**](docs/comparison.md) is an honest look at Boltrope next to deepagents and hive — including where they're the better choice.

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
- **Client API** — a resumable `Run` server-stream (Last-Event-ID semantics) plus a unary `Control` RPC (approve / deny / interrupt / reattach), served over **gRPC and a REST/JSON + SSE facade** (`Run` streams as `text/event-stream`; identical auth and ownership checks by construction — the facade calls the same server). See [REST API](#rest-api-sse) and [examples/python/run_task.py](examples/python/run_task.py) for the zero-SDK Python path.
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

Plus `boltrope-migrate` (runs DDL and exits — a release gate) and `harnessctl` (the client CLI/SDK). Services talk gRPC + protobuf with mTLS; the client edge is gRPC plus a minimal [REST/SSE facade](#rest-api-sse) on the orchestrator's HTTP listener (a full REST mapping for every RPC is [roadmap](#roadmap--deferred)).

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
- **Client-edge auth** validates OIDC/bearer tokens (`iss`/`aud`/`exp`, `alg=none` rejected, JWKS rotation via refresh-on-miss) and verifies session ownership on every call. Production wiring is two env vars (`BOLTROPE_OIDC_ISSUER` / `_AUDIENCE`) — the orchestrator runs OIDC discovery at startup and **refuses to start** without a reachable, issuer-matching IdP; see the [deploy walkthrough](deploy/README.md#client-edge-auth-in-production-oidc).
- **Tenant isolation** at the database layer via PostgreSQL Row-Level Security (non-owner role, `SET LOCAL` GUC from the verified token, `FORCE ROW LEVEL SECURITY`).
- **Deny-by-default egress** <a id="web-access-egress"></a> — the per-session sandbox runs with `--network none`, so in-sandbox `bash` and MCP-HTTP have **no external network**. The `webfetch`/`websearch` tools reach the outside through the **egress data path** ([ADR-0021](docs/decisions/0021-egress-data-path.md)): a hardened in-process fetcher at the tool-runtime trust boundary, mediated **per request and per redirect hop** by the deny-by-default broker (`BOLTROPE_TOOLRT_EGRESS_ALLOWLIST`; empty ⇒ deny-all), with DNS-pinned dialing and public-address-only egress (SSRF defense). `websearch` queries a configured SearXNG-compatible JSON endpoint (`BOLTROPE_TOOLRT_SEARCH_URL`). Nothing is reachable until an operator allowlists the host — and even then the sandbox namespace itself stays severed. Provider-native/server-side tools are disabled in v1.
- **Secrets** live only in model-gateway configuration (env), never in the log or any response; secret-bearing types redact via `slog.LogValuer`.

**Deploying to Kubernetes?** The production kit is the Helm chart at
[`deploy/helm/boltrope/`](deploy/helm/boltrope/) — SPIRE-issued identity,
OIDC edge auth, the migration-gate hook Job, and the sandboxed tool runtime,
**fail-closed at render time** (no OIDC issuer / no SPIRE / `stub` provider /
unacknowledged dev-insecure all refuse to render). SPIRE from zero:
[`deploy/k8s/spire/`](deploy/k8s/spire/).

Found a vulnerability? Please report it privately — see [SECURITY.md](SECURITY.md). The [ADR-0013 security model](docs/decisions/0013-security-model.md) has the full picture.

---

## Roadmap / Deferred

v1 is a deliberately focused, irreducible harness. The `Provider`, `Workspace`/`Runtime`, `EventLogPort`, and MCP abstractions are shaped so these slot in **without re-architecture** ([ADR-0003](docs/decisions/0003-v1-scope.md)):

- **MCP server mode** (and A2A interoperability) — v1 ships the MCP *client* only.
- **microVM / gVisor / OS-native sandbox backends** — v1 is containers-only behind the `Workspace`/`Runtime` port; multi-tenant execution of mutually-untrusted code is therefore out of scope for v1.
- **In-sandbox egress proxy** — `webfetch`/`websearch` reach allowlisted hosts today through the [egress data path](#web-access-egress) (an in-process hardened fetcher mediated by the broker; [ADR-0021](docs/decisions/0021-egress-data-path.md)). The sandbox itself stays `--network none`, so in-sandbox `bash` and MCP-HTTP still have no network; the forward proxy that would give the sandbox namespace a per-connection-gated path is deferred (the `EgressBroker` port and `--network` seam are shaped to slot it in without re-architecture).
- **Model routing** and advanced multi-agent topologies.
- **Full REST mapping for every RPC** — v1 ships the minimal facade ([REST API](#rest-api-sse): CreateSession / GetSession / `Run` over SSE / Control / Fork); a generated, annotations-based mapping of the complete proto surface is deferred.
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

## Contributing

Contributions are welcome. Boltrope is built **spec-first and test-first** — see [CONTRIBUTING.md](CONTRIBUTING.md) for the full workflow and [docs/decisions/0006-engineering-conventions.md](docs/decisions/0006-engineering-conventions.md) for the conventions CI enforces. In short:

- **Sign off every commit** (`git commit -s`) — we use the [Developer Certificate of Origin](https://developercertificate.org/), not a CLA; PRs with unsigned commits fail the DCO check.
- **[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)** — the type drives semantic versioning (`fix:` → patch, `feat:` → minor, `feat!:` / `BREAKING CHANGE:` → major).
- **Tests first.** Keep `go test ./...` green; add `-race` and `-tags integration` for concurrency/DB changes; keep `golangci-lint run` clean — the depguard/forbidigo rules mechanically enforce the architecture boundaries.
- Open a PR from a topic branch against `main`. All PRs need green CI (lint, unit `-race`, integration, build) and one maintainer approval; third-party GitHub Actions are pinned to commit SHAs — keep them pinned.

## Community & support

- **Questions & ideas** — open a [GitHub Discussion](https://github.com/xd1lab/harness-ai/discussions).
- **Bugs & feature requests** — use the [issue templates](https://github.com/xd1lab/harness-ai/issues/new/choose).
- **Security vulnerabilities** — do **not** open a public issue; report privately per [SECURITY.md](SECURITY.md).

### Design partners wanted

Boltrope is young and built by a small team, so it is shaped deliberately around
one kind of user: teams that **self-host**, need **DB-enforced tenant
isolation** and an **auditable event log**, and can't afford to re-bill a
crashed run. If that's you — a platform or security team standing up an internal
agent service — we want your requirements driving the roadmap. Open a
[design-partner discussion](https://github.com/xd1lab/harness-ai/discussions/categories/design-partners)
with your use case and constraints. Honest about fit: if you're prototyping
quickly or want a UI/integration catalog, [other harnesses](docs/comparison.md)
will serve you better today, and we'll say so.

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
