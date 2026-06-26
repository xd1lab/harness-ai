<!-- SPDX-License-Identifier: Apache-2.0 -->

# Boltrope

**The self-hostable AI-agent engine for teams that must own their data, prove tenant isolation, and audit every run.**

[![CI](https://github.com/xd1lab/harness-ai/actions/workflows/ci.yml/badge.svg)](https://github.com/xd1lab/harness-ai/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/xd1lab/harness-ai.svg)](https://pkg.go.dev/github.com/xd1lab/harness-ai)
[![Go Report Card](https://goreportcard.com/badge/github.com/xd1lab/harness-ai)](https://goreportcard.com/report/github.com/xd1lab/harness-ai)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/xd1lab/harness-ai/badge)](https://securityscorecards.dev/viewer/?uri=github.com/xd1lab/harness-ai)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**English** · [繁體中文](README.zh-Hant.md)

Boltrope is an AI-agent engine you run on your own infrastructure. Point it at a hosted model (Anthropic, Google, OpenAI) or one you host yourself — the same application code works against any of them. It is backend-only: no UI, no proprietary cloud, no vendor telemetry. The only things it talks to are the endpoints you point it at — your database, your model provider, your observability stack.

One idea runs through it: every run is a permanent, ordered record in a PostgreSQL database, not state held in memory. That record is what lets a run resume after a crash, be replayed or branched later, and be handed to an auditor as a complete, replayable record of what happened — and it's what keeps an agent from firing the same real-world action twice.

**Why another harness?** Many agent frameworks tie you to one model vendor, hold a run's state in memory (so a crash loses it), and run tools with the same access as the process that launched them. For a team that has to self-host and answer to a security review, those are three liabilities. Boltrope is built the opposite way:

- **No vendor lock-in.** A single internal interface means the agent works the same against any supported model, hosted or self-hosted — switching models doesn't change your application.
- **Nothing is lost in a crash, and nothing fires twice.** Every run lives in a durable database log, so a failed run resumes where it stopped, and a real-world action that already happened — an email sent, a card charged, a file written — isn't repeated.
- **Model-driven code runs in a locked box.** Tools execute in a per-session sandbox with no network by default — the one place model-influenced code can run, and it can't reach out unless you allow it.

Context is treated as a finite resource — actively managed via token accounting, automatic compaction, and prompt caching.

> **Status:** v1, backend-only (no UI yet). What works today: the agent loop, support for four model families (Anthropic, Google, OpenAI, and self-hosted/OpenAI-compatible), the durable event log, the per-session sandbox, permissions and human approvals, an MCP client, and built-in observability. See [Feature overview](#feature-overview) for the full list and [Roadmap](#roadmap--deferred) for what is deliberately left out for now.

---

## Contents

- [Quickstart](#quickstart) · [Use a real model](#use-a-real-model)
- [Install — binaries & container images](#install)
- [REST API (SSE)](#rest-api-sse) — drive it from Python/curl, no SDK
- [MCP Server mode (callee)](#mcp-server-mode-callee) — other agents delegate to Boltrope
- [Structured output](#structured-output) — get JSON your code can parse
- [Long-term memory](#long-term-memory) — durable, tenant-scoped recall across sessions
- [Planning](#planning) — durable, time-travelable task plans
- [Sub-agents](#sub-agents) — delegate a focused subtask to a depth-bounded child loop
- [Local dev mode (`boltrope-dev`)](#local-dev-mode-boltrope-dev) — one binary, no Docker, no keys
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

Routes: `POST /v1/sessions` · `GET /v1/sessions` · `GET /v1/sessions/{id}` · `GET /v1/sessions/{id}/usage` · `GET /v1/sessions/{id}/events` · `GET /v1/sessions/{id}/state` · `GET /v1/sessions/{id}/cost` · `GET /v1/cost` · `GET /v1/sessions/{id}/integrity` · `POST /v1/sessions/{id}/run` (SSE) · `POST /v1/sessions/{id}/control` · `POST /v1/sessions/{id}/fork`.

### Admin/tenant session management

`GET /v1/sessions` lists your tenant's sessions — filter by `?status=active&status=failed` (repeatable) and a half-open `[created_after_ms, created_before_ms)` window, page with an opaque `page_token` (keyset on `(created_at, id)`, `page_size` defaults to 50 and is capped at 200), and sort newest-first with `?descending=true`. It returns the control/lineage projection only (status, mode, head_seq, lineage, timestamps) — no usage/cost and no event payloads. `GET /v1/sessions/{id}/usage` returns one session's accumulated usage/cost/turns folded from the event log (an interrupted run's partial is included, never re-billed), tagged with its provenance. **Stop** a running session by reusing the existing control route — `POST /v1/sessions/{id}/control` with `{"action":"interrupt"}` — which cooperatively aborts a live run (resumable) and is an idempotent no-op on an already-finished session; no separate kill endpoint exists. Everything is RLS-scoped to your tenant: the request never carries a `tenant_id` filter, and cross-tenant/global-admin views are intentionally out of scope ([ADR-0027](docs/decisions/0027-admin-session-api.md)).

### Event-log read + time-travel

`GET /v1/sessions/{id}/events` lists a session's events as **redacted descriptors** — seq, type, actor, timestamps, blob metadata, and a bounded summary — keyset-paginated on `?after_seq=` (`page_size` defaults to 100, capped at 1000). Sensitive payloads never leak: `provider_raw` and the system prompt are always omitted (even with `?include_payload=true`), streaming crash-checkpoints are never exposed, and large tool output stays a blob reference. `GET /v1/sessions/{id}/state?at_seq=N` is **time-travel**: it reconstructs the folded control/billing projection at sequence `N` via Load-then-fold — it creates no session and re-bills nothing (`at_seq` past head clamps to head) ([ADR-0025](docs/decisions/0025-event-log-read-and-time-travel.md)).

### Session/tenant cost

`GET /v1/sessions/{id}/cost` returns a session's cost broken down **per model** (sorted by cost; an uncorrelated model is the `unknown` bucket) plus the session total; `GET /v1/cost` returns your tenant's per-model aggregate, the tenant total, and the count of distinct sessions carrying cost. The rollup is persisted by `projectord` into a rebuildable `session_cost_events` projection — idempotent over the projection cursor (keyed on each event's `global_id`), with the per-model attribution correlated at write time (`TurnStarted.Model` ⋈ the terminal turn by `TurnID`). The event log stays the billing authority; the projection is fully rebuildable. Both endpoints are RLS-scoped to your tenant ([ADR-0026](docs/decisions/0026-session-tenant-cost-read.md)).

### Tamper-evident audit

The event log isn't just append-only by convention — it's **cryptographically chained**, so a later mutation of any stored event is *detectable*. At append time, inside the same single-writer transaction that already enforces optimistic concurrency, lease fencing, and `request_id` idempotency, each event gets a **`content_hash`** (SHA-256 over the exact stored payload bytes) and a per-session **`chain_hash`** = `SHA256(prev_chain_hash ‖ content_hash)`, folded in `seq` order from a session-derived genesis. The running head lives on `sessions.chain_head`; the chain is **per-session** (it aligns with seq contiguity, RLS, and the session as the audit unit) ([ADR-0033](docs/decisions/0033-tamper-evident-hash-chain.md)).

Verify a session from any facade — it re-reads the events, recomputes both hashes, and compares against what's stored:

```bash
# REST: returns {valid, first_bad_seq, reason, checked}. Optional ?from_seq=&to_seq=.
curl -fsS "localhost:8080/v1/sessions/$SESSION/integrity"
# => {"valid":true,"firstBadSeq":"0","reason":"","checked":"11"}
```

If anyone `UPDATE`s a stored payload, its `content_hash` no longer matches (a **content mismatch**); rewrite a `chain_hash` and the link no longer verifies (a **broken link**) — either way `valid` is `false` and `first_bad_seq` points at the offending event. The two integrity digests are also exposed (as non-sensitive `content_hash`/`chain_hash` fields) on every event descriptor from [`GET /v1/sessions/{id}/events`](#event-log-read--time-travel), regardless of `include_payload`. The same operation is available as the gRPC `VerifySessionIntegrity` RPC and the MCP `verify_session_integrity` tool, and is RLS-scoped to your tenant.

**Forward-only, and tamper-*evident* not tamper-*proof*.** The hash columns are added by [migration 0009](migrations/0009_event_hash_chain.up.sql) — additive and nullable, so events written before it stay unchained and verify gracefully skips that leading prefix. This batch delivers the detection substrate; it does **not** stop an attacker with full database write access from forging a self-consistent rewrite. Anchoring the chain head **outside** the database — **signed checkpoints + a SIEM/WORM export** — is the follow-on that makes the log tamper-*proof*, and is on the [roadmap](#roadmap--deferred).

---

## MCP Server mode (callee)

Boltrope also exposes **itself** as a [Model Context Protocol](https://modelcontextprotocol.io) server, so any compliant MCP client — Claude Desktop, Cursor, another agent framework — can delegate a whole governed task to it: create a session, run an agent task, inspect state, approve/deny a pending tool call, and fork. This is the **callee** position ([ADR-0022](docs/decisions/0022-mcp-server-mode.md)): Boltrope as a sandboxed, tenant-isolated, auditable, durable, replayable execution backend other agents call over the network.

The endpoint is `POST /mcp` on the **same** HTTP listener as the REST facade and `/readyz`. It is a thin adapter over the *same* server methods, so OIDC auth, multi-tenant RLS, the approval gate, the per-tenant in-flight cap, durable resumable delivery, and at-most-once mutating actions are all inherited — identical to the gRPC and REST edges.

```bash
# 1. initialize — the MCP handshake (returns serverInfo + capabilities{tools}).
curl -fsS -X POST localhost:8080/mcp -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'

# 2. tools/list — the 12 tools: create_session, run, get_session, control, fork,
#    list_sessions, get_session_usage (admin reads), list_session_events,
#    get_state_at_seq (event reads), get_session_cost, get_tenant_cost (cost reads),
#    verify_session_integrity (tamper-evident audit verify).
# 3. tools/call run with _meta.progressToken — the reply streams back on a
#    text/event-stream leg as notifications/progress, then the terminal result.
```

The **run + approval loop keeps the call open**: a `run` `tools/call` holds its SSE leg open until the run terminates (exactly like REST's `POST .../run`). A risky tool call surfaces as an in-band approval `notifications/progress` frame carrying a `call_id`; you resolve it with a **concurrent** `tools/call control` (approve/deny) on a separate connection while the run stays open. A run needing N approvals is one `run` call interleaved with N `control` calls; a dropped leg reconnects via the durable `after_seq` cursor.

**v1 ships Streamable HTTP only**, hand-rolled (no MCP SDK), mounted on the existing listener. Honestly deferred to roadmap: stdio transport, MCP `elicitation`, stateful `Mcp-Session-Id` redelivery, full OAuth Protected-Resource-Metadata discovery, and the `prompts`/`resources`/`sampling` capabilities — see [ADR-0022](docs/decisions/0022-mcp-server-mode.md). Walkthrough: [**examples/mcp-server/**](examples/mcp-server/) (initialize → tools/list → create_session → run, in ~50 lines of POSIX shell).

---

## Structured output

Need a run to return JSON your code can parse instead of prose? Set an `output_schema` — a JSON Schema — on the run, and Boltrope holds the model to it. The same two fields work on every edge (gRPC, REST, and MCP):

- **`output_schema`** — a JSON Schema *object* the final answer must satisfy (passed inline; a non-object is rejected with a `400` before the run even starts).
- **`strict`** — ask for provider-native strict enforcement where the model supports it.

```bash
curl -NfsS -X POST "localhost:8080/v1/sessions/$SESSION/run" -d '{
  "text": "Extract the invoice as JSON.",
  "output_schema": {"type":"object","required":["total"],"properties":{"total":{"type":"number"}}},
  "strict": true
}'
```

Where the provider supports it natively — **OpenAI (Responses), Gemini, and current Anthropic models** — Boltrope sends your schema as the provider's own structured-output mode. Everywhere else (OpenAI-compatible/self-hosted endpoints, older models) it falls back to **validate-then-retry**: the final answer is checked against the schema and the model is re-asked on a mismatch, up to a cap (after which the run ends `error_max_structured_output_retries`). Either way the contract you get is identical — a schema-valid result, or an explicit, recorded failure ([ADR-0023](docs/decisions/0023-structured-output.md)).

---

## Long-term memory

The agent can **remember things across sessions**. Memory is exposed to the model as three native tools — there's no new API to call, no proto, no facade: the model writes and recalls on its own, mid-run ([ADR-0030](docs/decisions/0030-long-term-memory-via-tools.md)).

- **`memory_write`** `{namespace?, key, value, tags?}` — store a durable key/value memory (a fact, preference, or decision) that persists across sessions for this tenant. Upserts on `(namespace, key)`.
- **`memory_read`** `{namespace?, key}` — recall a memory by key. A miss is a normal "no memory found" result, not an error.
- **`memory_search`** `{query?, tags?, limit?}` — find memories by **case-insensitive substring** over the value, AND-filtered by **all** supplied tags. `limit` is capped; an all-empty search lists recent entries.

**Tenant-isolated by construction.** In production, memory lives in a Postgres table (`agent_memory`, migration `0008`) under the same **Row-Level Security** as the event log: `FORCE ROW LEVEL SECURITY` with per-operation policies keyed on the request tenant, **fail-closed** when the tenant context is unset. **Tenant A can never read or modify tenant B's memory** — proven by an integration test against real Postgres and a unit test of the in-memory store.

**Dev vs prod backing.** [`boltrope-dev`](#local-dev-mode-boltrope-dev) backs the same three tools with an **in-memory** store (a tenant-keyed map) so the feature works locally with no Postgres — enforcing the same tenant isolation via the same context seam. Production uses the **Postgres/RLS** store. The two impls live in separate packages so the dev binary keeps its pgx-free build-time fence.

**Deliberately simple — no vector/RAG.** Retrieval is key/value + tag/substring, on purpose. There is **no embedding model, no vector index, and no RAG pipeline** — that me-too complexity is explicitly out of scope ([ADR-0030](docs/decisions/0030-long-term-memory-via-tools.md)). The job is durable recall of facts by key or tag, not semantic search over a corpus.

---

## Planning

The agent can **author and track a multi-step plan** — a durable, replayable todo list — via a single tool. Like memory, there's no new API to call; the model plans on its own, mid-run ([ADR-0031](docs/decisions/0031-in-loop-virtual-tools-planning-and-subagents.md)).

- **`todo_write`** `{items: [{content, status}]}` — record or update the current task plan. Send the **complete** ordered list every time; it replaces the previous plan. Each item has a `content` (the step) and a `status` (`pending` / `in_progress` / `completed`); keep exactly one item `in_progress`. An empty array clears the plan.

**Durable and time-travelable.** Each `todo_write` appends a `PlanUpdated` event to the session's event log. The plan therefore survives replay, shows up in the [event-log read + time-travel API](#event-log-read--time-travel) as a **non-redacted** descriptor (plan text isn't a secret, unlike provider raw or system prompts), and reconstructs correctly at any `GetStateAtSeq`. Billing totals are unaffected by it.

**Re-surfaced to the model.** The **latest** plan is re-surfaced into the model's context window as a single `[current plan]` note, so the agent always sees where it is — stale plan updates never pile up.

**Not a permission mode.** This is a *planning primitive*, distinct from the session-scoped `plan` permission mode ([ADR-0019](docs/decisions/0019-session-scoped-permission-mode.md)), which is a guardrail that denies mutating tools. `todo_write` records intent; the permission mode constrains action.

---

## Sub-agents

The agent can **delegate a focused subtask to a child sub-agent** that runs its own bounded loop and returns a condensed result ([ADR-0031](docs/decisions/0031-in-loop-virtual-tools-planning-and-subagents.md)).

- **`spawn_subagent`** `{task, model?}` — hand a self-contained `task` to a child agent (it does **not** see the parent conversation). The child runs its own loop, and its condensed result is fed back to the parent as the tool result. `model` optionally overrides the child's model; omit it to inherit the parent's.

**Depth-bounded.** Recursion is capped by `BOLTROPE_SUBAGENT_MAX_DEPTH` (default `2`). The tool is **only advertised below the limit**, so the model is never offered a spawn that would be rejected — an advertised `spawn_subagent` call always succeeds the depth check. A child knows its own depth, so grandchild spawning is bounded the same way.

**Gated like any mutation.** A child can do anything, so `spawn_subagent` is classified as a mutating tool: it's **serialized** (never auto-parallelized) and flows through the **full permission pipeline** — PreToolUse hooks → policy → approval. A denied spawn never runs.

**In the loop, not the runtime.** Both `todo_write` and `spawn_subagent` are **virtual tools** handled inside the orchestrator loop rather than the tool sandbox, because they need the event log and the sub-agent spawner — which the tool-runtime deliberately can't reach. They still emit the same `ToolExecutionStarted` + `ToolResult` events as real tools, so audit, replay, and idempotency are identical ([ADR-0031](docs/decisions/0031-in-loop-virtual-tools-planning-and-subagents.md)).

---

## Local dev mode (`boltrope-dev`)

The [Quickstart](#quickstart) brings up four services plus Postgres over mTLS. That's the honest production shape — but it's a lot to stand up just to *feel* the agent loop. `boltrope-dev` is the **30-second on-ramp**: a single, pure-Go binary that runs the **same** agent loop in **one process** — in-memory event store, the keyless `stub` model, a no-exec tool sandbox, plaintext loopback, no mTLS/OIDC/Postgres ([ADR-0024](docs/decisions/0024-boltrope-dev-local-mode.md)).

```bash
# Build the one binary (no Docker, no Postgres, no keys) and run it.
go run ./cmd/boltrope-dev run
# It prints a loud NOT-FOR-PRODUCTION banner, then serves:
#   gRPC     : 127.0.0.1:8089
#   REST/SSE : 127.0.0.1:8088

# Drive a keyless task over the REST/SSE facade — no Authorization header.
curl -s -X POST localhost:8088/v1/sessions -d '{}'          # => {"sessionId":"019e…"}
curl -s -N -X POST localhost:8088/v1/sessions/<id>/run -d '{"text":"hello"}'
# event: text_delta … "I received your task and I am working on it."
# event: result      … "subtype":"TERMINATION_SUBTYPE_SUCCESS","numTurns":"1"
```

`harnessctl --insecure --endpoint localhost:8089 …` is the gRPC client against the same binary. It runs the **real** loop, policy pipeline, read-only-vs-mutation scheduling, approval gate, streaming, fork, and structured-output validate-retry — the only things stubbed are the network edges (model = keyless stub; tools = no-exec).

**This is not, and cannot become, a production deployment — by construction:**

- **It's a separate binary.** Production images never package `cmd/boltrope-dev`, so "can't run in prod by accident" is a *build-time* property, not a runtime flag. An import-graph test enforces that it pulls in **no** pgx, SPIRE/mTLS, or cross-service gRPC client edges.
- **It refuses to start on production signals.** Any of `KUBERNETES_SERVICE_HOST`, `BOLTROPE_POSTGRES__DSN`, or `BOLTROPE_OIDC_ISSUER` → fail-closed exit. It binds **loopback only**; a non-loopback bind requires the conspicuous `--i-understand-this-is-not-production` flag.
- **It's loud.** Every start prints a multi-line banner: `NOT FOR PRODUCTION · IN-MEMORY · NO RLS · NO mTLS · NO OIDC · LOOPBACK ONLY · NO-EXEC`.
- **It still runs the tenant check.** With OIDC skipped it injects a fixed synthetic single-tenant principal, so `igrpc`'s `authorizeTenant` runs the same code path — single-tenant loopback semantics *replace* multi-tenant RLS, they don't delete the check.

**Default scope, honestly:** with **no flags**, sessions are **in-memory** (non-persistent, lost on exit) and the sandbox is **no-exec** — `read`/`compute`/`sub-agent` work, but `bash` is a refusing placeholder (`"dev sandbox exec disabled"`), so the default run demonstrates the whole loop but does not run arbitrary shell/coding tasks. **SQLite/file persistence** is still re-scoped to roadmap and its `--store` flag is **rejected, not silently ignored**. See [ADR-0024](docs/decisions/0024-boltrope-dev-local-mode.md).

### Opt in: a real local model + a Docker sandbox (gemma / Ollama)

You can point `boltrope-dev` at a **real local OpenAI-compatible model** and have it **actually execute tools in a strongly-isolated Docker sandbox** — both behind **explicit, default-OFF** flags ([ADR-0029](docs/decisions/0029-boltrope-dev-real-model-and-local-exec-opt-in.md)). The stub model + no-exec sandbox stays the **default**.

```bash
# Prereqs: Docker running; Ollama serving an OpenAI-compatible API.
ollama serve                 # exposes http://localhost:11434/v1
ollama pull gemma            # or any model id you want to use

# Talk to the local model AND execute tools in a Docker sandbox.
go run ./cmd/boltrope-dev run \
  --model-url http://localhost:11434/v1 \  # OpenAI-compatible base URL
  --model gemma \                          # model id (default: stub)
  --enable-local-exec \                    # real tools in a per-session Docker container
  --enable-native-schema                   # turn on native json_schema structured output
```

- `--model-url <base-url>` points the loop at any **OpenAI-compatible** endpoint (Ollama, vLLM, LM Studio, llama.cpp, TGI, LiteLLM). When unset, the keyless `stub` model is used.
- `--model <id>` (default `stub`) sets the model id threaded through the loop and the gRPC default.
- `--model-api-key-env <ENVVAR>` (optional) names an env var whose **value** is sent as the API key — the value is **never** logged or printed in the banner (only the model endpoint + id are shown).
- `--enable-native-schema` turns on native `json_schema` structured output for the endpoint.
- `--enable-local-exec` swaps the no-exec sandbox for a real one: each session runs in its **own Docker container** with `--network none` + cgroup/PID limits, deny-by-default egress (so `webfetch`/`websearch` are denied), an in-memory dedup ledger (no Postgres), and an FS blob store in a temp dir. The container image/binary reuse `BOLTROPE_TOOLRT_IMAGE` / `BOLTROPE_TOOLRT_DOCKER_BIN`.

When local-exec is on, the banner replaces the `NO-EXEC` marker with `Sandbox     : LOCAL-EXEC ENABLED (Docker isolation: per-session container, --network none, cgroup/PID limits)` and adds a `Model       : <endpoint> <id>` line. **Everything stays NOT-FOR-PRODUCTION:** the loud banner, the loopback-only bind, and the prod-signal refusal (`KUBERNETES_SERVICE_HOST` / `BOLTROPE_POSTGRES__DSN` / `BOLTROPE_OIDC_ISSUER` → fail-closed exit) are unchanged and still run even with these flags set. **Docker is required only for `--enable-local-exec`;** the default path needs no Docker.

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

The capabilities behind the promises above — own your stack, isolate every tenant, act safely, audit everything — broken out in detail. Everything below is implemented in v1 unless explicitly marked _roadmap_.

- **Agent loop** — a single-threaded gather → act → verify (ReAct-style) loop with turns, `max_turns` / `max_budget_usd` caps, and typed termination subtypes (`success`, `error_max_turns`, `error_max_budget_usd`, `error_during_execution`, `error_max_structured_output_retries`). Cooperative cancellation and depth-limited sub-agents-as-tools. **Doom-loop (stuck-loop) detection now *terminates* the run** — when the model repeats the *same* tool call (identical name + args) `DoomLoopThreshold` times in a row (default **3**, default-on), the loop stops with the `error_doom_loop` termination reason instead of burning through `max_turns` ([ADR-0032](docs/decisions/0032-estimator-doomloop-durable-approvals.md)).
- **Multi-LLM, provider-portable** — one normalized `Provider` interface (Generate / Stream / CountTokens / Capabilities) behind the model-gateway, with adapters for **Anthropic Claude**, **Google Gemini**, **OpenAI** (Responses API primary, Chat Completions sub-flag), and an **OpenAI-compatible** adapter covering **self-hosted** endpoints (vLLM, Ollama, LM Studio, llama.cpp, TGI, LiteLLM). Capability flags resolve per `(endpoint, model)`, not per provider family. The loop holds **zero** vendor-SDK imports — adding a provider touches only an adapter package plus a capabilities-table entry.
- **Event-sourced sessions with resume & fork** — an append-only PostgreSQL log is the single source of truth. Appends are **optimistic** (compare `expected_seq`), **fenced** (lease epoch), and **idempotent** (a re-sent `request_id` is a no-op, not a conflict). After a crash, a run resumes from the durable log exactly where it stopped instead of starting over, replaying its recorded steps without re-doing completed work. Fork branches a session at any point in its history without touching the original — for time-travel debugging, or to freeze a real run into a test. _(Because completed turns aren't re-run, a resumed run isn't re-charged for them — which only matters for long, expensive runs; for short ones the difference is negligible.)_
- **Sandboxed tools** — core native tools (`read`, `edit`, `write`, `glob`, `grep`, `bash`, `webfetch`, `websearch`) run inside per-session containers behind a `Workspace`/`Runtime` port. Tool inputs are JSON-Schema-validated before execution; errors surface as an `Observation`, never a panic. On cancellation the process group is killed at the cgroup/PID-namespace boundary. A durable dedup ledger makes mutating tools at-most-once across restarts.
- **Permissions & human-in-the-loop** — a layered `deny → mode → allow → tool` policy pipeline with `default` / `acceptEdits` / `plan` / `bypass` modes, a taint-tracking egress gate for the lethal-trifecta risk, and approval decisions persisted as events (re-checkable on replay). A session's standing mode is set at creation: `harnessctl --permission-mode default|acceptEdits|plan` (env `BOLTROPE_CTL_PERMISSION_MODE`) applies when the CLI creates the session; `bypass` is operator-only and a client-supplied bypass is rejected server-side (ADR-0019). **Approvals are durable across a crash:** the moment a tool call enters the ask gate, an `ApprovalRequested` event is written *before* the loop blocks, so a restart mid-ask re-raises the same approval to the reconnecting operator (bounded by a resume timeout) and continues the run once answered — instead of silently losing the pending ask ([ADR-0032](docs/decisions/0032-estimator-doomloop-durable-approvals.md)).
- **MCP (client)** — connect Model Context Protocol servers over **stdio or HTTP** with lazy schema loading; each server runs in its own confined sandbox; first-use registration requires explicit human approval and MCP tool descriptions are treated as untrusted input.
- **Hooks / middleware** — `PreToolUse`, `PostToolUse`, `Stop`, and `PreCompact` hooks run as host subprocesses behind a `CommandRunner` port; a `PreToolUse` block prevents dispatch.
- **Context management** — running token accounting, automatic compaction before the budget threshold, append-only tool-result clearing (stubs in the window, full content retained in the log/blob store), and tenant-scoped prompt-cache prefixes. When the model gateway can't count tokens (e.g. self-hosted/Ollama endpoints), the local fallback estimate now counts **all** token-bearing content — tool results, tool-call names and args, and thinking text — not just plain text, so a noisy run with large tool outputs still trips threshold compaction ([ADR-0032](docs/decisions/0032-estimator-doomloop-durable-approvals.md)).
- **Observability** — OpenTelemetry GenAI spans (`invoke_agent` / `chat` / `execute_tool`) with `gen_ai.*` attributes and trace-context propagation over gRPC; RED metrics per RPC (errors broken down by termination subtype) and USE/saturation gauges (worker-pool, live sandboxes, PG pool, blob bytes, projection lag); `slog` JSON logs with `LogValuer` secret redaction; gRPC health + HTTP `/livez` / `/readyz` with dependency-gated readiness.
- **Client API** — a resumable `Run` server-stream (Last-Event-ID semantics) plus a unary `Control` RPC (approve / deny / interrupt / reattach), served over **gRPC, a REST/JSON + SSE facade, and an MCP server endpoint** (`Run` streams as `text/event-stream`; identical auth and ownership checks by construction — every edge calls the same server). See [REST API](#rest-api-sse), [MCP Server mode](#mcp-server-mode-callee), and [examples/python/run_task.py](examples/python/run_task.py) for the zero-SDK Python path.
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

Plus `boltrope-migrate` (runs DDL and exits — a release gate) and `harnessctl` (the client CLI/SDK). Services talk gRPC + protobuf with mTLS; the client edge is gRPC plus a minimal [REST/SSE facade](#rest-api-sse) and an [MCP server endpoint](#mcp-server-mode-callee) on the orchestrator's HTTP listener (all three call the same server methods through the same auth; a full REST mapping for every RPC is [roadmap](#roadmap--deferred)).

```
Client ──gRPC / REST+SSE / MCP (/mcp)──> Orchestrator ──┬─ gRPC ─> Model Gateway ──> LLM APIs / self-hosted
  (resumable Run / Control;               (agent loop +  │                            (Anthropic/Gemini/OpenAI)
   other agents call IN via MCP)           event store)  ├─ gRPC ─> Tool Runtime ──> Sandbox (per session)
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
- **Deny-by-default egress** <a id="web-access-egress"></a> — by default an agent's sandbox has **no internet access at all**, so model-driven code can't quietly reach out to the network; the only way out is the `webfetch`/`websearch` tools, and even they reach **only hosts an operator has explicitly allowed**. In detail: the per-session sandbox runs with `--network none`, so in-sandbox `bash` and MCP-HTTP have **no external network**. The `webfetch`/`websearch` tools reach the outside through the **egress data path** ([ADR-0021](docs/decisions/0021-egress-data-path.md)): a hardened in-process fetcher at the tool-runtime trust boundary, mediated **per request and per redirect hop** by the deny-by-default broker (`BOLTROPE_TOOLRT_EGRESS_ALLOWLIST`; empty ⇒ deny-all), with DNS-pinned dialing and public-address-only egress (SSRF defense). `websearch` queries a configured SearXNG-compatible JSON endpoint (`BOLTROPE_TOOLRT_SEARCH_URL`). Nothing is reachable until an operator allowlists the host — and even then the sandbox namespace itself stays severed. Provider-native/server-side tools are disabled in v1.
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

- **MCP server mode** ([ADR-0022](docs/decisions/0022-mcp-server-mode.md)) ships in v1 over Streamable HTTP ([see above](#mcp-server-mode-callee)). Deferred: stdio transport, MCP `elicitation`, full OAuth Protected-Resource-Metadata discovery, the `prompts`/`resources`/`sampling` capabilities, and **A2A interoperability**.
- **microVM / gVisor / OS-native sandbox backends** — v1 is containers-only behind the `Workspace`/`Runtime` port; multi-tenant execution of mutually-untrusted code is therefore out of scope for v1.
- **`boltrope-dev` real model + local exec** ([ADR-0029](docs/decisions/0029-boltrope-dev-real-model-and-local-exec-opt-in.md), amending [ADR-0024](docs/decisions/0024-boltrope-dev-local-mode.md)) — [local dev mode](#local-dev-mode-boltrope-dev) now ships **opt-in real-model wiring** (`--model-url`/`--model`, any OpenAI-compatible endpoint) and an **opt-in Docker local-exec sandbox** (`--enable-local-exec`, reusing the production runtime's per-session container with `--network none` + cgroup/PID limits), both default-off behind the loud banner + prod-signal fence. Still deferred: SQLite/file persistence (`--store`), whose flag is rejected today so the deferral is explicit and which slots onto the existing `EventLogPort` seam.
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
one kind of user: teams that **self-host**, need **database-enforced tenant
isolation** and an **auditable record of every run**, and run agents that take
real-world actions that must never fire twice. If that's you — a platform or
security team standing up an internal agent service — we want your requirements
driving the roadmap. Open a
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
