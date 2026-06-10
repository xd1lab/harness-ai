# Boltrope — v1 Architecture

> **Status:** Final architecture (Gate 3). Supersedes `00-architecture-DRAFT.md`. Drives the next-stage protobuf/Go-interface contracts and the TDD plan.
> **Date:** 2026-06-10
> **Project:** Boltrope (`github.com/boltrope/boltrope`) — a provider-portable, event-sourced AI agent harness.
> **Scope:** Backend-only, open-source (Apache-2.0), Go + PostgreSQL, microservices with internal clean/hexagonal architecture.
> **Inputs:** `docs/research/00-research-report.md`; ADRs 0001–0008; the SETTLED CONSTRAINTS; and a hostile six-lens design review whose accepted findings are folded into this document.
> **Audience:** Engineers writing the service contracts, the event-store DDL, and the first TDD harness.

This document decides and justifies the items the research deliberately left open: **service decomposition**, **inter-service communication**, **the Postgres event-store schema**, **cross-service security**, **the concurrency/cancellation model**, **the repo layout**, and the **operability/observability** posture. Each is concrete enough to generate `.proto` files, Go interfaces, and failing tests.

The design was stress-tested by an adversarial review. The most consequential change from the draft: the **event-store is no longer a separate service** — it is an in-process package inside the orchestrator backed by PostgreSQL. PostgreSQL remains the durable spine. The deployment unit count is **three** long-lived services.

---

## Table of Contents

1. [System Context Overview](#1-system-context-overview)
2. [Service Decomposition (stress-tested)](#2-service-decomposition-stress-tested)
3. [One Agent Turn — Component & Sequence Sketch](#3-one-agent-turn--component--sequence-sketch)
4. [Inter-Service Communication & Contracts](#4-inter-service-communication--contracts)
5. [Per-Service Clean-Architecture Layout](#5-per-service-clean-architecture-layout)
6. [PostgreSQL Event-Store Schema](#6-postgresql-event-store-schema)
7. [Durability, Recovery & Exactly-Once Side Effects](#7-durability-recovery--exactly-once-side-effects)
8. [Security Model](#8-security-model)
9. [Concurrency & Cancellation Model](#9-concurrency--cancellation-model)
10. [Operability: Health, Startup, Metrics, Lifecycle](#10-operability-health-startup-metrics-lifecycle)
11. [Provider Abstraction & Streaming Across Four Families](#11-provider-abstraction--streaming-across-four-families)
12. [Repository Layout](#12-repository-layout)
13. [Decisions (with rationale & rejected alternatives)](#13-decisions-with-rationale--rejected-alternatives)
14. [Open Questions Deferred Past This Gate](#14-open-questions-deferred-past-this-gate)

---

## 1. System Context Overview

The harness turns a stateless LLM completion API into a stateful, tool-using, self-correcting coding agent. The single source of truth is an **append-only, event-sourced log in PostgreSQL**; every externally observable behaviour — resume, fork, replay, cost accounting, observability — is derived from that log.

```
                              ┌──────────────────────────────────────────────────────────────┐
                              │                      BOLTROPE (backend, Go)                     │
   ┌──────────┐  gRPC (mTLS)  │                                                                  │
   │  Client  │ ───────────▶  │  ┌──────────────────────────────┐                               │
   │ (CLI/IDE │ ◀─ resumable ─│  │        Orchestrator           │                               │
   │  /CI/SDK)│   event stream│  │  (agent loop, turns, hooks,   │   in-process pgx              │
   └──────────┘  (events w/   │  │   permissions, context mgr,   │ ─────────────┐                │
                  Last-Event-ID)│   sub-agents, EVENT STORE pkg) │              ▼                │
                              │  └───┬──────────────────┬────────┘        ┌───────────┐          │
                              │      │ gRPC             │ gRPC             │ PostgreSQL│          │
                              │      │ (mTLS,           │ (mTLS,           │ (event log│          │
                              │      │  svr-stream)     │  svr-stream)     │  = SSoT)  │          │
                              │  ┌───▼─────┐    ┌───────▼────────┐         └─────┬─────┘          │
                              │  │  Model  │    │  Tool-Runtime  │    exec       │ LISTEN/poll    │
                              │  │ Gateway │    │  (registry,    │ ───────┐      │                │
                              │  │(provider│    │   Workspace/   │        ▼      ▼                │
                              │  │ adapters│    │   Runtime,     │  ┌──────────┐ ┌────────────┐   │
                              │  │ +retry/ │    │   MCP client)  │  │ Sandbox  │ │ projectord │   │
                              │  │ normaliz│    │                │  │(container │ │ (cost roll-│   │
                              │  └────┬────┘    └───┬────────┬───┘  │ per       │ │  up, OTel  │   │
                              └───────┼────────────┼────────┼──────│ session)  │ │  export)   │   │
                                      │            │        │      └──────────┘ └────────────┘   │
                                      │            │        │ egress broker (deny-by-default,     │
                               ┌──────▼─────┐ ┌────▼────────▼──┐  per-session allowlist) ─────────┘
                               │  LLM APIs  │ │ External MCP    │      (ALL model-influenced
                               │ Anthropic /│ │ servers (3rd    │       egress flows through it)
                               │ Gemini /   │ │ party tools, in │
                               │ OpenAI /   │ │ confined sboxes)│
                               │ self-hosted│ └─────────────────┘
                               └────────────┘

  Cross-cutting (every service): OTel GenAI traces + RED/USE metrics -> Collector; slog JSON logs w/
  LogValuer redaction; koanf config (flags>env>file>defaults); SPIFFE/SPIRE workload identity -> mTLS;
  gRPC health checking + HTTP readiness/liveness on every service.
```

**Reading guide.** **Three deployable services** plus one ops/projection worker:

- The **Orchestrator** owns the agent loop. It also **embeds the event store as an in-process package** (`internal/orchestrator/adapter/outbound/eventstore`) talking to PostgreSQL over `pgx`. PostgreSQL — a separate process with its own backup/migration/replica story — is the durable spine; the orchestrator is stateless and resumes from the log.
- The **Model Gateway** is a stateless provider-abstraction proxy (three heavy vendor SDKs + an OpenAI-compatible adapter): a genuinely distinct dependency surface.
- The **Tool-Runtime Service** owns the tool registry, the Workspace/Runtime sandbox abstraction, and the MCP client: the genuinely distinct *trust boundary* for model-influenced code execution.
- The **`projectord`** worker (a small deployable, not on the request path) consumes the event log's subscription feed to run cost-rollup and OTel-export projections.

The sandbox is *not* a service — it is a runtime resource managed by Tool-Runtime behind the Workspace/Runtime port. PostgreSQL and the egress broker are infrastructure, not Boltrope services.

---

## 2. Service Decomposition (stress-tested)

### 2.1 Method: stress-test the candidate split, don't recite it

The research proposed a **candidate** four-way split (model-gateway, tool/execution-runtime, persistence/event-store, orchestrator). This document's job is not to recite that list but to **stress-test each boundary** and keep it only if it earns its place against the rule below. Two boundaries survive on their merits; one (event-store) does not and is demoted to an in-process package.

**The rule applied:** *a subsystem becomes its own deployable service only if it has a distinct trust boundary, a distinct failure/scaling profile, or a distinct dependency surface that would otherwise contaminate the rest of the binary — AND extraction buys something an in-process package against a shared external datastore cannot.* Everything else is an in-process package inside the service that owns its bounded context.

### 2.2 The decision: three deployable services (+ one projection worker)

| # | Service (`cmd/<app>`) | Single-sentence responsibility | Why a separate service? |
|---|---|---|---|
| 1 | **orchestrator** (`cmd/boltrope-orchestratord`) | Runs the gather→act→verify agent loop; drives turns; enforces permissions + hooks; manages the context/token budget; spawns depth-limited sub-agents; **and persists/replays the event log to PostgreSQL via an in-process package**. The system's only "brain". | It *is* the brain; everything else exists to serve the loop. |
| 2 | **model-gateway** (`cmd/boltrope-modelgwd`) | Stateless provider abstraction: normalizes the internal message/tool model to/from each LLM provider SDK, streams token deltas, counts tokens, exposes per-`(endpoint,model)` capability flags, centralizes provider retry + stop-reason/error normalization. | **Distinct dependency surface** (three heavy vendor SDKs + OpenAI-compatible adapter) and **distinct failure/scaling profile** (I/O-bound on slow upstreams with their own rate limits; scalable and restartable independently of the loop). Isolation keeps SDK churn and provider credentials out of the brain. |
| 3 | **tool-runtime** (`cmd/boltrope-toolruntimed`) | Owns the tool registry (native + MCP), validates tool inputs against JSON Schema, executes tools inside per-session sandboxes via the Workspace/Runtime port, runs MCP servers in confined sandboxes, and enforces egress policy at the network layer. | **Distinct trust boundary.** It is the only component that runs model-influenced code and touches the sandbox + filesystem + network egress + third-party MCP servers — the blast-radius container for the lethal trifecta. Separation lets it run under the tightest network policy and lets us swap container→microVM behind its port. |

Plus **`projectord`** (`cmd/boltrope-projectord`): a long-lived worker that tails the event log's `Subscribe` feed and runs read-side projections (cost-rollup, OTel export). It is deployable and scalable but **not on the request path**; if it lags or restarts it never blocks a turn (see §10.4).

**Why event-store is NOT a separate service (the biggest change from the draft).** A thin Go gRPC shim in front of PostgreSQL is a CRUD-over-DB nano-service. Its draft justifications collapse:

- *"data-gravity, independently backed-up/migrated/read-scaled"* — that is **PostgreSQL's** property. Postgres is already a separate process with its own backup, migration (`cmd/boltrope-migrate`), and read-replica story. A Go shim adds nothing to any of those; it adds a network hop.
- *"a stateless orchestrator can crash and resume only if the log survives the orchestrator"* — true, and satisfied identically by an in-process repository talking to a separate PostgreSQL process. The log surviving requires **Postgres** to be separate, not a Go service.
- *"single place to enforce ordering + RLS"* — ordering is enforced by the `(session_id, seq)` unique constraint inside Postgres, and RLS is enforced by Postgres. Neither needs a Go service.

The cost of the shim was severe and on the **hottest path**: §3 shows multiple appends *per turn* (UserMessage, AssistantMessage+ToolCall, ApprovalGranted, ToolExecutionStarted, ToolResult, TurnFinished), recursively multiplied by sub-agents (§9.5). Each append, as a service, paid gRPC/mTLS amortization, protobuf marshal/unmarshal, an extra network failure mode, and a retry regime — for what is a single SQL `INSERT`. Demoting event-store to a package removes a gRPC service, its proto, its mTLS pair, and a per-append round-trip from the loop while **losing nothing**: resume/fork/replay still work because the events live in PostgreSQL. The `EventLogPort` interface (consumer-defined in the orchestrator's `agent` package) makes a *later* extraction — if a concrete need appears, e.g. multiple independent writers outliving the orchestrator — a non-breaking change.

**Deployment unit count: 3 long-lived service binaries + 1 projection worker**, plus one client entrypoint (`cmd/boltrope-ctl`, a CLI/SDK, not a server) and one ops binary (`cmd/boltrope-migrate`, runs DDL and exits). The sandbox runtime is a managed resource, not a deployable. PostgreSQL and the egress broker are infrastructure.

### 2.3 In-process packages (NOT services) and why not

**Inside the orchestrator:**

- **Event store** (`adapter/outbound/eventstore`) — append/load/fork/subscribe over `pgx` against PostgreSQL. A single `Store` struct with ~5 methods (`Append`, `Load`, `Fork`, `Subscribe`, `LoadSnapshot`) returning domain structs; it satisfies the consumer-defined `EventLogPort`. No internal `domain/app/adapter` ceremony — its "business logic" is SQL with an optimistic-version check; a `ConflictError` sentinel in the same package is enough (see §5.1, §6). Demoted from the draft's four-layer hexagonal stack, which was over-abstraction for a data-access layer.
- **Context/memory manager** (`internal/.../context`) — token accounting, compaction, tool-result clearing, prompt-cache prefix marking. Pure logic over the event log + token counts; servicifying it would create a chatty nano-service on the hot path. **YAGNI.**
- **Permissions / guardrails** (`internal/.../policy`) — allow/deny/ask pipeline, modes, secret masking, lethal-trifecta taint gates. A synchronous decision function called once per tool dispatch.
- **Hooks / middleware** (`internal/.../hooks`) — `PreToolUse`/`PostToolUse`/`Stop`/`PreCompact`. Modelled as an **in-process `HookRunner` port** (interface); the subprocess-invoking implementation lives in an adapter behind a `CommandRunner` port so loop tests stay deterministic (see §5.1).
- **Sub-agent orchestration** (`internal/.../subagent`) — a sub-agent is "an ordinary tool": a depth-limited child loop reusing the same loop, model-gateway, and tool-runtime, in its own goroutine with its own session. Correct and idiomatic; it is *confirming evidence* for the event-store demotion (see §9.5).
- **Planner / task tracking** — implicit in the loop for v1.

**Inside tool-runtime:** tool registry, native tools, the Workspace/Runtime adapter, and the MCP client (with lazy schema loading) — same trust boundary and lifecycle; splitting them would be nano-service sprawl.

### 2.4 Granularity justification (mandate vs. YAGNI)

- **Against the microservices mandate:** the chosen boundaries (model-gateway, tool-runtime) are precisely the ones with genuinely distinct *dependency* and *trust* characteristics — the textbook reasons to draw a service line — each with a clean, usable gRPC interface (§4). The orchestrator is the cohesive brain. `projectord` is a real read-side worker. This is "clearly bounded services with highly usable interfaces."
- **Against YAGNI / anti-nano-service:** we *reject* servicifying the context manager, permissions engine, planner, hooks, **and the event store**. A common failure mode of "microservices done badly" is turning every diagram box into a service; the draft fell into exactly that with a CRUD-over-Postgres shim on the hottest path. Three services is the smallest decomposition that honours the mandate while remaining operable by a small team.

---

## 3. One Agent Turn — Component & Sequence Sketch

A **turn** = one model round-trip plus any tool calls it requests and their results fed back; turns repeat until the model emits a text-only response, a continuation pause resolves, or a termination cap fires. The sketch shows a turn with (a) a **streaming model call** crossing orchestrator→model-gateway and (b) a **state-mutating tool call** crossing orchestrator→tool-runtime, with all state changes appended to the in-process event store (PostgreSQL). Note the **durable execution-intent** events (`TurnStarted`, `ToolExecutionStarted`) and the **checkpoint** that make mid-turn crashes recoverable (§7).

```
Client          Orchestrator (+ event-store pkg -> Postgres)      Model Gateway        Tool Runtime        Sandbox
  │                     │                                              │                    │                 │
  │ Run(session, msg,   │                                             │                    │                 │
  │     last_event_id?) │  Append(UserMessage) [seq=N, optimistic]    │                    │                 │
  ├────────────────────▶├──► Postgres INSERT (in-proc)                │                    │                 │
  │(resumable stream     │  LoadContext(session) ◀── fold events ──    │                    │                 │
  │  open w/ event seq)  │  [context: build prompt in token budget; mark tenant-scoped cache prefix]          │
  │                     │  Append(TurnStarted{turn_id})  [seq=N+1]    │                    │                 │
  │                     ├──► Postgres INSERT                          │                    │                 │
  │                     │   Generate(stream=true, normalized Request) │                    │                 │
  │                     ├────────────────────────────────────────────▶│ (adapter -> SDK, SSE; gateway      │
  │  Δ(seq,text) ◀──────┤◀── StreamEvent{TextDelta} ──────────────────┤  NORMALIZES + ACCUMULATES tool args│
  │  Δ(seq,text) ◀──────┤◀── StreamEvent{TextDelta} ──────────────────┤  internally; emits complete calls) │
  │  [periodic checkpoint: Append(AssistantMessageDelta{turn_id, text_so_far}) every M s / K tokens]         │
  │                     │◀── StreamEvent{Done|Pause: stop_reason, usage, provider_raw} ─────┤                 │
  │                     │  [assembler (app layer): assemble Message from StreamReader]       │                 │
  │                     │  Append(AssistantMessage{+ToolCall, usage, provider_raw}) [seq=N+2]│                 │
  │                     ├──► Postgres INSERT                          │                    │                 │
  │                     │  [policy: allow/deny/ask + taint gate]  [hooks: PreToolUse -> may block]            │
  │  (ask gate -> )     │  Append(ApprovalRequested) ; stream ApprovalRequest to client      │                 │
  │◀── ApprovalReq ─────┤                                            │                    │                 │
  │── Approve(Control) ─▶│  Append(ApprovalGranted) [seq=N+3]         │                    │                 │
  │   (separate RPC)    │  Append(ToolExecutionStarted{call_id, idem_key=hash(sid,seq)}) [seq=N+4]  ◀── DURABLE
  │                     ├──► Postgres INSERT  (committed BEFORE dispatch)                   │                 │
  │                     │   ExecuteTool(call, session, idem_key)      │                    │                 │
  │                     ├──────────────────────────────────────────────────────────────────▶│ run in sandbox │
  │                     │                                            │                    ├────────────────▶│
  │  ToolProgress ◀─────┤◀── server-stream progress/partial stdout ──────────────────────────┤◀─ stdout/exit ─┤
  │                     │◀── ToolResult{content, isError, truncated, blobRef?} ──────────────┤                 │
  │                     │  [hooks: PostToolUse]   [policy: secret masking on output]         │                 │
  │                     │  Append(ToolResult) [seq=N+5; large output -> blob, see §6.4]       │                 │
  │                     ├──► Postgres INSERT                          │                    │                 │
  │                     │  -- loop: feed tool result back, Generate again --                 │                 │
  │                     │   ... eventually model returns Done{StopReason=Stop} (text-only) ...│                 │
  │                     │  Append(TurnFinished{usage, cost_usd, num_turns}) [seq=N+k]         │                 │
  │  Result{final text, ├──► Postgres INSERT                          │                    │                 │
  │   usage, cost,      │                                            │                    │                 │
  │   subtype=success}  │                                            │                    │                 │
  │◀────────────────────┤  (stream closes; client holds last delivered seq for reattach)     │                 │
```

**Key properties shown:**

- **In-flight turns are durable and re-attachable** (§7). `TurnStarted` marks a turn in flight before `Generate`; periodic `AssistantMessageDelta` checkpoints persist partial generation; `ToolExecutionStarted` records execution intent *before* dispatch. A fresh orchestrator that folds the log can tell a turn was in flight and recover deterministically.
- **The client stream is resumable.** Every delivered frame carries the event `seq` (a Last-Event-ID). `Run` accepts an optional `last_event_id`; a reconnecting client resumes from the last delivered event via the `Reattach` path rather than getting a broken stream.
- **Generation is decoupled from delivery.** The orchestrator forwards deltas to the client but does **not** let a slow client backpressure the provider into holding a rate-limit slot; a relay stall deadline (§9.4) detaches a stalled client while generation completes to the log.
- **Permissions, hooks, and the egress taint gate are in-process** (no extra hops); only the *ask* gate round-trips to the client (on the side `Control` RPC).
- **Provider-opaque continuation state** (`provider_raw`) rides on the `AssistantMessage` event so Anthropic `pause_turn`/thinking-signature and similar can be replayed/continued byte-faithfully (§11.1).

---

## 4. Inter-Service Communication & Contracts

### 4.1 Decision: gRPC + Protocol Buffers for all service-to-service calls; client edge is gRPC with an optional REST/JSON gateway

- **Sync RPC, not a message bus, for the request path.** The agent loop is intrinsically synchronous request/response with streaming. A broker (NATS/Kafka) would add an always-on dependency and at-least-once redelivery semantics that fight the loop's need for ordered, exactly-relayed token deltas and immediate backpressure. We get async durability where it matters — the **event log** — without a separate broker. (Read-side fan-out off the log goes to `projectord`, which *may* later adopt a broker; the request path does not.)
- **gRPC over REST internally** because: (1) native server/bidi streaming over HTTP/2 with built-in flow control, backpressure, and cancellation — exactly what relaying token deltas needs; (2) a strongly-typed IDL (protobuf) that generates Go stubs and forces the "highly usable interface" discipline; (3) deadline propagation and standard status codes.
- **IDL = Protocol Buffers (proto3)**, compiled with `buf` (lint + breaking-change detection in CI) → `protoc-gen-go` + `protoc-gen-go-grpc`.
- **Client edge:** the services speak gRPC to each other. The outer client API (CLI/IDE/CI/SDK → orchestrator) is **gRPC by default** with an **optional REST/JSON facade via grpc-gateway** generated from the same protos; streaming degrades to chunked/SSE (server-streaming only). **The REST facade enforces identical authentication, authorization, and rate limiting to the gRPC edge** (§8.7) — it is a transcoding layer, not a second trust boundary.

Because event-store is now in-process, there is **no `event_store.proto`** and no orchestrator↔event-store RPC. The protos are `orchestrator.proto`, `model_gateway.proto`, `tool_runtime.proto`, plus a shared `common.proto`.

### 4.2 Streaming pattern per boundary

| Boundary | RPC shape | Why |
|---|---|---|
| Client → orchestrator `Run` | **server-streaming**, resumable (request carries optional `last_event_id`; each frame carries `seq`) | Client submits a turn; harness streams `EventFrame`s (`TextDelta`, `ToolProgress`, `ApprovalRequest`, terminal `Result`). A reconnecting client resumes from `last_event_id`. Control messages (approve/deny/interrupt) and reattach go on a *separate* unary `Control` RPC keyed by session id. |
| Client → orchestrator `Control` | **unary** | `Approve`/`Deny`/`Interrupt`/`Reattach{session_id, from_seq}`. Decouples control from the data stream; avoids bidi at the public edge. |
| Orchestrator → model-gateway `Generate` | **server-streaming** | Gateway streams normalized `StreamEvent{TextDelta|ThinkingDelta|ToolCallDelta|Pause|Done}`. **All provider stream-shape handling lives in the gateway** (§11.2); the orchestrator relay is provider-agnostic. |
| Orchestrator → tool-runtime `ExecuteTool` | **server-streaming** | Long-running tools stream progress + partial stdout, then a terminal `ToolResult`. Cancellation via context (§9). |
| (in-process) event store `Append`/`Load`/`Fork`/`Subscribe` | **direct Go calls** over `EventLogPort` | `Append` is a single optimistic SQL transaction; `Load`/`Subscribe` stream events; no network, no RPC. |

We deliberately **avoid client-streaming and bidirectional streaming** in v1.

### 4.3 How token deltas cross the boundary, and where assembly happens

`StreamEvent` is a protobuf `oneof`:

```protobuf
// model_gateway.proto (excerpt)
message StreamEvent {
  oneof event {
    TextDelta      text_delta      = 1;
    ThinkingDelta  thinking_delta  = 2;
    ToolCallDelta  tool_call_delta = 3;  // see §11.2: opaque per-call id + structured fragment
    Pause          pause           = 4;  // NON-terminal: re-issue with prior assistant content (§11.1)
    Done           done            = 5;  // terminal: open stop_reason string + usage + provider_raw
  }
}
service ModelGateway {
  rpc Generate(GenerateRequest) returns (stream StreamEvent);
  rpc CountTokens(CountTokensRequest) returns (CountTokensResponse); // capability-gated per (endpoint,model)
  rpc Capabilities(CapabilitiesRequest) returns (Capabilities);      // request carries model id
}
```

**Assembly lives in the orchestrator's app layer, not in the gRPC adapter.** The most defect-prone logic — turning a delta stream into a complete `Message` — is a pure app-layer component:

1. `ModelPort.Stream(...)` returns a provider-agnostic `llm.StreamReader` (`Recv() (StreamEvent, error)` / `Close()`), yielding domain `llm.StreamEvent` values. The gRPC adapter is a *thin* `StreamReader` that only calls `stream.Recv()` and maps protobuf→`llm.StreamEvent`.
2. A pure **`app/agent/assembler.go`** consumes the `StreamReader` and emits an assembled `Message` plus the terminal outcome (final / needs-tool-execution / needs-continuation). **Zero gRPC imports.**
3. The loop forwards `text_delta`/`thinking_delta` to the client live (with `seq`), periodically checkpoints partial text (§7.1), and on the terminal outcome appends **one** `AssistantMessage` event (assembled message + `provider_raw` continuation slot; raw byte-deltas are not stored — replay is deterministic over assembled messages, except the deliberately-preserved `provider_raw`, see §11.1).

This makes the accumulator unit-testable with a hand-written fake `StreamReader` feeding adversarial delta sequences (split mid-UTF-8, out-of-order ids, Pause-before-Done, duplicate Done); the gRPC mapping is tested separately against an in-memory `bufconn` server.

> **Note:** the partial-JSON-by-integer-index accumulation that the draft placed in the orchestrator relay is *deleted from the orchestrator entirely*. Stream normalization (including Gemini's path-addressed fragments and OpenAI Responses' item-scoped deltas) is the **gateway's** job; the gateway may buffer and emit only complete tool calls when a `(endpoint,model)` lacks streaming tool-call support (§11.2).

### 4.4 Service-to-service timeout / retry / error semantics (distinct from LLM-provider retries)

Two retry regimes that must not be conflated:

- **Provider retries (inside model-gateway):** honor `Retry-After`, then exponential backoff + full jitter; retry only 429/500/502/503/504/529; never 4xx. Surfaced as the normalized `ProviderError{Kind, RetryAfter, Raw}`. Timing uses an **injected `Clock` and jitter source** (§5.2) so backoff schedules are asserted deterministically. This is *upstream* (gateway↔LLM).
- **Service-to-service RPC retries (between our services):** governed separately, below. Because event-store is in-process, only `Generate`/`ExecuteTool`/`Control` are RPC boundaries.

| Concern | Policy |
|---|---|
| **Deadlines** | The client sets a deadline on `Run`; the orchestrator propagates a derived `context.Context` deadline on every downstream RPC. `Generate` inherits the turn deadline (long, up to 10 min for reasoning models) plus a *relay stall deadline* (§9.4); `ExecuteTool` carries the tool's own timeout. |
| **Idempotency** | `ExecuteTool` carries a **log-derived** `idempotency_key = hash(session_id, seq_of_ToolCall)` (NOT a fresh in-memory UUID), so any orchestrator replaying the log reconstructs the same key. Dedup state is durable (§7.2). `ExecuteTool` of a **Mutating** tool is **never** auto-retried (§7.2). `Generate` is not idempotent and is not auto-retried; on transient failure the orchestrator decides explicitly (resume the provider stream or fail-and-retry the turn). |
| **Retryable RPC codes** | Auto-retry only `UNAVAILABLE`/`DEADLINE_EXCEEDED` on genuinely **read-only/idempotent** calls (`Capabilities`, `CountTokens`, `ExecuteTool` of a tool *declared* `ReadOnly`). Configured via the gRPC `retryPolicy`. **Never** auto-retry `ExecuteTool` of a Mutating tool or `Generate`. |
| **Non-retryable** | `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `PERMISSION_DENIED`, `UNAUTHENTICATED` — surfaced as typed domain errors. |
| **Circuit breaking / load** | Bounded connections with keepalive; on sustained `UNAVAILABLE` the orchestrator surfaces a typed `error_during_execution` rather than hanging, after the bounded in-turn retry budget (§10.3) is exhausted. |
| **Error mapping** | gRPC status codes map to typed errors at the adapter boundary (`adapter/transport`), then to domain sentinels inspected via `errors.Is/As`. Domain layers never see a raw `*status.Status`. |

---

## 5. Per-Service Clean-Architecture Layout

Applied **pragmatically and Go-idiomatically**: four concentric concerns — `domain` (entities + business rules, zero external deps), `app` (use-cases orchestrating domain via **ports**), `adapter` (inbound transport + outbound integrations implementing ports), `infra` (process wiring). **Ports are small, consumer-defined interfaces** declared in the package that *uses* them. Dependency direction is strictly outer→inner. The full split is reserved for the **orchestrator** (where loop/policy/context/hooks have genuine business rules worth isolating from transport); thinner services do not boilerplate-replicate it.

**Cross-cutting determinism rule (applies to every service):** every component that sleeps, times out on its own, expires state, or generates ids takes an injected **`Clock`**, **jitter/rand source**, and/or **`IDGenerator`** through its `ports.go`. No domain/app code calls `time.Now()`, `rand.*`, or `uuid.New()` directly. This is enforced by a `depguard`/`forbidigo` rule.

### 5.1 orchestrator (`cmd/boltrope-orchestratord`) — includes the event store

```
internal/orchestrator/
  domain/                 # pure: no imports of grpc/sql/sdk
    session.go            #   Session, Turn, TerminationSubtype (success|error_max_turns|
                          #     error_max_budget_usd|error_during_execution|error_max_structured_output_retries)
    event.go              #   EventEnvelope, EventType, version rules; ToolExecution lifecycle states
    budget.go             #   token + cost accounting rules, max-turns/max-budget caps
    # message/tool/content/stop-reason types are imported from platform/llm (single source of truth);
    # NOT re-declared here (no "mirrored" copy). See §12.2.
  app/
    agent/                # the loop use-case (gather->act->verify->repeat)
      loop.go             #   orchestrates ports below; owns turn lifecycle + termination + recovery
      assembler.go        #   PURE delta-stream -> Message assembler over llm.StreamReader (§4.3)
      ports.go            #   consumer ifaces: ModelPort, ToolPort, EventLogPort, HookRunner,
                          #     ApprovalGate, Clock, IDGenerator
    context/              # token accounting, compaction, tool-result clearing, cache-prefix marking
    policy/               # allow/deny/ask pipeline, modes, secret masking, taint-tracking egress gate
    hooks/                # PreToolUse/PostToolUse/Stop/PreCompact chain (over HookRunner port)
    subagent/             # depth-limited sub-agent-as-tool runner (reuses agent.loop)
    recovery/             # fold-the-log resume: open-turn / open-tool-execution adjudication (§7)
  adapter/
    inbound/grpc/         # implements OrchestratorService; resumable Run stream; Control RPC
    outbound/modelgw/     # ModelPort impl: thin gRPC StreamReader (maps proto -> llm.StreamEvent)
    outbound/toolrt/      # ToolPort impl over tool-runtime gRPC client
    outbound/eventstore/  # EventLogPort impl: a single Store struct over pgx -> PostgreSQL
                          #   Append (optimistic tx), Load, Fork, Subscribe, LoadSnapshot; ConflictError sentinel
    outbound/hooks/       # HookRunner impl invoking subprocess hooks behind a CommandRunner port
    outbound/control/     # ApprovalGate impl backed by the Control RPC (in tests: in-memory channel)
  infra/
    config/               # koanf load+validate (flags>env>file>defaults), fail-fast
    server/               # gRPC server bootstrap, mTLS (SPIFFE), interceptors, health, readiness
    db/                   # pgx pool, RLS session-var hook, migration gate check (§6, §10)
    obs/                  # OTel tracer/meter init + RED/USE metrics + slog JSONHandler + redaction
main.go (cmd/boltrope-orchestratord)  # wiring only
```

The event-store `Store` is an *outbound adapter* of the orchestrator. `EventLogPort` is defined in `app/agent` (the consumer). If a separate event-store service is ever justified, only the adapter implementation changes — the port and the loop do not.

### 5.2 model-gateway (`cmd/boltrope-modelgwd`)

```
internal/modelgateway/
  domain/
    # Request/Response/Message/ContentPart/ToolDef/StopReason/Usage imported from platform/llm.
    capabilities.go       # Capabilities flags resolved per (endpoint, model)
    errors.go             # ProviderError{Kind,RetryAfter,Raw}; ErrorKind enum
  app/
    generate/             # use-case: select adapter, apply harness retry policy, NORMALIZE+ASSEMBLE stream
      service.go
      ports.go            # ProviderPort (Generate/Stream/CountTokens/Capabilities), Clock, Jitter — consumer-defined
      retry.go            # Retry-After -> backoff+jitter over injected Clock + Jitter (deterministic tests)
  adapter/
    inbound/grpc/         # implements ModelGateway service; maps StreamEvent oneof
    provider/
      anthropic/          # github.com/anthropics/anthropic-sdk-go
      gemini/             # google.golang.org/genai
      openai/             # github.com/openai/openai-go/v3 — Responses default; ChatCompletions sub-flag
      openaicompat/       # OpenAI-compatible base-URL adapter; uses the SHARED Chat-Completions normalizer
    normalize/            # >=3 stream normalizers: anthropic SSE, openai chat-completions SSE,
                          #   openai responses typed events (+ gemini iterator). Each emits llm.StreamEvent.
  infra/
    config/               # provider creds (env-only); per-(endpoint,model) capability table
    server/  obs/         # as above (health, readiness incl. provider reachability if probed)
```

### 5.3 tool-runtime (`cmd/boltrope-toolruntimed`)

```
internal/toolruntime/
  domain/
    tool.go               # Tool, ToolSpec (name/description/JSONSchema, SideEffect, EgressClass), Observation
    workspace.go          # Workspace/Runtime port shape: Exec, Read, Write, Mkdir, NetworkPolicy
  app/
    execute/              # validate-then-execute; parallel read-only scheduling (§9); durable dedup check
      service.go
      ports.go            # RuntimePort, RegistryPort, MCPClientPort, DedupStore, Clock — consumer-defined
    sandboxmgr/           # sandbox lifecycle: idle/absolute TTL, max-live cap, reaper keyed off session status
  adapter/
    inbound/grpc/         # implements ToolRuntime service; streams progress + terminal ToolResult
    registry/             # tool registration; merges native + MCP tools; JSON-Schema validation;
                          #   MCP tool descriptions treated as UNTRUSTED (approval-on-first-use)
    tools/                # native tools: read, edit, write, glob, grep, bash, webfetch, websearch
    mcp/                  # MCP CLIENT (stdio/http) with LAZY schema loading; each server in a confined sandbox
    runtime/
      container/          # default Workspace impl: per-session container/cgroup, deny-by-default egress,
                          #   cgroup/PID-namespace kill, hard CPU/mem/PID/wall-clock limits
      // (microvm/ , ossandbox/  -> deferred, slot in behind RuntimePort later)
    dedup/postgres/       # DedupStore impl (durable) — see §7.2
  infra/
    config/  server/  obs/  egress/   # egress broker client / network-policy enforcement
```

### 5.4 projectord (`cmd/boltrope-projectord`) — read-side worker

```
internal/projector/
  app/
    runner/               # subscribe to event log; dispatch to projections; checkpoint advance (§10.4)
      ports.go            # SubscribePort (over pgx), ProjectionPort, Clock
  adapter/
    cost/                 # cost-rollup projection
    otel/                 # OTel-export projection
    source/postgres/      # safe-advance Subscribe query (xmin-bounded cursor, §6.6)
  infra/
    config/  server/  obs/   # exposes its own health/readiness + max-lag metric
```

`projectord` connects to the same PostgreSQL. It does **not** import the orchestrator's `domain`/`app`; it reads events through the read-side schema/contract only.

---

## 6. PostgreSQL Event-Store Schema

### 6.1 Decision summary

- **One append-only `events` table**, partition-ready but not pre-sharded (range-partition by `created_at` later if volume demands).
- **Stream = session.** Per-session ordering via a `(session_id, seq)` unique constraint with a contiguous integer `seq`, with **DB-enforced contiguity** (§6.3): the insert ties `seq` to the `head_seq` transition; a `CHECK`/trigger rejects gaps and appends to non-`active` sessions.
- **Optimistic concurrency** on append via `expected_seq` against `sessions.head_seq` in one transaction, plus a **fencing token** (`lease_epoch`) check (§9.6) and a **per-append `request_id`** for true idempotency (§6.3, §7.3).
- **Global ordering for projections** uses the dual `(transaction_id xid8, global_id bigint)` cursor — the eugene-khyst pattern — read **only up to `pg_snapshot_xmin(pg_current_snapshot())`** so an out-of-order-committing transaction is never skipped (§6.6).
- **Minimum PostgreSQL version is pinned to 13** (for `xid8`/`pg_current_xact_id()`), validated at config/startup and in CI.
- **Large tool outputs**: inline `JSONB` up to a threshold (32 KiB); above it, bytes go to a tenant-scoped blob store and the event keeps a `blob_ref` (the `blobs` metadata row is written in the **same transaction** as the event; bytes are written **before** the row — §6.4, §7.4). **Tool-result clearing** is a new event that supersedes a prior result (append-only).
- **Migrations:** `golang-migrate`, embedded SQL, applied by `cmd/boltrope-migrate`; **expand/contract, forward-only** for `events`/`sessions` (destructive `down` on the log is a CI-blocked anti-pattern).
- **Multi-tenant isolation:** every row carries `tenant_id`; **RLS is enforced concretely** via a non-owner role + `SET LOCAL` GUC + `FORCE ROW LEVEL SECURITY` (§6.7, §8.3). v1's containerized isolation limits the *untrusted-code* multi-tenancy claim (§8.6).

### 6.2 DDL sketch

```sql
-- ============================================================
-- Tenancy & sessions (aggregate roots)
-- ============================================================
CREATE TABLE tenants (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A session is the event-sourcing stream/aggregate. head_seq is the optimistic
-- version. lease_* implement the fenced single-writer lease (§9.6).
CREATE TABLE sessions (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL REFERENCES tenants(id),
    parent_id       UUID REFERENCES sessions(id),         -- set for forks
    forked_from_seq BIGINT,                                -- frozen parent seq the fork branched at
    status          TEXT NOT NULL DEFAULT 'active',        -- active | finished | failed
    mode            TEXT NOT NULL DEFAULT 'default',       -- permission mode (ADR-0019; migration 0004)
    head_seq        BIGINT NOT NULL DEFAULT 0,             -- optimistic version (last seq)
    lease_owner     TEXT,                                  -- current writer identity (SPIFFE ID + instance)
    lease_epoch     BIGINT NOT NULL DEFAULT 0,             -- monotonic fencing token
    lease_expiry    TIMESTAMPTZ,                           -- TTL; heartbeat renews
    last_event_at   TIMESTAMPTZ,                           -- stuck-session detector input
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fork_seq_le_parent CHECK (forked_from_seq IS NULL OR forked_from_seq >= 0),
    CONSTRAINT sessions_mode_chk CHECK (mode IN ('default', 'acceptEdits', 'plan', 'bypass'))
);
CREATE INDEX ON sessions (tenant_id);

-- ============================================================
-- The append-only event log (single source of truth)
-- ============================================================
CREATE TABLE events (
    global_id      BIGINT GENERATED ALWAYS AS IDENTITY,     -- global ordering id
    transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id(), -- gap-safe poll cursor (Postgres >=13)
    tenant_id      UUID NOT NULL REFERENCES tenants(id),
    session_id     UUID NOT NULL REFERENCES sessions(id),
    seq            BIGINT NOT NULL,                          -- per-session contiguous sequence (1..N)
    request_id     UUID NOT NULL,                            -- per-append idempotency token (§7.3)
    event_type     TEXT NOT NULL,                            -- UserMessage | AssistantMessage | AssistantMessageDelta |
                                                             -- ToolCall | ToolExecutionStarted | ToolResult |
                                                             -- ToolResultCleared | ApprovalRequested | ApprovalGranted |
                                                             -- ApprovalDenied | CompactBoundary | TurnStarted |
                                                             -- TurnFinished | TurnAborted | AgentError | ...
    schema_version INT NOT NULL DEFAULT 1,                   -- per-event-type payload versioning
    payload        JSONB NOT NULL,                           -- normalized event body
    provider_raw   JSONB,                                    -- opaque provider continuation state (§11.1)
    blob_ref       TEXT REFERENCES blobs(ref),               -- set when payload offloaded
    token_usage    JSONB,                                    -- {input,output,cache_read,cache_write}
    cost_usd       NUMERIC(12,6),                            -- per-event cost when relevant
    actor          TEXT NOT NULL DEFAULT 'system',           -- user | assistant | tool | system
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (global_id),
    -- per-session ordering + optimistic concurrency:
    CONSTRAINT uq_events_session_seq UNIQUE (session_id, seq),
    -- true idempotency: a re-sent append with the same request_id is a no-op, not a conflict:
    CONSTRAINT uq_events_session_request UNIQUE (session_id, request_id)
);

CREATE INDEX idx_events_session_seq ON events (session_id, seq);   -- replay/load
CREATE INDEX idx_events_txn_global  ON events (transaction_id, global_id); -- gap-safe projection cursor
CREATE INDEX idx_events_tenant      ON events (tenant_id, global_id);      -- tenant-scoped scans

-- ============================================================
-- Snapshots (replay-from-snapshot optimization)
-- ============================================================
CREATE TABLE session_snapshots (
    session_id    UUID NOT NULL REFERENCES sessions(id),
    seq           BIGINT NOT NULL,           -- snapshot reflects this session's events up to & incl. seq
    parent_prefix JSONB,                      -- for forks: the inherited (parent_id, at_seq) lineage folded in
    state         JSONB NOT NULL,             -- derived session state
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, seq)
);

-- ============================================================
-- Projection / subscription checkpoints (read-side offsets)
-- ============================================================
CREATE TABLE event_subscriptions (
    name                  TEXT PRIMARY KEY,        -- e.g. "cost-rollup", "otel-exporter"
    last_transaction_id   xid8 NOT NULL DEFAULT '0',
    last_global_id        BIGINT NOT NULL DEFAULT 0,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- Large-output blob references (object-store offload) — TENANT-SCOPED identity
-- ============================================================
CREATE TABLE blobs (
    tenant_id   UUID NOT NULL REFERENCES tenants(id),
    ref         TEXT NOT NULL,              -- per-tenant content key (e.g. sha256 of bytes); NOT global
    media_type  TEXT NOT NULL,
    size_bytes  BIGINT NOT NULL,
    storage_uri TEXT NOT NULL,              -- tenant-prefixed path or s3://tenant=<id>/... ; bytes OUT of PG
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, ref)            -- composite: cross-tenant dedup is FORBIDDEN
);

-- ============================================================
-- Tool-execution dedup ledger (durable; survives restart) — §7.2
-- ============================================================
CREATE TABLE tool_executions (
    tenant_id      UUID NOT NULL REFERENCES tenants(id),
    session_id     UUID NOT NULL REFERENCES sessions(id),
    idempotency_key TEXT NOT NULL,          -- = hash(session_id, seq_of_ToolCall)
    status         TEXT NOT NULL,           -- started | completed | failed | unknown
    result_ref     TEXT,                    -- pointer to the ToolResult event/blob when completed
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id, idempotency_key)
);
```

`FORCE ROW LEVEL SECURITY` + policies are added by the same migration on `events`, `sessions`, `session_snapshots`, `blobs`, `event_subscriptions`, and `tool_executions` (§6.7).

### 6.3 Append (optimistic concurrency + fencing + idempotency + contiguity) — the critical transaction

```sql
-- Inputs: :session_id, :tenant_id, :expected_seq, :lease_epoch, :request_id, :event_type, :payload, ...
-- Tenant GUC is set per-connection on acquire (see §6.7), so RLS applies.
BEGIN;
  -- (a) idempotency short-circuit: if this exact append already landed, return it (NOT a conflict).
  --     handled in Go: SELECT ... WHERE session_id=:session_id AND request_id=:request_id; if found -> OK.

  -- (b) optimistic gate + fencing token + status guard, in one UPDATE:
  UPDATE sessions
     SET head_seq = head_seq + 1, updated_at = now(), last_event_at = now()
   WHERE id = :session_id
     AND head_seq = :expected_seq      -- optimistic version
     AND lease_epoch = :lease_epoch     -- fencing: reject a stale (fenced-out) writer
     AND status = 'active'              -- reject appends to finished/failed sessions
  RETURNING head_seq;                   -- new seq, tied to the head transition (contiguity by construction)

  -- if 0 rows -> ROLLBACK -> classify: stale seq => FAILED_PRECONDITION (reload/rebase);
  --   stale epoch => FENCED (yield session); non-active => FAILED_PRECONDITION.

  INSERT INTO events (tenant_id, session_id, seq, request_id, event_type, payload,
                      provider_raw, blob_ref, token_usage, cost_usd, actor)
       VALUES (:tenant_id, :session_id, :returned_head_seq, :request_id, :event_type, :payload,
               :provider_raw, :blob_ref, :usage, :cost, :actor);
COMMIT;
```

- **Contiguity** is by construction: `seq` is the value returned by the head-transition `UPDATE`, never an app-computed `expected_seq+1` derived from a possibly-stale read. The unique constraint is a backstop; a periodic gap-scan in `projectord` alerts if one ever appears.
- **Idempotency** (step a + `uq_events_session_request`): a retried append whose original committed but whose ack was lost returns the existing row as success — it is **not** misclassified as a lost race. A genuine conflict (same seq, different `request_id`) returns `FAILED_PRECONDITION`. This corrects the draft's conflation of "does not double-write" with "idempotent".
- **Fencing** (`lease_epoch`): a writer that lost its lease (TTL expiry, takeover) is rejected even if its `expected_seq` is current, so a stolen-lease writer cannot append (§9.6).

### 6.4 Large tool outputs (write-before-reference, transactional reference)

1. Tool-runtime/orchestrator writes the **bytes to the blob store first** and waits for fsync/200.
2. The append transaction inserts the `blobs` metadata row **and** the `events` row **together** (the `blob_ref` FK makes a dangling reference impossible: the event cannot reference a blobs row that is not in the same committed tx).
3. The event `payload` keeps a lightweight descriptor (first/last N bytes + summary) for just-in-time retrieval; full bytes are fetched on demand, **authorized by `(tenant_id, ref)` and session ownership**, never by `ref` alone (§8.5).

A background sweeper in `projectord` deletes blob bytes whose `(tenant_id, ref)` has no referencing event after a grace period (refcount-by-scan), and a startup check flags any event whose `blob_ref` is absent from `blobs`.

### 6.5 Tool-result clearing (append-only, validated, fork-safe)

`ToolResultCleared{cleared_ref, reason}` does not delete rows. The reference is a **`(session_id, seq)` pair** (the global ordering key), not a bare `seq` — so after a fork it is unambiguous. At append time it is validated: the target must exist, be a `ToolResult`, and not already be cleared (`FAILED_PRECONDITION` otherwise); clearing is **idempotent** (clearing an already-cleared result is a no-op) so replay and fork composition are order-insensitive. When `context` rebuilds the model-visible window it renders the superseded result as a stub; the full result stays in the log/blob store.

### 6.6 Resume / fork / replay

- **Resume:** `Load(session_id, from_seq=last_snapshot_seq)` folds events (optionally from a snapshot) into in-memory state. The `recovery` package then adjudicates any open turn / open tool-execution (§7) before continuing.
- **Fork:** `Fork(session_id, at_seq)` captures `at_seq` as an **immutable, frozen** `forked_from_seq` (validated `<=` parent `head_seq` at fork time; the parent may keep appending past it without affecting the child). **The child's `seq` continues from `at_seq+1`**, so the composed root-to-leaf timeline has a single monotonic `seq` namespace — there is no `seq=1` collision between parent and child. Snapshots of a forked child record the inherited `parent_prefix` lineage they fold. A fork is a **new branch, never a rewrite**. **Fork requires the caller's tenant to own the parent session** (enforced in the handler and by RLS); a fork never crosses tenant boundaries (§8.9).
- **Replay:** deterministic because events store assembled messages (not raw token deltas) and tool *results* (not re-executed tools). Determinism is explicitly **only for committed messages**; an interrupted live generation is recovered, not replayed identically (§7.1). The `provider_raw` slot makes provider-opaque continuation faithful where the provider requires it (§11.1).

### 6.7 RLS mechanism (concrete)

RLS is enforced, not decorative:

- The app connects via a **non-owner role without `BYPASSRLS`**.
- On every connection acquire (a `pgx` `AfterConnect`/acquire hook), the orchestrator runs `SET LOCAL app.current_tenant = :tenant_id` where `tenant_id` is derived from the **verified** principal token (§8.2), never a client-supplied field.
- `FORCE ROW LEVEL SECURITY` policies on `events`, `sessions`, `session_snapshots`, `blobs`, `event_subscriptions`, `tool_executions` are keyed on `current_setting('app.current_tenant')::uuid` for `SELECT`/`INSERT`/`UPDATE`.
- Enabling RLS is part of the migration that creates each table, and policies cover **INSERT/UPDATE** on the append path (not just SELECT) so a missing policy fails-closed in tests rather than silently in production.
- An integration test runs an append/load **with the `WHERE tenant_id=` predicate removed** and proves RLS still blocks cross-tenant rows.

---

## 7. Durability, Recovery & Exactly-Once Side Effects

This section closes the draft's central operability/consistency gap: the headline "resume after a mid-turn crash" promise was false for the longest in-flight operations (model generation, tool execution).

### 7.1 In-flight model turns are durable and re-attachable

- **`TurnStarted{turn_id}`** is appended *before* `Generate`. A recovered orchestrator that folds the log and finds an open `TurnStarted` with no terminal `AssistantMessage`/`TurnAborted` knows a turn was in flight.
- **Periodic checkpoints:** during streaming the loop appends `AssistantMessageDelta{turn_id, text_so_far}` every M seconds or K tokens (configurable), and records the provider's resumable cursor when one exists (e.g. Anthropic `pause_turn` content; see §11.1). This bounds lost work to one checkpoint interval.
- **Recovery decision** is explicit, not silent re-run: on resume, an open turn is either (a) continued from the provider's resumable cursor where available, or (b) deterministically marked `TurnAborted{usage_so_far}` and surfaced as a resumable decision. The partial generation is never silently re-billed.
- **Client reattach:** every delivered frame carries its event `seq`. The `Control.Reattach{session_id, from_seq}` RPC re-opens a stream from the last delivered event. Client delivery is decoupled from provider generation (§9.4), so a client disconnect never loses the generation — it continues to the log and the client tails it.
- **Cost on abandonment:** an aborted partial turn appends `TurnAborted{usage_so_far}` (provider usage is read from the stream's last `message_delta`/`usageMetadata`/`response.*` usage, §11.6) so cost is accounted, not under-counted.

§3's "a crash anywhere lets a fresh orchestrator resume" is now true because of these durable boundaries; the draft's contradictory claim is corrected.

### 7.2 Exactly-once-ish side effects for state-mutating tools

The draft's idempotency story (in-memory dedup + declarative gRPC retries) was unsound: the retries that matter fire exactly when tool-runtime crashed/restarted, wiping an in-memory cache; gRPC auto-retry of a streaming RPC that already received a response is not retry-eligible; and an idempotency key cannot make `bash` idempotent. The corrected protocol:

1. **Durable execution intent before side effects.** The orchestrator appends **`ToolExecutionStarted{call_id, idempotency_key}`** and commits it *before* dispatching `ExecuteTool`. The key is **log-derived**: `idempotency_key = hash(session_id, seq_of_ToolCall)`, so any orchestrator replaying the log reconstructs the same key.
2. **Durable dedup ledger.** Tool-runtime records execution status in the `tool_executions` table (Postgres), keyed `(tenant_id, session_id, idempotency_key)` — **not** in-memory/TTL. A retried call with a known completed key returns the prior result instead of re-running.
3. **Recovery is at-most-once for mutating tools.** On resume, if a `ToolExecutionStarted` has no terminal `ToolResult`, the outcome is **UNKNOWN**: a Mutating tool is **not** blindly re-dispatched. The orchestrator queries `tool_executions` by key; if still unknown, it surfaces the call for human/hook adjudication or requires the tool to be explicitly declared idempotent/compensatable. Default mutating tools terminate in an explicit `unknown` state rather than at-least-once double execution.
4. **gRPC retry scope.** Declarative `retryPolicy` applies **only** to genuinely idempotent reads (`Capabilities`, `CountTokens`, `ExecuteTool` of a `ReadOnly` tool), never to `ExecuteTool` of a Mutating tool.

This also fixes the data-consistency critical (at-least-once mutating tools) and makes §6.6's determinism claim honest: the log carries durable execution intent, so recovery is safe.

### 7.3 Append idempotency cache scoping

The `tool_executions` dedup namespace and the per-append `request_id` are both **tenant+session scoped** (`(tenant_id, session_id, ...)`); a cache hit re-checks authorization against the current caller's tenant/session before returning bytes. A client-supplied idempotency value is never trusted as a global key — keys are server-derived (`hash(session_id, seq)`), closing the cross-session/cross-tenant leak and poisoning vectors (§8.8).

### 7.4 Blob write atomicity

Covered in §6.4: write-before-reference, blobs-row-in-same-tx-as-event, FK prevents dangling references, sweeper reclaims orphans.

### 7.5 Workspace recovery on resume (explicit model)

The workspace **filesystem is not an event**; `bash` side effects (installed packages, created files, git state) cannot be replayed from results. v1 therefore adopts an explicit, documented model rather than the draft's incorrect "resume just works":

- **v1 default — clean-workspace resume:** resume re-attaches to a **fresh** per-session container; the model-visible history is intact but uncommitted filesystem state from the prior container is **not** restored. The harness records a `WorkspaceReset` marker on resume and the system prompt/notice informs the agent that uncommitted FS state was lost. Tools that mutate durable state are expected to use git (committed) or the event log; this matches how a developer's machine behaves after a crash.
- **Seam for durable workspaces (deferred):** the Workspace port is shaped so a future backend can snapshot the container/volume and reference the snapshot from an event, giving consistent FS re-attach. This is deferred (consistent with ADR-0005's microVM/snapshot direction) but the port does not foreclose it.

This makes the resume guarantee precise and compatible with non-replayable `bash` side effects.

---

## 8. Security Model

Tied explicitly to the **lethal trifecta** (private-data access + untrusted content + external communication). The draft claimed "breaking any one leg defeats the attack"; the review showed that claim was **false as drawn** — all three legs converge in the orchestrator's model context, and the sandbox egress control did not sit on the model-driven egress paths (`webfetch`/`websearch`/MCP). This section states what is *actually* severed and proves it with required tests.

### 8.1 Service identity & authN/Z (service-to-service)

- **Workload identity via SPIFFE/SPIRE.** Each service gets a short-lived, auto-rotated X.509-SVID from the SPIRE Workload API (`go-spiffe`), SPIFFE IDs like `spiffe://boltrope.local/ns/default/sa/orchestrator`. No long-lived shared service secrets.
- **mTLS between all services**, from SVIDs via `go-spiffe`'s `tlsconfig` helpers wired into gRPC `credentials.TransportCredentials`.
- **AuthZ — verb + row.** A gRPC interceptor checks the peer SPIFFE ID against a per-RPC allowlist (only `orchestrator` may call `tool-runtime.ExecuteTool`, etc.): coarse, deny-by-default service-level RBAC that gates the **verb**. **It is paired with object-level checks** so it is not the only line: every data access is also constrained by the propagated tenant token (§8.2) and by RLS at the DB. *Service identity gates the verb; the tenant token + RLS gate the row; neither alone is sufficient.*
- **Dev fallback fails closed.** A static-cert mTLS provider exists behind the `infra/server` port for local `docker compose`. It **refuses to start unless `BOLTROPE_DEV_INSECURE=1`** is set, logs a loud warning, generates ephemeral certs at startup (none committed), and is compiled out / disabled in release images. **Startup assertion:** if not in dev mode, the SPIFFE provider MUST be present or the process exits. This prevents a silent production downgrade to static certs.

### 8.2 End-user / tenant identity propagation (concrete token)

- The client authenticates to the orchestrator at the edge (§8.7). The verified `tenant_id` + `principal` are placed in a typed `context.Context` value and propagated to tool-runtime as a **short-lived signed token** — concretely a **PASETO/JWT** with `aud` = the callee's SPIFFE ID, `exp` in seconds, a `jti` + nonce for replay protection, **bound to the specific RPC** (method + `session_id`), signed by the orchestrator's SVID and verified via the SPIFFE trust bundle. This is not a bare trusted header.
- **Threat-model acknowledgement:** because the orchestrator asserts tenancy on downstream calls, **orchestrator compromise = tenant compromise** under this design. Mitigations: the token is RPC-bound and short-lived (a captured token cannot be replayed across calls or after expiry); `projectord`/cost paths derive tenancy from the event rows (RLS-scoped), not from an orchestrator assertion; and we add anomaly monitoring for an orchestrator asserting many distinct tenants in a short window. For the event store specifically, tenancy is enforced by RLS on the same connection (§6.7), so even the brain cannot read across tenants without setting the GUC, which is driven by the verified token.

### 8.3 Tenant isolation in the data layer

RLS is enforced concretely per §6.7 (non-owner role, `SET LOCAL` GUC from the verified token, `FORCE ROW LEVEL SECURITY`, INSERT/UPDATE policies, a predicate-removed cross-tenant test). RLS is the **backstop**; the app-layer `WHERE tenant_id=` is the primary path, but a single forgotten predicate no longer becomes a cross-tenant breach.

### 8.4 The trifecta: what is actually severed (egress as a request property)

The decisive **v1** control is the **per-session sandbox network namespace**: every sandbox is created with `--network none` by **default**, so ALL model-influenced tools — in-sandbox `bash`, and any process it spawns — have **no external network at all**. The namespace is the actual containment; severing the network is not a per-request decision in v1, it is the standing posture of the sandbox. The egress broker is the **policy layer** layered on top of that, not a replacement for it:

- **Sandbox network namespace is the enforcement.** `bash` (and everything it forks) is contained because the container has no network, not because a broker individually gates each connection. There is no "arbitrary egress" path: with `--network none` there is no egress path to be arbitrary. (This corrects the draft's self-contradictory "arbitrary egress (to allowlisted hosts)" and the earlier overstatement that bash/MCP-http traffic is individually routed through the broker today.)
- **The egress broker is the deny-by-default allowlist (policy).** It enforces a per-session host allowlist and is the place an operator widens egress: the tool-runtime wiring constructs the broker and installs the policy from configuration (`BOLTROPE_TOOLRT_EGRESS_ALLOWLIST`; empty ⇒ deny-all). It fails closed and is an INFRA control (non-bypassable by mode). On its own, however, it only decides *policy* — it does not open a network path. Gating *allowlisted* egress (letting the allowlist actually permit a connection instead of the namespace denying all) additionally requires the **egress-proxy data path** below.
- **`webfetch`/`websearch` are external communication, not reads.** They carry `EgressClass = External` so they consult the egress broker (and the **taint gate** below) before fetching — they are **not** silently parallelized as harmless `ReadOnly`. A read of `https://attacker.tld/?secret=...` is a write to the attacker. **In v1 they are effectively disabled**: with the sandbox on `--network none` and no egress data path wired, a fetch has nowhere to go unless an operator both configures an allowlist **and** provisions the egress-proxy path (roadmap).
- **Taint-tracking gate.** The orchestrator's `policy` package taint-tracks untrusted ingress: once any untrusted content (web/MCP/file output) enters a session's context, external-comms tools targeting non-allowlisted hosts require a human **ask** gate for the rest of that turn/session. This gates the "external communication" leg *on the model-driven path* — the leg an injection actually controls — as a policy decision, complementing the namespace.
- **Honest mapping.** *Private data* is reachable only under RLS/tenant-token scoping; *untrusted content* is confined to tool-runtime and masked (best-effort, §8.10) before entering context; *external communication* is **severed in v1 by the sandbox network namespace** (`--network none`), with the egress broker (deny-by-default allowlist) + taint gate as the policy layer over it. We prove the severed leg with a test that injects a web page instructing exfil via `webfetch` and asserts the request is blocked/gated.

> **Roadmap (egress-proxy data path — ADR-0003 deferred).** v1 contains egress by the namespace and ships the broker as the *policy* layer; it does **not** yet wire a network data path that lets an allowlisted host actually be reached. A future **egress proxy** — the sandbox is given a constrained network whose only route is a forward proxy that consults the broker's per-session allowlist per connection — is the path that turns the allowlist into live, gated egress (and re-enables `webfetch`/`websearch` for allowlisted hosts). Until then, the allowlist is enforced "deny-all in practice" by the namespace, and a configured allowlist is carried for policy/observability and forward-compatibility. The `EgressBroker` port and the `--network` seam are shaped so this slots in without re-architecture.

### 8.5 Blob access control

Blob identity is **tenant-scoped** (`PRIMARY KEY (tenant_id, ref)`, §6.2); cross-tenant content-addressed dedup is **forbidden** (it was an existence oracle). Every blob fetch authorizes against the requesting session's verified `tenant_id` **and** ownership, never the `ref` alone; `storage_uri` is tenant-prefixed and the `BlobStorePort` enforces the prefix. RLS covers `blobs`.

### 8.6 Multi-tenant vs. isolation-runtime coupling (stated honestly)

v1 runs model-influenced code in a **shared-kernel container**; microVM/gVisor is deferred (ADR-0005). The research is explicit that containers alone are insufficient for *untrusted* code. Therefore:

- **v1 containerized isolation is declared safe only for single-tenant or trusted-code deployments.** The multi-tenant data model (RLS, `tenant_id`) is **future-proofing**; it is *not* a claim that v1 safely runs mutually-untrusted tenants' code on shared hosts.
- **Multi-tenant-with-untrusted-code REQUIRES the microVM/gVisor runtime** behind `RuntimePort`, which is consequently a prerequisite for any such deployment — not a silent assumption. The README/threat-model state this plainly so the diagram (RLS) cannot imply a safety level the kernel boundary cannot deliver.

### 8.7 Client edge authN/Z, rate limiting, session ownership

- **AuthN:** validate `iss`/`aud`/`exp`, **pin signing algorithms** (reject `alg=none`), cache JWKS with rotation. v1 supports bearer/OIDC; pluggable.
- **AuthZ (ownership, not just authentication):** `session_id` on every `Run`/`Control` is **verified to be owned by the authenticated principal/tenant**. This is the public-edge analogue of the RLS check: tenant A cannot `Interrupt`, `Reattach`, or `Run` against tenant B's session even with a valid token of its own.
- **DoS/cost bounds:** per-tenant rate limiting on `Run`/`Control`, and a per-tenant concurrent-session + budget cap (a `Run` can pin a goroutine and provider spend for up to 10 min).
- **REST parity:** the grpc-gateway facade enforces identical auth, ownership, and rate limiting.

### 8.8 Idempotency-key safety

Tool dedup keys are **server-derived** (`hash(session_id, seq)`) and namespaced `(tenant_id, session_id, ...)` (§7.3). A client/model-supplied value is never a global cache key, closing cross-session leakage and pre-seeding/poisoning.

### 8.9 Fork ownership

`Fork` verifies the caller's tenant owns the parent session and enforces `parent_id`/`tenant_id` equality at the DB layer and via RLS; a fork never crosses tenants. Test: forking a foreign session is denied (§6.6).

### 8.10 Secrets, masking (demoted), and prompt-cache scoping

- **Provider API keys** live **only** in model-gateway config (env-only), never in the orchestrator or in events.
- **Output masking is defense-in-depth only — never a trifecta containment leg.** It catches only known/registry/pattern secrets and is trivially defeated by an adversarial model (base64/hex/rot13, splitting across calls, paraphrase) and does nothing for non-secret private data. We keep it for **logs/telemetry hygiene** and state explicitly in this section that it is best-effort. The real exfiltration control is egress restriction (§8.4), because a laundered secret leaving cannot be reliably detected.
- **slog `LogValuer` redaction** on secret-bearing types.
- **Prompt-cache prefixes are tenant-scoped.** Only tenant-agnostic content (system prompt, tool schemas, `AGENTS.md`/`CLAUDE.md`) may live in a shared stable prefix; private/session data is never placed in a cached prefix. Cache keys are scoped per `(tenant_id[, session])` to prevent a cross-tenant cache hit or a hit-latency timing oracle. Provider-side prompt-cache retention/data-handling assumptions are documented per the trifecta privacy concern. Test: two tenants never share a cache entry containing private content.

### 8.11 MCP trust (third-party servers)

Third-party MCP servers are an untrusted supply-chain and injection vector and are now first-class in the threat model:

- **Each MCP server (stdio and http) runs inside its own confined sandbox** with the same `--network none` default as every other sandbox — **never** as a bare child of tool-runtime. In v1 the network namespace is the containment, so an http MCP server has no egress path; the egress broker (§8.4) is the deny-by-default allowlist policy, and reaching an allowlisted MCP/registry host requires the egress-proxy data path (§8.4 roadmap). The SPIRE Workload API socket / SVID is **never** exposed into that namespace.
- **MCP tool descriptions/schemas are treated as untrusted content** (tool-poisoning is a known attack): raw third-party descriptions are not injected into the system/tool-def region without review, and **first registration of a server and each of its tools requires explicit human approval**.
- **Identity/version pinning:** MCP server identity/version is pinned (hash); a newly-appearing tool is gated.
- **MCP tool outputs** flow through the same masking + taint-tracking as other untrusted content and are subject to the egress/ask gate (§8.4).

### 8.12 Provider-native / server-side tools (v1 stance)

Provider-native tools (Anthropic `web_search`/`web_fetch`, OpenAI Responses built-ins) execute *inside the provider*, bypassing tool-runtime's registry, JSON-Schema validation, the ask gate, the egress broker, and the read-only/mutating scheduler — both an extensibility hole and a trifecta gap (provider-side web fetch over untrusted content with none of §8's controls).

**v1 decision: provider-native/server-side tools are DISABLED in the gateway.** All tools flow through tool-runtime so §8's controls hold and the trifecta model is sound. The gateway carries a `supports_server_side_tools` capability flag and a hard policy switch; enabling them is deferred and, if ever enabled, must model them as a distinct tool category with their own permissions/observability path and an explicit amendment to §8.4 acknowledging provider-network egress. This interacts with the `Pause` handling in §11.1.

### 8.13 Tool execution guardrails & the `bypass` mode

- **Validate-then-execute:** JSON-Schema validation of tool inputs before any execution (tool-runtime `registry`).
- **Permissions pipeline (orchestrator `policy`):** ordered deny→mode→allow→ask; **deny always wins unconditionally** regardless of allow rules or operating mode; human-in-the-loop `ApprovalRequest` for risk-tiered actions, persisted as events.
- **`bypass` mode is operator-only and constrained.** It is a **server-side, audited** setting that is (a) **forbidden when untrusted content is present or in multi-tenant mode**, (b) **never settable by the client request or by the model/hooks**, (c) emitted as a prominent audit event when active. Even under `bypass`, the **egress broker denial and tenant isolation remain non-bypassable** (they are infra controls, not policy). `bypass` collapses only the allow/deny/ask pipeline, never the infra controls — and only for an operator who explicitly enabled it outside the untrusted/multi-tenant guardrails.

---

## 9. Concurrency & Cancellation Model

### 9.1 Turn execution (one goroutine owns the loop)

Each active session's agent loop runs in **one orchestrator goroutine** that owns the turn lifecycle and the per-session in-memory state (folded from the event log). It is the single writer to that session's stream — which is *why* optimistic append rarely conflicts. `context.Context` threads through the entire turn (client `Run` deadline → loop ctx → each downstream gRPC call); cancelling the client stream, hitting a budget/turn cap, or an `Interrupt` cancels the loop ctx, which propagates.

### 9.2 Parallel read-only tool execution (bounded worker pool)

When one model response requests multiple tool calls, the orchestrator partitions them:

- **Read-only tools** (`glob`, `grep`, `read`, and any tool whose `ToolSpec.SideEffect == ReadOnly`) dispatch **concurrently** through a bounded pool (`errgroup` with `SetLimit(N)`; default `N = min(4, GOMAXPROCS)`, configurable). **Note:** `webfetch`/`websearch` are **not** in this set — they are `EgressClass = External` and subject to the egress/ask gate (§8.4), so they are scheduled through the policy path, not parallelized as harmless reads.
- **State-mutating tools** (`edit`, `write`, `bash`, any `SideEffect == Mutating`) are **serialized** in emitted order via a per-session mutation mutex — never concurrent with each other. Concurrent edits to one workspace are a correctness hazard and break deterministic replay.

Classification lives on `ToolSpec` (declared per tool, validated centrally); MCP tools default to `Mutating`/`External` unless explicitly annotated — fail safe.

### 9.3 Cancellation propagation — including a real sandbox kill

- **In-process:** the worker pool shares the loop's `context.Context`; on cancellation `errgroup` cancels the derived context and each worker abandons its RPC.
- **Across the boundary:** gRPC propagates context cancellation/deadline to the server; tool-runtime's `ExecuteTool` handler sees its ctx cancelled.
- **Into the sandbox (corrected):** the draft's "cancel the `docker exec` client" kills only the exec wrapper, not the in-container process tree — a detached/double-forked child, a SIGTERM-trapping process, or a fork bomb survives. v1 **kills at the container/cgroup boundary**: each tool exec runs in its own **PID namespace / cgroup**; on cancellation the signal goes to the **whole process group/cgroup** (or the per-session container is stopped) with a SIGTERM→SIGKILL deadline. Hard resource limits (CPU, memory, **PIDs/ulimit**, wall-clock) bound a non-cooperating process regardless of signal handling. **Adversarial tests are required:** a SIGTERM-trapping process, a double-forked detached child, and a fork bomb must each be terminated within the deadline.
- **Interrupt mid-turn:** `Control.Interrupt{session_id}` signals the loop goroutine via a per-session cancel function (delivered through the `ApprovalGate`/control port so it is testable without real gRPC); the loop appends a typed termination event and closes the stream cleanly. State is in the log, so an interrupted session is resumable.

### 9.4 Backpressure and relay stall (decoupled generation)

gRPC/HTTP-2 flow control bounds memory end to end. But coupling client delivery to provider generation is an availability/cost hazard: a slow client would otherwise stall the provider stream, holding a scarce provider rate-limit/concurrency slot and burning wall-clock/billing. Therefore:

- **A relay stall/idle deadline** (independent of the turn deadline) detaches a stalled client.
- **Generation is decoupled from delivery:** the loop generates → persists checkpoints/assembled message to the log → the client *tails the log* (via the resumable stream / `Reattach`). A slow client cannot backpressure the upstream provider into holding a slot.
- **Per-tenant in-flight generation caps** bound provider-concurrency exhaustion (§8.7).

### 9.5 Sub-agent concurrency (in-process, confirming the event-store demotion)

Sub-agents run as child loops in their **own goroutines** with their **own sessions** (forked or fresh), so their event appends never contend with the parent's stream; the parent blocks on the sub-agent tool call like any other tool. Because the event store is **in-process**, a parent turn that spawns sub-agents recursively writes events as **local pgx calls to PostgreSQL** — durable and resumable — with **no per-event network hop multiplied across recursion depth**. (Under the draft's event-store-as-service, the same recursion fanned out into N×per-event gRPC round-trips; the in-process design removes that amplification entirely. The correct sub-agent design is thus *confirming evidence* for the demotion.)

### 9.6 Single-writer lease (fenced, with takeover) — decided for v1

The draft both claimed and deferred the lease, and a session-scoped Postgres advisory lock has the wrong properties (connection-pool-scoped, no TTL, no fencing; a hung-but-alive owner keeps its lock; a connection blip silently frees it). v1 uses a **fenced lease with takeover**:

- A **lease row** on `sessions` (`lease_owner`, `lease_epoch`, `lease_expiry`) with an explicit **TTL** renewed by **heartbeat**, and a **monotonically increasing fencing token** (`lease_epoch`).
- **Every append carries the writer's `lease_epoch`**; the append transaction rejects a stale epoch (§6.3), so a fenced-out or stuck owner cannot append even if its `expected_seq` is current — the lease protects the **side-effect-gating log**, not just liveness.
- **Takeover:** after TTL expiry a new owner atomically bumps `lease_epoch` and becomes the writer; the old owner is fenced.
- **Re-drive path:** when the optimistic-append loser hits `FAILED_PRECONDITION`, the loop **reloads-and-rebases** if it still holds the lease, or **yields the session** if fenced — not merely "fails the turn".
- **Stuck-session detector:** a `projectord`/ops check flags sessions marked `active` with no event appended within X minutes (`last_event_at`), enabling recovery.
- Combined with §7.2's durable execution intent, a stolen/expired lease cannot cause **double side effects** (the unknown-outcome adjudication still applies).

---

## 10. Operability: Health, Startup, Metrics, Lifecycle

The draft had no health/readiness/startup/metrics story — a hard blocker for "operable by a small OSS team." This section adds it.

### 10.1 Health, readiness, liveness

- **gRPC health checking** (`grpc.health.v1.Health`) on every service, plus **HTTP `/livez` and `/readyz`** endpoints.
- **Readiness gates on actual dependency reachability**, not just process-up: orchestrator `/readyz` checks PostgreSQL ping + downstream gRPC health (model-gateway, tool-runtime) + SVID present; model-gateway checks SVID + (optionally) a provider reachability probe; tool-runtime checks SVID + container runtime availability; `projectord` checks PostgreSQL + reports projection lag.
- Crash-loop/restart behaviour is defined by the orchestrator's degradation policy (§10.3).

### 10.2 Startup ordering & migration gate

- **Migrations are a release gate:** `cmd/boltrope-migrate` runs to completion **before** any orchestrator instance accepts traffic. Event-store schema changes are **expand/contract, forward-only** (§6.1) so a rolling deploy is safe; additive-only DDL ordering is documented.
- **`docker-compose.yml`** declares ordering via `depends_on` + `healthcheck` (Postgres healthy → migrate completes → services start). For k8s: an init container / readiness gate runs migrate; services use readiness probes so traffic is not routed to not-yet-ready pods.
- **SPIRE bootstrap ordering** is specified: no service completes an mTLS handshake until the SPIRE agent has attested it and issued an SVID; readiness gates on SVID presence. The dev static-cert fallback (`BOLTROPE_DEV_INSECURE=1`, §8.1) gives laptops/CI a deterministic, SPIRE-free start.

### 10.3 Degradation policy & append resilience

- **Orchestrator degradation:** when a dependency is down, the orchestrator **fails the in-flight turn with a typed `error_during_execution`** rather than hanging, but only after a **bounded retry-with-backoff** budget within the turn deadline for transient `UNAVAILABLE`/`DEADLINE_EXCEEDED`. New `Run`s are rejected with a typed unavailable error while a hard dependency is down (`/readyz` already false).
- **Event-store (in-process) append resilience:** because the event log is the SSoT and every turn appends, a brief PostgreSQL blip must not be a fleet-wide outage. The append path uses **bounded retry-with-backoff on transient errors** (the append is idempotent via `request_id`, §6.3, so retry is safe), **PgBouncer/pool sizing guidance** so pool exhaustion does not masquerade as a hard failure, and **documented blast radius + target availability** for PostgreSQL with a redrive note for turns failed by a DB outage. (A local durable spool is considered but deferred — it complicates the SSoT model; bounded retry + pool sizing is the v1 stance.)

### 10.4 `projectord` lifecycle & safe-advance

- **Named, deployed owner:** projections run in `cmd/boltrope-projectord` (no longer orphaned). It is deployable and horizontally shardable by subscription name.
- **Safe-advance cursor (corrected):** the poller reads only **fully-settled** transactions (`transaction_id < pg_snapshot_xmin(pg_current_snapshot())`), orders by `(transaction_id, global_id)`, and advances the checkpoint to the last such row — **never past the snapshot xmin**. This closes the out-of-order-commit hole so cost-rollup/audit projections cannot silently drop events. LISTEN/NOTIFY is only a wakeup hint; on (re)connect the poller resumes from the stored `(last_transaction_id, last_global_id)` cursor to catch anything missed while disconnected.
- **Lag alerting:** `projectord` exposes a max-lag metric and alerts when a projection falls behind a threshold.

### 10.5 Metrics, SLOs, and stuck-loop detection

Tracing alone is insufficient for fleet operability. We add a metrics layer alongside OTel traces:

- **RED metrics per RPC** (request count, error rate broken down by **typed termination subtype**, duration) for `Run`, `Generate`, `ExecuteTool`, `Control`.
- **USE/saturation gauges** for the parts most likely to fail: errgroup worker-pool occupancy, **live sandbox count** (vs. cap), PostgreSQL connection-pool utilization, blob-store usage, projection lag.
- **Baseline SLOs and the handful of alerts** a small team needs: event-store/append error rate, sandbox count near cap, pool exhaustion, projection lag, stuck-session count.
- **Stuck-loop ("doom-loop") detection** is reinstated as an operational signal (repeated identical tool calls / no progress), not only the eventual max-turns/max-budget cap, so a stuck agent raises an alarm before it exhausts budget.

### 10.6 Sandbox lifecycle management

A `sandboxmgr` in tool-runtime prevents container/disk leaks:

- **Idle and absolute TTLs** per sandbox; a **max-live-sandboxes cap** per node with backpressure.
- **Reconciliation/GC loop** reaps containers whose session is `finished`/`failed`/abandoned (keyed off session status in PostgreSQL) and on orchestrator crash.
- Combined with the **clean-workspace resume** model (§7.5), an abandoned mid-turn container is safely reaped and a fresh one created on resume.

---

## 11. Provider Abstraction & Streaming Across Four Families

The normalized model and `Provider` interface are per ADR-0004 and the research sketch (`llm.Provider`, `Message`/`ContentPart`/`ToolCall`, `StopReason`, `Usage`, `StreamReader`). This section records the architecture-level corrections the review surfaced; all of them are contained **inside model-gateway** so the loop stays provider-agnostic.

### 11.1 Provider-opaque continuation state (replay/resume correctness)

Two of four providers require echoing opaque state back to make progress, which a "discard everything but the assembled message" log cannot do:

- Anthropic **server tools** return `stop_reason=pause_turn` with a `server_tool_use` block and no result; the protocol is "send the response back as-is to continue."
- Anthropic **extended thinking** returns `thinking` blocks with an opaque `signature` that must be returned unmodified or the request is rejected.
- OpenAI **Responses** uses server-stored Items / `previous_response_id` as conversation state.

Decisions:

- **`provider_raw` slot** on the `AssistantMessage` event (the `provider_raw` JSONB column, §6.2) carries the provider-native opaque blocks (raw content incl. `server_tool_use` + thinking signatures). The normalized `Message` is the **model-visible projection**; `provider_raw` is the source of truth for the *next provider call*, so a turn can be continued/replayed byte-faithfully. (Replay is deterministic over assembled messages **plus** this deliberately-preserved opaque slot.)
- **`Pause` is a non-terminal stream outcome** distinct from `Done` (§4.3, §11.3): the loop re-issues with the prior assistant content rather than treating the stream as final.
- **OpenAI Responses is pinned to STATELESS Item-passing** in the gateway (no reliance on server-side `store:true`/`previous_response_id` for the harness's own state) so self-host/replay portability is preserved. *(This is where we partially reject the review's suggestion to persist a Responses `response_id`: rather than store a provider-side handle that breaks portability and self-hosting, we carry the Items in `provider_raw` and replay statelessly. The opaque-continuation mechanism is adopted; a provider-side state handle is not.)*

Until `provider_raw` exists, the draft's "assembled-messages-only, deterministic replay" claim was false for 2/4 providers; it is now true.

### 11.2 Tool-call delta shape is provider-specific — normalize in the gateway

The draft modelled streamed tool calls 1:1 on OpenAI Chat Completions (flat integer `index` + a single concatenable `args_partial` string). That does not hold for: **Gemini** (path-addressed `partialArgs` fragments with `willContinue`, and only on some models), or **OpenAI Responses** (`response.function_call_arguments.delta/.done` keyed by `item_id`/`output_index`). Corrections:

- **All stream normalization (including tool-call accumulation) lives in model-gateway adapters**; the orchestrator relay is provider-agnostic and the draft's "accumulate by index" step is deleted from the loop (§4.3).
- `ToolCallDelta` is generalized: an **opaque per-call identifier (string, not int index)** plus a structured-fragment representation able to encode Gemini's `jsonPath` fragments and Responses' item-scoped deltas — **or** the gateway buffers internally and emits only complete tool calls on `Done`.
- A per-`(endpoint,model)` `SupportsStreamingToolCalls` gate makes the gateway **buffer the whole call** when streamed args are unsupported (e.g. Gemini 3.1 Flash-Lite, LM Studio) rather than assuming concatenation works.
- The draft's "matches the provider SSE shape 1:1" claim is **struck**; the gateway has ≥3 stream normalizers (Anthropic SSE, OpenAI Chat-Completions SSE, OpenAI Responses typed events) plus the Gemini iterator, all feeding the one `StreamEvent` `oneof`.

### 11.3 Stop reasons: open enum + explicit Pause/Continue

The normalized stop set is **open**, not closed: a normalized enum for known terminal reasons **plus** a raw string + `StopOther` variant (per ADR-0004), and **`Pause` is a separate non-terminal Continue signal** distinct from `Done`. The loop branches on **three** outcomes — *final* / *needs-tool-execution* / *needs-continuation(pause)* — not two. `Refusal` and `ContextWindowExceeded` are first-class normalized reasons (a refusal → fallback-model policy; context-exceeded → compact-and-retry), and a refusal maps to a distinct termination subtype, **not** `error_during_execution`.

### 11.4 Capabilities are per-`(endpoint, model)`

Capability variability is per-**model** within one endpoint (one Anthropic key serves models with different thinking/vision/context; one Gemini endpoint serves Gemini 3 Pro *with* streamed tool args and 3.1 Flash-Lite *without*; one OpenAI Responses endpoint serves models with different parallel-tool-call/strict-schema support). Therefore **`CapabilitiesRequest` carries the model id** and the gateway returns model-specific flags (a static table keyed by model, overridable per endpoint for self-hosted). Flags include `SupportsStreamingToolCalls`, `SupportsThinking`, `SupportsParallelToolCalls`, `SupportsVision`, `SupportsTokenCounting`, `MaxOutputTokens`. For OpenAI-compatible self-hosted endpoints with an unknown model set, a startup probe is deferred (§14) but the loop never default-assumes Chat-Completions-shaped capabilities.

### 11.5 OpenAI-compatible path targets Chat Completions, not Responses

The native OpenAI adapter defaults to **Responses**, but self-hosted servers (vLLM/Ollama/LM Studio/llama.cpp/TGI) implement **Chat Completions**. So `openaicompat` shares the **Chat-Completions normalizer**, not the Responses one (§5.2). It is documented that `openaicompat` targets Chat Completions only, and that LM Studio specifically disables streaming + parallel tool calls in its per-endpoint capabilities.

### 11.6 Usage/cost is read from the provider's authoritative field, computed in the gateway

Usage semantics differ: Anthropic streams **cumulative** usage under `message_delta` (with `cache_read`/`cache_write` split); Gemini reports `usageMetadata` per chunk; OpenAI Responses bills the full chained context as input each turn. The gateway takes `Usage` from the **authoritative** field per surface (final cumulative `message_delta` usage / last-chunk `usageMetadata` / `response.completed` usage), normalizes to `{input, output, cache_read, cache_write}`, and **computes cost in the gateway** (which holds model pricing) — not recomputed from token counts in the orchestrator. `CountTokens` is capability-gated and never used for billing on Anthropic/Gemini. The Responses chained-input billing is documented so cost-per-turn is interpreted correctly.

---

## 12. Repository Layout

### 12.1 Decision: single Go module, monorepo, official go.dev layout (no `pkg/`)

- **Single module** (`module github.com/boltrope/boltrope`). Services that share the normalized `llm` types, the generated protobuf stubs, and the platform bootstrap are simplest to version and refactor atomically in one module. Multi-module adds `go.work`/replace friction with no benefit at this size.
- **Follows `go.dev/doc/modules/layout`:** `cmd/<app>/main.go` entrypoints + `internal/` for everything private. **No `pkg/`/`api/` scaffolding** (the research is explicit `golang-standards/project-layout` is not official and `pkg/` is not universally accepted).
- **Generated protobuf code is committed** so `go build`/`go test` work without a codegen step; regenerated by `buf generate` (`make proto`) and checked in CI.

### 12.2 Concrete top-level tree

```
boltrope/
├── go.mod                      # single module: github.com/boltrope/boltrope
├── go.sum
├── buf.yaml  buf.gen.yaml      # protobuf lint + codegen config
├── Makefile                    # proto, lint, test, test-integration, build
├── LICENSE                     # Apache-2.0   |   NOTICE
├── README.md  CONTRIBUTING.md  CODE_OF_CONDUCT.md  SECURITY.md
├── .golangci.yml               # v2: linters.default: standard + curated enable + depguard/forbidigo rules
├── docker-compose.yml          # local dev: postgres (+healthcheck), migrate (gate), spire(optional),
│                               #   the 3 services + projectord; depends_on ordering
│
├── cmd/                        # entrypoints ONLY (wiring), one per deployable
│   ├── boltrope-orchestratord/main.go   # includes the event-store package wiring
│   ├── boltrope-modelgwd/main.go
│   ├── boltrope-toolruntimed/main.go
│   ├── boltrope-projectord/main.go      # read-side projection worker
│   ├── boltrope-ctl/main.go             # client CLI/SDK (not a server)
│   └── boltrope-migrate/main.go         # runs golang-migrate, exits
│
├── proto/                      # protobuf IDL (the contracts) — NO event_store.proto (in-process)
│   └── boltrope/v1/
│       ├── common.proto        # Message, ContentPart, ToolCall, Usage, StopReason, ...
│       ├── orchestrator.proto  # Run (resumable server stream), Control (unary: approve/deny/interrupt/reattach)
│       ├── model_gateway.proto # Generate (server stream), CountTokens, Capabilities(model)
│       └── tool_runtime.proto  # ExecuteTool (server stream), ListTools
│
├── gen/                        # GENERATED Go from proto (committed)
│   └── boltrope/v1/            # *.pb.go, *_grpc.pb.go, gateway (grpc-gateway)
│
├── internal/                   # all private code
│   ├── orchestrator/           # §5.1 (domain/ app/ adapter/ infra/) — INCLUDES adapter/outbound/eventstore
│   ├── modelgateway/           # §5.2
│   ├── toolruntime/            # §5.3
│   ├── projector/              # §5.4
│   └── platform/               # genuinely cross-service shared code
│       ├── grpcx/              # mTLS (SPIFFE), interceptors (auth, otel, recovery, logging, RBAC)
│       ├── obs/                # OTel bootstrap + RED/USE metrics + slog JSONHandler + LogValuer helpers
│       ├── config/             # koanf loader (flags>env>file>defaults, validate-fail-fast)
│       └── llm/                # the normalized message/tool/stop-reason model + Provider interface
│                               #   PURE: zero infra deps, NO imports of gen/ or any SDK (depguard-enforced)
│
├── migrations/                 # *.sql (golang-migrate), embedded by internal/orchestrator; expand/contract
│
├── test/
│   ├── integration/            # //go:build integration — testcontainers (Postgres) E2E
│   └── eval/                   # bespoke deterministic eval harness (ADR-0007): scripted fake Provider + fake clock
│
└── docs/
    ├── research/00-research-report.md
    ├── decisions/              # ADRs 0001..00NN
    └── architecture/00-architecture.md  01-impact-analysis.md
```

### 12.3 The message model has ONE source of truth (no triple representation)

The draft maintained the normalized message/tool/stop-reason union in **three** places (in-process `platform/llm`, protobuf wire types, and a "mirrored" copy in the orchestrator `domain`). That triples the drift surface of the exact thing the multi-LLM abstraction exists to keep consistent. Resolution:

- **`internal/platform/llm` is a pure, dependency-free domain-shared kernel** (no protobuf, no SDK imports). Each service's `domain`/`app` imports it directly; the orchestrator `domain` does **not** re-declare or "mirror" the types.
- **Generated protobuf types live in `gen/` and are strictly separate.** The gateway and orchestrator adapters map `gen/` ⇄ `llm` at the transport edge only.
- A **`depguard` rule asserts `platform/llm` imports nothing from `gen/` or any SDK**, so the "pure contract" claim is machine-enforced.
- The model-gateway↔orchestrator boundary is gRPC (defensible: distinct dependency surface), so protobuf exists for the wire — but `llm` is the in-process source of truth and the proto is generated to match it; we do **not** also keep a hand-mirrored domain copy. This collapses three representations to two with a tested mapping at one seam, instead of three drifting by hand.

### 12.4 Per-service internal boundary

Each service's code is under `internal/<service>/...`. Go's `internal/` rule prevents external import; a `depguard` rule additionally forbids one service importing another service's `domain`/`app` — services communicate only via the generated gRPC stubs in `gen/` (and the event store is reached only through `EventLogPort` within the orchestrator). This makes the service boundary real, not aspirational.

---

## 13. Decisions (with rationale & rejected alternatives)

### D1 — Service decomposition: 3 services (orchestrator [+ in-process event store], model-gateway, tool-runtime) + projectord worker

- **Rationale:** model-gateway (distinct vendor-SDK dependency surface) and tool-runtime (distinct trust boundary for model-influenced code) earn service lines under a stress-test; the orchestrator is the cohesive brain. The event store is a CRUD-over-Postgres concern with no distinct trust/scaling/dependency profile that an in-process package against a separate PostgreSQL process cannot satisfy — so it is an in-process package, removing a gRPC service and a per-append network hop from the hot path while losing no resume/fork/replay capability. `projectord` is a real read-side worker, off the request path.
- **Rejected — event-store as a separate service (the draft):** a distributed-monolith nano-service on the highest-frequency operation; its justifications are PostgreSQL's properties, not a Go shim's. The `EventLogPort` interface makes later extraction non-breaking if a concrete need arises.
- **Rejected — single binary (modular monolith):** violates the microservices mandate and the code-execution trust-boundary goal; tool-runtime must be isolatable.
- **Rejected — 7–9 services (one per subsystem):** nano-service sprawl on the hot path for zero isolation benefit.
- **Rejected — folding tool-runtime into orchestrator:** puts model-influenced execution in the brain's trust domain — the opposite of trifecta containment.

### D2 — Inter-service comms: synchronous gRPC + protobuf; server-streaming; resumable client edge; no broker on the request path

- **Rationale:** the loop is request/response-with-streaming; gRPC gives typed contracts, HTTP/2 streaming with flow control + cancellation (ideal for token deltas), and deadline propagation. Durability/async lives in the event log. The client `Run` stream is resumable (Last-Event-ID/seq + `Reattach`) so client disconnects do not lose generation. Event-store is in-process, so there is no `event_store.proto`.
- **Rejected — REST/JSON internally; message broker on the request path; bidi for `Run`** — as in the draft, for the same reasons (weaker streaming/contract story; at-least-once redelivery fights ordered deltas; bidi over-engineered and proxy-hostile). A broker remains a *later* option for read-side fan-out off the log into `projectord`.

### D3 — Event-store schema: single `events` table; per-session `seq` (DB-enforced contiguity) + optimistic version + fencing token + per-append `request_id`; dual `(xid8, global_id)` cursor read xmin-bounded; tenant-scoped blobs; concrete RLS; Postgres ≥13

- **Rationale:** the `(session_id, seq)` unique constraint + head-transition `RETURNING` give gap-free in-stream ordering and optimistic concurrency in one transaction; `request_id` gives true idempotency (lost-ACK retry returns success, not a spurious conflict); `lease_epoch` fences stuck/stolen writers; the xmin-bounded `(xid8, global_id)` cursor gives projections a genuinely gap-safe global feed (the naive cursor skips out-of-order commits); tenant-scoped `(tenant_id, ref)` blob identity forbids cross-tenant dedup; concrete RLS (non-owner role + `SET LOCAL` GUC + `FORCE RLS`) makes the backstop real. Postgres ≥13 is pinned for `xid8`.
- **Rejected — gRPC shim in front (D1); gap-free global sequence (contended global lock); raw token-delta storage (non-deterministic, bloat); always-inline large outputs (WAL bloat); global content-addressed blobs (cross-tenant oracle); advisory-lock-only lease (no fencing/TTL); destructive `down` migrations on the log.**

### D4 — Durability & exactly-once: durable turn/tool-execution intent in the log; at-most-once mutating tools with explicit unknown outcome; resumable client stream; clean-workspace resume

- **Rationale:** `TurnStarted` + periodic `AssistantMessageDelta` checkpoints + `ToolExecutionStarted`-before-dispatch (log-derived idempotency key) + a durable `tool_executions` ledger make mid-turn crashes recoverable and prevent double execution of `bash`/`git push`/patches. Recovery adjudicates open turns/executions explicitly; mutating tools default to at-most-once with an `unknown` terminal state, never blind re-run. Workspace resume is explicitly clean-workspace (uncommitted FS state lost) because `bash` side effects are not replayable.
- **Rejected — assembled-messages-only with no in-flight durability (the draft):** lost the longest, most expensive, most user-visible path and silently re-billed/re-ran. **Rejected — in-memory TTL dedup + declarative gRPC retries for mutating tools:** wiped by the crash that triggers the retry; gRPC won't safely retry a streamed RPC anyway.

### D5 — Security: SPIFFE/SPIRE mTLS (fail-closed dev fallback); RPC-bound short-lived tenant token; concrete RLS; egress broker on EVERY model-influenced channel + taint gate; MCP servers confined; provider-native tools disabled; masking is defense-in-depth only; constrained `bypass`

- **Rationale:** the trifecta is severed on the leg an injection controls — **external communication** — in v1 by the per-session sandbox network namespace (`--network none` by default) — the actual containment for all model-influenced tools, including in-sandbox `bash` — with the deny-by-default per-session allowlist broker as the egress *policy* layer plus a taint gate over it, not a broker that individually routes each connection today (a future egress-proxy data path turns the allowlist into live, gated egress; §8.4 roadmap). `webfetch`/`websearch` are reclassified as external comms (not silent reads). MCP servers run confined (tool-poisoning + supply-chain), with approval-on-first-use and identity pinning, and never see the SVID. Provider-native tools are disabled in v1 so all tools obey §8's controls. Concrete RLS + RPC-bound tenant token enforce row-level isolation. Masking is best-effort hygiene, never a containment boundary. `bypass` is operator-only, audited, forbidden under untrusted/multi-tenant, and cannot disable infra controls.
- **Multi-tenant honesty:** v1 shared-kernel containers are safe only for single-tenant/trusted-code; multi-tenant-untrusted-code requires the deferred microVM/gVisor runtime. The RLS data model is future-proofing, stated as such.
- **Rejected — the draft's "breaking any one leg defeats the attack" claim (false as drawn); "arbitrary egress" for tool-runtime; masking-as-containment; client-settable/unscoped `bypass`; RLS-without-a-set-tenant-context; global blob refs as capabilities; static-cert downgrade with no guard.**
- **Grounding:** SPIFFE/SPIRE concepts; go-spiffe; lethal-trifecta threat model (research Risk #1, #2).

### D6 — Concurrency: single goroutine per session loop; bounded errgroup pool for read-only tools (web tools excluded, gated); serialized mutations; cgroup/PID-namespace kill; fenced lease with takeover; decoupled generation/delivery

- **Rationale:** a single owning goroutine keeps the loop simple and append conflict-free in the common case; `errgroup.SetLimit` is the idiomatic bounded pool; serializing mutations preserves workspace correctness/replay; killing at the cgroup/PID-namespace boundary (with hard CPU/mem/PID/wall-clock limits) actually stops a runaway `bash` (the exec-wrapper cancel does not); a fenced lease (TTL + `lease_epoch` checked on every append) gives real takeover and prevents a stuck/stolen writer from causing double side effects; decoupling delivery from generation prevents a slow client from holding a provider concurrency slot.
- **Rejected — unbounded fan-out; parallel mutations; cooperative-only/exec-wrapper cancellation; advisory-lock-only lease; coupling client delivery to provider generation.**

### D7 — Repo: single Go module, monorepo, go.dev layout, committed generated protos, shared `internal/platform`; ONE message-model source of truth

- **Rationale:** atomic refactors + a shared `llm` contract; official layout avoids the disputed `pkg/`; committed `gen/` keeps `go build` codegen-free; `depguard`/`forbidigo` make the service boundary and the determinism rule (no direct `time.Now`/`rand`/`uuid.New`) enforceable; `platform/llm` is the single in-process source of truth for the normalized model (no hand-mirrored domain copy, no triple representation), with a `depguard` rule keeping it free of `gen/`/SDK imports.
- **Rejected — multi-module monorepo; polyrepo; `pkg/` scaffolding; maintaining proto + platform/llm + a mirrored domain copy of the same union.**

### D8 — OpenAI surface, providers, capability flags, provider-opaque continuation

- **Decision:** OpenAI adapter defaults to **Responses** (Chat Completions behind a sub-flag, declared in the root `openai` package); self-hosted + LiteLLM via the OpenAI-compatible adapter sharing the **Chat-Completions** normalizer; native adapters for Anthropic/Gemini/OpenAI over the official Go SDKs; **capability flags per `(endpoint, model)`** (model id in `CapabilitiesRequest`); one harness-level retry keyed on `ProviderError.Kind` over the SDKs' backoff; **stop-reason set is open** (`StopOther` + raw string) with a **non-terminal `Pause`**; **`provider_raw` opaque-continuation slot** carries Anthropic `server_tool_use`/thinking signatures and Responses Items; **Responses pinned to stateless Item-passing** (no reliance on server-side state) for portability; usage/cost read from the authoritative provider field and computed in the gateway; provider-native server-side tools **disabled** in v1.
- **Rejected — Chat Completions as primary; per-*provider* (not per-endpoint/model) capability flags; "1:1 with provider SSE" (struck — ≥3 normalizers); assembled-messages-only with no opaque continuation (broke 2/4 providers); persisting a provider-side Responses `response_id` as harness state (breaks self-host/replay portability — we carry Items in `provider_raw` and replay statelessly instead).**

### D9 — Observability & operability: OTel GenAI traces + RED/USE metrics + SLOs; health/readiness; startup/migration gate; stuck-loop detection

- **Decision & mapping:** orchestrator emits an `invoke_agent` span per `Run` (and per sub-agent); model-gateway a `chat` span per `Generate` (`gen_ai.*` attrs incl. usage + finish reason); tool-runtime an `execute_tool` span per `ExecuteTool`. Trace context propagates over gRPC (`otelgrpc`); `trace_id`/`span_id` correlate into slog. **Alongside traces:** RED metrics per RPC (errors by typed termination subtype), USE gauges (worker pool, live sandbox count, DB pool, blob usage, projection lag), baseline SLOs, and the alert set in §10.5; **stuck-loop detection** as an operational signal. gRPC health + HTTP `/livez`/`/readyz` (readiness gates on dependency reachability + SVID). Migrations are a release gate; `docker-compose` declares `depends_on`/`healthcheck` ordering; SPIRE bootstrap ordering specified with a fail-closed dev fallback. Token + cost on `TurnFinished`/`TurnAborted` events.
- **Rationale:** tracing alone cannot tell a small team the fleet is degrading, sandboxes are leaking, or the pool is exhausted; metrics/SLOs/health/startup are required to be operable. Logs stay slog-JSON (OTel logs still beta).
- **Rejected — tracing-only observability with no metrics/health/startup story (the draft); dropping stuck-loop detection.**
- **Grounding:** OTel GenAI agent/chat/execute_tool spans; OTel-Go traces/metrics stable, logs beta. OTel GenAI semantic conventions are status 'Development' (unstable) as of 2026; attribute and span names are pinned at adoption and documented here. Span names follow the convention `{gen_ai.operation.name} {model}` (e.g. `"chat <model-id>"`); `invoke_agent` and `execute_tool` spans append the agent or tool name respectively — i.e. `"chat"` is the `gen_ai.operation.name` value, not the literal full span name.

---

## 14. Open Questions Deferred Past This Gate

These do not block contract/TDD work and are explicitly scheduled later:

- **Durable workspace snapshots:** the consistent-FS-on-resume backend behind the Workspace port (container/volume snapshot referenced by an event), beyond v1's clean-workspace resume (§7.5). Tied to the microVM/gVisor runtime.
- **Local durable append spool:** whether the orchestrator should spool events locally to ride out a longer PostgreSQL outage (vs. v1's bounded retry + pool sizing, §10.3) — reconsider if measured DB availability requires it.
- **Snapshot cadence:** when to write `session_snapshots` (every K events? on compaction boundary?) — tune after measuring replay cost.
- **Blob store backend:** filesystem (single-node dev) vs. S3-compatible (multi-node), both behind `BlobStorePort`; pick the default in the impl gate.
- **grpc-gateway REST facade scope:** which RPCs get a REST mapping in v1 (at minimum `Run` via SSE + `Control`), with identical auth.
- **Per-endpoint capability discovery:** static config vs. a startup probe for OpenAI-compatible endpoints with unknown model sets (§11.4).
- **Eval target beyond v1:** SWE-bench-Lite as an external nightly benchmark (the v1 deterministic suite is settled in ADR-0007); a live smoke tier gated by API keys is in scope, a live model as a per-PR gate is not.
- **`projectord` sharding/HA:** multi-instance ownership per subscription if projection throughput demands it.
- **Deferred per constraints (not v1):** MCP SERVER mode, native-Ollama NDJSON adapter, model routing, microVM/OS-sandbox runtimes (behind `RuntimePort`), advanced multi-agent topologies, constrained decoding, tree-sitter repo map, provider-native server-side tools (§8.12).

---

*End of architecture. Next: derive `proto/boltrope/v1/*.proto` and the consumer-defined Go port interfaces from §4–5 (note: no `event_store.proto`); write failing tests for the agent loop (mock `ModelPort`/`ToolPort`/`EventLogPort`/`HookRunner`/`ApprovalGate`, fake `Clock`/`IDGenerator`), the pure stream `assembler` (fake `StreamReader` with adversarial deltas), the optimistic+fenced+idempotent append and the RLS predicate-removed test (testcontainers), the durable tool-execution recovery path, the read-only/mutating scheduler, and the adversarial sandbox-kill tests (integration).*
