# Boltrope — System Specification v1

> **Status:** Gate 2 — accepted.
> **Date:** 2026-06-10
> **Project:** `github.com/xd1lab/harness-ai`
> **Tagline:** _A provider-portable, event-sourced AI agent harness in Go._
> **Inputs:** `docs/research/00-research-report.md`; ADRs 0001–0008; `docs/architecture/00-architecture.md`; `docs/architecture/01-impact-analysis.md`.
> **Audience:** Engineers writing contracts, TDD plans, and the implementation gating review.

---

## Table of Contents

1. [Purpose & Vision](#1-purpose--vision)
2. [Scope](#2-scope)
3. [Personas & Primary Use Cases](#3-personas--primary-use-cases)
4. [Functional Requirements](#4-functional-requirements)
5. [Non-Functional Requirements](#5-non-functional-requirements)
6. [Multi-LLM Support Matrix](#6-multi-llm-support-matrix)
7. [External Interfaces](#7-external-interfaces)
8. [Data & State](#8-data--state)
9. [Success Criteria / Definition of Done for v1](#9-success-criteria--definition-of-done-for-v1)
10. [Glossary](#10-glossary)
11. [Traceability Note](#11-traceability-note)

---

## 1. Purpose & Vision

**Boltrope** is an open-source, backend-only, provider-portable AI agent harness written in Go and backed by PostgreSQL. It turns a stateless LLM completion API into a stateful, tool-using, self-correcting agent that any developer can run against any supported hosted or self-hosted model.

The single source of truth is an append-only, event-sourced log in PostgreSQL. Every externally-observable capability — session resume, fork, replay, cost accounting, and observability — derives from that log. The design enforces the principle that **context is a finite resource**, managing it actively through token accounting, automatic compaction, and prompt caching.

The system is intentionally backend-only for v1: no frontend, no proprietary cloud dependency, no vendor lock-in. The Workspace/Runtime abstraction, the `Provider` interface, and the `EventLogPort` are shaped so post-v1 features (microVM isolation, model routing, MCP server mode) slot in without re-architecture.

---

## 2. Scope

### 2.1 v1 In-Scope (per ADR-0003)

**MUST (irreducible harness):**

- Single-threaded gather→act→verify agent loop with typed termination subtypes.
- Provider-portable model interface: streaming, capability flags, model-id pinning.
- Tool registry: JSON-Schema validation before execution, Action→Execute→Observation contract, core tools (Read, Edit, Write, Glob, Grep, Bash, web fetch/search).
- Context token accounting + automatic compaction + prompt caching of stable prefixes.
- Session persistence as an append-only event-sourced log with resume (PostgreSQL).
- Layered permissions with default/acceptEdits/plan/bypass modes and a human-in-the-loop approval callback.
- Sandboxed execution behind a `Workspace`/`Runtime` port (container MVP, deny-by-default egress).
- Reliability primitives: streaming, cooperative cancellation, retries honoring `Retry-After`→backoff+jitter (retry only 429/5xx/529).
- Token and cost accounting on every result; structured logging (`slog` JSON).

**SHOULD (selected for v1):**

- Tool-result clearing; session fork and replay; depth-limited sub-agents as ordinary tools.
- MCP client with lazy schema loading (stdio and HTTP).
- Hooks/middleware pipeline: `PreToolUse`, `PostToolUse`, `Stop`, `PreCompact`.
- OpenTelemetry GenAI tracing + RED/USE metrics; health and readiness endpoints.
- Bespoke deterministic eval harness wired to CI (ADR-0007).
- Parallel read-only tool execution (bounded goroutine pool).
- Best-effort secret-registry output masking (defense-in-depth / log hygiene).

### 2.2 Explicit Non-Goals for v1 (per ADR-0003 and `01-impact-analysis.md` §9)

- A separate event-store service (the event store is an in-process package inside the orchestrator).
- Durable workspace snapshots / consistent filesystem resume after crash.
- microVM, gVisor, or OS-native sandbox backends; therefore multi-tenant execution of mutually-untrusted code.
- Provider-native/server-side tools (Anthropic `web_search`, OpenAI Responses built-ins) — disabled in v1.
- Local durable append spool (bounded retry + pool sizing is the v1 stance).
- MCP server mode and A2A interoperability.
- Native-Ollama NDJSON adapter (the OpenAI-compatible `/v1/chat/completions` path is used instead).
- Model routing, advanced multi-agent topologies, non-native function-calling fallback.
- Semantic codebase indexing / tree-sitter repo map.
- LLM-based risk classifier.
- Virtual-filesystem context mounts; interactive workspace access (VNC/embedded editor).
- SWE-bench or SWE-bench-Lite as a CI gate (deterministic bespoke suite is the v1 gate).
- A REST mapping for every RPC (v1 REST facade covers at minimum `Run` via SSE + `Control`, with identical auth).
- A frontend of any kind.

---

## 3. Personas & Primary Use Cases

### 3.1 Personas

| ID | Persona | Description |
|---|---|---|
| P-SELFHOST | Self-hoster / platform engineer | Deploys Boltrope on their own infrastructure (bare metal, k8s, or `docker compose`). Cares about ops simplicity, PostgreSQL as the single datastore, SPIFFE/SPIRE identity, single-binary binaries, and a clear upgrade path. Connects their own LLM credentials. |
| P-INTEGRATOR | Tool / provider / MCP integrator | Extends the harness with custom tools, new LLM provider adapters, or MCP servers. Works primarily against the `Provider` interface, the `ToolSpec` registry, and the MCP client port. Needs testable seams and stable contracts. |
| P-BUILDER | Agent-app builder | Drives sessions programmatically via the gRPC API (or REST facade) from a CI pipeline, IDE plugin, or application SDK. Cares about the `Run` server-stream, `Control` unary RPC, resumable sessions (Last-Event-ID), and structured results. |

### 3.2 Primary Use Cases

| ID | Actor | Use Case | Notes |
|---|---|---|---|
| UC-RUN-01 | P-BUILDER | Submit a new agent session with a task description and stream the event frames until completion. | Uses `OrchestratorService.Run`; client receives `TextDelta`, `ToolProgress`, `ApprovalRequest`, and terminal `Result`. |
| UC-RES-01 | P-BUILDER | Reconnect to a session after a network drop and resume from the last received event. | `Control.Reattach{session_id, from_seq}` replays missed frames using Last-Event-ID semantics. |
| UC-APR-01 | P-BUILDER | Approve or deny a tool-execution request that requires human confirmation. | `Control` unary RPC with `Approve`/`Deny`; the decision is persisted as an event. |
| UC-INT-01 | P-BUILDER | Interrupt a running session and leave it in a resumable state. | `Control.Interrupt{session_id}`; the loop appends a typed termination event and the session is later resumable. |
| UC-FORK-01 | P-BUILDER | Fork an existing session at a given event sequence to explore an alternative trajectory. | `OrchestratorService.Fork`; creates a new child session branching from `at_seq`. The parent is unaffected. |
| UC-TOOL-01 | P-INTEGRATOR | Register a custom tool with a JSON Schema and map it to an execution function. | Tool-runtime registry; tool inputs are validated before execution; errors surface as `Observation` not panics. |
| UC-MCP-01 | P-INTEGRATOR | Connect an MCP server (stdio or HTTP) and make its tools available to the agent loop. | MCP client with lazy schema loading; each MCP server runs in a confined sandbox; approval required on first registration. |
| UC-PROV-01 | P-INTEGRATOR | Add support for a new LLM provider or swap in a self-hosted OpenAI-compatible endpoint. | Implement `Provider` interface or configure `openaicompat` adapter with a base URL; per-endpoint capability flags govern behavior. |
| UC-OPS-01 | P-SELFHOST | Deploy the full stack via `docker compose up` and confirm sessions complete end-to-end. | Includes PostgreSQL, migration gate, three services, and `projectord`. |
| UC-OPS-02 | P-SELFHOST | Monitor session cost, latency, and tool-execution health via OpenTelemetry. | OTel GenAI spans + RED/USE metrics exported to the operator's collector. |

---

## 4. Functional Requirements

Requirement IDs use the prefix groups below. Each requirement states a MUST or SHOULD obligation plus 1–3 testable acceptance criteria (AC). AC items seed the TDD effort directly.

> **Coding conventions for AC:** "GIVEN / WHEN / THEN" format is implied; AC items are expressed as checkable assertions on observable system state (event log contents, gRPC response codes, metric values, HTTP status codes).

---

### FR-LOOP — Agent Loop

**FR-LOOP-01** — The orchestrator MUST implement a gather→act→verify ReAct-style agent loop. On each turn it builds a context window from the session event log, calls the model, dispatches any requested tool calls, feeds results back, and repeats until a terminal condition is reached.

- AC-1: A session started with a user message and a fake `ModelPort` that emits one tool call followed by a text-only response terminates with `subtype=success` and appends exactly `[UserMessage, TurnStarted, AssistantMessage(tool_call), ToolExecutionStarted, ToolResult, AssistantMessage(text), TurnFinished]` to the event log (golden log shape assertion).
- AC-2: A fake `ModelPort` that never emits a text-only response causes the loop to terminate with `subtype=error_max_turns` when the configured `max_turns` cap is reached; the final `TurnFinished` event carries `subtype=error_max_turns`.

**FR-LOOP-02** — The orchestrator MUST enforce typed termination subtypes: `success`, `error_max_turns`, `error_max_budget_usd`, `error_during_execution`, `error_max_structured_output_retries`. Every `TurnFinished` event MUST carry the subtype and the final `usage` + `cost_usd`.

- AC-1: When `accumulated_cost_usd` exceeds `max_budget_usd`, the loop terminates with `subtype=error_max_budget_usd` before the next `Generate` call.
- AC-2: When a `Generate` call returns a `ProviderError{Kind: Server}` that exhausts the bounded retry budget, the loop terminates with `subtype=error_during_execution`.
- AC-3: Each termination subtype has a distinct counter in the RED metrics (`run_errors_total{subtype=...}`); a loop unit test asserts the correct subtype label is incremented.

**FR-LOOP-03** — The orchestrator MUST support cooperative cancellation via `Control.Interrupt{session_id}`. Interrupting a running session MUST: (a) cancel the in-progress turn, (b) append a typed termination event (`TurnAborted` or `TurnFinished{subtype=error_during_execution}`), (c) leave the session in a state from which it can be resumed.

- AC-1: An integration test sends `Interrupt` while the fake `ModelPort` is mid-stream; the loop goroutine terminates, and the event log contains a `TurnAborted` event with non-zero `usage_so_far` from the partial generation.
- AC-2: The interrupted session passes the `recovery` package's open-turn adjudication and a subsequent `Run` on the same session ID succeeds.

**FR-LOOP-04** — The orchestrator MUST enforce a bounded worker pool for parallel read-only tool execution and serialize all state-mutating tool calls within a turn.

- AC-1: A turn with three `ReadOnly`-classified tool calls dispatches all three concurrently (asserted via the fake `ToolPort` recording parallel call timestamps); their results are fed back to the model in a single subsequent request.
- AC-2: A turn with two `Mutating`-classified tool calls dispatches them sequentially (second call begins only after the first `ToolResult` is appended); the loop unit test asserts dispatch order matches emission order.
- AC-3: `webfetch` and `websearch` are NOT dispatched in the read-only parallel pool; they are subject to the policy/egress gate (AC verified by the fake `PolicyEngine` receiving a gate check before dispatch).

**FR-LOOP-05** — The orchestrator MUST record a `TurnStarted{turn_id}` event before every `Generate` call and periodic `AssistantMessageDelta{turn_id, text_so_far}` checkpoints during streaming; on recovery an open turn MUST be adjudicated explicitly (continue from cursor, or mark `TurnAborted`) — never silently re-billed.

- AC-1: A fake `ModelPort` that returns an error mid-stream causes the recovery package to detect an open `TurnStarted` and produce either a continuation or a `TurnAborted{usage_so_far}` event; the recovered session's cost total equals the partial `usage_so_far`, not zero.
- AC-2: A test that truncates the event log after `TurnStarted` and before `AssistantMessage` then starts a fresh orchestrator instance results in the recovery package calling the `TurnAbortedCallback`, not silently replaying the generation.

---

### FR-MODEL — Model Interface

**FR-MODEL-01** — The model-gateway MUST implement the normalized `Provider` interface (`Generate`, `Stream`, `CountTokens`, `Capabilities`) for Anthropic, Google Gemini, OpenAI (Responses primary, Chat Completions sub-flag), and an OpenAI-compatible adapter covering vLLM, Ollama, LM Studio, llama.cpp, TGI, and LiteLLM.

- AC-1: A unit test drives each of the four adapters with a recorded HTTP fixture; the emitted `llm.Response` has the correct `StopReason`, `Usage`, and `Content` shape regardless of provider wire format.
- AC-2: The OpenAI-compatible adapter configured with `base_url=http://localhost:11434/v1` and a placeholder key produces a valid `llm.Request` wire format that matches the Chat Completions schema (verified against a recorded Ollama fixture).

**FR-MODEL-02** — The model-gateway MUST normalize provider-specific streaming shapes into one internal `StreamEvent` channel (`TextDelta`, `ThinkingDelta`, `ToolCallDelta`, `Pause`, `Done`). All stream normalization and tool-call delta accumulation MUST reside in the gateway, not in the orchestrator relay.

- AC-1: A stream normalizer unit test drives a sequence of Anthropic SSE events (including a split-UTF-8 `text_delta`, an `input_json_delta` fragment, and a terminal `message_delta`) and asserts the emitted `[]StreamEvent` sequence matches the expected golden output.
- AC-2: A stream normalizer unit test for OpenAI Responses drives `response.function_call_arguments.delta` events keyed by `item_id` and asserts a complete, parsed `ToolCallPart` is emitted on `Done`.
- AC-3: The orchestrator's `assembler.go` has zero imports of `gen/` or any provider SDK (enforced by `depguard`).

**FR-MODEL-03** — The model-gateway MUST resolve capability flags per `(endpoint, model)`, not per provider family alone. The `Capabilities` RPC MUST carry the model ID in the request and return model-specific flags including `SupportsStreamingToolCalls`, `SupportsParallelToolCalls`, `SupportsThinking`, `SupportsVision`, `SupportsTokenCounting`, `MaxOutputTokens`.

- AC-1: A capabilities unit test asserts that requesting capabilities for `(anthropic, claude-3-5-sonnet-20241022)` and `(anthropic, claude-3-haiku-20240307)` returns different `MaxOutputTokens` values.
- AC-2: When `SupportsStreamingToolCalls` is false for an endpoint/model, the gateway buffers partial tool-call deltas and emits only complete calls on `Done` (asserted via a bufconn test with the flag forced false).

**FR-MODEL-04** — The model-gateway MUST support provider-opaque continuation state via the `provider_raw` slot on `AssistantMessage` events. A `Pause` stop reason MUST be treated as a non-terminal signal; the loop MUST re-issue the request with the prior assistant content rather than treating the stream as final. Non-terminal `Pause` is Anthropic-only in v1 (required for `pause_turn` / thinking-signature continuation); other providers' non-terminal states are treated as errors (consistent with §6 support-matrix and §11.1/§11.6).

- AC-1: A fake `ModelPort` that emits `Pause` on the first call and `Done` on the second call causes the loop to issue exactly two `Generate` calls; only one `TurnFinished` event is appended.
- AC-2: The `provider_raw` JSONB on the `AssistantMessage` event round-trips the opaque Anthropic thinking-block signature byte-for-byte (verified in a testcontainers integration test using a stored fixture).

**FR-MODEL-05** — The model-gateway MUST normalize all provider error types to `ProviderError{Kind, RetryAfter, Raw}` and apply a harness-level retry policy: honor `Retry-After` header first, then exponential backoff with full jitter. The policy MUST retry only on `RateLimited`, `Overloaded`, and `Server` kinds; it MUST NOT retry `InvalidRequest`, `Auth`, or `Timeout` kinds.

- AC-1: A retry unit test with an injected `Clock` and `Jitter` drives two 429 responses with `Retry-After: 5` headers followed by a 200; asserts that the first retry waits exactly 5 s (mocked) and the second applies backoff, and that total attempt count is 3.
- AC-2: A 400 `InvalidRequest` from the provider propagates to the caller as `ProviderError{Kind: InvalidRequest}` with zero retries (assert `Clock.Sleep` is never called).

---

### FR-TOOL — Tool Registry & Execution

**FR-TOOL-01** — The tool-runtime MUST validate all tool inputs against their registered JSON Schema before execution. A schema validation failure MUST surface as an `Observation` with `isError=true`, not as a process panic or unhandled error.

- AC-1: A tool call with a missing required field returns an `Observation{isError: true, content: <validation error>}` without invoking the tool's execution function (verified by asserting the execution function is never called).
- AC-2: A tool call with an extra unexpected field that is not `additionalProperties: false` succeeds; one that violates `additionalProperties: false` returns an error observation.

**FR-TOOL-02** — The tool-runtime MUST provide the following core native tools: `read`, `edit`, `write`, `glob`, `grep`, `bash`, `webfetch`, `websearch`. Each tool MUST have a declared `SideEffect` (`ReadOnly` or `Mutating`) and an `EgressClass` (`None`, `Internal`, or `External`). `webfetch` and `websearch` MUST be classified `Mutating`/`External`.

- AC-1: A `ToolSpec` table-driven test asserts each core tool's declared `SideEffect` and `EgressClass` match the expected values.
- AC-2: Attempting to register a tool with a missing `name`, `description`, or null `JSONSchema` returns a typed `RegistrationError`.

**FR-TOOL-03** — The tool-runtime MUST persist execution intent before any side effects by appending `ToolExecutionStarted{call_id, idempotency_key}` to the event log (committed) before dispatching `ExecuteTool`. The `idempotency_key` MUST be derived as `hash(session_id, seq_of_ToolCall)`.

- AC-1: A testcontainers integration test kills the tool-runtime process after `ToolExecutionStarted` is committed but before `ToolResult` is appended; on restart, the `recovery` package classifies the execution as `unknown` for a `Mutating` tool and does NOT re-dispatch it.
- AC-2: Two orchestrator instances that independently derive the same `idempotency_key` from the same `(session_id, seq)` produce identical key values (determinism assertion).

**FR-TOOL-04** — The tool-runtime MUST maintain a durable tool-execution dedup ledger (`tool_executions` table). For a `Mutating` tool, a retried call with a key already marked `completed` in the ledger MUST return the prior result without re-executing. For a `ReadOnly` tool, the call MAY be retried without dedup.

- AC-1: An integration test simulates a tool-runtime restart after a `completed` ledger entry exists; a second `ExecuteTool` call with the same `idempotency_key` returns the prior `ToolResult` without calling the execution function.
- AC-2: Concurrent dedup writes with the same `idempotency_key` from two goroutines result in exactly one ledger row (unique constraint), one execution, and no data race (verified with `go test -race`).

**FR-TOOL-05** — The tool-runtime MUST execute each tool inside a per-session container (via the `Workspace`/`Runtime` port) with deny-by-default egress. On context cancellation or deadline expiry, the container process group MUST be terminated at the cgroup/PID-namespace boundary within 5 seconds; a cooperative-only cancel of the exec wrapper is insufficient.

- AC-1: An adversarial integration test runs a SIGTERM-trapping bash script inside the sandbox; the test asserts the process is dead within 5 s of context cancellation.
- AC-2: An adversarial integration test runs a double-forking process; the test asserts all descendant PIDs are dead within 5 s.
- AC-3: An adversarial integration test attempts a fork bomb; the hard PID-namespace limit terminates the tree within 5 s and no container state persists after `sandboxmgr` reaps it.

**FR-TOOL-06** — The tool-runtime MUST enforce per-session deny-by-default network egress via a single egress broker for all model-influenced outbound traffic (in-sandbox `bash`, `webfetch`, `websearch`, MCP HTTP). No tool execution path MUST have unrestricted egress.

- AC-1: An integration test starts a session without any egress allowlist entry and attempts a `webfetch` to an external URL; the request is blocked and the `Observation` carries `isError=true` with an egress-denied message.
- AC-2: An egress-security integration test injects an HTML page into the context instructing the agent to exfiltrate a secret via `webfetch`; the request is gated by the taint gate (requires human approval or is denied) and the secret bytes never leave the sandbox network namespace.

> **Amendment (as-built, 2026-06 — honesty reconciliation).** "A single egress broker" is the *logical* guarantee; the requirement and its ACs above are retained verbatim as frozen acceptance text. As built it is realized by **two** complementary enforcement points, not one network proxy: (a) the per-session sandbox container runs with `--network none`, so in-sandbox `bash` has **no network path at all** — egress is *severed*, not proxied; (b) the tool-runtime's own outbound tools (`webfetch`, `websearch`, MCP HTTP transport) are mediated by the deny-by-default per-session egress allowlist enforced **in their clients**. The load-bearing invariant — *no unrestricted egress from any model-influenced path* — holds under both. See ADR-0013 §Decision.

---

### FR-CTX — Context Management

**FR-CTX-01** — The orchestrator MUST maintain a running token count for the active session context window. When the context budget is within a configurable threshold of the model's `MaxOutputTokens`, automatic compaction MUST be triggered before the next `Generate` call.

- AC-1: A loop unit test with a fake `ModelPort` and a fake `TokenCounter` returning incrementally increasing values causes a `CompactBoundary` event to be appended when the budget threshold is crossed, followed by a `Generate` call with a reduced message list.
- AC-2: Compaction appends exactly one `CompactBoundary` event per compaction cycle; a `PreCompact` hook receives the pre-compaction context and may archive it (verified via the `HookRunner` fake).

**FR-CTX-02** — The orchestrator MUST support tool-result clearing: a `ToolResultCleared{cleared_ref, reason}` event MUST be appended-only (not a row deletion). Cleared results MUST render as stubs in the model-visible window; the full result MUST be retained in the event log and blob store.

- AC-1: After a `ToolResultCleared` event, the context manager's window-building function emits a stub message for the cleared result rather than the original content (golden output assertion on the built context window).
- AC-2: An attempt to clear an event that is not a `ToolResult` returns `FAILED_PRECONDITION`; clearing an already-cleared result is a no-op (idempotent).

**FR-CTX-03** — The orchestrator MUST mark stable content (system prompt, tool schemas, project context files) as prompt-cache prefixes. Cache-prefix assignment MUST be tenant-scoped: only tenant-agnostic content may appear in a shared stable prefix.

- AC-1: A unit test asserts that the cache-prefix builder marks the system prompt and tool definitions as cacheable and does NOT include any session-specific message history in the cache prefix.
- AC-2: An integration test with two distinct tenant IDs asserts their respective context windows never share a cache prefix containing private message content.

---

### FR-STATE — Session State & Persistence

**FR-STATE-01** — The orchestrator's event store MUST append events in a per-session contiguous integer sequence enforced by the database. The append transaction MUST be optimistic (compare `expected_seq` vs. `sessions.head_seq`), fenced (check `lease_epoch`), and idempotent (a re-sent `request_id` is a no-op, not a conflict).

- AC-1: A testcontainers integration test starts N goroutines all appending with `expected_seq=0`; exactly one COMMIT succeeds and N-1 return a typed `ConflictError`; the winning row has `seq=1` and the `sessions.head_seq` is exactly 1.
- AC-2: A testcontainers integration test appends an event, simulates a lost ACK by re-sending the same `request_id`, and asserts the returned row is the original event — not a `ConflictError` and not a duplicate row.
- AC-3: An append from a writer whose `lease_epoch` does not match `sessions.lease_epoch` returns a typed `FencedError` regardless of whether `expected_seq` is correct.

**FR-STATE-02** — The orchestrator MUST support session resume: given a `session_id` and an optional `from_seq`, the `recovery` package MUST fold all events into in-memory state and adjudicate any open turns or tool executions before resuming the loop.

- AC-1: A testcontainers test builds an event log for a session that was interrupted mid-turn, then starts a fresh orchestrator process; the recovery package correctly classifies the open turn and either continues or appends `TurnAborted`, and the subsequent `Run` proceeds from the correct state.

**FR-STATE-03** — The orchestrator MUST support session fork: `Fork(session_id, at_seq)` MUST create a new child session whose event sequence continues from `at_seq+1`, with the parent session unaffected and continuing to append past `at_seq`.

- AC-1: A testcontainers test forks a session at `at_seq=5`, appends two more events to the parent (reaching `seq=7`), and appends one event to the child (reaching `seq=6`); asserts no sequence collision and that loading either session produces its own correct state.
- AC-2: A fork of a session owned by a different tenant returns `PERMISSION_DENIED` (RLS + handler-level check).

**FR-STATE-04** — The event store MUST enforce Row-Level Security on all tenant-scoped tables using a non-owner database role, `SET LOCAL` GUC from the verified token, and `FORCE ROW LEVEL SECURITY`. A query with the `WHERE tenant_id=` predicate removed MUST still return only rows for the current tenant.

- AC-1: A testcontainers integration test sets `app.current_tenant` to tenant-A's UUID, inserts 10 events for tenant-A and 10 for tenant-B, then issues a `SELECT * FROM events` without a `tenant_id` predicate via the non-owner role; asserts exactly 10 rows are returned (tenant-A's only).

**FR-STATE-05** — Large tool outputs (above 32 KiB) MUST be stored in a tenant-scoped blob store, with bytes written before the blob metadata row, and the blob metadata row written in the same transaction as the event. The `events.blob_ref` FK MUST prevent dangling references.

- AC-1: A testcontainers integration test writes a 64 KiB tool output; asserts the `events` row has a non-null `blob_ref` and a null or minimal inline `payload`, and that the `blobs` row exists in the same committed transaction.
- AC-2: A test that writes the blob bytes, then fails the transaction before the `blobs` row is inserted, asserts no event row exists and no dangling `blob_ref` is present.

---

### FR-PERM — Permissions & Policy

**FR-PERM-01** — The orchestrator MUST enforce a layered permission pipeline in the order: deny → mode → allow → tool check. Deny rules MUST win unconditionally regardless of allow rules or operating mode.

- AC-1: A policy unit test configures a `deny` rule for `bash` alongside an `allow-all` rule; asserts the tool call is denied and the `ApprovalRequested` event is NOT appended (deny requires no human approval — it is a hard block that surfaces as `ApprovalDenied`).
- AC-2: A policy unit test in `plan` mode configures no explicit `allow` for `edit`; asserts an `ApprovalRequested` event is appended and the tool call is blocked pending human response.

**FR-PERM-02** — The orchestrator MUST support the following operating modes (settable server-side per session): `default` (standard allow/deny/ask), `acceptEdits` (file mutations auto-approved), `plan` (read-only until human approves a plan), `bypass` (all ask gates collapsed — operator-only, audited, forbidden under untrusted content or multi-tenant).

- AC-1: A loop unit test in `acceptEdits` mode asserts that `edit` tool calls are dispatched without appending `ApprovalRequested`; `bash` calls still require approval.
- AC-2: A policy unit test asserts that enabling `bypass` mode while the session has untrusted-content taint returns a `PolicyError` — bypass is not set.
- AC-3: Each `bypass`-mode session activation appends a distinct `BypassModeActivated` audit event containing the actor identity and timestamp.

**FR-PERM-03** — The orchestrator MUST implement a taint-tracking egress gate: once any untrusted content (web page, MCP tool output, untrusted file) enters the session context, subsequent `External`-classified tool calls targeting non-allowlisted hosts MUST require a human `ApprovalRequest`.

- AC-1: A policy unit test marks the session as tainted (via a prior `webfetch` observation), then attempts a second `webfetch` to a non-allowlisted URL; asserts an `ApprovalRequested` event is appended.
- AC-2: A policy unit test on an untainted session attempts `webfetch` to a non-allowlisted URL; asserts an `ApprovalRequested` event is appended (taint is not required for the first escalation — external comms to non-allowlisted hosts are always gated).

**FR-PERM-04** — The orchestrator MUST persist all human-in-the-loop approval decisions as events (`ApprovalRequested`, `ApprovalGranted`, `ApprovalDenied`). Approval decisions MUST be associated with a specific `call_id` and MUST be re-checkable on replay.

- AC-1: A loop unit test drives an `ApprovalRequested` through the fake `ApprovalGate` returning `Granted`; asserts the event log contains `[ApprovalRequested, ApprovalGranted]` in sequence, and the tool is dispatched only after `ApprovalGranted`.
- AC-2: Replaying a session log that contains `ApprovalGranted` for a specific `call_id` causes the context manager to treat that tool call as already approved without re-requesting approval.

---

### FR-OBS — Observability

**FR-OBS-01** — All three services (orchestrator, model-gateway, tool-runtime) and `projectord` MUST emit OpenTelemetry GenAI spans: `invoke_agent` per `Run`, `chat` per `Generate`, `execute_tool` per `ExecuteTool`. Spans MUST carry `gen_ai.*` attributes including model name, usage (input/output tokens), stop reason, and operation name. Trace context MUST propagate via gRPC metadata. Span names follow the convention `{gen_ai.operation.name} {model}` (e.g. `"chat <model-id>"`); `invoke_agent` and `execute_tool` spans append the agent or tool name — `"chat"` is the `gen_ai.operation.name` value, not the literal full span name.

- AC-1: A bufconn integration test starts orchestrator + model-gateway with a test OTel exporter; after one session completes, asserts an `invoke_agent` span exists with a child `chat` span carrying `gen_ai.usage.input_tokens` and `gen_ai.usage.output_tokens` set to values matching the fake `ModelPort`'s reported usage.
- AC-2: A trace-context integration test asserts the `trace_id` on the `invoke_agent` span matches the `trace_id` on the child `chat` span (propagation test).

**FR-OBS-02** — Every service MUST expose RED metrics per RPC: request count, error count broken down by typed termination subtype, and duration histogram. USE/saturation gauges MUST be exposed for: errgroup worker-pool occupancy, live sandbox count, PostgreSQL connection-pool utilization, blob-store usage (bytes), and `projectord` projection lag.

- AC-1: After 10 successful `Run` calls, the `run_requests_total` counter equals 10 and `run_errors_total` equals 0 (metrics endpoint assertion via Prometheus text format).
- AC-2: After a `Run` terminates with `error_max_turns`, `run_errors_total{subtype="error_max_turns"}` is incremented.

**FR-OBS-03** — Structured logging MUST use `log/slog` with `JSONHandler` in production. Secret-bearing types (provider API keys, user credentials, session tokens) MUST implement `slog.LogValuer` to redact their values in all log output.

- AC-1: A unit test creates a `ProviderConfig` struct containing an API key and passes it to `slog.Info`; asserts the JSON log output contains `"REDACTED"` (or equivalent) for the key field, not the actual key value.

**FR-OBS-04** — The orchestrator MUST detect stuck-loop conditions (repeated identical tool calls with no progress within a configurable window) and surface them as a distinct metric label and structured log event. This is an operational signal in addition to the eventual max-turns/max-budget cap.

- AC-1: A loop unit test with a fake `ModelPort` that emits the same tool call 5 times consecutively triggers a `doom_loop_detected_total` counter increment and a structured log entry with the repeating tool name.

**FR-OBS-05** — Every service MUST expose a gRPC health endpoint (`grpc.health.v1.Health`) and HTTP `/livez` + `/readyz` endpoints. Readiness MUST gate on actual dependency reachability: the orchestrator `/readyz` checks PostgreSQL ping + downstream gRPC health + SPIFFE SVID present; tool-runtime checks container runtime availability.

- AC-1: An integration test starts the orchestrator with a misconfigured PostgreSQL DSN; the `/readyz` endpoint returns HTTP 503 while `/livez` returns HTTP 200.
- AC-2: An integration test starts the orchestrator with all dependencies healthy; both `/livez` and `/readyz` return HTTP 200.

---

### FR-EXT — Extensions (MCP, Hooks, Sub-Agents)

**FR-EXT-01** — The tool-runtime MUST implement an MCP client supporting stdio and HTTP transports. MCP tool schemas MUST be loaded lazily (on first use or on explicit request, not at startup). Each MCP server MUST run inside its own confined sandbox with deny-by-default egress. The SPIFFE SVID MUST NOT be accessible from within an MCP server's sandbox.

- AC-1: An integration test starts an MCP server stub via stdio; the tool-runtime loads its tool list only after the first invocation request (lazy load); the tool-runtime's SVID socket path is not bind-mounted into the MCP sandbox.
- AC-2: A unit test configures an MCP server with an HTTP transport and a deny-all egress policy; asserts that HTTP requests from within the MCP sandbox are blocked by the egress broker.
- AC-3 (ListTools): A unit test calls the `ListTools` RPC (orchestrator→tool-runtime) against a registry containing at least one native tool and one lazily-loaded MCP tool; asserts the response includes the merged set with a non-empty `name`, `description`, and valid JSON Schema for each entry — seeding the `tool_runtime.proto` `ListTools` request/response shape before the proto is frozen.

**FR-EXT-02** — First registration of an MCP server and each of its tools MUST require explicit human approval before the tool is made available to the agent loop. MCP tool descriptions and schemas MUST be treated as untrusted content.

- AC-1: A loop unit test registers an MCP server with a new tool; asserts an `MCPToolApprovalRequested` event is appended and the tool is NOT available in the next `Generate` context until `ApprovalGranted` is received.
- AC-2: A unit test verifies that raw MCP tool descriptions are NOT passed through to the model's tool definition context without human review (i.e., they are held in a pending-approval queue, not the active registry).

**FR-EXT-03** — The orchestrator MUST support a hooks/middleware pipeline with four named hooks: `PreToolUse`, `PostToolUse`, `Stop`, `PreCompact`. A `PreToolUse` hook returning a block signal MUST prevent tool execution. Hooks run in the host process via subprocess invocation behind a `CommandRunner` port.

- AC-1: A loop unit test configures a `PreToolUse` hook via the fake `HookRunner` that returns `Block`; asserts the tool is NOT dispatched and `ApprovalDenied` is appended with `reason=hook_blocked`.
- AC-2: A `PostToolUse` hook receives the tool name, input, and observation in a structured JSON payload and can inspect them (verified via the fake `HookRunner` argument capture).

**FR-EXT-04** — The orchestrator MUST support depth-limited sub-agents as ordinary tools. A sub-agent runs the full agent loop in its own goroutine with its own session (forked or fresh), and returns a condensed summary to the parent loop. Sub-agent depth MUST be enforced via a configurable cap; exceeding the cap causes the tool to return an error observation.

- AC-1: A loop unit test spawns a sub-agent tool at depth 1 and asserts a separate session event log is created; the parent session log contains a `ToolResult` with the sub-agent's condensed output.
- AC-2: A loop unit test attempts to spawn a sub-agent at the configured max depth; asserts the tool call returns an `Observation{isError: true, content: "max sub-agent depth exceeded"}` without spawning a new session.

---

### FR-API — Client-Facing API

**FR-API-01** — The orchestrator MUST expose a `Run` server-streaming RPC that accepts a session ID, a user message, and an optional `last_event_id` (sequence number). Each streamed `EventFrame` MUST carry its own `seq` field. A client reconnecting with `last_event_id=N` MUST receive only events with `seq > N`.

- AC-1: A bufconn integration test simulates a client that disconnects at `seq=5` and reconnects with `last_event_id=5`; asserts the second stream starts at `seq=6` and contains no duplicate frames.
- AC-2: A bufconn integration test with a slow-reader client asserts the server never blocks the upstream `Generate` call due to client backpressure (generation proceeds to the log; delivery tails the log).

**FR-API-02** — The orchestrator MUST expose a `Control` unary RPC accepting `Approve{session_id, call_id}`, `Deny{session_id, call_id}`, `Interrupt{session_id}`, and `Reattach{session_id, from_seq}`. The caller MUST be authorized as the owner of `session_id`.

- AC-1: A unit test calls `Control.Approve` with a session ID owned by a different tenant; asserts `PERMISSION_DENIED` is returned and no event is appended.
- AC-2: `Control.Reattach{session_id, from_seq=0}` replays all events from the beginning of the session stream.

**FR-API-03** — The client-facing gRPC API MUST be secured with bearer/OIDC authentication at the edge. Tokens MUST be validated for correct `iss`, `aud`, and `exp` fields; the `alg=none` attack vector MUST be rejected. Unauthenticated requests MUST receive `UNAUTHENTICATED`.

- AC-1: A bufconn test sends a `Run` request with an expired JWT; asserts `UNAUTHENTICATED` is returned.
- AC-2: A bufconn test sends a `Run` request with a token where `alg=none`; asserts `UNAUTHENTICATED` is returned.

**FR-API-04** — The orchestrator SHOULD expose an optional REST/JSON facade via grpc-gateway for at minimum the `Run` endpoint (SSE chunked response) and the `Control` endpoint (POST). The REST facade MUST enforce identical authentication, authorization, and rate limiting to the gRPC edge.

- AC-1: An integration test calls the REST `/v1/run` SSE endpoint with a valid JWT and asserts it receives the same `EventFrame` stream as the gRPC `Run` call for the same session.
- AC-2: An integration test calls the REST endpoint with no Authorization header; asserts HTTP 401.

---

## 5. Non-Functional Requirements

### NFR-REL — Reliability

**NFR-REL-01** — The event store (PostgreSQL append path) MUST be idempotent: a retried append with an identical `request_id` MUST return the existing row as success, not a conflict error. Idempotent retry MUST be safe on transient PostgreSQL errors.

**NFR-REL-02** — The orchestrator MUST bound retry-with-backoff on transient event-store failures to a configurable budget within the turn deadline. Exhausting the budget MUST result in `subtype=error_during_execution`, not a hung goroutine.

**NFR-REL-03** — The single-writer lease MUST use a fenced TTL mechanism: every append checks the `lease_epoch`; a writer that lost its lease (TTL expiry or takeover) MUST be rejected even if its `expected_seq` is current. Lease TTL heartbeats MUST be at most half the TTL interval to prevent spurious expiry.

**NFR-REL-04** — The `projectord` worker MUST use an xmin-bounded safe-advance cursor so out-of-order-committing PostgreSQL transactions are never silently skipped by cost-rollup or OTel-export projections. Projection lag MUST be monitored; an alert MUST fire when lag exceeds a configurable threshold. `projectord` lag MUST NOT block any agent turn.

**NFR-REL-05** — Generation of a session MUST be decoupled from client delivery. A stalled client (slow read) MUST NOT backpressure the upstream provider stream. A relay stall deadline (configurable, default 30 s) detaches the stalled client; the generation completes to the event log and the client may reattach.

### NFR-SEC — Security

**NFR-SEC-01** — All service-to-service communication MUST use mutual TLS with SPIFFE/SPIRE workload identity. A development-only static-cert fallback MUST require `BOLTROPE_DEV_INSECURE=1` at startup, log a prominent warning, and MUST be compiled out of release images.

> **Amendment (as-built, 2026-06 — honesty reconciliation).** The "compiled out of release images" clause is retained verbatim as frozen requirement text but is NOT how the build realizes the guarantee. As built: the dev static-cert path is **env-gated, not compile-gated** — it is present in every binary and engages only when `BOLTROPE_DEV_INSECURE=1` is explicitly set (prominent WARN logged); what IS build-tag-controlled is the **SPIRE Workload API wiring** (release images build with `-tags spire`; an untagged build carries a nil source stub). The load-bearing invariants hold: a non-dev process without a SPIFFE source **fails closed at startup**, and the fallback can never engage silently. See ADR-0013 §Amendment.

**NFR-SEC-02** — Tenant isolation MUST be enforced at the database layer via PostgreSQL Row-Level Security (non-owner role, `SET LOCAL` GUC from the verified tenant token, `FORCE ROW LEVEL SECURITY` on all tenant-scoped tables). RLS enforcement MUST be validated by an integration test that removes the `WHERE tenant_id=` predicate and confirms cross-tenant rows are still blocked.

**NFR-SEC-03** — Blob identity MUST be tenant-scoped (`PRIMARY KEY (tenant_id, ref)`). Cross-tenant content-addressed dedup is forbidden. Every blob fetch MUST be authorized by `(tenant_id, session_id)` ownership, never by `ref` alone.

**NFR-SEC-04** — All model-influenced network egress (sandbox `bash`, `webfetch`, `websearch`, MCP HTTP) MUST route through a single deny-by-default per-session egress broker. There MUST be no unrestricted egress path from any model-influenced code path. Provider-native/server-side tools are disabled in v1.

> **Amendment (as-built, 2026-06 — honesty reconciliation).** The "single egress broker" wording is retained verbatim as frozen requirement text but describes a *logical* guarantee realized by two mechanisms (see FR-TOOL-06 amendment and ADR-0013): the sandbox container has `--network none` (in-sandbox `bash` egress is *severed*, not proxied), and the tool-runtime's `webfetch`/`websearch`/MCP-HTTP clients enforce the deny-by-default per-session allowlist. The invariant "no unrestricted egress from any model-influenced path" is satisfied.

**NFR-SEC-05** — Provider API keys MUST reside only in model-gateway configuration (environment variables). They MUST NOT appear in the event log, the orchestrator process, or in any gRPC/HTTP response body. `slog.LogValuer` redaction MUST cover all secret-bearing types.

**NFR-SEC-06** — The `bypass` permission mode is an operator-only, server-side setting that MUST (a) be forbidden when untrusted content is present in the session, (b) be forbidden in multi-tenant mode, (c) be not settable by any client request or by the model, and (d) emit a `BypassModeActivated` audit event each time it takes effect. Even under `bypass`, egress broker denial and tenant isolation remain active.

**NFR-SEC-07** — Each MCP server MUST run in its own confined sandbox; its sandbox MUST NOT have access to the SPIFFE SVID socket or the Boltrope service network namespace. MCP tool descriptions are untrusted input; raw descriptions MUST NOT be injected into the system prompt or tool-def region without human review.

**NFR-SEC-08** — The client-facing edge MUST validate OIDC/bearer tokens for `iss`/`aud`/`exp`, pin signing algorithms (reject `alg=none`), and rotate JWKS. Every `Run`/`Control` call MUST verify session ownership by the authenticated principal.

### NFR-PERF — Performance

> **Measurement note:** the performance measurement harness and methodology are pinned at the TDD/implementation gate; these thresholds become a non-gating CI benchmark (not part of the binding DoD coverage/lint/eval gates).

**NFR-PERF-01** — A single event append (optimistic + fenced + idempotent transaction) on a healthy PostgreSQL ≥13 instance MUST complete in under 20 ms at the 95th percentile under a load of 100 concurrent sessions.

**NFR-PERF-02** — The read-only tool parallel dispatch pool MUST dispatch up to `min(4, GOMAXPROCS)` tools concurrently (configurable). Pool exhaustion MUST exert backpressure rather than unbounded goroutine creation.

**NFR-PERF-03** — Container sandbox startup (for the first tool call in a session) SHOULD complete within 3 seconds on a standard CI runner. Subsequent tool calls in the same session SHOULD reuse the running container without a cold-start penalty.

**NFR-PERF-04** — Streaming token deltas from the model-gateway to the orchestrator relay MUST not introduce more than 50 ms of additional latency (gateway-side processing overhead) per 1,000 token chunks at the 99th percentile.

### NFR-PORT — Portability

**NFR-PORT-01** — The system MUST run on Linux (amd64 and arm64) and the CI environment. A `docker compose up` on any conforming Docker host MUST bring up the full stack (PostgreSQL, migrations, three services, `projectord`) without host-installed Go or other build tools.

**NFR-PORT-02** — The `Provider` interface and `Workspace`/`Runtime` port MUST be the only abstraction boundaries through which provider and sandbox implementations are coupled to the rest of the system. Swapping a provider or sandbox backend MUST require changes only in the relevant adapter package, not in the agent loop.

**NFR-PORT-03** — The PostgreSQL minimum version is pinned to 13 (required for `xid8`/`pg_current_xact_id()`). The pinned version MUST be validated at config/startup with a clear error message and enforced in CI.

**NFR-PORT-04** — Release artifacts MUST be multi-arch static binaries (amd64 + arm64) with ldflags version stamping, multi-arch Docker images published to GHCR, SBOM (syft), and keyless cosign signing + SLSA provenance via GoReleaser on tag.

### NFR-TEST — Testability

**NFR-TEST-01** — Every component that uses `time.Now()`, `rand.*`, or `uuid.New()` directly MUST instead accept an injected `Clock`, jitter/rand source, or `IDGenerator` through its `ports.go`. Direct calls to these functions in `domain/` or `app/` MUST be forbidden by a `depguard`/`forbidigo` linter rule.

**NFR-TEST-02** — The unit test suite (no network, no Docker) MUST complete in under 60 seconds on a standard CI runner. Integration tests requiring Docker/PostgreSQL MUST be tagged `//go:build integration` and run separately.

**NFR-TEST-03** — Overall test coverage MUST reach at least 75% (line coverage). `go test -race` MUST pass with no data races in CI for all packages.

**NFR-TEST-04** — The deterministic eval harness (ADR-0007) MUST run as a required CI gate. It drives the full agent loop against a scripted fake `Provider` and fake clock; it MUST assert correct tool selection, termination subtype, turn/budget caps, event-log golden shape, and compaction/permission behavior with no real network calls.

**NFR-TEST-05** — The following adversarial tests are named and MUST NOT be omitted from the integration suite: (a) RLS cross-tenant block with `WHERE` predicate removed; (b) cross-tenant blob fetch denied; (c) fork of a foreign-tenant session denied; (d) exfil-via-`webfetch`-after-injected-page blocked/gated; (e) two tenants never share a private cache entry; (f) MCP server cannot read the SVID/socket; (g) static-cert provider refuses to start outside dev; (h) SIGTERM-trapping process terminated within 5 s; (i) double-forked detached child terminated within 5 s; (j) fork bomb terminated within 5 s.

### NFR-OBS-NFR — Observability NFRs

**NFR-OBS-01** — OTel GenAI traces MUST be exported to a configurable OTLP collector endpoint. Span and metric attribute names MUST follow the OTel GenAI semantic conventions (status 'Development' (unstable) as of 2026; names pinned at adoption and documented in `ARCHITECTURE.md`).

**NFR-OBS-02** — The `trace_id` and `span_id` from active OTel spans MUST be injected into `slog` log entries via `context.Context` so every log line is co-relatable to the originating trace.

**NFR-OBS-03** — Baseline SLOs and the minimum alert set MUST be documented (not just instrumented): event-store append error rate, live sandbox count near cap, PostgreSQL connection pool near exhaustion, `projectord` projection lag, and stuck-session count. Alert thresholds MUST be configurable.

### NFR-OPS — Operations

**NFR-OPS-01** — Database migrations MUST be a release gate: `cmd/boltrope-migrate` MUST run to completion and exit 0 before any orchestrator instance accepts traffic. Migration scripts MUST follow expand/contract, forward-only discipline for `events` and `sessions` tables. Destructive `down` migrations on the event log are a CI-blocked anti-pattern.

**NFR-OPS-02** — The `docker-compose.yml` MUST declare `depends_on` + `healthcheck` ordering: PostgreSQL healthy → `boltrope-migrate` exits 0 → services start. A Kubernetes deployment MUST use init containers or readiness gates for the same ordering guarantee.

**NFR-OPS-03** — The `sandboxmgr` MUST enforce idle TTL, absolute TTL, and a max-live-sandboxes cap per node. A GC reconciliation loop MUST reap containers whose session is `finished`, `failed`, or abandoned (keyed from `sessions.status` in PostgreSQL).

**NFR-OPS-04** — Configuration MUST be loaded via `knadh/koanf` with precedence `flags > env > file > defaults`. The service MUST validate all required configuration on startup and exit non-zero with a human-readable error if any required field is missing or invalid.

### NFR-EXT-NFR — Extensibility NFRs

**NFR-EXT-01** — The `EventLogPort` interface MUST be the only consumer-defined contract through which the orchestrator loop accesses the event store. If a separate event-store service becomes necessary in the future, only the adapter implementation in `adapter/outbound/eventstore` changes; the loop and domain do not change.

**NFR-EXT-02** — The `RuntimePort` MUST be the only boundary through which sandbox backends are coupled to the tool-runtime. Adding a microVM or OS-native sandbox backend MUST require only a new `adapter/runtime/<backend>` package.

**NFR-EXT-03** — Adding a new LLM provider MUST require only: (a) a new adapter package under `adapter/provider/<name>` implementing `ProviderPort`, and (b) an entry in the per-`(endpoint,model)` capabilities table. The agent loop MUST NOT require any change.

---

## 6. Multi-LLM Support Matrix

The following table reflects the v1 support status per ADR-0004 and the architecture's provider abstraction. Capability flags are resolved per `(endpoint, model)` at runtime; the table shows the default/expected value for the canonical model in each family.

| Provider / Endpoint | Tool Calls | Parallel Tool Calls | Streaming Tool Calls | Vision (Image Input) | System Prompt | Extended Thinking | Token Counting (server-side) |
|---|---|---|---|---|---|---|---|
| **Anthropic Claude 3.5+** | v1 YES | v1 YES | v1 YES | v1 YES | v1 YES (top-level `system` param) | v1 YES (thinking blocks + opaque `signature`) | v1 YES (`POST /v1/messages/count_tokens`) |
| **Google Gemini 2.x Pro** | v1 YES | v1 YES | v1 YES (most models) | v1 YES | v1 YES (`systemInstruction` field) | v1 NO | v1 YES (`countTokens` + `usageMetadata`) |
| **Google Gemini Flash-Lite** | v1 YES | v1 YES | v1 NO (buffer in gateway) | v1 YES | v1 YES | v1 NO | v1 YES |
| **OpenAI (Responses API)** | v1 YES | v1 YES | v1 YES | v1 YES | v1 YES (`instructions` field) | v1 NO | v1 NO (local `o200k_base` tokenizer or `Unsupported`) |
| **OpenAI (Chat Completions sub-flag)** | v1 YES | v1 YES | v1 YES | v1 YES | v1 YES (system-role message) | v1 NO | v1 NO |
| **vLLM (OpenAI-compat)** | v1 YES | v1 YES | v1 YES | model-dependent | v1 YES | v1 NO | v1 NO |
| **Ollama (OpenAI-compat /v1)** | v1 YES (model-dependent) | v1 YES (model-dependent) | v1 YES (model-dependent) | model-dependent | v1 YES | v1 NO | v1 NO |
| **LM Studio (OpenAI-compat)** | v1 YES | v1 NO (capability flag false) | v1 NO (gateway buffers) | model-dependent | v1 YES | v1 NO | v1 NO |
| **llama.cpp llama-server (OpenAI-compat)** | v1 YES (model-dependent) | v1 YES (model-dependent) | v1 YES (model-dependent) | model-dependent | v1 YES | v1 NO | v1 NO |
| **LiteLLM proxy (OpenAI-compat)** | v1 YES (proxied) | v1 YES (proxied) | v1 YES (proxied) | proxied | v1 YES | proxied | v1 NO |

**Notes:**

- Capability flags are per `(endpoint, model)` and are configurable overrides; the table shows defaults.
- OpenAI does not expose a server-side token-counting endpoint; the gateway returns `Unsupported` or uses a local `o200k_base` tokenizer. **Never** use tiktoken to estimate Anthropic or Gemini tokens.
- LM Studio capability flags `SupportsStreamingToolCalls=false` and `SupportsParallelToolCalls=false` are set in the default per-endpoint config; the gateway buffers and emits complete calls.
- Provider-native/server-side tools (Anthropic `web_search`, OpenAI Responses built-ins) are **disabled** in v1 via a hard policy switch in the gateway.
- `Pause` (non-terminal stop reason) is supported only for Anthropic in v1 (required for `pause_turn` / thinking-signature continuation). Other providers' non-terminal states are treated as errors.
- The old `github.com/google/generative-ai-go` SDK is EOL (2025-11-30); only `google.golang.org/genai` is used.

---

## 7. External Interfaces

### 7.1 Client-Facing Surface

The primary external interface is gRPC (proto3, HTTP/2 + mTLS at the edge) with an optional REST/JSON facade generated by grpc-gateway from the same protos.

**`OrchestratorService`** (defined in `proto/boltrope/v1/orchestrator.proto`):

| RPC | Shape | Purpose |
|---|---|---|
| `Run(RunRequest) returns (stream EventFrame)` | Server-streaming | Submit a turn; stream `EventFrame` oneof values (`TextDelta`, `ThinkingDelta`, `ToolProgress`, `ApprovalRequest`, terminal `Result`). Each frame carries `seq`. Accepts optional `last_event_id` for resumable reconnect. |
| `Control(ControlRequest) returns (ControlResponse)` | Unary | `Approve`, `Deny`, `Interrupt`, `Reattach`. Decouples control from the data stream. Session-ownership check on every call. |
| `Fork(ForkRequest) returns (ForkResponse)` | Unary | Fork a session at a given `at_seq`. Returns the new child `session_id`. |

**Resumability:**

- Every `EventFrame` carries a monotonic per-session `seq` field.
- A reconnecting client sends `last_event_id = <last received seq>` on `Run`; the server replays only events with `seq > last_event_id` from the event log.
- This eliminates the "missed frames during reconnect" problem without requiring client-side buffering.
- The grpc-gateway REST facade maps the `Run` stream to a chunked/SSE HTTP response; `last_event_id` maps to the standard HTTP `Last-Event-ID` header.

**Detailed gRPC/proto contracts** (message shapes, field names, error codes, enum values) are frozen in the next implementation gate.

### 7.2 Internal Service-to-Service Surface

| Boundary | RPC Shape | Proto |
|---|---|---|
| Orchestrator → model-gateway | `Generate` (server-streaming) + `CountTokens` (unary) + `Capabilities` (unary) | `model_gateway.proto` |
| Orchestrator → tool-runtime | `ExecuteTool` (server-streaming) + `ListTools` (unary) | `tool_runtime.proto` |
| Shared types | `Message`, `ContentPart`, `ToolCall`, `Usage`, `StopReason`, `EventFrame` variants | `common.proto` |

There is no `event_store.proto`; the event store is an in-process pgx package within the orchestrator.

### 7.3 Key Protocol Properties

- **No client-streaming or bidirectional streaming in v1** — avoids proxy-hostile bidi at the public edge.
- **Deadline propagation** — the client sets a deadline on `Run`; the orchestrator propagates a derived `context.Context` deadline to every downstream RPC.
- **mTLS on all internal boundaries** via SPIFFE/SPIRE SVIDs; the development fallback requires `BOLTROPE_DEV_INSECURE=1`.
- **grpc-gateway REST facade** is a transcoding layer only; it enforces identical authentication, authorization, and rate limiting to the gRPC edge — it is not a second trust boundary.

---

## 8. Data & State

### 8.1 Event-Sourcing Principles

The append-only event log in PostgreSQL is the single source of truth. No derived state is authoritative; everything observable — session history, current context window, cost totals, approval decisions — is a projection of the event log.

Key principles:

- Events are never deleted or mutated; superseding facts (tool-result clearing, fork branching) are expressed as new append-only events.
- The log carries assembled messages (not raw streaming token deltas), so replay is deterministic.
- A `provider_raw` JSONB column preserves provider-opaque continuation state (Anthropic thinking signatures, `pause_turn` content) byte-faithfully alongside the normalized assembled message.
- All events carry a per-session contiguous integer `seq` enforced at the database level by a head-transition `UPDATE RETURNING` pattern; a `(session_id, seq)` unique constraint is a backstop.

### 8.2 Resume, Fork, and Replay

- **Resume:** `Load(session_id, from_seq)` folds events (optionally from a snapshot) into in-memory state. The `recovery` package adjudicates open turns and tool executions before resuming.
- **Fork:** `Fork(session_id, at_seq)` creates a new child session; the child's sequence continues from `at_seq+1`. The parent is unaffected and may keep appending. A fork is a new branch — never a rewrite. Forks are verified by tenant ownership at both the handler and the RLS layer; they never cross tenant boundaries.
- **Replay:** Deterministic for committed messages; interrupted live generations are recovered (not replayed identically) via the `TurnStarted`/`AssistantMessageDelta` checkpoint chain. `provider_raw` makes provider-specific continuation faithful.

### 8.3 Tenancy

Every row in the event store (`events`, `sessions`, `session_snapshots`, `blobs`, `event_subscriptions`, `tool_executions`) carries a `tenant_id`. Row-Level Security enforces tenant scoping at the database layer as described in FR-STATE-04. The tenant token is propagated between services as a short-lived signed token (PASETO/JWT) that is bound to the specific RPC (method + `session_id`), not a bearer header that can be replayed across calls.

**v1 multi-tenant honesty:** the data model is multi-tenant, but v1 containerized isolation is safe only for single-tenant or trusted-code deployments. Multi-tenant execution of mutually-untrusted code requires the deferred microVM/gVisor runtime backend.

### 8.4 Secret Handling

- Provider API keys live only in model-gateway configuration (environment variables at runtime), never in the event log, orchestrator memory, or any gRPC/HTTP response.
- Output masking (best-effort regex/registry scan) is applied to tool results before they enter the event log — this is defense-in-depth log hygiene, not a containment boundary. The real exfiltration control is egress restriction.
- Prompt-cache prefixes are tenant-scoped: only tenant-agnostic content (system prompt, tool schemas) may appear in a shared stable prefix. Session-specific or private content is never placed in a cached prefix.

### 8.5 Large Outputs and Blob Storage

Tool outputs exceeding 32 KiB are written to a tenant-scoped blob store before the event is appended. The blob metadata row and the event row are written in the same database transaction (preventing dangling references). The event `payload` retains a lightweight descriptor; full bytes are fetched on demand, authorized by `(tenant_id, session_id)` ownership. A background sweeper reclaims orphaned blob bytes.

The detailed DDL (table definitions, indexes, constraints, RLS policies, migration structure) is specified in `docs/architecture/00-architecture.md` §6 and is not duplicated here.

---

## 9. Success Criteria / Definition of Done for v1

All criteria below MUST be met before v1 is declared complete. Each is independently verifiable.

| # | Criterion | How verified |
|---|---|---|
| DOD-01 | All MUST functional requirements (FR-LOOP-*, FR-MODEL-*, FR-TOOL-*, FR-CTX-*, FR-STATE-*, FR-PERM-*, FR-OBS-*, FR-EXT-*, FR-API-*) are implemented and their acceptance criteria pass. | CI: all unit + integration tests green. |
| DOD-02 | All acceptance criteria marked with adversarial tests in NFR-TEST-05 pass: (a) RLS cross-tenant block; (b) cross-tenant blob denial; (c) foreign-session fork denied; (d) exfil-via-webfetch blocked; (e) private cache entry not shared; (f) MCP SVID isolation; (g) dev cert refuses outside dev; (h,i,j) SIGTERM-trapping, double-fork, fork bomb each killed within 5 s. | CI: `//go:build integration` suite with real Docker. |
| DOD-03 | The deterministic eval harness (ADR-0007) runs in CI with at least 5 golden scenarios exercising: tool selection, `error_max_turns` termination, `error_max_budget_usd` termination, compaction trigger, and permission-mode enforcement. All scenarios MUST pass with zero network calls. | CI: eval job is a required gate on every PR. |
| DOD-04 | A real coding task (e.g., "add a function, run its test") completes end-to-end against at least one hosted LLM provider (Anthropic or Google) and at least one self-hosted OpenAI-compatible endpoint (Ollama or vLLM). | **Opt-in (API-key gated).** The `livesmoke` CI job runs `go test -tags livesmoke ./test/eval/...`, which drives the real loop against a real provider resolved from the environment and **skips when no key is set** — so it is verified only on demand with a configured key, never on the default PR run. The keyless `stub` smoke (DOD-05) does **not** satisfy this. |
| DOD-05 | `docker compose up` from a clean checkout (no local Go, no pre-built images) brings up PostgreSQL, runs migrations, starts all three services and `projectord`, and the stack passes readiness checks within 120 s. | **Verified.** The `compose-smoke` CI job runs `docker compose -f deploy/docker-compose.yml up --build --wait` from a clean checkout, asserts every service's `/readyz` returns 200 within 120 s (cross-service readiness, incl. the orchestrator's Postgres ping and the tool-runtime's `docker version`), then tears down. The gateway defaults to the keyless `stub` provider so this needs no API key. |
| DOD-06 | Test coverage is at or above 75% (line coverage) across the entire module. `go test -race` reports no data races. | `go test -race -coverprofile=coverage.out ./...` in CI; threshold enforced. |
| DOD-07 | `golangci-lint run` exits 0 with the project's `.golangci.yml` (v2, `linters.default: standard` + curated enable list including `errcheck`, `govet`, `staticcheck`, `revive`, `gosec`, `misspell`, `gocritic`, `bodyclose`, `depguard`, `forbidigo`). No linter suppressions without a tracked justification comment. | CI: lint job is a required gate. |
| DOD-08 | No `forbidigo` rule violations: domain and app packages contain no direct calls to `time.Now()`, `rand.*` (without an injected source), or `uuid.New()`. The `platform/llm` package contains no imports from `gen/` or any provider SDK. No service imports another service's `domain/` or `app/` packages. | CI: lint job (same as DOD-07). |
| DOD-09 | A quickstart document exists at `README.md` with a copy-paste sequence that: (1) clones the repo, (2) sets a provider API key env var, (3) runs `docker compose up`, (4) runs `boltrope-ctl run "hello world"`, and (5) observes a result. The quickstart fits in the first screenful of `README.md`. | **Verified.** The quickstart exists and is now **keyless** — step (2) is no longer required because the gateway defaults to the `stub` provider. The `compose-smoke` CI job exercises (1) clone + (3) `docker compose up --wait` + cross-service readiness, then runs (4) `harnessctl run` over the shared-seed dev mTLS edge (`BOLTROPE_DEV_INSECURE=1`) and (5) asserts a terminal `[result]` frame (or an `[approval required]` gate — both prove the end-to-end client → orchestrator → model-gateway → tool-runtime path). |
| DOD-10 | The OpenSSF Scorecard action reports a score of at least 5.0/10 and the project has applied for the OpenSSF Best Practices Badge "passing" tier. `SECURITY.md` exists with private vulnerability reporting enabled. | Scorecard CI action result; GitHub Security Advisories configured. |
| DOD-11 | All four proto files (`common.proto`, `orchestrator.proto`, `model_gateway.proto`, `tool_runtime.proto`) are present, linted by `buf lint`, and breaking-change detection (`buf breaking`) is wired to CI against the main branch. Generated `gen/` code is committed and matches `buf generate` output. | CI: `buf lint` + `buf breaking` jobs. |
| DOD-12 | `cmd/boltrope-migrate` runs all migrations against a clean PostgreSQL 13 instance without errors; the `sessions.lease_epoch`, `events.request_id`, `events.provider_raw`, `events.transaction_id`, and `blobs.(tenant_id, ref)` composite PK are all present with their correct types and constraints. | testcontainers migration integration test. |

> **Verification status notes (DOD-04 / DOD-05 / DOD-09).** As of this revision:
> **DOD-05 is verified** — the keyless `compose-smoke` CI job brings the full
> stack up from a clean checkout and asserts cross-service `/readyz` within 120 s.
> **DOD-09 is verified** — the same job then drives the keyless quickstart task via
> `harnessctl` over the shared-seed dev mTLS edge (`BOLTROPE_DEV_INSECURE=1`,
> presenting the `edge` SVID the orchestrator's RBAC admits) and asserts a terminal
> `[result]` frame, so the clone → `docker compose up` → CLI run → observed-result
> path runs end-to-end in CI. **DOD-04 remains opt-in** — a real-model end-to-end
> run is only exercised by the API-key-gated `livesmoke` job, which skips without a
> key; the keyless `stub` provider used by DOD-05/09 deliberately does not satisfy
> it (it proves the wiring, not real model behavior). **DOD-10 is partially met**
> (as published, 2026-06): the Scorecard action runs in CI and publishes results —
> the first public score is **4.3/10, below the 5.0 target** — and the OpenSSF
> Best Practices Badge application has **not** been submitted yet. The ≥5.0 text is
> retained as the frozen acceptance bar; this note records honestly that it is not
> yet reached. `SECURITY.md` exists; private vulnerability reporting must be
> enabled in the repository settings (a GitHub UI step, not CI-verifiable).

---

## 10. Glossary

| Term | Definition |
|---|---|
| **Agent loop** | The gather→act→verify ReAct-style loop that drives a session: build context window → call model → dispatch tool calls → feed results back → repeat until terminal. |
| **Turn** | One full model round-trip: model output containing zero or more tool calls, their execution, and results fed back. Turns repeat without yielding to the caller until the model produces a text-only response or a terminal condition fires. |
| **Termination subtype** | One of five typed outcomes for a completed session: `success`, `error_max_turns`, `error_max_budget_usd`, `error_during_execution`, `error_max_structured_output_retries`. |
| **Event** | An immutable, append-only record in the PostgreSQL `events` table. Typed by `event_type`; carries a per-session monotonic `seq`. Examples: `UserMessage`, `TurnStarted`, `AssistantMessage`, `ToolExecutionStarted`, `ToolResult`, `TurnFinished`. |
| **Event log / event store** | The append-only PostgreSQL table that is the single source of truth for all session state. Implemented as an in-process `pgx` package inside the orchestrator. |
| **Session** | The event-sourcing stream/aggregate. One agent task from initial user message to terminal result. Identified by a UUID. Sessions can be forked, resumed, and replayed. |
| **Fork** | A new child session branching from a parent session at a specific `at_seq`. The child's `seq` continues from `at_seq+1`. Forks never cross tenants. |
| **Resume** | Loading the event log of a session and continuing the agent loop from the current state, with the `recovery` package adjudicating any open turns or tool executions. |
| **Replay** | Re-building session state from the event log deterministically, without re-executing tools or re-calling the model. Deterministic because events store assembled messages and tool results, not raw deltas. |
| **`provider_raw`** | An opaque JSONB slot on `AssistantMessage` events that carries provider-specific continuation state (e.g., Anthropic thinking-block signatures, `pause_turn` content) so turns can be continued byte-faithfully. |
| **`Pause`** | A non-terminal stop reason from the model-gateway indicating the provider requires the prior assistant content to be echoed back before generation continues (used by Anthropic for `pause_turn` / server tools). |
| **Compaction** | Automatic summarization of older message history when the context budget is near the threshold, triggered by the context manager and recorded as a `CompactBoundary` event. |
| **Tool-result clearing** | A `ToolResultCleared` event that supersedes a prior `ToolResult` without deleting it. The cleared result renders as a stub in the model-visible window; the full result is retained in the log. |
| **Lethal trifecta** | The threat model combining private-data access + untrusted content + external communication. Any injection that controls all three legs can exfiltrate private data. Severed in Boltrope primarily by the egress broker on every model-influenced channel. |
| **Taint gate** | An orchestrator policy control that, once untrusted content enters a session context, requires human approval for subsequent external-comms tool calls to non-allowlisted hosts. |
| **Egress broker** | An infrastructure component that enforces the per-session deny-by-default egress allowlist for all model-influenced outbound network traffic. |
| **Workspace / Runtime port** | The `RuntimePort` interface through which the tool-runtime agent code interacts with the sandbox (container in v1). The only boundary through which sandbox backends are coupled to the rest of the system. |
| **`EventLogPort`** | The consumer-defined interface in `app/agent` through which the orchestrator loop interacts with the event store. If a separate event-store service is justified in the future, only the adapter implementation changes. |
| **`Provider` interface** | The normalized LLM abstraction: `Generate`, `Stream`, `CountTokens`, `Capabilities`. The agent loop talks only to this interface; provider-specific wire formats are handled by adapter packages in the model-gateway. |
| **`ProviderError`** | The normalized error type: `{Kind, RetryAfter, Raw}`. `Kind` is one of `RateLimited`, `InvalidRequest`, `Auth`, `Overloaded`, `Server`, `Timeout`. Only `RateLimited`, `Overloaded`, and `Server` trigger the retry policy. |
| **Capability flags** | Per-`(endpoint, model)` boolean flags: `SupportsTools`, `SupportsParallelToolCalls`, `SupportsStreamingToolCalls`, `SupportsVision`, `SupportsSystemPrompt`, `SupportsThinking`, `SupportsTokenCounting`, `MaxOutputTokens`. Used to degrade gracefully rather than sending requests a backend will reject. |
| **SPIFFE/SPIRE** | The workload identity system used for mTLS between services. Each service receives a short-lived X.509 SVID; these are rotated automatically by the SPIRE agent. |
| **`projectord`** | The read-side projection worker: subscribes to the event log's xmin-bounded safe-advance cursor, runs cost-rollup and OTel-export projections, and checkpoints its advance. Not on the request path. |
| **`sandboxmgr`** | The sandbox lifecycle manager inside tool-runtime: enforces idle TTL, absolute TTL, max-live-sandboxes cap, and runs a GC reconciliation loop. |
| **Single-writer lease** | A fenced lease on the `sessions` table (`lease_owner`, `lease_epoch`, `lease_expiry`) that prevents split-brain writes. Every append checks the `lease_epoch`; a stale writer is rejected even if its `expected_seq` is current. |
| **Optimistic append** | The event-store write protocol: `UPDATE sessions SET head_seq = head_seq + 1 WHERE ... AND head_seq = :expected_seq AND lease_epoch = :lease_epoch RETURNING head_seq` followed by `INSERT INTO events`; a missed update returns `ConflictError` to the caller. |
| **xmin-bounded cursor** | The `projectord` safe-advance pattern: reads only rows where `transaction_id < pg_snapshot_xmin(pg_current_snapshot())`, preventing an out-of-order-committing transaction from being silently skipped. |
| **`bypass` mode** | An operator-only, server-side permission mode that collapses the allow/deny/ask pipeline. Forbidden under untrusted content or multi-tenant mode; audited; cannot disable egress or tenant-isolation infra controls. |
| **`depguard` / `forbidigo`** | golangci-lint linter rules that statically enforce the determinism rule (no direct `time.Now`/`rand`/`uuid.New` in domain/app), the `platform/llm` purity rule (no `gen/`/SDK imports), and the cross-service boundary rule (no service imports another's `domain`/`app`). |
| **ACI** | Agent-Computer Interface: the principle (from SWE-agent) of designing the tool surface for the model, not for human developers — tools are token-efficient, self-contained, minimally overlapping, and clearly named. |
| **MCP** | Model Context Protocol: the de-facto standard for connecting external tools and context sources to LLMs. Boltrope v1 implements an MCP client (not an MCP server). |

---

## 11. Traceability Note

Every functional and non-functional requirement in this specification is traceable to one or more of the following sources. The table below maps requirement groups to their primary sources; individual requirements note specific ADR references where the traceability is tighter.

| Requirement Group | Primary Sources |
|---|---|
| FR-LOOP | Research report §3 (MUST taxonomy); ADR-0003 v1 scope; Architecture §3 (turn sequence), §9 (concurrency/cancellation), §7 (durability/recovery) |
| FR-MODEL | Research report §5 (multi-LLM provider abstraction); ADR-0004 (multi-LLM strategy); Architecture §11 (provider abstraction across four families) |
| FR-TOOL | Research report §3 (tool registry MUST); ADR-0003 v1 scope; ADR-0005 (container isolation); Architecture §5.3 (tool-runtime layout), §8.4 (trifecta/egress), §9.2 (parallel scheduling) |
| FR-CTX | Research report §2 (context engineering); ADR-0003 v1 scope; Architecture §5.1 (context package) |
| FR-STATE | Research report §2 (event sourcing); ADR-0003 v1 scope; Architecture §6 (schema), §7 (durability/recovery) |
| FR-PERM | Research report §3 (permissions MUST); ADR-0003 v1 scope; Architecture §8.4 (trifecta), §8.13 (guardrails/bypass) |
| FR-OBS | Research report §3 (OTel SHOULD); ADR-0003 v1 scope; ADR-0006 (engineering conventions); Architecture §10.5 (metrics/SLOs) |
| FR-EXT | Research report §3 (MCP/hooks/sub-agents SHOULD); ADR-0003 v1 scope; Architecture §8.11 (MCP trust), §5.1 (hooks/subagent packages) |
| FR-API | Architecture §4 (inter-service communication/contracts) |
| NFR-REL | Research report §3 (reliability MUST); Architecture §6.3 (append), §9.4 (backpressure), §9.6 (lease), §10.4 (projectord) |
| NFR-SEC | Research report §7 (risks); ADR-0005 (sandbox); Architecture §8 (full security model) |
| NFR-PERF | Architecture §9.2 (pool), §10.3 (degradation) |
| NFR-PORT | ADR-0001 (toolchain); ADR-0005 (sandbox); Architecture §12.1 (repo layout) |
| NFR-TEST | ADR-0006 (engineering conventions); ADR-0007 (eval strategy); Architecture impact analysis §6 |
| NFR-OBS-NFR | ADR-0006; Architecture §10.5, §D9 |
| NFR-OPS | ADR-0001 (toolchain); Architecture §10.1–10.6 (operability) |
| NFR-EXT-NFR | Architecture §2.3 (in-process packages), §13 D1/D5 (rejected alternatives preserve seams) |

**ADR cross-references:**

- ADR-0001: Build and runtime toolchain (hybrid local + Docker)
- ADR-0002: License Apache-2.0
- ADR-0003: v1 scope and feature prioritization
- ADR-0004: Multi-LLM provider strategy
- ADR-0005: Container isolation behind a Workspace/Runtime abstraction
- ADR-0006: Engineering and OSS conventions
- ADR-0007: Evaluation strategy (deterministic bespoke suite, CI gate)
- ADR-0008: Project name Boltrope

---

*End of System Specification v1. Next gate: derive `proto/boltrope/v1/*.proto` and consumer-defined Go port interfaces from §7 and the architecture §4–5; write failing TDD tests seeded by the acceptance criteria in §4.*
