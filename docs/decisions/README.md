# Architecture Decision Records (ADRs)

This directory records the significant technical decisions made while building the
harness, using lightweight [Michael Nygard-style ADRs](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions).

Each decision the team (or an autonomous research agent) makes is captured here so
the *why* is never lost. ADRs are immutable once `Accepted`; to change a decision,
add a new ADR that supersedes the old one (and update the old one's `Status`).

## Format

```markdown
# NNNN. Short title of the decision

Date: YYYY-MM-DD
Status: Proposed | Accepted | Superseded by ADR-XXXX | Deprecated

## Context
The forces at play: the problem, constraints, and options considered.

## Decision
The choice we made, stated in active voice ("We will ...").

## Consequences
The resulting trade-offs — good, bad, and neutral — and any follow-up work.
```

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-build-and-runtime-toolchain.md) | Build and runtime toolchain | Accepted |
| [0002](0002-license-apache-2.0.md) | License: Apache-2.0 with DCO sign-off | Accepted |
| [0003](0003-v1-scope.md) | v1 scope and feature prioritization | Accepted |
| [0004](0004-multi-llm-provider-strategy.md) | Multi-LLM provider strategy | Accepted |
| [0005](0005-container-isolation.md) | Sandbox isolation: containers behind a Workspace abstraction | Accepted |
| [0006](0006-engineering-conventions.md) | Engineering & OSS conventions | Accepted |
| [0007](0007-eval-strategy.md) | Evaluation strategy | Accepted |
| [0008](0008-project-name-boltrope.md) | Project name: Boltrope | Accepted |
| [0009](0009-service-decomposition.md) | Service decomposition: 3 services + projectord (event store in-process) | Accepted |
| [0010](0010-inter-service-communication.md) | Inter-service communication: gRPC/protobuf, server-streaming, resumable client edge, no broker on request path | Accepted |
| [0011](0011-event-store-schema.md) | Event-store schema: optimistic + fenced lease + request_id idempotency, xmin-bounded projection cursor, tenant-scoped blobs, concrete RLS, Postgres >= 13 | Accepted |
| [0012](0012-durability-and-exactly-once.md) | Durability and at-most-once mutating tools: durable turn/tool-execution intent, tool_executions ledger, clean-workspace resume | Accepted |
| [0013](0013-security-model.md) | Security model: egress broker on all model-influenced channels + taint gate, MCP confinement, provider-native tools disabled, RLS, RPC-bound tenant tokens, constrained bypass | Accepted |
| [0014](0014-concurrency-and-cancellation.md) | Concurrency and cancellation: single-goroutine loop, gated read-only parallelism, cgroup/PID-namespace kill, fenced lease, decoupled generation | Accepted |
| [0015](0015-repository-layout.md) | Repository layout: single Go module, go.dev layout, platform/llm single source of truth, depguard/forbidigo enforcement | Accepted |
| [0016](0016-provider-abstraction.md) | Provider abstraction: per-(endpoint,model) capabilities, open stop reasons + non-terminal Pause, provider_raw opaque continuation, stateless Responses, gateway-side normalization and cost | Accepted |
| [0017](0017-operability-and-observability.md) | Operability and observability: health/readiness, startup/migration gate, RED/USE metrics + SLOs, stuck-loop detection, sandbox lifecycle | Accepted |
| [0018](0018-keyless-demo-provider-and-gate7-reconciliations.md) | Keyless demo provider is text-only; Gate-7 deploy reconciliations (.env default, egress amendment, per-run-mode deferral) | Accepted |
| [0019](0019-session-scoped-permission-mode.md) | Session-scoped permission mode persisted as `sessions.mode` (resolves ADR-0018 §4) | Accepted |
| [0020](0020-production-oidc-edge-auth.md) | Production client-edge auth: OIDC discovery + JWKS Keyfunc (dependency-light, fail-closed startup, rate-limited rotation refresh) | Accepted |
| [0021](0021-egress-data-path.md) | Egress data path: in-process hardened fetcher (DNS-pinned, SSRF-safe, redirect re-gated) for webfetch/websearch; sandbox stays `--network none` | Accepted |
| [0022](0022-mcp-server-mode.md) | MCP Server mode: expose Boltrope as a callable MCP server (Streamable HTTP, hand-rolled thin adapter, shared OIDC+RLS, call-stays-open approvals via concurrent control) | Accepted |
| [0023](0023-structured-output.md) | Structured output usable end-to-end: additive public `output_schema`/`strict` on `RunRequest` (all three facades), per-run plumbing into the loop, native `response_format` on OpenAI-Responses/Gemini/Anthropic-stable via an injected central-caps seam, loop validate-retry backstop | Accepted |
| [0024](0024-boltrope-dev-local-mode.md) | `boltrope dev` local mode: separate single-process, loopback-only, in-memory, no-exec dev binary running the real loop; build-time prod-exclusion + tested misuse fence | Accepted |
| [0025](0025-event-log-read-and-time-travel.md) | Event-log read + time-travel API: additive `ListSessionEvents`/`GetStateAtSeq` read RPCs (all three facades), seq keyset pagination + size cap, descriptor-by-default redaction (provider_raw/system-prompt never leak; AssistantMessageDelta never exposed; blobs as descriptors), time-travel via Load-then-fold (never Fork, side-effect-free), tenant-RLS scope | Accepted |
| [0026](0026-session-tenant-cost-read.md) | Session/tenant cost-read API: persistent `session_cost_events` rollup (migration 0007, idempotent by `global_id`, rebuildable, RLS like 0003), additive `GetSessionCost`/`GetTenantCost` read RPCs (all three facades) with per-model breakdown, write-side `TurnStarted.Model`⋈terminal-by-TurnID correlation in projectord (in-flight map + point-lookup recovery), per-tool DROPPED, tenant-RLS scope | Accepted |
| [0027](0027-admin-session-api.md) | Admin/tenant session-management API: additive `ListSessions`/`GetSessionUsage` read RPCs (all three facades), keyset `(created_at,id)` pagination + opaque page_token + size cap, STOP via existing Control interrupt (no new kill), `GetSessionUsage` v1 = `EVENT_FOLD`, tenant-RLS scope (no global-admin) | Accepted |
| [0029](0029-boltrope-dev-real-model-and-local-exec-opt-in.md) | `boltrope-dev` real model + Docker local-exec behind explicit default-OFF opt-in flags (`--model-url`/`--model`/`--model-api-key-env`/`--enable-native-schema`/`--enable-local-exec`); import-fence refined to exact-match the `modelgateway/app` Service while allowing the pure-data capabilities leaf; in-process tool-runtime bridge over `execute.Service` (deny-by-default egress, in-memory dedup, FS blob temp dir, per-session Docker `--network none`); `BOLTROPE_MODELGW_NATIVE_SCHEMA` prod override (amends ADR-0024) | Accepted |
| [0030](0030-long-term-memory-via-tools.md) | Long-term cross-session memory exposed ONLY as tools (`memory_write`/`memory_read`/`memory_search`; no proto/facade/RPC) over a simple RLS-protected key/value + tag/substring store (migration 0008 `agent_memory`, FORCE RLS, fail-closed on unset tenant GUC, DELETE-grant superset over 0003/0007); explicit NO vector/embeddings/RAG scope; tenant-via-ctx seam (new clean `toolruntime/infra/tenant` helper + `execute.Service` propagation, NOT importing the orchestrator helper — layering); dev in-memory vs prod pg store split driven by the `boltrope-dev` pgx import fence | Accepted |
| [0031](0031-in-loop-virtual-tools-planning-and-subagents.md) | In-loop **virtual tools** (`spawn_subagent` + `todo_write`) handled INSIDE the orchestrator loop — NOT tool-runtime tools, because the runtime has no event-log/sub-agent access (layering); advertise (appended after base defs; `spawn_subagent` gated by `Depth < MaxDepth`, `todo_write` always) / classify (inline classes WIN over runtime registry; `spawn_subagent`=mutating/serialized, `todo_write`=read-only) / gate (NOT bypassed — hooks→policy→approval; a denied spawn never calls `Spawn`) / intercept-before-`ExecuteTool`; wires the previously DEAD `deps.SubAgent.Spawn` live with `Depth+1`; new sealed `PlanUpdated` event (codec + read-plane non-redacted descriptor + time-travel fold-ignore + dev store + agentctx latest-plan re-surfacing) with full `ToolExecutionStarted`+`ToolResult` replay/idempotency parity, `PlanUpdated` appended on the serial post-dispatch path for deterministic ordering; no proto change | Accepted |
