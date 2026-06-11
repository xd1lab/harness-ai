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
