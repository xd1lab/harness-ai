<!-- SPDX-License-Identifier: Apache-2.0 -->

# Architecture

This is a short orientation map. The authoritative, detailed documents live under
[`docs/`](docs/README.md); this page exists to point you at the right one quickly.

## The one-paragraph version

Boltrope turns a stateless LLM completion API into a stateful, tool-using agent.
The single source of truth is an **append-only, event-sourced log in PostgreSQL**;
every externally observable behaviour — session resume, fork, replay, cost
accounting, observability — is derived from that log. The deployment is **three
long-lived services** (orchestrator, model-gateway, tool-runtime) plus one
read-side projection worker (`projectord`), all over a single PostgreSQL instance.
The event store is an **in-process package inside the orchestrator**, not a
separate service. Inside each service the layering is pragmatic hexagonal — pure
`domain`, infra-agnostic `app` (use cases) over consumer-defined **ports**,
`adapter`s at the edges, and `infra` for process wiring — with dependencies
pointing strictly inward.

## Services and where their code lives

| Service | Binary (`cmd/`) | Code (`internal/`) | Responsibility |
|---|---|---|---|
| Orchestrator | `boltrope-orchestratord` | `internal/orchestrator/` | Agent loop, turns, permissions, hooks, context budget, sub-agents, and the embedded event store. The system's only "brain". |
| Model Gateway | `boltrope-modelgwd` | `internal/modelgateway/` | Stateless provider abstraction: normalize/stream/count/capabilities across Anthropic, Gemini, OpenAI, and OpenAI-compatible endpoints; provider retry + error normalization. |
| Tool Runtime | `boltrope-toolruntimed` | `internal/toolruntime/` | Trust boundary for model-influenced code: tool registry (native + MCP), JSON-Schema validation, per-session sandboxes, MCP client, deny-by-default egress. |
| Projector | `boltrope-projectord` | `internal/projector/` | Read-side worker (off the request path): cost-rollup and OTel-export projections over an xmin-bounded safe-advance cursor. |
| Migrator | `boltrope-migrate` | `migrations/` | Runs the forward-only DDL and exits — a release gate before services accept traffic. |
| Client CLI | `harnessctl` | — | Thin gRPC client/SDK for `CreateSession` / `Run` / `Control` / `Fork`. |

Cross-cutting platform code (the normalized `llm` kernel, clock/ids/secret/blob
ports, config, observability, gRPC bootstrap, JSON-Schema, pricing) lives under
`internal/platform/`. The frozen service contracts are the `.proto` files in
[`proto/boltrope/v1/`](proto/boltrope/v1) with committed stubs in `gen/`.

## Read the details

- **[docs/architecture/00-architecture.md](docs/architecture/00-architecture.md)** — the full v1 architecture (Gate 3, final). Section map:
  1. System context overview
  2. Service decomposition (why three services, why the event store is in-process)
  3. One agent turn — component & sequence sketch
  4. Inter-service communication & contracts (gRPC/protobuf, streaming patterns, retry/idempotency)
  5. Per-service clean-architecture layout
  6. PostgreSQL event-store schema (the DDL, RLS, optimistic + fenced + idempotent append)
  7. Durability, recovery & exactly-once side effects
  8. Security model (mTLS/SPIFFE, RLS, egress broker, taint gate, edge auth)
  9. Concurrency & cancellation model
  10. Operability: health, startup/migration gate, RED/USE metrics, lifecycle
  11. Provider abstraction & streaming across four families
  12. Repository layout
  13–14. Decisions & deferred open questions
- **[docs/spec/00-system-specification.md](docs/spec/00-system-specification.md)** — functional + non-functional requirements, the multi-LLM support matrix, and the v1 definition of done.
- **[docs/architecture/01-impact-analysis.md](docs/architecture/01-impact-analysis.md)** — impact analysis of the accepted design.
- **[docs/architecture/02-implementation-plan.md](docs/architecture/02-implementation-plan.md)** — the dependency-ordered, test-first implementation plan.

## Architecture Decision Records

Every significant decision is recorded as an immutable, Nygard-style ADR under
[`docs/decisions/`](docs/decisions/) (see the [ADR index](docs/decisions/README.md)):

| ADR | Decision |
|---|---|
| [0001](docs/decisions/0001-build-and-runtime-toolchain.md) | Build & runtime toolchain |
| [0002](docs/decisions/0002-license-apache-2.0.md) | License: Apache-2.0 with DCO sign-off |
| [0003](docs/decisions/0003-v1-scope.md) | v1 scope & feature prioritization |
| [0004](docs/decisions/0004-multi-llm-provider-strategy.md) | Multi-LLM provider strategy |
| [0005](docs/decisions/0005-container-isolation.md) | Sandbox isolation: containers behind a Workspace abstraction |
| [0006](docs/decisions/0006-engineering-conventions.md) | Engineering & OSS conventions |
| [0007](docs/decisions/0007-eval-strategy.md) | Evaluation strategy |
| [0008](docs/decisions/0008-project-name-boltrope.md) | Project name: Boltrope |
| [0009](docs/decisions/0009-service-decomposition.md) | Service decomposition (3 services + projectord) |
| [0010](docs/decisions/0010-inter-service-communication.md) | Inter-service communication: gRPC/protobuf |
| [0011](docs/decisions/0011-event-store-schema.md) | Event-store schema |
| [0012](docs/decisions/0012-durability-and-exactly-once.md) | Durability & at-most-once mutating tools |
| [0013](docs/decisions/0013-security-model.md) | Security model |
| [0014](docs/decisions/0014-concurrency-and-cancellation.md) | Concurrency & cancellation |
| [0015](docs/decisions/0015-repository-layout.md) | Repository layout |
| [0016](docs/decisions/0016-provider-abstraction.md) | Provider abstraction |
| [0017](docs/decisions/0017-operability-and-observability.md) | Operability & observability |

To change a decision, add a new ADR that supersedes the old one (ADRs are
immutable once `Accepted`).
