# 15. Repository layout: single Go module, go.dev layout, platform/llm single source of truth, depguard/forbidigo enforcement

Date: 2026-06-10
Status: Accepted

## Context

The project hosts three services plus a projection worker that share a normalized
message/tool/stop-reason model, generated protobuf stubs, and platform bootstrap code.
Layout decisions affect how easily cross-service refactors propagate, whether the
normalized model drifts across representations, and whether the determinism rule
(no direct time.Now/rand/uuid.New in domain/app) can be mechanically enforced.

The draft maintained the normalized message/tool/stop-reason union in three places:
the in-process `platform/llm` package, the protobuf wire types, and a hand-mirrored
copy in the orchestrator domain. Tripling the drift surface of the exact types the
multi-LLM abstraction exists to keep consistent is counterproductive.

The golang-standards/project-layout `pkg/` convention is not the official Go layout
and is not universally accepted; the official go.dev/doc/modules/layout guidance is
the reference.

## Decision

**Single Go module** (`module github.com/xd1lab/harness-ai`). Services that share
normalized llm types, generated protobuf stubs, and platform bootstrap are simplest to
version and refactor atomically in one module. Multi-module `go.work`/replace friction
provides no benefit at this size.

**Official go.dev layout.** Entrypoints are `cmd/<app>/main.go` (wiring only, no
business logic). Private code lives in `internal/`. No `pkg/`, `api/`, or `configs/`
scaffolding is created until a concrete need exists.

**Generated protobuf code is committed** in `gen/` so `go build` and `go test` work
without a codegen step. buf generates the code via `make proto`; CI checks that `gen/`
is up to date. buf also runs lint and breaking-change detection on every PR.

**`internal/platform/llm` is the single in-process source of truth** for the normalized
message/tool/stop-reason/usage model and the Provider/StreamReader interfaces. Each
service's domain and app layers import it directly. The orchestrator domain does not
re-declare or mirror the types. Generated protobuf types live in `gen/` and are strictly
separate; the gateway and orchestrator adapters map `gen/` to and from `llm` at the
transport edge only. A depguard rule asserts that `platform/llm` imports nothing from
`gen/` or any provider SDK, keeping it a pure, dependency-free shared contract.

**Depguard and forbidigo rules** in `.golangci.yml` enforce two cross-cutting
constraints mechanically:

1. No domain or app code calls `time.Now()`, `rand.*`, or `uuid.New()` directly. Every
   component that sleeps, times out, expires state, or generates IDs takes an injected
   Clock, jitter/rand source, or IDGenerator through its ports.go. This makes
   backoff schedules, dedup windows, and lease TTLs deterministically assertable in
   tests.

2. No service imports another service's domain or app packages. Services communicate
   only via the generated gRPC stubs in `gen/`. The event store is reached only through
   EventLogPort within the orchestrator.

**Top-level tree** (abbreviated):

```
boltrope/
├── cmd/                        # entrypoints: orchestratord, modelgwd, toolruntimed,
│                               #   projectord, ctl, migrate
├── proto/boltrope/v1/          # common, orchestrator, model_gateway, tool_runtime
│                               #   NO event_store.proto (in-process)
├── gen/boltrope/v1/            # GENERATED, committed
├── internal/
│   ├── orchestrator/           # domain/ app/ adapter/ infra/; includes adapter/outbound/eventstore
│   ├── modelgateway/
│   ├── toolruntime/
│   ├── projector/
│   └── platform/
│       ├── grpcx/              # mTLS/SPIFFE, interceptors, RBAC
│       ├── obs/                # OTel bootstrap, RED/USE metrics, slog
│       ├── config/             # koanf loader
│       └── llm/                # PURE: zero infra deps, no gen/ or SDK imports
├── migrations/                 # golang-migrate embedded SQL, expand/contract
├── test/
│   ├── integration/            # //go:build integration, testcontainers
│   └── eval/                   # deterministic bespoke eval harness (ADR-0007)
└── docs/decisions/             # ADRs 0001..NNNN
```

## Consequences

- Atomic cross-service refactors: a type rename in `platform/llm` is a single-module
  change that the compiler checks in one pass.
- Eliminating the hand-mirrored domain copy collapses three representations of the
  normalized model to two (llm + proto wire) with a tested mapping at one seam.
- The depguard purity rule on `platform/llm` is machine-enforced: proto/SDK drift into
  the shared contract is a CI failure, not a review-time catch.
- The forbidigo no-direct-time.Now rule makes every component that depends on time
  deterministically testable; backoff schedules, dedup windows, lease TTLs, and retry
  intervals are all assertable without sleeps.
- The cross-service import depguard rule makes the service boundary real, not
  aspirational: an accidental orchestrator import of tool-runtime's domain fails CI.
- Committed gen/ means contributors can build and test without running buf or any
  codegen toolchain; CI re-generates and diffs to detect stale stubs.
