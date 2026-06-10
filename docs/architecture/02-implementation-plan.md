# Boltrope — v1 Implementation Task Split (Test-First Work Breakdown)

> **Status:** Gate 4 — implementation plan. Drives test-first (TDD) implementation.
> **Date:** 2026-06-10
> **Project:** Boltrope (`github.com/xd1lab/harness-ai`) — provider-portable, event-sourced AI agent harness.
> **Inputs (frozen):** `docs/spec/00-system-specification.md` (41 FRs + 12 DoD); `docs/architecture/00-architecture.md` (§§1–14); ADRs 0001–0017; the frozen contracts — `proto/boltrope/v1/*.proto` + committed `gen/`, `internal/platform/llm` (the normalized kernel), `internal/platform/{clock,ids,secret,blob}` (cross-service ports), `internal/orchestrator/{domain,app/ports.go,policy}`, `internal/toolruntime/{domain,app/ports.go}`.
> **Audience:** Engineers writing failing tests then code, and the gating reviewer.

---

## 0. How to read this document

This is a **dependency-ordered Work Breakdown Structure** organized into **waves** that maximize safe parallelism without integration conflict. Each task carries:

- a **stable id** (`T-<AREA>-NN`) referenced by `dependsOn`;
- the **component/package** it touches (one owner package per task → no two parallel tasks edit the same file);
- the **FR(s)** it satisfies (traceable to the spec);
- **dependsOn** — task ids that must merge first;
- a **tests-first** note: the failing test(s) to write *before* the code, per TDD (ADR-0006, ADR-0007; the AC items in spec §4 are the seed).

**The contract surface is already frozen** (Gate 4 input). Every `domain` type, every consumer-defined port (`EventLogPort`, `ModelGatewayPort`, `ToolRuntimePort`, `ApprovalGate`, `HookRunner`, `ToolRegistry`, `RuntimePort`, `Workspace`, `EgressBroker`, `MCPClientPort`, `DedupStore`, `PolicyEngine`), every platform port (`clock.Clock`, `ids.IDGenerator`, `secret.{SecretsPort,Secret,Redactor}`, `blob.BlobStorePort`, `llm.Provider`, `llm.StreamReader`), and all four `.proto` files with committed `gen/` exist. **This plan implements behind those interfaces; it does not redesign them.** A task that needs a contract change is out of scope and must escalate, not edit silently.

**The TDD ratchet.** The architecture's closing note names the exact first tests: the agent loop against fakes, the pure stream `assembler` with adversarial deltas, the optimistic+fenced+idempotent append + RLS-predicate-removed test (testcontainers), durable tool-execution recovery, the read-only/mutating scheduler, the adversarial sandbox-kill tests. Those are scheduled as early as their dependencies allow so **the loop is provable against fakes before any real provider/sandbox exists** (Wave 3).

**Determinism rule (applies to every task).** No `domain/`/`app/` code calls `time.Now`, `rand.*`, or `uuid.New` directly — inject `clock.Clock` / a rand source / `ids.IDGenerator` (NFR-TEST-01). The `forbidigo`/`depguard` rules are wired in Wave 0 (`T-PLAT-01`) so violations fail CI from the first PR. `internal/platform/llm` must import nothing from `gen/` or any SDK (DOD-08); no service may import another service's `domain`/`app` (DOD-08, architecture §12.4).

**Test build tags.** Unit tests (no network/Docker) must finish < 60 s and run by default (NFR-TEST-02). Integration tests are `//go:build integration` and run separately on a Docker job. Eval scenarios (`test/eval`) are network-free and a required CI gate (NFR-TEST-04, DOD-03).

---

## 1. Parallelization waves (summary)

| Wave | Theme | Parallel tracks | Gate exit |
|---|---|---|---|
| **0** | Repo/CI/lint/proto/test-harness foundation | toolchain+lint+proto, fakes/testkit | `make lint test` green on empty tree; `buf lint`/`buf breaking` wired; depguard/forbidigo active; in-repo fakes for every port compile. |
| **1** | Platform concretes + fixtures (no business logic) | clock·ids · secret/config · obs · grpcx · blob · jsonschema · cost-pricing | every platform impl unit-tested; `slog` redaction proven; koanf precedence proven; gRPC mTLS/interceptors bufconn-tested. |
| **2** | Event store (pgx) + migrations + RLS | migrations · Store.Append/Load/Fork/Subscribe · recovery-fold | testcontainers: optimistic/fenced/idempotent append, contiguity, RLS-predicate-removed, fork, blob-in-tx, subscribe cursor — all green. |
| **3** | **Agent loop + assembler against FAKES** (the keystone) | assembler · context/compaction · policy-engine impl · hooks · loop · subagent · recovery-adjudication | loop golden-log shape, termination subtypes, parallel/serial scheduling, compaction, permissions, hooks, sub-agent depth — all unit-tested with fakes; **no real provider/sandbox/db needed**. |
| **4** | Model-gateway (provider adapters + normalize + retry + cost) | retry · 3 normalizers · anthropic · gemini · openai · openaicompat · capabilities · gateway gRPC server | each adapter fixture-driven; stream normalizers golden; retry deterministic; gateway bufconn-tested; orchestrator `outbound/modelgw` adapter maps gen⇄llm. |
| **5** | Tool-runtime (registry + tools + sandbox + egress + MCP + dedup) | registry/jsonschema · native tools · container runtime · cgroup-kill · egress broker · MCP client · dedup pg · execute service · scheduler · tool-runtime gRPC server | tool validation, registry merge, dedup ledger, **adversarial sandbox kills**, egress deny + exfil-gate, lazy MCP, SVID isolation — green (unit + integration). |
| **6** | Orchestrator transport + wiring; projectord; cmd binaries | orchestrator gRPC `Run`/`Control`/`Fork` + edge auth + REST · outbound adapters wiring · projectord (cursor+cost+otel+sweeper) · 5 cmd mains | resumable `Run`, `Control` ownership, edge JWT (alg=none reject), REST parity, projectord safe-advance + lag metric, `/readyz` dependency-gated. |
| **7** | Eval harness + ops + end-to-end + DoD closeout | eval scenarios · docker-compose+migrate gate · adversarial integration suite assembly · live smoke · CLI polish · OSS/CI gates · coverage/scorecard | DOD-01..12: eval gate ≥5 scenarios, compose-up < 120 s, NFR-TEST-05 adversarial suite, coverage ≥75%, lint 0, buf gates, scorecard ≥5.0. |

**Critical path (longest serial chain):** `T-PLAT-01` (repo/lint/proto) → `T-PLAT-02` (clock/ids) → `T-EVT-02` (migrations) → `T-EVT-03` (`Store.Append` optimistic/fenced/idempotent) → `T-LOOP-05` (the loop) → `T-ORCH-01` (orchestrator gRPC `Run`) → `T-OPS-01` (docker-compose gate) → `T-EVAL-02` (eval CI gate) → `T-DONE-01` (DoD closeout). Everything else parallelizes around this spine.

### Strictly serial tasks (do not parallelize these against their dependents)

1. **`T-PLAT-01` is the universal prerequisite.** It establishes `go.mod` deps, the Makefile, `.golangci.yml` (with `depguard`/`forbidigo`), the `buf` lint/breaking wiring, and the CI skeleton. Nothing else can be verified green until it lands.
2. **Event-store chain is serial within itself:** `T-EVT-02` (migrations DDL) → `T-EVT-03` (append transaction) → `T-EVT-04` (load/fork/subscribe) → `T-EVT-05` (recovery fold). Each builds on the prior's schema/Store surface and they edit the same `eventstore` package and `migrations/`.
3. **The loop core is serial:** `T-LOOP-05` (loop) depends on `T-LOOP-01`(assembler) + `T-LOOP-02`(context) + `T-LOOP-03`(policy impl) + `T-LOOP-04`(hooks) all landing first; `T-LOOP-06` (sub-agent) and `T-LOOP-07` (recovery adjudication) depend on `T-LOOP-05`. The loop is one package (`app/agent`) so its sub-parts are sequenced, not parallel-in-file.
4. **Each gRPC server adapter is serial after its service's use-case** (`T-MGW-09` after gateway use-cases; `T-TR-10` after execute service; `T-ORCH-01` after the loop). Transport maps to an already-tested core.
5. **`T-OPS-01` (compose ordering: Postgres healthy → migrate exits 0 → services)** must land after all five `cmd/` mains compile (`T-CMD-*`) and gates the end-to-end DoD jobs.
6. **`T-DONE-01` (DoD closeout: coverage ≥75%, lint 0, scorecard, badges)** is by construction last.

Within a wave, the listed tracks have **no shared files** and merge in any order.

---

## 2. Wave 0 — Foundation (repo, CI, proto gates, test kit)

> Goal: a buildable, lintable, CI-gated empty tree with in-repo fakes for every frozen port, so all later TDD is red-green against a green baseline. **The contracts and `gen/` already exist** — this wave wires the enforcement and the test scaffolding around them.

### T-FND-01 — Toolchain, Makefile, lint config, CI skeleton, buf gates
- **Component:** repo root (`Makefile`, `.golangci.yml`, `.github/workflows/*`, `buf.yaml`/`buf.gen.yaml` verification, `go.mod` tool directives)
- **FRs:** DOD-07, DOD-08, DOD-11; NFR-TEST-02, NFR-OPS-04 (toolchain), NFR-PORT-04 (release wiring stub)
- **dependsOn:** —
- **Tests first:** a CI smoke that asserts `golangci-lint run` exits 0 on the existing tree; a `buf lint` + `buf breaking` (against `main`) job that must pass on the committed protos; a `go test ./...` job that is green (no tests yet) and a `-race` variant; a unit test asserting the `depguard` config *would* reject an `import "github.com/xd1lab/harness-ai/gen/..."` from `internal/platform/llm` and a `forbidigo` rule rejecting `time.Now()` in `domain`/`app` (table-driven golden against a tiny fixture file under `testdata/`).

### T-FND-02 — In-repo fakes/testkit for every consumer-defined + platform port
- **Component:** `internal/<svc>/app/apptest/` (fakes live next to the ports that define them) + `internal/platform/<p>/<p>test/`
- **FRs:** NFR-TEST-01, NFR-TEST-04 (seeds the eval fake-provider), and underpins every loop/gateway/runtime unit test
- **dependsOn:** T-FND-01
- **Tests first:** compile-time `var _ Port = (*Fake…)(nil)` assertions for **every** port — `EventLogPort`, `ModelGatewayPort`, `ToolRuntimePort`, `ApprovalGate`, `HookRunner`, `PolicyEngine`, `ToolRegistry`, `RuntimePort`/`Workspace`, `EgressBroker`, `MCPClientPort`, `DedupStore`, `BlobStorePort`, `SecretsPort`, `Redactor`, `llm.Provider`, `llm.StreamReader`, `clock.Clock`/`Timer`, `ids.IDGenerator`; plus a **fake clock** (controllable `Now`/`After`/`NewTimer`, virtual time advance) and **fake IDGenerator** (scripted sequence) unit-tested for determinism (a test asserts `After(5s)` does not fire until virtual time advances ≥5 s). This task delivers the scripted-`StreamReader` and scripted-`Provider` the loop tests and eval harness consume.

---

## 3. Wave 1 — Platform concretes & fixtures (no business logic)

> Goal: real implementations of the cross-cutting platform ports + shared fixtures, each independently unit-testable. **No shared files between tracks** — fully parallel.

### T-PLAT-02 — Real `clock.System` + `ids.System` (UUIDv7) wiring
- **Component:** `internal/platform/clock` (System body exists), `internal/platform/ids` (System currently panics — implement infra UUID minting in `infra`/wiring)
- **FRs:** NFR-TEST-01; supports FR-LOOP-05, FR-STATE-01 (request_id), FR-TOOL-03 (note: tool idem key is *derived*, not minted here)
- **dependsOn:** T-FND-01
- **Tests first:** `ids` test asserts `NewID`/`NewSessionID`/`NewRequestID` return distinct, non-empty, parseable UUIDs across 10k calls (collision-free); a test asserts `ids.System` (contract stub) panics until wired, and the infra generator satisfies `ids.IDGenerator`; `clock.System.After`/`NewTimer` fire (real, short durations, fast).

### T-PLAT-03 — Config loader (koanf, flags>env>file>defaults, fail-fast)
- **Component:** `internal/platform/config`
- **FRs:** NFR-OPS-04; supports FR-OBS-05 (DSN), NFR-PORT-03 (PG≥13 validation), NFR-SEC-01 (`BOLTROPE_DEV_INSECURE`)
- **dependsOn:** T-FND-01
- **Tests first:** table-driven precedence test (a flag overrides env overrides file overrides default for the same key); a missing-required-field test asserts a non-zero, human-readable validation error; a test asserts a configured PostgreSQL version `< 13` is rejected at validate time (NFR-PORT-03).

### T-PLAT-04 — Observability bootstrap: slog JSON + LogValuer redaction + OTel meter/tracer + RED/USE helpers
- **Component:** `internal/platform/obs`; `internal/platform/secret` (Secret type exists; add the registry `Redactor` impl + a `SecretsPort` env backend)
- **FRs:** FR-OBS-01, FR-OBS-02, FR-OBS-03; NFR-OBS-01/02/03; NFR-SEC-05
- **dependsOn:** T-FND-01
- **Tests first:** FR-OBS-03 AC-1 — log a struct containing a `secret.Secret`/provider key via `slog.Info` and assert JSON output contains `[REDACTED]`, never the value; a test asserts `trace_id`/`span_id` from an active span are injected into the log record (NFR-OBS-02); a RED-metrics helper test asserts a labeled counter (`run_errors_total{subtype=...}`) increments and renders in Prometheus text format (seeds FR-OBS-02); an env `SecretsPort.Get` test returns `ErrNotFound` for an unset name.

### T-PLAT-05 — gRPC bootstrap: mTLS (SPIFFE + fail-closed dev fallback) + interceptor chain
- **Component:** `internal/platform/grpcx`
- **FRs:** NFR-SEC-01 (mTLS, dev fallback), FR-OBS-01 (otel propagation), FR-OBS-05 (health), FR-API-03 partial (interceptor seam); NFR-TEST-05(g)
- **dependsOn:** T-PLAT-04
- **Tests first:** NFR-TEST-05(g) — a test asserts the static-cert provider refuses to start unless `BOLTROPE_DEV_INSECURE=1` (and logs a loud warning); a bufconn test asserts the otel + logging + recovery + auth interceptor chain runs in order and propagates trace context across a call (FR-OBS-01 AC-2 propagation seam); a peer-SPIFFE-ID RBAC interceptor test asserts a disallowed caller→RPC pair is rejected (architecture §8.1 verb gate).

### T-PLAT-06 — Blob store backend (filesystem default behind `BlobStorePort`) + tenant-prefix enforcement
- **Component:** `internal/platform/blob` (port exists) + `adapter`/infra fs backend
- **FRs:** FR-STATE-05 (the port side), NFR-SEC-03; NFR-TEST-05(b) seam
- **dependsOn:** T-FND-01
- **Tests first:** `Put`→`Get`→`Stat` round-trips bytes and `SizeBytes`; `Put` over the max returns `ErrTooLarge`; a **tenant-prefix** test asserts a `Ref{TenantID:A}` can never address tenant B's bytes (path is tenant-prefixed) and an absent ref returns `ErrNotFound` indistinguishably from a wrong-tenant ref (no existence oracle, NFR-SEC-03); idempotent re-`Put` of the same `(tenant,ref)`.

### T-PLAT-07 — JSON-Schema validator (shared) + model-pricing/cost table
- **Component:** `internal/platform/jsonschema` (new, pure) and `internal/platform/pricing` (new, pure) — both dependency-light, no `gen/`
- **FRs:** FR-TOOL-01 (validator), FR-MODEL-05/§11.6 cost (pricing); FR-LOOP-02 (cost on TurnFinished)
- **dependsOn:** T-FND-01
- **Tests first:** validator table tests — missing required field → error; extra field under `additionalProperties:false` → error; extra field otherwise → ok (FR-TOOL-01 AC-1/AC-2); pricing test computes `cost_usd` from a `llm.Usage` (with cache-read/write split) for a known `(model)` and asserts the gateway-side number, and returns a typed "unknown model" error rather than guessing.

---

## 4. Wave 2 — Event store (the durable spine)

> Goal: the in-process pgx `Store` satisfying `EventLogPort`, the migrations, and concrete RLS. **Serial within the wave** (shared package + schema). All tests are testcontainers `//go:build integration` except the recovery-fold folding logic (pure, unit).

### T-EVT-01 — `golang-migrate` runner (`cmd/boltrope-migrate` core) + embedded SQL plumbing
- **Component:** `internal/orchestrator/infra/db` (migration gate check) + `migrations/` package wiring + the migrate command's library half
- **FRs:** NFR-OPS-01, NFR-PORT-03, DOD-12 (runner half)
- **dependsOn:** T-PLAT-03
- **Tests first:** testcontainers test runs the (initially empty) migration set to completion and exits 0; a forward-only guard test asserts a destructive `down` on `events`/`sessions` is rejected by the CI lint check (NFR-OPS-01); a PG-version gate test asserts startup fails on PG < 13.

### T-EVT-02 — Schema DDL migrations (tenants, sessions, events, snapshots, subscriptions, blobs, tool_executions) + indexes + RLS policies
- **Component:** `migrations/*.sql`
- **FRs:** FR-STATE-01/04/05, FR-TOOL-04 (ledger table), NFR-SEC-02; DOD-12
- **dependsOn:** T-EVT-01
- **Tests first:** DOD-12 testcontainers migration test asserts presence + type/constraint of `sessions.lease_epoch`, `events.request_id`, `events.provider_raw`, `events.transaction_id (xid8)`, `blobs (tenant_id, ref)` composite PK, `uq_events_session_seq`, `uq_events_session_request`, and that `FORCE ROW LEVEL SECURITY` + INSERT/UPDATE/SELECT policies exist on all six tenant-scoped tables (architecture §6.2, §6.7).

### T-EVT-03 — `Store.Append`: optimistic + fenced + idempotent + contiguity transaction
- **Component:** `internal/orchestrator/adapter/outbound/eventstore` (`Append`, `ConflictError`/`FencedError`/`SessionNotActiveError` sentinels)
- **FRs:** FR-STATE-01, NFR-REL-01/03; DOD-01
- **dependsOn:** T-EVT-02
- **Tests first:** FR-STATE-01 AC-1 — N goroutines append with `expected_seq=0`; exactly one COMMIT wins, N−1 return `ConflictError` (via `errors.Is`), winner has `seq=1`, `head_seq=1` (run under `-race`); AC-2 — re-send same `request_id`, assert the original envelope returns as success, not a conflict, no duplicate row; AC-3 — stale `lease_epoch` returns `FencedError` even when `expected_seq` is current; an append to a `finished` session returns `SessionNotActiveError`.

### T-EVT-04 — `Store.Load` / `Fork` / `Subscribe` / `LoadSession` + blob-in-same-tx append path + RLS predicate-removed proof
- **Component:** same `eventstore` package
- **FRs:** FR-STATE-02/03/04/05, FR-API-01 (subscribe backs resumable Run); NFR-SEC-02; NFR-TEST-05(a,c)
- **dependsOn:** T-EVT-03, T-PLAT-06
- **Tests first:** FR-STATE-04 AC-1 / NFR-TEST-05(a) — set `app.current_tenant=A`, insert 10 A + 10 B rows, `SELECT * FROM events` **with the `WHERE tenant_id=` predicate removed** via the non-owner role returns exactly 10 (A's); FR-STATE-03 AC-1 — fork at `at_seq=5`, append to parent→`seq=7` and child→`seq=6`, assert no collision and independent loads; FR-STATE-03 AC-2 / NFR-TEST-05(c) — fork of a foreign-tenant session returns `PERMISSION_DENIED`; FR-STATE-05 AC-1/AC-2 — 64 KiB output → event has non-null `blob_ref`, blobs row in the same committed tx; bytes-written-then-tx-fail leaves no event and no dangling ref; `Subscribe(fromSeq=N)` delivers only `seq>N` then tails live and closes on ctx cancel.

### T-EVT-05 — Recovery fold: open-turn / open-tool-execution adjudication (pure logic)
- **Component:** `internal/orchestrator/app/recovery`
- **FRs:** FR-LOOP-05, FR-STATE-02, FR-TOOL-03; DOD-01
- **dependsOn:** T-EVT-04, T-FND-02
- **Tests first:** FR-LOOP-05 AC-2 / FR-STATE-02 AC-1 — a folded log truncated after `TurnStarted` (no `AssistantMessage`) classifies the turn **open** and invokes the `TurnAbortedCallback` (never silent replay); FR-TOOL-03 AC-1 — a `ToolExecutionStarted` with no terminal `ToolResult` for a **Mutating** tool classifies the execution **unknown** and does *not* mark it re-dispatchable; the recovered cost total equals the partial `usage_so_far` from the last `AssistantMessageDelta`, not zero (FR-LOOP-05 AC-1). All folding is pure → unit test with hand-built `[]EventEnvelope`, no DB.

---

## 5. Wave 3 — Agent loop + assembler against fakes (the keystone)

> Goal: **the whole brain, provable with fakes** — no real provider, sandbox, or DB. This is the architecture's named first TDD target. All tasks are unit tests with the Wave-0 fakes + fake clock/ids. `app/agent`, `app/context`, `app/hooks`, `app/subagent`, `policy` impl are distinct packages → the tracks parallelize except the loop's own internal sequencing.

### T-LOOP-01 — Pure stream `assembler` (delta → Message) over `llm.StreamReader`
- **Component:** `internal/orchestrator/app/agent/assembler.go`
- **FRs:** FR-MODEL-02 (assembly-in-loop boundary), FR-MODEL-04 (Pause); DOD-08 (zero `gen/` imports)
- **dependsOn:** T-FND-02
- **Tests first:** FR-MODEL-02 — feed a hand-written fake `StreamReader` adversarial sequences (split mid-UTF-8 `TextDelta`, out-of-order `ToolCallDelta` CallIDs, `Pause`-before-`Done`, duplicate `Done`) and assert the assembled `llm.Message` + terminal outcome (final / needs-tool-execution / needs-continuation) match golden; a `depguard` assertion that `assembler.go` imports no `gen/` or SDK (FR-MODEL-02 AC-3, DOD-08).

### T-LOOP-02 — Context manager: token accounting, compaction trigger, tool-result clearing, cache-prefix marking
- **Component:** `internal/orchestrator/app/context`
- **FRs:** FR-CTX-01/02/03; NFR-SEC tenant-scoped cache (architecture §8.10)
- **dependsOn:** T-FND-02, T-PLAT-07
- **Tests first:** FR-CTX-01 AC-1 — fake `TokenCounter` returning rising values crosses the threshold → exactly one `CompactionPerformed` is emitted and the next built window is reduced; FR-CTX-02 AC-1 — after a `ToolResultCleared`, the built window renders a **stub** for that result (golden) while the full result stays in the log; AC-2 — clearing a non-`ToolResult` → `FAILED_PRECONDITION`, clearing twice is a no-op; FR-CTX-03 AC-1/AC-2 — cache-prefix builder marks system prompt + tool defs cacheable, never session history, and two tenants never share a prefix carrying private content.

### T-LOOP-03 — `PolicyEngine` implementation (deny→mode→allow→ask + taint escalation)
- **Component:** `internal/orchestrator/policy` (impl behind the frozen `PolicyEngine` interface)
- **FRs:** FR-PERM-01/02/03; NFR-SEC-06 (bypass constraints)
- **dependsOn:** T-FND-02
- **Tests first:** FR-PERM-01 AC-1 — `deny bash` + `allow-all` → `Deny`, no ask (deny wins unconditionally); AC-2 — `plan` mode, no allow for `edit` → `Ask`; FR-PERM-02 AC-2 — `bypass` with taint present → `PolicyError` (not set); FR-PERM-03 AC-1/AC-2 — external-comms tool to non-allowlisted host → `Ask` whether or not tainted; taint escalates an otherwise-`Allow` external call to `Ask`. Pure `Evaluate` → table-driven, no I/O.

### T-LOOP-04 — Hooks pipeline (`PreToolUse`/`PostToolUse`/`Stop`/`PreCompact`) over `HookRunner`
- **Component:** `internal/orchestrator/app/hooks`
- **FRs:** FR-EXT-03; FR-CTX-01 AC-2 (`PreCompact`)
- **dependsOn:** T-FND-02
- **Tests first:** FR-EXT-03 AC-1 — a `PreToolUse` fake returning `Block` prevents dispatch and the loop appends `PermissionDecided{deny, reason=hook_blocked}` (asserted in `T-LOOP-05`); AC-2 — `PostToolUse` receives name/input/observation in structured payload (fake `HookRunner` arg capture); `PreCompact` receives the pre-compaction context (verified against the context manager fake).

### T-LOOP-05 — The agent loop: gather→act→verify, turns, termination subtypes, parallel/serial scheduler, budget caps, doom-loop, checkpoints
- **Component:** `internal/orchestrator/app/agent/loop.go`
- **FRs:** FR-LOOP-01/02/03/04/05, FR-CTX-01 (trigger), FR-PERM-04 (approval persistence), FR-EXT-03 (hook block), FR-OBS-04 (doom-loop), FR-MODEL-04 (Pause re-issue); DOD-01
- **dependsOn:** T-LOOP-01, T-LOOP-02, T-LOOP-03, T-LOOP-04, T-EVT-05
- **Tests first (the golden-log battery):** FR-LOOP-01 AC-1 — fake `ModelPort` emitting one tool call then text-only terminates `success` with the exact event sequence `[MessageAppended, TurnStarted, AssistantMessage(tool_call), ToolExecutionStarted, ToolResult, AssistantMessage(text), TurnFinished]` (golden shape); FR-LOOP-01 AC-2 / FR-LOOP-02 AC-3 — never-text-only fake → `error_max_turns`, and `run_errors_total{subtype=error_max_turns}` increments; FR-LOOP-02 AC-1 — cost over budget → `error_max_budget_usd` before the next `Generate`; AC-2 — exhausted retry on `Server` error → `error_during_execution`; FR-LOOP-04 AC-1 — three ReadOnly calls dispatch concurrently (fake `ToolPort` parallel timestamps), results fed back in one request; AC-2 — two Mutating calls serialize in emission order; AC-3 — `webfetch`/`websearch` go through the policy/egress gate, not the read-only pool; FR-LOOP-05 — `TurnStarted` precedes `Generate`, periodic `AssistantMessageDelta` checkpoints emitted; FR-PERM-04 AC-1 — `ApprovalRequested→ApprovalGranted` ordering via fake `ApprovalGate`, tool dispatched only after grant; FR-MODEL-04 AC-1 — `Pause` then `Done` → exactly two `Generate` calls, one `TurnFinished`; FR-OBS-04 AC-1 — same tool call ×5 → `doom_loop_detected_total` increments + structured log. (Fake clock makes retry/relay timing deterministic.)

### T-LOOP-06 — Depth-limited sub-agents as ordinary tools
- **Component:** `internal/orchestrator/app/subagent`
- **FRs:** FR-EXT-04
- **dependsOn:** T-LOOP-05
- **Tests first:** FR-EXT-04 AC-1 — a sub-agent tool at depth 1 creates its own session event log and returns a condensed `ToolResult` to the parent; AC-2 — spawn at max depth → `Observation{isError:true, content:"max sub-agent depth exceeded"}`, no new session. Uses the same loop + fakes in a child goroutine/session.

### T-LOOP-07 — Interrupt + cooperative cancellation wiring (loop side)
- **Component:** `internal/orchestrator/app/agent` (cancel plumbing) + `recovery` adjudication of interrupt
- **FRs:** FR-LOOP-03; NFR-TEST-05 (resumability seam)
- **dependsOn:** T-LOOP-05, T-EVT-05
- **Tests first:** FR-LOOP-03 AC-1 — deliver interrupt (loop-context cancel via the control port) mid-stream; the loop goroutine exits and appends `TurnAborted` with non-zero `usage_so_far`; AC-2 — the interrupted session passes recovery open-turn adjudication and a subsequent `Run` on the same id proceeds. (Integration-resume variant deferred to `T-ORCH-*`/Wave 6 with real DB.)

---

## 6. Wave 4 — Model gateway (providers, normalization, retry, cost)

> Goal: the stateless provider abstraction. Each adapter + each stream normalizer is an independent track (separate files) → highly parallel. Retry and capabilities are pure-logic tracks. The gateway gRPC server and the orchestrator's `outbound/modelgw` client adapter close the boundary.

### T-MGW-01 — Harness retry policy (`Retry-After`→backoff+jitter, kind-gated) over injected Clock+Jitter
- **Component:** `internal/modelgateway/app/generate/retry.go`
- **FRs:** FR-MODEL-05; NFR-REL-02
- **dependsOn:** T-PLAT-02
- **Tests first:** FR-MODEL-05 AC-1 — two 429s with `Retry-After: 5` then 200, injected fake `Clock`+`Jitter`: first wait is exactly 5 s (mocked), second applies backoff, total attempts = 3; AC-2 — a 400 `ErrInvalidRequest` → propagated with zero retries (`Clock` sleep never called); `ErrAuth`/`ErrUnsupported`/`ErrTimeout` retry behavior matches `ProviderError.Retryable()`.

### T-MGW-02 — Capabilities resolver (per-`(endpoint,model)` table + per-endpoint overrides)
- **Component:** `internal/modelgateway/domain/capabilities` + `infra/config` table
- **FRs:** FR-MODEL-03; architecture §11.4
- **dependsOn:** T-FND-01
- **Tests first:** FR-MODEL-03 AC-1 — `(anthropic, claude-3-5-sonnet…)` vs `(anthropic, claude-3-haiku…)` return different `MaxOutputTokens`; default flags from §6 support-matrix table-driven (LM Studio → `SupportsStreamingToolCalls=false`, `SupportsParallelToolCalls=false`); `supports_server_side_tools=false` for all v1 models (architecture §8.12).

### T-MGW-03 — Stream normalizer: Anthropic SSE → `llm.StreamEvent`
- **Component:** `internal/modelgateway/adapter/normalize/anthropic`
- **FRs:** FR-MODEL-02 AC-1, FR-MODEL-04 AC-2 (provider_raw round-trip), §11.6 (cumulative usage)
- **dependsOn:** T-FND-02
- **Tests first:** FR-MODEL-02 AC-1 — drive a recorded Anthropic SSE sequence incl. split-UTF-8 `text_delta`, an `input_json_delta` fragment, and terminal `message_delta`; assert the emitted `[]StreamEvent` matches golden; assert cumulative `message_delta` usage normalizes to `llm.Usage` with cache split; thinking `signature` + `pause_turn` content captured into `Done.ProviderRaw` byte-faithfully.

### T-MGW-04 — Stream normalizer: OpenAI Chat-Completions SSE → `llm.StreamEvent` (shared by `openaicompat`)
- **Component:** `internal/modelgateway/adapter/normalize/openaichat`
- **FRs:** FR-MODEL-01 AC-2, FR-MODEL-02
- **dependsOn:** T-FND-02
- **Tests first:** drive a recorded Chat-Completions SSE with a concatenable `function.arguments` fragment stream; assert a complete parsed `ToolCallDelta`/`ToolCall` and correct `StopReason`; assert the `args` JSON string is parsed into `ToolCall.Args` map (architecture §11.2, llm/tool.go contract).

### T-MGW-05 — Stream normalizer: OpenAI Responses typed events → `llm.StreamEvent`
- **Component:** `internal/modelgateway/adapter/normalize/openairesponses`
- **FRs:** FR-MODEL-02 AC-2, §11.1 (stateless Item-passing)
- **dependsOn:** T-FND-02
- **Tests first:** FR-MODEL-02 AC-2 — drive `response.function_call_arguments.delta` events keyed by `item_id`/`output_index`; assert a complete parsed `ToolCallPart` emitted on `Done`; assert Responses Items are carried in `Done.ProviderRaw` (stateless replay, no `previous_response_id` reliance, §11.1).

### T-MGW-06 — Provider adapter: Anthropic (`anthropic-sdk-go`)
- **Component:** `internal/modelgateway/adapter/provider/anthropic`
- **FRs:** FR-MODEL-01, FR-MODEL-04, FR-MODEL-05; §6 matrix
- **dependsOn:** T-MGW-01, T-MGW-03, T-PLAT-04 (secret resolution)
- **Tests first:** FR-MODEL-01 AC-1 — recorded HTTP fixture → `llm.Response` with correct `StopReason`/`Usage`/`Content`; error mapping (429/5xx/529→retryable kinds, 400/401→non-retryable) via `ProviderError`; `count_tokens` endpoint wired (`SupportsTokenCounting=true`).

### T-MGW-07 — Provider adapter: Google Gemini (`google.golang.org/genai`)
- **Component:** `internal/modelgateway/adapter/provider/gemini`
- **FRs:** FR-MODEL-01, FR-MODEL-03 (Flash-Lite no streaming tool calls); §6 matrix
- **dependsOn:** T-MGW-01, T-PLAT-04
- **Tests first:** FR-MODEL-01 AC-1 — recorded fixture (incl. `usageMetadata` per chunk → normalized `Usage`); path-addressed `partialArgs` (`jsonPath`/`willContinue`) normalize correctly when supported; with `SupportsStreamingToolCalls=false` the gateway buffers and emits a complete call (FR-MODEL-03 AC-2). (Uses `google.golang.org/genai`, never the EOL SDK — DOD-08-adjacent.)

### T-MGW-08 — Provider adapter: OpenAI (Responses primary + Chat-Completions sub-flag) + `openaicompat`
- **Component:** `internal/modelgateway/adapter/provider/openai` + `.../openaicompat`
- **FRs:** FR-MODEL-01 AC-1/AC-2, §11.5
- **dependsOn:** T-MGW-01, T-MGW-04, T-MGW-05, T-PLAT-04
- **Tests first:** FR-MODEL-01 AC-2 — `openaicompat` with `base_url=http://localhost:11434/v1` + placeholder key produces a Chat-Completions-shaped request matching a recorded Ollama fixture; Responses-vs-Chat selection by sub-flag; `openaicompat` uses the **Chat-Completions** normalizer (T-MGW-04), not Responses (§11.5).

### T-MGW-09 — Model-gateway gRPC server (`Generate`/`CountTokens`/`GetCapabilities`) + buffering gate + server-side-tools hard-off
- **Component:** `internal/modelgateway/adapter/inbound/grpc` + `infra/server`
- **FRs:** FR-MODEL-01/02/03/05, FR-OBS-01 (`chat` span); §8.12
- **dependsOn:** T-MGW-06, T-MGW-07, T-MGW-08, T-MGW-02, T-PLAT-05
- **Tests first:** bufconn — `Generate` streams `StreamEvent` oneof mapping `llm`⇄`gen` correctly; FR-MODEL-03 AC-2 — flag forced false → only complete tool calls emitted on `Done`; `CountTokens` returns `UNIMPLEMENTED` when `supports_token_counting=false`; a `chat` OTel span carries `gen_ai.usage.*` (FR-OBS-01 AC-1 seed); provider-native tools rejected by the hard policy switch (§8.12).

### T-MGW-10 — Orchestrator `outbound/modelgw` adapter (`ModelGatewayPort` over gateway gRPC; thin `StreamReader`)
- **Component:** `internal/orchestrator/adapter/outbound/modelgw`
- **FRs:** FR-MODEL-02 (relay), FR-API deadline propagation; DOD-08 (mapping at edge only)
- **dependsOn:** T-MGW-09
- **Tests first:** bufconn against the gateway — the adapter's `Stream` returns a `llm.StreamReader` that yields domain `StreamEvent`s mapped from proto; `Generate`/`CountTokens`/`Capabilities` round-trip; a depguard assertion that the loop package does not import this adapter's `gen/` usage transitively (boundary stays at the adapter).

---

## 7. Wave 5 — Tool runtime (registry, tools, sandbox, egress, MCP, dedup)

> Goal: the trust boundary for model-influenced code. Independent tracks: registry/validation, native tools, container runtime + kill, egress broker, MCP client, dedup store. The execute service + scheduler + gRPC server close it. Adversarial sandbox/egress tests are `//go:build integration`.

### T-TR-01 — Tool registry (`ToolRegistry` impl): native+MCP merge, JSON-Schema validation, registration errors, lazy MCP, approval-on-first-use
- **Component:** `internal/toolruntime/adapter/registry`
- **FRs:** FR-TOOL-01, FR-TOOL-02 (registration validation), FR-EXT-01 AC-3 (ListTools merge), FR-EXT-02 (approval gating)
- **dependsOn:** T-PLAT-07, T-FND-02
- **Tests first:** FR-TOOL-01 AC-1/AC-2 — missing required field → `Observation{isError:true}` *without* calling Execute; `additionalProperties:false` violation → error observation; FR-TOOL-02 AC-2 — register with missing name/description/null schema → typed `RegistrationError`; FR-EXT-01 AC-3 — `List` merges a native + a lazily-loaded MCP tool, each with non-empty name/description/valid schema; FR-EXT-02 AC-2 — raw MCP descriptions held in a pending-approval queue, not the active registry until approved.

### T-TR-02 — Native tools: read, edit, write, glob, grep, bash, webfetch, websearch (+ SideEffect/EgressClass declarations)
- **Component:** `internal/toolruntime/adapter/tools`
- **FRs:** FR-TOOL-02; FR-LOOP-04 AC-3 (web tools classification)
- **dependsOn:** T-TR-01
- **Tests first:** FR-TOOL-02 AC-1 — `ToolSpec` table test asserts each core tool's `SideEffect`/`EgressClass` (read/glob/grep=ReadOnly/None; edit/write/bash=Mutating; `webfetch`/`websearch`=Mutating/External); per-tool execution unit tests against a fake `Workspace` (read returns file bytes, edit/write mutate, grep/glob match) with the schema validated upstream by T-TR-01.

### T-TR-03 — Egress broker (`EgressBroker` impl): per-session deny-by-default allowlist, fail-closed
- **Component:** `internal/toolruntime/infra/egress`
- **FRs:** FR-TOOL-06, NFR-SEC-04; NFR-TEST-05(d) seam
- **dependsOn:** T-FND-02
- **Tests first:** FR-TOOL-06 AC-1 — no allowlist entry → `Allow(host)` returns false (deny) and the `webfetch` path surfaces `Observation{isError:true, egress-denied}`; empty allowlist means deny-all (not allow-all); `SetPolicy` widen/tighten is honored and never model-driven; fail-closed on ambiguity.

### T-TR-04 — Container `Workspace`/`RuntimePort` backend + hard limits + cgroup/PID-namespace kill
- **Component:** `internal/toolruntime/adapter/runtime/container` + `app/sandboxmgr`
- **FRs:** FR-TOOL-05, NFR-PERF-03 (startup), NFR-OPS-03 (TTL/cap/reaper), §7.5 (clean-workspace resume); NFR-TEST-05(h,i,j)
- **dependsOn:** T-TR-03
- **Tests first (adversarial, integration):** FR-TOOL-05 AC-1 / NFR-TEST-05(h) — SIGTERM-trapping bash dead within 5 s of ctx cancel (`Killed=true`); AC-2 / (i) — double-forked detached child: all descendants dead within 5 s; AC-3 / (j) — fork bomb terminated by the hard PID limit within 5 s, no container state persists after reap; `Create` re-attaches a **fresh** workspace on resume (records `WorkspaceReset`, §7.5); reaper destroys sandboxes whose `sessions.status` is finished/failed (NFR-OPS-03); max-live cap applies backpressure.

### T-TR-05 — Durable dedup ledger (`DedupStore` impl, pgx `tool_executions`)
- **Component:** `internal/toolruntime/adapter/dedup/postgres`
- **FRs:** FR-TOOL-03, FR-TOOL-04; architecture §7.2/§7.3
- **dependsOn:** T-EVT-02 (table), T-FND-02
- **Tests first (integration):** FR-TOOL-04 AC-1 — simulate restart with a `completed` entry; a second `ExecuteTool` with the same key returns the prior result without calling Execute; AC-2 — concurrent `Begin` with the same key from two goroutines → exactly one row (unique constraint), one execution, no race (`-race`); a cache hit re-checks the caller's tenant before returning bytes (§7.3); key namespace is `(tenant_id, session_id, idempotency_key)`.

### T-TR-06 — MCP client (`MCPClientPort` impl): stdio + HTTP, lazy schema load, confined sandbox, SVID isolation
- **Component:** `internal/toolruntime/adapter/mcp`
- **FRs:** FR-EXT-01, FR-EXT-02; NFR-SEC-07; NFR-TEST-05(f)
- **dependsOn:** T-TR-03, T-TR-04
- **Tests first (integration):** FR-EXT-01 AC-1 — stdio MCP stub: tool list loaded only after first invocation (lazy); the runtime's SVID socket path is **not** bind-mounted into the MCP sandbox (NFR-TEST-05(f)); AC-2 — HTTP-transport MCP with deny-all egress: requests from inside the MCP sandbox blocked by the broker; version-pin mismatch surfaces an error (server gated); fail-safe defaults `Mutating`/`External`.

### T-TR-07 — Execute use-case + read-only parallel / mutating-serial scheduler + masking + blob offload
- **Component:** `internal/toolruntime/app/execute`
- **FRs:** FR-TOOL-01/03/04/05, FR-LOOP-04 (runtime-side scheduling), FR-STATE-05 (offload), NFR-PERF-02; §8.10 masking
- **dependsOn:** T-TR-01, T-TR-02, T-TR-04, T-TR-05, T-PLAT-06
- **Tests first:** validate-then-execute path (Begin ledger → Execute → Complete); a Mutating tool whose ledger key is `completed` returns prior result (FR-TOOL-04); read-only parallel pool dispatches up to `min(4,GOMAXPROCS)` and exerts backpressure rather than unbounded goroutines (NFR-PERF-02, `-race`); output > 32 KiB offloaded to blob with `truncated=true` + `BlobRef` (FR-STATE-05); best-effort secret masking applied to output before it leaves (defense-in-depth, §8.10).

### T-TR-08 — Tool-runtime gRPC server (`ExecuteTool` stream + `ListTools`) + `execute_tool` span
- **Component:** `internal/toolruntime/adapter/inbound/grpc` + `infra/server`
- **FRs:** FR-TOOL-01 (errors as Observation, not RPC fault), FR-EXT-01 AC-3, FR-OBS-01 (`execute_tool` span), FR-OBS-05 (readiness: container runtime)
- **dependsOn:** T-TR-07, T-TR-06, T-PLAT-05
- **Tests first:** bufconn — `ExecuteTool` streams `ToolProgress`* then one `TerminalToolResult`; a tool error surfaces as `result.is_error=true`, never a gRPC fault (FR-TOOL-01); cancellation propagates to a sandbox kill (ties to T-TR-04); `ListTools` returns merged native+approved-MCP specs (FR-EXT-01 AC-3); `/readyz` checks container-runtime availability (FR-OBS-05 seam); `execute_tool` span emitted (FR-OBS-01).

### T-TR-09 — Orchestrator `outbound/toolrt` adapter (`ToolRuntimePort` over tool-runtime gRPC)
- **Component:** `internal/orchestrator/adapter/outbound/toolrt`
- **FRs:** FR-TOOL-03 (idem key passthrough), FR-LOOP-04 (descriptors), §4.4 (no auto-retry of Mutating)
- **dependsOn:** T-TR-08
- **Tests first:** bufconn — `ExecuteTool` maps `ToolExecution`(incl. `IdempotencyKey`)→proto and returns a `ToolStream`; `ListTools` returns `[]ToolDescriptor` with `SideEffect`/`EgressClass`; a Mutating `ExecuteTool` is never auto-retried at the RPC layer while a ReadOnly one may be (retryPolicy config asserted).

---

## 8. Wave 6 — Orchestrator transport, projectord, binaries

> Goal: close the client edge, wire the orchestrator infra, build the read-side worker and all entrypoints. The orchestrator gRPC server is serial after the loop + both outbound adapters; projectord and the cmd mains parallelize.

### T-ORCH-01 — Orchestrator gRPC server: `CreateSession`/`GetSession`/`Run` (resumable)/`Control`/`Fork` + relay decoupling
- **Component:** `internal/orchestrator/adapter/inbound/grpc` + `app/agent` relay glue
- **FRs:** FR-API-01/02, FR-LOOP-03 (interrupt via Control), FR-STATE-02/03, FR-OBS-01 (`invoke_agent` span), NFR-REL-05 (relay stall)
- **dependsOn:** T-LOOP-05, T-LOOP-07, T-MGW-10, T-TR-09, T-EVT-04, T-PLAT-05
- **Tests first:** FR-API-01 AC-1 — client disconnects at `seq=5`, reconnects `after_seq=5`, second stream starts at `seq=6`, no dup frames; AC-2 / NFR-REL-05 — slow-reader client does not backpressure upstream `Generate` (generation tails the log; relay-stall deadline detaches via fake clock); FR-API-02 AC-1 — `Control.Approve` for a foreign-tenant session → `PERMISSION_DENIED`, no event; AC-2 — `Reattach{from_seq=0}` replays from start; FR-LOOP-03 — `Interrupt` cancels the loop ctx → `TurnAborted`; `invoke_agent` span with child `chat` span shares `trace_id` (FR-OBS-01 AC-2).

### T-ORCH-02 — Client-edge auth (OIDC/bearer: iss/aud/exp, reject alg=none, JWKS rotation) + per-session ownership + rate limit
- **Component:** `internal/orchestrator/adapter/inbound/grpc` auth interceptor + `infra/server`
- **FRs:** FR-API-03, NFR-SEC-08; architecture §8.7
- **dependsOn:** T-ORCH-01
- **Tests first:** FR-API-03 AC-1 — expired JWT → `UNAUTHENTICATED`; AC-2 — `alg=none` token → `UNAUTHENTICATED`; ownership check: a valid token of tenant A targeting tenant B's `session_id` on `Run`/`Control`/`Fork` → `PERMISSION_DENIED`; per-tenant rate-limit returns the typed limit error.

### T-ORCH-03 — REST/JSON facade (grpc-gateway): `Run` via SSE + `Control` POST, identical auth
- **Component:** `internal/orchestrator/adapter/inbound/rest` (grpc-gateway) + `gen/` gateway stubs
- **FRs:** FR-API-04, NFR-SEC-08 (parity)
- **dependsOn:** T-ORCH-02
- **Tests first:** FR-API-04 AC-1 — REST `/v1/run` SSE with a valid JWT receives the same `EventFrame` stream (mapped to SSE; `Last-Event-ID` header → `after_seq`); AC-2 — no Authorization header → HTTP 401; a `Control` POST with a foreign session → 403 (parity with gRPC).

### T-ORCH-04 — Orchestrator infra: pgx pool + RLS `SET LOCAL` GUC acquire-hook + migration-gate check + `/livez`+`/readyz`
- **Component:** `internal/orchestrator/infra/{db,server,obs,config}`
- **FRs:** FR-STATE-04 (GUC), FR-OBS-05, NFR-SEC-02, NFR-OPS-01 (gate check)
- **dependsOn:** T-EVT-04, T-PLAT-03, T-PLAT-04, T-PLAT-05
- **Tests first:** FR-OBS-05 AC-1 — misconfigured DSN → `/readyz` 503 while `/livez` 200; AC-2 — all deps healthy → both 200 (readiness checks PG ping + downstream gRPC health + SVID present); an integration test asserts the pgx acquire-hook runs `SET LOCAL app.current_tenant` from the verified token so RLS applies on every borrowed conn (ties to T-EVT-04).

### T-PROJ-01 — `projectord` runner: xmin-bounded safe-advance cursor + checkpoint + LISTEN/NOTIFY wakeup
- **Component:** `internal/projector/app/runner` + `adapter/source/postgres`
- **FRs:** NFR-REL-04, FR-OBS-02 (lag metric input); architecture §10.4
- **dependsOn:** T-EVT-04, T-PLAT-04
- **Tests first (integration):** an out-of-order-committing transaction is **not** skipped — the cursor never advances past `pg_snapshot_xmin` (NFR-REL-04); on reconnect the poller resumes from the stored `(last_transaction_id, last_global_id)`; a gap-scan alert fires on an injected gap; `projectord` lag never blocks an append (no shared lock).

### T-PROJ-02 — Projections: cost-rollup + OTel-export + orphan-blob sweeper + projection-lag gauge
- **Component:** `internal/projector/adapter/{cost,otel}` + sweeper + `infra/obs`
- **FRs:** FR-OBS-01 (export), FR-OBS-02 (lag gauge, USE), FR-STATE-05 (sweeper), NFR-OBS-03 (alerts)
- **dependsOn:** T-PROJ-01, T-PLAT-06
- **Tests first:** cost-rollup folds `TurnFinished`/`TurnAborted` cost into a per-session total matching the event sum; the orphan sweeper deletes blob bytes whose `(tenant,ref)` has no referencing event after a grace period and never deletes a referenced blob (FR-STATE-05); the projection-lag gauge reflects injected lag (FR-OBS-02 USE).

### T-CMD-01 — `cmd/boltrope-orchestratord` main (wiring only)
- **Component:** `cmd/boltrope-orchestratord/main.go`
- **FRs:** NFR-OPS-04, NFR-OPS-01 (refuses traffic before migrate), FR-OBS-05; DOD-05
- **dependsOn:** T-ORCH-03, T-ORCH-04, T-EVT-04
- **Tests first:** a startup test asserts the process fails fast on missing required config (NFR-OPS-04) and refuses to accept traffic until the migration-gate check passes (NFR-OPS-01); `/livez`/`/readyz` served (FR-OBS-05). Wiring-only; behavior covered by component tests.

### T-CMD-02 — `cmd/boltrope-modelgwd` + `cmd/boltrope-toolruntimed` + `cmd/boltrope-projectord` mains
- **Component:** three `cmd/*/main.go`
- **FRs:** NFR-OPS-04, FR-OBS-05, NFR-SEC-01 (SPIFFE-or-exit)
- **dependsOn:** T-MGW-09, T-TR-08, T-PROJ-02
- **Tests first:** each main fails fast on missing config and (non-dev) exits if no SPIFFE SVID provider is present (NFR-SEC-01 startup assertion); health/readiness served.

### T-CMD-03 — `cmd/boltrope-migrate` (runs DDL, exits 0) + `cmd/boltrope-ctl` (client CLI/SDK)
- **Component:** `cmd/boltrope-migrate/main.go`, `cmd/boltrope-ctl/main.go`
- **FRs:** NFR-OPS-01, DOD-09 (`boltrope-ctl run`), DOD-12
- **dependsOn:** T-EVT-01, T-ORCH-03
- **Tests first:** migrate runs all migrations against a clean PG13 and exits 0 (DOD-12, integration); `boltrope-ctl run "hello world"` against a bufconn/echo orchestrator streams frames to stdout and prints the terminal result (DOD-09 quickstart seam); `ctl` honors the same auth env (token) as the edge.

---

## 9. Wave 7 — Eval harness, ops, end-to-end, DoD closeout

> Goal: the required deterministic eval gate, the compose stack, the assembled adversarial integration suite, and the DoD ledger. Eval scenarios and ops scaffolding parallelize; the closeout is last.

### T-EVAL-01 — Deterministic eval harness scaffold (scripted fake `Provider` + fake clock driving the real loop)
- **Component:** `test/eval` harness + scenario DSL
- **FRs:** NFR-TEST-04 (ADR-0007); DOD-03
- **dependsOn:** T-LOOP-05, T-LOOP-02, T-LOOP-03
- **Tests first:** the harness itself is exercised by a trivial scenario: a scripted provider that returns text-only terminates `success` with the golden event-log shape, with zero network calls — proving the harness drives the real `app/agent` loop deterministically against the Wave-0 fake provider + fake clock.

### T-EVAL-02 — Golden eval scenarios (≥5) + CI gate
- **Component:** `test/eval/scenarios/*`
- **FRs:** NFR-TEST-04; DOD-03
- **dependsOn:** T-EVAL-01
- **Tests first (these ARE the scenarios, network-free):** (1) correct tool selection + `success` golden log; (2) `error_max_turns` termination; (3) `error_max_budget_usd` termination; (4) compaction trigger (`CompactionPerformed` appears, window shrinks); (5) permission-mode enforcement (`plan`/`acceptEdits`/deny). The eval job is a **required PR gate** (DOD-03).

### T-EVAL-03 — Live smoke eval (opt-in, API-key-gated)
- **Component:** `test/eval/live`
- **FRs:** DOD-04
- **dependsOn:** T-MGW-06, T-MGW-08, T-CMD-01
- **Tests first:** skipped-without-keys guard test; when keyed, a real coding task ("add a function, run its test") completes end-to-end against one hosted provider (Anthropic/Gemini) and one self-hosted OpenAI-compatible endpoint (Ollama/vLLM) (DOD-04). Not a per-PR gate.

### T-OPS-01 — `docker-compose.yml` ordering gate (Postgres healthy → migrate exit 0 → services + projectord) + Dockerfiles
- **Component:** `docker-compose.yml`, per-service `Dockerfile`s, healthchecks
- **FRs:** NFR-OPS-02, NFR-PORT-01; DOD-05
- **dependsOn:** T-CMD-01, T-CMD-02, T-CMD-03
- **Tests first:** DOD-05 CI shell job — `docker compose up` from a clean checkout (no host Go, no prebuilt images) brings up PG + migrate + 3 services + projectord and passes readiness within 120 s; `depends_on`+`healthcheck` ordering asserted (migrate completes before services accept traffic, NFR-OPS-02).

### T-OPS-02 — Adversarial integration suite assembly (NFR-TEST-05 a–j) + `//go:build integration` job
- **Component:** `test/integration` (aggregates the adversarial tests authored in Waves 2/5/6)
- **FRs:** NFR-TEST-05; DOD-02
- **dependsOn:** T-EVT-04, T-TR-04, T-TR-06, T-PLAT-05, T-PLAT-06, T-ORCH-01
- **Tests first:** assemble and gate all ten named adversarial tests as a single integration job: (a) RLS predicate-removed; (b) cross-tenant blob denied; (c) foreign-session fork denied; (d) exfil-via-webfetch-after-injected-page blocked/gated; (e) two tenants never share a private cache entry; (f) MCP cannot read SVID/socket; (g) static-cert refuses outside dev; (h/i/j) SIGTERM-trap / double-fork / fork-bomb killed within 5 s. Each already has a home task; this task guarantees none is omitted (DOD-02).

### T-OPS-03 — Release/supply-chain wiring: GoReleaser (multi-arch, ldflags, SBOM, cosign, SLSA), release-please, Dependabot, Scorecard
- **Component:** `.goreleaser.yaml`, `.github/workflows/release.yml`, repo metadata
- **FRs:** NFR-PORT-04, DOD-10
- **dependsOn:** T-FND-01
- **Tests first:** a `goreleaser check`/dry-run job validates multi-arch static-binary + GHCR image + SBOM(syft) + keyless-cosign + SLSA config; a Scorecard action job runs; third-party actions are SHA-pinned (lint check). (Some artifacts only materialize on tag; the dry-run is the gate.)

### T-OPS-04 — OSS docs: README quickstart, LICENSE/NOTICE, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY (private reporting)
- **Component:** repo root docs
- **FRs:** DOD-09, DOD-10
- **dependsOn:** T-CMD-03
- **Tests first:** DOD-09 — the README quickstart (clone → set key → `docker compose up` → `boltrope-ctl run "hello world"` → observe result) is executed in the clean-environment smoke job and must fit the first screenful; `SECURITY.md` present with private vulnerability reporting enabled (DOD-10).

### T-DONE-01 — DoD closeout: coverage ≥75% + `-race` clean + lint 0 + buf gates + scorecard ≥5.0 ledger
- **Component:** CI thresholds + `docs/architecture/02-implementation-plan.md` DoD checklist back-reference
- **FRs:** DOD-01..DOD-12 (the ledger)
- **dependsOn:** T-EVAL-02, T-OPS-01, T-OPS-02, T-OPS-03, T-OPS-04, T-ORCH-03, T-MGW-10, T-TR-09, T-PROJ-02
- **Tests first:** CI enforces `go test -race -coverprofile` ≥ 75% line coverage module-wide (DOD-06); `golangci-lint run` exits 0 with no unjustified suppressions (DOD-07); the `depguard`/`forbidigo` purity + boundary checks pass (DOD-08); `buf lint`+`buf breaking` green and `gen/` matches `buf generate` (DOD-11); a final traceability check maps every FR-* AC to at least one passing test (DOD-01).

---

## 10. FR → primary task coverage map (traceability)

| FR group | Primary tasks |
|---|---|
| FR-LOOP-01..05 | T-LOOP-05 (core), T-LOOP-01 (assembler), T-LOOP-07 (interrupt), T-EVT-05 (recovery), T-MGW-01 (retry→error_during_execution) |
| FR-MODEL-01..05 | T-MGW-03..08 (adapters+normalizers), T-MGW-01 (retry), T-MGW-02 (capabilities), T-MGW-09 (server), T-MGW-10 (relay) |
| FR-TOOL-01..06 | T-TR-01 (validate/register), T-TR-02 (native+classes), T-TR-05 (dedup), T-TR-04 (sandbox kill), T-TR-03 (egress), T-TR-07 (execute) |
| FR-CTX-01..03 | T-LOOP-02 (context/compaction/clearing/cache-prefix), T-LOOP-04 (PreCompact) |
| FR-STATE-01..05 | T-EVT-03 (append), T-EVT-04 (load/fork/subscribe/RLS/blob), T-EVT-02 (schema), T-PLAT-06 (blob) |
| FR-PERM-01..04 | T-LOOP-03 (policy engine), T-LOOP-05 (approval persistence), T-ORCH-01 (control approve/deny) |
| FR-OBS-01..05 | T-PLAT-04 (slog/redaction/OTel/RED), T-MGW-09/T-TR-08/T-ORCH-01 (spans), T-PROJ-02 (USE/lag), T-ORCH-04 (health/readiness), T-LOOP-05 (doom-loop) |
| FR-EXT-01..04 | T-TR-06 (MCP client), T-TR-01 (MCP approval/registry), T-LOOP-04 (hooks), T-LOOP-06 (sub-agents) |
| FR-API-01..04 | T-ORCH-01 (Run/Control/Fork), T-ORCH-02 (edge auth/ownership), T-ORCH-03 (REST facade) |
| NFR-SEC / NFR-REL / NFR-TEST-05 | T-PLAT-05 (mTLS/dev-fallback), T-EVT-03/04 (lease/RLS/idempotency), T-TR-03/04/06 (egress/kill/MCP-SVID), T-OPS-02 (adversarial suite assembly) |
| Eval / Ops / DoD | T-EVAL-01..03, T-OPS-01..04, T-DONE-01 |

---

*End of implementation task split. Next gate (Gate 5): author the failing tests named in each task's "tests first" note — beginning with the Wave-0 fakes (`T-FND-02`), the pure assembler (`T-LOOP-01`), and the optimistic+fenced+idempotent append + RLS-predicate-removed integration tests (`T-EVT-03`/`T-EVT-04`) — then implement to green, wave by wave.*
