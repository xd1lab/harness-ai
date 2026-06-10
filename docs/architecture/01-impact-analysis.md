# Boltrope — v1 Architecture Impact Analysis

> **Status:** Companion to `00-architecture.md` (Gate 3, final). Supersedes the draft's implied scope.
> **Date:** 2026-06-10
> **Purpose:** Translate the finalized architecture into concrete impact across components, build order, data model, security, testing, and operations — and state what is explicitly out of scope for v1.

This analysis assumes the **greenfield** state: there is no implementation yet, so "impact" here means *what must be built, in what order, with what dependencies and risks* — and where the finalized design diverges from the draft so the divergence is traceable.

---

## 1. What changed vs. the draft (so reviewers can trace it)

The largest structural change and the highest-leverage accepted findings:

| Area | Draft | Final | Driving finding |
|---|---|---|---|
| **Event store** | Separate gRPC service (`cmd/eventstored`, `event_store.proto`, mTLS pair) | **In-process package** in orchestrator over `pgx`→Postgres; `EventLogPort` consumer-defined | yagni-critical, yagni-major (layering), yagni-minor (sub-agent) |
| **Deployable count** | 4 services | **3 services + 1 projection worker** (`projectord`) | yagni-critical; operability-minor (orphaned projections) |
| **Mid-turn durability** | Only completed turns durable | `TurnStarted` + periodic `AssistantMessageDelta` checkpoints + resumable client stream + `Reattach` | operability-critical, data-consistency-major |
| **Mutating-tool idempotency** | In-memory TTL dedup + gRPC retry | Durable `ToolExecutionStarted` before dispatch; log-derived key; `tool_executions` ledger; at-most-once + `unknown` outcome | operability-critical, data-consistency-critical |
| **Trifecta containment** | "breaking one leg defeats it"; sandbox-egress only | Egress broker on **every** model-influenced channel + taint gate; `webfetch`/`websearch` reclassified external; provider-native tools disabled; MCP confined | security-critical ×3, security-minor (arbitrary egress) |
| **RLS** | Asserted as backstop, no mechanism | Concrete: non-owner role + `SET LOCAL` GUC + `FORCE RLS` + INSERT/UPDATE policies + predicate-removed test | security-critical |
| **Blob identity** | Global `ref` PK | **`(tenant_id, ref)`** composite; cross-tenant dedup forbidden; fetch authorized by tenant+ownership | security-critical |
| **Single-writer lease** | Advisory lock (deferred + decided, racy) | **Fenced lease** (TTL + heartbeat + `lease_epoch` checked on every append) with takeover/re-drive | operability-major, data-consistency-major |
| **Stream assembly** | In the gRPC outbound adapter | Pure `app/agent/assembler.go` over `llm.StreamReader`; gateway owns all normalization | testability-critical, multi-llm-critical |
| **Message model** | 3 representations (platform/llm + proto + mirrored domain) | **One** source of truth (`platform/llm`); proto generated to match; no mirrored domain copy | yagni-major, testability-major |
| **Provider continuation** | Assembled-messages-only | `provider_raw` opaque slot + non-terminal `Pause`; Responses stateless Item-passing | multi-llm-critical |
| **Operability** | Tracing only | Health/readiness, startup/migration gate, RED/USE metrics, SLOs, stuck-loop detection, sandbox reaper | operability-major ×3, operability-minor ×2 |
| **Sandbox kill** | Cancel `docker exec` wrapper | Cgroup/PID-namespace kill + hard limits + adversarial tests | operability-major |

Full accept/reject dispositions are in the structured summary accompanying this document.

---

## 2. Components / services touched

Because this is greenfield, every component below is *new*. Listed by deployable with the build surface each implies.

### 2.1 orchestrator (`cmd/boltrope-orchestratord`) — largest surface
- `domain/`: `session`, `event` (incl. `ToolExecution` lifecycle states + `TerminationSubtype`), `budget`. Imports `platform/llm` for the message model (no mirror).
- `app/agent/`: `loop`, **`assembler`** (pure), `ports` (`ModelPort`, `ToolPort`, `EventLogPort`, `HookRunner`, `ApprovalGate`, `Clock`, `IDGenerator`).
- `app/`: `context`, `policy` (incl. **taint-tracking egress gate**), `hooks`, `subagent`, **`recovery`** (open-turn/open-execution adjudication).
- `adapter/inbound/grpc`: resumable `Run` stream + `Control` (approve/deny/interrupt/**reattach**).
- `adapter/outbound/`: `modelgw` (thin `StreamReader`), `toolrt`, **`eventstore` (the Store struct over pgx)**, `hooks` (subprocess behind `CommandRunner`), `control` (`ApprovalGate` over Control RPC).
- `infra/`: `config`, `server` (mTLS/SPIFFE + health + readiness), `db` (pgx pool + RLS acquire hook + migration-gate check), `obs` (traces + RED/USE metrics + redaction).

### 2.2 model-gateway (`cmd/boltrope-modelgwd`)
- `app/generate/`: adapter selection, harness retry (over injected `Clock`+`Jitter`), **stream normalization + tool-call accumulation** (moved here from the orchestrator).
- `adapter/provider/`: `anthropic`, `gemini`, `openai` (Responses default, CC sub-flag), `openaicompat` (shares **Chat-Completions** normalizer).
- `adapter/normalize/`: ≥3 stream normalizers feeding one `StreamEvent` oneof; emits `Pause` + `Done{provider_raw}`; open stop-reason set.
- Capabilities resolved **per `(endpoint, model)`** (model id in request).

### 2.3 tool-runtime (`cmd/boltrope-toolruntimed`)
- `app/execute/`: validate-then-execute, parallel read-only scheduling, **durable dedup check** (`DedupStore`).
- `app/sandboxmgr/`: idle/absolute TTL, max-live cap, reaper keyed off session status.
- `adapter/tools/`: native tools (read/edit/write/glob/grep/bash/webfetch/websearch); `webfetch`/`websearch` carry `EgressClass = External`.
- `adapter/mcp/`: MCP client; each server in a **confined sandbox**; descriptions treated as untrusted; approval-on-first-use; identity pinning.
- `adapter/runtime/container/`: per-session container, **cgroup/PID-namespace kill**, hard CPU/mem/PID/wall-clock limits, deny-by-default egress via broker.
- `adapter/dedup/postgres/`: `tool_executions` ledger.
- `infra/egress/`: egress-broker client / network-policy enforcement.

### 2.4 projectord (`cmd/boltrope-projectord`) — new worker
- `app/runner/`: subscribe + dispatch + checkpoint advance.
- `adapter/cost/`, `adapter/otel/`: projections. `adapter/source/postgres/`: **xmin-bounded safe-advance** Subscribe; blob-orphan sweeper; gap-scan invariant check.
- Exposes own health/readiness + max-lag metric.

### 2.5 Shared platform & infra
- `internal/platform/`: `grpcx` (mTLS/SPIFFE + interceptors incl. per-RPC RBAC), `obs`, `config`, **`llm` (single source of truth, depguard-pure)**.
- `cmd/boltrope-ctl` (client CLI/SDK), `cmd/boltrope-migrate` (DDL, exits).
- Infra not owned by Boltrope: **PostgreSQL ≥13**, the **egress broker** (network policy / proxy), SPIRE (optional in dev), object store for blobs.

---

## 3. Build order & dependencies

A dependency-ordered path that lets failing tests be written first (TDD) at each step.

1. **Foundations (no service logic).** Module `github.com/boltrope/boltrope`; `buf` + `proto/boltrope/v1/{common,orchestrator,model_gateway,tool_runtime}.proto` (**no event_store.proto**); commit `gen/`; `platform/{config,obs,grpcx}`; `.golangci.yml` with `depguard`/`forbidigo` (no direct `time.Now`/`rand`/`uuid.New`; `platform/llm` imports no `gen/`/SDK; no cross-service `domain`/`app` imports).
2. **`platform/llm`.** The normalized message/tool/stop-reason/usage model + `Provider`/`StreamReader` interfaces (the single source of truth). Pure unit tests.
3. **Event-store package + migrations.** `migrations/*.sql` (expand/contract); `boltrope-migrate`; the `Store` struct (`Append` optimistic+fenced+idempotent, `Load`, `Fork`, `Subscribe`, `LoadSnapshot`). **Testcontainers** tests: optimistic conflict, fencing, idempotent re-append (`request_id`), contiguity, RLS predicate-removed, concurrent-append race.
4. **model-gateway.** `ProviderPort` + per-`(endpoint,model)` capabilities + retry (injected Clock/Jitter) + the ≥3 stream normalizers + `Pause`/`provider_raw`. Adapter unit tests with recorded fixtures; `bufconn` mapping tests. (Real-SDK wiring can lag behind fakes.)
5. **tool-runtime.** Registry + JSON-Schema validation; native tools; `RuntimePort` container impl with cgroup kill + limits; `DedupStore`; `sandboxmgr`; MCP client (confined). Unit tests with a fake runtime; **integration** adversarial-kill + dedup tests.
6. **orchestrator loop.** `assembler` (pure, adversarial deltas) → `loop` (mock `ModelPort`/`ToolPort`/`EventLogPort`/`HookRunner`/`ApprovalGate`, fake `Clock`/`IDGenerator`) → `recovery` (open-turn/open-execution) → `policy`/`context`/`hooks`/`subagent`. The `eventstore` adapter from step 3 plugs in here.
7. **projectord.** Safe-advance subscribe + cost/OTel projections + sweeper. Testcontainers lag/no-miss test.
8. **Edge & wiring.** Resumable `Run` + `Control`/`Reattach`; grpc-gateway facade with auth parity; `docker-compose` with `depends_on`/`healthcheck` + migrate gate; SPIRE bootstrap + fail-closed dev fallback.
9. **Deterministic eval harness** (ADR-0007) wired to CI; live-smoke tier gated by API keys.

**Critical-path dependencies:** `platform/llm` (2) blocks the gateway (4) and orchestrator (6); the event-store package + migrations (3) block orchestrator (6) and projectord (7); the gateway stream contract (4) blocks the orchestrator assembler (6). The orchestrator depends on the gateway and tool-runtime *contracts* (protos) but can be tested against mocks before those services are real.

---

## 4. Data-model migrations

All DDL is greenfield (initial migration set), but ordered/structured for the constraints the design imposes:

- **`tenants`, `sessions`, `events`, `session_snapshots`, `event_subscriptions`, `blobs`, `tool_executions`** per §6.2.
- **Constraints to encode now:** `uq_events_session_seq`; `uq_events_session_request` (idempotency); head-transition `RETURNING` append (contiguity by construction) + a `CHECK`/trigger backstop; `sessions` lease columns (`lease_owner`/`lease_epoch`/`lease_expiry`/`last_event_at`); `blobs` composite PK `(tenant_id, ref)` + `events.blob_ref` FK; `tool_executions` PK `(tenant_id, session_id, idempotency_key)`; `events.provider_raw`.
- **RLS migration:** `FORCE ROW LEVEL SECURITY` + SELECT/INSERT/UPDATE policies keyed on `current_setting('app.current_tenant')` for all tenant-scoped tables; create the **non-owner app role** (no `BYPASSRLS`).
- **Version pin:** Postgres **≥13** asserted in config validation and CI (the DDL uses `xid8`/`pg_current_xact_id()`).
- **Migration policy:** **expand/contract, forward-only** for `events`/`sessions`; destructive `down` on the log is a CI-blocked anti-pattern; payload evolution uses `schema_version` + `provider_raw`, never column drops.
- **Indexes:** `idx_events_session_seq`, `idx_events_txn_global`, `idx_events_tenant`, `sessions(tenant_id)`.
- **No service-boundary migration needed for event-store** (in-process); a *future* extraction is a code change behind `EventLogPort`, not a schema change.

---

## 5. Security surface

- **Service-to-service:** SPIFFE/SPIRE SVIDs + mTLS; per-RPC SPIFFE-ID allowlist (verb-level RBAC) **paired** with the tenant token + RLS (row-level). Fail-closed dev fallback (`BOLTROPE_DEV_INSECURE=1`, ephemeral certs, startup assertion).
- **Edge:** OIDC/bearer with `iss`/`aud`/`exp` validation + algorithm pinning (reject `alg=none`) + JWKS rotation; **session-ownership authorization** on every `Run`/`Control`; per-tenant rate limits + concurrent-session/budget caps; REST facade parity.
- **Tenant propagation:** RPC-bound, short-lived signed token (`aud`=callee SPIFFE ID, `exp`, `jti`+nonce, method+session bound). Threat model explicitly records **orchestrator compromise = tenant compromise**, with RPC-binding/expiry and event-derived tenancy on read paths as mitigations.
- **Data isolation:** concrete RLS (non-owner role + `SET LOCAL` GUC from verified token + `FORCE RLS`); tenant-scoped blobs (fetch authorized by tenant+ownership, never `ref` alone); fork ownership check; server-derived tenant+session-scoped idempotency keys.
- **Egress (the decisive trifecta control):** single deny-by-default per-session allowlist broker on **all** model-influenced channels (`bash`, `webfetch`, `websearch`, MCP http); taint gate escalates external-comms to an ask once untrusted content enters context; no "arbitrary egress" anywhere.
- **MCP trust:** servers confined in their own sandboxes (no SVID exposure); descriptions untrusted; approval-on-first-use; identity/version pinning.
- **Provider-native tools:** disabled in v1 (would bypass all of the above).
- **Secrets:** provider keys only in model-gateway (env-only); masking is **defense-in-depth/hygiene only**, never a containment leg; `LogValuer` redaction.
- **Prompt cache:** tenant-scoped prefixes; only tenant-agnostic content shared; provider-cache retention documented.
- **`bypass` mode:** operator-only, server-side, audited; forbidden under untrusted/multi-tenant; cannot disable egress/tenant-isolation infra controls.
- **Multi-tenant honesty:** v1 containers safe for single-tenant/trusted-code only; multi-tenant-untrusted-code requires the deferred microVM/gVisor runtime.

**Required security tests (named so they are not dropped):** exfil-via-`webfetch`-after-injected-page is blocked/gated; RLS blocks cross-tenant rows with the WHERE predicate removed; cross-tenant blob fetch denied; fork of a foreign session denied; two tenants never share a private cache entry; MCP server cannot read the SVID/socket; static-cert provider refuses to start outside dev.

---

## 6. Testing impact

- **Determinism seams (cross-cutting):** `Clock`, jitter/rand source, and `IDGenerator` injected everywhere that sleeps/times-out/expires/generates ids — including model-gateway retry/backoff and tool-runtime dedup window (the draft omitted these from the two components that needed them most). `forbidigo` forbids direct `time.Now`/`rand`/`uuid.New` in domain/app.
- **Pure unit (fast, no network):** `platform/llm`; the **`assembler`** with adversarial delta sequences (split mid-UTF-8, out-of-order ids, Pause-before-Done, duplicate Done); `policy` (deny-wins, taint gate); `context` compaction/clearing; `budget` caps; per-`(endpoint,model)` capability resolution.
- **Loop unit:** mock `ModelPort`/`ToolPort`/`EventLogPort`/`HookRunner`/`ApprovalGate` + fake `Clock`/`IDGenerator`. Deterministic assertions on termination subtype, tool dispatch order (read-only parallel vs. mutating serialized), and **event-log shape** (golden). Approval/interrupt are deterministic via the `ApprovalGate`/control port (no real gRPC, no sleeps). `HookRunner` makes "PreToolUse blocks" testable.
- **Recovery unit/integration:** fold a log with an open `TurnStarted`/`ToolExecutionStarted` and assert the explicit recovery decision (continue / `TurnAborted` / `unknown`-outcome adjudication) — never silent re-run/re-bill.
- **Integration (`//go:build integration`, testcontainers):** optimistic+fenced+idempotent append; **concurrent-append race** (N goroutines, one COMMIT, N−1 typed conflicts, barrier-forced); contiguity; **RLS predicate-removed**; cross-tenant blob/fork denial; durable dedup across a simulated tool-runtime restart; projectord no-miss under out-of-order commits; blob write-before-reference + sweeper.
- **Adversarial sandbox (integration, real Docker):** SIGTERM-trapping process, double-forked detached child, fork bomb — each terminated within the deadline; ctx-cancel unit-tested separately against a fake runtime.
- **gRPC mapping:** `bufconn` tests for proto⇄`llm` mapping (kept separate from the pure assembler).
- **Eval (ADR-0007):** deterministic golden-scenario suite (scripted fake Provider + fake clock) is the required CI gate; live smokes gated by API keys; SWE-bench deferred (nightly/external).
- **Coverage:** >=75% (binding per spec DOD-06), `go test -race` in CI.

---

## 7. Operational / deployment impact

- **Deployables:** 3 services + `projectord` + `boltrope-ctl` + `boltrope-migrate`; infra: PostgreSQL ≥13, egress broker, SPIRE (optional dev), object store.
- **Health/readiness:** gRPC health + HTTP `/livez`/`/readyz` on every service; readiness gates on real dependency reachability (Postgres ping, downstream gRPC health, SVID present, container runtime, projection lag) — not process-up.
- **Startup ordering:** Postgres healthy → `migrate` completes (release gate) → services start; `docker-compose` `depends_on`+`healthcheck`; k8s init-container/readiness-gate for migrate; SPIRE attestation before any mTLS handshake; deterministic SPIRE-free dev start via fail-closed static-cert fallback.
- **Degradation & append resilience:** typed `error_during_execution` (not hang) after a bounded in-turn retry budget; bounded retry-with-backoff on the append path (safe via `request_id`); PgBouncer/pool-sizing guidance; documented event-store blast radius/target availability + redrive for DB-outage-failed turns.
- **Lifecycle hygiene:** `sandboxmgr` idle/absolute TTL + max-live cap + GC keyed off session status (prevents container/disk leaks); clean-workspace resume reaps abandoned containers.
- **Observability:** OTel GenAI spans (`invoke_agent`/`chat`/`execute_tool`) + RED metrics (errors by termination subtype) + USE gauges (worker pool, live sandbox count, DB pool, blob usage, projection lag) + baseline SLOs + the alert set (append error rate, sandbox near cap, pool exhaustion, projection lag, stuck-session count) + stuck-loop detection. slog-JSON logs with redaction (OTel logs still beta).
- **`projectord`:** named owner of cost-rollup/OTel-export; xmin-bounded safe-advance; LISTEN/NOTIFY only as a wakeup hint; max-lag alert. If it lags/restarts it never blocks a turn.

---

## 8. Risks & mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| **Sandbox/container escape** (shared kernel) for untrusted/multi-tenant code | High | v1 declared safe only for single-tenant/trusted-code; microVM/gVisor behind `RuntimePort` required for multi-tenant-untrusted (and made a prerequisite, not a silent assumption); hard cgroup limits + egress broker shrink blast radius. |
| **Orchestrator compromise = tenant compromise** (it asserts tenancy) | High | RPC-bound short-lived tokens; event-derived tenancy on read paths; RLS on the same connection; anomaly monitoring for many-tenant assertion. Documented in the threat model. |
| **Prompt-injection exfiltration** via web/MCP content | High | Egress broker on every model-driven channel + taint gate; `webfetch`/`websearch` reclassified external; provider-native tools disabled; MCP confined. Masking is hygiene only. Proven by required exfil test. |
| **Double execution of mutating tools** on crash/retry/lease-steal | High | Durable `ToolExecutionStarted` before dispatch + log-derived key + durable `tool_executions` ledger + fenced lease; at-most-once with explicit `unknown` outcome; mutating tools never auto-retried. |
| **Lost/under-billed in-flight generation** on mid-turn crash | High | `TurnStarted` + periodic checkpoints + `TurnAborted{usage_so_far}`; resumable client stream + `Reattach`; decoupled generation/delivery. |
| **PostgreSQL blip = fleet-wide turn failure** (synchronous append SSoT) | Medium | Bounded retry-with-backoff (idempotent appends) + pool sizing + documented blast radius; local spool deferred but flagged. |
| **Projection silently drops events** (out-of-order commits) | Medium | xmin-bounded safe-advance cursor; cursor-resume on reconnect; gap-scan invariant + max-lag alert. |
| **Provider continuation breaks replay/resume** (pause_turn, thinking sig, Responses) | Medium | `provider_raw` opaque slot + non-terminal `Pause`; Responses pinned stateless. |
| **Provider stream-shape drift** (Gemini fragments, Responses item deltas) | Medium | All normalization in the gateway; ≥3 normalizers; `SupportsStreamingToolCalls` gate buffers when unsupported; live-smoke tier catches drift. |
| **Container leak / disk exhaustion** | Medium | `sandboxmgr` TTL + cap + reaper keyed off session status. |
| **Message-model drift** (the thing the abstraction exists to prevent) | Medium | One source of truth (`platform/llm`); proto generated to match; depguard purity rule; no mirrored domain copy. |
| **Static-cert downgrade in production** | Medium | Fail-closed dev provider (`BOLTROPE_DEV_INSECURE=1` explicitly required — env-gated, present in all builds), ephemeral certs, startup assertion; SPIRE wiring enabled in release images via `-tags spire` (ADR-0013 §Amendment). |
| **Reasoning-model long turns (≤10 min) amplify crash window** | Medium | Periodic checkpoints bound lost work to one interval; relay stall deadline; per-tenant in-flight caps. |
| **Provider SDK churn** (esp. Responses) | Low/Med | SDK churn isolated in model-gateway; pinned versions; re-validate at impl gate. |

---

## 9. Explicitly OUT OF SCOPE for v1

- A **separate event-store service** (in-process now; `EventLogPort` keeps later extraction non-breaking).
- **Durable workspace snapshots / consistent-FS resume** (v1 = clean-workspace resume; uncommitted FS state is lost on resume).
- **microVM/gVisor/OS-native sandbox** backends (behind `RuntimePort`); therefore **multi-tenant execution of mutually-untrusted code** is out of scope for v1.
- **Provider-native/server-side tools** (Anthropic/OpenAI built-ins) — disabled in v1.
- **Local durable append spool** (bounded retry + pool sizing instead).
- **MCP SERVER mode + A2A**; **native-Ollama NDJSON adapter**; **model routing**; **advanced multi-agent topologies**; **non-native function-calling fallback + constrained decoding**; **semantic codebase indexing / tree-sitter repo map**; **LLM risk-classifier**; **virtual-filesystem context mounts**; **interactive workspace access (VNC/editor)**.
- **SWE-bench / SWE-bench-Lite** as a CI gate (deterministic bespoke suite is the v1 gate; SWE-bench is a later external/nightly target).
- **Message broker on the request path** (event log is the durability spine; broker remains a possible later read-side fan-out for `projectord`).
- A **REST mapping for every RPC** (v1 facade is at minimum `Run` via SSE + `Control`, with identical auth).

---

*This impact analysis pairs with `00-architecture.md`. ADRs to be recorded from this gate are listed in the structured summary; once written they supersede the relevant "Pending design workflow" entry in `docs/decisions/README.md`.*
