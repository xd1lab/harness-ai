<!-- SPDX-License-Identifier: Apache-2.0 -->

# ADR-0024: `boltrope dev` local mode — a separate single-process, loopback-only, in-memory dev binary that runs the real agent loop

- **Status:** Accepted — amended by [ADR-0029](0029-boltrope-dev-real-model-and-local-exec-opt-in.md) (the §3 roadmap deferrals of a real model edge and an opt-in local-exec sandbox are now shipped behind explicit, default-OFF flags; the import-graph fence of §5 is refined to exact-match the `modelgateway/app` Service while admitting the pure-data capabilities leaf)
- **Date:** 2026-06-15
- **Relates to:** ADR-0009/0010 (service decomposition, gRPC client edge), ADR-0011 (event-store schema — RLS, fenced lease, pg_notify), ADR-0013/NFR-SEC-01 (security model, fail-closed edge), ADR-0014 (sandbox isolation: docker kill / `--network none` / cgroup limits), ADR-0018/0019 (keyless demo provider, session-scoped permission mode), ADR-0020 (production OIDC edge auth), the eval harness (`test/eval/harness.go`), the REST/SSE facade and `igrpc.Server`. Decisions recorded in full in `C:/Users/123/Documents/harness-wave1-build/boltrope-dev/DECISIONS.md` (決策 K-1, K-2).

## Context

Onboarding is Boltrope's biggest self-inflicted churn point: a first run requires
standing up four services plus Postgres, seeding a tenant, and driving a Go client
over mTLS. A developer loses interest in the first 30 seconds. Yet the agent loop
itself needs none of that distributed machinery to demonstrate value:
`test/eval/harness.go` already drives the REAL `agent.Loop` end-to-end in one
process against an in-memory event log, a fake clock/ids, and a scripted provider,
asserting exact golden event sequences — proving the loop is single-process-capable
and that a local mode is **wiring, not new core**.

The hazard is that a local mode necessarily bypasses three production safeguards
at once: row-level security (a non-Postgres store has no RLS), mTLS (loopback
plaintext), and OIDC (no real issuer). It must be **impossible to mistake for, or
use as, a production deployment** (NFR-SEC-01 fail-closed; ADR-0013).

Two questions this ADR settles: (1) the form factor that makes "can't run in prod"
a build-time property, not a runtime flag; and (2) what the v1 event store and
sandbox actually are, given that the obvious richer options (SQLite persistence, a
real local shell sandbox) each pull in heavyweight dependencies or genuine new
core that contradict the "wiring not core" premise.

## Decision

### 1. Form factor — a SEPARATE top-level binary (K-1)

Ship a new standalone command `cmd/boltrope-dev` whose only v1 subcommand is `run`
(`boltrope-dev run [flags]`; a bare or unknown invocation prints usage and exits
non-zero). We REJECT both the hidden-subcommand-of-`orchestratord` and the
orchestrator-flag options, because both bake the in-memory/no-RLS bypass into the
SAME binary that ships to production, turning a single misconfigured flag into a
security regression. A separate binary means production images simply never package
it — **"can't be run in prod by accident" becomes a build-time property.**

It reuses the maximum existing wiring with zero core change, exactly as the eval
harness proves: `agent.NewLoop` assembled in-process with the real `policy.Engine`,
`clock.System`/`ids.System`, a dev-owned in-memory `EventLogPort`, the keyless stub
`llm.Provider` wrapped DIRECTLY as a 4-method `app.ModelGatewayPort` (NOT
`modelgateway/app.Service`, which has no `Generate` and fails closed without a
capability resolver + cost func + endpoint), and a dev-owned no-exec
`ToolRuntimePort`; fronted by `igrpc.NewLoopRunner` → `igrpc.NewServer` exposed on a
plaintext loopback gRPC listener AND the existing `rest.NewHandler` on a loopback
HTTP listener. `harnessctl --insecure` is the client unchanged.

### 2. Event store — IN-MEMORY ONLY (K-2)

The v1 event store is in-memory only. We promote the semantics of
`apptest.FakeEventLog` into a clean `cmd/boltrope-dev`-owned `Store` that satisfies
the 6-method `igrpc.EventStore` superset (the 5 `app.EventLogPort` methods PLUS
`CreateSession`). We do NOT add SQLite: `mattn/go-sqlite3` needs cgo (kills the
pure-Go single-binary distribution that is the whole point), and `modernc.org/sqlite`
is a huge transpiled dep tree — both violate the dependency-light/honest ethos and
require a whole second `EventLogPort` adapter + schema/migration/optimistic-SQL
(real new core). The production store's extra machinery (`SET LOCAL
app.current_tenant` RLS, `lease_epoch` fencing, `pg_notify`) serves multi-tenant +
multi-writer + cross-process — none of which exist in single-process,
single-writer dev mode. The `--store=sqlite` flag is **re-scoped to roadmap and
rejected in v1**, not silently ignored.

### 3. Sandbox — IN-PROCESS NO-EXEC subset (K-2)

The v1 sandbox is a dev-owned `Runtime` satisfying `app.ToolRuntimePort` that
advertises a host-side-effect-free tool subset (`read`/`compute`/`sub-agent`) plus a
`bash` PLACEHOLDER that returns a deterministic `"dev sandbox exec disabled"` error.
This keeps the loop's full dispatch path — read-only-vs-mutation scheduling, the
policy/approval gate, egress classification — fully exercised WITHOUT executing
model-generated commands on the developer's host. We do NOT reuse the docker
per-session runtime (its isolation is bound to a Docker daemon + PID-namespace
reaping + `--network none` + `PidsLimit` per ADR-0014/§9.3 — the exact onboarding
friction dev mode removes) and do NOT open bare local exec (no PID namespace, no
egress severance, no cgroup limits; process-group signaling is unsupported on the
common Windows dev host). A real shell-capable local sandbox is roadmap and, if
shipped, must hang on the existing `Workspace`/`RuntimePort` seam, carry
ADR-0014-grade cancellation→process-tree-kill + fork-bomb + resource-limit
adversarial tests, and require a SECOND explicit opt-in (`--enable-local-exec`,
default off, **rejected in v1**).

### 4. The three-layer misuse fence (tested)

Because the mode bypasses RLS, mTLS, and OIDC, all three layers are enforced by a
single hermetic `dispatch(args, env, stderr)` seam (injected env + writer, so the
fence is unit-tested without binding a listener):

1. **Non-default.** Requires the explicit `run` subcommand; a bare/unknown
   invocation prints usage and exits 2.
2. **Loud banner.** A multi-line stderr banner carrying the markers
   `NOT FOR PRODUCTION` / `IN-MEMORY` / `NO RLS` / `NO mTLS` / `NO OIDC` /
   `LOOPBACK ONLY` / `NO-EXEC`.
3. **Fail-closed refusal.** Any production signal (`KUBERNETES_SERVICE_HOST`,
   `BOLTROPE_POSTGRES__DSN`, `BOLTROPE_OIDC_ISSUER`) forces a non-zero refusal; the
   default bind is `127.0.0.1` (never `0.0.0.0`), and a non-loopback bind on EITHER
   listener requires the explicit `--i-understand-this-is-not-production`
   acknowledgement.

Because OIDC is skipped, dev mode injects a fixed synthetic single-tenant principal
(`{TenantID: igrpc.DevTenantID, Subject: "local-dev"}`) via `AuthConfig{DevInsecure:
true, ...}`, so `igrpc`'s `authorizeTenant` runs the SAME code path —
**single-tenant + loopback semantics REPLACE multi-tenant RLS rather than deleting
the tenant check.**

### 5. Build-time prod-exclusion invariant (`infra/db` split)

The dev binary is required (by its import-graph test) to transitively import NONE of
the production daemon's heavy/sensitive edges: the pgx event store, the raw pgx
driver, SPIFFE/SPIRE mTLS, `modelgateway/app.Service`, or the orchestrator→
model-gateway / →tool-runtime gRPC client adapters. Satisfying this surfaced a
latent coupling: `igrpc`'s client-edge auth needs only the pure RLS
tenant-context helper `db.WithTenant`, but that helper shared a package with the
pgx + golang-migrate **migration runner**, dragging the entire Postgres driver into
every importer's graph. We therefore (a) made `igrpc`'s duplicate-key detection
driver-agnostic (an `interface{ SQLState() string }` instead of importing
`pgconn`), and (b) moved the migration runner into a new
`internal/orchestrator/infra/dbmigrate` package, leaving `internal/orchestrator/
infra/db` as the pure, stdlib-only tenant-context package. No production behavior or
public contract changed; this is the architecturally correct severance the
build-time invariant is designed to force.

## Consequences

**Good.**
- First success at near pip-install speed: one pure-Go binary, no Docker, no
  Postgres, no keys, no mTLS. The same loop, policy, scheduling, approval gate,
  egress classification, streaming, and terminal-result delivery a production run
  exercises — proven by the in-process gRPC and REST/SSE e2e tests reaching
  termination `Success` against the keyless stub.
- "Can't run in prod by accident" is a build-time property (separate binary never
  packaged into production images), reinforced at runtime by the production-signal
  fence + loopback-only default + loud banner, all unit-tested.
- The `igrpc`/`infra/db` cleanup removes a real leaky-abstraction coupling
  (transport edge → Postgres driver) repo-wide.

**Bad / accepted trade-offs (honestly documented).**
- Dev sessions are **non-persistent** (in-memory; lost on exit) and **single-tenant**
  — never a production or multi-tenant backend.
- v1 **cannot run arbitrary shell/coding tasks**: `bash` is a refusing placeholder.
  Everything else in the loop is fully demoable/testable; real local exec is roadmap
  behind a second opt-in.
- SQLite persistence and a real local sandbox are deferred to roadmap; their flags
  are rejected (not ignored) so the deferral is explicit.

**Follow-up.**
- Roadmap: optional file persistence and an opt-in local exec sandbox, each on the
  existing port seams, each with its own ADR-0014-grade adversarial tests and the
  same prod-signal fence + loud banner.
