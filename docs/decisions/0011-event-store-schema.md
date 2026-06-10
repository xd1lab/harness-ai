# 11. Event-store schema: optimistic + fenced lease + request_id idempotency, xmin-bounded projection cursor, tenant-scoped blobs, concrete RLS, Postgres >= 13

Date: 2026-06-10
Status: Accepted

## Context

The event log in PostgreSQL is the single source of truth for all agent state. Every
design property — resume, fork, replay, cost accounting, observability — is derived from
it. The schema must enforce per-session ordering, prevent double-writes on crash/retry,
fence out stuck or stolen writers, give projections a gap-safe global feed, isolate
tenant data concretely, and handle large tool outputs without WAL bloat.

The draft left several gaps: the idempotency mechanism conflated "does not double-write"
with "is idempotent on retry after a lost ACK"; the global-ordering cursor for
projections naively skipped out-of-order-committing transactions; blob identity was
global (a cross-tenant existence oracle); RLS was asserted as a backstop without a
concrete enforcement mechanism; and the minimum PostgreSQL version was unspecified
despite using xid8.

## Decision

We will use a single append-only `events` table with the following design, enforced by
the initial migration set:

**Per-session ordering and optimistic concurrency.** The `(session_id, seq)` unique
constraint carries a contiguous integer seq enforced by construction: the append
transaction does an UPDATE on `sessions.head_seq` (optimistic gate + fencing + status
guard) and uses the RETURNING value as the new seq, so seq is always `head_seq + 1`
with no gap possible. A periodic gap-scan in projectord is a backstop alert.

**Fenced lease.** Every append carries the writer's `lease_epoch`. The UPDATE on
sessions checks `lease_epoch = :lease_epoch`; a stale (fenced-out) writer is rejected
even if its expected_seq is current. The lease mechanism is specified in ADR-0015;
this ADR records it as a schema-level constraint.

**Per-append request_id idempotency.** Each Append call carries a `request_id` (UUID).
The `uq_events_session_request` unique constraint on `(session_id, request_id)` ensures
a retried append whose original committed but whose ACK was lost returns the existing
row as success — not a spurious conflict. A genuine conflict (same seq, different
request_id) returns FAILED_PRECONDITION.

**Global ordering for projections.** The `events` table carries both a
`global_id BIGINT GENERATED ALWAYS AS IDENTITY` and a
`transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id()`. Projections poll with
the dual `(transaction_id, global_id)` cursor ordered by `(transaction_id, global_id)`,
reading only rows where `transaction_id < pg_snapshot_xmin(pg_current_snapshot())` —
the eugene-khyst pattern. This guarantees that a long-running transaction whose commit
is delayed is never silently skipped; the cursor only advances past fully-settled
transactions.

**Minimum PostgreSQL version is 13.** The `xid8` type and `pg_current_xact_id()`
function are required for the gap-safe cursor. This is asserted in config validation
and CI.

**Tenant-scoped blobs.** Large tool outputs (above 32 KiB) are stored outside the
events table. The `blobs` table uses a composite primary key `(tenant_id, ref)` rather
than a global ref, forbidding cross-tenant content-addressed dedup (which would be a
cross-tenant existence oracle). Every blob fetch authorizes against both the requesting
session's verified tenant_id and ownership, never the ref alone. The blob bytes are
written to the object store before the append transaction; the transaction inserts the
`blobs` metadata row and the `events` row together, so the FK on `events.blob_ref`
makes a dangling reference impossible.

**Concrete RLS.** The application connects via a non-owner role without BYPASSRLS. On
every connection acquire, the orchestrator runs `SET LOCAL app.current_tenant =
:tenant_id` where tenant_id comes from the verified principal token, never from a
client-supplied field. `FORCE ROW LEVEL SECURITY` policies on events, sessions,
session_snapshots, blobs, event_subscriptions, and tool_executions are keyed on
`current_setting('app.current_tenant')::uuid` for SELECT, INSERT, and UPDATE. Policies
cover INSERT and UPDATE on the append path (not just SELECT) so a missing policy
fails-closed in tests rather than silently in production. An integration test runs an
append and load with the WHERE tenant_id predicate removed and proves RLS still blocks
cross-tenant rows.

**Migration policy.** DDL is managed by golang-migrate with embedded SQL applied by
`cmd/boltrope-migrate`. Migrations for events and sessions are expand/contract,
forward-only; destructive down migrations on the log are a CI-blocked anti-pattern.
Payload evolution uses schema_version and provider_raw, never column drops.

**Tool-result clearing** is append-only: a `ToolResultCleared{cleared_ref, reason}`
event references the target by `(session_id, seq)` pair (not bare seq, so it is
unambiguous after a fork). Clearing is idempotent and validated at append time.

## Consequences

- Contiguity is guaranteed by construction (not by app-level assertion), with a
  unique-constraint backstop and a gap-scan alert.
- The fencing token on every append prevents a stuck or stolen writer from corrupting
  the stream even if its expected_seq happens to be current.
- The request_id idempotency mechanism correctly handles the lost-ACK case without
  conflating it with a write conflict.
- The xmin-bounded projection cursor closes the out-of-order-commit hole that would
  cause cost-rollup or audit projections to silently drop events.
- Tenant-scoped blob identity closes the cross-tenant dedup oracle; concrete RLS makes
  tenant isolation a tested, enforced guarantee rather than a claim.
- Pinning Postgres >= 13 is a concrete, checkable prerequisite; it is validated at
  startup and in CI so a misconfigured environment fails fast.
- Large outputs leave the WAL via the blob store; the events table remains compact.
