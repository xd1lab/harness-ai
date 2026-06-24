<!-- SPDX-License-Identifier: Apache-2.0 -->

# ADR-0027: Admin/tenant session-management API — additive `ListSessions`/`GetSessionUsage` read RPCs, STOP via existing Control interrupt, keyset pagination, RLS-scoped (no global-admin)

- **Status:** Accepted
- **Date:** 2026-06-24
- **Relates to:** ADR-0009/0010 (service decomposition, gRPC client edge), ADR-0011 (event-store schema, optimistic + fenced lease, tenant RLS), ADR-0013 (security model, RLS, RPC-bound tenant tokens), ADR-0019 (session-scoped permission mode), ADR-0020 (production OIDC edge auth), ADR-0022 (MCP Server mode), ADR-0023 (additive-proto precedent), the wave-2 read-plane siblings (event-read, cost-read), the frozen `proto/` contract and `internal/platform/llm` kernel, the frozen `app.EventLogPort`

## Context

A platform team running Boltrope could not answer the basic operational
questions about its own tenant: *which agent sessions exist, what state are they
in, how much has a session cost, and can I stop one that is running.* The durable
event log and the `sessions` aggregate table held all of this, but there was no
read surface over them: `GetSession` reads exactly one session by id, and there
was no list/search, no per-session usage endpoint, and no documented "stop"
control for an admin. This is the wave-2 "make the invisible moat visible" theme:
expose the existing durable state as a tenant-scoped **read plane**.

The questions this ADR settles: what carrier exposes the management surface; how
list/search is shaped and paginated safely; how an admin STOP maps onto existing
machinery without inventing a new kill path; where per-session usage comes from on
this branch; what the RLS/ownership scope is; and what is honestly deferred.

## Decision

**1. Carrier — two additive read-only RPCs on `OrchestratorService`, with thin
REST + MCP facades (reject REST-only).** Add
`rpc ListSessions(ListSessionsRequest) returns (ListSessionsResponse)` and
`rpc GetSessionUsage(GetSessionUsageRequest) returns (GetSessionUsageResponse)`
to `proto/boltrope/v1/orchestrator.proto`, regenerate and commit `gen/` in sync.
The REST facade adds `GET /v1/sessions` and `GET /v1/sessions/{id}/usage`; the MCP
facade adds the `list_sessions` and `get_session_usage` tools. Each facade is a
thin shell over the SAME `*igrpc.Server` method, so ownership/RLS, error mapping,
and the wire vocabulary cannot drift between transports — the read-plane pattern
the wave-2 siblings (event-read, cost-read) already set, and the precedent
ADR-0022/ADR-0023 established. The change is **additive** (a new rpc/message/enum)
so `buf breaking` passes at FILE granularity (verified).

**2. `ListSessions` shape — control/lineage projection, status + half-open time
filter, opaque keyset pagination, hard-capped page size.** A `SessionSummary`
carries ONLY `sessions`-table columns (id, tenant_id, status, mode, head_seq,
lineage, created/updated/last_event timestamps) — **no per-row usage/cost** (that
would force an N+1 fold) and **no event payloads** (so there is no message text,
system prompt, provider_raw, or blob to redact by construction). Filters: a
`repeated SessionStatus` OR-filter (empty = all) and a half-open
`[created_after_ms, created_before_ms)` window. Pagination is **keyset on the
composite `(created_at, id)` total order** — never `OFFSET` (so deep pages do not
degrade) — exposed as an **opaque, URL-safe base64 `page_token`** that the client
must treat as opaque; the sort direction rides inside the token so a paging walk
cannot reverse mid-walk. A malformed token is `InvalidArgument` (never a silent
first page). `page_size` defaults to 50 and is **clamped to 200** (never
rejected), bounding DB load.

**3. STOP = the existing `Control{InterruptAction}` — no new rpc, no new kill.**
Admin "stop" reuses `Control(session_id, action:interrupt)`, which cancels the
in-process run loop's context (FR-LOOP-03); the loop's cooperative abort appends
`TurnAborted{UsageSoFar, CostUSD}`. It is documentation-only across all three
facades (REST `POST /v1/sessions/{id}/control {"action":"interrupt"}` and the MCP
`control` tool already exist). On an active session with a live loop it
cooperatively aborts (resumable); on a finished/idle session it is an **idempotent
no-op success** returning the current head_seq; the only non-success surfaces are
ownership (cross-tenant `PermissionDenied`, missing `NotFound`). It is explicitly
**NOT** `eventstore.SetSessionStatus`, which is lease-fenced
(`WHERE lease_epoch = $3`): an admin holds no lease and that path would neither
stop a loop running elsewhere nor be idempotent.

**4. `GetSessionUsage` v1 source = `USAGE_SOURCE_EVENT_FOLD`.** Per-session
usage/cost/turns are folded on the read path by the existing
`foldTotals(ctx, sessionID)` — the SAME fold `GetSession` uses — so the values
exactly equal `GetSession`'s, and an interrupted session's `TurnAborted` partial
is included (accounted, never re-billed). The response is tagged
`source = USAGE_SOURCE_EVENT_FOLD`. The cost-rollup projection (the wave-2
cost-read sibling) is **not merged on this branch**, so `USAGE_SOURCE_COST_ROLLUP`
is reserved enum-space only; the future "try the rollup, else fold" branch is a
wire-shape-preserving internal additive (both sources sum the same event
`CostUSD`/`Usage`, so no value drift when it lands).

**5. RLS scope — zero new auth.** `ListSessions` runs `authorizeTenant` ONLY (a
tenant-range guard: the request `tenant_id` must match the principal when
non-empty, but it is **never a filter key**); the row set is RLS-scoped to the
principal's tenant at the store (`beginTenantTx` → `SET LOCAL app.current_tenant`
→ the 0003 `sessions` SELECT policy), so the SQL carries no tenant filter yet a
foreign tenant sees nothing. `GetSessionUsage` additionally runs `authorizeSession`
(cross-tenant → `PermissionDenied`, missing/RLS-invisible → `NotFound`). The new
read-only `ListSessions` store method is added to the adapter-side `EventStore`
consumer-superset, **NOT** the frozen `app.EventLogPort`.

**6. Storage — one additive index-only migration (0006).** Add
`idx_sessions_tenant_created (tenant_id, created_at, id)` and
`idx_sessions_tenant_status_created (tenant_id, status, created_at, id)` —
tenant-prefixed to match the RLS predicate, with the `(created_at, id)` keyset as
the tail so both the sort and the keyset comparison are index-ordered. (0001 had
only `idx_sessions_tenant` and `idx_sessions_status`.) Forward-only; no new RLS
policy and no GRANT change (0003 already covers the read).

**7. Timestamps are `int64` Unix epoch milliseconds**, never
`google.protobuf.Timestamp`: `proto/` imports zero well-known types and uses
`int64` everywhere (seq, head_seq, Usage), so this stays dependency-light and
consistent.

## Consequences

- A tenant can now list/search its sessions, read per-session usage, and stop a
  running session over gRPC, REST, and MCP — using the same auth/RLS as every
  other edge call, with no new credential or kill path.
- Keyset pagination + a hard page-size cap bound DB load and make deep paging
  cheap; the opaque token keeps the cursor an implementation detail.
- `SessionSummary` reads no events, so the list surface has nothing to redact —
  the strongest form of the redaction rule.
- `GetSessionUsage` is forward-compatible with the cost-rollup projection: the
  enum tag makes the eventual source switch observable without a wire change.

## Deferred (honest, conflict with tenant-RLS scope)

- **Cross-tenant / global-admin views** — require bypassing tenant RLS; they
  belong in a separate operator plane with its own `BYPASSRLS` connection role and
  its own ADR, not on the tenant edge.
- **User-level (subject × cost) attribution** — `TurnFinished`/`TurnAborted`
  carry no user/subject dimension, so there is no data to attribute; inventing one
  is out of scope.
- **Reliable cross-process STOP** — `Control.Interrupt` cancels only the
  in-process loop; a session running on another instance is unaffected. This is a
  pre-existing Control limitation, not introduced here.
- **Sort keys beyond `created_at`, metadata/full-text search, and list-embedded
  usage** — all future additive changes (`SessionSortField` reserves room for the
  first).
