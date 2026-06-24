<!-- SPDX-License-Identifier: Apache-2.0 -->

# ADR-0026: Session/tenant cost-read API — persistent per-event cost rollup, additive `GetSessionCost`/`GetTenantCost` RPCs, write-side per-model correlation, idempotent-by-`global_id` projection

- **Status:** Accepted
- **Date:** 2026-06-24
- **Relates to:** ADR-0011 (event-store schema, xmin-bounded projection cursor, tenant RLS), ADR-0012 (durable turn boundaries — partial turns billed), ADR-0013 (security model, RLS), ADR-0016 (gateway-side cost), ADR-0017 (observability/cost counter), ADR-0020 (production OIDC edge auth), ADR-0022 (MCP Server mode), ADR-0023 (additive-proto precedent), ADR-0025/0027 (wave-2 read-plane siblings), the frozen `proto/` contract and `app.EventLogPort`

## Context

Per-turn cost/usage was computed by the gateway and durably written to the event
log (`TurnFinished.CostUSD/Usage`, `TurnAborted.CostUSD/UsageSoFar`), but the
*aggregated, queryable* cost lived only in memory (the projection runner's
in-process accumulator) plus a single-attribute OTel counter. There was no way to
answer "what has this session/tenant cost, broken down by model." This is the
wave-2 "make the invisible moat visible" theme — persist and expose cost.

The questions settled: where cost is persisted and how the persist stays correct
under the at-least-once projection cursor; how per-model attribution is produced
(model is on `TurnStarted`, cost is on the terminal event); what carrier exposes
the read; and the RLS scope.

## Decision

**1. Persist per-EVENT cost detail, idempotent by `global_id` — migration
`0007_cost_rollup`.** A new `session_cost_events` table keys each row on the
source event's `global_id` (the natural PK) and is written with
`INSERT ... ON CONFLICT (global_id) DO NOTHING`. Because the projection cursor is
at-least-once, a crash re-read of the same event is an *identity* no-op — never a
double count, with or without a wrapping transaction. per-session / per-tenant /
per-model totals are derived at read time via `SUM(...) GROUP BY model`, so the
projection is fully rebuildable (TRUNCATE then re-fold from cursor 0 reproduces
identical aggregates). Number coordination: Feature I shipped `0006_sessions_list_index`
on this branch, so the cost table takes `0007` (the documented merge outcome).

**2. RLS — mirror 0003.** `session_cost_events` gets `FORCE ROW LEVEL SECURITY`
with tenant-scoped SELECT/INSERT/UPDATE `WITH CHECK` policies and a `GRANT` to
`boltrope_app`. Reads use the existing `boltrope_app` + `SET LOCAL
app.current_tenant`, so cross-tenant reads see zero rows and a missing GUC fails
closed. The projectord writer scopes each insert to the source row's tenant via a
best-effort `SET LOCAL` (advisory under a bypassing role, enforcing under a
NOBYPASSRLS writer); the written `tenant_id` is COPIED from the source event, so
correctness does not depend on the GUC.

**3. per-model correlation at the WRITE side.** A new `CostProjector` sink,
attached to the projection `Runner` via `WithCostProjector`, correlates
`TurnStarted.Model` to the terminal `TurnFinished`/`TurnAborted` by `(session,
TurnID)`: an in-flight map (fast path) plus an events point lookup (slow path, for
cross-batch / post-restart recovery), falling back to `""` (rendered "unknown") on
a total miss. `EventRow` gains an additive `Seq` field and `fetchBatchSQL` selects
`seq` to bound the point lookup. per-tool cost is DROPPED (events carry no
tool×cost dimension). The projector writes BEFORE the cursor save, so a crash
before the save re-reads the batch idempotently.

**4. Carrier — two additive read-only RPCs on `OrchestratorService`, with thin
REST + MCP facades.** Add `rpc GetSessionCost(GetSessionCostRequest) returns
(GetSessionCostResponse)` (authorizeTenant + authorizeSession) and `rpc
GetTenantCost(GetTenantCostRequest) returns (GetTenantCostResponse)`
(authorizeTenant only). Each returns a `CostTotals total` + `repeated ModelCost
by_model` (sorted by cost descending; the total is the partition sum); the tenant
response adds `session_count` (`COUNT(DISTINCT session_id)`). REST adds
`GET /v1/sessions/{id}/cost` and `GET /v1/cost`; MCP adds the `get_session_cost`
and `get_tenant_cost` tools. Zero new auth. The request `tenant_id` is a GUARD
only — RLS scopes the rows, never the request field. Aggregation is in SQL NUMERIC
(`SUM(cost_usd)::float8`) to avoid Go float accumulation drift.

**5. Read store methods — adapter-side superset, not the frozen port.**
`SessionCostByModel`, `TenantCostByModel`, and `TenantSessionCostCount` are added
to the `EventStore` consumer-superset (NOT `app.EventLogPort`) and implemented by
the pgx `*Store` over `session_cost_events`, RLS auto-scoped.

## Consequences

- proto is additive (passes `buf breaking` FILE); `gen/` regenerated and committed
  in sync.
- The event log remains the billing authority; `session_cost_events` is a
  rebuildable projection (same fold, same numbers as the OTel cost counter).
- `cost_usd` is `NUMERIC(20,10)` in the table, returned as `double` on the wire
  (consistent with `Session.total_cost_usd`); a strict-decimal wire field is a
  future additive.
- DEFERRED (honest): per-tool cost (no data); a per-session cost LIST endpoint and
  time-window filters (future additive); historical backfill (0007 accumulates new
  events only; a one-time re-fold from cursor 0 is an ops runbook, safe because the
  projection is rebuildable).
