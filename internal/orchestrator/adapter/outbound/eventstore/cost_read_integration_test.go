//go:build integration

package eventstore

// TDD (red) integration tests for Feature O's read-only cost methods:
// SessionCostByModel (per-session per-model rollup) and TenantCostByModel
// (per-tenant aggregate + distinct session count). These back the new
// GetSessionCost / GetTenantCost gRPC RPCs.
//
// Per DECISIONS.md / SPEC these are READ-ONLY additive methods on the EventStore
// consumer-superset (NOT on the FROZEN app.EventLogPort), so the
// `var _ app.EventLogPort = (*Store)(nil)` assertion is unaffected. They go
// through the same beginTenantTx→setLocalTenant→RLS path as Load, so a foreign
// tenant sees NOTHING. They are EXPECTED to fail to compile until migration 0006
// (session_cost_events) and the store methods land — that absence is the red
// proof.
//
// Properties pinned here:
//   - SessionCostByModel: per-model SUM(cost_usd)/SUM(tokens)/turns for one
//     session, summing to the seeded per-model truth.
//   - TenantCostByModel: aggregates EVERY session of the principal tenant and
//     reports COUNT(DISTINCT session_id) sessions.
//   - Cross-tenant RLS (AC-9): tenant B's scoped read sees ZERO of tenant A's
//     cost rows (the SELECT policy on session_cost_events).
//   - Fail-closed without a tenant (AC-8): a context with no tenant errors before
//     any read (the GUC is unset; current_setting fails closed).
//   - Side-effect-free: a cost read inserts nothing.

import (
	"context"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// ModelCostRow (the per-model cost row the store returns and the grpc EventStore
// interface consumes) is the package-level alias of igrpc.ModelCostRow declared in
// the production cost_read.go; these tests reference it directly.

// insertCostEvent inserts one session_cost_events row via the OWNER connection
// (RLS-bypassing) so a read test has material independent of the projectord write
// path. global_id is the natural idempotency PK; model "" is the unknown bucket.
func insertCostEvent(t *testing.T, h *harness, globalID int64, tenantID, sessionID, model, eventType string, cost float64, inTok, outTok int64) {
	t.Helper()
	owner := h.ownerConn(t)
	_, err := owner.Exec(context.Background(), `
		INSERT INTO session_cost_events
			(global_id, tenant_id, session_id, model, event_type, cost_usd,
			 input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, reasoning_tokens)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,0,0,0)`,
		globalID, tenantID, sessionID, model, eventType, cost, inTok, outTok)
	if err != nil {
		t.Fatalf("insert session_cost_events global_id=%d: %v", globalID, err)
	}
}

// costRowByModel reduces a SessionCostByModel/TenantCostByModel result to a map
// keyed by model for order-independent assertions.
func costRowByModel(rows []ModelCostRow) map[string]ModelCostRow {
	out := make(map[string]ModelCostRow, len(rows))
	for _, r := range rows {
		out[r.Model] = r
	}
	return out
}

// TestSessionCostByModel_PerModelSums covers the per-session per-model rollup:
// two turns on m1 and one on m2 fold into the correct per-model SUMs, scoped to
// the session's tenant via RLS.
func TestSessionCostByModel_PerModelSums(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	insertCostEvent(t, h, 1, tenantID, sessionID, "m1", "TurnFinished", 0.10, 100, 20)
	insertCostEvent(t, h, 2, tenantID, sessionID, "m1", "TurnFinished", 0.20, 200, 40)
	insertCostEvent(t, h, 3, tenantID, sessionID, "m2", "TurnAborted", 0.05, 50, 5)

	rows, err := h.store.SessionCostByModel(ctx, sessionID)
	if err != nil {
		t.Fatalf("SessionCostByModel: %v", err)
	}
	by := costRowByModel(rows)

	m1, ok := by["m1"]
	if !ok {
		t.Fatalf("no m1 row in %+v", rows)
	}
	if !floatEqStore(m1.CostUSD, 0.30) || m1.Turns != 2 {
		t.Fatalf("m1 = cost %v / %d turns, want 0.30 / 2", m1.CostUSD, m1.Turns)
	}
	if m1.Usage.InputTokens != 300 || m1.Usage.OutputTokens != 60 {
		t.Fatalf("m1 usage = %+v, want input 300 / output 60", m1.Usage)
	}
	m2, ok := by["m2"]
	if !ok || !floatEqStore(m2.CostUSD, 0.05) || m2.Turns != 1 {
		t.Fatalf("m2 = %+v, want cost 0.05 / 1 turn", m2)
	}
}

// TestTenantCostByModel_AggregatesSessions covers the per-tenant aggregate across
// two sessions: the per-model sums fold both sessions and the distinct session
// count reflects both.
func TestTenantCostByModel_AggregatesSessions(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionA := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	sessionB := newUUID(t)
	if _, err := h.store.CreateSession(ctx, sessionB, domain.ModeDefault); err != nil {
		t.Fatalf("CreateSession B: %v", err)
	}

	insertCostEvent(t, h, 1, tenantID, sessionA, "m1", "TurnFinished", 0.10, 100, 20)
	insertCostEvent(t, h, 2, tenantID, sessionB, "m1", "TurnFinished", 0.20, 200, 40)
	insertCostEvent(t, h, 3, tenantID, sessionB, "m2", "TurnFinished", 0.30, 300, 60)

	rows, err := h.store.TenantCostByModel(ctx)
	if err != nil {
		t.Fatalf("TenantCostByModel: %v", err)
	}
	// session_count comes from the SEPARATE distinct-session count method
	// (selectTenantSessionCountSQL = COUNT(DISTINCT session_id)) — TenantCostByModel
	// is the 2-return per-model rollup, consistent with the grpc EventStore fake.
	count, err := h.store.TenantSessionCostCount(ctx)
	if err != nil {
		t.Fatalf("TenantSessionCostCount: %v", err)
	}
	if count != 2 {
		t.Fatalf("session_count = %d, want 2 (two distinct sessions with cost)", count)
	}
	by := costRowByModel(rows)
	if m1 := by["m1"]; !floatEqStore(m1.CostUSD, 0.30) || m1.Turns != 2 {
		t.Fatalf("tenant m1 = %+v, want cost 0.30 / 2 turns (across both sessions)", m1)
	}
	if m2 := by["m2"]; !floatEqStore(m2.CostUSD, 0.30) || m2.Turns != 1 {
		t.Fatalf("tenant m2 = %+v, want cost 0.30 / 1 turn", m2)
	}
}

// TestCostRead_CrossTenantRLS (AC-9): tenant B's scoped read sees ZERO of tenant
// A's cost rows — the RLS SELECT policy on session_cost_events, identical to the
// events/sessions isolation guarantee.
func TestCostRead_CrossTenantRLS(t *testing.T) {
	h := newHarness(t)
	tenantA, sessionA := h.seedTenantAndSession(t)
	insertCostEvent(t, h, 1, tenantA, sessionA, "m1", "TurnFinished", 0.10, 100, 20)

	tenantB := newUUID(t)
	ctxB := tenantCtx(tenantB)
	if err := h.store.CreateTenant(ctxB, tenantB, "B"); err != nil {
		t.Fatalf("create tenant B: %v", err)
	}

	// Tenant B reading A's session sees nothing under RLS (no error, no rows).
	rows, err := h.store.SessionCostByModel(ctxB, sessionA)
	if err != nil {
		t.Fatalf("SessionCostByModel as tenant B: unexpected error %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("tenant B saw %d of tenant A's session cost rows, want 0 (RLS SELECT policy)", len(rows))
	}

	// Tenant B's tenant-wide read also sees nothing (its own tenant has no rows).
	trows, err := h.store.TenantCostByModel(ctxB)
	if err != nil {
		t.Fatalf("TenantCostByModel as tenant B: unexpected error %v", err)
	}
	count, err := h.store.TenantSessionCostCount(ctxB)
	if err != nil {
		t.Fatalf("TenantSessionCostCount as tenant B: unexpected error %v", err)
	}
	if len(trows) != 0 || count != 0 {
		t.Fatalf("tenant B tenant-cost = %d rows / %d sessions, want 0 / 0 (RLS-scoped)", len(trows), count)
	}
}

// TestCostRead_FailsClosedWithoutTenant (AC-8 store-level): a context with no
// tenant fails closed before any read — the GUC is unset and the read path
// refuses rather than returning cross-tenant data.
func TestCostRead_FailsClosedWithoutTenant(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	insertCostEvent(t, h, 1, tenantID, sessionID, "m1", "TurnFinished", 0.10, 100, 20)

	// context.Background carries no tenant -> beginTenantTx returns ErrNoTenant.
	if _, err := h.store.SessionCostByModel(context.Background(), sessionID); err == nil {
		t.Fatal("SessionCostByModel with no tenant in context returned nil error, want fail-closed")
	}
	if _, err := h.store.TenantCostByModel(context.Background()); err == nil {
		t.Fatal("TenantCostByModel with no tenant in context returned nil error, want fail-closed")
	}
	if _, err := h.store.TenantSessionCostCount(context.Background()); err == nil {
		t.Fatal("TenantSessionCostCount with no tenant in context returned nil error, want fail-closed")
	}
}

// TestCostRead_IsSideEffectFree asserts a cost read inserts nothing into
// session_cost_events (the read path must never mutate the projection).
func TestCostRead_IsSideEffectFree(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)
	insertCostEvent(t, h, 1, tenantID, sessionID, "m1", "TurnFinished", 0.10, 100, 20)

	owner := h.ownerConn(t)
	var before int
	if err := owner.QueryRow(context.Background(), "SELECT COUNT(*) FROM session_cost_events").Scan(&before); err != nil {
		t.Fatalf("count cost rows before: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := h.store.SessionCostByModel(ctx, sessionID); err != nil {
			t.Fatalf("SessionCostByModel: %v", err)
		}
		if _, err := h.store.TenantCostByModel(ctx); err != nil {
			t.Fatalf("TenantCostByModel: %v", err)
		}
		if _, err := h.store.TenantSessionCostCount(ctx); err != nil {
			t.Fatalf("TenantSessionCostCount: %v", err)
		}
	}

	var after int
	if err := owner.QueryRow(context.Background(), "SELECT COUNT(*) FROM session_cost_events").Scan(&after); err != nil {
		t.Fatalf("count cost rows after: %v", err)
	}
	if after != before {
		t.Fatalf("session_cost_events count changed %d -> %d: a cost read mutated the projection", before, after)
	}
}

// floatEqStore compares two USD costs within float slack.
func floatEqStore(a, b float64) bool {
	const eps = 1e-9
	d := a - b
	return d < eps && d > -eps
}
