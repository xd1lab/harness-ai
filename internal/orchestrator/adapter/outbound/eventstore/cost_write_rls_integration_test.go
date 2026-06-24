//go:build integration

package eventstore

// TDD (red) integration test for SPEC AC-10b: the write-side RLS WITH CHECK
// enforcement on migration 0006's session_cost_events table.
//
// WHY this exists (coverage-gap closure): the rest of Feature O's suite never
// exercises the table's INSERT/UPDATE WITH CHECK policy. The projector (AC-10)
// writes under a cross-tenant RLS-BYPASSING role (SPEC §2.2/§2.3), so a
// migration 0006 that shipped with only a SELECT policy — or a wrong/absent
// WITH CHECK — would pass every OTHER authored test (TestCostRead_CrossTenantRLS
// only proves the SELECT policy: a foreign read sees 0 rows) while violating
// SPEC §2 / §3.1, which require 0006 to MIRROR migrations/0003's explicit
// `FOR INSERT WITH CHECK` + `FOR UPDATE … WITH CHECK` policies
// (0003_rls_policies.up.sql L66-81). AC-10b is the proof those WITH CHECK
// clauses are correctly defined on the write path — so that if a future
// NOBYPASSRLS projectord writer is introduced (SPEC §2.5(e), option (b)), the
// per-row `SET LOCAL app.current_tenant` binding becomes ENFORCING, not merely
// advisory.
//
// This test deliberately uses the NON-OWNER boltrope_app pool (h.pool,
// NOBYPASSRLS) — the SAME role and harness path the existing RLS tests use
// (store_integration_test.go:235 TestRLS_PredicateRemoved, harness header:
// appRole="boltrope_app") — NOT the OWNER connection, because a superuser/owner
// is exempt from FORCE ROW LEVEL SECURITY and would NOT be rejected by WITH
// CHECK (that is exactly the §2.2 mutual-exclusivity the SPEC pins).
//
// RED proof: session_cost_events does not exist until migration 0006 lands, so
// every INSERT below errors on the missing relation; the assertions can only
// pass once 0006 creates the table WITH the insert/update WITH CHECK policies.
//
// AC pinned: AC-10b (write-side WITH CHECK enforcement under a NOBYPASSRLS role).

import (
	"context"
	"strings"
	"testing"
)

// insertCostEventAs attempts to INSERT one session_cost_events row through the
// NON-OWNER boltrope_app pool, with the connection scoped to scopeTenant via
// `SET LOCAL app.current_tenant` (the real write-path GUC, set via setLocalTenant
// — the same helper the store uses), while the row's tenant_id column is rowTenant.
// It returns the INSERT error (nil on success). When scopeTenant != rowTenant the
// FORCE-RLS WITH CHECK policy on session_cost_events must reject it.
func insertCostEventAs(t *testing.T, h *harness, scopeTenant, rowTenant, sessionID string, globalID int64) error {
	t.Helper()
	ctx := context.Background()
	pc, err := h.pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire app conn: %v", err)
	}
	defer pc.Release()
	tx, err := pc.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Scope the connection to scopeTenant — exactly the per-row SET LOCAL the
	// SPEC's write path performs (advisory under the bypassing role, ENFORCING
	// under this NOBYPASSRLS role — which is what AC-10b proves).
	if err := setLocalTenant(ctx, tx, scopeTenant); err != nil {
		t.Fatalf("set tenant GUC: %v", err)
	}
	_, insErr := tx.Exec(ctx, `
		INSERT INTO session_cost_events
			(global_id, tenant_id, session_id, model, event_type, cost_usd,
			 input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, reasoning_tokens)
		VALUES ($1,$2,$3,'m1','TurnFinished',0.10,100,20,0,0,0)`,
		globalID, rowTenant, sessionID)
	if insErr != nil {
		return insErr
	}
	// Commit only matters for the success leg; the WITH CHECK violation surfaces
	// on Exec, so a rejected insert never reaches here.
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

// TestCostWrite_WithCheckRejectsMismatchedTenant (AC-10b): under the NON-OWNER
// boltrope_app role (NOBYPASSRLS) with app.current_tenant = X, an INSERT into
// session_cost_events with tenant_id = Y (Y != X) is REJECTED by the table's
// INSERT WITH CHECK policy, while the same INSERT with tenant_id = X SUCCEEDS.
// This proves 0006 mirrors 0003's `FOR INSERT WITH CHECK` (not merely a SELECT
// policy). It is the ONLY test in the suite that exercises the write-side WITH
// CHECK — the projector (AC-10) writes under a bypassing role where WITH CHECK is
// inert.
func TestCostWrite_WithCheckRejectsMismatchedTenant(t *testing.T) {
	h := newHarness(t)

	// Tenant X owns a session (FK target for session_cost_events.session_id).
	tenantX, sessionX := h.seedTenantAndSession(t)

	// Tenant Y must exist too (session_cost_events.tenant_id REFERENCES tenants(id)),
	// so a rejection is unambiguously the WITH CHECK policy and NOT a FK violation.
	tenantY := newUUID(t)
	if err := h.store.CreateTenant(tenantCtx(tenantY), tenantY, "Y"); err != nil {
		t.Fatalf("create tenant Y: %v", err)
	}

	// SUCCESS leg: scope = X, row tenant = X → WITH CHECK passes.
	if err := insertCostEventAs(t, h, tenantX, tenantX, sessionX, 1001); err != nil {
		t.Fatalf("matching-tenant INSERT (scope=X, row=X) was rejected, want success: %v", err)
	}

	// REJECT leg: scope = X, row tenant = Y → WITH CHECK must reject.
	err := insertCostEventAs(t, h, tenantX, tenantY, sessionX, 1002)
	if err == nil {
		t.Fatal("mismatched-tenant INSERT (scope=X, row=Y) was ACCEPTED, want rejection by session_cost_events INSERT WITH CHECK policy (AC-10b)")
	}
	// Postgres reports an RLS WITH CHECK violation with SQLSTATE 42501 and a
	// message mentioning the row-level security policy; assert it is that, not an
	// incidental error (e.g. the missing-table RED state is a DIFFERENT message,
	// so once 0006 lands this distinguishes a real WITH CHECK rejection from a
	// schema/constraint accident).
	if msg := strings.ToLower(err.Error()); !strings.Contains(msg, "row-level security") && !strings.Contains(msg, "42501") {
		t.Fatalf("mismatched-tenant INSERT rejected by the WRONG error (want a row-level security / 42501 WITH CHECK violation): %v", err)
	}
}

// TestCostWrite_WithCheckRejectsMismatchedTenantUpdate (AC-10b, UPDATE leg):
// 0006 must also mirror 0003's `FOR UPDATE … WITH CHECK`. Under boltrope_app
// scoped to X, an UPDATE of an X-owned row that flips tenant_id to Y is rejected
// by the UPDATE WITH CHECK policy. (The row is seeded via the OWNER connection so
// the test isolates the UPDATE policy, not the INSERT one.)
func TestCostWrite_WithCheckRejectsMismatchedTenantUpdate(t *testing.T) {
	h := newHarness(t)
	tenantX, sessionX := h.seedTenantAndSession(t)
	tenantY := newUUID(t)
	if err := h.store.CreateTenant(tenantCtx(tenantY), tenantY, "Y"); err != nil {
		t.Fatalf("create tenant Y: %v", err)
	}

	// Seed one X-owned cost row via the OWNER (RLS-bypassing) connection.
	insertCostEvent(t, h, 2001, tenantX, sessionX, "m1", "TurnFinished", 0.10, 100, 20)

	ctx := context.Background()
	pc, err := h.pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire app conn: %v", err)
	}
	defer pc.Release()
	tx, err := pc.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setLocalTenant(ctx, tx, tenantX); err != nil {
		t.Fatalf("set tenant GUC: %v", err)
	}

	// Flip the row's tenant to Y — the UPDATE WITH CHECK (new-row predicate) must
	// reject this even though USING (old-row predicate, tenant=X) lets us see it.
	_, updErr := tx.Exec(ctx,
		"UPDATE session_cost_events SET tenant_id = $1 WHERE global_id = 2001", tenantY)
	if updErr == nil {
		t.Fatal("UPDATE flipping tenant_id X→Y was ACCEPTED, want rejection by session_cost_events UPDATE WITH CHECK policy (AC-10b)")
	}
	if msg := strings.ToLower(updErr.Error()); !strings.Contains(msg, "row-level security") && !strings.Contains(msg, "42501") {
		t.Fatalf("UPDATE rejected by the WRONG error (want a row-level security / 42501 WITH CHECK violation): %v", updErr)
	}
}
