//go:build integration

package eventstore

// TDD (red) integration tests for Feature M's read-only store methods:
// LoadRange (keyset page) and LoadUpTo (upper-bounded fold window). These back
// the new ListSessionEvents / GetStateAtSeq gRPC RPCs.
//
// Per DECISIONS.md (Load-then-fold, NOT Fork) these are READ-ONLY additive
// methods on the EventStore consumer-superset (NOT on the FROZEN
// app.EventLogPort), so the `var _ app.EventLogPort = (*Store)(nil)` assertion is
// unaffected. They are EXPECTED to fail to compile until the methods land —
// that absence is the red proof.
//
// Properties pinned here:
//   - LoadRange: keyset paging WHERE seq > afterSeq ORDER BY seq LIMIT limit
//     (strictly greater, ordered, capped). limit<=0 / over-cap behavior is the
//     server's concern; the store honors a positive limit verbatim.
//   - LoadUpTo: bounded window WHERE seq <= atSeq ORDER BY seq (inclusive upper
//     bound), oldest first.
//   - Both are TENANT-SCOPED via the same setLocalTenant + RLS path as Load: a
//     foreign tenant sees nothing.
//   - Both are SIDE-EFFECT-FREE: no new sessions row, no new events row — the
//     read path must never mutate (the load-bearing "no Fork" guarantee).

import (
	"context"
	"fmt"
	"testing"
)

// seedNEvents appends n TurnStarted events (seq 1..n) to sessionID and returns
// nothing; it fails the test on any append error.
func seedNEvents(ctx context.Context, t *testing.T, s *Store, sessionID string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := appendOne(ctx, s, sessionID, int64(i), 0, newUUID(t), fmt.Sprintf("turn-%d", i)); err != nil {
			t.Fatalf("seed append %d: %v", i, err)
		}
	}
}

// TestLoadRange_KeysetPage covers the keyset page: seq strictly greater than
// afterSeq, oldest first, capped at limit.
func TestLoadRange_KeysetPage(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)
	seedNEvents(ctx, t, h.store, sessionID, 10) // seq 1..10

	// Page 1: after_seq=0, limit=4 -> seq 1,2,3,4.
	page1, err := h.store.LoadRange(ctx, sessionID, 0, 4)
	if err != nil {
		t.Fatalf("LoadRange page1: %v", err)
	}
	if len(page1) != 4 {
		t.Fatalf("page1 len = %d, want 4", len(page1))
	}
	for i, env := range page1 {
		if env.Seq != int64(i+1) {
			t.Fatalf("page1[%d].Seq = %d, want %d (oldest-first, contiguous)", i, env.Seq, i+1)
		}
	}

	// Page 2: after_seq=4, limit=4 -> seq 5,6,7,8 (strictly greater, no overlap).
	page2, err := h.store.LoadRange(ctx, sessionID, 4, 4)
	if err != nil {
		t.Fatalf("LoadRange page2: %v", err)
	}
	if len(page2) != 4 || page2[0].Seq != 5 {
		t.Fatalf("page2 = %d events starting at seq %d, want 4 starting at 5", len(page2), seqOf(page2))
	}

	// Final page: after_seq=8, limit=4 -> seq 9,10 (short last page).
	page3, err := h.store.LoadRange(ctx, sessionID, 8, 4)
	if err != nil {
		t.Fatalf("LoadRange page3: %v", err)
	}
	if len(page3) != 2 || page3[0].Seq != 9 || page3[1].Seq != 10 {
		t.Fatalf("page3 = %v, want seq 9,10", seqsOf(page3))
	}
}

// TestLoadRange_RespectsLimit asserts a positive limit caps the returned rows
// even when more events match the cursor.
func TestLoadRange_RespectsLimit(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)
	seedNEvents(ctx, t, h.store, sessionID, 50)

	got, err := h.store.LoadRange(ctx, sessionID, 0, 10)
	if err != nil {
		t.Fatalf("LoadRange: %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("LoadRange limit=10 returned %d rows, want exactly 10", len(got))
	}
}

// TestLoadUpTo_BoundedWindow covers the inclusive upper-bound window used for
// at-seq reconstruction: WHERE seq <= atSeq ORDER BY seq.
func TestLoadUpTo_BoundedWindow(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)
	seedNEvents(ctx, t, h.store, sessionID, 10) // seq 1..10

	upTo5, err := h.store.LoadUpTo(ctx, sessionID, 5)
	if err != nil {
		t.Fatalf("LoadUpTo: %v", err)
	}
	if len(upTo5) != 5 {
		t.Fatalf("LoadUpTo(5) len = %d, want 5 (seq 1..5 inclusive)", len(upTo5))
	}
	for i, env := range upTo5 {
		if env.Seq != int64(i+1) {
			t.Fatalf("upTo5[%d].Seq = %d, want %d", i, env.Seq, i+1)
		}
	}

	// atSeq >= head returns the whole stream.
	all, err := h.store.LoadUpTo(ctx, sessionID, 1000)
	if err != nil {
		t.Fatalf("LoadUpTo(1000): %v", err)
	}
	if len(all) != 10 {
		t.Fatalf("LoadUpTo past head returned %d, want 10 (full stream)", len(all))
	}

	// atSeq <= 0 returns the empty window.
	none, err := h.store.LoadUpTo(ctx, sessionID, 0)
	if err != nil {
		t.Fatalf("LoadUpTo(0): %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("LoadUpTo(0) returned %d, want 0 (empty state)", len(none))
	}
}

// TestLoadRange_CrossTenantInvisible asserts a foreign tenant cannot page
// another tenant's events — the same RLS guarantee Load has. Tenant B's
// LoadRange over A's session returns no rows (the events are invisible under
// RLS, never an error that leaks existence).
func TestLoadRange_CrossTenantInvisible(t *testing.T) {
	h := newHarness(t)
	tenantA, sessionA := h.seedTenantAndSession(t)
	ctxA := tenantCtx(tenantA)
	seedNEvents(ctxA, t, h.store, sessionA, 5)

	tenantB := newUUID(t)
	ctxB := tenantCtx(tenantB)
	if err := h.store.CreateTenant(ctxB, tenantB, "B"); err != nil {
		t.Fatalf("create tenant B: %v", err)
	}

	got, err := h.store.LoadRange(ctxB, sessionA, 0, 100)
	if err != nil {
		t.Fatalf("LoadRange as tenant B: unexpected error %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("tenant B saw %d of tenant A's events, want 0 (RLS-scoped read)", len(got))
	}
}

// TestLoadUpTo_CrossTenantInvisible is the LoadUpTo counterpart of the RLS
// isolation guarantee.
func TestLoadUpTo_CrossTenantInvisible(t *testing.T) {
	h := newHarness(t)
	tenantA, sessionA := h.seedTenantAndSession(t)
	ctxA := tenantCtx(tenantA)
	seedNEvents(ctxA, t, h.store, sessionA, 5)

	tenantB := newUUID(t)
	ctxB := tenantCtx(tenantB)
	if err := h.store.CreateTenant(ctxB, tenantB, "B"); err != nil {
		t.Fatalf("create tenant B: %v", err)
	}

	got, err := h.store.LoadUpTo(ctxB, sessionA, 1000)
	if err != nil {
		t.Fatalf("LoadUpTo as tenant B: unexpected error %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("tenant B saw %d of tenant A's events via LoadUpTo, want 0", len(got))
	}
}

// TestLoadRangeAndUpTo_AreSideEffectFree is the load-bearing read-API guarantee:
// neither method creates a session row nor an events row (the "no Fork, no
// billing, no side effect" requirement). It snapshots the sessions/events
// counts before and after a series of reads and asserts they are unchanged.
func TestLoadRangeAndUpTo_AreSideEffectFree(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)
	seedNEvents(ctx, t, h.store, sessionID, 6)

	owner := h.ownerConn(t)
	var sessionsBefore, eventsBefore int
	if err := owner.QueryRow(context.Background(), "SELECT COUNT(*) FROM sessions").Scan(&sessionsBefore); err != nil {
		t.Fatalf("count sessions before: %v", err)
	}
	if err := owner.QueryRow(context.Background(), "SELECT COUNT(*) FROM events").Scan(&eventsBefore); err != nil {
		t.Fatalf("count events before: %v", err)
	}

	// Exercise both read methods several times.
	for i := 0; i < 3; i++ {
		if _, err := h.store.LoadRange(ctx, sessionID, int64(i), 2); err != nil {
			t.Fatalf("LoadRange: %v", err)
		}
		if _, err := h.store.LoadUpTo(ctx, sessionID, int64(i+1)); err != nil {
			t.Fatalf("LoadUpTo: %v", err)
		}
	}

	var sessionsAfter, eventsAfter int
	if err := owner.QueryRow(context.Background(), "SELECT COUNT(*) FROM sessions").Scan(&sessionsAfter); err != nil {
		t.Fatalf("count sessions after: %v", err)
	}
	if err := owner.QueryRow(context.Background(), "SELECT COUNT(*) FROM events").Scan(&eventsAfter); err != nil {
		t.Fatalf("count events after: %v", err)
	}

	if sessionsAfter != sessionsBefore {
		t.Fatalf("sessions count changed %d -> %d: a read created a session row (Fork would; Load must not)", sessionsBefore, sessionsAfter)
	}
	if eventsAfter != eventsBefore {
		t.Fatalf("events count changed %d -> %d: a read appended an event (the read path must be side-effect-free)", eventsBefore, eventsAfter)
	}
}
