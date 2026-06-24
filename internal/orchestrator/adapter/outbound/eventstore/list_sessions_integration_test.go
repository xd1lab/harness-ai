//go:build integration

package eventstore

// TDD (red) integration tests for Feature I's read-only ListSessions store
// method (keyset list over the sessions table), which backs the new
// OrchestratorService.ListSessions admin RPC.
//
// Per DECISIONS.md / SPEC this is a READ-ONLY additive method on the EventStore
// consumer-superset (NOT on the FROZEN app.EventLogPort), so the
// `var _ app.EventLogPort = (*Store)(nil)` assertion is unaffected. It goes
// through the same beginTenantTx -> SET LOCAL app.current_tenant -> RLS path as
// Load, so a foreign tenant sees NOTHING even though the SQL applies no tenant
// filter. It is EXPECTED to fail to compile until ListSessionsQuery + listCursor
// and (*Store).ListSessions land (and migration 0006's composite index) — that
// absence is the red proof.
//
// Properties pinned here:
//   - RLS scoping (AC-16): tenant B's ListSessions sees ZERO of tenant A's
//     sessions — even with no tenant filter in SQL.
//   - Status + half-open time-window filter (AC-17) over a seeded set returns the
//     correct subset in (created_at, id) order.
//   - Same-millisecond created_at tie pages correctly via the id tie-break (AC-5
//     at the store layer): no drop, no duplicate.
//   - Keyset paging with page_size+1 / no OFFSET over a set larger than the page.
//   - Side-effect-free: a list inserts no session/event row.

import (
	"context"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// insertSessionAt inserts one sessions row via the OWNER connection (RLS-bypassing)
// with an explicit status and created_at, so a list test controls the ordering /
// time-window inputs directly (CreateSession stamps created_at = now()). The
// tenant row is assumed to exist (seed it via the Store or CreateTenant first).
func insertSessionAt(t *testing.T, h *harness, sessionID, tenantID string, status domain.SessionStatus, createdAt time.Time) {
	t.Helper()
	owner := h.ownerConn(t)
	_, err := owner.Exec(context.Background(), `
		INSERT INTO sessions (id, tenant_id, status, head_seq, lease_epoch, mode, created_at, updated_at)
		VALUES ($1, $2, $3, 1, 0, 'default', $4, $4)`,
		sessionID, tenantID, string(status), createdAt)
	if err != nil {
		t.Fatalf("insert session %s: %v", sessionID, err)
	}
}

// listIDs reduces a ListSessions result to the ordered slice of session ids.
func listIDs(sessions []domain.Session) []string {
	out := make([]string, len(sessions))
	for i, s := range sessions {
		out[i] = s.ID
	}
	return out
}

// TestListSessions_CrossTenantInvisible (AC-16): tenant B's ListSessions sees
// ZERO of tenant A's sessions under FORCE RLS, even though the query applies no
// tenant_id filter — the SET LOCAL tenant + RLS predicate does the scoping.
func TestListSessions_CrossTenantInvisible(t *testing.T) {
	h := newHarness(t)
	tenantA, _ := h.seedTenantAndSession(t)
	base := time.Now().UTC().Truncate(time.Millisecond)
	insertSessionAt(t, h, newUUID(t), tenantA, domain.StatusActive, base)

	tenantB := newUUID(t)
	ctxB := tenantCtx(tenantB)
	if err := h.store.CreateTenant(ctxB, tenantB, "B"); err != nil {
		t.Fatalf("create tenant B: %v", err)
	}

	got, err := h.store.ListSessions(ctxB, ListSessionsQuery{Limit: 100})
	if err != nil {
		t.Fatalf("ListSessions as tenant B: unexpected error %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("tenant B saw %d of tenant A's sessions, want 0 (RLS-scoped, no tenant filter in SQL)", len(got))
	}
}

// TestListSessions_FailsClosedWithoutTenant: a context with no tenant fails
// closed before any read (the GUC is unset; beginTenantTx returns ErrNoTenant).
func TestListSessions_FailsClosedWithoutTenant(t *testing.T) {
	h := newHarness(t)
	h.seedTenantAndSession(t)

	if _, err := h.store.ListSessions(context.Background(), ListSessionsQuery{Limit: 100}); err == nil {
		t.Fatal("ListSessions with no tenant in context returned nil error, want fail-closed")
	}
}

// TestListSessions_StatusAndTimeWindow (AC-17): a status filter plus a half-open
// [after, before) created_at window returns exactly the matching subset, in
// (created_at, id) order — exercising idx_sessions_tenant_status_created.
func TestListSessions_StatusAndTimeWindow(t *testing.T) {
	h := newHarness(t)
	tenantID := newUUID(t)
	ctx := tenantCtx(tenantID)
	if err := h.store.CreateTenant(ctx, tenantID, "test-tenant"); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}

	base := time.Now().UTC().Truncate(time.Millisecond)
	at := func(deltaMs int) time.Time { return base.Add(time.Duration(deltaMs) * time.Millisecond) }

	// Active sessions across the timeline; one finished (filtered out by status);
	// one active before the window and one active after it (filtered out by time).
	// sessions.id is a UUID column, so each id is a fresh UUID; the ordering under
	// test is created_at-driven (all created_at distinct), so the id values do not
	// affect the expected order.
	sessBefore := newUUID(t)
	sessIn1 := newUUID(t)
	sessFin := newUUID(t)
	sessIn2 := newUUID(t)
	sessAfter := newUUID(t)
	insertSessionAt(t, h, sessBefore, tenantID, domain.StatusActive, at(0))
	insertSessionAt(t, h, sessIn1, tenantID, domain.StatusActive, at(1000))
	insertSessionAt(t, h, sessFin, tenantID, domain.StatusFinished, at(1500)) // wrong status
	insertSessionAt(t, h, sessIn2, tenantID, domain.StatusActive, at(2000))
	insertSessionAt(t, h, sessAfter, tenantID, domain.StatusActive, at(3000)) // at the exclusive upper bound

	got, err := h.store.ListSessions(ctx, ListSessionsQuery{
		Statuses:      []domain.SessionStatus{domain.StatusActive},
		CreatedAfter:  at(1000), // inclusive
		CreatedBefore: at(3000), // exclusive
		Limit:         100,
	})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	gotIDs := listIDs(got)
	want := []string{sessIn1, sessIn2}
	if len(gotIDs) != len(want) {
		t.Fatalf("ListSessions returned %v, want %v (status=active, [1000,3000))", gotIDs, want)
	}
	for i := range want {
		if gotIDs[i] != want[i] {
			t.Fatalf("ListSessions order %v, want %v (created_at ascending)", gotIDs, want)
		}
	}
}

// TestListSessions_KeysetPaging covers keyset paging over a set larger than the
// page: each page returns Limit rows, the next page continues strictly after the
// last (created_at, id) with no overlap and no gap, and the union is the full set.
func TestListSessions_KeysetPaging(t *testing.T) {
	h := newHarness(t)
	tenantID := newUUID(t)
	ctx := tenantCtx(tenantID)
	if err := h.store.CreateTenant(ctx, tenantID, "test-tenant"); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}

	base := time.Now().UTC().Truncate(time.Millisecond)
	const n = 5
	for i := 1; i <= n; i++ {
		insertSessionAt(t, h, newUUID(t), tenantID, domain.StatusActive, base.Add(time.Duration(i)*time.Second))
	}

	seen := map[string]int{}
	var cursor listCursor
	for pages := 0; pages < n+2; pages++ {
		page, err := h.store.ListSessions(ctx, ListSessionsQuery{Cursor: cursor, Limit: 2})
		if err != nil {
			t.Fatalf("ListSessions page: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, s := range page {
			seen[s.ID]++
		}
		last := page[len(page)-1]
		cursor = listCursor{CreatedAtMs: last.CreatedAt.UnixMilli(), ID: last.ID}
		if len(page) < 2 {
			break
		}
	}
	if len(seen) != n {
		t.Fatalf("keyset paging saw %d distinct sessions, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("session %s appeared %d times across pages, want exactly 1 (no overlap, no dup)", id, c)
		}
	}
}

// TestListSessions_SameMillisecondTieBreak (AC-5 at the store layer): several
// sessions sharing the exact same created_at millisecond all page correctly via
// the id tie-break — a single-key (created_at) cursor would drop or duplicate at
// the boundary.
func TestListSessions_SameMillisecondTieBreak(t *testing.T) {
	h := newHarness(t)
	tenantID := newUUID(t)
	ctx := tenantCtx(tenantID)
	if err := h.store.CreateTenant(ctx, tenantID, "test-tenant"); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}

	same := time.Now().UTC().Truncate(time.Millisecond)
	const n = 4
	for i := 0; i < n; i++ {
		insertSessionAt(t, h, newUUID(t), tenantID, domain.StatusActive, same) // all identical created_at
	}

	seen := map[string]int{}
	var cursor listCursor
	for pages := 0; pages < n+2; pages++ {
		page, err := h.store.ListSessions(ctx, ListSessionsQuery{Cursor: cursor, Limit: 1})
		if err != nil {
			t.Fatalf("ListSessions page: %v", err)
		}
		if len(page) == 0 {
			break
		}
		seen[page[0].ID]++
		last := page[0]
		cursor = listCursor{CreatedAtMs: last.CreatedAt.UnixMilli(), ID: last.ID}
	}
	if len(seen) != n {
		t.Fatalf("same-ms paging saw %d distinct sessions, want %d (id tie-break)", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("same-ms session %s appeared %d times, want 1 (no drop, no dup at the ms boundary)", id, c)
		}
	}
}

// TestListSessions_Descending returns newest-first when Descending is set.
func TestListSessions_Descending(t *testing.T) {
	h := newHarness(t)
	tenantID := newUUID(t)
	ctx := tenantCtx(tenantID)
	if err := h.store.CreateTenant(ctx, tenantID, "test-tenant"); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}

	base := time.Now().UTC().Truncate(time.Millisecond)
	// sessions.id is a UUID column; ids are distinct UUIDs and the order under test
	// is created_at-driven (distinct created_at), so the id values do not affect it.
	oldID := newUUID(t)
	midID := newUUID(t)
	newID := newUUID(t)
	insertSessionAt(t, h, oldID, tenantID, domain.StatusActive, base)
	insertSessionAt(t, h, midID, tenantID, domain.StatusActive, base.Add(time.Second))
	insertSessionAt(t, h, newID, tenantID, domain.StatusActive, base.Add(2*time.Second))

	got, err := h.store.ListSessions(ctx, ListSessionsQuery{Descending: true, Limit: 100})
	if err != nil {
		t.Fatalf("ListSessions descending: %v", err)
	}
	want := []string{newID, midID, oldID}
	gotIDs := listIDs(got)
	for i := range want {
		if i >= len(gotIDs) || gotIDs[i] != want[i] {
			t.Fatalf("descending order %v, want %v (newest first)", gotIDs, want)
		}
	}
}

// TestListSessions_IsSideEffectFree asserts a list inserts nothing into sessions
// or events (the read path must never mutate).
func TestListSessions_IsSideEffectFree(t *testing.T) {
	h := newHarness(t)
	tenantID, _ := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	owner := h.ownerConn(t)
	var sessionsBefore, eventsBefore int
	if err := owner.QueryRow(context.Background(), "SELECT COUNT(*) FROM sessions").Scan(&sessionsBefore); err != nil {
		t.Fatalf("count sessions before: %v", err)
	}
	if err := owner.QueryRow(context.Background(), "SELECT COUNT(*) FROM events").Scan(&eventsBefore); err != nil {
		t.Fatalf("count events before: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := h.store.ListSessions(ctx, ListSessionsQuery{Limit: 100}); err != nil {
			t.Fatalf("ListSessions: %v", err)
		}
	}

	var sessionsAfter, eventsAfter int
	if err := owner.QueryRow(context.Background(), "SELECT COUNT(*) FROM sessions").Scan(&sessionsAfter); err != nil {
		t.Fatalf("count sessions after: %v", err)
	}
	if err := owner.QueryRow(context.Background(), "SELECT COUNT(*) FROM events").Scan(&eventsAfter); err != nil {
		t.Fatalf("count events after: %v", err)
	}
	if sessionsAfter != sessionsBefore || eventsAfter != eventsBefore {
		t.Fatalf("ListSessions mutated the store: sessions %d->%d, events %d->%d (read must be side-effect-free)",
			sessionsBefore, sessionsAfter, eventsBefore, eventsAfter)
	}
}
