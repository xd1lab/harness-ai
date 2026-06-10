//go:build integration

package dedup

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// ctx is a convenience for a plain background context.
// The Store reads the tenant from the ExecutionRecord / Lookup parameters, not
// from context — so tests pass context.Background() throughout.
var ctx = context.Background()

// TestBeginRecordsStartedIntent verifies that Begin persists an ExecStarted
// record for a new (TenantID, SessionID, IdempotencyKey) key (FR-TOOL-04 AC-1
// premise; ADR-0012 §"Durable execution intent before side effects").
func TestBeginRecordsStartedIntent(t *testing.T) {
	h := newDedupHarness(t)
	t.Logf("mode: %s", h.mode)

	tenantID, sessionID := h.seedTenantAndSession(t)
	idemKey := "key-" + newUUID(t)

	rec, err := h.store.Begin(ctx, app.ExecutionRecord{
		TenantID:       tenantID,
		SessionID:      sessionID,
		IdempotencyKey: idemKey,
	})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if rec.Status != app.ExecStarted {
		t.Errorf("Begin returned status=%q, want %q", rec.Status, app.ExecStarted)
	}
	if rec.TenantID != tenantID {
		t.Errorf("Begin returned tenantID=%q, want %q", rec.TenantID, tenantID)
	}
	if rec.SessionID != sessionID {
		t.Errorf("Begin returned sessionID=%q, want %q", rec.SessionID, sessionID)
	}
	if rec.IdempotencyKey != idemKey {
		t.Errorf("Begin returned idempotencyKey=%q, want %q", rec.IdempotencyKey, idemKey)
	}
}

// TestCompleteAndRetryReturnsPriorResult simulates a restart scenario: a
// completed entry is present; a second Begin call with the same key returns
// the prior completed record rather than re-executing (FR-TOOL-04 AC-1;
// ADR-0012 §"Durable dedup ledger").
func TestCompleteAndRetryReturnsPriorResult(t *testing.T) {
	h := newDedupHarness(t)

	tenantID, sessionID := h.seedTenantAndSession(t)
	idemKey := "key-" + newUUID(t)

	// First: Begin (records started intent).
	_, err := h.store.Begin(ctx, app.ExecutionRecord{
		TenantID:       tenantID,
		SessionID:      sessionID,
		IdempotencyKey: idemKey,
	})
	if err != nil {
		t.Fatalf("Begin (first): %v", err)
	}

	// Complete with a known observation.
	wantObs := domain.Observation{
		Content: "tool output text",
		IsError: false,
	}
	if err := h.store.Complete(ctx, app.ExecutionRecord{
		TenantID:       tenantID,
		SessionID:      sessionID,
		IdempotencyKey: idemKey,
		Status:         app.ExecCompleted,
		Result:         wantObs,
	}); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Simulated retry: Begin again with the same key. Must return the prior
	// completed record, not start a new execution.
	retried, err := h.store.Begin(ctx, app.ExecutionRecord{
		TenantID:       tenantID,
		SessionID:      sessionID,
		IdempotencyKey: idemKey,
	})
	if err != nil {
		t.Fatalf("Begin (retry): %v", err)
	}

	if retried.Status != app.ExecCompleted {
		t.Errorf("retry Begin status=%q, want %q", retried.Status, app.ExecCompleted)
	}
	if retried.Result.Content != wantObs.Content {
		t.Errorf("retry Begin result.Content=%q, want %q", retried.Result.Content, wantObs.Content)
	}
	if retried.Result.IsError != wantObs.IsError {
		t.Errorf("retry Begin result.IsError=%v, want %v", retried.Result.IsError, wantObs.IsError)
	}
}

// TestUnknownInProgressIsNotRedispatchable verifies that an in-progress
// (status=started) record returned by Begin signals UNKNOWN: the caller must
// NOT blindly re-dispatch a mutating tool (ADR-0012 §"At-most-once recovery";
// architecture §7.2). The test asserts the status is ExecStarted (not
// completed) so the caller's adjudication path applies.
func TestUnknownInProgressIsNotRedispatchable(t *testing.T) {
	h := newDedupHarness(t)

	tenantID, sessionID := h.seedTenantAndSession(t)
	idemKey := "key-" + newUUID(t)

	// Start execution (no Complete call — simulates a crash between Begin and
	// Complete).
	if _, err := h.store.Begin(ctx, app.ExecutionRecord{
		TenantID:       tenantID,
		SessionID:      sessionID,
		IdempotencyKey: idemKey,
	}); err != nil {
		t.Fatalf("Begin: %v", err)
	}

	// A second Begin with the same key (e.g. on retry after restart) must
	// return the started — not completed — record so the caller knows the
	// outcome is UNKNOWN.
	retried, err := h.store.Begin(ctx, app.ExecutionRecord{
		TenantID:       tenantID,
		SessionID:      sessionID,
		IdempotencyKey: idemKey,
	})
	if err != nil {
		t.Fatalf("Begin (retry on in-progress): %v", err)
	}

	if retried.Status != app.ExecStarted {
		t.Errorf("in-progress retry Begin status=%q, want %q (UNKNOWN — must not re-dispatch)",
			retried.Status, app.ExecStarted)
	}
	// Result should be zero — the execution never completed.
	if retried.Result.Content != "" {
		t.Errorf("in-progress retry result.Content=%q, want empty (not completed)", retried.Result.Content)
	}
}

// TestLookupReturnsRecord exercises the Lookup path used by the recovery
// adjudicator (ADR-0012 §"At-most-once recovery for mutating tools").
func TestLookupReturnsRecord(t *testing.T) {
	h := newDedupHarness(t)

	tenantID, sessionID := h.seedTenantAndSession(t)
	idemKey := "key-" + newUUID(t)

	// Begin + Complete so there is a deterministic final state to look up.
	if _, err := h.store.Begin(ctx, app.ExecutionRecord{
		TenantID:       tenantID,
		SessionID:      sessionID,
		IdempotencyKey: idemKey,
	}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	wantObs := domain.Observation{Content: "result", IsError: false, Truncated: false, BlobRef: ""}
	if err := h.store.Complete(ctx, app.ExecutionRecord{
		TenantID:       tenantID,
		SessionID:      sessionID,
		IdempotencyKey: idemKey,
		Status:         app.ExecCompleted,
		Result:         wantObs,
	}); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	rec, err := h.store.Lookup(ctx, tenantID, sessionID, idemKey)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if rec.Status != app.ExecCompleted {
		t.Errorf("Lookup status=%q, want %q", rec.Status, app.ExecCompleted)
	}
	if rec.Result.Content != wantObs.Content {
		t.Errorf("Lookup result.Content=%q, want %q", rec.Result.Content, wantObs.Content)
	}
}

// TestLookupNotFoundReturnsError verifies Lookup returns ErrNotFound for a key
// that was never registered.
func TestLookupNotFoundReturnsError(t *testing.T) {
	h := newDedupHarness(t)

	tenantID, sessionID := h.seedTenantAndSession(t)

	_, err := h.store.Lookup(ctx, tenantID, sessionID, "nonexistent-key")
	if err == nil {
		t.Fatal("Lookup on absent key: expected error, got nil")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Lookup absent key error=%v, want errors.Is(_, ErrNotFound)", err)
	}
}

// TestConcurrentBeginExactlyOneRow verifies that two goroutines racing to Begin
// the same key result in exactly one row committed — the unique primary key
// (tenant_id, session_id, idempotency_key) is the durability guarantee
// (FR-TOOL-04 AC-2; ADR-0012).
func TestConcurrentBeginExactlyOneRow(t *testing.T) {
	h := newDedupHarness(t)

	tenantID, sessionID := h.seedTenantAndSession(t)
	idemKey := "concurrent-key-" + newUUID(t)

	const goroutines = 10
	results := make([]app.ExecutionRecord, goroutines)
	errs := make([]error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = h.store.Begin(ctx, app.ExecutionRecord{
				TenantID:       tenantID,
				SessionID:      sessionID,
				IdempotencyKey: idemKey,
			})
		}(i)
	}
	wg.Wait()

	// All goroutines must succeed (no error).
	for i, e := range errs {
		if e != nil {
			t.Errorf("goroutine %d Begin error: %v", i, e)
		}
	}

	// All goroutines must see status=started (one row, one status).
	for i, r := range results {
		if r.Status != app.ExecStarted {
			t.Errorf("goroutine %d returned status=%q, want %q", i, r.Status, app.ExecStarted)
		}
	}

	// Verify exactly one physical row via owner connection (bypasses RLS).
	ownerConn, err := pgx.Connect(context.Background(), h.ownerDSN)
	if err != nil {
		t.Fatalf("owner connect: %v", err)
	}
	defer func() { _ = ownerConn.Close(context.Background()) }()

	var count int
	if err := ownerConn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM tool_executions WHERE tenant_id = $1 AND session_id = $2 AND idempotency_key = $3",
		tenantID, sessionID, idemKey,
	).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Errorf("concurrent Begin: got %d rows, want exactly 1", count)
	}
}

// TestTenantScopeRecheckOnLookup verifies that Lookup with tenantA's id in a
// context scoped to tenantB is rejected (architecture §7.3 tenant re-check).
func TestTenantScopeRecheckOnLookup(t *testing.T) {
	h := newDedupHarness(t)

	tenantA, sessionA := h.seedTenantAndSession(t)
	idemKey := "key-" + newUUID(t)

	// Seed a second tenant (no session needed).
	tenantB := newUUID(t)
	ownerConn, err := pgx.Connect(context.Background(), h.ownerDSN)
	if err != nil {
		t.Fatalf("owner connect: %v", err)
	}
	defer func() { _ = ownerConn.Close(context.Background()) }()
	if _, err := ownerConn.Exec(context.Background(),
		"INSERT INTO tenants (id, name) VALUES ($1, 'tenant-b')", tenantB); err != nil {
		t.Fatalf("seed tenant B: %v", err)
	}

	// Insert a record as tenant A.
	if _, err := h.store.Begin(ctx, app.ExecutionRecord{
		TenantID:       tenantA,
		SessionID:      sessionA,
		IdempotencyKey: idemKey,
	}); err != nil {
		t.Fatalf("Begin as tenant A: %v", err)
	}

	// Lookup with tenantA as the requested tenantID but the store must use
	// the exact tenantID from the parameter. To exercise the cross-tenant
	// mismatch guard: call Lookup passing tenantA as tenantID, but using a
	// Store constructed from a pool connecting as the app role (RLS enforced).
	// The real RLS guard: if we SET LOCAL app.current_tenant=tenantB and
	// SELECT WHERE tenant_id=tenantA, RLS blocks the read and returns not found.
	//
	// Directly simulate: create a pool connecting as app role, set tenantB in
	// the GUC (via Begin on a tenantB record), then attempt to read tenantA's
	// row — RLS should return 0 rows.
	pool2, err := NewSimplePool(h.appDSN)
	if err != nil {
		t.Fatalf("pool2: %v", err)
	}
	defer pool2.Close()

	store2 := New(pool2)
	// Lookup tenantA's key but from a "tenantB context": the Store's
	// Lookup(ctx, tenantA, ...) will SET LOCAL app.current_tenant=tenantA,
	// which should still work correctly because we pass tenantA.
	// For the cross-tenant guard, we need to test that Lookup(ctx, tenantB, ...)
	// on tenantA's session returns ErrNotFound (RLS hides it).
	_, lookupErr := store2.Lookup(ctx, tenantB, sessionA, idemKey)
	if lookupErr == nil {
		t.Fatal("Lookup with wrong tenant: expected error, got nil — cross-tenant isolation failed")
	}
	// ErrNotFound (RLS-invisible) is the expected outcome.
	t.Logf("Lookup cross-tenant correctly returned error: %v", lookupErr)
}

// TestCompletePreservesAllObservationFields verifies that the full
// [domain.Observation] round-trips through JSON serialization: Content,
// IsError, Truncated, and BlobRef are all preserved (architecture §7.2).
func TestCompletePreservesAllObservationFields(t *testing.T) {
	h := newDedupHarness(t)

	tenantID, sessionID := h.seedTenantAndSession(t)
	idemKey := "key-" + newUUID(t)

	if _, err := h.store.Begin(ctx, app.ExecutionRecord{
		TenantID:       tenantID,
		SessionID:      sessionID,
		IdempotencyKey: idemKey,
	}); err != nil {
		t.Fatalf("Begin: %v", err)
	}

	wantObs := domain.Observation{
		Content:   "large output truncated",
		IsError:   false,
		Truncated: true,
		BlobRef:   "sha256:abc123",
	}
	if err := h.store.Complete(ctx, app.ExecutionRecord{
		TenantID:       tenantID,
		SessionID:      sessionID,
		IdempotencyKey: idemKey,
		Status:         app.ExecCompleted,
		Result:         wantObs,
	}); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	rec, err := h.store.Lookup(ctx, tenantID, sessionID, idemKey)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	got := rec.Result
	if got.Content != wantObs.Content {
		t.Errorf("Content: got %q, want %q", got.Content, wantObs.Content)
	}
	if got.IsError != wantObs.IsError {
		t.Errorf("IsError: got %v, want %v", got.IsError, wantObs.IsError)
	}
	if got.Truncated != wantObs.Truncated {
		t.Errorf("Truncated: got %v, want %v", got.Truncated, wantObs.Truncated)
	}
	if got.BlobRef != wantObs.BlobRef {
		t.Errorf("BlobRef: got %q, want %q", got.BlobRef, wantObs.BlobRef)
	}
}

// TestKeyNamespaceIsTenantSessionScoped verifies that two sessions under
// different tenants with the same idempotency_key string are independent rows
// and do not interfere with each other (ADR-0012 §"Idempotency key scoping";
// architecture §7.3). Key namespace must be (tenant_id, session_id, key).
func TestKeyNamespaceIsTenantSessionScoped(t *testing.T) {
	h := newDedupHarness(t)

	// Two independent (tenant, session) pairs.
	tenantA, sessionA := h.seedTenantAndSession(t)
	tenantB, sessionB := h.seedTenantAndSession(t)

	// Both use the *same* idempotency_key string.
	sharedKey := "shared-idem-key"

	// Begin + Complete with different observations under each tenant/session.
	obsA := domain.Observation{Content: "result-A"}
	obsB := domain.Observation{Content: "result-B"}

	if _, err := h.store.Begin(ctx, app.ExecutionRecord{
		TenantID:       tenantA,
		SessionID:      sessionA,
		IdempotencyKey: sharedKey,
	}); err != nil {
		t.Fatalf("Begin A: %v", err)
	}
	if err := h.store.Complete(ctx, app.ExecutionRecord{
		TenantID:       tenantA,
		SessionID:      sessionA,
		IdempotencyKey: sharedKey,
		Status:         app.ExecCompleted,
		Result:         obsA,
	}); err != nil {
		t.Fatalf("Complete A: %v", err)
	}

	if _, err := h.store.Begin(ctx, app.ExecutionRecord{
		TenantID:       tenantB,
		SessionID:      sessionB,
		IdempotencyKey: sharedKey,
	}); err != nil {
		t.Fatalf("Begin B: %v", err)
	}
	if err := h.store.Complete(ctx, app.ExecutionRecord{
		TenantID:       tenantB,
		SessionID:      sessionB,
		IdempotencyKey: sharedKey,
		Status:         app.ExecCompleted,
		Result:         obsB,
	}); err != nil {
		t.Fatalf("Complete B: %v", err)
	}

	// Lookup each — they must return independent results.
	recA, err := h.store.Lookup(ctx, tenantA, sessionA, sharedKey)
	if err != nil {
		t.Fatalf("Lookup A: %v", err)
	}
	recB, err := h.store.Lookup(ctx, tenantB, sessionB, sharedKey)
	if err != nil {
		t.Fatalf("Lookup B: %v", err)
	}

	if recA.Result.Content != obsA.Content {
		t.Errorf("Lookup A content=%q, want %q", recA.Result.Content, obsA.Content)
	}
	if recB.Result.Content != obsB.Content {
		t.Errorf("Lookup B content=%q, want %q", recB.Result.Content, obsB.Content)
	}
	if recA.Result.Content == recB.Result.Content {
		t.Errorf("Lookup A and B both returned %q — namespacing failed", recA.Result.Content)
	}
}

// TestCompleteWithFailedStatus verifies ExecFailed is stored and retrieved
// correctly, and the observation's IsError flag is preserved.
func TestCompleteWithFailedStatus(t *testing.T) {
	h := newDedupHarness(t)

	tenantID, sessionID := h.seedTenantAndSession(t)
	idemKey := "key-" + newUUID(t)

	if _, err := h.store.Begin(ctx, app.ExecutionRecord{
		TenantID:       tenantID,
		SessionID:      sessionID,
		IdempotencyKey: idemKey,
	}); err != nil {
		t.Fatalf("Begin: %v", err)
	}

	wantObs := domain.Observation{Content: "bash: command not found", IsError: true}
	if err := h.store.Complete(ctx, app.ExecutionRecord{
		TenantID:       tenantID,
		SessionID:      sessionID,
		IdempotencyKey: idemKey,
		Status:         app.ExecFailed,
		Result:         wantObs,
	}); err != nil {
		t.Fatalf("Complete(failed): %v", err)
	}

	rec, err := h.store.Lookup(ctx, tenantID, sessionID, idemKey)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if rec.Status != app.ExecFailed {
		t.Errorf("status=%q, want %q", rec.Status, app.ExecFailed)
	}
	if !rec.Result.IsError {
		t.Errorf("result.IsError=%v, want true", rec.Result.IsError)
	}
}

// Ensure pgx is visibly used (pgx.Connect is in the concurrent test).
var _ = pgx.ErrNoRows
