//go:build integration

package eventstore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	infradb "github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
)

// appendOne is a convenience to append a single TurnStarted event.
func appendOne(ctx context.Context, s *Store, sessionID string, expected, epoch int64, requestID, turnID string) ([]domain.EventEnvelope, error) {
	return s.Append(ctx, sessionID, expected, epoch, requestID,
		app.AppendInput{Event: domain.TurnStarted{TurnID: turnID, Model: "test-model"}, Actor: domain.ActorSystem})
}

// TestAppend_OptimisticConcurrency_OneWinner covers FR-STATE-01 AC-1: N goroutines
// append with expected_seq=0; exactly one COMMITs (seq=1, head_seq=1) and the
// other N-1 return a typed ConflictError.
func TestAppend_OptimisticConcurrency_OneWinner(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	const n = 20
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
		conflicts int
		others    []error
		winnerSeq int64
	)
	wg.Add(n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start // release all goroutines together to maximize contention
			envs, err := appendOne(ctx, h.store, sessionID, 0, 0, newUUID(t), fmt.Sprintf("turn-%d", i))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
				if len(envs) == 1 {
					winnerSeq = envs[0].Seq
				}
			case errIsAny(err, app.ConflictError):
				conflicts++
			default:
				others = append(others, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if len(others) > 0 {
		t.Fatalf("unexpected non-conflict errors: %v", others)
	}
	if successes != 1 {
		t.Fatalf("expected exactly 1 successful append, got %d (conflicts=%d)", successes, conflicts)
	}
	if conflicts != n-1 {
		t.Fatalf("expected %d ConflictErrors, got %d", n-1, conflicts)
	}
	if winnerSeq != 1 {
		t.Fatalf("winner seq = %d, want 1", winnerSeq)
	}

	sess, err := h.store.LoadSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if sess.HeadSeq != 1 {
		t.Fatalf("head_seq = %d, want 1", sess.HeadSeq)
	}
}

// TestAppend_Idempotent_ReplayReturnsOriginal covers FR-STATE-01 AC-2: re-sending
// the same request_id returns the original envelope (success), not a conflict,
// and writes no duplicate row.
func TestAppend_Idempotent_ReplayReturnsOriginal(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	reqID := newUUID(t)
	first, err := appendOne(ctx, h.store, sessionID, 0, 0, reqID, "turn-A")
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	if len(first) != 1 || first[0].Seq != 1 {
		t.Fatalf("first append: got %d envelopes, seq=%d; want 1 envelope seq=1", len(first), seqOf(first))
	}

	// Re-send the SAME request_id (a lost-ACK replay). expected_seq is still 0,
	// which would normally conflict — but idempotency short-circuits first.
	replay, err := appendOne(ctx, h.store, sessionID, 0, 0, reqID, "turn-A")
	if err != nil {
		t.Fatalf("replay append: expected success, got %v", err)
	}
	if len(replay) != 1 {
		t.Fatalf("replay returned %d envelopes, want 1", len(replay))
	}
	if replay[0].Seq != first[0].Seq || replay[0].RequestID != reqID {
		t.Fatalf("replay envelope = (seq=%d,req=%s), want (seq=%d,req=%s)",
			replay[0].Seq, replay[0].RequestID, first[0].Seq, reqID)
	}

	// No duplicate row: exactly one event for this session.
	if got := h.countEvents(t, sessionID); got != 1 {
		t.Fatalf("event count = %d, want 1 (no duplicate row on idempotent replay)", got)
	}
}

// TestAppend_FencedAndNonActive covers FR-STATE-01 AC-3: a stale lease_epoch
// returns FencedError even when expected_seq is current; an append to a
// non-active session returns SessionNotActiveError.
func TestAppend_FencedAndNonActive(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	// Take over the lease: bump lease_epoch to 1 on the session (simulating a new
	// owner). Use the app role under the tenant so RLS admits the update.
	h.bumpLeaseEpoch(ctx, t, sessionID, 1)

	// A writer with the STALE epoch (0) and the CURRENT expected_seq (0) must be
	// fenced, not conflicted.
	_, err := appendOne(ctx, h.store, sessionID, 0, 0, newUUID(t), "turn-fenced")
	if !errIsAny(err, app.FencedError) {
		t.Fatalf("stale-epoch append: got %v, want FencedError", err)
	}
	if errIsAny(err, app.ConflictError) {
		t.Fatal("stale-epoch append must NOT be a ConflictError (fencing precedes head check)")
	}

	// Now append correctly with the current epoch to advance, then finish the
	// session and assert appends are rejected as not-active.
	if _, err := appendOne(ctx, h.store, sessionID, 0, 1, newUUID(t), "turn-ok"); err != nil {
		t.Fatalf("correct-epoch append: %v", err)
	}
	if err := h.store.SetSessionStatus(ctx, sessionID, 1, domain.StatusFinished); err != nil {
		t.Fatalf("SetSessionStatus(finished): %v", err)
	}
	_, err = appendOne(ctx, h.store, sessionID, 1, 1, newUUID(t), "turn-after-finish")
	if !errIsAny(err, app.SessionNotActiveError) {
		t.Fatalf("append to finished session: got %v, want SessionNotActiveError", err)
	}
}

// TestAppend_Contiguity covers contiguous seq assignment across single- and
// multi-event appends: seqs are 1,2,3,... with no gaps, tied to the head
// transition.
func TestAppend_Contiguity(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	// Single append -> seq 1.
	if _, err := appendOne(ctx, h.store, sessionID, 0, 0, newUUID(t), "t1"); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	// Multi-event append -> seqs 2,3 in one transaction.
	envs, err := h.store.Append(ctx, sessionID, 1, 0, newUUID(t),
		app.AppendInput{Event: domain.TurnStarted{TurnID: "t2"}},
		app.AppendInput{Event: domain.TurnFinished{TurnID: "t2", Reason: domain.Success, NumTurns: 1}},
	)
	if err != nil {
		t.Fatalf("multi append: %v", err)
	}
	if len(envs) != 2 || envs[0].Seq != 2 || envs[1].Seq != 3 {
		t.Fatalf("multi append seqs = %v, want [2 3]", seqsOf(envs))
	}

	loaded, err := h.store.Load(ctx, sessionID, 1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 3 {
		t.Fatalf("loaded %d events, want 3", len(loaded))
	}
	for i, e := range loaded {
		if e.Seq != int64(i+1) {
			t.Fatalf("event %d has seq %d, want %d (contiguity)", i, e.Seq, i+1)
		}
	}
}

// TestAppend_PlanUpdated_RoundTrip covers AC-12 (Gap #3): a domain.PlanUpdated
// event Appends and Loads back through the REAL pgx insert/scan + decodePayload
// path with its Items intact. It proves event_type "PlanUpdated" persists to the
// events column (the closed-set tag survives the DB) and that decodePayload
// reconstructs the typed payload — the durable, time-travelable planning
// primitive is genuinely round-trippable against Postgres, not just in the unit
// codec test. It also asserts the empty-plan (nil Items) edge case round-trips.
func TestAppend_PlanUpdated_RoundTrip(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	want := domain.PlanUpdated{
		TurnID: "turn-plan",
		Items: []domain.PlanItem{
			{Content: "investigate the failing test", Status: domain.PlanStatusCompleted},
			{Content: "wire the virtual tool", Status: domain.PlanStatusInProgress},
			{Content: "write the ADR", Status: domain.PlanStatusPending},
		},
	}

	envs, err := h.store.Append(ctx, sessionID, 0, 0, newUUID(t),
		app.AppendInput{Event: want, Actor: domain.ActorAssistant})
	if err != nil {
		t.Fatalf("Append(PlanUpdated): %v", err)
	}
	if len(envs) != 1 || envs[0].Seq != 1 {
		t.Fatalf("Append returned %d envelopes (seq %d), want 1 at seq 1", len(envs), seqOf(envs))
	}
	if envs[0].Type != domain.EventPlanUpdated {
		t.Fatalf("appended envelope Type = %q, want %q", envs[0].Type, domain.EventPlanUpdated)
	}

	// The event_type column persists the closed-set tag verbatim.
	owner := h.ownerConn(t)
	var gotType string
	if err := owner.QueryRow(context.Background(),
		"SELECT event_type FROM events WHERE session_id = $1 AND seq = 1", sessionID).Scan(&gotType); err != nil {
		t.Fatalf("read event_type: %v", err)
	}
	if gotType != string(domain.EventPlanUpdated) {
		t.Fatalf("persisted event_type = %q, want %q", gotType, domain.EventPlanUpdated)
	}

	// Load back through scanEnvelopes -> decodePayload and assert Items survive.
	loaded, err := h.store.Load(ctx, sessionID, 1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("loaded %d events, want 1", len(loaded))
	}
	if loaded[0].Type != domain.EventPlanUpdated {
		t.Fatalf("loaded envelope Type = %q, want %q", loaded[0].Type, domain.EventPlanUpdated)
	}
	got, ok := loaded[0].Event.(domain.PlanUpdated)
	if !ok {
		t.Fatalf("loaded payload type = %T, want domain.PlanUpdated", loaded[0].Event)
	}
	if got.TurnID != want.TurnID {
		t.Fatalf("loaded TurnID = %q, want %q", got.TurnID, want.TurnID)
	}
	if len(got.Items) != len(want.Items) {
		t.Fatalf("loaded %d items, want %d", len(got.Items), len(want.Items))
	}
	for i := range want.Items {
		if got.Items[i] != want.Items[i] {
			t.Fatalf("item %d = %+v, want %+v", i, got.Items[i], want.Items[i])
		}
	}

	// Empty-plan edge case: an empty Items slice is a valid plan and must
	// round-trip without becoming a non-PlanUpdated event or panicking.
	emptyEnvs, err := h.store.Append(ctx, sessionID, 1, 0, newUUID(t),
		app.AppendInput{Event: domain.PlanUpdated{TurnID: "turn-empty"}, Actor: domain.ActorAssistant})
	if err != nil {
		t.Fatalf("Append(empty PlanUpdated): %v", err)
	}
	if emptyEnvs[0].Seq != 2 {
		t.Fatalf("empty-plan append seq = %d, want 2", emptyEnvs[0].Seq)
	}
	loadedEmpty, err := h.store.Load(ctx, sessionID, 2)
	if err != nil {
		t.Fatalf("Load empty: %v", err)
	}
	if len(loadedEmpty) != 1 {
		t.Fatalf("loaded %d events from seq 2, want 1", len(loadedEmpty))
	}
	gotEmpty, ok := loadedEmpty[0].Event.(domain.PlanUpdated)
	if !ok {
		t.Fatalf("loaded empty payload type = %T, want domain.PlanUpdated", loadedEmpty[0].Event)
	}
	if len(gotEmpty.Items) != 0 {
		t.Fatalf("empty-plan loaded %d items, want 0", len(gotEmpty.Items))
	}
}

// TestRLS_PredicateRemoved covers FR-STATE-04 AC-1 / NFR-TEST-05(a): with
// app.current_tenant = A on the non-owner role, SELECT * FROM events WITHOUT a
// tenant_id predicate returns only A's rows.
func TestRLS_PredicateRemoved(t *testing.T) {
	h := newHarness(t)

	// Seed tenant A and tenant B, each with a session, via the app role.
	tenantA := newUUID(t)
	sessionA := newUUID(t)
	tenantB := newUUID(t)
	sessionB := newUUID(t)
	ctxA := tenantCtx(tenantA)
	ctxB := tenantCtx(tenantB)
	if err := h.store.CreateTenant(ctxA, tenantA, "A"); err != nil {
		t.Fatalf("create tenant A: %v", err)
	}
	if err := h.store.CreateTenant(ctxB, tenantB, "B"); err != nil {
		t.Fatalf("create tenant B: %v", err)
	}
	if _, err := h.store.CreateSession(ctxA, sessionA, domain.ModeDefault); err != nil {
		t.Fatalf("create session A: %v", err)
	}
	if _, err := h.store.CreateSession(ctxB, sessionB, domain.ModeDefault); err != nil {
		t.Fatalf("create session B: %v", err)
	}
	// 10 events for A, 10 for B.
	for i := int64(0); i < 10; i++ {
		if _, err := appendOne(ctxA, h.store, sessionA, i, 0, newUUID(t), fmt.Sprintf("a-%d", i)); err != nil {
			t.Fatalf("append A %d: %v", i, err)
		}
		if _, err := appendOne(ctxB, h.store, sessionB, i, 0, newUUID(t), fmt.Sprintf("b-%d", i)); err != nil {
			t.Fatalf("append B %d: %v", i, err)
		}
	}

	// As the NON-OWNER app role with app.current_tenant=A, run SELECT COUNT(*)
	// FROM events with NO tenant predicate. RLS must return only A's 10 rows.
	pc, err := h.pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire app conn: %v", err)
	}
	defer pc.Release()
	tx, err := pc.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := setLocalTenant(context.Background(), tx, tenantA); err != nil {
		t.Fatalf("set tenant: %v", err)
	}
	var count int
	// NOTE: deliberately NO "WHERE tenant_id = ..." predicate — RLS is the only
	// thing scoping this query.
	if err := tx.QueryRow(context.Background(), "SELECT COUNT(*) FROM events").Scan(&count); err != nil {
		t.Fatalf("predicate-removed select: %v", err)
	}
	if count != 10 {
		t.Fatalf("predicate-removed SELECT returned %d rows, want 10 (tenant A only) — RLS not enforced", count)
	}
}

// TestFork_SeqContinuation covers FR-STATE-03 AC-1: fork at at_seq=5, append two
// more to the parent (reaching seq=7) and one to the child (reaching seq=6), with
// no collision and independent loads.
func TestFork_SeqContinuation(t *testing.T) {
	h := newHarness(t)
	tenantID, parentID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	// Drive the parent to head_seq=5.
	for i := int64(0); i < 5; i++ {
		if _, err := appendOne(ctx, h.store, parentID, i, 0, newUUID(t), fmt.Sprintf("p-%d", i)); err != nil {
			t.Fatalf("parent append %d: %v", i, err)
		}
	}

	childID := newUUID(t)
	child, err := h.store.Fork(ctx, parentID, 5, childID)
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if child.ForkedFromSeq != 5 || child.HeadSeq != 5 {
		t.Fatalf("child forked_from_seq=%d head_seq=%d, want 5/5", child.ForkedFromSeq, child.HeadSeq)
	}

	// Parent continues: two more appends -> seq 6, 7.
	p6, err := appendOne(ctx, h.store, parentID, 5, 0, newUUID(t), "p-5")
	if err != nil {
		t.Fatalf("parent append seq6: %v", err)
	}
	p7, err := appendOne(ctx, h.store, parentID, 6, 0, newUUID(t), "p-6")
	if err != nil {
		t.Fatalf("parent append seq7: %v", err)
	}
	if p6[0].Seq != 6 || p7[0].Seq != 7 {
		t.Fatalf("parent continuation seqs = %d,%d, want 6,7", p6[0].Seq, p7[0].Seq)
	}

	// Child continues from at_seq+1=6.
	c6, err := appendOne(ctx, h.store, childID, 5, 0, newUUID(t), "c-5")
	if err != nil {
		t.Fatalf("child append: %v", err)
	}
	if c6[0].Seq != 6 {
		t.Fatalf("child first seq = %d, want 6 (continues at at_seq+1)", c6[0].Seq)
	}

	// Independent loads: parent has its own 7 events, child has only its 1 own event.
	parentEvents, err := h.store.Load(ctx, parentID, 1)
	if err != nil {
		t.Fatalf("load parent: %v", err)
	}
	if len(parentEvents) != 7 {
		t.Fatalf("parent has %d events, want 7", len(parentEvents))
	}
	childEvents, err := h.store.Load(ctx, childID, 1)
	if err != nil {
		t.Fatalf("load child: %v", err)
	}
	if len(childEvents) != 1 || childEvents[0].Seq != 6 {
		t.Fatalf("child has %d own events (first seq %d), want 1 at seq 6", len(childEvents), seqOf(childEvents))
	}
}

// TestFork_CrossTenantDenied covers FR-STATE-03 AC-2 / NFR-TEST-05(c): forking a
// session owned by a different tenant is denied (the parent is invisible under
// RLS, surfaced as SessionNotActiveError), never silently creating a child.
func TestFork_CrossTenantDenied(t *testing.T) {
	h := newHarness(t)
	// Parent belongs to tenant A.
	tenantA, parentID := h.seedTenantAndSession(t)
	_ = tenantA

	// Tenant B attempts to fork A's session.
	tenantB := newUUID(t)
	ctxB := tenantCtx(tenantB)
	if err := h.store.CreateTenant(ctxB, tenantB, "B"); err != nil {
		t.Fatalf("create tenant B: %v", err)
	}
	childID := newUUID(t)
	_, err := h.store.Fork(ctxB, parentID, 0, childID)
	if !errIsAny(err, app.SessionNotActiveError) {
		t.Fatalf("cross-tenant fork: got %v, want SessionNotActiveError (the PERMISSION_DENIED equivalent)", err)
	}

	// And no child row leaked into tenant B.
	if _, err := h.store.LoadSession(ctxB, childID); !errIsAny(err, app.SessionNotActiveError) {
		t.Fatalf("child session should not exist for tenant B, LoadSession got %v", err)
	}
}

// TestBlobInSameTx covers FR-STATE-05 AC-1: a large (64 KiB) tool output is
// referenced by the events row, and the blobs row exists in the SAME committed
// transaction.
func TestBlobInSameTx(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	const ref = "sha256:deadbeef-64kib"
	blob := BlobUpload{Ref: ref, MediaType: "text/plain", SizeBytes: 64 * 1024, StorageURI: "file://tenant/" + ref}
	evs, err := h.store.AppendWithBlob(ctx, sessionID, 0, 0, newUUID(t), blob,
		app.AppendInput{
			Event: domain.ToolResult{CallID: "c1", Result: "<<64KiB offloaded>>", Truncated: true, BlobRef: ref},
			Actor: domain.ActorTool,
		})
	if err != nil {
		t.Fatalf("AppendWithBlob: %v", err)
	}
	if len(evs) != 1 || evs[0].Seq != 1 {
		t.Fatalf("AppendWithBlob returned %d envelopes (seq %d), want 1 at seq 1", len(evs), seqOf(evs))
	}

	// The events row has the blob_ref...
	owner := h.ownerConn(t)
	var gotRef *string
	if err := owner.QueryRow(context.Background(),
		"SELECT blob_ref FROM events WHERE session_id = $1 AND seq = 1", sessionID).Scan(&gotRef); err != nil {
		t.Fatalf("read event blob_ref: %v", err)
	}
	if gotRef == nil || *gotRef != ref {
		t.Fatalf("event blob_ref = %v, want %q", gotRef, ref)
	}
	// ...and the blobs row exists in the same tenant.
	var blobCount int
	if err := owner.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM blobs WHERE tenant_id = $1 AND ref = $2", tenantID, ref).Scan(&blobCount); err != nil {
		t.Fatalf("read blobs row: %v", err)
	}
	if blobCount != 1 {
		t.Fatalf("blobs row count = %d, want 1 (committed in the same tx)", blobCount)
	}
}

// TestBlob_TxFailLeavesNoEventOrRef covers FR-STATE-05 AC-2: if the append
// transaction fails (here: an optimistic conflict because expected_seq is wrong)
// AFTER the bytes were written, no event row and no dangling blobs row exist for
// that ref (the bytes are reclaimed by the sweeper, out of scope here).
func TestBlob_TxFailLeavesNoEventOrRef(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	// Bytes are "written" to the blob store by the caller before AppendWithBlob;
	// we simulate the tx failing by passing a wrong expected_seq (head is 0).
	const ref = "sha256:orphan-ref"
	blob := BlobUpload{Ref: ref, MediaType: "text/plain", SizeBytes: 64 * 1024, StorageURI: "file://tenant/" + ref}
	_, err := h.store.AppendWithBlob(ctx, sessionID, 99 /* wrong */, 0, newUUID(t), blob,
		app.AppendInput{Event: domain.ToolResult{CallID: "c1", Result: "x", BlobRef: ref}, Actor: domain.ActorTool})
	if !errIsAny(err, app.ConflictError) {
		t.Fatalf("expected ConflictError from wrong expected_seq, got %v", err)
	}

	owner := h.ownerConn(t)
	var eventCount, blobCount int
	if err := owner.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM events WHERE session_id = $1", sessionID).Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 0 {
		t.Fatalf("event count = %d, want 0 (failed tx leaves no event)", eventCount)
	}
	if err := owner.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM blobs WHERE tenant_id = $1 AND ref = $2", tenantID, ref).Scan(&blobCount); err != nil {
		t.Fatalf("count blobs: %v", err)
	}
	if blobCount != 0 {
		t.Fatalf("blobs row count = %d, want 0 (no dangling ref; the row is rolled back with the tx)", blobCount)
	}
}

// TestSubscribe_DeliversThenTailsThenClosesOnCancel covers the Subscribe
// contract: Subscribe(fromSeq=N) delivers only seq>N (catch-up), then tails
// newly-appended events live, then closes the channel on ctx cancel.
func TestSubscribe_DeliversThenTailsThenClosesOnCancel(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	baseCtx := tenantCtx(tenantID)

	// Pre-seed 3 events (seq 1,2,3).
	for i := int64(0); i < 3; i++ {
		if _, err := appendOne(baseCtx, h.store, sessionID, i, 0, newUUID(t), fmt.Sprintf("pre-%d", i)); err != nil {
			t.Fatalf("preseed %d: %v", i, err)
		}
	}

	subCtx, cancel := context.WithCancel(baseCtx)
	ch, err := h.store.Subscribe(subCtx, sessionID, 1) // fromSeq=1 -> deliver seq>1
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	var (
		mu       sync.Mutex
		received []int64
		closed   bool
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for env := range ch {
			mu.Lock()
			received = append(received, env.Seq)
			mu.Unlock()
		}
		mu.Lock()
		closed = true
		mu.Unlock()
	}()

	// Catch-up should deliver seq 2 and 3 (NOT seq 1).
	if !waitFor(5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) >= 2
	}) {
		t.Fatalf("catch-up did not deliver: got %v", snapshot(&mu, &received))
	}

	// Append a new event (seq 4); the tail should deliver it.
	if _, err := appendOne(baseCtx, h.store, sessionID, 3, 0, newUUID(t), "live-4"); err != nil {
		t.Fatalf("live append: %v", err)
	}
	if !waitFor(5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) >= 3
	}) {
		t.Fatalf("tail did not deliver the live event: got %v", snapshot(&mu, &received))
	}

	got := snapshot(&mu, &received)
	// First two delivered are the catch-up 2,3; the live one is 4. seq 1 must be excluded.
	for _, s := range got {
		if s == 1 {
			t.Fatalf("Subscribe delivered seq 1, but fromSeq=1 must exclude it; got %v", got)
		}
	}
	if got[0] != 2 || got[1] != 3 {
		t.Fatalf("catch-up order = %v, want first 2,3", got)
	}
	if got[len(got)-1] != 4 {
		t.Fatalf("tail did not deliver seq 4 last; got %v", got)
	}

	// Cancel -> channel closes.
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe channel did not close within 5s of ctx cancel")
	}
	mu.Lock()
	defer mu.Unlock()
	if !closed {
		t.Fatal("subscription goroutine did not observe channel close")
	}
}

// ---------------------------------------------------------------------------
// integration-only helpers
// ---------------------------------------------------------------------------

// countEvents returns the number of events for a session, read as owner.
func (h *harness) countEvents(t *testing.T, sessionID string) int {
	t.Helper()
	owner := h.ownerConn(t)
	var n int
	if err := owner.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM events WHERE session_id = $1", sessionID).Scan(&n); err != nil {
		t.Fatalf("countEvents: %v", err)
	}
	return n
}

// bumpLeaseEpoch sets a session's lease_epoch to epoch via the app role (under
// the tenant ctx so RLS admits the UPDATE), simulating a lease takeover.
func (h *harness) bumpLeaseEpoch(ctx context.Context, t *testing.T, sessionID string, epoch int64) {
	t.Helper()
	pc, err := h.pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer pc.Release()
	tx, err := pc.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tenantID, err := infradb.TenantFromContext(ctx)
	if err != nil {
		t.Fatalf("tenant from ctx: %v", err)
	}
	if err := setLocalTenant(ctx, tx, tenantID); err != nil {
		t.Fatalf("set tenant: %v", err)
	}
	if _, err := tx.Exec(ctx, "UPDATE sessions SET lease_epoch = $1 WHERE id = $2", epoch, sessionID); err != nil {
		t.Fatalf("bump lease: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit bump: %v", err)
	}
}

// seqOf returns the seq of the first envelope, or -1 when empty (test diagnostics).
func seqOf(envs []domain.EventEnvelope) int64 {
	if len(envs) == 0 {
		return -1
	}
	return envs[0].Seq
}

// seqsOf returns the seqs of all envelopes (test diagnostics).
func seqsOf(envs []domain.EventEnvelope) []int64 {
	out := make([]int64, len(envs))
	for i, e := range envs {
		out[i] = e.Seq
	}
	return out
}

// snapshot copies the received slice under the mutex (test diagnostics).
func snapshot(mu *sync.Mutex, s *[]int64) []int64 {
	mu.Lock()
	defer mu.Unlock()
	return append([]int64(nil), (*s)...)
}

// TestStore_SessionMode_PersistsLoadsAndForkInherits verifies the session
// permission mode round-trips through the REAL schema (sessions.mode; ADR-0019,
// migration 0004): CreateSession persists the requested mode, LoadSession reads
// it back, a fork inherits the parent's mode, and the default path stores
// 'default'. This exercises the actual SQL + CHECK constraint the unit-level fake
// cannot.
func TestStore_SessionMode_PersistsLoadsAndForkInherits(t *testing.T) {
	h := newHarness(t)
	tenantID := newUUID(t)
	sessionID := newUUID(t)
	ctx := tenantCtx(tenantID)

	if err := h.store.CreateTenant(ctx, tenantID, "mode-tenant"); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}

	// CreateSession persists the requested (non-default) mode and returns it.
	created, err := h.store.CreateSession(ctx, sessionID, domain.ModePlan)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if created.Mode != domain.ModePlan {
		t.Fatalf("CreateSession returned mode %q, want %q", created.Mode, domain.ModePlan)
	}

	// LoadSession reads the persisted mode back.
	loaded, err := h.store.LoadSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if loaded.Mode != domain.ModePlan {
		t.Fatalf("LoadSession mode = %q, want %q", loaded.Mode, domain.ModePlan)
	}

	// A fork inherits the parent's mode. Append one event so there is a head to
	// fork at (head 0 -> 1).
	if _, err := appendOne(ctx, h.store, sessionID, 0, 0, newUUID(t), "m-0"); err != nil {
		t.Fatalf("append: %v", err)
	}
	child, err := h.store.Fork(ctx, sessionID, 1, newUUID(t))
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if child.Mode != domain.ModePlan {
		t.Fatalf("forked child mode = %q, want %q (fork must inherit parent mode)", child.Mode, domain.ModePlan)
	}

	// The default-mode path stores and reads 'default'.
	defSession := newUUID(t)
	if _, err := h.store.CreateSession(ctx, defSession, domain.ModeDefault); err != nil {
		t.Fatalf("CreateSession default: %v", err)
	}
	def, err := h.store.LoadSession(ctx, defSession)
	if err != nil {
		t.Fatalf("LoadSession default: %v", err)
	}
	if def.Mode != domain.ModeDefault {
		t.Fatalf("default-mode session loaded mode = %q, want %q", def.Mode, domain.ModeDefault)
	}
}
