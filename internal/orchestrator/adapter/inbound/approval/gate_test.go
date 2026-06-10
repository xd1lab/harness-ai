package approval_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/approval"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// compile-time assertion: *Gate satisfies app.ApprovalGate.
var _ app.ApprovalGate = (*approval.Gate)(nil)

// TestRequestBlocksUntilResolve verifies the primary contract: Request blocks and
// returns exactly the resolution delivered by a concurrent Resolve.
func TestRequestBlocksUntilResolve(t *testing.T) {
	t.Parallel()
	g := approval.New()

	req := app.ApprovalRequest{
		SessionID: "session-1",
		CallID:    "call-1",
		ToolName:  "bash",
		Reason:    "mutating tool",
	}

	// Channel to capture the result of Request.
	type result struct {
		res domain.AskResolution
		err error
	}
	ch := make(chan result, 1)

	ctx := context.Background()

	go func() {
		res, err := g.Request(ctx, req)
		ch <- result{res, err}
	}()

	// Give the goroutine time to register and block before we resolve.
	time.Sleep(10 * time.Millisecond)

	if err := g.Resolve(ctx, "session-1", "call-1", domain.AskAllowed); err != nil {
		t.Fatalf("Resolve: unexpected error: %v", err)
	}

	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("Request returned error: %v", r.err)
		}
		if r.res != domain.AskAllowed {
			t.Fatalf("got resolution %q, want %q", r.res, domain.AskAllowed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Request did not unblock within timeout")
	}
}

// TestRequestDenied verifies that AskDenied resolves Request without error.
func TestRequestDenied(t *testing.T) {
	t.Parallel()
	g := approval.New()

	type result struct {
		res domain.AskResolution
		err error
	}
	ch := make(chan result, 1)
	ctx := context.Background()

	go func() {
		res, err := g.Request(ctx, app.ApprovalRequest{
			SessionID: "session-denied",
			CallID:    "call-denied",
		})
		ch <- result{res, err}
	}()

	time.Sleep(10 * time.Millisecond)

	if err := g.Resolve(ctx, "session-denied", "call-denied", domain.AskDenied); err != nil {
		t.Fatalf("Resolve: unexpected error: %v", err)
	}

	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("Request returned error: %v", r.err)
		}
		if r.res != domain.AskDenied {
			t.Fatalf("got resolution %q, want %q", r.res, domain.AskDenied)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Request did not unblock within timeout")
	}
}

// TestResolveWithNoPendingRequestErrors verifies that a Resolve call with no
// matching pending Request returns a non-nil error.
func TestResolveWithNoPendingRequestErrors(t *testing.T) {
	t.Parallel()
	g := approval.New()

	err := g.Resolve(context.Background(), "no-such-session", "no-such-call", domain.AskAllowed)
	if err == nil {
		t.Fatal("expected error resolving a non-pending (sessionID, callID), got nil")
	}
}

// TestContextCancelUnblocksRequest verifies that cancelling the context causes
// Request to return ctx.Err() (context.Canceled).
func TestContextCancelUnblocksRequest(t *testing.T) {
	t.Parallel()
	g := approval.New()

	ctx, cancel := context.WithCancel(context.Background())

	type result struct {
		res domain.AskResolution
		err error
	}
	ch := make(chan result, 1)

	go func() {
		res, err := g.Request(ctx, app.ApprovalRequest{
			SessionID: "session-cancel",
			CallID:    "call-cancel",
		})
		ch <- result{res, err}
	}()

	// Give goroutine time to register and block.
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case r := <-ch:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Request did not unblock after context cancel")
	}
}

// TestContextDeadlineUnblocksRequest verifies that a deadline-exceeded context
// also unblocks Request with ctx.Err().
func TestContextDeadlineUnblocksRequest(t *testing.T) {
	t.Parallel()
	g := approval.New()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	type result struct {
		res domain.AskResolution
		err error
	}
	ch := make(chan result, 1)

	go func() {
		res, err := g.Request(ctx, app.ApprovalRequest{
			SessionID: "session-deadline",
			CallID:    "call-deadline",
		})
		ch <- result{res, err}
	}()

	select {
	case r := <-ch:
		if !errors.Is(r.err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Request did not unblock after context deadline")
	}
}

// TestContextCancelLeavesNoPendingEntry verifies that after a cancelled Request
// a subsequent Resolve for the same key returns an error (the pending entry was
// cleaned up).
func TestContextCancelLeavesNoPendingEntry(t *testing.T) {
	t.Parallel()
	g := approval.New()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = g.Request(ctx, app.ApprovalRequest{
			SessionID: "session-cleanup",
			CallID:    "call-cleanup",
		})
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()
	<-done // wait for Request to return

	// After cancellation the entry must be cleaned up.
	err := g.Resolve(context.Background(), "session-cleanup", "call-cleanup", domain.AskAllowed)
	if err == nil {
		t.Fatal("expected error after cancelled-and-cleaned request, got nil")
	}
}

// TestConcurrentSessionsAreIsolated verifies that multiple concurrent sessions
// do not interfere with one another: each Resolve only unblocks the matching
// Request.
func TestConcurrentSessionsAreIsolated(t *testing.T) {
	t.Parallel()
	const n = 20
	g := approval.New()
	ctx := context.Background()

	type result struct {
		sessionID string
		res       domain.AskResolution
		err       error
	}

	results := make(chan result, n)
	var wg sync.WaitGroup
	wg.Add(n)

	for i := range n {
		sessionID := "session-" + itoa(i)
		callID := "call-" + itoa(i)
		go func() {
			defer wg.Done()
			res, err := g.Request(ctx, app.ApprovalRequest{
				SessionID: sessionID,
				CallID:    callID,
			})
			results <- result{sessionID, res, err}
		}()
	}

	// Give all goroutines time to register.
	time.Sleep(20 * time.Millisecond)

	// Resolve in reverse order to exercise arbitrary orderings.
	for i := n - 1; i >= 0; i-- {
		sessionID := "session-" + itoa(i)
		callID := "call-" + itoa(i)
		want := domain.AskAllowed
		if i%2 == 0 {
			want = domain.AskDenied
		}
		if err := g.Resolve(ctx, sessionID, callID, want); err != nil {
			t.Errorf("Resolve(%s,%s): %v", sessionID, callID, err)
		}
	}

	wg.Wait()
	close(results)

	gotCount := 0
	for r := range results {
		gotCount++
		if r.err != nil {
			t.Errorf("session %s: unexpected error %v", r.sessionID, r.err)
		}
	}
	if gotCount != n {
		t.Errorf("got %d results, want %d", gotCount, n)
	}
}

// TestSameSessionDifferentCallsAreIsolated exercises multiple simultaneous
// pending approvals within a single session (different callIDs).
func TestSameSessionDifferentCallsAreIsolated(t *testing.T) {
	t.Parallel()
	g := approval.New()
	ctx := context.Background()

	const sessionID = "shared-session"
	calls := []string{"call-A", "call-B", "call-C"}
	resolutions := map[string]domain.AskResolution{
		"call-A": domain.AskAllowed,
		"call-B": domain.AskDenied,
		"call-C": domain.AskAllowed,
	}

	type result struct {
		callID string
		res    domain.AskResolution
		err    error
	}
	ch := make(chan result, len(calls))

	for _, callID := range calls {
		go func(cid string) {
			res, err := g.Request(ctx, app.ApprovalRequest{
				SessionID: sessionID,
				CallID:    cid,
			})
			ch <- result{cid, res, err}
		}(callID)
	}

	time.Sleep(20 * time.Millisecond)

	for _, callID := range calls {
		if err := g.Resolve(ctx, sessionID, callID, resolutions[callID]); err != nil {
			t.Errorf("Resolve(%s): %v", callID, err)
		}
	}

	deadline := time.After(2 * time.Second)
	for range calls {
		select {
		case r := <-ch:
			if r.err != nil {
				t.Errorf("call %s: unexpected error %v", r.callID, r.err)
			}
			want := resolutions[r.callID]
			if r.res != want {
				t.Errorf("call %s: got %q, want %q", r.callID, r.res, want)
			}
		case <-deadline:
			t.Fatal("timed out waiting for all Request goroutines to return")
		}
	}
}

// approvalNotifier mirrors the Run relay's optional ApprovalNotifier capability
// (internal/orchestrator/adapter/inbound/grpc). Asserting *Gate satisfies it here
// guards the gate's notifier surface against regressing without importing the grpc
// package into this test (the orchestratord wiring holds the real cross-package
// assertion). A gate that loses this method makes default-mode tool calls hang with
// no client-visible ApprovalRequest frame.
type approvalNotifier interface {
	SubscribeApprovals(sessionID string, fn func(app.ApprovalRequest)) func()
}

var _ approvalNotifier = (*approval.Gate)(nil)

// TestSubscribeApprovals_NotifiesPendingAsk verifies the ApprovalNotifier path the
// Run relay depends on: a registered subscriber is invoked with the ApprovalRequest
// when a Request lands, so the relay can emit an in-band ApprovalRequest frame while
// the loop waits. Without it the gate blocks invisibly and the client never sees the
// pending ask (the quickstart hang this closes).
func TestSubscribeApprovals_NotifiesPendingAsk(t *testing.T) {
	t.Parallel()
	g := approval.New()
	ctx := context.Background()

	got := make(chan app.ApprovalRequest, 1)
	cancelSub := g.SubscribeApprovals("session-sub", func(r app.ApprovalRequest) { got <- r })
	defer cancelSub()

	req := app.ApprovalRequest{SessionID: "session-sub", CallID: "call-sub", ToolName: "bash", Reason: "mutating"}
	go func() { _, _ = g.Request(ctx, req) }()

	select {
	case r := <-got:
		if r.CallID != "call-sub" || r.ToolName != "bash" {
			t.Fatalf("subscriber got %+v, want call-sub/bash", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber was not notified of the pending approval")
	}

	// The pending entry was registered, so Resolve must succeed and unblock Request.
	if err := g.Resolve(ctx, "session-sub", "call-sub", domain.AskAllowed); err != nil {
		t.Fatalf("Resolve after notify: %v", err)
	}
}

// TestSubscribeApprovals_CancelStopsDelivery verifies that after the returned cancel
// func runs, a later Request no longer notifies the removed subscriber.
func TestSubscribeApprovals_CancelStopsDelivery(t *testing.T) {
	t.Parallel()
	g := approval.New()
	ctx := context.Background()

	var mu sync.Mutex
	calls := 0
	cancelSub := g.SubscribeApprovals("session-x", func(app.ApprovalRequest) {
		mu.Lock()
		calls++
		mu.Unlock()
	})
	cancelSub() // unsubscribe before any request

	go func() { _, _ = g.Request(ctx, app.ApprovalRequest{SessionID: "session-x", CallID: "call-x"}) }()
	time.Sleep(20 * time.Millisecond)
	_ = g.Resolve(ctx, "session-x", "call-x", domain.AskAllowed)

	mu.Lock()
	defer mu.Unlock()
	if calls != 0 {
		t.Fatalf("cancelled subscriber was notified %d times, want 0", calls)
	}
}

// TestSubscribeApprovals_ScopedToSession verifies a subscriber is notified only for
// its own session's requests, not another session's.
func TestSubscribeApprovals_ScopedToSession(t *testing.T) {
	t.Parallel()
	g := approval.New()
	ctx := context.Background()

	got := make(chan app.ApprovalRequest, 1)
	cancelSub := g.SubscribeApprovals("session-A", func(r app.ApprovalRequest) { got <- r })
	defer cancelSub()

	// A request on a DIFFERENT session must not notify session-A's subscriber.
	go func() { _, _ = g.Request(ctx, app.ApprovalRequest{SessionID: "session-B", CallID: "call-B"}) }()
	select {
	case r := <-got:
		t.Fatalf("subscriber for session-A was notified of a session-B request: %+v", r)
	case <-time.After(100 * time.Millisecond):
		// expected: no notification across sessions
	}
	_ = g.Resolve(ctx, "session-B", "call-B", domain.AskAllowed)
}

// itoa is a tiny helper to avoid importing strconv in the test.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
