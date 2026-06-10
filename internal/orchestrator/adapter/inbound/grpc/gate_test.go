package grpc

import (
	"context"
	"runtime"
	"sync"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// notifyingGate is a test [app.ApprovalGate] that also implements
// [ApprovalNotifier], so the Run relay's in-band ApprovalRequest path is
// exercised. Request blocks until Resolve delivers a decision (or ctx is
// cancelled); pendingCount lets a test wait until the loop is actually blocked
// on the gate before resolving.
// approvalObserver is a named type for an approval-notification callback so
// slice literals are unambiguous to the parser.
type approvalObserver func(app.ApprovalRequest)

type notifyingGate struct {
	mu        sync.Mutex
	pending   map[string]chan domain.AskResolution
	observers map[string][]approvalObserver
}

var (
	_ app.ApprovalGate = (*notifyingGate)(nil)
	_ ApprovalNotifier = (*notifyingGate)(nil)
)

func newNotifyingGate() *notifyingGate {
	return &notifyingGate{
		pending:   make(map[string]chan domain.AskResolution),
		observers: make(map[string][]approvalObserver),
	}
}

func gateKey(sessionID, callID string) string { return sessionID + "\x00" + callID }

func (g *notifyingGate) Request(ctx context.Context, req app.ApprovalRequest) (domain.AskResolution, error) {
	ch := make(chan domain.AskResolution, 1)
	key := gateKey(req.SessionID, req.CallID)

	g.mu.Lock()
	g.pending[key] = ch
	obs := append([]approvalObserver(nil), g.observers[req.SessionID]...)
	g.mu.Unlock()

	for _, fn := range obs {
		fn(req)
	}

	defer func() {
		g.mu.Lock()
		delete(g.pending, key)
		g.mu.Unlock()
	}()
	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return domain.AskUnresolved, ctx.Err()
	}
}

func (g *notifyingGate) Resolve(_ context.Context, sessionID, callID string, resolution domain.AskResolution) error {
	key := gateKey(sessionID, callID)
	const maxYields = 2000
	for i := 0; i < maxYields; i++ {
		g.mu.Lock()
		ch, ok := g.pending[key]
		g.mu.Unlock()
		if ok {
			ch <- resolution
			return nil
		}
		runtime.Gosched()
	}
	return context.DeadlineExceeded
}

func (g *notifyingGate) SubscribeApprovals(sessionID string, fn func(app.ApprovalRequest)) func() {
	g.mu.Lock()
	g.observers[sessionID] = append(g.observers[sessionID], approvalObserver(fn))
	idx := len(g.observers[sessionID]) - 1
	g.mu.Unlock()
	return func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		obs := g.observers[sessionID]
		if idx < len(obs) {
			obs[idx] = func(app.ApprovalRequest) {}
		}
	}
}

func (g *notifyingGate) pendingCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.pending)
}
