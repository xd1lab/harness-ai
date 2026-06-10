// Package approval implements [app.ApprovalGate]: the in-process human-in-the-loop
// ask gate that the agent loop uses to pause on risk-tiered tool calls and wait for
// an explicit human decision before proceeding (architecture §3, §8.13, §9.3).
//
// # Concurrency model
//
// [Gate] is safe for concurrent use across multiple sessions.  Internally it keeps
// a map keyed by (sessionID, callID) → pending entry, guarded by a sync.Mutex.
// Each pending entry owns an unbuffered channel of size 1 so the delivering
// goroutine (Resolve) never blocks.
//
// [Gate.Request] registers a pending entry under the lock, then immediately drops
// the lock and selects on:
//
//   - the entry's resolution channel — happy path, returns the delivered resolution;
//   - ctx.Done() — returns ctx.Err() and removes the entry from the map so a
//     late Resolve does not observe a dangling channel.
//
// [Gate.Resolve] acquires the lock, looks up the entry, removes it (so a second
// Resolve on the same key gets "not found"), and sends the resolution on the
// channel (non-blocking send into a size-1 channel whose only reader is the
// corresponding Request goroutine).
//
// This design means:
//   - zero shared state between unrelated (sessionID, callID) pairs;
//   - a cancelled Request always cleans up — Resolve after cancel returns an error;
//   - the mutex is held only for map operations, not for the blocking wait, so
//     there is no lock-ordering concern.
package approval

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/boltrope/boltrope/internal/orchestrator/app"
	"github.com/boltrope/boltrope/internal/orchestrator/domain"
)

// pendingKey is the map key identifying a pending approval.
type pendingKey struct {
	sessionID string
	callID    string
}

// pendingEntry holds the resolution channel for one in-flight [Gate.Request] call.
type pendingEntry struct {
	// ch carries exactly one [domain.AskResolution] sent by [Gate.Resolve].
	// It is buffered with capacity 1 so [Gate.Resolve] never blocks even when
	// the [Gate.Request] goroutine has already moved on (e.g. cancelled).
	ch chan domain.AskResolution
}

// Gate is a concurrency-safe, in-memory implementation of [app.ApprovalGate].
// It is used by the agent loop in production (wired by the orchestrator infra)
// and in tests (exercised without real gRPC, as required by architecture §9.3).
//
// It ALSO implements the Run relay's optional ApprovalNotifier capability
// ([SubscribeApprovals]): when a tool call lands on the gate, every subscriber
// registered for that session is notified so the relay can emit an in-band
// ApprovalRequest frame on the client's Run stream. Without that signal a
// default-mode tool call blocks here invisibly — the client never learns the
// call_id to approve and the run appears to hang (the bug this closes).
//
// The zero value is not usable; construct with [New].
type Gate struct {
	mu      sync.Mutex
	pending map[pendingKey]*pendingEntry

	// subs holds the per-session approval subscribers (the Run relay registers one
	// per stream), keyed by a monotonic id so an individual subscription can be
	// cancelled. Notifying them when a Request arrives is what surfaces a pending
	// ask to the client; see [Gate.SubscribeApprovals].
	subs   map[string]map[int64]func(app.ApprovalRequest)
	nextID int64
}

// New returns a new, ready-to-use [Gate].
func New() *Gate {
	return &Gate{
		pending: make(map[pendingKey]*pendingEntry),
		subs:    make(map[string]map[int64]func(app.ApprovalRequest)),
	}
}

// Request registers req as a pending approval and blocks until either a
// human decision arrives via [Gate.Resolve] or ctx is cancelled.
//
// On a normal resolution it returns the [domain.AskResolution] ([domain.AskAllowed]
// or [domain.AskDenied]) and a nil error.  On a cancelled context it returns the
// zero resolution and ctx.Err(); the pending entry is removed so a subsequent
// Resolve for the same (sessionID, callID) returns a "not pending" error.
//
// Multiple concurrent Request calls with distinct (sessionID, callID) pairs are
// fully independent.
func (g *Gate) Request(ctx context.Context, req app.ApprovalRequest) (domain.AskResolution, error) {
	key := pendingKey{sessionID: req.SessionID, callID: req.CallID}

	entry := &pendingEntry{
		// Buffered 1 so Resolve can send without blocking even if Request already
		// returned via context cancellation.
		ch: make(chan domain.AskResolution, 1),
	}

	g.mu.Lock()
	g.pending[key] = entry
	// Snapshot this session's subscribers under the lock, then notify them after
	// releasing it: the contract forbids holding g.mu during the blocking wait, and
	// the notify callbacks must not run under the lock.
	subs := make([]func(app.ApprovalRequest), 0, len(g.subs[req.SessionID]))
	for _, fn := range g.subs[req.SessionID] {
		subs = append(subs, fn)
	}
	g.mu.Unlock()

	// Surface the pending ask to any in-band notifier (the Run relay emits an
	// ApprovalRequest frame) BEFORE blocking, so a client on the Run stream sees the
	// request and can resolve it via Control.Approve/Deny. fn must not block (per the
	// ApprovalNotifier contract), so this stays off the critical path.
	for _, notify := range subs {
		notify(req)
	}

	// Wait for either a resolution or context cancellation.
	select {
	case res := <-entry.ch:
		return res, nil

	case <-ctx.Done():
		// Remove the pending entry so a late Resolve cannot land on a stale slot.
		g.mu.Lock()
		// Only delete if the map still holds our exact entry pointer.  This guards
		// against a theoretical ABA where a new Request for the same key was
		// registered between our cancellation and this lock acquisition.
		if g.pending[key] == entry {
			delete(g.pending, key)
		}
		g.mu.Unlock()
		return "", ctx.Err()
	}
}

// Resolve delivers resolution for the pending approval identified by (sessionID,
// callID), unblocking the corresponding [Gate.Request] call.
//
// It returns an error if no matching pending request exists (the request was
// never registered, has already been resolved, or was cancelled).
func (g *Gate) Resolve(_ context.Context, sessionID, callID string, resolution domain.AskResolution) error {
	key := pendingKey{sessionID: sessionID, callID: callID}

	g.mu.Lock()
	entry, ok := g.pending[key]
	if ok {
		// Remove immediately under the lock so a second Resolve on the same key
		// will find nothing.
		delete(g.pending, key)
	}
	g.mu.Unlock()

	if !ok {
		return fmt.Errorf(
			"approval: no pending request for session %q call %q: %w",
			sessionID, callID, ErrNotPending,
		)
	}

	// Non-blocking send: the channel is buffered(1) so this never blocks even
	// when the Request goroutine has just seen ctx.Done() and is about to drain
	// the map under the lock.  If Request wins the select on ctx.Done() first,
	// it will delete the entry before we can reach here (we checked ok==true above),
	// so the send is always paired with a live reader or a safe drop into the buffer.
	entry.ch <- resolution
	return nil
}

// SubscribeApprovals registers fn to be invoked for every approval [Gate.Request]
// raised on sessionID, until the returned cancel func is called. It implements the
// optional ApprovalNotifier capability the Run relay consumes to surface a pending
// ask as an in-band ApprovalRequest frame (architecture §4.2): without it a
// default-mode tool call blocks at the gate with no client-visible signal, so the
// operator never learns the call_id to approve and the run appears to hang.
//
// fn is invoked synchronously from Request and MUST NOT block (the relay callback
// only serializes a single stream write). Multiple subscribers per session are
// supported and each Subscribe is independently cancellable; cancel is idempotent.
// Safe for concurrent use.
func (g *Gate) SubscribeApprovals(sessionID string, fn func(app.ApprovalRequest)) func() {
	g.mu.Lock()
	if g.subs[sessionID] == nil {
		g.subs[sessionID] = make(map[int64]func(app.ApprovalRequest))
	}
	id := g.nextID
	g.nextID++
	g.subs[sessionID][id] = fn
	g.mu.Unlock()

	return func() {
		g.mu.Lock()
		if m := g.subs[sessionID]; m != nil {
			delete(m, id)
			if len(m) == 0 {
				delete(g.subs, sessionID)
			}
		}
		g.mu.Unlock()
	}
}

// ErrNotPending is returned by [Gate.Resolve] when the supplied (sessionID, callID)
// pair has no registered pending request.  Callers can test for it with [errors.Is].
var ErrNotPending = errors.New("approval: no pending request for the given session and call")
