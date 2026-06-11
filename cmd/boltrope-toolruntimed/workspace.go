package main

import (
	"context"
	"errors"
	"sync"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
)

// sessionWorkspaces is the production [app.SessionWorkspaces]: it routes every
// tool call to the CALLING session's own sandbox container, provisioning it
// lazily the first time that session touches the filesystem or runs a command
// (so the daemon constructs without Docker) and re-attaching to the live
// container thereafter. Per-session sandboxes are the v1 containment boundary
// (architecture §2.2, §5.3, §8.4): two sessions must never share /workspace
// state, so there is deliberately NO shared-sandbox fallback — an empty
// session id is refused. Tool cancellation still maps to a real in-sandbox
// kill via the resolved container's Exec (architecture §9.3).
//
// Each session's sandbox is created with the operator-configured egress
// allowlist stamped onto its own session-scoped [app.EgressPolicy], matching
// the broker's default policy for sessions (see buildEgress); the dedup ledger
// and egress broker remain per-(tenant,session) keyed independently.
type sessionWorkspaces struct {
	runtime app.RuntimePort
	// allowedHosts is the operator-configured egress allowlist stamped onto
	// every session's policy (deny-by-default when empty; architecture §8.4).
	allowedHosts []string

	// mu guards locks; locks serializes Get-or-Create PER SESSION so a
	// concurrent first use provisions exactly one container per session
	// (runtime.Create tears down an existing same-name container for
	// clean-workspace resume, so an unserialized double-Create would destroy a
	// sibling call's live sandbox), while distinct sessions provision in
	// parallel.
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// newSessionWorkspaces returns a [sessionWorkspaces] router over rt. Sessions'
// sandboxes are provisioned on first use with allowedHosts as their egress
// allowlist policy.
func newSessionWorkspaces(rt app.RuntimePort, allowedHosts []string) *sessionWorkspaces {
	return &sessionWorkspaces{
		runtime:      rt,
		allowedHosts: allowedHosts,
		locks:        make(map[string]*sync.Mutex),
	}
}

// Workspace returns sessionID's live sandbox workspace, creating it on first
// use. An empty session id is refused rather than routed to any shared
// sandbox — a shared fallback is exactly the cross-session isolation breach
// the per-session contract forbids.
func (r *sessionWorkspaces) Workspace(ctx context.Context, sessionID string) (app.Workspace, error) {
	if sessionID == "" {
		return nil, errors.New("toolruntimed: tool call carries no session id; refusing shared-sandbox fallback")
	}
	lock := r.lockFor(sessionID)
	lock.Lock()
	defer lock.Unlock()

	// Prefer the session's existing live workspace (created by a prior call),
	// else provision a fresh one with the session-scoped egress policy.
	if ws, err := r.runtime.Get(ctx, sessionID); err == nil && ws != nil {
		return ws, nil
	}
	return r.runtime.Create(ctx, sessionID, app.EgressPolicy{
		SessionID:    sessionID,
		AllowedHosts: r.allowedHosts,
	})
}

// lockFor returns the per-session mutex, creating it on first use. The lock
// map grows by one small entry per distinct session id seen; the sandboxes
// themselves are bounded and reaped by the runtime's lifecycle manager
// (architecture §10.6).
func (r *sessionWorkspaces) lockFor(sessionID string) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	l, ok := r.locks[sessionID]
	if !ok {
		l = &sync.Mutex{}
		r.locks[sessionID] = l
	}
	return l
}

// Compile-time assertion that sessionWorkspaces satisfies app.SessionWorkspaces.
var _ app.SessionWorkspaces = (*sessionWorkspaces)(nil)
