package main

import (
	"context"
	"sync"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
)

// routingWorkspace adapts a [app.RuntimePort] (the per-session sandbox manager)
// into the single [app.Workspace] the native tools bind to at registry-build
// time. The frozen [app.Workspace] methods do not carry a session id, so the
// native tools operate on whatever workspace they were constructed with; this
// router lazily provisions a container the first time a tool touches the
// filesystem or runs a command and reuses it thereafter, so the daemon
// constructs without Docker (provisioning is deferred to first use) and tool
// cancellation still maps to a real in-sandbox kill via the underlying
// container's Exec (architecture §9.3).
//
// In v1 the tool-runtime serves a single orchestrator and binds the native tools
// to one session-scoped sandbox keyed by [sessionID]; the durable dedup ledger
// and egress broker remain per-(tenant,session) keyed independently. A future
// multi-session backend would construct a per-session registry per ExecuteTool
// stream; the [app.Workspace] contract does not currently express per-call
// routing, so this binding is the documented v1 wiring.
type routingWorkspace struct {
	runtime   app.RuntimePort
	sessionID string
	egress    app.EgressPolicy

	mu sync.Mutex
	ws app.Workspace
}

// newRoutingWorkspace returns a [routingWorkspace] over rt for the given session
// id and initial egress policy. No container is created until first use.
func newRoutingWorkspace(rt app.RuntimePort, sessionID string, egress app.EgressPolicy) *routingWorkspace {
	return &routingWorkspace{runtime: rt, sessionID: sessionID, egress: egress}
}

// resolve returns the live workspace, creating it on first use. It is safe for
// concurrent use; a concurrent first-use is serialized so only one container is
// created.
func (r *routingWorkspace) resolve(ctx context.Context) (app.Workspace, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ws != nil {
		return r.ws, nil
	}
	// Prefer an existing live workspace (e.g. created by a prior call), else create.
	if ws, err := r.runtime.Get(ctx, r.sessionID); err == nil && ws != nil {
		r.ws = ws
		return ws, nil
	}
	ws, err := r.runtime.Create(ctx, r.sessionID, r.egress)
	if err != nil {
		return nil, err
	}
	r.ws = ws
	return ws, nil
}

// Exec resolves the session workspace and runs req inside it.
func (r *routingWorkspace) Exec(ctx context.Context, req app.ExecRequest) (app.ExecResult, error) {
	ws, err := r.resolve(ctx)
	if err != nil {
		return app.ExecResult{}, err
	}
	return ws.Exec(ctx, req)
}

// Read resolves the session workspace and reads path.
func (r *routingWorkspace) Read(ctx context.Context, path string) ([]byte, error) {
	ws, err := r.resolve(ctx)
	if err != nil {
		return nil, err
	}
	return ws.Read(ctx, path)
}

// Write resolves the session workspace and writes data to path.
func (r *routingWorkspace) Write(ctx context.Context, path string, data []byte) error {
	ws, err := r.resolve(ctx)
	if err != nil {
		return err
	}
	return ws.Write(ctx, path, data)
}

// Mkdir resolves the session workspace and creates path.
func (r *routingWorkspace) Mkdir(ctx context.Context, path string) error {
	ws, err := r.resolve(ctx)
	if err != nil {
		return err
	}
	return ws.Mkdir(ctx, path)
}

// NetworkPolicy returns the configured egress policy without provisioning a
// container (so a policy read never forces a sandbox to be created).
func (r *routingWorkspace) NetworkPolicy(_ context.Context) (app.EgressPolicy, error) {
	return r.egress, nil
}

// Compile-time assertion that routingWorkspace satisfies app.Workspace.
var _ app.Workspace = (*routingWorkspace)(nil)
