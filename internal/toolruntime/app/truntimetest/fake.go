// Package truntimetest provides deterministic fakes for every consumer-defined
// port in [github.com/boltrope/boltrope/internal/toolruntime/app]:
// [app.ToolRegistry], [app.RuntimePort], [app.Workspace], [app.EgressBroker],
// [app.MCPClientPort], and [app.DedupStore].
//
// All fakes are scriptable stubs suitable for deterministic unit tests:
// no file system, no containers, no database, no network.
package truntimetest

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/boltrope/boltrope/internal/toolruntime/app"
	"github.com/boltrope/boltrope/internal/toolruntime/domain"
)

// ---------------------------------------------------------------------------
// Compile-time interface assertions
// ---------------------------------------------------------------------------

var (
	_ app.ToolRegistry  = (*FakeToolRegistry)(nil)
	_ app.RuntimePort   = (*FakeRuntimePort)(nil)
	_ app.Workspace     = (*FakeWorkspace)(nil)
	_ app.EgressBroker  = (*FakeEgressBroker)(nil)
	_ app.MCPClientPort = (*FakeMCPClient)(nil)
	_ app.DedupStore    = (*FakeDedupStore)(nil)
)

// ---------------------------------------------------------------------------
// FakeToolRegistry
// ---------------------------------------------------------------------------

// FakeToolRegistry is an in-memory [app.ToolRegistry] that stores registered
// tools and supports lazy Get via the domain.Tool interface.
type FakeToolRegistry struct {
	mu    sync.Mutex
	tools map[string]domain.Tool
}

// NewFakeToolRegistry returns an empty FakeToolRegistry.
func NewFakeToolRegistry() *FakeToolRegistry {
	return &FakeToolRegistry{tools: make(map[string]domain.Tool)}
}

// Register stores the tool, or returns an error if the name is empty or
// already registered.
func (r *FakeToolRegistry) Register(_ context.Context, t domain.Tool) error {
	spec := t.Spec()
	if spec.Name == "" {
		return fmt.Errorf("truntimetest: tool name must not be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.tools[spec.Name]; ok {
		return fmt.Errorf("truntimetest: tool %q already registered", spec.Name)
	}
	r.tools[spec.Name] = t
	return nil
}

// Get returns the tool by name or [app.ErrToolNotFound].
func (r *FakeToolRegistry) Get(_ context.Context, name string) (domain.Tool, error) {
	r.mu.Lock()
	t, ok := r.tools[name]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", app.ErrToolNotFound, name)
	}
	return t, nil
}

// List returns all registered tool specs.
func (r *FakeToolRegistry) List(_ context.Context) ([]domain.ToolSpec, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.ToolSpec, 0, len(r.tools))
	for _, t := range r.tools {
		out = append(out, t.Spec())
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// FakeTool — implements domain.Tool for registry tests
// ---------------------------------------------------------------------------

// FakeTool is a minimal [domain.Tool] that records Execute calls.
type FakeTool struct {
	spec            domain.ToolSpec
	observations    []domain.Observation
	observationErrs []error
	idx             atomic.Int64
	mu              sync.Mutex
	ExecCalls       []ExecToolCall
}

// ExecToolCall records one Execute invocation.
type ExecToolCall struct {
	SessionID string
	Args      map[string]any
}

// NewFakeTool returns a FakeTool with the given spec.
func NewFakeTool(spec domain.ToolSpec) *FakeTool { return &FakeTool{spec: spec} }

// AddObservation enqueues one scripted Execute result.
func (f *FakeTool) AddObservation(obs domain.Observation, err error) {
	f.mu.Lock()
	f.observations = append(f.observations, obs)
	f.observationErrs = append(f.observationErrs, err)
	f.mu.Unlock()
}

// Spec returns the tool's declaration.
func (f *FakeTool) Spec() domain.ToolSpec { return f.spec }

// Execute returns the next scripted observation.
func (f *FakeTool) Execute(_ context.Context, sessionID string, args map[string]any) (domain.Observation, error) {
	f.mu.Lock()
	f.ExecCalls = append(f.ExecCalls, ExecToolCall{SessionID: sessionID, Args: args})
	f.mu.Unlock()
	idx := int(f.idx.Add(1) - 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if idx >= len(f.observations) {
		return domain.Observation{Content: "ok"}, nil
	}
	return f.observations[idx], f.observationErrs[idx]
}

// ---------------------------------------------------------------------------
// FakeWorkspace
// ---------------------------------------------------------------------------

// FakeWorkspace is an in-memory [app.Workspace]. Read/Write/Mkdir operate on
// an in-memory map. Exec returns scripted results.
type FakeWorkspace struct {
	mu       sync.Mutex
	files    map[string][]byte
	execQ    []app.ExecResult
	execErrs []error
	execIdx  atomic.Int64
	Policy   app.EgressPolicy
	ExecLog  []app.ExecRequest
}

// NewFakeWorkspace returns an empty FakeWorkspace.
func NewFakeWorkspace() *FakeWorkspace {
	return &FakeWorkspace{files: make(map[string][]byte)}
}

// AddExecResult enqueues one scripted Exec outcome.
func (w *FakeWorkspace) AddExecResult(r app.ExecResult, err error) {
	w.mu.Lock()
	w.execQ = append(w.execQ, r)
	w.execErrs = append(w.execErrs, err)
	w.mu.Unlock()
}

// Exec returns the next scripted ExecResult.
func (w *FakeWorkspace) Exec(_ context.Context, req app.ExecRequest) (app.ExecResult, error) {
	w.mu.Lock()
	w.ExecLog = append(w.ExecLog, req)
	w.mu.Unlock()
	idx := int(w.execIdx.Add(1) - 1)
	w.mu.Lock()
	defer w.mu.Unlock()
	if idx >= len(w.execQ) {
		return app.ExecResult{ExitCode: 0}, nil
	}
	return w.execQ[idx], w.execErrs[idx]
}

// Read returns the in-memory file content.
func (w *FakeWorkspace) Read(_ context.Context, path string) ([]byte, error) {
	w.mu.Lock()
	data, ok := w.files[path]
	w.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("truntimetest: file not found: %q", path)
	}
	return append([]byte(nil), data...), nil
}

// Write stores data in the in-memory file map.
func (w *FakeWorkspace) Write(_ context.Context, path string, data []byte) error {
	w.mu.Lock()
	w.files[path] = append([]byte(nil), data...)
	w.mu.Unlock()
	return nil
}

// Mkdir is a no-op in the fake.
func (w *FakeWorkspace) Mkdir(_ context.Context, _ string) error { return nil }

// NetworkPolicy returns the workspace's EgressPolicy.
func (w *FakeWorkspace) NetworkPolicy(_ context.Context) (app.EgressPolicy, error) {
	w.mu.Lock()
	p := w.Policy
	w.mu.Unlock()
	return p, nil
}

// ---------------------------------------------------------------------------
// FakeRuntimePort
// ---------------------------------------------------------------------------

// FakeRuntimePort is a scriptable [app.RuntimePort] that creates and destroys
// FakeWorkspaces.
type FakeRuntimePort struct {
	mu         sync.Mutex
	workspaces map[string]*FakeWorkspace
}

// NewFakeRuntimePort returns an empty FakeRuntimePort.
func NewFakeRuntimePort() *FakeRuntimePort {
	return &FakeRuntimePort{workspaces: make(map[string]*FakeWorkspace)}
}

// Create provisions a new FakeWorkspace for sessionID.
func (r *FakeRuntimePort) Create(_ context.Context, sessionID string, egress app.EgressPolicy) (app.Workspace, error) {
	ws := NewFakeWorkspace()
	ws.Policy = egress
	r.mu.Lock()
	r.workspaces[sessionID] = ws
	r.mu.Unlock()
	return ws, nil
}

// Get returns the workspace for sessionID, or an error if none exists.
func (r *FakeRuntimePort) Get(_ context.Context, sessionID string) (app.Workspace, error) {
	r.mu.Lock()
	ws, ok := r.workspaces[sessionID]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("truntimetest: no workspace for session %q", sessionID)
	}
	return ws, nil
}

// Destroy removes the workspace for sessionID. Idempotent.
func (r *FakeRuntimePort) Destroy(_ context.Context, sessionID string) error {
	r.mu.Lock()
	delete(r.workspaces, sessionID)
	r.mu.Unlock()
	return nil
}

// WorkspaceFor returns the underlying FakeWorkspace for inspection in tests.
func (r *FakeRuntimePort) WorkspaceFor(sessionID string) *FakeWorkspace {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.workspaces[sessionID]
}

// ---------------------------------------------------------------------------
// FakeEgressBroker
// ---------------------------------------------------------------------------

// FakeEgressBroker is a simple [app.EgressBroker] that consults a per-session
// allowlist. Deny-by-default: a host absent from the session's allowlist
// returns false.
type FakeEgressBroker struct {
	mu       sync.Mutex
	policies map[string]app.EgressPolicy
}

// NewFakeEgressBroker returns an empty FakeEgressBroker (deny-all default).
func NewFakeEgressBroker() *FakeEgressBroker {
	return &FakeEgressBroker{policies: make(map[string]app.EgressPolicy)}
}

// Allow reports whether sessionID may connect to host per its configured
// EgressPolicy. Deny-by-default: absent session or absent host returns false.
func (b *FakeEgressBroker) Allow(_ context.Context, sessionID, host string) (bool, error) {
	b.mu.Lock()
	p, ok := b.policies[sessionID]
	b.mu.Unlock()
	if !ok {
		return false, nil // no policy configured → deny
	}
	for _, h := range p.AllowedHosts {
		if h == host {
			return true, nil
		}
	}
	return false, nil
}

// SetPolicy installs or replaces the EgressPolicy for a session.
func (b *FakeEgressBroker) SetPolicy(_ context.Context, policy app.EgressPolicy) error {
	b.mu.Lock()
	b.policies[policy.SessionID] = policy
	b.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// FakeMCPClient
// ---------------------------------------------------------------------------

// MCPListToolsCall records one ListTools invocation.
type MCPListToolsCall struct {
	Server app.MCPServerRef
}

// MCPCallToolCall records one CallTool invocation.
type MCPCallToolCall struct {
	Server    app.MCPServerRef
	SessionID string
	Name      string
	Args      map[string]any
}

// FakeMCPClient is a scriptable [app.MCPClientPort].
type FakeMCPClient struct {
	mu            sync.Mutex
	listResults   [][]domain.ToolSpec
	listErrs      []error
	listIdx       atomic.Int64
	callResults   []domain.Observation
	callErrs      []error
	callIdx       atomic.Int64
	ListToolCalls []MCPListToolsCall
	CallToolCalls []MCPCallToolCall
}

// NewFakeMCPClient returns an empty FakeMCPClient.
func NewFakeMCPClient() *FakeMCPClient { return &FakeMCPClient{} }

// AddListResult enqueues one scripted ListTools result.
func (c *FakeMCPClient) AddListResult(specs []domain.ToolSpec, err error) {
	c.mu.Lock()
	c.listResults = append(c.listResults, specs)
	c.listErrs = append(c.listErrs, err)
	c.mu.Unlock()
}

// AddCallResult enqueues one scripted CallTool result.
func (c *FakeMCPClient) AddCallResult(obs domain.Observation, err error) {
	c.mu.Lock()
	c.callResults = append(c.callResults, obs)
	c.callErrs = append(c.callErrs, err)
	c.mu.Unlock()
}

// ListTools returns the next scripted tool spec list.
func (c *FakeMCPClient) ListTools(_ context.Context, server app.MCPServerRef) ([]domain.ToolSpec, error) {
	c.mu.Lock()
	c.ListToolCalls = append(c.ListToolCalls, MCPListToolsCall{Server: server})
	c.mu.Unlock()
	idx := int(c.listIdx.Add(1) - 1)
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx >= len(c.listResults) {
		return nil, nil
	}
	return c.listResults[idx], c.listErrs[idx]
}

// CallTool returns the next scripted observation.
func (c *FakeMCPClient) CallTool(_ context.Context, server app.MCPServerRef, sessionID, name string, args map[string]any) (domain.Observation, error) {
	c.mu.Lock()
	c.CallToolCalls = append(c.CallToolCalls, MCPCallToolCall{
		Server:    server,
		SessionID: sessionID,
		Name:      name,
		Args:      args,
	})
	c.mu.Unlock()
	idx := int(c.callIdx.Add(1) - 1)
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx >= len(c.callResults) {
		return domain.Observation{Content: "ok"}, nil
	}
	return c.callResults[idx], c.callErrs[idx]
}

// ---------------------------------------------------------------------------
// FakeDedupStore
// ---------------------------------------------------------------------------

// FakeDedupStore is an in-memory [app.DedupStore]. Begin records ExecStarted
// for new keys and returns the existing record for already-known keys.
// Complete updates the record's status and result.
type FakeDedupStore struct {
	mu      sync.Mutex
	records map[string]*app.ExecutionRecord // key: tenantID+"\x00"+sessionID+"\x00"+idemKey
}

// NewFakeDedupStore returns an empty FakeDedupStore.
func NewFakeDedupStore() *FakeDedupStore {
	return &FakeDedupStore{records: make(map[string]*app.ExecutionRecord)}
}

func dedupKey(tenantID, sessionID, idemKey string) string {
	return tenantID + "\x00" + sessionID + "\x00" + idemKey
}

// Begin records ExecStarted for new keys, or returns the existing record.
func (s *FakeDedupStore) Begin(_ context.Context, rec app.ExecutionRecord) (app.ExecutionRecord, error) {
	k := dedupKey(rec.TenantID, rec.SessionID, rec.IdempotencyKey)
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.records[k]; ok {
		return *existing, nil
	}
	rec.Status = app.ExecStarted
	s.records[k] = &rec
	return rec, nil
}

// Complete records a terminal status and result for the key.
func (s *FakeDedupStore) Complete(_ context.Context, rec app.ExecutionRecord) error {
	k := dedupKey(rec.TenantID, rec.SessionID, rec.IdempotencyKey)
	s.mu.Lock()
	s.records[k] = &rec
	s.mu.Unlock()
	return nil
}

// Lookup returns the current record for (tenantID, sessionID, idempotencyKey),
// or an error if no record exists.
func (s *FakeDedupStore) Lookup(_ context.Context, tenantID, sessionID, idempotencyKey string) (app.ExecutionRecord, error) {
	k := dedupKey(tenantID, sessionID, idempotencyKey)
	s.mu.Lock()
	rec, ok := s.records[k]
	s.mu.Unlock()
	if !ok {
		return app.ExecutionRecord{}, fmt.Errorf("truntimetest: no dedup record for %q/%q/%q", tenantID, sessionID, idempotencyKey)
	}
	// Re-check tenant (defense-in-depth, architecture §7.3).
	if rec.TenantID != tenantID {
		return app.ExecutionRecord{}, fmt.Errorf("truntimetest: dedup lookup tenant mismatch")
	}
	return *rec, nil
}

// ---------------------------------------------------------------------------
// Compile-time: ensure io is used (for io.EOF in FakeTool / FakeWorkspace)
// ---------------------------------------------------------------------------

var _ = io.EOF
