// SPDX-License-Identifier: Apache-2.0

package mcpserver_test

// This file is the package's test scaffolding (T-1): the in-memory fakes
// (mirroring rest_test.go), TWO gate options (the stub fakeGate for unary/error
// tests and the REAL *approval.Gate for the concurrent-approval ACs), a harness
// that builds the MCP *Handler over a REAL igrpc.NewServer served by
// httptest.NewServer, a JSON-RPC POST helper (doRPC), and an SSE-reading helper
// that reads frames until a predicate (no fixed sleeps).
//
// The whole package is RED until the mcpserver implementation lands: NewHandler
// and Routes do not yet exist, so this file does not compile — exactly the TDD
// red state the run mandates.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/approval"
	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/mcpserver"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
	"github.com/xd1lab/harness-ai/internal/platform/ids"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ---------------------------------------------------------------------------
// Fakes — an in-memory EventStore, ApprovalGate, and Runner so the MCP adapter
// is exercised against the REAL igrpc.Server (ownership, caps, mapping) without
// a database or loop. Shapes mirror rest_test.go verbatim.
// ---------------------------------------------------------------------------

type fakeStore struct {
	mu       sync.Mutex
	sessions map[string]domain.Session
	events   map[string][]domain.EventEnvelope
	subs     map[string][]chan domain.EventEnvelope

	subscribedFrom []int64
	createdModes   []domain.PermissionMode
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		sessions: map[string]domain.Session{},
		events:   map[string][]domain.EventEnvelope{},
		subs:     map[string][]chan domain.EventEnvelope{},
	}
}

func (f *fakeStore) CreateSession(ctx context.Context, sessionID string, mode domain.PermissionMode) (domain.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	tenant, _ := db.TenantFromContext(ctx)
	s := domain.Session{ID: sessionID, TenantID: tenant, Mode: mode}
	f.sessions[sessionID] = s
	f.createdModes = append(f.createdModes, mode)
	return s, nil
}

func (f *fakeStore) Append(_ context.Context, sessionID string, _, _ int64, _ string, ins ...app.AppendInput) ([]domain.EventEnvelope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.sessions[sessionID]
	s.ID = sessionID
	var out []domain.EventEnvelope
	for _, in := range ins {
		s.HeadSeq++
		env := domain.EventEnvelope{
			Type:      in.Event.EventType(),
			Seq:       s.HeadSeq,
			SessionID: sessionID,
			TenantID:  s.TenantID,
			Event:     in.Event,
		}
		f.events[sessionID] = append(f.events[sessionID], env)
		for _, ch := range f.subs[sessionID] {
			select {
			case ch <- env:
			default:
			}
		}
		out = append(out, env)
	}
	f.sessions[sessionID] = s
	return out, nil
}

func (f *fakeStore) Load(_ context.Context, sessionID string, fromSeq int64) ([]domain.EventEnvelope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []domain.EventEnvelope
	for _, e := range f.events[sessionID] {
		if e.Seq > fromSeq {
			out = append(out, e)
		}
	}
	return out, nil
}

func (f *fakeStore) LoadSession(_ context.Context, sessionID string) (domain.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.sessions[sessionID]
	if !ok {
		return domain.Session{}, fmt.Errorf("fake: session %q not found", sessionID)
	}
	return s, nil
}

func (f *fakeStore) Subscribe(ctx context.Context, sessionID string, fromSeq int64) (<-chan domain.EventEnvelope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribedFrom = append(f.subscribedFrom, fromSeq)
	ch := make(chan domain.EventEnvelope, 64)
	for _, e := range f.events[sessionID] {
		if e.Seq > fromSeq {
			ch <- e
		}
	}
	f.subs[sessionID] = append(f.subs[sessionID], ch)
	go func() { <-ctx.Done() }()
	return ch, nil
}

func (f *fakeStore) Fork(_ context.Context, parentID string, atSeq int64, newSessionID string) (domain.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.sessions[parentID]
	if !ok {
		return domain.Session{}, fmt.Errorf("fake: parent %q not found", parentID)
	}
	child := domain.Session{ID: newSessionID, TenantID: p.TenantID, ParentID: parentID, ForkedFromSeq: atSeq}
	f.sessions[newSessionID] = child
	return child, nil
}

// seed inserts a session directly (bypassing CreateSession).
func (f *fakeStore) seed(s domain.Session) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessions[s.ID] = s
}

func (f *fakeStore) createdModesLen() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.createdModes)
}

func (f *fakeStore) firstCreatedMode() domain.PermissionMode {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.createdModes) == 0 {
		return ""
	}
	return f.createdModes[0]
}

func (f *fakeStore) firstSubscribedFrom() (int64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.subscribedFrom) == 0 {
		return 0, false
	}
	return f.subscribedFrom[0], true
}

// ---- gate stub (unary/error tests) -----------------------------------------

type resolveCall struct {
	sessionID, callID string
	resolution        domain.AskResolution
}

type fakeGate struct {
	mu       sync.Mutex
	resolves []resolveCall
	err      error
}

func (g *fakeGate) Request(ctx context.Context, _ app.ApprovalRequest) (domain.AskResolution, error) {
	<-ctx.Done()
	return domain.AskDenied, ctx.Err()
}

func (g *fakeGate) Resolve(_ context.Context, sessionID, callID string, res domain.AskResolution) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.resolves = append(g.resolves, resolveCall{sessionID, callID, res})
	return g.err
}

func (g *fakeGate) snapshot() []resolveCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]resolveCall, len(g.resolves))
	copy(out, g.resolves)
	return out
}

// ---- runner --------------------------------------------------------------

type fakeRunner struct {
	fn func(ctx context.Context, spec igrpc.RunSpec) (igrpc.RunOutcome, error)

	mu    sync.Mutex
	specs []igrpc.RunSpec
}

func (r *fakeRunner) Run(ctx context.Context, spec igrpc.RunSpec) (igrpc.RunOutcome, error) {
	r.mu.Lock()
	r.specs = append(r.specs, spec)
	r.mu.Unlock()
	if r.fn != nil {
		return r.fn(ctx, spec)
	}
	return igrpc.RunOutcome{Reason: domain.Success}, nil
}

func (r *fakeRunner) specCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.specs)
}

// gate is the minimal interface both fakeGate and *approval.Gate satisfy so a
// harness can be built over either (the server requires app.ApprovalGate; the
// real gate ALSO implements igrpc.ApprovalNotifier, which the relay needs to
// surface the in-band approval frame).
type gate interface {
	app.ApprovalGate
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type harness struct {
	store  *fakeStore
	gate   *fakeGate      // non-nil only for stub-gate harnesses
	real   *approval.Gate // non-nil only for real-gate harnesses
	runner *fakeRunner
	srv    *httptest.Server
}

const (
	testVersion  = "test-version"
	mcpProtoVers = "2025-06-18"
)

// newHarness builds the MCP handler over a REAL igrpc.Server with the stub gate.
func newHarness(t *testing.T, ac igrpc.AuthConfig, allowedOrigins []string) *harness {
	t.Helper()
	h := &harness{store: newFakeStore(), gate: &fakeGate{}, runner: &fakeRunner{}}
	h.build(t, h.gate, ac, allowedOrigins)
	return h
}

// newRealGateHarness builds the MCP handler over a REAL igrpc.Server with the
// REAL *approval.Gate (so Resolve actually unblocks Request) — required by the
// concurrent-approval ACs (AC-9/10).
func newRealGateHarness(t *testing.T, ac igrpc.AuthConfig) *harness {
	t.Helper()
	h := &harness{store: newFakeStore(), real: approval.New(), runner: &fakeRunner{}}
	h.build(t, h.real, ac, nil)
	return h
}

func (h *harness) build(t *testing.T, g gate, ac igrpc.AuthConfig, allowedOrigins []string) {
	t.Helper()
	grpcSrv := igrpc.NewServer(h.store, g, h.runner, ids.System{}, igrpc.Config{})
	auth, err := igrpc.NewAuthenticator(ac)
	require.NoError(t, err)
	mux := http.NewServeMux()
	mcpserver.NewHandler(grpcSrv, auth, testVersion, allowedOrigins).Routes(mux)
	h.srv = httptest.NewServer(mux)
	t.Cleanup(h.srv.Close)
}

func devHarness(t *testing.T) *harness {
	t.Helper()
	return newHarness(t, igrpc.AuthConfig{DevInsecure: true}, nil)
}

func devRealGateHarness(t *testing.T) *harness {
	t.Helper()
	return newRealGateHarness(t, igrpc.AuthConfig{DevInsecure: true})
}

// ---- prod auth helpers (mirror rest_test.go) -------------------------------

const hsSecret = "test-secret"

func prodHarness(t *testing.T) *harness {
	t.Helper()
	return newHarness(t, igrpc.AuthConfig{
		Algorithms: []string{"HS256"},
		Keyfunc:    func(*jwt.Token) (any, error) { return []byte(hsSecret), nil },
	}, nil)
}

func hs256Token(t *testing.T, tenant string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":       "mcp-user",
		"tenant_id": tenant,
		"exp":       time.Now().Add(time.Hour).Unix(),
	})
	s, err := tok.SignedString([]byte(hsSecret))
	require.NoError(t, err)
	return s
}

// ---------------------------------------------------------------------------
// JSON-RPC client helpers
// ---------------------------------------------------------------------------

// rpcEnvelope is the decoded JSON-RPC response envelope the helpers return.
type rpcEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

// rawRPC posts a raw JSON-RPC body (string) and returns the http.Response for
// status/header assertions. The caller closes the body.
func (h *harness) rawRPC(t *testing.T, token, body string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.srv.URL+"/mcp", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// doRPC posts a single JSON-RPC request {method, params, id} and decodes the
// JSON-RPC response envelope. id is fixed to 1 (a JSON number) unless the test
// uses rawRPC directly.
func (h *harness) doRPC(t *testing.T, token, method string, params any) (rpcEnvelope, *http.Response) {
	t.Helper()
	body := buildRequest(t, 1, method, params)
	resp := h.rawRPC(t, token, body, nil)
	defer func() { _ = resp.Body.Close() }()
	var env rpcEnvelope
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	// re-wrap the consumed body status into a fresh response copy for header use
	return env, resp
}

// buildRequest renders a JSON-RPC request with the given (numeric) id, method,
// and params. A nil params is omitted.
func buildRequest(t *testing.T, id int64, method string, params any) string {
	t.Helper()
	m := map[string]any{"jsonrpc": "2.0", "id": id, "method": method}
	if params != nil {
		m["params"] = params
	}
	b, err := json.Marshal(m)
	require.NoError(t, err)
	return string(b)
}

// callTool is a convenience for tools/call: posts {name, arguments, _meta?} and
// returns the decoded envelope plus the response.
func (h *harness) callTool(t *testing.T, token, name string, args map[string]any) (rpcEnvelope, *http.Response) {
	t.Helper()
	return h.doRPC(t, token, "tools/call", map[string]any{"name": name, "arguments": args})
}

// callResult is the decoded CallToolResult body.
type callResult struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	StructuredContent map[string]any `json:"structuredContent"`
	IsError           bool           `json:"isError"`
}

func decodeCallResult(t *testing.T, env rpcEnvelope) callResult {
	t.Helper()
	require.Nil(t, env.Error, "expected a CallToolResult, got JSON-RPC error: %+v", env.Error)
	var cr callResult
	require.NoError(t, json.Unmarshal(env.Result, &cr))
	return cr
}

// ---------------------------------------------------------------------------
// SSE reading helper (no fixed sleeps; read frames until a predicate; R-4)
// ---------------------------------------------------------------------------

// sseFrame is one parsed SSE event from the run leg: the MCP JSON-RPC payload
// carried in the `data:` field, with the SSE `id:`/`event:` if present.
type sseFrame struct {
	id    string
	event string
	data  string
}

// openRunLeg opens a streaming POST /mcp carrying a tools/call run and returns
// the live response plus a scanner-backed reader. The caller reads frames with
// readSSEUntil and MUST close resp.Body.
func (h *harness) openRunLeg(t *testing.T, token string, args map[string]any, progressToken any) *http.Response {
	t.Helper()
	meta := map[string]any{}
	if progressToken != nil {
		meta["progressToken"] = progressToken
	}
	params := map[string]any{"name": "run", "arguments": args}
	if progressToken != nil {
		params["_meta"] = meta
	}
	body := buildRequest(t, 1, "tools/call", params)
	req, err := http.NewRequest(http.MethodPost, h.srv.URL+"/mcp", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// readSSEUntil reads SSE events from r until pred(frame) is true or the bounded
// deadline elapses. It returns the matching frame and all frames read so far.
// No fixed sleeps: it blocks on the reader, which is fed by the live stream.
func readSSEUntil(t *testing.T, r *http.Response, deadline time.Duration, pred func(sseFrame) bool) (sseFrame, []sseFrame, bool) {
	t.Helper()
	type res struct {
		match sseFrame
		all   []sseFrame
		ok    bool
	}
	ch := make(chan res, 1)
	go func() {
		sc := bufio.NewScanner(r.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		var all []sseFrame
		var cur sseFrame
		flush := func() {
			if cur.data != "" || cur.event != "" || cur.id != "" {
				all = append(all, cur)
			}
			cur = sseFrame{}
		}
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				f := cur
				flush()
				if (f.data != "" || f.event != "") && pred(f) {
					ch <- res{match: f, all: all, ok: true}
					return
				}
			case strings.HasPrefix(line, "id:"):
				cur.id = strings.TrimSpace(line[len("id:"):])
			case strings.HasPrefix(line, "event:"):
				cur.event = strings.TrimSpace(line[len("event:"):])
			case strings.HasPrefix(line, "data:"):
				cur.data += strings.TrimSpace(line[len("data:"):])
			}
		}
		ch <- res{all: all, ok: false}
	}()
	select {
	case got := <-ch:
		return got.match, got.all, got.ok
	case <-time.After(deadline):
		return sseFrame{}, nil, false
	}
}

// progressParams decodes a notifications/progress data payload's params.
type progressParams struct {
	ProgressToken json.RawMessage `json:"progressToken"`
	Progress      float64         `json:"progress"`
	Message       string          `json:"message"`
	// the in-band approval fields (T-16); present only on the approval frame.
	CallID   string `json:"call_id"`
	ToolName string `json:"tool_name"`
	ArgsJSON string `json:"args_json"`
	Reason   string `json:"reason"`
	AfterSeq int64  `json:"after_seq"`
}

// decodeProgress parses an SSE frame whose data is a notifications/progress
// JSON-RPC message and returns its params.
func decodeProgress(t *testing.T, f sseFrame) (string, progressParams) {
	t.Helper()
	var msg struct {
		JSONRPC string         `json:"jsonrpc"`
		Method  string         `json:"method"`
		Params  progressParams `json:"params"`
	}
	require.NoError(t, json.Unmarshal([]byte(f.data), &msg), "frame data must be a JSON-RPC message: %s", f.data)
	return msg.Method, msg.Params
}

// reqCtx is a background context for direct fake-store reads in assertions.
func reqCtx() context.Context { return context.Background() }

// stringReader wraps s as an io.Reader for http.Post bodies without importing
// strings at every call site that needs one.
func stringReader(s string) *strings.Reader { return strings.NewReader(s) }

// jsonString renders an arbitrary decoded JSON value as a string for token
// substring assertions (protojson enums decode to strings; numbers to floats).
func jsonString(t *testing.T, v any) string {
	t.Helper()
	if s, ok := v.(string); ok {
		return s
	}
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// assistantEvent builds a committed AssistantMessage carrying text (mirror
// rest_test.go).
func assistantEvent(text string) domain.AssistantMessage {
	return domain.AssistantMessage{
		Message: llm.Message{
			Role:    llm.RoleAssistant,
			Content: []llm.ContentPart{{Text: &llm.TextPart{Text: text}}},
		},
	}
}

// ---------------------------------------------------------------------------
// T-1 — the canonical RED: the package + NewHandler/Routes skeleton must exist.
// ---------------------------------------------------------------------------

// TestHarness_Boots constructs the dev harness and asserts POST /mcp with a
// trivially-malformed body returns *something* (a parse-error JSON-RPC
// response). Until the skeleton lands this does not compile — the canonical RED.
func TestHarness_Boots(t *testing.T) {
	h := devHarness(t)
	require.NotEmpty(t, h.srv.URL)

	resp := h.rawRPC(t, "", "not json at all", nil)
	defer func() { _ = resp.Body.Close() }()
	// A malformed body is a JSON-RPC parse error (-32700); the HTTP envelope is
	// still 200 (JSON-RPC carries the error), per the protocol.
	var env rpcEnvelope
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	require.NotNil(t, env.Error, "a malformed body must yield a JSON-RPC error")
}
