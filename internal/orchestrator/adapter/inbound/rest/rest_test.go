// SPDX-License-Identifier: Apache-2.0

package rest_test

import (
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/rest"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
	"github.com/xd1lab/harness-ai/internal/platform/ids"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ---------------------------------------------------------------------------
// Fakes — an in-memory EventStore, ApprovalGate, and Runner so the facade is
// exercised against the REAL igrpc.Server (ownership, caps, mapping) without a
// database or loop.
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

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type harness struct {
	store  *fakeStore
	gate   *fakeGate
	runner *fakeRunner
	srv    *httptest.Server
}

// newHarness builds the REST handler over a REAL igrpc.Server with the given
// authenticator config.
func newHarness(t *testing.T, ac igrpc.AuthConfig) *harness {
	t.Helper()
	h := &harness{store: newFakeStore(), gate: &fakeGate{}, runner: &fakeRunner{}}
	grpcSrv := igrpc.NewServer(h.store, h.gate, h.runner, ids.System{}, igrpc.Config{})
	auth, err := igrpc.NewAuthenticator(ac)
	require.NoError(t, err)
	mux := http.NewServeMux()
	rest.NewHandler(grpcSrv, auth).Routes(mux)
	h.srv = httptest.NewServer(mux)
	t.Cleanup(h.srv.Close)
	return h
}

func devHarness(t *testing.T) *harness {
	t.Helper()
	return newHarness(t, igrpc.AuthConfig{DevInsecure: true})
}

// doJSON performs a request with an optional JSON body and bearer token.
func (h *harness) doJSON(t *testing.T, method, path, token, body string) *http.Response {
	t.Helper()
	var rd *strings.Reader
	if body == "" {
		rd = strings.NewReader("")
	} else {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, h.srv.URL+path, rd)
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func decodeJSON(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var m map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&m))
	return m
}

// ---------------------------------------------------------------------------
// CreateSession
// ---------------------------------------------------------------------------

// TestCreateSession_DevMode pins the dev path: no token needed, the session is
// created under the dev tenant (proving ContextWithPrincipal scoped RLS), and
// the requested permission mode is persisted.
func TestCreateSession_DevMode(t *testing.T) {
	h := devHarness(t)
	resp := h.doJSON(t, http.MethodPost, "/v1/sessions", "", `{"mode":"acceptEdits"}`)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := decodeJSON(t, resp)
	sid, _ := body["sessionId"].(string)
	require.NotEmpty(t, sid, "response must carry sessionId (protojson)")

	sess, err := h.store.LoadSession(context.Background(), sid)
	require.NoError(t, err)
	assert.Equal(t, igrpc.DevTenantID, sess.TenantID, "session must be owned by the authenticated (dev) tenant via the RLS context")
	require.Len(t, h.store.createdModes, 1)
	assert.Equal(t, domain.PermissionMode("acceptEdits"), h.store.createdModes[0])
}

// TestCreateSession_BypassRejected pins that the facade cannot smuggle bypass
// mode past the server-side guard.
func TestCreateSession_BypassRejected(t *testing.T) {
	h := devHarness(t)
	resp := h.doJSON(t, http.MethodPost, "/v1/sessions", "", `{"mode":"bypass"}`)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestCreateSession_UnknownModeRejected pins strict mode parsing at the edge.
func TestCreateSession_UnknownModeRejected(t *testing.T) {
	h := devHarness(t)
	resp := h.doJSON(t, http.MethodPost, "/v1/sessions", "", `{"mode":"yolo"}`)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// Auth (production)
// ---------------------------------------------------------------------------

const hsSecret = "test-secret"

func prodHarness(t *testing.T) *harness {
	t.Helper()
	return newHarness(t, igrpc.AuthConfig{
		Algorithms: []string{"HS256"},
		Keyfunc:    func(*jwt.Token) (any, error) { return []byte(hsSecret), nil },
	})
}

func hs256Token(t *testing.T, tenant string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":       "rest-user",
		"tenant_id": tenant,
		"exp":       time.Now().Add(time.Hour).Unix(),
	})
	s, err := tok.SignedString([]byte(hsSecret))
	require.NoError(t, err)
	return s
}

// TestAuth_ProductionRequiresBearer pins FR-API-03 on the REST transport: no
// token => 401, a valid token => the claim tenant owns the created session.
func TestAuth_ProductionRequiresBearer(t *testing.T) {
	h := prodHarness(t)

	resp := h.doJSON(t, http.MethodPost, "/v1/sessions", "", `{}`)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, "missing bearer must be 401")

	tenant := "33333333-3333-4333-8333-333333333333"
	resp = h.doJSON(t, http.MethodPost, "/v1/sessions", hs256Token(t, tenant), `{}`)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := decodeJSON(t, resp)
	sid := body["sessionId"].(string)
	sess, err := h.store.LoadSession(context.Background(), sid)
	require.NoError(t, err)
	assert.Equal(t, tenant, sess.TenantID)
}

// TestAuth_GarbageTokenRejected pins that a malformed token is 401, never 500.
func TestAuth_GarbageTokenRejected(t *testing.T) {
	h := prodHarness(t)
	resp := h.doJSON(t, http.MethodPost, "/v1/sessions", "not-a-jwt", `{}`)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// Run (SSE)
// ---------------------------------------------------------------------------

// assistantEvent builds a committed AssistantMessage carrying text.
func assistantEvent(text string) domain.AssistantMessage {
	return domain.AssistantMessage{
		Message: llm.Message{
			Role:    llm.RoleAssistant,
			Content: []llm.ContentPart{{Text: &llm.TextPart{Text: text}}},
		},
	}
}

// TestRun_SSEStreamsFramesAndResult drives a run whose loop appends one
// assistant message: the response must be a text/event-stream carrying the
// text frame (with its durable seq as the SSE id) and the terminal result.
func TestRun_SSEStreamsFramesAndResult(t *testing.T) {
	h := devHarness(t)
	h.store.seed(domain.Session{ID: "s1", TenantID: igrpc.DevTenantID, HeadSeq: 1})
	h.runner.fn = func(ctx context.Context, spec igrpc.RunSpec) (igrpc.RunOutcome, error) {
		_, err := h.store.Append(ctx, spec.SessionID, 0, 0, "r1", app.AppendInput{Event: assistantEvent("hello from the loop")})
		if err != nil {
			return igrpc.RunOutcome{}, err
		}
		return igrpc.RunOutcome{Reason: domain.Success, FinalText: "hello from the loop", NumTurns: 1}, nil
	}

	resp := h.doJSON(t, http.MethodPost, "/v1/sessions/s1/run", "", `{"text":"hi"}`)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/event-stream")

	buf := new(strings.Builder)
	_, err := copyAll(buf, resp.Body)
	require.NoError(t, err)
	body := buf.String()

	assert.Contains(t, body, "id: 2", "the text frame must carry its durable seq as the SSE id")
	assert.Contains(t, body, "hello from the loop")
	assert.Contains(t, body, "event: result", "the stream must end with the terminal result frame")
	// protojson deliberately randomizes whitespace, so assert on the enum token
	// rather than exact JSON bytes.
	assert.Contains(t, body, "TERMINATION_SUBTYPE_SUCCESS", "result payload is canonical protojson")

	// The runner received the user text and the session's verified tenant.
	require.Len(t, h.runner.specs, 1)
	assert.Equal(t, igrpc.DevTenantID, h.runner.specs[0].TenantID)
}

// copyAll is io.Copy without importing io for one call site.
func copyAll(dst *strings.Builder, src interface{ Read([]byte) (int, error) }) (int64, error) {
	var n int64
	b := make([]byte, 4096)
	for {
		r, err := src.Read(b)
		dst.Write(b[:r])
		n += int64(r)
		if err != nil {
			if err.Error() == "EOF" {
				return n, nil
			}
			return n, err
		}
	}
}

// TestRun_LastEventIDResumes pins SSE reattach semantics: the standard
// Last-Event-ID header becomes the subscription cursor, so a reconnecting
// client never receives duplicate frames (FR-API-01).
func TestRun_LastEventIDResumes(t *testing.T) {
	h := devHarness(t)
	h.store.seed(domain.Session{ID: "s2", TenantID: igrpc.DevTenantID, HeadSeq: 7})

	req, err := http.NewRequest(http.MethodPost, h.srv.URL+"/v1/sessions/s2/run", strings.NewReader(`{}`))
	require.NoError(t, err)
	req.Header.Set("Last-Event-ID", "7")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, _ = copyAll(new(strings.Builder), resp.Body)

	require.NotEmpty(t, h.store.subscribedFrom)
	assert.Equal(t, int64(7), h.store.subscribedFrom[0], "Last-Event-ID must drive the resume cursor")
}

// TestRun_UnknownSessionIs404 pins error mapping before any SSE bytes.
func TestRun_UnknownSessionIs404(t *testing.T) {
	h := devHarness(t)
	resp := h.doJSON(t, http.MethodPost, "/v1/sessions/nope/run", "", `{"text":"hi"}`)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json", "pre-stream failures are plain JSON errors")
}

// ---------------------------------------------------------------------------
// Control / ownership / Fork / GetSession
// ---------------------------------------------------------------------------

// TestControl_ApproveResolvesGate pins the approve action wiring.
func TestControl_ApproveResolvesGate(t *testing.T) {
	h := devHarness(t)
	h.store.seed(domain.Session{ID: "s3", TenantID: igrpc.DevTenantID, HeadSeq: 4})

	resp := h.doJSON(t, http.MethodPost, "/v1/sessions/s3/control", "", `{"action":"approve","call_id":"call-9"}`)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := decodeJSON(t, resp)
	assert.Equal(t, "4", fmt.Sprint(body["headSeq"]), "response carries the session head (protojson int64 is a string)")

	require.Len(t, h.gate.resolves, 1)
	assert.Equal(t, resolveCall{"s3", "call-9", domain.AskAllowed}, h.gate.resolves[0])
}

// TestControl_DenyResolvesGate pins the deny action wiring.
func TestControl_DenyResolvesGate(t *testing.T) {
	h := devHarness(t)
	h.store.seed(domain.Session{ID: "s4", TenantID: igrpc.DevTenantID})
	resp := h.doJSON(t, http.MethodPost, "/v1/sessions/s4/control", "", `{"action":"deny","call_id":"call-1"}`)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, h.gate.resolves, 1)
	assert.Equal(t, domain.AskDenied, h.gate.resolves[0].resolution)
}

// TestControl_UnknownActionRejected pins strict action parsing.
func TestControl_UnknownActionRejected(t *testing.T) {
	h := devHarness(t)
	h.store.seed(domain.Session{ID: "s5", TenantID: igrpc.DevTenantID})
	resp := h.doJSON(t, http.MethodPost, "/v1/sessions/s5/control", "", `{"action":"explode"}`)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestOwnership_ForeignSessionDenied pins the cross-tenant guard on the REST
// transport: the dev principal cannot touch another tenant's session
// (FR-API-02 AC-1 analog).
func TestOwnership_ForeignSessionDenied(t *testing.T) {
	h := devHarness(t)
	h.store.seed(domain.Session{ID: "alien", TenantID: "99999999-9999-4999-8999-999999999999"})

	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/v1/sessions/alien/run", `{"text":"x"}`},
		{http.MethodPost, "/v1/sessions/alien/control", `{"action":"interrupt"}`},
		{http.MethodPost, "/v1/sessions/alien/fork", `{"at_seq":1}`},
		{http.MethodGet, "/v1/sessions/alien", ""},
	} {
		resp := h.doJSON(t, tc.method, tc.path, "", tc.body)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusForbidden, resp.StatusCode, "%s %s must be 403", tc.method, tc.path)
	}
}

// TestFork_CreatesChild pins the fork route.
func TestFork_CreatesChild(t *testing.T) {
	h := devHarness(t)
	h.store.seed(domain.Session{ID: "s6", TenantID: igrpc.DevTenantID, HeadSeq: 9})
	resp := h.doJSON(t, http.MethodPost, "/v1/sessions/s6/fork", "", `{"at_seq":5}`)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := decodeJSON(t, resp)
	child, _ := body["sessionId"].(string)
	require.NotEmpty(t, child)
	got, err := h.store.LoadSession(context.Background(), child)
	require.NoError(t, err)
	assert.Equal(t, "s6", got.ParentID)
	assert.Equal(t, int64(5), got.ForkedFromSeq)
}

// TestGetSession_ReturnsProjection pins the read route.
func TestGetSession_ReturnsProjection(t *testing.T) {
	h := devHarness(t)
	h.store.seed(domain.Session{ID: "s7", TenantID: igrpc.DevTenantID, HeadSeq: 3})
	resp := h.doJSON(t, http.MethodGet, "/v1/sessions/s7", "", "")
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := decodeJSON(t, resp)
	sess, _ := body["session"].(map[string]any)
	require.NotNil(t, sess, "response must carry the session projection")
	assert.Equal(t, "s7", sess["sessionId"])
}
