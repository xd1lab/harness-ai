package grpc

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
	"github.com/xd1lab/harness-ai/internal/platform/ids/idstest"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ---------------------------------------------------------------------------
// tailingEventLog — an in-memory app.EventLogPort whose Subscribe tails LIVE
// appends (unlike apptest.FakeEventLog, whose Subscribe is snapshot-only). This
// lets the server's Run relay observe events the fake Runner appends after the
// subscription opens, exercising the resumable streaming path realistically.
// ---------------------------------------------------------------------------

type tailingEventLog struct {
	mu           sync.Mutex
	events       map[string][]domain.EventEnvelope
	heads        map[string]int64
	tenants      map[string]string                // sessionID -> owning tenant
	modes        map[string]domain.PermissionMode // sessionID -> persisted permission mode
	subs         map[string][]chan domain.EventEnvelope
	forkErr      error
	appendCB     func(domain.EventEnvelope) // optional hook fired after each append
	created      []string                   // session ids passed to CreateSession, in order
	createdModes []domain.PermissionMode    // mode passed to CreateSession, parallel to created
	createErr    error                      // optional error returned by CreateSession

	// adminSessions is the per-session control/lineage projection the Feature I
	// ListSessions fake (admin_api_test.go) lists over (status, created_at, mode,
	// lineage), keyed by session id. It is populated by seedAdminSession and is
	// independent of the events map so a list test can control created_at directly.
	adminSessions map[string]adminSession
}

// Compile-time assertions: the fake satisfies both the frozen port AND the
// Server's consumer-side EventStore (CreateSession + EventLogPort), since it is
// injected as the Server's log.
var (
	_ app.EventLogPort = (*tailingEventLog)(nil)
	_ EventStore       = (*tailingEventLog)(nil)
)

func newTailingEventLog() *tailingEventLog {
	return &tailingEventLog{
		events:  make(map[string][]domain.EventEnvelope),
		heads:   make(map[string]int64),
		tenants: make(map[string]string),
		modes:   make(map[string]domain.PermissionMode),
		subs:    make(map[string][]chan domain.EventEnvelope),
	}
}

// seed creates a session owned by tenant with a SessionStarted at seq 1, in the
// default permission mode.
func (l *tailingEventLog) seed(sessionID, tenant string) {
	l.seedMode(sessionID, tenant, domain.ModeDefault)
}

// seedMode is seed with an explicit standing permission mode (so a Run test can
// assert the session's mode flows into the RunSpec).
func (l *tailingEventLog) seedMode(sessionID, tenant string, mode domain.PermissionMode) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tenants[sessionID] = tenant
	l.modes[sessionID] = mode
	l.heads[sessionID] = 1
	l.events[sessionID] = []domain.EventEnvelope{{
		Type: domain.EventSessionStarted, Seq: 1, SessionID: sessionID,
		TenantID: tenant, Actor: domain.ActorSystem, Event: domain.SessionStarted{},
	}}
}

// CreateSession records a fresh active session aggregate (head_seq=0) owned by
// the RLS-context tenant, mirroring the real store's session-creation half. It
// returns the created session so the Server's CreateSession can proceed to append
// the first SessionStarted.
func (l *tailingEventLog) CreateSession(ctx context.Context, sessionID string, mode domain.PermissionMode) (domain.Session, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.createErr != nil {
		return domain.Session{}, l.createErr
	}
	tenant, _ := db.TenantFromContext(ctx)
	l.tenants[sessionID] = tenant
	l.modes[sessionID] = mode.OrDefault()
	l.heads[sessionID] = 0
	if _, ok := l.events[sessionID]; !ok {
		l.events[sessionID] = nil
	}
	l.created = append(l.created, sessionID)
	l.createdModes = append(l.createdModes, mode)
	return domain.Session{ID: sessionID, TenantID: tenant, Status: domain.StatusActive, HeadSeq: 0, Mode: mode.OrDefault()}, nil
}

func (l *tailingEventLog) Append(ctx context.Context, sessionID string, _, _ int64, requestID string, events ...app.AppendInput) ([]domain.EventEnvelope, error) {
	l.mu.Lock()
	// A first append to an unknown session establishes its owning tenant from the
	// RLS context the auth interceptor placed (mirrors the real store stamping
	// tenant_id from the SET LOCAL app.current_tenant GUC).
	if _, known := l.tenants[sessionID]; !known {
		if tenant, err := db.TenantFromContext(ctx); err == nil {
			l.tenants[sessionID] = tenant
		}
	}
	out := make([]domain.EventEnvelope, 0, len(events))
	var fired []domain.EventEnvelope
	for _, e := range events {
		l.heads[sessionID]++
		env := domain.EventEnvelope{
			Type: e.Event.EventType(), Seq: l.heads[sessionID], SessionID: sessionID,
			TenantID: l.tenants[sessionID], RequestID: requestID, Actor: e.Actor, Event: e.Event,
		}
		l.events[sessionID] = append(l.events[sessionID], env)
		out = append(out, env)
		fired = append(fired, env)
		for _, ch := range l.subs[sessionID] {
			select {
			case ch <- env:
			default:
			}
		}
	}
	cb := l.appendCB
	l.mu.Unlock()
	if cb != nil {
		for _, env := range fired {
			cb(env)
		}
	}
	return out, nil
}

func (l *tailingEventLog) Load(_ context.Context, sessionID string, fromSeq int64) ([]domain.EventEnvelope, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []domain.EventEnvelope
	for _, e := range l.events[sessionID] {
		if e.Seq >= fromSeq {
			out = append(out, e)
		}
	}
	return out, nil
}

func (l *tailingEventLog) LoadSession(_ context.Context, sessionID string) (domain.Session, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	tenant, ok := l.tenants[sessionID]
	if !ok {
		return domain.Session{}, errors.New("not found")
	}
	return domain.Session{ID: sessionID, TenantID: tenant, HeadSeq: l.heads[sessionID], Status: domain.StatusActive, Mode: l.modes[sessionID]}, nil
}

func (l *tailingEventLog) Subscribe(ctx context.Context, sessionID string, fromSeq int64) (<-chan domain.EventEnvelope, error) {
	ch := make(chan domain.EventEnvelope, 256)
	l.mu.Lock()
	// Deliver the backlog strictly greater than fromSeq.
	for _, e := range l.events[sessionID] {
		if e.Seq > fromSeq {
			ch <- e
		}
	}
	l.subs[sessionID] = append(l.subs[sessionID], ch)
	l.mu.Unlock()

	go func() {
		<-ctx.Done()
		l.mu.Lock()
		subs := l.subs[sessionID]
		for i, c := range subs {
			if c == ch {
				l.subs[sessionID] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		l.mu.Unlock()
		close(ch)
	}()
	return ch, nil
}

func (l *tailingEventLog) Fork(_ context.Context, parentID string, atSeq int64, newSessionID string) (domain.Session, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.forkErr != nil {
		return domain.Session{}, l.forkErr
	}
	tenant := l.tenants[parentID]
	l.tenants[newSessionID] = tenant
	l.modes[newSessionID] = l.modes[parentID]
	l.heads[newSessionID] = atSeq
	return domain.Session{ID: newSessionID, ParentID: parentID, ForkedFromSeq: atSeq, HeadSeq: atSeq, TenantID: tenant, Status: domain.StatusActive, Mode: l.modes[parentID]}, nil
}

// ---------------------------------------------------------------------------
// fakeRunner — a configurable Runner. Its run function receives the spec, the
// event log to append to, and the gate, so each test scripts the loop behavior.
// ---------------------------------------------------------------------------

type fakeRunner struct {
	log  *tailingEventLog
	gate app.ApprovalGate
	fn   func(ctx context.Context, spec RunSpec, log *tailingEventLog) (RunOutcome, error)
}

var _ Runner = (*fakeRunner)(nil)

func (r *fakeRunner) Run(ctx context.Context, spec RunSpec) (RunOutcome, error) {
	return r.fn(ctx, spec, r.log)
}

// appendAssistantText appends a text-only AssistantMessage turn to the session.
func appendAssistantText(ctx context.Context, log *tailingEventLog, sessionID, turnID, text string) {
	_, _ = log.Append(ctx, sessionID, 0, 0, "req-"+turnID, app.AppendInput{
		Event: domain.AssistantMessage{
			TurnID:     turnID,
			Message:    llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: text}}}},
			StopReason: llm.StopEnd,
		},
		Actor: domain.ActorAssistant,
	})
}

// newFakeIDs returns a deterministic, effectively-inexhaustible id generator for
// the server tests (it cycles a long sequence so a test never exhausts it).
func newFakeIDs() *idstest.Fake {
	g := idstest.Sequential(10000)
	g.Cyclic = true
	return g
}

// ---------------------------------------------------------------------------
// test harness — bufconn server wired with the (dev-mode by default) auth
// interceptor and the Server under test.
// ---------------------------------------------------------------------------

type testHarness struct {
	client genproto.OrchestratorServiceClient
	log    *tailingEventLog
	gate   *notifyingGate
	srv    *Server
}

// startServerWith stands up the given *Server on a bufconn listener with the
// auth interceptor, returning a connected client.
func startServerWith(t *testing.T, authCfg AuthConfig, server *Server) *grpc.ClientConn {
	t.Helper()

	unaryAuth, err := NewAuthInterceptor(authCfg)
	require.NoError(t, err)
	streamAuth, err := NewStreamAuthInterceptor(authCfg)
	require.NoError(t, err)

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unaryAuth),
		grpc.ChainStreamInterceptor(streamAuth),
	)
	genproto.RegisterOrchestratorServiceServer(srv, server)

	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// startServer builds a default Server (log/gate/runner) and serves it.
func startServer(t *testing.T, authCfg AuthConfig, log *tailingEventLog, gate app.ApprovalGate, runner Runner) *grpc.ClientConn {
	t.Helper()
	server := NewServer(log, gate, runner, newFakeIDs(), Config{})
	return startServerWith(t, authCfg, server)
}

// devHarness builds a dev-mode harness for a given tenant with a notifying gate
// and the supplied runner.
func devHarness(t *testing.T, tenant string, runner Runner, log *tailingEventLog) *testHarness {
	gate := newNotifyingGate()
	server := NewServer(log, gate, runner, newFakeIDs(), Config{})
	conn := startServerWith(t, AuthConfig{DevInsecure: true, DevPrincipal: Principal{TenantID: tenant, Subject: "dev"}}, server)
	return &testHarness{client: genproto.NewOrchestratorServiceClient(conn), log: log, gate: gate, srv: server}
}

// collectRunEvents reads the Run stream to completion (terminal Result or EOF),
// returning all received frames.
func collectRunEvents(t *testing.T, stream genproto.OrchestratorService_RunClient) []*genproto.RunEvent {
	t.Helper()
	var got []*genproto.RunEvent
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return got
		}
		require.NoError(t, err)
		got = append(got, ev)
		if ev.GetResult() != nil {
			return got
		}
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestRun_StreamsLoopEvents(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-1", "tenant-A")

	runner := &fakeRunner{log: log, fn: func(ctx context.Context, spec RunSpec, l *tailingEventLog) (RunOutcome, error) {
		// One tool result then a text-only assistant turn.
		_, _ = l.Append(ctx, spec.SessionID, 0, 0, "r-tool", app.AppendInput{
			Event: domain.ToolResult{CallID: "c1", Result: "tool-output"}, Actor: domain.ActorTool,
		})
		appendAssistantText(ctx, l, spec.SessionID, "t1", "hello world")
		return RunOutcome{Reason: domain.Success, FinalText: "hello world", NumTurns: 1}, nil
	}}

	h := devHarness(t, "tenant-A", runner, log)
	stream, err := h.client.Run(context.Background(), &genproto.RunRequest{TenantId: "tenant-A", SessionId: "sess-1"})
	require.NoError(t, err)

	got := collectRunEvents(t, stream)

	// Expect: a ToolProgress (from ToolResult), a TextDelta (assembled assistant
	// text), and a terminal Result — each frame carrying a seq.
	var sawToolProgress, sawText bool
	var result *genproto.RunResult
	for _, ev := range got {
		assert.Greater(t, ev.GetSeq(), int64(0), "every frame carries a seq")
		switch {
		case ev.GetToolProgress() != nil:
			sawToolProgress = true
			assert.Equal(t, "tool-output", ev.GetToolProgress().GetMessage())
		case ev.GetTextDelta() != nil:
			sawText = true
			assert.Equal(t, "hello world", ev.GetTextDelta().GetText())
		case ev.GetResult() != nil:
			result = ev.GetResult()
		}
	}
	assert.True(t, sawToolProgress, "tool result should be relayed as ToolProgress")
	assert.True(t, sawText, "assistant text should be relayed as a TextDelta")
	require.NotNil(t, result, "stream must end with a terminal Result")
	assert.Equal(t, genproto.TerminationSubtype_TERMINATION_SUBTYPE_SUCCESS, result.GetSubtype())
	assert.Equal(t, "hello world", result.GetFinalText())
	assert.Equal(t, int64(1), result.GetNumTurns())
}

func TestRun_ResumeAfterSeqSkipsAlreadyDeliveredFrames(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-r", "tenant-A")
	// Pre-commit an assistant turn at seq 2 (already delivered to a prior client).
	appendAssistantText(context.Background(), log, "sess-r", "t0", "earlier text")

	runner := &fakeRunner{log: log, fn: func(ctx context.Context, spec RunSpec, l *tailingEventLog) (RunOutcome, error) {
		appendAssistantText(ctx, l, spec.SessionID, "t1", "newer text")
		return RunOutcome{Reason: domain.Success, FinalText: "newer text", NumTurns: 2}, nil
	}}
	h := devHarness(t, "tenant-A", runner, log)

	// Reconnect with after_seq=2: the earlier turn (seq 2) must NOT be re-sent.
	stream, err := h.client.Run(context.Background(), &genproto.RunRequest{TenantId: "tenant-A", SessionId: "sess-r", AfterSeq: 2})
	require.NoError(t, err)
	got := collectRunEvents(t, stream)

	for _, ev := range got {
		if ev.GetTextDelta() != nil {
			assert.NotEqual(t, "earlier text", ev.GetTextDelta().GetText(), "frames at/below after_seq must not be re-sent")
			assert.Greater(t, ev.GetSeq(), int64(2))
		}
	}
}

func TestRun_RejectsForeignTenantSession(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-A", "tenant-A") // owned by A

	runner := &fakeRunner{log: log, fn: func(_ context.Context, _ RunSpec, _ *tailingEventLog) (RunOutcome, error) {
		return RunOutcome{Reason: domain.Success}, nil
	}}
	// Authenticated as tenant-B.
	h := devHarness(t, "tenant-B", runner, log)

	stream, err := h.client.Run(context.Background(), &genproto.RunRequest{TenantId: "tenant-B", SessionId: "sess-A"})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "tenant B cannot Run tenant A's session (ownership)")
}

func TestControl_ApproveResolvesGateAndLoopProceeds(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-ap", "tenant-A")

	gate := newNotifyingGate()
	proceeded := make(chan struct{})
	runner := &fakeRunner{log: log, gate: gate, fn: func(ctx context.Context, spec RunSpec, l *tailingEventLog) (RunOutcome, error) {
		// Block on a human approval; proceed only after it is granted.
		res, err := gate.Request(ctx, app.ApprovalRequest{SessionID: spec.SessionID, CallID: "call-1", ToolName: "bash", Reason: "mutating"})
		if err != nil {
			return RunOutcome{}, err
		}
		if res != domain.AskAllowed {
			return RunOutcome{Reason: domain.ErrorDuringExecution}, nil
		}
		appendAssistantText(ctx, l, spec.SessionID, "t1", "done after approval")
		close(proceeded)
		return RunOutcome{Reason: domain.Success, FinalText: "done after approval", NumTurns: 1}, nil
	}}

	server := NewServer(log, gate, runner, newFakeIDs(), Config{})
	conn := startServerWith(t, AuthConfig{DevInsecure: true, DevPrincipal: Principal{TenantID: "tenant-A"}}, server)
	client := genproto.NewOrchestratorServiceClient(conn)

	// Start Run in the background; it will block awaiting approval.
	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Run(streamCtx, &genproto.RunRequest{TenantId: "tenant-A", SessionId: "sess-ap"})
	require.NoError(t, err)
	// Drain the stream in the background WITHOUT test assertions: when the test
	// ends and cancels streamCtx, Recv returns an error, and using require/assert
	// from a goroutine after the test completes would panic. Just read until error.
	go func() {
		for {
			if _, e := stream.Recv(); e != nil {
				return
			}
		}
	}()

	// Wait until the loop is actually blocked on the gate, then approve.
	require.Eventually(t, func() bool { return gate.pendingCount() > 0 }, 2*time.Second, 5*time.Millisecond)

	_, err = client.Control(context.Background(), &genproto.ControlRequest{
		TenantId: "tenant-A", SessionId: "sess-ap",
		Action: &genproto.ControlRequest_Approve{Approve: &genproto.ApproveAction{CallId: "call-1"}},
	})
	require.NoError(t, err)

	select {
	case <-proceeded:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not proceed after Control.Approve resolved the gate")
	}
}

func TestControl_DenyForeignTenantSession(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-A", "tenant-A")
	gate := newNotifyingGate()
	runner := &fakeRunner{log: log, fn: func(_ context.Context, _ RunSpec, _ *tailingEventLog) (RunOutcome, error) { return RunOutcome{}, nil }}
	conn := startServer(t, AuthConfig{DevInsecure: true, DevPrincipal: Principal{TenantID: "tenant-B"}}, log, gate, runner)
	client := genproto.NewOrchestratorServiceClient(conn)

	_, err := client.Control(context.Background(), &genproto.ControlRequest{
		TenantId: "tenant-B", SessionId: "sess-A",
		Action: &genproto.ControlRequest_Approve{Approve: &genproto.ApproveAction{CallId: "x"}},
	})
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestControl_InterruptCancelsLoop(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-int", "tenant-A")

	cancelled := make(chan struct{})
	runner := &fakeRunner{log: log, fn: func(ctx context.Context, spec RunSpec, l *tailingEventLog) (RunOutcome, error) {
		// Block until the loop context is cancelled by Interrupt.
		<-ctx.Done()
		// Record the cooperative abort, as the real loop would.
		_, _ = l.Append(context.Background(), spec.SessionID, 0, 0, "r-abort", app.AppendInput{
			Event: domain.TurnAborted{TurnID: "t1", Reason: domain.ErrorDuringExecution}, Actor: domain.ActorSystem,
		})
		close(cancelled)
		return RunOutcome{Reason: domain.ErrorDuringExecution}, ctx.Err()
	}}
	h := devHarness(t, "tenant-A", runner, log)

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	stream, err := h.client.Run(streamCtx, &genproto.RunRequest{TenantId: "tenant-A", SessionId: "sess-int"})
	require.NoError(t, err)
	// Collect the stream in the background; after interrupt it must end with a
	// typed terminal Result (not a bare Canceled), since the client did not
	// disconnect.
	resultCh := make(chan *genproto.RunResult, 1)
	go func() {
		var last *genproto.RunResult
		for {
			ev, e := stream.Recv()
			if e != nil {
				resultCh <- last
				return
			}
			if ev.GetResult() != nil {
				last = ev.GetResult()
			}
		}
	}()

	// Wait until the run has registered (an in-flight slot is held), then
	// interrupt — this guarantees the loop's cancel func is registered.
	require.Eventually(t, func() bool { return h.srv.inFlightFor("tenant-A") == 1 }, 2*time.Second, 5*time.Millisecond)

	_, err = h.client.Control(context.Background(), &genproto.ControlRequest{
		TenantId: "tenant-A", SessionId: "sess-int",
		Action: &genproto.ControlRequest_Interrupt{Interrupt: &genproto.InterruptAction{}},
	})
	require.NoError(t, err)

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("loop was not cancelled by Control.Interrupt")
	}

	// After the interrupt, the client stream must terminate with the typed
	// outcome (error_during_execution), since the run was interrupted, not the
	// client disconnected (FR-LOOP-03).
	select {
	case res := <-resultCh:
		require.NotNil(t, res, "interrupted run should still deliver a terminal Result")
		assert.Equal(t, genproto.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_DURING_EXECUTION, res.GetSubtype())
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not deliver a terminal Result after interrupt")
	}
}

func TestFork_DeniesCrossTenant(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-A", "tenant-A")
	gate := newNotifyingGate()
	runner := &fakeRunner{log: log, fn: func(_ context.Context, _ RunSpec, _ *tailingEventLog) (RunOutcome, error) { return RunOutcome{}, nil }}
	conn := startServer(t, AuthConfig{DevInsecure: true, DevPrincipal: Principal{TenantID: "tenant-B"}}, log, gate, runner)
	client := genproto.NewOrchestratorServiceClient(conn)

	_, err := client.Fork(context.Background(), &genproto.ForkRequest{TenantId: "tenant-B", SessionId: "sess-A", AtSeq: 1})
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "fork of a foreign-tenant session is denied (FR-STATE-03 AC-2)")
}

func TestFork_SameTenantSucceeds(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-A", "tenant-A")
	gate := newNotifyingGate()
	runner := &fakeRunner{log: log, fn: func(_ context.Context, _ RunSpec, _ *tailingEventLog) (RunOutcome, error) { return RunOutcome{}, nil }}
	conn := startServer(t, AuthConfig{DevInsecure: true, DevPrincipal: Principal{TenantID: "tenant-A"}}, log, gate, runner)
	client := genproto.NewOrchestratorServiceClient(conn)

	resp, err := client.Fork(context.Background(), &genproto.ForkRequest{TenantId: "tenant-A", SessionId: "sess-A", AtSeq: 1})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.GetSessionId())
}

func TestCreateAndGetSession(t *testing.T) {
	log := newTailingEventLog()
	gate := newNotifyingGate()
	runner := &fakeRunner{log: log, fn: func(_ context.Context, _ RunSpec, _ *tailingEventLog) (RunOutcome, error) { return RunOutcome{}, nil }}
	conn := startServer(t, AuthConfig{DevInsecure: true, DevPrincipal: Principal{TenantID: "tenant-A"}}, log, gate, runner)
	client := genproto.NewOrchestratorServiceClient(conn)

	created, err := client.CreateSession(context.Background(), &genproto.CreateSessionRequest{TenantId: "tenant-A"})
	require.NoError(t, err)
	require.NotEmpty(t, created.GetSessionId())

	// The session aggregate row was created (create-then-append path) — without it
	// the SessionStarted Append would have been rejected SessionNotActive.
	log.mu.Lock()
	createdIDs := append([]string(nil), log.created...)
	log.mu.Unlock()
	assert.Equal(t, []string{created.GetSessionId()}, createdIDs, "CreateSession must create the aggregate row for the new session")

	got, err := client.GetSession(context.Background(), &genproto.GetSessionRequest{TenantId: "tenant-A", SessionId: created.GetSessionId()})
	require.NoError(t, err)
	assert.Equal(t, created.GetSessionId(), got.GetSession().GetSessionId())
	assert.Equal(t, "tenant-A", got.GetSession().GetTenantId())
	assert.Equal(t, genproto.SessionStatus_SESSION_STATUS_ACTIVE, got.GetSession().GetStatus())
}

// TestCreateSession_CreatesRowThenAppendsStarted asserts the ordering the live
// bug violated: the Server must create the session aggregate row BEFORE appending
// the first SessionStarted (so the active-status guard on Append is satisfied),
// and the happy path returns a non-empty session id with no "session is not
// active" failure.
func TestCreateSession_CreatesRowThenAppendsStarted(t *testing.T) {
	log := newTailingEventLog()
	gate := newNotifyingGate()
	runner := &fakeRunner{log: log, fn: func(_ context.Context, _ RunSpec, _ *tailingEventLog) (RunOutcome, error) { return RunOutcome{}, nil }}

	// Record the operation order: a CreateSession marker, then each appended event.
	var (
		mu    sync.Mutex
		order []string
	)
	log.appendCB = func(env domain.EventEnvelope) {
		mu.Lock()
		order = append(order, "append:"+string(env.Type))
		mu.Unlock()
	}

	conn := startServer(t, AuthConfig{DevInsecure: true, DevPrincipal: Principal{TenantID: "tenant-A"}}, log, gate, runner)
	client := genproto.NewOrchestratorServiceClient(conn)

	created, err := client.CreateSession(context.Background(), &genproto.CreateSessionRequest{TenantId: "tenant-A"})
	require.NoError(t, err, "happy path must not fail with 'session is not active'")
	require.NotEmpty(t, created.GetSessionId())

	log.mu.Lock()
	gotCreated := append([]string(nil), log.created...)
	headSeq := log.heads[created.GetSessionId()]
	log.mu.Unlock()
	mu.Lock()
	gotOrder := append([]string(nil), order...)
	mu.Unlock()

	// CreateSession created exactly this session, the SessionStarted was appended
	// afterwards (bumping head_seq 0->1), and the session is owned by the tenant.
	assert.Equal(t, []string{created.GetSessionId()}, gotCreated, "CreateSession created the new session aggregate")
	assert.Equal(t, []string{"append:" + string(domain.EventSessionStarted)}, gotOrder, "exactly one SessionStarted appended after the row is created")
	assert.Equal(t, int64(1), headSeq, "SessionStarted append bumps head_seq 0->1")
}

// TestCreateSession_CreateRowFailureMapsToError asserts a CreateSession failure
// is surfaced as a gRPC error (not silently swallowed) and that the subsequent
// Append is NOT attempted when the row was never created.
func TestCreateSession_CreateRowFailureMapsToError(t *testing.T) {
	log := newTailingEventLog()
	log.createErr = errors.New("boom: insert failed")
	gate := newNotifyingGate()
	runner := &fakeRunner{log: log, fn: func(_ context.Context, _ RunSpec, _ *tailingEventLog) (RunOutcome, error) { return RunOutcome{}, nil }}
	var appended bool
	log.appendCB = func(domain.EventEnvelope) { appended = true }

	conn := startServer(t, AuthConfig{DevInsecure: true, DevPrincipal: Principal{TenantID: "tenant-A"}}, log, gate, runner)
	client := genproto.NewOrchestratorServiceClient(conn)

	_, err := client.CreateSession(context.Background(), &genproto.CreateSessionRequest{TenantId: "tenant-A"})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err), "a bare CreateSession failure maps to Internal")
	assert.False(t, appended, "no SessionStarted is appended when the aggregate row was not created")
}

func TestCreateSession_RejectsClientBypassMode(t *testing.T) {
	log := newTailingEventLog()
	gate := newNotifyingGate()
	runner := &fakeRunner{log: log, fn: func(_ context.Context, _ RunSpec, _ *tailingEventLog) (RunOutcome, error) { return RunOutcome{}, nil }}
	conn := startServer(t, AuthConfig{DevInsecure: true, DevPrincipal: Principal{TenantID: "tenant-A"}}, log, gate, runner)
	client := genproto.NewOrchestratorServiceClient(conn)

	_, err := client.CreateSession(context.Background(), &genproto.CreateSessionRequest{
		TenantId: "tenant-A", Mode: genproto.PermissionMode_PERMISSION_MODE_BYPASS,
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

// TestCreateSession_PersistsRequestedMode asserts a (verified, non-bypass)
// permission mode on the request is persisted on the session aggregate at
// creation and surfaced back by GetSession (ADR-0019).
func TestCreateSession_PersistsRequestedMode(t *testing.T) {
	log := newTailingEventLog()
	gate := newNotifyingGate()
	runner := &fakeRunner{log: log, fn: func(_ context.Context, _ RunSpec, _ *tailingEventLog) (RunOutcome, error) { return RunOutcome{}, nil }}
	conn := startServer(t, AuthConfig{DevInsecure: true, DevPrincipal: Principal{TenantID: "tenant-A"}}, log, gate, runner)
	client := genproto.NewOrchestratorServiceClient(conn)

	created, err := client.CreateSession(context.Background(), &genproto.CreateSessionRequest{
		TenantId: "tenant-A", Mode: genproto.PermissionMode_PERMISSION_MODE_ACCEPT_EDITS,
	})
	require.NoError(t, err)

	log.mu.Lock()
	gotModes := append([]domain.PermissionMode(nil), log.createdModes...)
	log.mu.Unlock()
	require.Equal(t, []domain.PermissionMode{domain.ModeAcceptEdits}, gotModes, "the requested mode is persisted at CreateSession")

	got, err := client.GetSession(context.Background(), &genproto.GetSessionRequest{TenantId: "tenant-A", SessionId: created.GetSessionId()})
	require.NoError(t, err)
	assert.Equal(t, genproto.PermissionMode_PERMISSION_MODE_ACCEPT_EDITS, got.GetSession().GetMode(), "GetSession surfaces the session's stored mode")
}

// TestRun_UsesSessionMode asserts the session's stored permission mode flows into
// the RunSpec the loop runs under (ADR-0019) — not a hardcoded default.
func TestRun_UsesSessionMode(t *testing.T) {
	log := newTailingEventLog()
	log.seedMode("sess-plan", "tenant-A", domain.ModePlan)

	modeCh := make(chan policy.Mode, 1)
	runner := &fakeRunner{log: log, fn: func(ctx context.Context, spec RunSpec, l *tailingEventLog) (RunOutcome, error) {
		modeCh <- spec.Mode
		appendAssistantText(ctx, l, spec.SessionID, "t1", "ok")
		return RunOutcome{Reason: domain.Success, FinalText: "ok", NumTurns: 1}, nil
	}}

	h := devHarness(t, "tenant-A", runner, log)
	stream, err := h.client.Run(context.Background(), &genproto.RunRequest{TenantId: "tenant-A", SessionId: "sess-plan"})
	require.NoError(t, err)
	_ = collectRunEvents(t, stream)

	select {
	case gotMode := <-modeCh:
		assert.Equal(t, policy.ModePlan, gotMode, "the session's stored mode must drive the run's policy mode")
	default:
		t.Fatal("runner was not invoked")
	}
}

func TestRun_PerTenantConcurrencyCap(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-1", "tenant-A")
	log.seed("sess-2", "tenant-A")

	release := make(chan struct{})
	runner := &fakeRunner{log: log, fn: func(_ context.Context, _ RunSpec, _ *tailingEventLog) (RunOutcome, error) {
		<-release // hold the slot
		return RunOutcome{Reason: domain.Success}, nil
	}}

	gate := newNotifyingGate()
	srv := NewServer(log, gate, runner, newFakeIDs(), Config{MaxInFlightPerTenant: 1})
	c2 := startServerWith(t, AuthConfig{DevInsecure: true, DevPrincipal: Principal{TenantID: "tenant-A"}}, srv)
	client := genproto.NewOrchestratorServiceClient(c2)

	// First Run occupies the only slot.
	s1, err := client.Run(context.Background(), &genproto.RunRequest{TenantId: "tenant-A", SessionId: "sess-1"})
	require.NoError(t, err)
	go func() { _, _ = s1.Recv() }()
	require.Eventually(t, func() bool { return srv.inFlightFor("tenant-A") == 1 }, time.Second, 5*time.Millisecond)

	// Second Run must be rejected with RESOURCE_EXHAUSTED.
	s2, err := client.Run(context.Background(), &genproto.RunRequest{TenantId: "tenant-A", SessionId: "sess-2"})
	require.NoError(t, err)
	_, err = s2.Recv()
	require.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))

	close(release)
}

// TestRun_ProdAuthRejectsMissingToken exercises the full edge in production-auth
// mode over bufconn: a Run without a bearer token is rejected UNAUTHENTICATED,
// and the same Run with a valid token (tenant matching the token) streams to a
// terminal result.
func TestRun_ProdAuthRejectsMissingToken(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-1", "tenant-A")
	gate := newNotifyingGate()
	runner := &fakeRunner{log: log, fn: func(_ context.Context, _ RunSpec, _ *tailingEventLog) (RunOutcome, error) {
		return RunOutcome{Reason: domain.Success}, nil
	}}
	conn := startServer(t, prodAuthConfig(), log, gate, runner)
	client := genproto.NewOrchestratorServiceClient(conn)

	// No Authorization metadata → UNAUTHENTICATED on the stream.
	stream, err := client.Run(context.Background(), &genproto.RunRequest{TenantId: "tenant-A", SessionId: "sess-1"})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	// With a valid bearer token it is accepted (tenant matches the token).
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(map[string]string{"authorization": "Bearer " + signToken(t, validClaims())}))
	stream2, err := client.Run(ctx, &genproto.RunRequest{TenantId: "tenant-A", SessionId: "sess-1"})
	require.NoError(t, err)
	got := collectRunEvents(t, stream2)
	require.NotEmpty(t, got)
	assert.NotNil(t, got[len(got)-1].GetResult())
}
