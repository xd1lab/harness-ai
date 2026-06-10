// Package apptest provides deterministic fakes for every consumer-defined port
// in [github.com/xd1lab/harness-ai/internal/orchestrator/app]:
// [app.EventLogPort], [app.ModelGatewayPort], [app.ToolRuntimePort],
// [app.ToolStream], [app.ApprovalGate], [app.HookRunner], and
// [app.SubAgentPort].
//
// All fakes are scriptable/recordable: callers configure responses in advance
// and can inspect recorded inputs afterward, making the agent-loop unit tests
// fully deterministic without any gRPC or database involvement.
package apptest

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ---------------------------------------------------------------------------
// Compile-time interface assertions
// ---------------------------------------------------------------------------

var (
	_ app.EventLogPort     = (*FakeEventLog)(nil)
	_ app.ModelGatewayPort = (*FakeModelGateway)(nil)
	_ app.ToolRuntimePort  = (*FakeToolRuntime)(nil)
	_ app.ToolStream       = (*FakeToolStream)(nil)
	_ app.ApprovalGate     = (*FakeApprovalGate)(nil)
	_ app.HookRunner       = (*FakeHookRunner)(nil)
	_ app.SubAgentPort     = (*FakeSubAgent)(nil)
)

// ---------------------------------------------------------------------------
// FakeEventLog
// ---------------------------------------------------------------------------

// AppendCall records one call to FakeEventLog.Append for later inspection.
type AppendCall struct {
	SessionID       string
	ExpectedHeadSeq int64
	LeaseEpoch      int64
	RequestID       string
	Events          []app.AppendInput
}

// FakeEventLog is an in-memory [app.EventLogPort]. Append records each call
// and builds a per-session event list; Load reads it back. Subscribe delivers
// all existing events then blocks until the context is cancelled. Fork creates
// a new child entry. An error may be configured per append call via
// AppendErr.
type FakeEventLog struct {
	mu sync.Mutex
	// sessions maps sessionID to its ordered EventEnvelopes.
	sessions map[string][]domain.EventEnvelope
	// appendCalls records every call to Append.
	appendCalls []AppendCall
	// seq tracks the current head seq per session.
	seqs map[string]int64
	// AppendErrs is a queue of errors to return from Append, consumed in order.
	AppendErrs []error
}

// NewFakeEventLog returns an empty FakeEventLog.
func NewFakeEventLog() *FakeEventLog {
	return &FakeEventLog{
		sessions: make(map[string][]domain.EventEnvelope),
		seqs:     make(map[string]int64),
	}
}

// Append records the call and appends envelope(s) to the session's in-memory
// list. It returns the next scripted error from AppendErrs, or nil.
func (f *FakeEventLog) Append(
	_ context.Context,
	sessionID string,
	expectedHeadSeq, leaseEpoch int64,
	requestID string,
	events ...app.AppendInput,
) ([]domain.EventEnvelope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appendCalls = append(f.appendCalls, AppendCall{
		SessionID:       sessionID,
		ExpectedHeadSeq: expectedHeadSeq,
		LeaseEpoch:      leaseEpoch,
		RequestID:       requestID,
		Events:          events,
	})
	if len(f.AppendErrs) > 0 {
		err := f.AppendErrs[0]
		f.AppendErrs = f.AppendErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	envelopes := make([]domain.EventEnvelope, 0, len(events))
	for _, e := range events {
		f.seqs[sessionID]++
		env := domain.EventEnvelope{
			Type:      e.Event.EventType(),
			Seq:       f.seqs[sessionID],
			SessionID: sessionID,
			RequestID: requestID,
			Actor:     e.Actor,
			Event:     e.Event,
		}
		f.sessions[sessionID] = append(f.sessions[sessionID], env)
		envelopes = append(envelopes, env)
	}
	return envelopes, nil
}

// Load returns all stored envelopes for sessionID from fromSeq (inclusive).
func (f *FakeEventLog) Load(_ context.Context, sessionID string, fromSeq int64) ([]domain.EventEnvelope, error) {
	f.mu.Lock()
	all := append([]domain.EventEnvelope(nil), f.sessions[sessionID]...)
	f.mu.Unlock()
	var out []domain.EventEnvelope
	for _, e := range all {
		if e.Seq >= fromSeq {
			out = append(out, e)
		}
	}
	return out, nil
}

// LoadSession returns a minimal Session aggregate for sessionID.
func (f *FakeEventLog) LoadSession(_ context.Context, sessionID string) (domain.Session, error) {
	f.mu.Lock()
	seq := f.seqs[sessionID]
	f.mu.Unlock()
	return domain.Session{
		ID:      sessionID,
		HeadSeq: seq,
		Status:  domain.StatusActive,
	}, nil
}

// Subscribe returns a channel that delivers all existing events then delivers
// any future events as they are Appended, until ctx is cancelled.
func (f *FakeEventLog) Subscribe(ctx context.Context, sessionID string, fromSeq int64) (<-chan domain.EventEnvelope, error) {
	f.mu.Lock()
	existing := append([]domain.EventEnvelope(nil), f.sessions[sessionID]...)
	f.mu.Unlock()

	ch := make(chan domain.EventEnvelope, 64)
	go func() {
		defer close(ch)
		for _, e := range existing {
			if e.Seq > fromSeq {
				select {
				case ch <- e:
				case <-ctx.Done():
					return
				}
			}
		}
		<-ctx.Done()
	}()
	return ch, nil
}

// Fork creates a new child session entry.
func (f *FakeEventLog) Fork(_ context.Context, parentID string, atSeq int64, newSessionID string) (domain.Session, error) {
	f.mu.Lock()
	f.sessions[newSessionID] = nil
	f.seqs[newSessionID] = atSeq
	f.mu.Unlock()
	return domain.Session{
		ID:            newSessionID,
		ParentID:      parentID,
		ForkedFromSeq: atSeq,
		HeadSeq:       atSeq,
		Status:        domain.StatusActive,
	}, nil
}

// AppendCalls returns a copy of all recorded Append calls.
func (f *FakeEventLog) AppendCalls() []AppendCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]AppendCall(nil), f.appendCalls...)
}

// ---------------------------------------------------------------------------
// FakeModelGateway
// ---------------------------------------------------------------------------

// ModelGatewayCall records one call to FakeModelGateway for inspection.
type ModelGatewayCall struct {
	Method string // "Generate", "Stream", "CountTokens", "Capabilities"
	Req    llm.Request
	Model  string // used for Capabilities
}

// FakeModelGateway is a scriptable [app.ModelGatewayPort]. Scripted results are
// consumed in queue order. Enqueue entries with AddGenerate, AddStream, etc.
type FakeModelGateway struct {
	mu sync.Mutex
	// calls records every method invocation.
	calls []ModelGatewayCall
	// queued scripted entries
	generates    []scriptedGWGenerate
	streams      []scriptedGWStream
	tokenCounts  []scriptedGWToken
	capabilities []scriptedGWCaps
	genIdx       atomic.Int64
	streamIdx    atomic.Int64
	tokenIdx     atomic.Int64
	capsIdx      atomic.Int64
}

type scriptedGWGenerate struct {
	resp *llm.Response
	err  error
}
type scriptedGWStream struct {
	events []llm.StreamEvent
	err    error
}
type scriptedGWToken struct {
	n   int
	err error
}
type scriptedGWCaps struct {
	caps llm.Capabilities
	err  error
}

// NewFakeModelGateway returns an empty FakeModelGateway.
func NewFakeModelGateway() *FakeModelGateway { return &FakeModelGateway{} }

// AddGenerate enqueues a scripted Generate result.
func (f *FakeModelGateway) AddGenerate(resp *llm.Response, err error) {
	f.generates = append(f.generates, scriptedGWGenerate{resp: resp, err: err})
}

// AddGenerateText enqueues a text-only Generate result with StopEnd.
func (f *FakeModelGateway) AddGenerateText(text string) {
	f.AddGenerate(&llm.Response{
		Content:    []llm.ContentPart{{Text: &llm.TextPart{Text: text}}},
		StopReason: llm.StopEnd,
	}, nil)
}

// AddStream enqueues a scripted Stream result.
func (f *FakeModelGateway) AddStream(events []llm.StreamEvent, err error) {
	f.streams = append(f.streams, scriptedGWStream{events: events, err: err})
}

// AddStreamEvents is a convenience: appends a Done(StopEnd) if the last event
// is not already Done, then enqueues the stream.
func (f *FakeModelGateway) AddStreamEvents(events ...llm.StreamEvent) {
	if len(events) == 0 || events[len(events)-1].Done == nil {
		events = append(events, llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopEnd}})
	}
	f.AddStream(events, nil)
}

// AddTokenCount enqueues a scripted CountTokens result.
func (f *FakeModelGateway) AddTokenCount(n int, err error) {
	f.tokenCounts = append(f.tokenCounts, scriptedGWToken{n: n, err: err})
}

// AddCapabilities enqueues a scripted Capabilities result.
func (f *FakeModelGateway) AddCapabilities(caps llm.Capabilities, err error) {
	f.capabilities = append(f.capabilities, scriptedGWCaps{caps: caps, err: err})
}

func (f *FakeModelGateway) record(m string, req llm.Request, model string) {
	f.mu.Lock()
	f.calls = append(f.calls, ModelGatewayCall{Method: m, Req: req, Model: model})
	f.mu.Unlock()
}

// Generate returns the next scripted response.
func (f *FakeModelGateway) Generate(_ context.Context, req llm.Request) (*llm.Response, error) {
	f.record("Generate", req, "")
	idx := int(f.genIdx.Add(1) - 1)
	if idx >= len(f.generates) {
		panic(fmt.Sprintf("apptest.FakeModelGateway: Generate queue exhausted (call %d)", idx+1))
	}
	s := f.generates[idx]
	return s.resp, s.err
}

// Stream returns the next scripted StreamReader.
func (f *FakeModelGateway) Stream(_ context.Context, req llm.Request) (llm.StreamReader, error) {
	f.record("Stream", req, "")
	idx := int(f.streamIdx.Add(1) - 1)
	if idx >= len(f.streams) {
		panic(fmt.Sprintf("apptest.FakeModelGateway: Stream queue exhausted (call %d)", idx+1))
	}
	s := f.streams[idx]
	if s.err != nil {
		return nil, s.err
	}
	return newSimpleStreamReader(s.events), nil
}

// CountTokens returns the next scripted token count.
func (f *FakeModelGateway) CountTokens(_ context.Context, req llm.Request) (int, error) {
	f.record("CountTokens", req, "")
	idx := int(f.tokenIdx.Add(1) - 1)
	if idx >= len(f.tokenCounts) {
		return 0, &llm.ProviderError{Kind: llm.ErrUnsupported}
	}
	s := f.tokenCounts[idx]
	return s.n, s.err
}

// Capabilities returns the next scripted capabilities.
func (f *FakeModelGateway) Capabilities(_ context.Context, model string) (llm.Capabilities, error) {
	f.record("Capabilities", llm.Request{}, model)
	idx := int(f.capsIdx.Add(1) - 1)
	if idx >= len(f.capabilities) {
		return llm.Capabilities{}, nil
	}
	s := f.capabilities[idx]
	return s.caps, s.err
}

// Calls returns a snapshot of all recorded method calls.
func (f *FakeModelGateway) Calls() []ModelGatewayCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ModelGatewayCall(nil), f.calls...)
}

// simpleStreamReader is a minimal StreamReader used internally by FakeModelGateway.
type simpleStreamReader struct {
	events []llm.StreamEvent
	pos    int
}

func newSimpleStreamReader(events []llm.StreamEvent) llm.StreamReader {
	return &simpleStreamReader{events: events}
}

func (r *simpleStreamReader) Recv() (llm.StreamEvent, error) {
	if r.pos >= len(r.events) {
		return llm.StreamEvent{}, io.EOF
	}
	ev := r.events[r.pos]
	r.pos++
	return ev, nil
}
func (r *simpleStreamReader) Close() error { return nil }

// ---------------------------------------------------------------------------
// FakeToolRuntime
// ---------------------------------------------------------------------------

// ExecuteToolCall records one call to FakeToolRuntime.ExecuteTool.
type ExecuteToolCall struct {
	Exec app.ToolExecution
}

// FakeToolRuntime is a scriptable [app.ToolRuntimePort]. Each AddExecution
// call queues one ToolStream to return.
type FakeToolRuntime struct {
	mu          sync.Mutex
	execStreams []app.ToolStream
	execErrs    []error
	execIdx     atomic.Int64
	calls       []ExecuteToolCall
	tools       []app.ToolDescriptor
}

// NewFakeToolRuntime returns an empty FakeToolRuntime.
func NewFakeToolRuntime() *FakeToolRuntime { return &FakeToolRuntime{} }

// AddExecution enqueues a scripted ExecuteTool result.
func (f *FakeToolRuntime) AddExecution(stream app.ToolStream, err error) {
	f.mu.Lock()
	f.execStreams = append(f.execStreams, stream)
	f.execErrs = append(f.execErrs, err)
	f.mu.Unlock()
}

// AddSuccessfulExecution enqueues an immediate-result stream returning the given content.
func (f *FakeToolRuntime) AddSuccessfulExecution(content string) {
	f.AddExecution(NewFakeToolStream(app.ToolResult{Content: content}), nil)
}

// SetTools sets the list returned by ListTools.
func (f *FakeToolRuntime) SetTools(tools []app.ToolDescriptor) {
	f.mu.Lock()
	f.tools = tools
	f.mu.Unlock()
}

// ExecuteTool returns the next scripted ToolStream.
func (f *FakeToolRuntime) ExecuteTool(_ context.Context, exec app.ToolExecution) (app.ToolStream, error) {
	f.mu.Lock()
	f.calls = append(f.calls, ExecuteToolCall{Exec: exec})
	f.mu.Unlock()
	idx := int(f.execIdx.Add(1) - 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if idx >= len(f.execStreams) {
		panic(fmt.Sprintf("apptest.FakeToolRuntime: ExecuteTool queue exhausted (call %d)", idx+1))
	}
	return f.execStreams[idx], f.execErrs[idx]
}

// ListTools returns the configured tools.
func (f *FakeToolRuntime) ListTools(_ context.Context, _ string) ([]app.ToolDescriptor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]app.ToolDescriptor(nil), f.tools...), nil
}

// Calls returns a snapshot of all ExecuteTool calls.
func (f *FakeToolRuntime) Calls() []ExecuteToolCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ExecuteToolCall(nil), f.calls...)
}

// ---------------------------------------------------------------------------
// FakeToolStream
// ---------------------------------------------------------------------------

// FakeToolStream is a scriptable [app.ToolStream] that delivers an optional
// list of [app.ToolProgress] events followed by a single terminal
// [app.ToolResult].
type FakeToolStream struct {
	progress []app.ToolProgress
	result   app.ToolResult
	pos      int // 0..len(progress) = progress events; len(progress) = result; len(progress)+1 = EOF
}

// Compile-time assertion.
var _ app.ToolStream = (*FakeToolStream)(nil)

// NewFakeToolStream returns a FakeToolStream with the given progress events and
// terminal result. Pass no progress events for a simple result-only stream.
func NewFakeToolStream(result app.ToolResult, progress ...app.ToolProgress) *FakeToolStream {
	return &FakeToolStream{progress: progress, result: result}
}

// Recv returns the next ToolEvent. Progress events are returned first, then the
// terminal result, then io.EOF.
func (s *FakeToolStream) Recv() (app.ToolEvent, error) {
	if s.pos < len(s.progress) {
		p := s.progress[s.pos]
		s.pos++
		return app.ToolEvent{Progress: &p}, nil
	}
	if s.pos == len(s.progress) {
		s.pos++
		r := s.result
		return app.ToolEvent{Result: &r}, nil
	}
	return app.ToolEvent{}, io.EOF
}

// Close is a no-op.
func (s *FakeToolStream) Close() error { return nil }

// ---------------------------------------------------------------------------
// FakeApprovalGate
// ---------------------------------------------------------------------------

// FakeApprovalGate is a scriptable [app.ApprovalGate]. Pending requests block
// until Resolve is called or the context is cancelled. If you call Request and
// nothing resolves it, the call blocks until ctx is cancelled.
type FakeApprovalGate struct {
	mu      sync.Mutex
	pending map[string]chan domain.AskResolution // key: sessionID+":"+callID
}

// NewFakeApprovalGate returns an empty FakeApprovalGate.
func NewFakeApprovalGate() *FakeApprovalGate {
	return &FakeApprovalGate{pending: make(map[string]chan domain.AskResolution)}
}

func pendingKey(sessionID, callID string) string { return sessionID + ":" + callID }

// Request blocks until Resolve delivers a resolution or ctx is cancelled.
func (f *FakeApprovalGate) Request(ctx context.Context, req app.ApprovalRequest) (domain.AskResolution, error) {
	ch := make(chan domain.AskResolution, 1)
	key := pendingKey(req.SessionID, req.CallID)
	f.mu.Lock()
	f.pending[key] = ch
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.pending, key)
		f.mu.Unlock()
	}()
	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return domain.AskUnresolved, ctx.Err()
	}
}

// Resolve delivers the resolution for the pending (sessionID, callID) request.
// It spins briefly to let a concurrent Request goroutine register its channel
// before giving up, which avoids a test-only race when Request and Resolve are
// called back-to-back from different goroutines.
func (f *FakeApprovalGate) Resolve(_ context.Context, sessionID, callID string, resolution domain.AskResolution) error {
	key := pendingKey(sessionID, callID)
	const maxYields = 1000
	for i := 0; i < maxYields; i++ {
		f.mu.Lock()
		ch, ok := f.pending[key]
		f.mu.Unlock()
		if ok {
			ch <- resolution
			return nil
		}
		runtime.Gosched()
	}
	return fmt.Errorf("apptest.FakeApprovalGate: no pending approval for %q after spinning", key)
}

// ---------------------------------------------------------------------------
// FakeHookRunner
// ---------------------------------------------------------------------------

// HookRunCall records one call to FakeHookRunner.Run.
type HookRunCall struct {
	In app.HookInput
}

// FakeHookRunner is a scriptable [app.HookRunner]. By default it allows all
// hook events (Allow=true). Configure BlockNext to make the next Run call
// return a blocking decision.
type FakeHookRunner struct {
	mu    sync.Mutex
	calls []HookRunCall
	// BlockNext, if non-empty, is consumed in order: each Run call returns the
	// matching decision (false = block) and reason.
	BlockNext []struct {
		Allow  bool
		Reason string
	}
}

// NewFakeHookRunner returns a FakeHookRunner that allows all hooks by default.
func NewFakeHookRunner() *FakeHookRunner { return &FakeHookRunner{} }

// AddDecision enqueues one decision for the next Run call.
func (f *FakeHookRunner) AddDecision(allow bool, reason string) {
	f.mu.Lock()
	f.BlockNext = append(f.BlockNext, struct {
		Allow  bool
		Reason string
	}{Allow: allow, Reason: reason})
	f.mu.Unlock()
}

// Run records the call and returns the next scripted decision, or Allow=true if none queued.
func (f *FakeHookRunner) Run(_ context.Context, in app.HookInput) (app.HookDecision, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, HookRunCall{In: in})
	if len(f.BlockNext) > 0 {
		d := f.BlockNext[0]
		f.BlockNext = f.BlockNext[1:]
		return app.HookDecision{Allow: d.Allow, Reason: d.Reason}, nil
	}
	return app.HookDecision{Allow: true}, nil
}

// Calls returns a snapshot of all Run calls.
func (f *FakeHookRunner) Calls() []HookRunCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]HookRunCall(nil), f.calls...)
}

// ---------------------------------------------------------------------------
// FakeSubAgent
// ---------------------------------------------------------------------------

// SpawnCall records one call to FakeSubAgent.Spawn.
type SpawnCall struct {
	In app.SubAgentSpawn
}

// FakeSubAgent is a scriptable [app.SubAgentPort]. Spawn returns scripted
// results in queue order.
type FakeSubAgent struct {
	mu       sync.Mutex
	results  []app.ToolResult
	errs     []error
	calls    []SpawnCall
	idx      atomic.Int64
	maxDepth int
}

// NewFakeSubAgent returns a FakeSubAgent with the given maximum depth.
func NewFakeSubAgent(maxDepth int) *FakeSubAgent {
	return &FakeSubAgent{maxDepth: maxDepth}
}

// AddResult enqueues one scripted Spawn result.
func (f *FakeSubAgent) AddResult(r app.ToolResult, err error) {
	f.mu.Lock()
	f.results = append(f.results, r)
	f.errs = append(f.errs, err)
	f.mu.Unlock()
}

// Spawn returns the next scripted result, enforcing the depth cap.
func (f *FakeSubAgent) Spawn(_ context.Context, in app.SubAgentSpawn) (app.ToolResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, SpawnCall{In: in})
	f.mu.Unlock()
	if in.Depth > f.maxDepth {
		return app.ToolResult{IsError: true, Content: "max sub-agent depth exceeded"}, nil
	}
	idx := int(f.idx.Add(1) - 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if idx >= len(f.results) {
		panic(fmt.Sprintf("apptest.FakeSubAgent: Spawn queue exhausted (call %d)", idx+1))
	}
	return f.results[idx], f.errs[idx]
}

// MaxDepth returns the configured maximum sub-agent recursion depth.
func (f *FakeSubAgent) MaxDepth() int { return f.maxDepth }

// Calls returns a snapshot of all Spawn calls.
func (f *FakeSubAgent) Calls() []SpawnCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]SpawnCall(nil), f.calls...)
}
