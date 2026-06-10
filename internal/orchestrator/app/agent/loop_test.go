package agent_test

// Package agent_test exercises the agent loop (T-LOOP-05 + T-LOOP-07) entirely
// against the in-repo fakes — scripted ModelGateway/StreamReader, in-memory
// EventLog, scriptable ToolRuntime/ApprovalGate/HookRunner/PolicyEngine, a fake
// clock and a fake IDGenerator. No real provider, sandbox, or database is
// involved, so every test is deterministic and network-free (NFR-TEST-01/02).
//
// The battery covers the FRs named in the task:
//
//   - FR-LOOP-01 — golden event-log shape for a 2-turn task.
//   - FR-LOOP-02 — termination subtypes: success, error_max_turns,
//     error_max_budget_usd, error_during_execution, refusal,
//     error_max_structured_output_retries.
//   - FR-LOOP-04 — read-only tools dispatch concurrently while mutating tools
//     serialize (asserted via the recording fake runtime).
//   - FR-PERM-01 — a deny short-circuits to a PermissionDecided{deny} WITHOUT an
//     ApprovalRequested.
//   - FR-EXT-03 — a PreToolUse hook block yields PermissionDecided{deny,
//     reason:"hook_blocked"}.
//   - FR-LOOP-05 / T-LOOP-07 — resume aborts an open turn and skips an unknown
//     mutating execution.

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/orchestrator/app"
	"github.com/boltrope/boltrope/internal/orchestrator/app/agent"
	"github.com/boltrope/boltrope/internal/orchestrator/app/apptest"
	"github.com/boltrope/boltrope/internal/orchestrator/domain"
	"github.com/boltrope/boltrope/internal/orchestrator/policy"
	"github.com/boltrope/boltrope/internal/orchestrator/policy/policytest"
	"github.com/boltrope/boltrope/internal/platform/clock/clocktest"
	"github.com/boltrope/boltrope/internal/platform/ids/idstest"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// ---------------------------------------------------------------------------
// Test harness: assemble a Loop over the fakes with sensible defaults.
// ---------------------------------------------------------------------------

// harness bundles every fake so a test can configure scripts and then inspect
// recorded interactions after Run returns.
type harness struct {
	eventlog *apptest.FakeEventLog
	model    *apptest.FakeModelGateway
	tools    *apptest.FakeToolRuntime
	gate     *apptest.FakeApprovalGate
	hooks    *apptest.FakeHookRunner
	pol      *policytest.FakePolicyEngine
	clk      *clocktest.Fake
	ids      *idstest.Fake
	sink     *recordingSink
	metrics  *recordingMetrics
}

// newHarness builds a harness with empty scripts. The caller scripts the model
// gateway and (optionally) the tool runtime / policy / hooks before calling
// run.
func newHarness(t *testing.T, idSeq ...string) *harness {
	t.Helper()
	if len(idSeq) == 0 {
		// A generous deterministic id sequence for turn ids and request ids.
		idSeq = make([]string, 0, 64)
		for i := 1; i <= 64; i++ {
			idSeq = append(idSeq, idStr(i))
		}
	}
	return &harness{
		eventlog: apptest.NewFakeEventLog(),
		model:    apptest.NewFakeModelGateway(),
		tools:    apptest.NewFakeToolRuntime(),
		gate:     apptest.NewFakeApprovalGate(),
		hooks:    apptest.NewFakeHookRunner(),
		pol:      policytest.NewFakePolicyEngine(),
		clk:      clocktest.NewFake(time.Unix(0, 0)),
		ids:      idstest.NewFake(idSeq...),
		sink:     &recordingSink{},
		metrics:  &recordingMetrics{},
	}
}

func idStr(i int) string {
	const digits = "0123456789"
	// Render a short deterministic id; the exact value is asserted in the
	// golden test, so keep it simple and stable.
	if i < 10 {
		return "id-" + string(digits[i])
	}
	return "id-" + string(digits[i/10]) + string(digits[i%10])
}

// loop constructs the Loop under test from this harness with the given config.
func (h *harness) loop(cfg agent.Config) *agent.Loop {
	return agent.NewLoop(agent.Deps{
		EventLog:  h.eventlog,
		Model:     h.model,
		Tools:     h.tools,
		Approvals: h.gate,
		Hooks:     h.hooks,
		Policy:    h.pol,
		Clock:     h.clk,
		IDs:       h.ids,
		Sink:      h.sink,
		Metrics:   h.metrics,
		CostFunc:  nil, // default: zero cost unless a test sets one
	}, cfg)
}

// defaultConfig returns a config with permissive caps so a test that does not
// care about caps does not accidentally trip one.
func defaultConfig() agent.Config {
	return agent.Config{
		Model:                      "test-model",
		MaxTurns:                   16,
		MaxBudgetUSD:               1000,
		MaxStructuredOutputRetries: 3,
		DoomLoopThreshold:          5,
	}
}

// run is a convenience that runs the loop with a single user message.
func (h *harness) run(t *testing.T, cfg agent.Config, sessionID, userText string) (agent.RunResult, error) {
	t.Helper()
	lp := h.loop(cfg)
	return lp.Run(context.Background(), agent.RunInput{
		SessionID:   sessionID,
		UserMessage: userMsg(userText),
	})
}

func userMsg(text string) llm.Message {
	return llm.Message{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: text}}}}
}

// eventTypes extracts the ordered EventType sequence appended to sessionID,
// flattening multi-event appends in order.
func (h *harness) eventTypes(sessionID string) []domain.EventType {
	evs, _ := h.eventlog.Load(context.Background(), sessionID, 0)
	out := make([]domain.EventType, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.Type)
	}
	return out
}

// payloadsOf returns the typed payloads of a given event type appended to
// sessionID, in order.
func payloadsOf[T domain.Event](h *harness, sessionID string) []T {
	evs, _ := h.eventlog.Load(context.Background(), sessionID, 0)
	var out []T
	for _, e := range evs {
		if p, ok := e.Event.(T); ok {
			out = append(out, p)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// recordingSink — captures forwarded text/thinking deltas.
// ---------------------------------------------------------------------------

type recordingSink struct {
	mu       sync.Mutex
	text     []string
	thinking []string
}

func (s *recordingSink) OnTextDelta(_ string, _ string, text string) {
	s.mu.Lock()
	s.text = append(s.text, text)
	s.mu.Unlock()
}

func (s *recordingSink) OnThinkingDelta(_ string, _ string, text string) {
	s.mu.Lock()
	s.thinking = append(s.thinking, text)
	s.mu.Unlock()
}

func (s *recordingSink) textJoined() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out string
	for _, t := range s.text {
		out += t
	}
	return out
}

// ---------------------------------------------------------------------------
// recordingMetrics — captures RED/doom-loop signals.
// ---------------------------------------------------------------------------

type recordingMetrics struct {
	mu        sync.Mutex
	errors    []string
	doomLoops []string
}

func (m *recordingMetrics) RecordRunError(subtype string) {
	m.mu.Lock()
	m.errors = append(m.errors, subtype)
	m.mu.Unlock()
}

func (m *recordingMetrics) RecordDoomLoop(tool string) {
	m.mu.Lock()
	m.doomLoops = append(m.doomLoops, tool)
	m.mu.Unlock()
}

func (m *recordingMetrics) errorCount(subtype string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, e := range m.errors {
		if e == subtype {
			n++
		}
	}
	return n
}

func (m *recordingMetrics) doomLoopCount(tool string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, d := range m.doomLoops {
		if d == tool {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Stream-event builders shared by the tests.
// ---------------------------------------------------------------------------

// textStream is a stream that emits one text delta then a terminal Done(end).
func textStream(text string) []llm.StreamEvent {
	return []llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: text}},
		{Done: &llm.Done{StopReason: llm.StopEnd}},
	}
}

// toolCallStream emits a single complete (buffered) tool call then Done(tool_use).
func toolCallStream(callID, name string, args map[string]any) []llm.StreamEvent {
	raw, _ := json.Marshal(args)
	return []llm.StreamEvent{
		{ToolCallDelta: &llm.ToolCallDelta{CallID: callID, Name: name, ArgsFragment: raw}},
		{Done: &llm.Done{StopReason: llm.StopToolUse}},
	}
}

// multiToolCallStream emits several complete tool calls in one assistant turn.
func multiToolCallStream(calls ...llm.ToolCall) []llm.StreamEvent {
	var evs []llm.StreamEvent
	for _, c := range calls {
		raw, _ := json.Marshal(c.Args)
		evs = append(evs, llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: c.ID, Name: c.Name, ArgsFragment: raw}})
	}
	evs = append(evs, llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopToolUse}})
	return evs
}

// ===========================================================================
// FR-LOOP-01 AC-1 — golden event-log shape for a 2-turn task.
// ===========================================================================

// TestRun_GoldenTwoTurnSuccess drives the canonical 2-turn task: the model
// requests a tool, the loop executes it, feeds the result back, and the model
// returns text-only. The exact event-log shape is asserted, and the run
// terminates with subtype=success (FR-LOOP-01 AC-1).
func TestRun_GoldenTwoTurnSuccess(t *testing.T) {
	h := newHarness(t)
	// Turn 1: assistant requests a read-only tool.
	h.model.AddStream(toolCallStream("call-1", "read", map[string]any{"path": "/x"}), nil)
	// Turn 2: assistant returns text-only.
	h.model.AddStream(textStream("done"), nil)

	// The read tool is declared read-only; its execution returns content.
	h.tools.SetTools([]app.ToolDescriptor{
		{Name: "read", SideEffect: domain.SideEffectReadOnly, EgressClass: domain.EgressClassNone},
	})
	h.tools.AddSuccessfulExecution("file contents")
	// Policy allows the call.
	h.pol.AddAllow("allow-read", "read-only tool")

	res, err := h.run(t, defaultConfig(), "sess-golden", "read /x please")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)
	assert.Equal(t, 2, res.NumTurns)

	// The golden event-log shape (FR-LOOP-01 AC-1):
	//   MessageAppended (user)
	//   TurnStarted
	//   AssistantMessage (tool_call)
	//   PermissionDecided (allow)
	//   ToolExecutionStarted
	//   ToolResult
	//   TurnFinished (success? no — turn 1 is not terminal)  -- see note
	//   TurnStarted
	//   AssistantMessage (text)
	//   TurnFinished (success)
	//
	// Turn 1 ends with a tool round-trip, not a TurnFinished: TurnFinished is
	// the run-terminal event. So the expected sequence is:
	want := []domain.EventType{
		domain.EventMessageAppended,
		domain.EventTurnStarted,
		domain.EventAssistantMessage,
		domain.EventPermissionDecided,
		domain.EventToolExecutionStarted,
		domain.EventToolResult,
		domain.EventMessageAppended, // tool results fed back as a user/tool turn
		domain.EventTurnStarted,
		domain.EventAssistantMessage,
		domain.EventTurnFinished,
	}
	assert.Equal(t, want, h.eventTypes("sess-golden"))

	// The terminal TurnFinished carries success + the cumulative turn count.
	fins := payloadsOf[domain.TurnFinished](h, "sess-golden")
	require.Len(t, fins, 1)
	assert.Equal(t, domain.Success, fins[0].Reason)
	assert.Equal(t, 2, fins[0].NumTurns)

	// The ToolExecutionStarted carries a non-empty, log-derived idempotency key.
	starts := payloadsOf[domain.ToolExecutionStarted](h, "sess-golden")
	require.Len(t, starts, 1)
	assert.Equal(t, "call-1", starts[0].CallID)
	assert.NotEmpty(t, starts[0].IdempotencyKey, "idempotency key must be log-derived, not empty")

	// The permission decision is an allow with no human ask.
	decs := payloadsOf[domain.PermissionDecided](h, "sess-golden")
	require.Len(t, decs, 1)
	assert.Equal(t, domain.PermissionAllow, decs[0].Decision)

	// The first assistant message carries the tool call; the second is text.
	asst := payloadsOf[domain.AssistantMessage](h, "sess-golden")
	require.Len(t, asst, 2)
	assert.Equal(t, llm.StopToolUse, asst[0].StopReason)
	assert.Equal(t, llm.StopEnd, asst[1].StopReason)

	// Forwarded text reached the client sink.
	assert.Equal(t, "done", h.sink.textJoined())
}

// TestRun_TextOnlyFirstTurnSucceeds covers the simplest path: a single
// text-only turn terminates success with no tool round-trip.
func TestRun_TextOnlyFirstTurnSucceeds(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(textStream("hello world"), nil)

	res, err := h.run(t, defaultConfig(), "sess-1", "hi")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)
	assert.Equal(t, 1, res.NumTurns)

	want := []domain.EventType{
		domain.EventMessageAppended,
		domain.EventTurnStarted,
		domain.EventAssistantMessage,
		domain.EventTurnFinished,
	}
	assert.Equal(t, want, h.eventTypes("sess-1"))
}

// ===========================================================================
// FR-LOOP-02 — termination subtypes.
// ===========================================================================

// TestRun_MaxTurns asserts that a model that never emits a text-only response
// terminates with error_max_turns once the cap is hit, the terminal
// TurnFinished carries the subtype, and the RED error counter increments
// (FR-LOOP-01 AC-2 / FR-LOOP-02 AC-3 / FR-OBS-02 AC-2).
func TestRun_MaxTurns(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.MaxTurns = 3
	cfg.DoomLoopThreshold = 0 // disable doom-loop so we isolate the max-turns cap

	h.tools.SetTools([]app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}})
	h.pol.AddAllow("a", "")
	h.pol.AddAllow("a", "")
	h.pol.AddAllow("a", "")
	// The model requests a tool every turn, never returning text-only. Vary the
	// args so the doom-loop detector (if any) does not fire first.
	for i := 0; i < 3; i++ {
		h.model.AddStream(toolCallStream("c", "read", map[string]any{"path": idStr(i)}), nil)
		h.tools.AddSuccessfulExecution("ok")
	}

	res, err := h.run(t, cfg, "sess-mt", "loop forever")
	require.NoError(t, err)
	assert.Equal(t, domain.ErrorMaxTurns, res.Reason)
	assert.Equal(t, 3, res.NumTurns)

	fins := payloadsOf[domain.TurnFinished](h, "sess-mt")
	require.NotEmpty(t, fins)
	assert.Equal(t, domain.ErrorMaxTurns, fins[len(fins)-1].Reason)
	assert.Equal(t, 1, h.metrics.errorCount("error_max_turns"))
}

// TestRun_MaxBudget asserts that when cumulative cost exceeds max_budget_usd the
// loop terminates with error_max_budget_usd BEFORE the next Generate call
// (FR-LOOP-02 AC-1).
func TestRun_MaxBudget(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.MaxTurns = 16
	cfg.MaxBudgetUSD = 1.0

	// Each turn costs $0.60; after turn 1 the cumulative cost is $0.60 (under
	// budget) and the loop would proceed to a tool round-trip; but to keep the
	// test about the budget cap, the model requests a tool each turn.
	lp := agent.NewLoop(agent.Deps{
		EventLog: h.eventlog, Model: h.model, Tools: h.tools, Approvals: h.gate,
		Hooks: h.hooks, Policy: h.pol, Clock: h.clk, IDs: h.ids, Sink: h.sink, Metrics: h.metrics,
		CostFunc: func(_ string, _ llm.Usage) (float64, error) { return 0.60, nil },
	}, cfg)

	h.tools.SetTools([]app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}})
	for i := 0; i < 5; i++ {
		h.model.AddStream(toolCallStream("c", "read", map[string]any{"i": i}), nil)
		h.tools.AddSuccessfulExecution("ok")
		h.pol.AddAllow("a", "")
	}

	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-mb", UserMessage: userMsg("go")})
	require.NoError(t, err)
	assert.Equal(t, domain.ErrorMaxBudgetUSD, res.Reason)
	// Budget is exceeded after the 2nd turn's cost ($1.20 > $1.00), so the loop
	// stops before the 3rd Generate. The model gateway must NOT have been called
	// a third time.
	streamCalls := 0
	for _, c := range h.model.Calls() {
		if c.Method == "Stream" {
			streamCalls++
		}
	}
	assert.LessOrEqual(t, streamCalls, 2, "must stop before the next Generate once budget is exceeded")
	assert.Equal(t, 1, h.metrics.errorCount("error_max_budget_usd"))

	fins := payloadsOf[domain.TurnFinished](h, "sess-mb")
	require.NotEmpty(t, fins)
	assert.Equal(t, domain.ErrorMaxBudgetUSD, fins[len(fins)-1].Reason)
}

// TestRun_RefusalIsDistinctSubtype asserts a StopRefusal maps to the Refusal
// termination subtype, NOT error_during_execution (architecture §11.3).
func TestRun_RefusalIsDistinctSubtype(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream([]llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: "I cannot help with that."}},
		{Done: &llm.Done{StopReason: llm.StopRefusal}},
	}, nil)

	res, err := h.run(t, defaultConfig(), "sess-refuse", "do something bad")
	require.NoError(t, err)
	assert.Equal(t, domain.Refusal, res.Reason)
	assert.NotEqual(t, domain.ErrorDuringExecution, res.Reason)

	fins := payloadsOf[domain.TurnFinished](h, "sess-refuse")
	require.Len(t, fins, 1)
	assert.Equal(t, domain.Refusal, fins[0].Reason)
}

// TestRun_StreamErrorIsErrorDuringExecution asserts that a provider stream error
// (after the loop has no retry budget) terminates the run with
// error_during_execution and appends a TurnAborted carrying usage_so_far
// (FR-LOOP-02 AC-2).
func TestRun_StreamErrorIsErrorDuringExecution(t *testing.T) {
	h := newHarness(t)
	// Stream fails to start with a retryable server error; with no retry budget
	// the loop surfaces error_during_execution.
	h.model.AddStream(nil, &llm.ProviderError{Kind: llm.ErrServer})

	res, err := h.run(t, defaultConfig(), "sess-err", "go")
	require.NoError(t, err)
	assert.Equal(t, domain.ErrorDuringExecution, res.Reason)
	assert.Equal(t, 1, h.metrics.errorCount("error_during_execution"))

	// The open turn is closed by a TurnAborted (not a TurnFinished) so the
	// partial turn is accounted, not silently dropped.
	aborts := payloadsOf[domain.TurnAborted](h, "sess-err")
	require.Len(t, aborts, 1)
	assert.Equal(t, domain.ErrorDuringExecution, aborts[0].Reason)
}

// ===========================================================================
// FR-LOOP-02 / structured output — retry exhaustion.
// ===========================================================================

// TestRun_StructuredOutputRetryExhaustion asserts that when an OutputSchema is
// configured and the assistant message never validates, the loop retries up to
// the cap and then terminates with error_max_structured_output_retries.
func TestRun_StructuredOutputRetryExhaustion(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.MaxStructuredOutputRetries = 2
	cfg.MaxTurns = 16
	// Require an object with a required "answer" string field; the model never
	// produces it.
	cfg.OutputSchema = json.RawMessage(`{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}`)

	// The model returns text that is not schema-valid JSON, every attempt.
	// initial + 2 retries = 3 attempts.
	for i := 0; i < 3; i++ {
		h.model.AddStream(textStream("not json"), nil)
	}

	res, err := h.run(t, cfg, "sess-so", "give me structured output")
	require.NoError(t, err)
	assert.Equal(t, domain.ErrorMaxStructuredOutputRetries, res.Reason)
	assert.Equal(t, 1, h.metrics.errorCount("error_max_structured_output_retries"))

	// Exactly initial+retries Generate/Stream calls were made.
	streamCalls := 0
	for _, c := range h.model.Calls() {
		if c.Method == "Stream" {
			streamCalls++
		}
	}
	assert.Equal(t, 3, streamCalls, "initial attempt + MaxStructuredOutputRetries")
}

// TestRun_StructuredOutputValidPasses asserts that a schema-valid assistant
// message terminates success on the first attempt.
func TestRun_StructuredOutputValidPasses(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.OutputSchema = json.RawMessage(`{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}`)
	h.model.AddStream(textStream(`{"answer":"42"}`), nil)

	res, err := h.run(t, cfg, "sess-so2", "structured")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)
}

// ===========================================================================
// FR-LOOP-04 — parallel read-only, serialized mutating scheduling.
// ===========================================================================

// TestRun_ReadOnlyToolsDispatchConcurrently asserts that three read-only tool
// calls in one assistant turn dispatch concurrently (overlapping in time) and
// their results are fed back in a single follow-up request (FR-LOOP-04 AC-1).
func TestRun_ReadOnlyToolsDispatchConcurrently(t *testing.T) {
	h := newHarness(t)
	rec := newConcurrencyRuntime()
	// Replace the tool runtime with the concurrency-recording one.
	lp := agent.NewLoop(agent.Deps{
		EventLog: h.eventlog, Model: h.model, Tools: rec, Approvals: h.gate,
		Hooks: h.hooks, Policy: h.pol, Clock: h.clk, IDs: h.ids, Sink: h.sink, Metrics: h.metrics,
	}, defaultConfig())

	rec.tools = []app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}}
	rec.barrier = 3 // each read blocks until all 3 are in-flight

	h.model.AddStream(multiToolCallStream(
		llm.ToolCall{ID: "r1", Name: "read", Args: map[string]any{"p": "a"}},
		llm.ToolCall{ID: "r2", Name: "read", Args: map[string]any{"p": "b"}},
		llm.ToolCall{ID: "r3", Name: "read", Args: map[string]any{"p": "c"}},
	), nil)
	h.model.AddStream(textStream("done"), nil)
	h.pol.AddAllow("a", "")
	h.pol.AddAllow("a", "")
	h.pol.AddAllow("a", "")

	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-par", UserMessage: userMsg("read all")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	// The barrier of 3 only releases if all three executions were in-flight at
	// once — proving concurrent dispatch.
	assert.Equal(t, int32(3), rec.maxConcurrent.Load(), "all three read-only calls must run concurrently")
	assert.Equal(t, 3, rec.callCount())
}

// TestRun_MutatingToolsSerializeInEmittedOrder asserts that two mutating tool
// calls in one assistant turn are dispatched one-at-a-time, in emitted order,
// never concurrently (FR-LOOP-04 AC-2).
func TestRun_MutatingToolsSerializeInEmittedOrder(t *testing.T) {
	h := newHarness(t)
	rec := newConcurrencyRuntime()
	lp := agent.NewLoop(agent.Deps{
		EventLog: h.eventlog, Model: h.model, Tools: rec, Approvals: h.gate,
		Hooks: h.hooks, Policy: h.pol, Clock: h.clk, IDs: h.ids, Sink: h.sink, Metrics: h.metrics,
	}, defaultConfig())

	rec.tools = []app.ToolDescriptor{{Name: "write", SideEffect: domain.SideEffectMutating}}

	h.model.AddStream(multiToolCallStream(
		llm.ToolCall{ID: "w1", Name: "write", Args: map[string]any{"p": "first"}},
		llm.ToolCall{ID: "w2", Name: "write", Args: map[string]any{"p": "second"}},
	), nil)
	h.model.AddStream(textStream("done"), nil)
	h.pol.AddAllow("a", "")
	h.pol.AddAllow("a", "")

	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-ser", UserMessage: userMsg("write twice")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	// Never more than one mutating execution in flight.
	assert.Equal(t, int32(1), rec.maxConcurrent.Load(), "mutating tools must serialize")
	// Dispatched in emitted order: w1 then w2.
	assert.Equal(t, []string{"w1", "w2"}, rec.order())
}

// ===========================================================================
// FR-PERM-01 — deny short-circuits to PermissionDecided{deny} WITHOUT an
// ApprovalRequested (asserted via the events the loop persists).
// ===========================================================================

// TestRun_DenyShortCircuitsWithoutApproval asserts that a policy Deny results in
// a PermissionDecided{deny} event and that the tool is never executed and the
// approval gate is never consulted (FR-PERM-01 AC-1).
func TestRun_DenyShortCircuitsWithoutApproval(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(toolCallStream("d1", "bash", map[string]any{"cmd": "rm -rf /"}), nil)
	// After the deny, the model returns text-only to end the run.
	h.model.AddStream(textStream("understood"), nil)

	h.tools.SetTools([]app.ToolDescriptor{{Name: "bash", SideEffect: domain.SideEffectMutating}})
	h.pol.AddDeny("deny-bash", "bash is denied")
	// No execution scripted: a dispatch would panic the fake runtime, proving
	// the tool was not executed.

	res, err := h.run(t, defaultConfig(), "sess-deny", "rm everything")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	decs := payloadsOf[domain.PermissionDecided](h, "sess-deny")
	require.Len(t, decs, 1)
	assert.Equal(t, domain.PermissionDeny, decs[0].Decision)
	assert.Equal(t, "bash is denied", decs[0].Reason)
	assert.Equal(t, domain.AskUnresolved, decs[0].Resolved, "a deny must not carry a human resolution")

	// No ToolExecutionStarted / ToolResult was ever appended (no dispatch).
	assert.Empty(t, payloadsOf[domain.ToolExecutionStarted](h, "sess-deny"))
	assert.Equal(t, 0, len(h.tools.Calls()), "denied tool must never be executed")
}

// TestRun_AskGrantedDispatches asserts the ask path: policy Ask raises an
// approval the human grants, after which the tool is dispatched and a
// PermissionDecided{ask, resolved:allowed} is recorded (FR-PERM-04 AC-1).
func TestRun_AskGrantedDispatches(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(toolCallStream("ask-1", "write", map[string]any{"p": "f"}), nil)
	h.model.AddStream(textStream("done"), nil)

	h.tools.SetTools([]app.ToolDescriptor{{Name: "write", SideEffect: domain.SideEffectMutating}})
	h.tools.AddSuccessfulExecution("written")
	h.pol.AddAsk("ask-write", "mutating tool requires approval")

	// Resolve the approval as allowed once the loop raises it.
	go func() {
		_ = h.gate.Resolve(context.Background(), "sess-ask", "ask-1", domain.AskAllowed)
	}()

	res, err := h.run(t, defaultConfig(), "sess-ask", "write a file")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	decs := payloadsOf[domain.PermissionDecided](h, "sess-ask")
	require.Len(t, decs, 1)
	assert.Equal(t, domain.PermissionAsk, decs[0].Decision)
	assert.Equal(t, domain.AskAllowed, decs[0].Resolved)

	// The tool was dispatched after the grant.
	require.Len(t, h.tools.Calls(), 1)
	assert.Equal(t, "ask-1", h.tools.Calls()[0].Exec.Call.ID)
}

// TestRun_AskDeniedSkipsDispatch asserts that a human deny of an ask records
// PermissionDecided{ask, resolved:denied} and does NOT dispatch the tool.
func TestRun_AskDeniedSkipsDispatch(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(toolCallStream("ask-2", "write", map[string]any{"p": "f"}), nil)
	h.model.AddStream(textStream("ok, skipping"), nil)

	h.tools.SetTools([]app.ToolDescriptor{{Name: "write", SideEffect: domain.SideEffectMutating}})
	h.pol.AddAsk("ask-write", "approval required")

	go func() {
		_ = h.gate.Resolve(context.Background(), "sess-askd", "ask-2", domain.AskDenied)
	}()

	res, err := h.run(t, defaultConfig(), "sess-askd", "write")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	decs := payloadsOf[domain.PermissionDecided](h, "sess-askd")
	require.Len(t, decs, 1)
	assert.Equal(t, domain.PermissionAsk, decs[0].Decision)
	assert.Equal(t, domain.AskDenied, decs[0].Resolved)
	assert.Equal(t, 0, len(h.tools.Calls()), "denied ask must not dispatch the tool")
}

// ===========================================================================
// FR-EXT-03 — hook block -> PermissionDecided{deny, Reason:"hook_blocked"}.
// ===========================================================================

// TestRun_HookBlockDeniesWithReason asserts that a PreToolUse hook returning
// block prevents dispatch and the loop appends PermissionDecided{deny,
// reason:"hook_blocked"} WITHOUT an approval request (FR-EXT-03 AC-1).
func TestRun_HookBlockDeniesWithReason(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(toolCallStream("h1", "bash", map[string]any{"cmd": "x"}), nil)
	h.model.AddStream(textStream("ok"), nil)

	h.tools.SetTools([]app.ToolDescriptor{{Name: "bash", SideEffect: domain.SideEffectMutating}})
	// PreToolUse hook blocks the call.
	h.hooks.AddDecision(false, "blocked by policy hook")
	// Policy would allow, but the hook runs first and blocks.
	h.pol.AddAllow("a", "")

	res, err := h.run(t, defaultConfig(), "sess-hook", "run bash")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	decs := payloadsOf[domain.PermissionDecided](h, "sess-hook")
	require.Len(t, decs, 1)
	assert.Equal(t, domain.PermissionDeny, decs[0].Decision)
	assert.Equal(t, "hook_blocked", decs[0].Reason, "hook block reason must be hook_blocked (FR-EXT-03 AC-1)")

	// The tool was never executed.
	assert.Empty(t, payloadsOf[domain.ToolExecutionStarted](h, "sess-hook"))
	assert.Equal(t, 0, len(h.tools.Calls()))
}

// ===========================================================================
// FR-OBS-04 — doom-loop detection.
// ===========================================================================

// TestRun_DoomLoopDetected asserts that the same tool call repeated up to the
// configured threshold increments the doom-loop counter labeled by tool name
// (FR-OBS-04 AC-1).
func TestRun_DoomLoopDetected(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.MaxTurns = 16
	cfg.DoomLoopThreshold = 5

	h.tools.SetTools([]app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}})
	// The model emits the IDENTICAL read call 5 times in a row.
	for i := 0; i < 6; i++ {
		h.model.AddStream(toolCallStream("c", "read", map[string]any{"path": "/same"}), nil)
		h.tools.AddSuccessfulExecution("same")
		h.pol.AddAllow("a", "")
	}
	// A final text-only turn in case the loop continues past detection.
	h.model.AddStream(textStream("done"), nil)

	_, err := h.run(t, cfg, "sess-doom", "spin")
	require.NoError(t, err)

	assert.GreaterOrEqual(t, h.metrics.doomLoopCount("read"), 1, "doom-loop must be detected for the repeating tool")
}

// ===========================================================================
// T-LOOP-07 / FR-LOOP-05 — resume adjudication.
// ===========================================================================

// TestRun_ResumeAbortsOpenTurn asserts that on start, an open turn in the loaded
// log (TurnStarted with no terminal event) is closed with a TurnAborted (with
// recovered usage) before the loop proceeds — never silently replayed.
func TestRun_ResumeAbortsOpenTurn(t *testing.T) {
	h := newHarness(t)
	// Pre-seed the event log with an open turn: a SessionStarted, a user
	// message, a TurnStarted with a checkpoint delta but no terminal event.
	seedOpenTurn(h, "sess-resume")

	// The resumed run produces a clean text-only turn.
	h.model.AddStream(textStream("resumed"), nil)

	res, err := h.run(t, defaultConfig(), "sess-resume", "continue")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	// A TurnAborted for the open turn was appended carrying the recovered usage.
	aborts := payloadsOf[domain.TurnAborted](h, "sess-resume")
	require.Len(t, aborts, 1)
	assert.Equal(t, "open-turn", aborts[0].TurnID)
	assert.Equal(t, llm.Usage{InputTokens: 7, OutputTokens: 3}, aborts[0].UsageSoFar,
		"recovered usage must come from the last AssistantMessageDelta, not zero")
}

// TestRun_ResumeSkipsUnknownMutatingExecution asserts that an unknown mutating
// tool execution (a ToolExecutionStarted with no terminal ToolResult) is NOT
// re-dispatched on resume (at-most-once; FR-TOOL-03 AC-1).
func TestRun_ResumeSkipsUnknownMutatingExecution(t *testing.T) {
	h := newHarness(t)
	seedUnknownMutatingExec(h, "sess-skip")

	h.model.AddStream(textStream("resumed"), nil)

	res, err := h.run(t, defaultConfig(), "sess-skip", "continue")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	// The unknown mutating execution must NOT have been re-dispatched: the fake
	// runtime recorded zero ExecuteTool calls.
	assert.Equal(t, 0, len(h.tools.Calls()), "unknown mutating execution must not be re-dispatched on resume")
}

// ---------------------------------------------------------------------------
// Seed helpers for resume tests — append events directly via the fake log.
// ---------------------------------------------------------------------------

func seedOpenTurn(h *harness, sessionID string) {
	ctx := context.Background()
	_, _ = h.eventlog.Append(ctx, sessionID, 0, 0, "seed-1",
		app.AppendInput{Event: domain.SessionStarted{SystemPrompt: "sys"}})
	_, _ = h.eventlog.Append(ctx, sessionID, 1, 0, "seed-2",
		app.AppendInput{Event: domain.MessageAppended{Message: userMsg("earlier")}})
	_, _ = h.eventlog.Append(ctx, sessionID, 2, 0, "seed-3",
		app.AppendInput{Event: domain.TurnStarted{TurnID: "open-turn", Model: "test-model"}})
	_, _ = h.eventlog.Append(ctx, sessionID, 3, 0, "seed-4",
		app.AppendInput{Event: domain.AssistantMessageDelta{
			TurnID:     "open-turn",
			TextSoFar:  "partial",
			UsageSoFar: llm.Usage{InputTokens: 7, OutputTokens: 3},
		}})
}

func seedUnknownMutatingExec(h *harness, sessionID string) {
	ctx := context.Background()
	_, _ = h.eventlog.Append(ctx, sessionID, 0, 0, "seed-1",
		app.AppendInput{Event: domain.SessionStarted{SystemPrompt: "sys"}})
	_, _ = h.eventlog.Append(ctx, sessionID, 1, 0, "seed-2",
		app.AppendInput{Event: domain.MessageAppended{Message: userMsg("earlier")}})
	_, _ = h.eventlog.Append(ctx, sessionID, 2, 0, "seed-3",
		app.AppendInput{Event: domain.TurnStarted{TurnID: "t-prev", Model: "test-model"}})
	_, _ = h.eventlog.Append(ctx, sessionID, 3, 0, "seed-4",
		app.AppendInput{Event: domain.AssistantMessage{
			TurnID:     "t-prev",
			Message:    llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentPart{{ToolCall: &llm.ToolCall{ID: "ex-1", Name: "write"}}}},
			StopReason: llm.StopToolUse,
		}})
	// Execution intent committed but no ToolResult followed (crash window).
	_, _ = h.eventlog.Append(ctx, sessionID, 4, 0, "seed-5",
		app.AppendInput{Event: domain.ToolExecutionStarted{CallID: "ex-1", ToolName: "write", IdempotencyKey: "k-1"}})
	// The runtime declares "write" as mutating, so recovery marks it unknown.
	h.tools.SetTools([]app.ToolDescriptor{{Name: "write", SideEffect: domain.SideEffectMutating}})
}

// ---------------------------------------------------------------------------
// concurrencyRuntime — a ToolRuntimePort that records concurrency and order.
// ---------------------------------------------------------------------------

// concurrencyRuntime is an app.ToolRuntimePort fake purpose-built to observe
// scheduling: it tracks the maximum number of simultaneously in-flight
// ExecuteTool calls and the dispatch order. An optional barrier makes each call
// block until `barrier` calls are in flight, which lets a test prove that N
// read-only calls genuinely overlap.
type concurrencyRuntime struct {
	mu            sync.Mutex
	inFlight      int32
	maxConcurrent atomic.Int32
	dispatchOrder []string
	calls         int
	tools         []app.ToolDescriptor
	barrier       int
	barrierMu     sync.Mutex
	barrierCond   *sync.Cond
	barrierCount  int
}

func newConcurrencyRuntime() *concurrencyRuntime {
	r := &concurrencyRuntime{}
	r.barrierCond = sync.NewCond(&r.barrierMu)
	return r
}

func (r *concurrencyRuntime) ExecuteTool(ctx context.Context, exec app.ToolExecution) (app.ToolStream, error) {
	r.mu.Lock()
	r.calls++
	r.dispatchOrder = append(r.dispatchOrder, exec.Call.ID)
	r.inFlight++
	cur := r.inFlight
	r.mu.Unlock()

	// Track the high-water mark of concurrency.
	for {
		prev := r.maxConcurrent.Load()
		if cur <= prev || r.maxConcurrent.CompareAndSwap(prev, cur) {
			break
		}
	}

	// Optional barrier: block until `barrier` calls have arrived, proving they
	// were dispatched concurrently. Guarded so a serialized path does not
	// deadlock (barrier 0 = disabled).
	if r.barrier > 0 {
		r.barrierMu.Lock()
		r.barrierCount++
		if r.barrierCount >= r.barrier {
			r.barrierCond.Broadcast()
		}
		for r.barrierCount < r.barrier {
			r.barrierCond.Wait()
		}
		r.barrierMu.Unlock()
	}

	r.mu.Lock()
	r.inFlight--
	r.mu.Unlock()

	return apptest.NewFakeToolStream(app.ToolResult{Content: "ok"}), nil
}

func (r *concurrencyRuntime) ListTools(_ context.Context, _ string) ([]app.ToolDescriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]app.ToolDescriptor(nil), r.tools...), nil
}

func (r *concurrencyRuntime) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *concurrencyRuntime) order() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.dispatchOrder...)
}

// compile-time assertion that concurrencyRuntime is a valid ToolRuntimePort.
var _ app.ToolRuntimePort = (*concurrencyRuntime)(nil)

// ensure policy import is used even if a future refactor drops a reference.
var _ = policy.Allow
