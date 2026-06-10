// Package eval is Boltrope's bespoke, network-free, DETERMINISTIC eval harness
// (ADR-0007; NFR-TEST-04; DOD-03). It drives the REAL orchestrator agent loop
// ([github.com/xd1lab/harness-ai/internal/orchestrator/app/agent.Loop]) end-to-end
// against a scripted, in-memory provider and fakes — no network, no Docker, no
// real clock — so each golden SCENARIO is reproducible and fast (the whole suite
// is a required CI gate that must finish well under 60 s).
//
// # What is real vs. faked
//
// The harness wires the loop with EXACTLY the production building blocks for
// everything that defines behavior, and fakes only the I/O edges:
//
//   - REAL: the agent loop itself, the real [policy.Engine] (deny→mode→allow→ask),
//     and the real context/window builder ([agentctx.BuildWindow], invoked by the
//     loop). Termination subtypes, event ordering, scheduling, and the permission
//     pipeline are therefore exercised as shipped, not re-implemented in the test.
//   - FAKED (deterministic): a SCRIPTED model gateway returning canned multi-turn
//     stream responses (incl. tool calls) via the in-repo
//     [apptest.FakeModelGateway]; an in-memory event log ([apptest.FakeEventLog])
//     that stands in for the pgx store; a scripted tool runtime
//     ([apptest.FakeToolRuntime]) returning canned tool results; an auto/deny
//     [apptest.FakeApprovalGate]; a no-op [apptest.FakeHookRunner]; a fake
//     [clocktest.Fake] clock and a scripted [idstest.Fake] id generator
//     (NFR-TEST-01); and a deterministic cost function.
//
// # The scenario DSL
//
// A [Scenario] is a declarative description of one end-to-end run: the canned
// model turns, the tool descriptors + canned results, the policy rules, the run
// config, and the EXPECTATIONS — the terminal [domain.TerminationReason], the
// exact ordered [domain.EventType] sequence appended to the log (golden shape),
// and the token/cost accounting. [Scenario.Run] builds the loop, runs it against
// the fakes, and [Result.assert] (via [Scenario.Check]) verifies every
// expectation. The concrete scenarios live in scenarios_test.go and run as a
// normal `go test ./test/eval/...`.
//
// # Live tier
//
// A separate, build-tagged live-smoke tier (live_test.go, //go:build livesmoke)
// runs ONE real coding task end-to-end against a provider configured from the
// environment and SKIPS when no key is set. It is NOT part of the per-PR gate.
package eval

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agentctx"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/apptest"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
	"github.com/xd1lab/harness-ai/internal/platform/clock/clocktest"
	"github.com/xd1lab/harness-ai/internal/platform/ids/idstest"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ----------------------------------------------------------------------------
// Scenario DSL
// ----------------------------------------------------------------------------

// Scenario is one declarative, deterministic eval case. It captures the canned
// inputs (model turns, tools, policy rules, config) and the expected outcome
// (terminal reason, ordered event-log shape, and token/cost accounting). The
// zero value is not useful; use the helpers in scenarios_test.go to build one.
type Scenario struct {
	// Name is the human-readable scenario name, used as the subtest name.
	Name string

	// SessionID is the session the run drives. Defaults to a name-derived id when
	// empty.
	SessionID string
	// UserMessage is the initial user task text appended before the first turn.
	UserMessage string
	// Tainted seeds the run's untrusted-content taint flag (threaded into the
	// policy taint gate).
	Tainted bool

	// ModelTurns are the canned provider stream responses, consumed one per
	// model round-trip in order. Build them with [TextTurn], [ToolCallTurn],
	// [MultiToolTurn], [RefusalTurn], or [StreamErrorTurn].
	ModelTurns []ModelTurn

	// Tools are the tool descriptors the scripted runtime advertises (drives
	// SideEffect/EgressClass classification, scheduling, and policy input).
	Tools []app.ToolDescriptor
	// ToolResults are the canned tool execution results, consumed one per
	// dispatched tool call in order.
	ToolResults []app.ToolResult

	// Policy is the real policy engine's rule set + mode for this run.
	Rules     []policy.Rule
	EditTools []string
	Mode      policy.Mode

	// Approvals maps a tool call id to the human resolution the auto approval
	// gate should deliver when that call's ask gate is raised. A call id absent
	// from the map (when an ask is raised for it) fails the scenario with a clear
	// message rather than hanging.
	Approvals map[string]domain.AskResolution

	// Config overrides for the run. Zero values fall back to permissive defaults
	// in [Scenario.config] so a scenario only sets the caps it cares about.
	MaxTurns                   int
	MaxBudgetUSD               float64
	MaxStructuredOutputRetries int
	DoomLoopThreshold          int
	Model                      string
	OutputSchema               []byte

	// CostPerTurnUSD is the deterministic per-turn USD cost the injected cost
	// function returns (so budget caps and cost accounting are exact). Zero means
	// a zero-cost run.
	CostPerTurnUSD float64

	// MaxContextTokens, when > 0, wires the REAL [agentctx.Manager] into the loop
	// with this context budget (threshold = budget x the default 0.8 fraction),
	// enabling the threshold-triggered compaction path (FR-CTX-01; DOD-03's
	// compaction golden). The window is measured by a deterministic scripted
	// counter fed from WindowTokenCounts.
	MaxContextTokens int
	// WindowTokenCounts are the scripted token measurements the fake
	// [agentctx.TokenCounter] returns, consumed one per Manager Count call (the
	// Manager measures twice per triggered compaction: current window, then the
	// projected post-boundary window). Exhausting the script fails the scenario
	// loudly rather than mis-measuring silently.
	WindowTokenCounts []int

	// --- Expectations -------------------------------------------------------

	// WantReason is the expected terminal termination subtype (FR-LOOP-02).
	WantReason domain.TerminationReason
	// WantEvents is the exact ordered sequence of event types appended to the
	// session log (the golden log shape; FR-LOOP-01 AC-1). When nil the event
	// shape is not asserted (a scenario may assert it via custom checks instead).
	WantEvents []domain.EventType
	// WantNumTurns, when > 0, is the expected number of model round-trips.
	WantNumTurns int
	// WantCostUSD, when set (>= 0 and WantCostAsserted true), is the expected
	// cumulative cost. Use [Scenario] with CostPerTurnUSD and WantCostAsserted.
	WantCostUSD      float64
	WantCostAsserted bool
	// WantErrorMetric, when non-empty, is the typed termination subtype expected
	// to have incremented the RED error counter exactly once (FR-OBS-02).
	WantErrorMetric string
	// WantDoomLoopTool, when non-empty, asserts the loop emitted at least one
	// doom-loop signal for the named tool (FR-OBS-04).
	WantDoomLoopTool string
	// WantNoToolExecution asserts the scripted tool runtime's ExecuteTool was
	// never called (used by the permission-denied scenario: no side effects). A
	// hard deny needing no human ask is asserted structurally instead — the golden
	// WantEvents for that scenario contains a PermissionDecided{deny} and NO ask.
	WantNoToolExecution bool

	// MaxRunTime bounds the wall-clock time the run may take; the harness fails
	// the scenario if exceeded (guards against an accidental hang). Defaults to
	// [defaultScenarioTimeout].
	MaxRunTime time.Duration

	// extraChecks are optional, scenario-specific assertions run against the
	// [Result] after the standard expectations. Set via [Scenario.WithCheck].
	extraChecks []func(t *testing.T, r Result)
}

// ModelTurn is one canned provider stream response for a single model
// round-trip. Exactly one of Events / Err is meaningful: Events is the scripted
// [llm.StreamEvent] sequence (terminated by a [llm.Done]); a non-nil Err makes
// the gateway's Stream call fail to start (a provider error), exercising the
// loop's error_during_execution path.
type ModelTurn struct {
	Events []llm.StreamEvent
	Err    error
}

// Result is the outcome of running a [Scenario]: the loop's [agent.RunResult]
// plus the recorded fakes, so checks can assert the event log, the metrics, and
// the tool/approval interactions.
type Result struct {
	Run        agent.RunResult
	Err        error
	EventTypes []domain.EventType
	Events     []domain.EventEnvelope
	ToolCalls  int
	Metrics    *recordingMetrics
}

const (
	// defaultScenarioTimeout bounds a single scenario run. Deterministic runs
	// complete in microseconds; this is purely a hang guard.
	defaultScenarioTimeout = 10 * time.Second
	// defaultMaxTurns is the permissive turn cap applied when a scenario leaves
	// MaxTurns zero (so a scenario that does not test the cap never trips it).
	defaultMaxTurns = 16
	// defaultModel is the model id used when a scenario leaves Model empty.
	defaultModel = "eval-model"
)

// WithCheck registers an extra assertion run after the standard expectations,
// returning the scenario so calls chain. It is how a scenario asserts something
// beyond the declarative expectation fields (e.g. a specific payload value).
func (s Scenario) WithCheck(fn func(t *testing.T, r Result)) Scenario {
	s.extraChecks = append(s.extraChecks, fn)
	return s
}

// config assembles the [agent.Config] for the scenario, applying permissive
// defaults for any cap the scenario left unset.
func (s Scenario) config() agent.Config {
	maxTurns := s.MaxTurns
	if maxTurns == 0 {
		maxTurns = defaultMaxTurns
	}
	model := s.Model
	if model == "" {
		model = defaultModel
	}
	return agent.Config{
		Model:                      model,
		MaxTurns:                   maxTurns,
		MaxBudgetUSD:               s.MaxBudgetUSD,
		MaxStructuredOutputRetries: s.MaxStructuredOutputRetries,
		DoomLoopThreshold:          s.DoomLoopThreshold,
		Mode:                       s.Mode,
		OutputSchema:               s.OutputSchema,
	}
}

// sessionID returns the scenario's session id, deriving a stable one from the
// name when unset.
func (s Scenario) sessionID() string {
	if s.SessionID != "" {
		return s.SessionID
	}
	return "sess-" + s.Name
}

// Run builds the real loop wired against the deterministic fakes, drives it with
// the scenario's user message, and returns the [Result]. It does NOT assert the
// expectations — call [Scenario.Check] (or use [Scenario.Exec] which does both).
func (s Scenario) Run(t *testing.T) Result {
	t.Helper()

	eventlog := apptest.NewFakeEventLog()
	model := apptest.NewFakeModelGateway()
	for _, turn := range s.ModelTurns {
		model.AddStream(turn.Events, turn.Err)
	}

	tools := apptest.NewFakeToolRuntime()
	if len(s.Tools) > 0 {
		tools.SetTools(s.Tools)
	}
	for _, r := range s.ToolResults {
		tools.AddExecution(apptest.NewFakeToolStream(r), nil)
	}

	gate := newAutoApprovalGate(s.Approvals)
	hooks := apptest.NewFakeHookRunner() // no-op: allows every PreToolUse by default

	eng, err := policy.NewEngine(policy.Config{
		RuleSet:   policy.RuleSet{Rules: s.Rules},
		EditTools: s.EditTools,
	})
	if err != nil {
		t.Fatalf("eval: build policy engine: %v", err)
	}

	clk := clocktest.NewFake(time.Unix(0, 0))
	idgen := idstest.NewFake(deterministicIDs(256)...)
	metrics := &recordingMetrics{}

	var costFn agent.CostFunc
	if s.CostPerTurnUSD != 0 {
		cost := s.CostPerTurnUSD
		costFn = func(string, llm.Usage) (float64, error) { return cost, nil }
	}

	// Wire the REAL context manager when the scenario opts into compaction
	// (FR-CTX-01): the only fake is the deterministic token counter.
	var ctxMgr *agentctx.Manager
	if s.MaxContextTokens > 0 {
		ctxMgr = agentctx.NewManager(
			&scriptedTokenCounter{t: t, counts: s.WindowTokenCounts},
			agentctx.Config{Model: s.config().Model, MaxContextTokens: s.MaxContextTokens},
		)
	}

	loop := agent.NewLoop(agent.Deps{
		EventLog:  eventlog,
		Model:     model,
		Tools:     tools,
		Approvals: gate,
		Hooks:     hooks,
		Policy:    eng,
		Context:   ctxMgr,
		Clock:     clk,
		IDs:       idgen,
		Metrics:   metrics,
		CostFunc:  costFn,
	}, s.config())

	timeout := s.MaxRunTime
	if timeout == 0 {
		timeout = defaultScenarioTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	sid := s.sessionID()
	done := make(chan struct{})
	var runRes agent.RunResult
	var runErr error
	go func() {
		defer close(done)
		runRes, runErr = loop.Run(ctx, agent.RunInput{
			SessionID:   sid,
			UserMessage: userMessage(s.UserMessage),
			Tainted:     s.Tainted,
		})
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("eval: scenario %q did not complete within %s (possible hang)", s.Name, timeout)
	}

	events, _ := eventlog.Load(context.Background(), sid, 0)
	types := make([]domain.EventType, 0, len(events))
	for _, e := range events {
		types = append(types, e.Type)
	}

	return Result{
		Run:        runRes,
		Err:        runErr,
		EventTypes: types,
		Events:     events,
		ToolCalls:  len(tools.Calls()),
		Metrics:    metrics,
	}
}

// Check asserts every declared expectation against the [Result] plus any
// registered extra checks. It uses t.Errorf (not Fatalf) for most assertions so
// a single scenario reports all mismatches at once.
func (s Scenario) Check(t *testing.T, r Result) {
	t.Helper()

	if r.Err != nil {
		t.Fatalf("eval: scenario %q returned an infrastructural error: %v", s.Name, r.Err)
	}

	if r.Run.Reason != s.WantReason {
		t.Errorf("terminal reason = %q, want %q", r.Run.Reason, s.WantReason)
	}

	if s.WantEvents != nil {
		assertEventTypes(t, s.WantEvents, r.EventTypes)
	}

	if s.WantNumTurns > 0 && r.Run.NumTurns != s.WantNumTurns {
		t.Errorf("num turns = %d, want %d", r.Run.NumTurns, s.WantNumTurns)
	}

	if s.WantCostAsserted {
		if !approxEqual(r.Run.CostUSD, s.WantCostUSD) {
			t.Errorf("cumulative cost = %.6f, want %.6f", r.Run.CostUSD, s.WantCostUSD)
		}
	}

	if s.WantErrorMetric != "" {
		if n := r.Metrics.errorCount(s.WantErrorMetric); n != 1 {
			t.Errorf("RED error counter for subtype %q = %d, want 1", s.WantErrorMetric, n)
		}
	}

	if s.WantNoToolExecution && r.ToolCalls != 0 {
		t.Errorf("tool executions = %d, want 0 (no side effects expected)", r.ToolCalls)
	}

	if s.WantDoomLoopTool != "" {
		if n := r.Metrics.doomLoopCount(s.WantDoomLoopTool); n < 1 {
			t.Errorf("doom-loop signals for tool %q = %d, want >= 1", s.WantDoomLoopTool, n)
		}
	}

	for _, fn := range s.extraChecks {
		fn(t, r)
	}
}

// Exec runs the scenario and asserts its expectations in one call. Concrete
// scenarios call this from a subtest.
func (s Scenario) Exec(t *testing.T) {
	t.Helper()
	r := s.Run(t)
	s.Check(t, r)
}

// ----------------------------------------------------------------------------
// Recording metrics fake (mirrors the loop's MetricsRecorder port)
// ----------------------------------------------------------------------------

// recordingMetrics is an [agent.MetricsRecorder] that records the RED error
// subtypes and doom-loop tool names the loop emits, so scenarios can assert
// FR-OBS-02 / FR-OBS-04 signals. It is safe for concurrent use because the loop
// may record from the streaming goroutine.
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

// doomLoopCount returns how many times the loop emitted a doom-loop signal for
// the given tool. It backs the doom-loop assertion in
// [Scenario.WantDoomLoopTool] (FR-OBS-04).
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

// ----------------------------------------------------------------------------
// Scripted token counter (compaction scenarios)
// ----------------------------------------------------------------------------

// scriptedTokenCounter is a deterministic [agentctx.TokenCounter]: each Count
// call consumes the next scripted measurement. Exhausting the script fails the
// scenario immediately — a silent default would let the compaction decision
// drift from what the scenario declared.
type scriptedTokenCounter struct {
	mu     sync.Mutex
	t      *testing.T
	counts []int
	calls  int
}

func (c *scriptedTokenCounter) Count(_ context.Context, _ string, _ []llm.Message, _ []llm.ToolDef) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.calls >= len(c.counts) {
		c.t.Fatalf("eval: WindowTokenCounts exhausted (call %d); script one count per Manager measurement", c.calls+1)
	}
	n := c.counts[c.calls]
	c.calls++
	return n, nil
}

// Compile-time assertion that the scripted counter satisfies the agentctx port.
var _ agentctx.TokenCounter = (*scriptedTokenCounter)(nil)

// ----------------------------------------------------------------------------
// Auto approval gate
// ----------------------------------------------------------------------------

// autoApprovalGate is an [app.ApprovalGate] that resolves each raised ask
// IMMEDIATELY from a pre-seeded decision table, so the deterministic harness
// never blocks. A request for a call id absent from the table returns an error,
// which surfaces as an infrastructural run error and fails the scenario with a
// clear message (better than hanging forever).
type autoApprovalGate struct {
	decisions map[string]domain.AskResolution
}

func newAutoApprovalGate(decisions map[string]domain.AskResolution) *autoApprovalGate {
	return &autoApprovalGate{decisions: decisions}
}

// Request returns the pre-seeded resolution for req.CallID, or an error when the
// scenario did not script a decision for a raised ask.
func (g *autoApprovalGate) Request(_ context.Context, req app.ApprovalRequest) (domain.AskResolution, error) {
	if res, ok := g.decisions[req.CallID]; ok {
		return res, nil
	}
	return domain.AskUnresolved, fmt.Errorf("eval: unscripted approval ask for call %q (tool %q); add it to Scenario.Approvals", req.CallID, req.ToolName)
}

// Resolve is unused in the auto gate (decisions are pre-seeded) but is required
// by the [app.ApprovalGate] interface.
func (g *autoApprovalGate) Resolve(context.Context, string, string, domain.AskResolution) error {
	return nil
}

// Compile-time assertion that autoApprovalGate satisfies the frozen port.
var _ app.ApprovalGate = (*autoApprovalGate)(nil)

// ----------------------------------------------------------------------------
// Stream / message / event builders shared by scenarios
// ----------------------------------------------------------------------------

// TextTurn is a model turn that emits one text delta then a terminal Done(end) —
// a normal text-only completion.
func TextTurn(text string) ModelTurn {
	return ModelTurn{Events: []llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: text}},
		{Done: &llm.Done{StopReason: llm.StopEnd}},
	}}
}

// TextTurnWithUsage is like [TextTurn] but attaches usage to the terminal Done
// so token accounting can be asserted.
func TextTurnWithUsage(text string, usage llm.Usage) ModelTurn {
	return ModelTurn{Events: []llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: text}},
		{Done: &llm.Done{StopReason: llm.StopEnd, Usage: usage}},
	}}
}

// ToolCallTurn is a model turn that emits a single complete (buffered) tool call
// then Done(tool_use), optionally with usage on the Done.
func ToolCallTurn(callID, name string, args map[string]any, usage llm.Usage) ModelTurn {
	return ModelTurn{Events: []llm.StreamEvent{
		{ToolCallDelta: &llm.ToolCallDelta{CallID: callID, Name: name, ArgsFragment: mustJSON(args)}},
		{Done: &llm.Done{StopReason: llm.StopToolUse, Usage: usage}},
	}}
}

// MultiToolTurn emits several complete tool calls in one assistant turn followed
// by Done(tool_use).
func MultiToolTurn(calls ...llm.ToolCall) ModelTurn {
	evs := make([]llm.StreamEvent, 0, len(calls)+1)
	for _, c := range calls {
		evs = append(evs, llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{
			CallID: c.ID, Name: c.Name, ArgsFragment: mustJSON(c.Args),
		}})
	}
	evs = append(evs, llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopToolUse}})
	return ModelTurn{Events: evs}
}

// RefusalTurn is a model turn that emits some text then a terminal
// Done(refusal), exercising the distinct Refusal termination subtype.
func RefusalTurn(text string) ModelTurn {
	return ModelTurn{Events: []llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: text}},
		{Done: &llm.Done{StopReason: llm.StopRefusal}},
	}}
}

// StreamErrorTurn is a model turn whose Stream call fails to start with err,
// exercising the loop's error_during_execution path.
func StreamErrorTurn(err error) ModelTurn { return ModelTurn{Err: err} }

// userMessage builds the initial user [llm.Message] (empty text yields no
// content, which appends nothing — a pure resume).
func userMessage(text string) llm.Message {
	if text == "" {
		return llm.Message{}
	}
	return llm.Message{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: text}}}}
}
