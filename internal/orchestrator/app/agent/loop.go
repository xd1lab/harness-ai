// Package agent holds the orchestrator's pure agent-loop building blocks: the
// stream assembler (assembler.go, T-LOOP-01) and the agent loop itself
// (loop.go/turn.go/tools.go, T-LOOP-05 + T-LOOP-07).
//
// # The loop (T-LOOP-05)
//
// [Loop.Run] drives the gather→act→verify cycle (architecture §3): on each turn
// it builds the model-visible window from the session event log (via the
// injected context manager), appends a [domain.TurnStarted], streams the model
// through [app.ModelGatewayPort], forwards text/thinking deltas to the injected
// [ClientSink] while feeding the same reader to the pure [Assemble], appends one
// [domain.AssistantMessage] with usage/cost/provider_raw, and — if the model
// requested tools — runs each through the permission pipeline (hooks PreToolUse →
// PolicyEngine deny→mode→allow→ask → ApprovalGate for ask), persisting a
// [domain.PermissionDecided] for every decision, then dispatches allowed calls
// and feeds the results back. The cycle repeats until the model emits a
// text-only response or a cap/termination fires (FR-LOOP-01/02).
//
// # Scheduling (architecture §9.2)
//
// Read-only tool calls in a single assistant turn dispatch CONCURRENTLY through
// a bounded errgroup (SetLimit(min(4,GOMAXPROCS))); mutating tools are serialized
// in emitted order via a per-session mutation mutex; external-egress tools
// (webfetch/websearch) are NOT auto-parallelized — they flow through the policy
// path one at a time (architecture §8.4, §9.2). See tools.go.
//
// # Termination (FR-LOOP-02)
//
// Every run ends with a typed [domain.TerminationReason]: Success,
// ErrorMaxTurns, ErrorMaxBudgetUSD, ErrorDuringExecution,
// ErrorMaxStructuredOutputRetries, or Refusal. A refusal ([llm.StopRefusal]) is
// its own subtype, never folded into ErrorDuringExecution (architecture §11.3).
//
// # Resume / adjudication (T-LOOP-07)
//
// On start [Loop.Run] loads the session, folds it through
// [github.com/xd1lab/harness-ai/internal/orchestrator/app/recovery.Analyze], and
// adjudicates: open turns are closed with a [domain.TurnAborted] carrying the
// recovered usage (never silently replayed), and unknown mutating tool
// executions are NOT re-dispatched (at-most-once; FR-TOOL-03 AC-1).
//
// # Determinism
//
// The loop injects [clock.Clock] and [ids.IDGenerator] (NFR-TEST-01; forbidigo
// enforces no time.Now/uuid.New here) so turn ids, request ids, and any timing
// are deterministic under test. It imports nothing from gen/ and no gRPC; the
// whole loop is provable against the in-repo fakes.
package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"strconv"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agentctx"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/recovery"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
	"github.com/xd1lab/harness-ai/internal/platform/clock"
	"github.com/xd1lab/harness-ai/internal/platform/ids"
	"github.com/xd1lab/harness-ai/internal/platform/jsonschema"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// Deps bundles the injected ports the loop depends on. Every field is a
// consumer-defined port (or platform port) so the whole loop is provable against
// the in-repo fakes (apptest, policytest, llmtest, clocktest, idstest). The
// non-port fields (Sink, Metrics, CostFunc) are optional and default to
// zero-behavior implementations when nil.
type Deps struct {
	// EventLog is the append-only event store (append/load/fork/subscribe).
	EventLog app.EventLogPort
	// Model is the client-side model-gateway port (Stream/Generate/Capabilities).
	Model app.ModelGatewayPort
	// Tools is the client-side tool-runtime port (ExecuteTool/ListTools).
	Tools app.ToolRuntimePort
	// Approvals is the human-in-the-loop ask gate.
	Approvals app.ApprovalGate
	// Hooks is the PreToolUse/PostToolUse/Stop/PreCompact pipeline.
	Hooks app.HookRunner
	// Policy is the deny→mode→allow→ask permission engine.
	Policy policy.PolicyEngine
	// Context is the optional context/compaction manager. When nil the loop
	// builds the model window directly from the folded log without compaction.
	Context *agentctx.Manager
	// SubAgent optionally exposes depth-limited sub-agents as a tool. It is not
	// required by the core loop (T-LOOP-06 wires it); it is carried here so the
	// loop's Deps are complete.
	SubAgent app.SubAgentPort
	// Clock is the injected time source (NFR-TEST-01). Required.
	Clock clock.Clock
	// IDs mints turn ids and per-append request ids (NFR-TEST-01). Required.
	IDs ids.IDGenerator
	// Sink forwards live text/thinking deltas to the client. Nil → discarded.
	Sink ClientSink
	// Metrics records RED error and doom-loop counters. Nil → no-op.
	Metrics MetricsRecorder
	// CostFunc computes per-turn USD cost from usage. Nil → zero cost.
	CostFunc CostFunc
}

// Config parameterizes a [Loop]: the model id, the termination caps, the
// structured-output retry cap, the doom-loop window, the read-only concurrency
// limit, and the tool definitions advertised to the model.
type Config struct {
	// Model is the target model id for this run (used for Generate, token
	// counting, and cost). Required.
	Model string
	// MaxTurns is the maximum number of model round-trips before the run
	// terminates with [domain.ErrorMaxTurns]. A non-positive value uses
	// [DefaultMaxTurns].
	MaxTurns int
	// MaxBudgetUSD is the maximum cumulative cost before the run terminates with
	// [domain.ErrorMaxBudgetUSD] (checked before each Generate). A non-positive
	// value disables the budget cap.
	MaxBudgetUSD float64
	// MaxStructuredOutputRetries is the number of additional attempts (beyond the
	// first) to obtain a schema-valid response when [Config.OutputSchema] /
	// [llm.Request.OutputSchema] is set. On exhaustion the run terminates with
	// [domain.ErrorMaxStructuredOutputRetries]. A non-positive value uses
	// [DefaultStructuredOutputRetries].
	MaxStructuredOutputRetries int
	// DoomLoopThreshold is the number of consecutive identical tool calls that
	// triggers a doom-loop signal (FR-OBS-04). A non-positive value disables
	// detection.
	DoomLoopThreshold int
	// ReadOnlyConcurrency bounds the read-only tool worker pool. A non-positive
	// value uses min(4, GOMAXPROCS) (architecture §9.2).
	ReadOnlyConcurrency int
	// ToolDefs are the tool definitions advertised to the model in each
	// [llm.Request]. When empty the loop derives them from
	// [app.ToolRuntimePort.ListTools].
	ToolDefs []llm.ToolDef
	// Mode is the permission operating mode for this run (default/acceptEdits/
	// plan/bypass). The zero value is treated as [policy.ModeDefault].
	Mode policy.Mode
	// LeaseEpoch is the writer's fencing token passed on every append (architecture
	// §9.6). Zero is valid for the single-writer test/in-memory path.
	LeaseEpoch int64
	// OutputSchema, when non-nil, requests structured output validated against
	// this JSON Schema with validate-and-retry (mirrors [llm.Request.OutputSchema]).
	OutputSchema []byte
	// Strict requests provider-native strict enforcement of [OutputSchema] where
	// the provider supports it (Capabilities.SupportsJSONSchemaStrict); otherwise
	// the loop falls back to validate-and-retry. Meaningful only when OutputSchema
	// is set. Mirrors [llm.Request.Strict].
	Strict bool
	// Depth is this loop's own sub-agent recursion depth (0 = root). It is used
	// (a) to gate whether the spawn_subagent virtual tool is advertised (only when
	// Depth < [app.SubAgentPort.MaxDepth]) and (b) to compute the child's depth:
	// the spawn_subagent intercept passes Depth+1 to [app.SubAgentPort.Spawn]. The
	// zero value (root) preserves current behavior for runs that never spawn.
	Depth int
}

const (
	// DefaultMaxTurns is the max-turns cap applied when [Config.MaxTurns] is
	// non-positive.
	DefaultMaxTurns = 32
	// DefaultStructuredOutputRetries is the structured-output retry cap applied
	// when [Config.MaxStructuredOutputRetries] is non-positive.
	DefaultStructuredOutputRetries = 3
)

// RunInput is the input to one [Loop.Run]: the session to run and the new user
// turn to append before generating.
type RunInput struct {
	// SessionID is the session/stream to run.
	SessionID string
	// UserMessage is the new user turn to append as a [domain.MessageAppended]
	// before the first Generate. A zero-value message (no content) appends
	// nothing (used by a pure resume that has no fresh user input).
	UserMessage llm.Message
	// Tainted reports whether untrusted content has entered the session, which
	// the policy taint gate uses to escalate external-comms calls (architecture
	// §8.4). It is threaded into every [policy.Input] for the run.
	Tainted bool
}

// RunResult is the terminal outcome of a [Loop.Run]: the typed termination
// reason plus the cumulative usage, cost, and turn count of the run.
type RunResult struct {
	// Reason is the typed termination subtype (FR-LOOP-02).
	Reason domain.TerminationReason
	// Usage is the cumulative normalized usage across all turns of the run.
	Usage llm.Usage
	// CostUSD is the cumulative USD cost across all turns of the run.
	CostUSD float64
	// NumTurns is the number of model round-trips the run performed.
	NumTurns int
}

// Loop is the orchestrator's agent loop. It is constructed once per run owner
// with [NewLoop] and driven via [Loop.Run]. A Loop holds no per-run mutable
// state on the struct itself; per-run state lives in [Loop.Run]'s call frame, so
// a single Loop value may serve sequential runs. (Concurrent runs on distinct
// sessions are safe provided the injected ports are; the per-session mutation
// mutex that serializes mutating tools is created per run.)
type Loop struct {
	deps Deps
	cfg  Config

	sink    ClientSink
	metrics MetricsRecorder
}

// NewLoop constructs a [Loop] from its dependencies and config. It substitutes
// zero-behavior defaults for an absent Sink/Metrics so callers never pass a nil
// just to satisfy the interface.
func NewLoop(deps Deps, cfg Config) *Loop {
	l := &Loop{deps: deps, cfg: cfg}
	l.sink = deps.Sink
	if l.sink == nil {
		l.sink = noopSink{}
	}
	l.metrics = deps.Metrics
	if l.metrics == nil {
		l.metrics = noopMetrics{}
	}
	return l
}

// runState is the mutable accounting carried through one [Loop.Run].
type runState struct {
	sessionID  string
	headSeq    int64
	leaseEpoch int64

	usage    llm.Usage
	cost     float64
	numTurns int

	// currentTurnID is the turn id of the most recently started turn, used by
	// finish to attach the terminal TurnFinished to the right turn.
	currentTurnID string
	// lastAssistantSeq is the seq assigned to the most recent AssistantMessage,
	// used to derive each tool call's log-derived idempotency key
	// hash(session_id, seq_of_ToolCall) (ADR-0012; architecture §7.2).
	lastAssistantSeq int64

	tainted bool

	// doom-loop detection: the signature of the last dispatched tool batch and
	// how many times it has repeated consecutively.
	lastToolSig string
	repeatCount int

	// structuredRetries counts structured-output retries taken so far (used only
	// when an OutputSchema is configured).
	structuredRetries int
}

// Run drives the agent loop for in.SessionID until a terminal condition fires,
// returning the typed [RunResult]. It first adjudicates any in-flight state from
// a prior crash/interrupt (resume; T-LOOP-07), then appends the user message and
// runs turns (FR-LOOP-01). It never returns a non-nil error for a normal typed
// termination — the terminal reason is carried on [RunResult.Reason] and the
// final [domain.TurnFinished] (or, for an aborted turn, [domain.TurnAborted]).
// A non-nil error is reserved for an infrastructural failure (e.g. the event log
// itself is unreachable) or context cancellation.
func (l *Loop) Run(ctx context.Context, in RunInput) (RunResult, error) {
	st := &runState{
		sessionID:  in.SessionID,
		leaseEpoch: l.cfg.LeaseEpoch,
		tainted:    in.Tainted,
	}

	// Load current session state to obtain the head seq before appending.
	sess, err := l.deps.EventLog.LoadSession(ctx, in.SessionID)
	if err != nil {
		return RunResult{}, fmt.Errorf("agent: load session: %w", err)
	}
	st.headSeq = sess.HeadSeq

	// --- Resume adjudication (T-LOOP-07) -----------------------------------
	if err := l.adjudicateResume(ctx, st); err != nil {
		return RunResult{}, err
	}

	// --- Append the new user message ---------------------------------------
	if hasContent(in.UserMessage) {
		if err := l.append(ctx, st, domain.ActorUser, domain.MessageAppended{Message: in.UserMessage}); err != nil {
			return RunResult{}, err
		}
	}

	// --- Turn loop ----------------------------------------------------------
	maxTurns := l.cfg.MaxTurns
	if maxTurns <= 0 {
		maxTurns = DefaultMaxTurns
	}

	for {
		// Budget cap is checked BEFORE the next Generate (FR-LOOP-02 AC-1).
		if l.cfg.MaxBudgetUSD > 0 && st.cost > l.cfg.MaxBudgetUSD {
			return l.terminate(ctx, st, domain.ErrorMaxBudgetUSD)
		}
		// Max-turns cap.
		if st.numTurns >= maxTurns {
			return l.terminate(ctx, st, domain.ErrorMaxTurns)
		}

		outcome, reason, err := l.runTurn(ctx, st)
		if err != nil {
			return RunResult{}, err
		}

		switch outcome {
		case turnTerminal:
			// The model produced a terminal (text-only/refusal/etc.) turn or a
			// cap fired inside the turn; reason carries the subtype.
			return l.finish(ctx, st, reason)
		case turnContinue:
			// A tool round-trip happened (or a Pause was continued); loop again.
			continue
		case turnAborted:
			// The turn was aborted (stream error with no retry budget); the
			// TurnAborted was already appended. Surface the reason.
			return l.result(st, reason), nil
		}
	}
}

// adjudicateResume folds the loaded log and closes any open turn with a
// TurnAborted (carrying the recovered usage) and refuses to re-dispatch unknown
// mutating executions (T-LOOP-07; FR-LOOP-05, FR-TOOL-03). Read-only unknown
// executions are left for the loop's normal flow; mutating ones are at-most-once
// and never re-run here. Recovery is a pure fold; this method performs the
// resulting appends.
func (l *Loop) adjudicateResume(ctx context.Context, st *runState) error {
	events, err := l.deps.EventLog.Load(ctx, st.sessionID, 0)
	if err != nil {
		return fmt.Errorf("agent: load for recovery: %w", err)
	}
	if len(events) == 0 {
		return nil
	}

	plan, err := recovery.Analyze(events, l.sideEffectLookup(ctx, st.sessionID))
	if err != nil {
		return fmt.Errorf("agent: recovery analyze: %w", err)
	}

	// Close each open turn with a TurnAborted carrying recovered usage so the
	// partial turn is accounted and never silently replayed (FR-LOOP-05 AC-2).
	for _, ot := range plan.OpenTurns {
		cost := l.computeCost(ot.RecoveredUsage)
		st.usage = addUsage(st.usage, ot.RecoveredUsage)
		st.cost += cost
		if err := l.append(ctx, st, domain.ActorSystem, domain.TurnAborted{
			TurnID:     ot.TurnID,
			Reason:     domain.ErrorDuringExecution,
			UsageSoFar: ot.RecoveredUsage,
			CostUSD:    cost,
		}); err != nil {
			return err
		}
	}

	// Unknown mutating executions are NOT re-dispatched (at-most-once). We make
	// the decision explicit and auditable, but we deliberately do not call
	// ExecuteTool for them. (The durable dedup ledger in the tool-runtime is the
	// authority on whether the side effect actually happened; the loop's
	// invariant is simply: never blind-re-run a mutating tool on resume.)
	// Re-dispatchable (read-only) unknown executions need no special handling —
	// the model will re-request them if it still needs them; we do not
	// speculatively re-run anything here, keeping resume side-effect-free.
	_ = plan.UnknownExecutions

	return nil
}

// sideEffectLookup builds a [recovery.SideEffectLookup] from the tool-runtime's
// advertised descriptors so recovery can classify an unknown execution. On any
// error enumerating tools it returns a nil lookup, which recovery treats as
// fail-safe mutating (ADR-0014).
func (l *Loop) sideEffectLookup(ctx context.Context, sessionID string) recovery.SideEffectLookup {
	descs, err := l.deps.Tools.ListTools(ctx, sessionID)
	if err != nil {
		return nil
	}
	byName := make(map[string]domain.SideEffect, len(descs))
	for _, d := range descs {
		byName[d.Name] = d.SideEffect
	}
	return func(name string) domain.SideEffect {
		if se, ok := byName[name]; ok {
			return se
		}
		return domain.SideEffectMutating
	}
}

// turnOutcome classifies how a single turn ended for the run loop.
type turnOutcome int

const (
	// turnTerminal: the model produced a terminal turn (or a cap fired inside);
	// the run should finish with the accompanying reason.
	turnTerminal turnOutcome = iota
	// turnContinue: a tool round-trip (or Pause continuation) happened; loop.
	turnContinue
	// turnAborted: the turn was aborted (e.g. unrecoverable stream error); the
	// TurnAborted event was already appended.
	turnAborted
)

// terminate appends a final TurnFinished carrying reason on a NEW turn boundary.
// It is used for caps that fire between turns (max-turns/max-budget) where there
// is no in-flight assistant turn to attach the reason to: a fresh TurnStarted/
// TurnFinished pair records the terminal decision deterministically. It records
// the RED error metric for the subtype.
func (l *Loop) terminate(ctx context.Context, st *runState, reason domain.TerminationReason) (RunResult, error) {
	turnID := l.deps.IDs.NewID().String()
	if err := l.append(ctx, st, domain.ActorSystem, domain.TurnStarted{TurnID: turnID, Model: l.cfg.Model}); err != nil {
		return RunResult{}, err
	}
	return l.finishTurn(ctx, st, turnID, reason)
}

// finish appends a terminal TurnFinished for the LAST started turn id. It is the
// path for a terminal model turn (success/refusal/structured-output exhaustion)
// where runTurn already appended the AssistantMessage on st's current turn.
func (l *Loop) finish(ctx context.Context, st *runState, reason domain.TerminationReason) (RunResult, error) {
	return l.finishTurn(ctx, st, st.currentTurnID, reason)
}

// finishTurn appends TurnFinished{reason} for turnID, records the RED error
// metric when the reason is an error subtype, and returns the assembled
// RunResult.
func (l *Loop) finishTurn(ctx context.Context, st *runState, turnID string, reason domain.TerminationReason) (RunResult, error) {
	if err := l.append(ctx, st, domain.ActorSystem, domain.TurnFinished{
		TurnID:   turnID,
		Reason:   reason,
		Usage:    st.usage,
		CostUSD:  st.cost,
		NumTurns: st.numTurns,
	}); err != nil {
		return RunResult{}, err
	}
	if reason.IsError() {
		l.metrics.RecordRunError(string(reason))
	}
	return l.result(st, reason), nil
}

// result assembles the RunResult from the run state.
func (l *Loop) result(st *runState, reason domain.TerminationReason) RunResult {
	return RunResult{Reason: reason, Usage: st.usage, CostUSD: st.cost, NumTurns: st.numTurns}
}

// append commits one event to the session, advancing st.headSeq from the
// returned envelope so the next append uses the correct optimistic version. It
// mints a fresh per-append request id via the injected generator (NFR-TEST-01).
func (l *Loop) append(ctx context.Context, st *runState, actor domain.Actor, ev domain.Event) error {
	reqID := l.deps.IDs.NewRequestID().String()
	envs, err := l.deps.EventLog.Append(ctx, st.sessionID, st.headSeq, st.leaseEpoch, reqID,
		app.AppendInput{Event: ev, Actor: actor})
	if err != nil {
		return fmt.Errorf("agent: append %s: %w", ev.EventType(), err)
	}
	if n := len(envs); n > 0 {
		st.headSeq = envs[n-1].Seq
	}
	return nil
}

// computeCost computes the USD cost for usage via the injected CostFunc,
// treating a nil func or an error as zero cost. It is the BEST-EFFORT path,
// used only where the run is already ending or being recovered (the
// stream-failure abort and resume adjudication) — for a live turn that decides
// whether the run keeps spending, use [Loop.priceTurnUsage] instead.
func (l *Loop) computeCost(u llm.Usage) float64 {
	if l.deps.CostFunc == nil {
		return 0
	}
	c, err := l.deps.CostFunc(l.cfg.Model, u)
	if err != nil {
		return 0
	}
	return c
}

// priceTurnUsage prices a live turn for budget enforcement. With the budget
// cap ENABLED (MaxBudgetUSD > 0) a CostFunc failure is fatal for the run:
// continuing would compare the cap against an under-counted total, silently
// disarming it (see the [CostFunc] contract). With the cap disabled, cost is
// best-effort observability and an unknown price degrades to zero. A nil
// CostFunc always yields zero — wiring no pricing at all is an explicit
// deployment decision, preserved as-is.
func (l *Loop) priceTurnUsage(u llm.Usage) (float64, error) {
	if l.deps.CostFunc == nil {
		return 0, nil
	}
	c, err := l.deps.CostFunc(l.cfg.Model, u)
	if err != nil {
		if l.cfg.MaxBudgetUSD > 0 {
			return 0, fmt.Errorf("agent: budget cap set but turn cost unknown: %w", err)
		}
		return 0, nil
	}
	return c, nil
}

// hasContent reports whether a message carries any content parts (so an empty
// RunInput.UserMessage appends nothing).
func hasContent(m llm.Message) bool { return len(m.Content) > 0 }

// addUsage returns the element-wise sum of two usage snapshots.
func addUsage(a, b llm.Usage) llm.Usage {
	return llm.Usage{
		InputTokens:      a.InputTokens + b.InputTokens,
		OutputTokens:     a.OutputTokens + b.OutputTokens,
		CacheReadTokens:  a.CacheReadTokens + b.CacheReadTokens,
		CacheWriteTokens: a.CacheWriteTokens + b.CacheWriteTokens,
		ReasoningTokens:  a.ReasoningTokens + b.ReasoningTokens,
	}
}

// readOnlyLimit returns the bounded read-only worker-pool size (architecture
// §9.2): the configured value, else min(4, GOMAXPROCS).
func (l *Loop) readOnlyLimit() int {
	if l.cfg.ReadOnlyConcurrency > 0 {
		return l.cfg.ReadOnlyConcurrency
	}
	n := runtime.GOMAXPROCS(0)
	if n > 4 {
		n = 4
	}
	if n < 1 {
		n = 1
	}
	return n
}

// deriveIdempotencyKey computes the log-derived tool-execution idempotency key
// hash(session_id, seq_of_ToolCall) (ADR-0012; architecture §7.2). It is a pure
// function of (session_id, seq) so any orchestrator replaying the log
// reconstructs the same key — it is NEVER a fresh id from the IDGenerator.
func deriveIdempotencyKey(sessionID string, seq int64) string {
	h := sha256.New()
	_, _ = h.Write([]byte(sessionID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.FormatInt(seq, 10)))
	return hex.EncodeToString(h.Sum(nil))
}

// compileOutputSchema compiles the run's structured-output schema once, or
// returns a zero Compiled and false when no schema is configured.
func (l *Loop) compileOutputSchema() (jsonschema.Compiled, bool, error) {
	if len(l.cfg.OutputSchema) == 0 {
		return jsonschema.Compiled{}, false, nil
	}
	c, err := jsonschema.Compile(l.cfg.OutputSchema)
	if err != nil {
		return jsonschema.Compiled{}, false, fmt.Errorf("agent: compile output schema: %w", err)
	}
	return c, true, nil
}

// errAssemble lets callers detect an assembly error that should fail the turn
// (kept distinct from a provider error so the loop can branch).
var errAssemble = errors.New("agent: assemble turn")
