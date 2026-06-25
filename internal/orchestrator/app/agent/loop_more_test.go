package agent_test

// This file extends loop_test.go with the core-loop ERROR paths and the
// defaulting branches the happy-path battery does not reach:
//
//   - NewLoop nil Sink/Metrics defaults (noop implementations must absorb every
//     callback without panicking).
//   - Config.Mode defaulting ("" → policy.ModeDefault) and pass-through.
//   - Config.ReadOnlyConcurrency overriding the min(4,GOMAXPROCS) default.
//   - gateCall failure paths: hook runner error, policy engine error, approval
//     gate error (the approval-timeout shape).
//   - Event-log append failures at EVERY append site (table-driven), so an
//     unreachable log always surfaces as a run error, never a silent drop.
//   - LoadSession/Load failures (run start, recovery load, window build).
//   - Pause continuation: ProviderRaw echo on the SAME turn, and budget
//     exhaustion aborting the turn (architecture §11.1).
//   - Truncated provider streams (Assemble error → TurnAborted).
//   - Tool dispatch failures: stream-start error (serial and parallel paths),
//     mid-stream recv error, an empty stream (no terminal result), and progress
//     events preceding the result.
//   - ListTools failure: fail-safe classification, no tool defs, no crash.
//
// All tests reuse the harness and fakes from loop_test.go; no network, no
// subprocess, injected clock/ids only (NFR-TEST-01/02).

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/apptest"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
	"github.com/xd1lab/harness-ai/internal/platform/llm/llmtest"
)

// ---------------------------------------------------------------------------
// NewLoop defaulting — nil Sink/Metrics must yield zero-behavior defaults.
// ---------------------------------------------------------------------------

// TestNewLoop_NilSinkAndMetricsDefaultToNoop runs a turn with thinking and text
// deltas through a Loop constructed WITHOUT a sink or metrics recorder: the
// noop defaults must absorb every forwarded delta without panicking.
func TestNewLoop_NilSinkAndMetricsDefaultToNoop(t *testing.T) {
	h := newHarness(t)
	lp := agent.NewLoop(agent.Deps{
		EventLog: h.eventlog, Model: h.model, Tools: h.tools, Approvals: h.gate,
		Hooks: h.hooks, Policy: h.pol, Clock: h.clk, IDs: h.ids,
		Sink: nil, Metrics: nil, // exercise the noop defaults
	}, defaultConfig())

	h.model.AddStream([]llm.StreamEvent{
		{ThinkingDelta: &llm.ThinkingDelta{Text: "pondering"}},
		{TextDelta: &llm.TextDelta{Text: "answer"}},
		{Done: &llm.Done{StopReason: llm.StopEnd}},
	}, nil)

	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-nilsink", UserMessage: userMsg("hi")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)
}

// TestNewLoop_NilMetricsAbsorbsErrorAndDoomLoop drives both metrics callbacks
// (RecordDoomLoop via a threshold-1 repeat, RecordRunError via the max-turns
// cap) through the noop recorder.
func TestNewLoop_NilMetricsAbsorbsErrorAndDoomLoop(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.MaxTurns = 1
	cfg.DoomLoopThreshold = 1
	lp := agent.NewLoop(agent.Deps{
		EventLog: h.eventlog, Model: h.model, Tools: h.tools, Approvals: h.gate,
		Hooks: h.hooks, Policy: h.pol, Clock: h.clk, IDs: h.ids,
		Sink: nil, Metrics: nil,
	}, cfg)

	h.tools.SetTools([]app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}})
	h.model.AddStream(toolCallStream("c1", "read", map[string]any{"p": "/x"}), nil)
	h.tools.AddSuccessfulExecution("ok")
	h.pol.AddAllow("a", "")

	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-nilmet", UserMessage: userMsg("go")})
	require.NoError(t, err)
	assert.Equal(t, domain.ErrorMaxTurns, res.Reason)
}

// ---------------------------------------------------------------------------
// Mode defaulting and pass-through into the policy pipeline.
// ---------------------------------------------------------------------------

// TestRun_ModeThreadedIntoPolicy asserts that the zero Config.Mode reaches the
// policy engine as policy.ModeDefault and that an explicit mode (plan) is
// passed through unchanged.
func TestRun_ModeThreadedIntoPolicy(t *testing.T) {
	cases := []struct {
		name    string
		cfgMode policy.Mode
		want    policy.Mode
	}{
		{name: "zero value defaults", cfgMode: "", want: policy.ModeDefault},
		{name: "plan passes through", cfgMode: policy.ModePlan, want: policy.ModePlan},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			cfg := defaultConfig()
			cfg.Mode = tc.cfgMode

			h.tools.SetTools([]app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}})
			h.model.AddStream(toolCallStream("c1", "read", map[string]any{"p": "/x"}), nil)
			h.model.AddStream(textStream("done"), nil)
			h.tools.AddSuccessfulExecution("ok")
			h.pol.AddAllow("a", "")

			res, err := h.run(t, cfg, "sess-mode", "go")
			require.NoError(t, err)
			assert.Equal(t, domain.Success, res.Reason)
			assert.Equal(t, tc.want, h.pol.LastInput().Mode)
		})
	}
}

// ---------------------------------------------------------------------------
// Configured read-only concurrency bound.
// ---------------------------------------------------------------------------

// TestRun_ReadOnlyConcurrencyConfiguredBound asserts that an explicit
// ReadOnlyConcurrency=1 serializes even read-only calls (the configured value
// overrides the min(4,GOMAXPROCS) default; architecture §9.2).
func TestRun_ReadOnlyConcurrencyConfiguredBound(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.ReadOnlyConcurrency = 1

	rec := newConcurrencyRuntime()
	rec.tools = []app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}}
	lp := agent.NewLoop(agent.Deps{
		EventLog: h.eventlog, Model: h.model, Tools: rec, Approvals: h.gate,
		Hooks: h.hooks, Policy: h.pol, Clock: h.clk, IDs: h.ids, Sink: h.sink, Metrics: h.metrics,
	}, cfg)

	h.model.AddStream(multiToolCallStream(
		llm.ToolCall{ID: "r1", Name: "read", Args: map[string]any{"p": "a"}},
		llm.ToolCall{ID: "r2", Name: "read", Args: map[string]any{"p": "b"}},
	), nil)
	h.model.AddStream(textStream("done"), nil)
	h.pol.AddAllow("a", "")
	h.pol.AddAllow("a", "")

	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-rolimit", UserMessage: userMsg("read")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)
	assert.Equal(t, 2, rec.callCount())
	assert.Equal(t, int32(1), rec.maxConcurrent.Load(),
		"ReadOnlyConcurrency=1 must serialize read-only dispatch")
}

// ---------------------------------------------------------------------------
// gateCall failure paths: hook error, policy error, approval-gate error.
// ---------------------------------------------------------------------------

// erroringHooks is an app.HookRunner whose mechanism itself fails (distinct
// from a clean block).
type erroringHooks struct{ err error }

func (h erroringHooks) Run(context.Context, app.HookInput) (app.HookDecision, error) {
	return app.HookDecision{}, h.err
}

// erroringGate is an app.ApprovalGate whose Request fails immediately — the
// deterministic stand-in for an approval that timed out / was torn down.
type erroringGate struct{ err error }

func (g erroringGate) Request(context.Context, app.ApprovalRequest) (domain.AskResolution, error) {
	return domain.AskUnresolved, g.err
}

func (g erroringGate) Resolve(context.Context, string, string, domain.AskResolution) error {
	return errors.New("erroringGate: nothing pending")
}

// TestRun_HookMechanismErrorFailsRun asserts a hook-runner failure (not a
// block) surfaces as a run error, with no PermissionDecided recorded.
func TestRun_HookMechanismErrorFailsRun(t *testing.T) {
	h := newHarness(t)
	lp := agent.NewLoop(agent.Deps{
		EventLog: h.eventlog, Model: h.model, Tools: h.tools, Approvals: h.gate,
		Hooks: erroringHooks{err: errors.New("hook binary missing")}, Policy: h.pol,
		Clock: h.clk, IDs: h.ids, Sink: h.sink, Metrics: h.metrics,
	}, defaultConfig())

	h.tools.SetTools([]app.ToolDescriptor{{Name: "bash", SideEffect: domain.SideEffectMutating}})
	h.model.AddStream(toolCallStream("c1", "bash", map[string]any{"cmd": "x"}), nil)

	_, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-hookerr", UserMessage: userMsg("go")})
	require.Error(t, err)
	assert.ErrorContains(t, err, "PreToolUse hook")
	assert.Empty(t, payloadsOf[domain.PermissionDecided](h, "sess-hookerr"),
		"a mechanism failure must not record a decision")
}

// TestRun_PolicyEngineErrorFailsRun asserts a policy Evaluate error surfaces as
// a run error and the tool is never dispatched.
func TestRun_PolicyEngineErrorFailsRun(t *testing.T) {
	h := newHarness(t)
	h.tools.SetTools([]app.ToolDescriptor{{Name: "bash", SideEffect: domain.SideEffectMutating}})
	h.model.AddStream(toolCallStream("c1", "bash", map[string]any{"cmd": "x"}), nil)
	h.pol.AddResult(policy.Result{}, errors.New("engine down"))

	_, err := h.run(t, defaultConfig(), "sess-polerr", "go")
	require.Error(t, err)
	assert.ErrorContains(t, err, "policy evaluate")
	assert.Equal(t, 0, len(h.tools.Calls()), "tool must not dispatch when policy evaluation fails")
}

// TestRun_ApprovalGateErrorFailsRun asserts an approval-gate failure (the
// timeout/teardown shape) propagates as a run error and records NO
// PermissionDecided — the open turn is adjudicated by the abort path.
func TestRun_ApprovalGateErrorFailsRun(t *testing.T) {
	h := newHarness(t)
	lp := agent.NewLoop(agent.Deps{
		EventLog: h.eventlog, Model: h.model, Tools: h.tools,
		Approvals: erroringGate{err: context.DeadlineExceeded},
		Hooks:     h.hooks, Policy: h.pol, Clock: h.clk, IDs: h.ids, Sink: h.sink, Metrics: h.metrics,
	}, defaultConfig())

	h.tools.SetTools([]app.ToolDescriptor{{Name: "write", SideEffect: domain.SideEffectMutating}})
	h.model.AddStream(toolCallStream("c1", "write", map[string]any{"p": "f"}), nil)
	h.pol.AddAsk("ask-rule", "needs human")

	_, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-gateerr", UserMessage: userMsg("go")})
	require.Error(t, err)
	assert.ErrorContains(t, err, "approval request")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Empty(t, payloadsOf[domain.PermissionDecided](h, "sess-gateerr"))
	assert.Equal(t, 0, len(h.tools.Calls()))
}

// ---------------------------------------------------------------------------
// Append failures at every append site (table-driven).
// ---------------------------------------------------------------------------

// TestRun_AppendFailuresSurfaceAsErrors fails the Nth event-log append and
// asserts the run surfaces an error naming the event type being appended. The
// loop wraps every append as "agent: append <EventType>", so the assertion
// pins WHICH append site failed.
func TestRun_AppendFailuresSurfaceAsErrors(t *testing.T) {
	const sessID = "sess-apf"
	errScripted := errors.New("event log unreachable (scripted)")

	// readToolTurn scripts one read-tool round-trip (descriptor + stream + allow).
	readToolTurn := func(h *harness, withExec bool) {
		h.tools.SetTools([]app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}})
		h.model.AddStream(toolCallStream("c1", "read", map[string]any{"p": "/x"}), nil)
		h.pol.AddAllow("a", "")
		if withExec {
			h.tools.AddSuccessfulExecution("ok")
		}
	}

	cases := []struct {
		name string
		// nilsBefore is how many appends succeed before the scripted failure.
		nilsBefore int
		wantErr    string
		emptyUser  bool
		setup      func(h *harness, cfg *agent.Config)
	}{
		{
			name: "user message", nilsBefore: 0, wantErr: "append MessageAppended",
			setup: func(_ *harness, _ *agent.Config) {}, // model never reached
		},
		{
			name: "turn started", nilsBefore: 1, wantErr: "append TurnStarted",
			setup: func(h *harness, _ *agent.Config) { h.model.AddStream(textStream("hi"), nil) },
		},
		{
			name: "assistant message", nilsBefore: 2, wantErr: "append AssistantMessage",
			setup: func(h *harness, _ *agent.Config) { h.model.AddStream(textStream("hi"), nil) },
		},
		{
			name: "turn finished", nilsBefore: 3, wantErr: "append TurnFinished",
			setup: func(h *harness, _ *agent.Config) { h.model.AddStream(textStream("hi"), nil) },
		},
		{
			name: "turn aborted after stream error", nilsBefore: 2, wantErr: "append TurnAborted",
			setup: func(h *harness, _ *agent.Config) {
				h.model.AddStream(nil, &llm.ProviderError{Kind: llm.ErrServer})
			},
		},
		{
			name: "permission decided on hook block", nilsBefore: 3, wantErr: "append PermissionDecided",
			setup: func(h *harness, _ *agent.Config) {
				h.tools.SetTools([]app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}})
				h.model.AddStream(toolCallStream("c1", "read", map[string]any{"p": "/x"}), nil)
				h.hooks.AddDecision(false, "blocked")
			},
		},
		{
			name: "permission decided on deny", nilsBefore: 3, wantErr: "append PermissionDecided",
			setup: func(h *harness, _ *agent.Config) {
				h.tools.SetTools([]app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}})
				h.model.AddStream(toolCallStream("c1", "read", map[string]any{"p": "/x"}), nil)
				h.pol.AddDeny("d", "denied")
			},
		},
		{
			name: "permission decided on allow", nilsBefore: 3, wantErr: "append PermissionDecided",
			setup: func(h *harness, _ *agent.Config) { readToolTurn(h, false) },
		},
		{
			name: "permission decided on resolved ask", nilsBefore: 3, wantErr: "append PermissionDecided",
			setup: func(h *harness, _ *agent.Config) {
				h.tools.SetTools([]app.ToolDescriptor{{Name: "write", SideEffect: domain.SideEffectMutating}})
				h.model.AddStream(toolCallStream("c1", "write", map[string]any{"p": "f"}), nil)
				h.pol.AddAsk("ask", "needs human")
				go func() {
					_ = h.gate.Resolve(context.Background(), sessID, "c1", domain.AskAllowed)
				}()
			},
		},
		{
			name: "tool execution started", nilsBefore: 4, wantErr: "append ToolExecutionStarted",
			setup: func(h *harness, _ *agent.Config) { readToolTurn(h, false) },
		},
		{
			name: "tool result", nilsBefore: 5, wantErr: "append ToolResult",
			setup: func(h *harness, _ *agent.Config) { readToolTurn(h, true) },
		},
		{
			name: "tool feedback message", nilsBefore: 6, wantErr: "append MessageAppended",
			setup: func(h *harness, _ *agent.Config) { readToolTurn(h, true) },
		},
		{
			// terminate() opens a fresh turn boundary for a between-turns cap; its
			// TurnStarted append failing must surface, not be swallowed.
			name: "terminate turn started", nilsBefore: 6, wantErr: "append TurnStarted",
			emptyUser: true,
			setup: func(h *harness, cfg *agent.Config) {
				cfg.MaxTurns = 1
				readToolTurn(h, true)
			},
		},
		{
			name: "structured retry message", nilsBefore: 3, wantErr: "append MessageAppended",
			setup: func(h *harness, cfg *agent.Config) {
				cfg.OutputSchema = json.RawMessage(`{"type":"object","required":["answer"]}`)
				h.model.AddStream(textStream("not json"), nil)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			cfg := defaultConfig()
			tc.setup(h, &cfg)

			errs := make([]error, tc.nilsBefore, tc.nilsBefore+1)
			errs = append(errs, errScripted)
			h.eventlog.AppendErrs = errs

			in := agent.RunInput{SessionID: sessID}
			if !tc.emptyUser {
				in.UserMessage = userMsg("go")
			}
			_, err := h.loop(cfg).Run(context.Background(), in)
			require.Error(t, err)
			assert.ErrorContains(t, err, tc.wantErr)
			assert.ErrorIs(t, err, errScripted)
		})
	}
}

// ---------------------------------------------------------------------------
// LoadSession / Load failures.
// ---------------------------------------------------------------------------

// failingLog wraps the in-memory fake log with injectable read failures.
type failingLog struct {
	*apptest.FakeEventLog
	loadSessionErr error
	loadErrOnCall  int // 1-based Load call number that fails; 0 = never
	loadErr        error
	loadCalls      int
}

func (f *failingLog) LoadSession(ctx context.Context, id string) (domain.Session, error) {
	if f.loadSessionErr != nil {
		return domain.Session{}, f.loadSessionErr
	}
	return f.FakeEventLog.LoadSession(ctx, id)
}

func (f *failingLog) Load(ctx context.Context, id string, fromSeq int64) ([]domain.EventEnvelope, error) {
	f.loadCalls++
	if f.loadErrOnCall != 0 && f.loadCalls == f.loadErrOnCall {
		return nil, f.loadErr
	}
	return f.FakeEventLog.Load(ctx, id, fromSeq)
}

// TestRun_EventLogReadFailures asserts each read site (session load, recovery
// load, window build) wraps and surfaces its failure.
func TestRun_EventLogReadFailures(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(fl *failingLog)
		wantErr string
	}{
		{
			name:    "load session fails",
			mutate:  func(fl *failingLog) { fl.loadSessionErr = errors.New("db gone") },
			wantErr: "load session",
		},
		{
			name:    "recovery load fails",
			mutate:  func(fl *failingLog) { fl.loadErrOnCall, fl.loadErr = 1, errors.New("db gone") },
			wantErr: "load for recovery",
		},
		{
			name:    "window load fails",
			mutate:  func(fl *failingLog) { fl.loadErrOnCall, fl.loadErr = 2, errors.New("db gone") },
			wantErr: "load window",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			fl := &failingLog{FakeEventLog: h.eventlog}
			tc.mutate(fl)
			lp := agent.NewLoop(agent.Deps{
				EventLog: fl, Model: h.model, Tools: h.tools, Approvals: h.gate,
				Hooks: h.hooks, Policy: h.pol, Clock: h.clk, IDs: h.ids, Sink: h.sink, Metrics: h.metrics,
			}, defaultConfig())

			_, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-readfail", UserMessage: userMsg("go")})
			require.Error(t, err)
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// TestRun_ResumeAbortAppendFailureSurfaces asserts that when adjudicating an
// open turn on resume, a failing TurnAborted append surfaces as a run error
// (the partial turn must never be silently dropped).
func TestRun_ResumeAbortAppendFailureSurfaces(t *testing.T) {
	h := newHarness(t)
	seedOpenTurn(h, "sess-resumefail")
	// Seed appends are done; fail the FIRST run-time append (the TurnAborted).
	h.eventlog.AppendErrs = []error{errors.New("log down")}

	_, err := h.run(t, defaultConfig(), "sess-resumefail", "continue")
	require.Error(t, err)
	assert.ErrorContains(t, err, "append TurnAborted")
}

// ---------------------------------------------------------------------------
// Cancellation between turns.
// ---------------------------------------------------------------------------

// TestRun_CancelledContextAbortsBeforeFirstTurn asserts a context cancelled
// before the first turn yields error_during_execution without ever starting a
// turn (FR-LOOP-03: prompt interrupt exit).
func TestRun_CancelledContextAbortsBeforeFirstTurn(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := h.loop(defaultConfig()).Run(ctx, agent.RunInput{SessionID: "sess-cancel", UserMessage: userMsg("go")})
	require.NoError(t, err)
	assert.Equal(t, domain.ErrorDuringExecution, res.Reason)
	assert.Equal(t, 0, res.NumTurns)
	assert.Empty(t, payloadsOf[domain.TurnStarted](h, "sess-cancel"),
		"no turn may start under a cancelled context")
}

// ---------------------------------------------------------------------------
// Pause continuation (architecture §11.1; FR-MODEL-04 AC-1).
// ---------------------------------------------------------------------------

// TestRun_PauseContinuationEchoesProviderRawSameTurn asserts a Pause→Done
// sequence is ONE turn: the loop re-issues the request echoing ProviderRaw
// byte-faithfully, appends a single AssistantMessage/TurnFinished pair, and
// the client saw the deltas of both stream legs.
func TestRun_PauseContinuationEchoesProviderRawSameTurn(t *testing.T) {
	h := newHarness(t)
	cont := json.RawMessage(`{"cursor":"abc"}`)
	h.model.AddStream([]llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: "part1"}},
		{Done: &llm.Done{StopReason: llm.Pause, ProviderRaw: cont}},
	}, nil)
	h.model.AddStream(textStream("part2"), nil)

	res, err := h.run(t, defaultConfig(), "sess-pause", "go")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)
	assert.Equal(t, 1, res.NumTurns, "Pause then Done is ONE turn (FR-MODEL-04 AC-1)")

	// Exactly one TurnStarted / AssistantMessage / TurnFinished.
	assert.Len(t, payloadsOf[domain.TurnStarted](h, "sess-pause"), 1)
	assert.Len(t, payloadsOf[domain.AssistantMessage](h, "sess-pause"), 1)
	assert.Len(t, payloadsOf[domain.TurnFinished](h, "sess-pause"), 1)

	// The continuation request echoed ProviderRaw byte-faithfully.
	calls := h.model.Calls()
	var streamReqs []llm.Request
	for _, c := range calls {
		if c.Method == "Stream" {
			streamReqs = append(streamReqs, c.Req)
		}
	}
	require.Len(t, streamReqs, 2)
	assert.Nil(t, streamReqs[0].ProviderRaw, "fresh turn carries no continuation blob")
	assert.Equal(t, cont, streamReqs[1].ProviderRaw)

	// The client sink saw both legs' deltas.
	assert.Equal(t, "part1part2", h.sink.textJoined())
}

// TestRun_PauseBudgetExhaustionAbortsTurn asserts a provider that pauses
// forever exhausts the in-turn continuation budget and the turn aborts with
// error_during_execution instead of spinning (architecture §11.1).
func TestRun_PauseBudgetExhaustionAbortsTurn(t *testing.T) {
	h := newHarness(t)
	// maxPauseContinuations is 16 (unexported const in turn.go); script exactly
	// that many Pause legs so the budget exhausts deterministically.
	for i := 0; i < 16; i++ {
		h.model.AddStream([]llm.StreamEvent{
			{Done: &llm.Done{StopReason: llm.Pause, ProviderRaw: json.RawMessage(`{"again":true}`)}},
		}, nil)
	}

	res, err := h.run(t, defaultConfig(), "sess-pausespin", "go")
	require.NoError(t, err)
	assert.Equal(t, domain.ErrorDuringExecution, res.Reason)

	aborts := payloadsOf[domain.TurnAborted](h, "sess-pausespin")
	require.Len(t, aborts, 1)
	assert.Equal(t, domain.ErrorDuringExecution, aborts[0].Reason)
	assert.Equal(t, 1, h.metrics.errorCount("error_during_execution"))
}

// TestRun_TruncatedStreamAbortsTurn asserts a stream that ends without a
// terminal Done (provider truncation) aborts the turn with
// error_during_execution rather than fabricating a terminal message.
func TestRun_TruncatedStreamAbortsTurn(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream([]llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: "cut off mid-"}},
		// no Done: the fake reader EOFs here.
	}, nil)

	res, err := h.run(t, defaultConfig(), "sess-trunc", "go")
	require.NoError(t, err)
	assert.Equal(t, domain.ErrorDuringExecution, res.Reason)
	require.Len(t, payloadsOf[domain.TurnAborted](h, "sess-trunc"), 1)
}

// ---------------------------------------------------------------------------
// Structured output: empty-text response retries; invalid schema fails the run.
// ---------------------------------------------------------------------------

// TestRun_StructuredOutputEmptyTextRetries asserts a final turn with NO text
// fails structured validation (nothing to parse) and triggers the corrective
// retry, which then succeeds.
func TestRun_StructuredOutputEmptyTextRetries(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.OutputSchema = json.RawMessage(`{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}`)

	// Attempt 1: terminal Done with no text at all.
	h.model.AddStream([]llm.StreamEvent{{Done: &llm.Done{StopReason: llm.StopEnd}}}, nil)
	// Attempt 2: schema-valid.
	h.model.AddStream(textStream(`{"answer":"42"}`), nil)

	res, err := h.run(t, cfg, "sess-sotext", "structured please")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)
	assert.Equal(t, 2, res.NumTurns)

	// The corrective instruction was appended between the attempts.
	msgs := payloadsOf[domain.MessageAppended](h, "sess-sotext")
	require.Len(t, msgs, 2, "user message + corrective retry message")
	require.NotEmpty(t, msgs[1].Message.Content)
	require.NotNil(t, msgs[1].Message.Content[0].Text)
	assert.Contains(t, msgs[1].Message.Content[0].Text.Text, "JSON schema")
}

// TestRun_InvalidOutputSchemaFailsRun asserts an unparseable OutputSchema is an
// infrastructural error surfaced from Run, not a silent free-form fallback.
func TestRun_InvalidOutputSchemaFailsRun(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.OutputSchema = []byte(`{"type":`) // malformed JSON schema

	h.model.AddStream(textStream("anything"), nil)

	_, err := h.run(t, cfg, "sess-badschema", "go")
	require.Error(t, err)
	assert.ErrorContains(t, err, "compile output schema")
}

// ---------------------------------------------------------------------------
// CostFunc error semantics.
// ---------------------------------------------------------------------------
// Pinned in loop_budget_pricing_test.go: with a budget cap SET an unpriceable
// turn fails the run closed (TestRun_BudgetCapUnknownPriceFailsClosed); with
// the cap disabled the error degrades to zero cost
// (TestRun_NoBudgetCapUnknownPriceIsBestEffort). The former
// TestRun_CostFuncErrorMeansZeroCost pinned the pre-hardening behavior
// (cap set + erroring CostFunc => success at $0), which silently disarmed the
// cap — superseded, not just moved.

// ---------------------------------------------------------------------------
// Tool dispatch failures and stream shapes.
// ---------------------------------------------------------------------------

// emptyToolStream yields no events at all (immediate EOF), modeling a runtime
// that closed the stream without a terminal result.
type emptyToolStream struct{}

func (emptyToolStream) Recv() (app.ToolEvent, error) { return app.ToolEvent{}, io.EOF }
func (emptyToolStream) Close() error                 { return nil }

// erroringToolStream yields one progress event then a transport error.
type erroringToolStream struct {
	err  error
	sent bool
}

func (s *erroringToolStream) Recv() (app.ToolEvent, error) {
	if !s.sent {
		s.sent = true
		return app.ToolEvent{Progress: &app.ToolProgress{Output: "partial..."}}, nil
	}
	return app.ToolEvent{}, s.err
}
func (s *erroringToolStream) Close() error { return nil }

// TestRun_ToolStreamStartErrorFailsRun asserts an ExecuteTool transport error
// on the SERIALIZED (mutating) path surfaces as a run error.
func TestRun_ToolStreamStartErrorFailsRun(t *testing.T) {
	h := newHarness(t)
	h.tools.SetTools([]app.ToolDescriptor{{Name: "write", SideEffect: domain.SideEffectMutating}})
	h.model.AddStream(toolCallStream("c1", "write", map[string]any{"p": "f"}), nil)
	h.pol.AddAllow("a", "")
	h.tools.AddExecution(nil, errors.New("dial refused"))

	_, err := h.run(t, defaultConfig(), "sess-execerr", "go")
	require.Error(t, err)
	assert.ErrorContains(t, err, "agent: tool stream")
	assert.ErrorContains(t, err, `execute "write"`)
}

// TestRun_ReadOnlyWorkerErrorFailsRun asserts an ExecuteTool transport error on
// the PARALLEL (read-only) path also surfaces (via the errgroup wait).
func TestRun_ReadOnlyWorkerErrorFailsRun(t *testing.T) {
	h := newHarness(t)
	h.tools.SetTools([]app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}})
	h.model.AddStream(toolCallStream("c1", "read", map[string]any{"p": "/x"}), nil)
	h.pol.AddAllow("a", "")
	h.tools.AddExecution(nil, errors.New("dial refused"))

	_, err := h.run(t, defaultConfig(), "sess-roerr", "go")
	require.Error(t, err)
	assert.ErrorContains(t, err, "agent: tool stream")
}

// TestRun_ToolStreamRecvErrorFailsRun asserts a mid-stream transport error
// (after progress) surfaces as a run error distinct from a tool-reported
// is_error result.
func TestRun_ToolStreamRecvErrorFailsRun(t *testing.T) {
	h := newHarness(t)
	h.tools.SetTools([]app.ToolDescriptor{{Name: "write", SideEffect: domain.SideEffectMutating}})
	h.model.AddStream(toolCallStream("c1", "write", map[string]any{"p": "f"}), nil)
	h.pol.AddAllow("a", "")
	h.tools.AddExecution(&erroringToolStream{err: errors.New("connection reset")}, nil)

	_, err := h.run(t, defaultConfig(), "sess-recverr", "go")
	require.Error(t, err)
	assert.ErrorContains(t, err, `recv "write"`)
}

// TestRun_ToolStreamWithoutResultFeedsErrorObservation asserts a tool stream
// that EOFs without a terminal result is fed back to the model as an error
// observation (FR-TOOL-01: the model adapts; the run does not crash).
func TestRun_ToolStreamWithoutResultFeedsErrorObservation(t *testing.T) {
	h := newHarness(t)
	h.tools.SetTools([]app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}})
	h.model.AddStream(toolCallStream("c1", "read", map[string]any{"p": "/x"}), nil)
	h.model.AddStream(textStream("adapted"), nil)
	h.pol.AddAllow("a", "")
	h.tools.AddExecution(emptyToolStream{}, nil)

	res, err := h.run(t, defaultConfig(), "sess-nores", "go")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	results := payloadsOf[domain.ToolResult](h, "sess-nores")
	require.Len(t, results, 1)
	assert.True(t, results[0].IsError)
	assert.Equal(t, "tool produced no result", results[0].Result)
}

// TestRun_ToolProgressEventsDoNotDisturbResult asserts progress chunks before
// the terminal result are consumed (relayed elsewhere) and the terminal result
// is what reaches the log and the model.
func TestRun_ToolProgressEventsDoNotDisturbResult(t *testing.T) {
	h := newHarness(t)
	h.tools.SetTools([]app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}})
	h.model.AddStream(toolCallStream("c1", "read", map[string]any{"p": "/x"}), nil)
	h.model.AddStream(textStream("done"), nil)
	h.pol.AddAllow("a", "")
	h.tools.AddExecution(apptest.NewFakeToolStream(
		app.ToolResult{Content: "final result"},
		app.ToolProgress{Output: "chunk-1"},
		app.ToolProgress{Output: "chunk-2"},
	), nil)

	res, err := h.run(t, defaultConfig(), "sess-progress", "go")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	results := payloadsOf[domain.ToolResult](h, "sess-progress")
	require.Len(t, results, 1)
	assert.Equal(t, "final result", results[0].Result)
	assert.False(t, results[0].IsError)
}

// ---------------------------------------------------------------------------
// ListTools failure: fail-safe classification everywhere.
// ---------------------------------------------------------------------------

// listErrRuntime delegates execution to the fake runtime but fails ListTools,
// exercising the fail-safe branches in toolClasses, sideEffectLookup, and
// toolDefsFromRuntime.
type listErrRuntime struct {
	inner *apptest.FakeToolRuntime
}

func (r *listErrRuntime) ExecuteTool(ctx context.Context, exec app.ToolExecution) (app.ToolStream, error) {
	return r.inner.ExecuteTool(ctx, exec)
}

func (r *listErrRuntime) ListTools(context.Context, string) ([]app.ToolDescriptor, error) {
	return nil, errors.New("runtime listing unavailable")
}

// TestRun_ListToolsErrorFailsSafe asserts that when the runtime cannot be
// enumerated, the loop still runs: no tool defs are advertised, recovery
// treats unknown executions as mutating, and an unknown tool is classified
// fail-safe (serialized, external) yet still dispatchable when policy allows.
func TestRun_ListToolsErrorFailsSafe(t *testing.T) {
	h := newHarness(t)
	// Seed an open turn so resume adjudication runs sideEffectLookup (which must
	// degrade to a nil lookup on the listing error).
	seedOpenTurn(h, "sess-listerr")

	rt := &listErrRuntime{inner: h.tools}
	lp := agent.NewLoop(agent.Deps{
		EventLog: h.eventlog, Model: h.model, Tools: rt, Approvals: h.gate,
		Hooks: h.hooks, Policy: h.pol, Clock: h.clk, IDs: h.ids, Sink: h.sink, Metrics: h.metrics,
	}, defaultConfig())

	h.model.AddStream(toolCallStream("c1", "mystery", map[string]any{"p": "/x"}), nil)
	h.model.AddStream(textStream("done"), nil)
	h.pol.AddAllow("a", "")
	h.tools.AddSuccessfulExecution("ok")

	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-listerr", UserMessage: userMsg("go")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)
	assert.Equal(t, 1, len(h.tools.Calls()), "an allowed unknown tool still dispatches (serialized)")

	// With ListTools failing and no configured ToolDefs, the model is offered no
	// RUNTIME tools in the request. The in-loop virtual tool todo_write is ALWAYS
	// advertised (ADR-0031, independent of the runtime registry); spawn_subagent is
	// absent because no SubAgentPort is wired. So the only def is todo_write.
	for _, c := range h.model.Calls() {
		if c.Method == "Stream" {
			require.Len(t, c.Req.Tools, 1, "only the always-on todo_write virtual tool is offered")
			assert.Equal(t, "todo_write", c.Req.Tools[0].Name)
		}
	}

	// The unknown tool reached policy with the fail-safe zero classification
	// (unset side effect / egress, treated as mutating/external downstream).
	in := h.pol.LastInput()
	assert.Equal(t, domain.SideEffect(""), in.SideEffect)
	assert.Equal(t, domain.EgressClass(""), in.EgressClass)
}

// TestRun_ResumeUnknownToolDefaultsToMutating asserts that an unknown
// in-flight execution whose tool is NOT in the runtime's descriptor list is
// classified mutating (fail-safe; ADR-0014) and therefore never re-dispatched
// on resume.
func TestRun_ResumeUnknownToolDefaultsToMutating(t *testing.T) {
	h := newHarness(t)
	seedUnknownMutatingExec(h, "sess-unkclass")
	// The runtime advertises a DIFFERENT tool, so the seeded "write" execution
	// resolves through the lookup's not-found fail-safe branch.
	h.tools.SetTools([]app.ToolDescriptor{{Name: "other", SideEffect: domain.SideEffectReadOnly}})

	h.model.AddStream(textStream("resumed"), nil)

	res, err := h.run(t, defaultConfig(), "sess-unkclass", "continue")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)
	assert.Equal(t, 0, len(h.tools.Calls()),
		"an unknown tool defaults to mutating and must not be re-dispatched on resume")
}

// TestRun_StructuredOutputDefaultRetryCap asserts a non-positive
// MaxStructuredOutputRetries falls back to DefaultStructuredOutputRetries:
// 1 initial attempt + 3 retries before exhaustion.
func TestRun_StructuredOutputDefaultRetryCap(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.MaxStructuredOutputRetries = 0 // → DefaultStructuredOutputRetries (3)
	cfg.OutputSchema = json.RawMessage(`{"type":"object","required":["answer"]}`)

	for i := 0; i < 4; i++ { // initial + 3 retries
		h.model.AddStream(textStream("still not json"), nil)
	}

	res, err := h.run(t, cfg, "sess-sodefault", "structured")
	require.NoError(t, err)
	assert.Equal(t, domain.ErrorMaxStructuredOutputRetries, res.Reason)

	streamCalls := 0
	for _, c := range h.model.Calls() {
		if c.Method == "Stream" {
			streamCalls++
		}
	}
	assert.Equal(t, 4, streamCalls, "default cap is 1 initial attempt + 3 retries")
}

// ---------------------------------------------------------------------------
// Outcome.String — log/diagnostic rendering.
// ---------------------------------------------------------------------------

// TestOutcomeString pins the diagnostic rendering of every Outcome, including
// the out-of-range fallback.
func TestOutcomeString(t *testing.T) {
	cases := []struct {
		o    agent.Outcome
		want string
	}{
		{agent.OutcomeFinal, "final"},
		{agent.OutcomeNeedsToolExecution, "needs-tool-execution"},
		{agent.OutcomeNeedsContinuation, "needs-continuation"},
		{agent.Outcome(99), "Outcome(99)"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, tc.o.String())
	}
}

// ---------------------------------------------------------------------------
// Assembler partial-message paths.
// ---------------------------------------------------------------------------

// TestAssemble_PartialMessageIncludesPendingToolCalls asserts that on a
// mid-stream failure the partial message preserves thinking, text, AND the
// pending tool calls (with best-effort empty args) so diagnostics see the full
// in-flight shape.
func TestAssemble_PartialMessageIncludesPendingToolCalls(t *testing.T) {
	reader := &erroringReader{
		events: []llm.StreamEvent{
			{ThinkingDelta: &llm.ThinkingDelta{Text: "hmm"}},
			{TextDelta: &llm.TextDelta{Text: "let me check"}},
			{ToolCallDelta: &llm.ToolCallDelta{CallID: "c1", Name: "read", ArgsFragment: json.RawMessage(`{"pa`)}},
		},
		err: &llm.ProviderError{Kind: llm.ErrServer, Raw: errors.New("mid-stream drop")},
	}

	res, err := agent.Assemble(reader)
	require.Error(t, err)

	assert.Equal(t, "hmm", thinkingOf(t, res.Message))
	assert.Equal(t, "let me check", textOf(t, res.Message))
	calls := toolCallsOf(t, res.Message)
	require.Len(t, calls, 1, "pending tool call must appear in the partial message")
	assert.Equal(t, "c1", calls[0].ID)
	assert.Equal(t, "read", calls[0].Name)
	assert.Empty(t, calls[0].Args, "partial args are unparsed: best-effort empty map")
}

// TestAssemble_PathAddressedMalformedFragmentErrors asserts a path-addressed
// fragment that is not valid JSON surfaces ErrMalformedToolArgs when the
// per-path object is assembled (the Gemini-style encoding's malformed case).
func TestAssemble_PathAddressedMalformedFragmentErrors(t *testing.T) {
	reader := llmtest.NewFakeStreamReader(
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{
			CallID: "c1", Name: "edit", ArgsPath: "path", ArgsFragment: json.RawMessage(`{"unterminated`),
		}},
		llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopToolUse}},
	)

	_, err := agent.Assemble(reader)
	require.Error(t, err)
	assert.ErrorIs(t, err, agent.ErrMalformedToolArgs)
}

// TestAssemble_NullArgsTreatedAsEmpty asserts a tool call whose buffered
// arguments are JSON null parses to empty args (a zero-argument call), never a
// nil map or an error.
func TestAssemble_NullArgsTreatedAsEmpty(t *testing.T) {
	reader := llmtest.NewFakeStreamReader(
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: "c1", Name: "ping", ArgsFragment: json.RawMessage(`null`)}},
		llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopToolUse}},
	)

	res, err := agent.Assemble(reader)
	require.NoError(t, err)
	calls := toolCallsOf(t, res.Message)
	require.Len(t, calls, 1)
	require.NotNil(t, calls[0].Args)
	assert.Empty(t, calls[0].Args)
}
