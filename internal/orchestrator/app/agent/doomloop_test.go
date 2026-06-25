package agent_test

// RED tests for FIX 2 (doom-loop termination). Today detectDoomLoop only RECORDS
// A METRIC and has no control-flow effect — a model stuck repeating one identical
// tool call is stopped only by MaxTurns=32. These tests pin AC-2.6 / AC-2.7:
//
//   - a scripted model repeating ONE identical tool call (same Name + same Args)
//     terminates with RunResult.Reason == domain.ErrorDoomLoop strictly BEFORE
//     MaxTurns;
//   - a model that VARIES its tool args each turn does NOT trip ErrorDoomLoop
//     (it instead hits MaxTurns), proving the signature compare is by (name+args).
//
// They reuse the harness + fakes from loop_test.go. domain.ErrorDoomLoop does not
// exist yet, so this file does not compile — the RED proof that the feature is
// absent.

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// TestRun_RepeatedIdenticalToolCallTerminatesDoomLoop asserts a model that emits
// the SAME tool call (same Name + same Args) on every turn trips the doom-loop
// guard and terminates with domain.ErrorDoomLoop BEFORE MaxTurns (AC-2.6).
func TestRun_RepeatedIdenticalToolCallTerminatesDoomLoop(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.MaxTurns = 32
	cfg.DoomLoopThreshold = 3

	h.tools.SetTools([]app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}})

	// Script far more identical turns than the threshold but fewer than MaxTurns:
	// if the doom-loop guard does NOT fire, the run would keep going (and hit
	// MaxTurns or exhaust the script), so reaching ErrorDoomLoop proves the guard.
	const identicalArgs = `same`
	for i := 0; i < 10; i++ {
		h.model.AddStream(toolCallStream("c1", "read", map[string]any{"path": identicalArgs}), nil)
		h.pol.AddAllow("a", "")
		h.tools.AddSuccessfulExecution("ok")
	}

	res, err := h.run(t, cfg, "sess-doomloop", "go")
	require.NoError(t, err)
	assert.Equal(t, domain.ErrorDoomLoop, res.Reason,
		"a model repeating one identical tool call must terminate with ErrorDoomLoop")
	assert.Less(t, res.NumTurns, cfg.MaxTurns,
		"the doom-loop must terminate strictly before MaxTurns")

	// The terminal TurnFinished carries ErrorDoomLoop, and the RED error metric was
	// recorded.
	fins := payloadsOf[domain.TurnFinished](h, "sess-doomloop")
	require.NotEmpty(t, fins)
	assert.Equal(t, domain.ErrorDoomLoop, fins[len(fins)-1].Reason)
	assert.GreaterOrEqual(t, h.metrics.doomLoopCount("read"), 1,
		"the doom-loop metric must still be emitted on the trip")
}

// TestRun_VariedToolCallsDoNotTripDoomLoop asserts a model that VARIES its tool
// args each turn never trips the doom-loop guard: the signature compare is by
// (name+args), so distinct args reset the consecutive-repeat count. The run
// instead hits MaxTurns (AC-2.6 negative case).
func TestRun_VariedToolCallsDoNotTripDoomLoop(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.MaxTurns = 5
	cfg.DoomLoopThreshold = 3

	h.tools.SetTools([]app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}})

	// Each turn varies the args, so no two consecutive batches share a signature.
	for i := 0; i < cfg.MaxTurns; i++ {
		h.model.AddStream(toolCallStream("c1", "read", map[string]any{"path": fmt.Sprintf("/p/%d", i)}), nil)
		h.pol.AddAllow("a", "")
		h.tools.AddSuccessfulExecution("ok")
	}

	res, err := h.run(t, cfg, "sess-varied", "go")
	require.NoError(t, err)
	assert.Equal(t, domain.ErrorMaxTurns, res.Reason,
		"varied tool args must NOT trip the doom-loop; the run hits MaxTurns instead")
	assert.NotEqual(t, domain.ErrorDoomLoop, res.Reason)
}

// TestRun_DoomLoopTripsBeforeDispatchingRepeatedBatch asserts that when the guard
// trips it terminates BEFORE dispatching the repeated batch — once tripped, no
// further tool execution happens (AC-2.3 / AC-2.4: terminate via the existing
// turnTerminal -> finish contract, exactly one TurnFinished, no double-append).
func TestRun_DoomLoopTripsBeforeDispatchingRepeatedBatch(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.MaxTurns = 32
	cfg.DoomLoopThreshold = 3

	h.tools.SetTools([]app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}})
	for i := 0; i < 10; i++ {
		h.model.AddStream(toolCallStream("c1", "read", map[string]any{"path": "same"}), nil)
		h.pol.AddAllow("a", "")
		h.tools.AddSuccessfulExecution("ok")
	}

	res, err := h.run(t, cfg, "sess-doomdispatch", "go")
	require.NoError(t, err)
	require.Equal(t, domain.ErrorDoomLoop, res.Reason)

	// Exactly one terminal TurnFinished (no duplicate/dangling turn from a side
	// l.terminate path).
	assert.Len(t, payloadsOf[domain.TurnFinished](h, "sess-doomdispatch"), 1,
		"doom-loop termination must append exactly one TurnFinished on the current turn")

	// The threshold is 3: the 3rd identical batch trips BEFORE dispatch, so at most
	// 2 batches' tool results were ever executed (the 3rd is not dispatched).
	results := payloadsOf[domain.ToolResult](h, "sess-doomdispatch")
	assert.LessOrEqual(t, len(results), cfg.DoomLoopThreshold-1,
		"the tripping (Nth) batch must NOT be dispatched — no further tool execution once tripped")
}
