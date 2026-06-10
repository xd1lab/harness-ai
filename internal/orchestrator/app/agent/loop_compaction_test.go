package agent_test

// Threshold-triggered compaction wiring (FR-CTX-01): these tests pin the loop's
// integration with the agentctx context manager — the decision (PlanCompaction),
// the PreCompact hook gate (AC-2), the appended CompactionPerformed boundary,
// and the fact that the SAME turn that detected token pressure already
// generates from the reduced window. Written TDD-first against the previously
// dead Deps.Context field (the manager existed but the loop never called it).

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/orchestrator/app"
	"github.com/boltrope/boltrope/internal/orchestrator/app/agent"
	"github.com/boltrope/boltrope/internal/orchestrator/app/agentctx"
	"github.com/boltrope/boltrope/internal/orchestrator/domain"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// scriptedCounter is a deterministic agentctx.TokenCounter: each Count call
// consumes the next scripted entry (count or error). It panics when exhausted
// so an unexpected extra measurement fails the test loudly.
type scriptedCounter struct {
	mu     sync.Mutex
	counts []int
	errs   []error
	calls  int
}

func (c *scriptedCounter) Count(_ context.Context, _ string, _ []llm.Message, _ []llm.ToolDef) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := c.calls
	c.calls++
	if i >= len(c.counts) {
		panic("scriptedCounter: Count queue exhausted")
	}
	var err error
	if i < len(c.errs) {
		err = c.errs[i]
	}
	return c.counts[i], err
}

// compactionManager builds a real agentctx.Manager over the scripted counter
// with a 100-token budget (threshold 80 at the default 0.8 fraction).
func compactionManager(counter *scriptedCounter) *agentctx.Manager {
	return agentctx.NewManager(counter, agentctx.Config{Model: "test-model", MaxContextTokens: 100})
}

// loopWithContext mirrors harness.loop but injects the context manager.
func (h *harness) loopWithContext(cfg agent.Config, mgr *agentctx.Manager) *agent.Loop {
	return agent.NewLoop(agent.Deps{
		EventLog:  h.eventlog,
		Model:     h.model,
		Tools:     h.tools,
		Approvals: h.gate,
		Hooks:     h.hooks,
		Policy:    h.pol,
		Context:   mgr,
		Clock:     h.clk,
		IDs:       h.ids,
		Sink:      h.sink,
		Metrics:   h.metrics,
	}, cfg)
}

// TestRun_CompactionTriggered: a window over the threshold appends exactly one
// CompactionPerformed boundary (after the PreCompact hook allowed it) BEFORE
// the turn's TurnStarted, and the model request for that same turn is the
// reduced window — a single summary message replacing the pre-boundary history.
func TestRun_CompactionTriggered(t *testing.T) {
	h := newHarness(t)
	// PlanCompaction measures twice: current window (1000 >= 80 → compact) and
	// the projected post-boundary window (10).
	counter := &scriptedCounter{counts: []int{1000, 10}}
	h.model.AddStreamEvents(llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "done"}})

	lp := h.loopWithContext(defaultConfig(), compactionManager(counter))
	res, err := lp.Run(context.Background(), agent.RunInput{
		SessionID:   "sess-compact",
		UserMessage: userMsg("a very long task"),
	})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	// Exactly one boundary, carrying the measured before/after counts.
	compactions := payloadsOf[domain.CompactionPerformed](h, "sess-compact")
	require.Len(t, compactions, 1, "exactly one CompactionPerformed boundary")
	assert.Equal(t, 1000, compactions[0].BeforeTokens)
	assert.Equal(t, 10, compactions[0].AfterTokens)
	assert.NotEmpty(t, compactions[0].Reason)

	// The boundary lands after the user message and before the turn it reduced.
	types := h.eventTypes("sess-compact")
	require.GreaterOrEqual(t, len(types), 3)
	assert.Equal(t, domain.EventMessageAppended, types[0], "user message first")
	assert.Equal(t, domain.EventCompactionPerformed, types[1], "boundary before the turn")
	assert.Equal(t, domain.EventTurnStarted, types[2])

	// The PreCompact hook ran (FR-CTX-01 AC-2).
	var sawPreCompact bool
	for _, c := range h.hooks.Calls() {
		if c.In.Event == app.HookPreCompact {
			sawPreCompact = true
			assert.Equal(t, "sess-compact", c.In.SessionID)
		}
	}
	assert.True(t, sawPreCompact, "PreCompact hook must run before compaction")

	// The SAME turn generates from the reduced window: the request's messages
	// collapse to the single default summary (the pre-boundary user message is
	// summarized away; its full content stays in the log).
	calls := h.model.Calls()
	require.NotEmpty(t, calls)
	reqMsgs := calls[len(calls)-1].Req.Messages
	require.Len(t, reqMsgs, 1, "post-boundary window is the summary only")
	require.Len(t, reqMsgs[0].Content, 1)
	require.NotNil(t, reqMsgs[0].Content[0].Text)
	assert.Equal(t, agentctx.DefaultCompactionSummary, reqMsgs[0].Content[0].Text.Text)
}

// TestRun_CompactionVetoedByPreCompactHook: a blocking PreCompact decision
// vetoes the boundary — nothing is appended and the turn generates from the
// full, un-compacted window.
func TestRun_CompactionVetoedByPreCompactHook(t *testing.T) {
	h := newHarness(t)
	counter := &scriptedCounter{counts: []int{1000, 10}} // plan computed, then vetoed
	h.hooks.AddDecision(false, "operator: keep history")
	h.model.AddStreamEvents(llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "done"}})

	lp := h.loopWithContext(defaultConfig(), compactionManager(counter))
	res, err := lp.Run(context.Background(), agent.RunInput{
		SessionID:   "sess-veto",
		UserMessage: userMsg("keep my history"),
	})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	assert.Empty(t, payloadsOf[domain.CompactionPerformed](h, "sess-veto"),
		"a hook block must veto the boundary append")

	// The request still carries the real user message, not a summary.
	calls := h.model.Calls()
	require.NotEmpty(t, calls)
	reqMsgs := calls[len(calls)-1].Req.Messages
	require.Len(t, reqMsgs, 1)
	require.NotNil(t, reqMsgs[0].Content[0].Text)
	assert.Equal(t, "keep my history", reqMsgs[0].Content[0].Text.Text)
}

// TestRun_CompactionCounterErrorSkips: a failing token counter means "cannot
// decide" — the loop proceeds un-compacted and never consults the hook, and the
// run is NOT failed (compaction is an optimization, never worth a run failure).
func TestRun_CompactionCounterErrorSkips(t *testing.T) {
	h := newHarness(t)
	counter := &scriptedCounter{counts: []int{0}, errs: []error{errors.New("gateway down")}}
	h.model.AddStreamEvents(llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "done"}})

	lp := h.loopWithContext(defaultConfig(), compactionManager(counter))
	res, err := lp.Run(context.Background(), agent.RunInput{
		SessionID:   "sess-counter-err",
		UserMessage: userMsg("hello"),
	})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	assert.Empty(t, payloadsOf[domain.CompactionPerformed](h, "sess-counter-err"))
	for _, c := range h.hooks.Calls() {
		assert.NotEqual(t, app.HookPreCompact, c.In.Event,
			"no PreCompact hook when the manager could not decide")
	}
}

// TestRun_CompactionBelowThresholdNoBoundary: a window under the threshold
// appends nothing and runs no PreCompact hook.
func TestRun_CompactionBelowThresholdNoBoundary(t *testing.T) {
	h := newHarness(t)
	counter := &scriptedCounter{counts: []int{10}} // 10 < threshold 80
	h.model.AddStreamEvents(llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "done"}})

	lp := h.loopWithContext(defaultConfig(), compactionManager(counter))
	res, err := lp.Run(context.Background(), agent.RunInput{
		SessionID:   "sess-under",
		UserMessage: userMsg("hello"),
	})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	assert.Empty(t, payloadsOf[domain.CompactionPerformed](h, "sess-under"))
	for _, c := range h.hooks.Calls() {
		assert.NotEqual(t, app.HookPreCompact, c.In.Event)
	}
}

// failCompactionAppendLog fails Append only for a CompactionPerformed payload,
// passing everything else through — so the boundary append is the single point
// of failure under test.
type failCompactionAppendLog struct {
	app.EventLogPort
	err error
}

func (f *failCompactionAppendLog) Append(ctx context.Context, sessionID string, expectedHeadSeq, leaseEpoch int64, requestID string, events ...app.AppendInput) ([]domain.EventEnvelope, error) {
	for _, e := range events {
		if _, ok := e.Event.(domain.CompactionPerformed); ok {
			return nil, f.err
		}
	}
	return f.EventLogPort.Append(ctx, sessionID, expectedHeadSeq, leaseEpoch, requestID, events...)
}

// TestRun_CompactionAppendFailureFailsRun: unlike a counter failure, a FAILED
// BOUNDARY APPEND is an event-log infrastructure error and must fail the run
// (the log is the single source of truth; proceeding after a failed append
// would desynchronize the window from the durable history).
func TestRun_CompactionAppendFailureFailsRun(t *testing.T) {
	h := newHarness(t)
	counter := &scriptedCounter{counts: []int{1000, 10}}
	boom := errors.New("append: db down")

	lp := agent.NewLoop(agent.Deps{
		EventLog:  &failCompactionAppendLog{EventLogPort: h.eventlog, err: boom},
		Model:     h.model,
		Tools:     h.tools,
		Approvals: h.gate,
		Hooks:     h.hooks,
		Policy:    h.pol,
		Context:   compactionManager(counter),
		Clock:     h.clk,
		IDs:       h.ids,
		Sink:      h.sink,
		Metrics:   h.metrics,
	}, defaultConfig())

	_, err := lp.Run(context.Background(), agent.RunInput{
		SessionID:   "sess-append-fail",
		UserMessage: userMsg("hello"),
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
}
