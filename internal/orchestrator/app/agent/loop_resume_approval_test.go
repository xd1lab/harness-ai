package agent_test

// FIX 3 resume (ADR-0032; AC-3.5/3.7/3.8) — ADDITIONAL coverage beyond the TARGET
// happy-path test in approval_suspend_test.go (TestRun_ResumeReRaisesSuspendedApproval).
//
// These cover the two remaining resume branches against the in-memory fakes:
//
//   - DENY on resume: the re-raised ask is resolved AskDenied; the gated tool does
//     NOT execute, a denied observation is recorded, and the loop continues (no
//     silent abort of the suspended turn).
//   - bounded-ctx FALLBACK: nobody answers within ResumeApprovalTimeout, so the
//     loop closes the suspended turn with an EXPLICIT auditable record
//     (PermissionDecided{AskDenied, suspended-approval-abandoned-on-resume} then
//     TurnAborted) instead of blocking forever or aborting silently. The deadline
//     is driven by the injected fake clock, proving it is real and clock-based.
//
// The integration variant (eventstore/approval_event_integration_test.go) proves
// the TARGET re-raise end-to-end over real Postgres with the real approval.Gate and
// its SubscribeApprovals notifier.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// TestRun_ResumeReRaisesSuspendedApproval_Denied proves a human DENY on resume: the
// re-raised ask resolves AskDenied, the gated tool does NOT execute, a denied
// PermissionDecided is recorded, and the loop continues to a terminal turn — the
// suspended turn is NOT silently aborted.
func TestRun_ResumeReRaisesSuspendedApproval_Denied(t *testing.T) {
	h := newHarness(t)
	const sessID = "sess-resume-deny"
	seedSuspendedApproval(h, sessID) // seeds susp-turn / call c1 / tool write

	h.tools.SetTools([]app.ToolDescriptor{{Name: "write", SideEffect: domain.SideEffectMutating}})
	// After the deny, the model returns text-only to end the run; no tool runs.
	h.model.AddStream(textStream("understood"), nil)

	gate := &inspectingGate{resolution: domain.AskDenied}
	lp := agent.NewLoop(agent.Deps{
		EventLog: h.eventlog, Model: h.model, Tools: h.tools, Approvals: gate,
		Hooks: h.hooks, Policy: h.pol, Clock: h.clk, IDs: h.ids, Sink: h.sink, Metrics: h.metrics,
	}, defaultConfig())

	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: sessID})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	var sawDenied bool
	for _, d := range payloadsOf[domain.PermissionDecided](h, sessID) {
		if d.CallID == "c1" && d.Resolved == domain.AskDenied {
			sawDenied = true
		}
	}
	assert.True(t, sawDenied, "a PermissionDecided{Ask,AskDenied} must record the human deny on resume")

	// A denied suspended tool must NOT execute.
	assert.Empty(t, payloadsOf[domain.ToolExecutionStarted](h, sessID),
		"a denied suspended tool must not be dispatched")
}

// TestRun_ResumeSuspendedApproval_TimeoutDurableClose proves the FALLBACK level
// (AC-3.8): when nobody answers the re-raised ask within the bounded
// ResumeApprovalTimeout, the loop closes the suspended turn with an EXPLICIT
// auditable record rather than hanging or silently aborting. The bound is driven by
// the injected fake clock — the timeout fires ONLY when the test advances virtual
// time, proving the deadline is clock-based and mandatory.
func TestRun_ResumeSuspendedApproval_TimeoutDurableClose(t *testing.T) {
	h := newHarness(t)
	const sessID = "sess-resume-timeout"
	seedSuspendedApproval(h, sessID)

	h.tools.SetTools([]app.ToolDescriptor{{Name: "write", SideEffect: domain.SideEffectMutating}})

	cfg := defaultConfig()
	cfg.ResumeApprovalTimeout = 2 * time.Minute

	// The real fake gate blocks until resolved or its ctx is cancelled. Nobody
	// resolves; instead, once the re-raise has registered its pending entry, advance
	// the fake clock past the timeout so the bounded ctx cancels the Request.
	go func() {
		for i := 0; i < 5_000_000; i++ {
			if h.gate.Pending(sessID, "c1") {
				h.clk.Advance(3 * time.Minute)
				return
			}
		}
	}()

	lp := agent.NewLoop(agent.Deps{
		EventLog: h.eventlog, Model: h.model, Tools: h.tools, Approvals: h.gate,
		Hooks: h.hooks, Policy: h.pol, Clock: h.clk, IDs: h.ids, Sink: h.sink, Metrics: h.metrics,
	}, cfg)

	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: sessID})
	require.NoError(t, err)
	assert.Equal(t, domain.ErrorDuringExecution, res.Reason,
		"an abandoned suspended approval closes with the auditable error reason")

	var sawAbandon bool
	for _, d := range payloadsOf[domain.PermissionDecided](h, sessID) {
		if d.CallID == "c1" && d.Resolved == domain.AskDenied && d.Reason == "suspended-approval-abandoned-on-resume" {
			sawAbandon = true
		}
	}
	assert.True(t, sawAbandon, "the abandoned ask must leave an explicit auditable PermissionDecided (not a silent abort)")

	var sawAborted bool
	for _, a := range payloadsOf[domain.TurnAborted](h, sessID) {
		if a.TurnID == "susp-turn" {
			sawAborted = true
		}
	}
	assert.True(t, sawAborted, "the abandoned suspended turn must be closed with a TurnAborted")
}
