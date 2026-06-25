package agent_test

// RED tests for FIX 3 (durable approval suspend/resume), the agent-loop side.
//
// Part (a) AC-3.3: gateCall must append a domain.ApprovalRequested BEFORE blocking
// on the ApprovalGate, then the existing PermissionDecided{ask,Resolved} AFTER the
// resolution — so the pair (ApprovalRequested -> PermissionDecided) brackets one
// ask. Deny / hook-block paths still append NO ApprovalRequested.
//
// These reference domain.ApprovalRequested / domain.EventApprovalRequested, which
// do not exist yet, so this file does not compile — the RED proof.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// inspectingGate is an app.ApprovalGate that, on Request, runs an injected hook
// (which inspects the event log) and then returns a scripted resolution. It lets
// a test prove ApprovalRequested was ALREADY appended at the instant the loop
// blocks on the gate — i.e. BEFORE the ask resolves (AC-3.3 ordering).
type inspectingGate struct {
	onRequest  func(app.ApprovalRequest)
	resolution domain.AskResolution
}

func (g *inspectingGate) Request(_ context.Context, req app.ApprovalRequest) (domain.AskResolution, error) {
	if g.onRequest != nil {
		g.onRequest(req)
	}
	return g.resolution, nil
}

func (g *inspectingGate) Resolve(context.Context, string, string, domain.AskResolution) error {
	return nil
}

// TestGateCall_AppendsApprovalRequestedBeforeBlocking asserts that at the moment
// the loop blocks on the gate, an ApprovalRequested for the call is ALREADY on the
// log; and that after resolution the PermissionDecided{ask,allowed} follows. The
// order on the log per ask is ApprovalRequested -> PermissionDecided (AC-3.3).
func TestGateCall_AppendsApprovalRequestedBeforeBlocking(t *testing.T) {
	h := newHarness(t)

	const sessID = "sess-approve"
	var sawAtRequest []domain.ApprovalRequested
	gate := &inspectingGate{
		resolution: domain.AskAllowed,
		onRequest: func(_ app.ApprovalRequest) {
			// At the instant the gate is asked, the ApprovalRequested must already
			// be persisted (the loop appended it BEFORE blocking).
			sawAtRequest = payloadsOf[domain.ApprovalRequested](h, sessID)
		},
	}

	lp := agent.NewLoop(agent.Deps{
		EventLog: h.eventlog, Model: h.model, Tools: h.tools, Approvals: gate,
		Hooks: h.hooks, Policy: h.pol, Clock: h.clk, IDs: h.ids, Sink: h.sink, Metrics: h.metrics,
	}, defaultConfig())

	h.tools.SetTools([]app.ToolDescriptor{{Name: "write", SideEffect: domain.SideEffectMutating}})
	h.model.AddStream(toolCallStream("c1", "write", map[string]any{"p": "f"}), nil)
	h.model.AddStream(textStream("done"), nil)
	h.pol.AddAsk("ask-rule", "needs human")
	h.tools.AddSuccessfulExecution("ok")

	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: sessID, UserMessage: userMsg("go")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	// (1) ApprovalRequested was on the log BEFORE the gate resolved.
	require.Len(t, sawAtRequest, 1, "ApprovalRequested must be appended BEFORE blocking on the gate")
	ar := sawAtRequest[0]
	assert.Equal(t, "c1", ar.CallID)
	assert.Equal(t, "write", ar.ToolName)
	assert.Equal(t, "needs human", ar.Reason)
	assert.Equal(t, map[string]any{"p": "f"}, ar.Args)

	// (2) The AFTER-resolution PermissionDecided still follows, completing the pair.
	decided := payloadsOf[domain.PermissionDecided](h, sessID)
	require.Len(t, decided, 1)
	assert.Equal(t, domain.PermissionAsk, decided[0].Decision)
	assert.Equal(t, domain.AskAllowed, decided[0].Resolved)

	// (3) Log order per ask: ApprovalRequested precedes PermissionDecided.
	types := h.eventTypes(sessID)
	reqIdx, decIdx := indexOf(types, domain.EventApprovalRequested), indexOf(types, domain.EventPermissionDecided)
	require.GreaterOrEqual(t, reqIdx, 0, "ApprovalRequested must be on the log")
	require.GreaterOrEqual(t, decIdx, 0, "PermissionDecided must be on the log")
	assert.Less(t, reqIdx, decIdx, "ApprovalRequested must precede PermissionDecided")
}

// TestGateCall_DenyAppendsNoApprovalRequested asserts a policy Deny still records
// PermissionDecided{deny} with NO ApprovalRequested (FR-PERM-01 AC-1 preserved;
// AC-3.3 deny path).
func TestGateCall_DenyAppendsNoApprovalRequested(t *testing.T) {
	h := newHarness(t)
	h.tools.SetTools([]app.ToolDescriptor{{Name: "write", SideEffect: domain.SideEffectMutating}})
	h.model.AddStream(toolCallStream("c1", "write", map[string]any{"p": "f"}), nil)
	h.model.AddStream(textStream("adapted"), nil)
	h.pol.AddDeny("deny-rule", "forbidden")

	res, err := h.run(t, defaultConfig(), "sess-deny", "go")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)
	assert.Empty(t, payloadsOf[domain.ApprovalRequested](h, "sess-deny"),
		"a deny must NOT append an ApprovalRequested (FR-PERM-01 AC-1)")
}

// TestGateCall_HookBlockAppendsNoApprovalRequested asserts a PreToolUse hook block
// records PermissionDecided{deny,hook_blocked} with NO ApprovalRequested
// (FR-EXT-03 AC-1 preserved; AC-3.3 hook-block path).
func TestGateCall_HookBlockAppendsNoApprovalRequested(t *testing.T) {
	h := newHarness(t)
	h.tools.SetTools([]app.ToolDescriptor{{Name: "write", SideEffect: domain.SideEffectMutating}})
	h.model.AddStream(toolCallStream("c1", "write", map[string]any{"p": "f"}), nil)
	h.model.AddStream(textStream("adapted"), nil)
	h.hooks.AddDecision(false, "blocked")

	res, err := h.run(t, defaultConfig(), "sess-hookblock", "go")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)
	assert.Empty(t, payloadsOf[domain.ApprovalRequested](h, "sess-hookblock"),
		"a hook block must NOT append an ApprovalRequested (FR-EXT-03 AC-1)")
}

// seedSuspendedApproval seeds a session that crashed mid-ask: a started turn with
// an assistant tool-call and an ApprovalRequested for that call, but NO matching
// PermissionDecided (the orchestrator died before the human resolved it).
func seedSuspendedApproval(h *harness, sessionID string) {
	ctx := context.Background()
	_, _ = h.eventlog.Append(ctx, sessionID, 0, 0, "seed-1",
		app.AppendInput{Event: domain.SessionStarted{SystemPrompt: "sys"}})
	_, _ = h.eventlog.Append(ctx, sessionID, 1, 0, "seed-2",
		app.AppendInput{Event: domain.MessageAppended{Message: userMsg("do the risky thing")}})
	_, _ = h.eventlog.Append(ctx, sessionID, 2, 0, "seed-3",
		app.AppendInput{Event: domain.TurnStarted{TurnID: "susp-turn", Model: "test-model"}})
	_, _ = h.eventlog.Append(ctx, sessionID, 3, 0, "seed-4",
		app.AppendInput{Event: domain.AssistantMessage{
			TurnID:     "susp-turn",
			Message:    llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentPart{{ToolCall: &llm.ToolCall{ID: "c1", Name: "write", Args: map[string]any{"p": "f"}}}}},
			StopReason: llm.StopToolUse,
		}})
	// Crash mid-ask: ApprovalRequested with no matching PermissionDecided.
	_, _ = h.eventlog.Append(ctx, sessionID, 4, 0, "seed-5",
		app.AppendInput{Event: domain.ApprovalRequested{TurnID: "susp-turn", CallID: "c1", ToolName: "write", Reason: "mutating", Args: map[string]any{"p": "f"}}})
}

// TestRun_ResumeReRaisesSuspendedApproval is the TARGET-level (AC-3.7/AC-3.9)
// resume test: a crash mid-ask is, on resume, RE-RAISED to the gate (NOT silently
// TurnAborted). When the human approves (AskAllowed) the suspended tool proceeds
// (ToolExecutionStarted/ToolResult appended) and the run finishes Success.
//
// If the implementation ships only the FALLBACK level (AC-3.8 durable+auditable
// close) this test will need to be replaced with a fallback assertion; per the
// guardrail the agent must clearly report which level was achieved. As written it
// pins the PREFERRED target.
func TestRun_ResumeReRaisesSuspendedApproval(t *testing.T) {
	h := newHarness(t)
	const sessID = "sess-resume-approve"
	seedSuspendedApproval(h, sessID)

	h.tools.SetTools([]app.ToolDescriptor{{Name: "write", SideEffect: domain.SideEffectMutating}})
	// After the re-raised approval resolves AskAllowed and the tool proceeds, the
	// model returns text-only to end the run.
	h.model.AddStream(textStream("done"), nil)
	h.tools.AddSuccessfulExecution("ok")

	reRaised := false
	gate := &inspectingGate{
		resolution: domain.AskAllowed,
		onRequest: func(req app.ApprovalRequest) {
			if req.CallID == "c1" {
				reRaised = true
			}
		},
	}
	lp := agent.NewLoop(agent.Deps{
		EventLog: h.eventlog, Model: h.model, Tools: h.tools, Approvals: gate,
		Hooks: h.hooks, Policy: h.pol, Clock: h.clk, IDs: h.ids, Sink: h.sink, Metrics: h.metrics,
	}, defaultConfig())

	// A pure resume: no fresh user message.
	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: sessID})
	require.NoError(t, err)

	assert.True(t, reRaised, "the suspended approval must be RE-RAISED to the gate on resume, not silently aborted")

	// The suspended turn must NOT be closed by a bare generic TurnAborted (that is
	// the silent-loss bug this fix closes).
	aborts := payloadsOf[domain.TurnAborted](h, sessID)
	for _, ab := range aborts {
		assert.NotEqualf(t, "susp-turn", ab.TurnID,
			"the suspended turn must not be silently TurnAborted (reason=%s); it must be re-raised", ab.Reason)
	}

	// After approval, the tool proceeds and the run finishes Success.
	assert.Equal(t, domain.Success, res.Reason)
	require.NotEmpty(t, payloadsOf[domain.ToolExecutionStarted](h, sessID), "the approved tool must proceed")
	require.NotEmpty(t, payloadsOf[domain.ToolResult](h, sessID), "the approved tool must produce a result")
}

// indexOf returns the index of the first occurrence of want in types, or -1.
func indexOf(types []domain.EventType, want domain.EventType) int {
	for i, t := range types {
		if t == want {
			return i
		}
	}
	return -1
}
