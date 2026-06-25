package recovery_test

// Package recovery_test verifies the pure fold-based recovery logic for
// T-EVT-05 (FR-LOOP-05 AC-2, FR-STATE-02 AC-1, FR-TOOL-03 AC-1).
//
// All tests use hand-built []domain.EventEnvelope slices — no database, no
// gRPC, no goroutines beyond the test runner itself. Every assertion is a
// pure function call.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app/recovery"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ---------------------------------------------------------------------------
// Helpers — small constructors that produce realistic EventEnvelopes without
// requiring a running database.
// ---------------------------------------------------------------------------

func envOf(seq int64, evt domain.Event) domain.EventEnvelope {
	return domain.EventEnvelope{
		Type:      evt.EventType(),
		Seq:       seq,
		SessionID: "sess-1",
		TenantID:  "tenant-1",
		RequestID: "req-0",
		Event:     evt,
	}
}

// sideEffectLookup is a deterministic test implementation of
// [recovery.SideEffectLookup] that maps tool names to their declared
// [domain.SideEffect].
func sideEffectLookup(name string) domain.SideEffect {
	switch name {
	case "bash", "edit", "write":
		return domain.SideEffectMutating
	case "read", "glob", "grep":
		return domain.SideEffectReadOnly
	default:
		// Fail-safe: unknown tools are treated as mutating (ADR-0012, ADR-0014).
		return domain.SideEffectMutating
	}
}

// ---------------------------------------------------------------------------
// FR-LOOP-05 AC-2 / FR-STATE-02 AC-1
// A folded log truncated after TurnStarted (no terminal event) must classify
// the turn as OPEN and surface its TurnID so the loop can append TurnAborted.
// ---------------------------------------------------------------------------

func TestAnalyze_OpenTurn_TruncatedAfterTurnStarted(t *testing.T) {
	events := []domain.EventEnvelope{
		envOf(1, domain.SessionStarted{SystemPrompt: "you are helpful"}),
		envOf(2, domain.MessageAppended{Message: llm.Message{Role: llm.RoleUser}}),
		envOf(3, domain.TurnStarted{TurnID: "turn-1", Model: "claude-3-5-sonnet"}),
		// No AssistantMessage, TurnAborted, or TurnFinished — the log was
		// truncated mid-turn (e.g. the orchestrator crashed while streaming).
	}

	plan, err := recovery.Analyze(events, sideEffectLookup)
	require.NoError(t, err)

	require.Len(t, plan.OpenTurns, 1, "expected exactly one open turn")
	assert.Equal(t, "turn-1", plan.OpenTurns[0].TurnID)
}

// ---------------------------------------------------------------------------
// FR-LOOP-05 AC-1
// The recovered cost for an open turn equals the partial UsageSoFar from the
// last AssistantMessageDelta, not zero.
// ---------------------------------------------------------------------------

func TestAnalyze_RecoveredUsage_FromLastDelta(t *testing.T) {
	partialUsage := llm.Usage{
		InputTokens:  120,
		OutputTokens: 47,
	}

	events := []domain.EventEnvelope{
		envOf(1, domain.SessionStarted{}),
		envOf(2, domain.TurnStarted{TurnID: "turn-2", Model: "claude-opus"}),
		// First checkpoint — no usage yet; will be superseded by the second.
		envOf(3, domain.AssistantMessageDelta{
			TurnID:    "turn-2",
			TextSoFar: "Hello,",
		}),
		// Second (last) checkpoint — this usage must be the recovered cost.
		envOf(4, domain.AssistantMessageDelta{
			TurnID:     "turn-2",
			TextSoFar:  "Hello, world",
			UsageSoFar: partialUsage,
		}),
		// No terminal event.
	}

	plan, err := recovery.Analyze(events, sideEffectLookup)
	require.NoError(t, err)

	require.Len(t, plan.OpenTurns, 1)
	assert.Equal(t, "turn-2", plan.OpenTurns[0].TurnID)
	assert.Equal(t, partialUsage, plan.OpenTurns[0].RecoveredUsage,
		"recovered cost must equal the last delta's UsageSoFar, not zero")
}

// ---------------------------------------------------------------------------
// FR-LOOP-05 AC-1 (zero-delta variant)
// When no AssistantMessageDelta was emitted before the crash, RecoveredUsage
// must be the zero llm.Usage — not garbage.
// ---------------------------------------------------------------------------

func TestAnalyze_RecoveredUsage_ZeroWhenNoDelta(t *testing.T) {
	events := []domain.EventEnvelope{
		envOf(1, domain.SessionStarted{}),
		envOf(2, domain.TurnStarted{TurnID: "turn-noDelta", Model: "gpt-4o"}),
		// No delta, no terminal.
	}

	plan, err := recovery.Analyze(events, sideEffectLookup)
	require.NoError(t, err)

	require.Len(t, plan.OpenTurns, 1)
	assert.Equal(t, llm.Usage{}, plan.OpenTurns[0].RecoveredUsage,
		"no delta seen => recovered usage is the zero value")
}

// ---------------------------------------------------------------------------
// FR-TOOL-03 AC-1 (mutating tool)
// A ToolExecutionStarted with no terminal ToolResult for a MUTATING tool must
// be classified UNKNOWN and must NOT be marked re-dispatchable.
// ---------------------------------------------------------------------------

func TestAnalyze_UnknownExecution_MutatingTool(t *testing.T) {
	events := []domain.EventEnvelope{
		envOf(1, domain.SessionStarted{}),
		envOf(2, domain.TurnStarted{TurnID: "turn-3", Model: "claude"}),
		envOf(3, domain.AssistantMessage{TurnID: "turn-3"}),
		envOf(4, domain.ToolExecutionStarted{
			CallID:         "call-42",
			ToolName:       "bash",
			IdempotencyKey: "idem-42",
		}),
		// No ToolResult — orchestrator crashed after dispatching the mutating
		// tool but before receiving its result.
	}

	plan, err := recovery.Analyze(events, sideEffectLookup)
	require.NoError(t, err)

	require.Len(t, plan.UnknownExecutions, 1, "expected one unknown execution")

	ue := plan.UnknownExecutions[0]
	assert.Equal(t, "call-42", ue.CallID)
	assert.Equal(t, "bash", ue.ToolName)
	assert.Equal(t, "idem-42", ue.IdempotencyKey)
	assert.Equal(t, domain.SideEffectMutating, ue.SideEffect)
	assert.False(t, ue.ReDispatchable,
		"mutating tool with unknown outcome must NOT be re-dispatchable (at-most-once)")
}

// ---------------------------------------------------------------------------
// FR-TOOL-03 AC-1 (read-only tool)
// A ToolExecutionStarted with no terminal ToolResult for a READ-ONLY tool
// may be safely re-run (re-dispatch is allowed).
// ---------------------------------------------------------------------------

func TestAnalyze_UnknownExecution_ReadOnlyTool_ReDispatchable(t *testing.T) {
	events := []domain.EventEnvelope{
		envOf(1, domain.SessionStarted{}),
		envOf(2, domain.TurnStarted{TurnID: "turn-4", Model: "claude"}),
		envOf(3, domain.AssistantMessage{TurnID: "turn-4"}),
		envOf(4, domain.ToolExecutionStarted{
			CallID:         "call-read",
			ToolName:       "read",
			IdempotencyKey: "idem-read",
		}),
		// No ToolResult.
	}

	plan, err := recovery.Analyze(events, sideEffectLookup)
	require.NoError(t, err)

	require.Len(t, plan.UnknownExecutions, 1)

	ue := plan.UnknownExecutions[0]
	assert.Equal(t, domain.SideEffectReadOnly, ue.SideEffect)
	assert.True(t, ue.ReDispatchable,
		"read-only tool with unknown outcome MAY be re-dispatched safely")
}

// ---------------------------------------------------------------------------
// Completed tool execution must NOT appear in UnknownExecutions.
// ---------------------------------------------------------------------------

func TestAnalyze_CompletedExecution_NotUnknown(t *testing.T) {
	events := []domain.EventEnvelope{
		envOf(1, domain.SessionStarted{}),
		envOf(2, domain.TurnStarted{TurnID: "turn-5", Model: "claude"}),
		envOf(3, domain.AssistantMessage{TurnID: "turn-5"}),
		envOf(4, domain.ToolExecutionStarted{
			CallID:         "call-done",
			ToolName:       "bash",
			IdempotencyKey: "idem-done",
		}),
		envOf(5, domain.ToolResult{
			CallID:  "call-done",
			Result:  "exit 0",
			IsError: false,
		}),
		// No terminal turn event (still open), but the tool completed.
	}

	plan, err := recovery.Analyze(events, sideEffectLookup)
	require.NoError(t, err)

	assert.Empty(t, plan.UnknownExecutions,
		"completed tool executions must not appear in UnknownExecutions")
}

// ---------------------------------------------------------------------------
// Closed turn (TurnFinished) must NOT appear in OpenTurns.
// ---------------------------------------------------------------------------

func TestAnalyze_ClosedTurn_TurnFinished_NotOpen(t *testing.T) {
	events := []domain.EventEnvelope{
		envOf(1, domain.SessionStarted{}),
		envOf(2, domain.TurnStarted{TurnID: "turn-done", Model: "claude"}),
		envOf(3, domain.TurnFinished{TurnID: "turn-done", Reason: domain.Success}),
	}

	plan, err := recovery.Analyze(events, sideEffectLookup)
	require.NoError(t, err)

	assert.Empty(t, plan.OpenTurns,
		"a TurnFinished closes the turn; it must not be open")
}

// ---------------------------------------------------------------------------
// Closed turn (TurnAborted) must NOT appear in OpenTurns.
// ---------------------------------------------------------------------------

func TestAnalyze_ClosedTurn_TurnAborted_NotOpen(t *testing.T) {
	events := []domain.EventEnvelope{
		envOf(1, domain.SessionStarted{}),
		envOf(2, domain.TurnStarted{TurnID: "turn-aborted", Model: "claude"}),
		envOf(3, domain.TurnAborted{
			TurnID: "turn-aborted",
			Reason: domain.ErrorDuringExecution,
		}),
	}

	plan, err := recovery.Analyze(events, sideEffectLookup)
	require.NoError(t, err)

	assert.Empty(t, plan.OpenTurns,
		"a TurnAborted closes the turn; it must not be open")
}

// ---------------------------------------------------------------------------
// AssistantMessage (the terminal generation event) closes the turn.
// ---------------------------------------------------------------------------

func TestAnalyze_ClosedTurn_AssistantMessage_NotOpen(t *testing.T) {
	events := []domain.EventEnvelope{
		envOf(1, domain.SessionStarted{}),
		envOf(2, domain.TurnStarted{TurnID: "turn-am", Model: "claude"}),
		envOf(3, domain.AssistantMessage{TurnID: "turn-am"}),
	}

	plan, err := recovery.Analyze(events, sideEffectLookup)
	require.NoError(t, err)

	assert.Empty(t, plan.OpenTurns,
		"an AssistantMessage closes the turn; it must not be open")
}

// ---------------------------------------------------------------------------
// Multiple turns — only the unclosed one appears.
// ---------------------------------------------------------------------------

func TestAnalyze_MultiTurn_OnlyUnclosedIsOpen(t *testing.T) {
	events := []domain.EventEnvelope{
		envOf(1, domain.SessionStarted{}),
		// Turn A: finished.
		envOf(2, domain.TurnStarted{TurnID: "turn-A", Model: "claude"}),
		envOf(3, domain.AssistantMessage{TurnID: "turn-A"}),
		envOf(4, domain.TurnFinished{TurnID: "turn-A", Reason: domain.Success}),
		// Turn B: open (crash).
		envOf(5, domain.TurnStarted{TurnID: "turn-B", Model: "claude"}),
		// No terminal event for B.
	}

	plan, err := recovery.Analyze(events, sideEffectLookup)
	require.NoError(t, err)

	require.Len(t, plan.OpenTurns, 1)
	assert.Equal(t, "turn-B", plan.OpenTurns[0].TurnID)
}

// ---------------------------------------------------------------------------
// Empty event log — no open turns, no unknown executions.
// ---------------------------------------------------------------------------

func TestAnalyze_EmptyLog(t *testing.T) {
	plan, err := recovery.Analyze(nil, sideEffectLookup)
	require.NoError(t, err)

	assert.Empty(t, plan.OpenTurns)
	assert.Empty(t, plan.UnknownExecutions)
}

// ---------------------------------------------------------------------------
// Nil lookup — Analyze must not panic; unknown tools default to mutating
// (fail-safe; ADR-0014).
// ---------------------------------------------------------------------------

func TestAnalyze_NilLookup_DefaultsMutating(t *testing.T) {
	events := []domain.EventEnvelope{
		envOf(1, domain.SessionStarted{}),
		envOf(2, domain.TurnStarted{TurnID: "t1", Model: "m"}),
		envOf(3, domain.AssistantMessage{TurnID: "t1"}),
		envOf(4, domain.ToolExecutionStarted{
			CallID:         "c1",
			ToolName:       "unknown-tool",
			IdempotencyKey: "ik1",
		}),
		// No ToolResult.
	}

	// nil lookup => treat all tools as mutating (fail-safe).
	plan, err := recovery.Analyze(events, nil)
	require.NoError(t, err)

	require.Len(t, plan.UnknownExecutions, 1)
	assert.Equal(t, domain.SideEffectMutating, plan.UnknownExecutions[0].SideEffect)
	assert.False(t, plan.UnknownExecutions[0].ReDispatchable)
}

// ---------------------------------------------------------------------------
// FIX 3 (AC-3.4): a crash mid-ask. An open turn whose log contains an
// ApprovalRequested{CallID} with NO matching PermissionDecided{CallID} is a
// SuspendedApproval — DISTINCT from a generic open turn, and it SUPPRESSES the
// plain OpenTurn TurnAborted for that same turn (the turn is awaiting approval,
// not a generic open turn). These reference recovery.SuspendedApproval and
// Plan.SuspendedApprovals, which do not exist yet — the RED proof.
// ---------------------------------------------------------------------------

func TestAnalyze_SuspendedApproval_RequestedNoResolution(t *testing.T) {
	events := []domain.EventEnvelope{
		envOf(1, domain.SessionStarted{SystemPrompt: "sys"}),
		envOf(2, domain.MessageAppended{Message: llm.Message{Role: llm.RoleUser}}),
		envOf(3, domain.TurnStarted{TurnID: "turn-1", Model: "m"}),
		envOf(4, domain.AssistantMessage{
			TurnID:     "turn-1",
			Message:    llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentPart{{ToolCall: &llm.ToolCall{ID: "c1", Name: "write"}}}},
			StopReason: llm.StopToolUse,
		}),
		// Crash mid-ask: ApprovalRequested with NO matching PermissionDecided.
		envOf(5, domain.ApprovalRequested{TurnID: "turn-1", CallID: "c1", ToolName: "write", Reason: "mutating", Args: map[string]any{"p": "f"}}),
	}

	plan, err := recovery.Analyze(events, sideEffectLookup)
	require.NoError(t, err)

	// Classified as a SuspendedApproval, carrying the call details.
	require.Len(t, plan.SuspendedApprovals, 1, "a requested-but-unresolved ask is a SuspendedApproval")
	sa := plan.SuspendedApprovals[0]
	assert.Equal(t, "turn-1", sa.TurnID)
	assert.Equal(t, "c1", sa.CallID)
	assert.Equal(t, "write", sa.ToolName)
	assert.Equal(t, "mutating", sa.Reason)
	assert.Equal(t, map[string]any{"p": "f"}, sa.Args)

	// The suspended turn must NOT also be classified as a generic OpenTurn — the
	// loop must re-raise the approval, not silently TurnAbort it.
	for _, ot := range plan.OpenTurns {
		assert.NotEqual(t, "turn-1", ot.TurnID,
			"a turn awaiting approval must NOT be a generic open turn (it would be silently aborted)")
	}
}

// TestAnalyze_ResolvedApproval_NotSuspended asserts that once a matching
// PermissionDecided{CallID} closes the ask, it is NOT a SuspendedApproval (the
// pair is complete). The turn then closes normally via its terminal event.
func TestAnalyze_ResolvedApproval_NotSuspended(t *testing.T) {
	events := []domain.EventEnvelope{
		envOf(1, domain.SessionStarted{}),
		envOf(2, domain.TurnStarted{TurnID: "turn-1", Model: "m"}),
		envOf(3, domain.AssistantMessage{
			TurnID:     "turn-1",
			Message:    llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentPart{{ToolCall: &llm.ToolCall{ID: "c1", Name: "write"}}}},
			StopReason: llm.StopToolUse,
		}),
		envOf(4, domain.ApprovalRequested{TurnID: "turn-1", CallID: "c1", ToolName: "write"}),
		// The ask WAS resolved — the pair is complete.
		envOf(5, domain.PermissionDecided{CallID: "c1", ToolName: "write", Decision: domain.PermissionAsk, Resolved: domain.AskAllowed}),
		envOf(6, domain.TurnFinished{TurnID: "turn-1", Reason: domain.Success, NumTurns: 1}),
	}

	plan, err := recovery.Analyze(events, sideEffectLookup)
	require.NoError(t, err)

	assert.Empty(t, plan.SuspendedApprovals, "a resolved ask is not suspended")
	assert.Empty(t, plan.OpenTurns, "a finished turn is not open")
}
