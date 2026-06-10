// Package recovery provides the pure fold-based recovery analysis used by the
// orchestrator loop on startup and after reconnect to determine what, if any,
// in-flight state must be adjudicated before the loop resumes (T-EVT-05;
// FR-LOOP-05, FR-STATE-02, FR-TOOL-03; ADR-0012).
//
// All operations in this package are pure functions over a slice of
// [domain.EventEnvelope] — they perform no I/O and carry no state. They are
// therefore fully unit-testable without a database.
//
// The rules applied (from ADR-0012):
//
//   - A [domain.TurnStarted] with no terminal [domain.AssistantMessage],
//     [domain.TurnAborted], or [domain.TurnFinished] for the same TurnID is
//     OPEN. The loop must invoke the TurnAborted path (never silent replay).
//     The recovered usage equals the partial [llm.Usage] from the last
//     [domain.AssistantMessageDelta] for that turn, or zero if no delta was
//     emitted.
//
//   - A [domain.ToolExecutionStarted] with no terminal [domain.ToolResult]
//     for a MUTATING tool is UNKNOWN and must NOT be marked re-dispatchable
//     (at-most-once semantics; the loop adjudicates). For a read-only tool
//     it may be safely re-run.
package recovery

import (
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// SideEffectLookup resolves the [domain.SideEffect] for a named tool. The
// implementation is injected by the caller so this package remains pure and
// requires no tool-registry dependency. When the lookup is nil or returns an
// empty string, the tool is treated as [domain.SideEffectMutating] (fail-safe;
// ADR-0014).
type SideEffectLookup func(toolName string) domain.SideEffect

// OpenTurn describes a turn that was in flight at the time of a crash:
// [domain.TurnStarted] was appended but no terminal event (AssistantMessage,
// TurnAborted, TurnFinished) followed. The loop must append
// [domain.TurnAborted] before resuming (ADR-0012 §"Durable turn boundaries").
type OpenTurn struct {
	// TurnID is the turn identifier from [domain.TurnStarted.TurnID]. The loop
	// uses it to populate [domain.TurnAborted.TurnID].
	TurnID string
	// RecoveredUsage is the partial [llm.Usage] read from the provider stream's
	// last usage metadata checkpoint. It is the zero value when no
	// [domain.AssistantMessageDelta] was emitted before the crash. The loop
	// carries it into [domain.TurnAborted.UsageSoFar] so the partial turn is
	// accounted and never silently re-billed (ADR-0012; architecture §7.1).
	RecoveredUsage llm.Usage
	// LastProviderRaw is the provider's resumable continuation cursor read from
	// the last [domain.AssistantMessageDelta], when available. It enables the
	// loop to continue rather than re-run the turn where the provider supports
	// resumption (architecture §7.1, §11.1). Nil when no delta was emitted.
	LastProviderRaw llm.ProviderRaw
}

// UnknownExecution describes a tool execution whose outcome is unknown: a
// [domain.ToolExecutionStarted] was appended but no [domain.ToolResult]
// followed (the orchestrator crashed after dispatching the tool but before
// recording the result). The loop must NOT blindly re-dispatch a mutating tool
// in this state — it must surface it for human or hook adjudication (ADR-0012
// §"At-most-once recovery").
type UnknownExecution struct {
	// CallID is the [domain.ToolExecutionStarted.CallID]. The loop uses it to
	// correlate the outstanding execution with the model's tool-call request.
	CallID string
	// ToolName is the [domain.ToolExecutionStarted.ToolName].
	ToolName string
	// IdempotencyKey is the log-derived dedup key from
	// [domain.ToolExecutionStarted.IdempotencyKey] used to query the durable
	// [tool_executions] ledger (ADR-0012 §"Durable dedup ledger").
	IdempotencyKey string
	// SideEffect is the resolved side-effect classification of the tool. For a
	// mutating tool the loop must NOT re-dispatch; for a read-only tool it may
	// safely re-run. Resolved via the injected [SideEffectLookup]; defaults to
	// [domain.SideEffectMutating] when the lookup is nil or unknown.
	SideEffect domain.SideEffect
	// ReDispatchable reports whether the execution may be safely re-run.
	// It is true only for read-only tools ([domain.SideEffectReadOnly]).
	// Mutating tools are never re-dispatchable without explicit adjudication
	// (ADR-0012 §"At-most-once recovery for mutating tools").
	ReDispatchable bool
}

// Plan is the result of folding a slice of [domain.EventEnvelope]: the
// complete set of adjudication decisions the loop must take before resuming.
// A zero-value Plan (all slices nil/empty) means the log is clean and the
// loop may resume without preconditions.
type Plan struct {
	// OpenTurns is the set of turns that were in flight at crash time and have
	// no terminal event. The loop must append [domain.TurnAborted] for each
	// before resuming (ADR-0012; FR-LOOP-05 AC-2). Ordered by ascending
	// [domain.TurnStarted.Seq].
	OpenTurns []OpenTurn
	// UnknownExecutions is the set of tool executions whose outcome is unknown.
	// The loop must surface each for adjudication; mutating ones must NOT be
	// re-dispatched automatically (ADR-0012; FR-TOOL-03 AC-1). Ordered by
	// ascending [domain.ToolExecutionStarted.Seq].
	UnknownExecutions []UnknownExecution
}

// turnState is internal fold state for one in-flight turn.
type turnState struct {
	turnID          string
	recoveredUsage  llm.Usage
	lastProviderRaw llm.ProviderRaw
	closed          bool // true once a terminal event is seen
}

// execState is internal fold state for one in-flight tool execution.
type execState struct {
	callID         string
	toolName       string
	idempotencyKey string
	closed         bool // true once a ToolResult is seen
}

// Analyze folds events into a [Plan] by scanning for open turns and unknown
// tool executions per the rules in ADR-0012. It is a pure function: it does
// not modify events and performs no I/O.
//
// lookup resolves a tool name to its [domain.SideEffect]. If lookup is nil or
// returns an empty value, the tool is treated as [domain.SideEffectMutating]
// (fail-safe default; ADR-0014). Passing nil is legal for tests that only
// exercise turn recovery.
func Analyze(events []domain.EventEnvelope, lookup SideEffectLookup) (Plan, error) {
	// turns tracks open turn state keyed by TurnID.
	turns := make(map[string]*turnState)
	// turnOrder preserves the insertion order for deterministic output.
	var turnOrder []string

	// execs tracks open tool execution state keyed by CallID.
	execs := make(map[string]*execState)
	// execOrder preserves insertion order.
	var execOrder []string

	for _, env := range events {
		switch p := env.Event.(type) {
		case domain.TurnStarted:
			if _, exists := turns[p.TurnID]; !exists {
				turns[p.TurnID] = &turnState{turnID: p.TurnID}
				turnOrder = append(turnOrder, p.TurnID)
			}

		case domain.AssistantMessageDelta:
			// Update the running checkpoint for the turn.
			if ts, ok := turns[p.TurnID]; ok && !ts.closed {
				ts.recoveredUsage = p.UsageSoFar
				ts.lastProviderRaw = p.ProviderRaw
			}

		case domain.AssistantMessage:
			// AssistantMessage closes the turn (the generation completed).
			if ts, ok := turns[p.TurnID]; ok {
				ts.closed = true
			}

		case domain.TurnAborted:
			// TurnAborted is a terminal event — the turn was already adjudicated.
			if ts, ok := turns[p.TurnID]; ok {
				ts.closed = true
			}

		case domain.TurnFinished:
			// TurnFinished is a terminal event — the turn completed normally.
			if ts, ok := turns[p.TurnID]; ok {
				ts.closed = true
			}

		case domain.ToolExecutionStarted:
			if _, exists := execs[p.CallID]; !exists {
				execs[p.CallID] = &execState{
					callID:         p.CallID,
					toolName:       p.ToolName,
					idempotencyKey: p.IdempotencyKey,
				}
				execOrder = append(execOrder, p.CallID)
			}

		case domain.ToolResult:
			// ToolResult closes the corresponding execution.
			if es, ok := execs[p.CallID]; ok {
				es.closed = true
			}
		}
	}

	var plan Plan

	// Collect open turns in insertion order.
	for _, tid := range turnOrder {
		ts := turns[tid]
		if ts.closed {
			continue
		}
		plan.OpenTurns = append(plan.OpenTurns, OpenTurn{
			TurnID:          ts.turnID,
			RecoveredUsage:  ts.recoveredUsage,
			LastProviderRaw: ts.lastProviderRaw,
		})
	}

	// Collect unknown executions in insertion order.
	for _, cid := range execOrder {
		es := execs[cid]
		if es.closed {
			continue
		}
		se := resolveSideEffect(es.toolName, lookup)
		plan.UnknownExecutions = append(plan.UnknownExecutions, UnknownExecution{
			CallID:         es.callID,
			ToolName:       es.toolName,
			IdempotencyKey: es.idempotencyKey,
			SideEffect:     se,
			ReDispatchable: se == domain.SideEffectReadOnly,
		})
	}

	return plan, nil
}

// resolveSideEffect resolves the [domain.SideEffect] for toolName via lookup.
// When lookup is nil or returns an empty/unrecognized value it returns
// [domain.SideEffectMutating] (fail-safe; ADR-0014: "MCP tools default to
// mutating").
func resolveSideEffect(toolName string, lookup SideEffectLookup) domain.SideEffect {
	if lookup == nil {
		return domain.SideEffectMutating
	}
	se := lookup(toolName)
	if se == "" {
		return domain.SideEffectMutating
	}
	return se
}
