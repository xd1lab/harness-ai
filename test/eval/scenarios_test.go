package eval_test

// Package eval_test holds the deterministic golden SCENARIOS that ARE the eval
// CI gate (ADR-0007; NFR-TEST-04; DOD-03). Each scenario drives the REAL agent
// loop end-to-end against scripted, in-memory fakes (no network, no Docker) and
// asserts, for a full run:
//
//   - the terminal [domain.TerminationReason] (FR-LOOP-02),
//   - the exact ordered event-log shape appended to the session (FR-LOOP-01 AC-1),
//   - and the token/cost accounting (FR-LOOP-02: usage + cost on TurnFinished).
//
// The five required scenarios are: (1) a simple single-turn text task → Success;
// (2) a tool-using task (assistant tool_call → tool result → final text) →
// correct event sequence + cost; (3) a permission-denied tool → no execution,
// fed back as an error observation; (4) a max-turns cap → ErrorMaxTurns; (5) a
// refusal → the distinct Refusal subtype. A sixth covers structured-output retry
// exhaustion → ErrorMaxStructuredOutputRetries. Scenarios 9 and 10 complete the
// DOD-03 set: the budget cap (→ ErrorMaxBudgetUSD) and permission-mode
// enforcement (plan mode holds a mutating tool; human deny → no execution).
//
// Run as: go test ./test/eval/...

import (
	"fmt"
	"math"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
	"github.com/xd1lab/harness-ai/test/eval"
)

// allowRule is a catch-all allow rule used by scenarios whose focus is not the
// policy decision (the deny/ask scenarios configure their own rules).
func allowRule() policy.Rule {
	return policy.Rule{ID: "allow-all", Effect: policy.EffectAllow}
}

// ---------------------------------------------------------------------------
// Scenario 1 — simple single-turn text task -> Success + golden log shape.
// ---------------------------------------------------------------------------

func TestScenario_SingleTurnText(t *testing.T) {
	eval.Scenario{
		Name:        "single-turn-text",
		UserMessage: "say hello",
		ModelTurns: []eval.ModelTurn{
			eval.TextTurnWithUsage("hello world", llm.Usage{InputTokens: 10, OutputTokens: 2}),
		},
		CostPerTurnUSD: 0.01,

		WantReason: domain.Success,
		WantEvents: []domain.EventType{
			domain.EventMessageAppended, // user task
			domain.EventTurnStarted,
			domain.EventAssistantMessage, // text-only
			domain.EventTurnFinished,     // success
		},
		WantNumTurns:     1,
		WantCostUSD:      0.01,
		WantCostAsserted: true,
	}.WithCheck(func(t *testing.T, r eval.Result) {
		// The terminal TurnFinished carries success + the run usage and cost.
		fins := eval.PayloadsOf[domain.TurnFinished](r)
		if len(fins) != 1 {
			t.Fatalf("want exactly one TurnFinished, got %d", len(fins))
		}
		if fins[0].Reason != domain.Success {
			t.Errorf("TurnFinished.Reason = %q, want success", fins[0].Reason)
		}
		wantUsage := llm.Usage{InputTokens: 10, OutputTokens: 2}
		if r.Run.Usage != wantUsage {
			t.Errorf("run usage = %+v, want %+v", r.Run.Usage, wantUsage)
		}
		if fins[0].Usage != wantUsage {
			t.Errorf("TurnFinished.Usage = %+v, want %+v", fins[0].Usage, wantUsage)
		}
	}).Exec(t)
}

// ---------------------------------------------------------------------------
// Scenario 2 — tool-using task: assistant tool_call -> tool result -> final
// text. Correct event sequence + cost accounting (FR-LOOP-01 AC-1).
// ---------------------------------------------------------------------------

func TestScenario_ToolUseRoundTrip(t *testing.T) {
	eval.Scenario{
		Name:        "tool-use-roundtrip",
		UserMessage: "read /etc/hosts",
		ModelTurns: []eval.ModelTurn{
			// Turn 1: the assistant requests a read-only tool.
			eval.ToolCallTurn("call-read-1", "read", map[string]any{"path": "/etc/hosts"},
				llm.Usage{InputTokens: 20, OutputTokens: 5}),
			// Turn 2: the assistant returns the final text.
			eval.TextTurnWithUsage("the file contains localhost", llm.Usage{InputTokens: 30, OutputTokens: 6}),
		},
		Tools: []app.ToolDescriptor{
			{Name: "read", SideEffect: domain.SideEffectReadOnly, EgressClass: domain.EgressClassNone},
		},
		ToolResults:    []app.ToolResult{{Content: "127.0.0.1 localhost"}},
		Rules:          []policy.Rule{allowRule()},
		CostPerTurnUSD: 0.02,

		WantReason: domain.Success,
		WantEvents: []domain.EventType{
			domain.EventMessageAppended,      // user task
			domain.EventTurnStarted,          // turn 1
			domain.EventAssistantMessage,     // tool_call
			domain.EventPermissionDecided,    // allow
			domain.EventToolExecutionStarted, // durable intent
			domain.EventToolResult,           // tool output
			domain.EventMessageAppended,      // results fed back (tool role)
			domain.EventTurnStarted,          // turn 2
			domain.EventAssistantMessage,     // final text
			domain.EventTurnFinished,         // success
		},
		WantNumTurns:     2,
		WantCostUSD:      0.04, // 2 turns * $0.02
		WantCostAsserted: true,
	}.WithCheck(func(t *testing.T, r eval.Result) {
		// The allow decision carried no human ask.
		decs := eval.PayloadsOf[domain.PermissionDecided](r)
		if len(decs) != 1 {
			t.Fatalf("want exactly one PermissionDecided, got %d", len(decs))
		}
		if decs[0].Decision != domain.PermissionAllow {
			t.Errorf("decision = %q, want allow", decs[0].Decision)
		}
		// The ToolExecutionStarted carried a non-empty, log-derived idempotency key.
		starts := eval.PayloadsOf[domain.ToolExecutionStarted](r)
		if len(starts) != 1 {
			t.Fatalf("want exactly one ToolExecutionStarted, got %d", len(starts))
		}
		if starts[0].CallID != "call-read-1" {
			t.Errorf("ToolExecutionStarted.CallID = %q, want call-read-1", starts[0].CallID)
		}
		if starts[0].IdempotencyKey == "" {
			t.Error("ToolExecutionStarted.IdempotencyKey is empty; want a log-derived key")
		}
		// The tool result was recorded and was not an error.
		results := eval.PayloadsOf[domain.ToolResult](r)
		if len(results) != 1 {
			t.Fatalf("want exactly one ToolResult, got %d", len(results))
		}
		if results[0].IsError {
			t.Error("ToolResult.IsError = true, want false")
		}
		if results[0].Result != "127.0.0.1 localhost" {
			t.Errorf("ToolResult.Result = %q, want the canned content", results[0].Result)
		}
		// Cumulative usage is the element-wise sum across both turns.
		wantUsage := llm.Usage{InputTokens: 50, OutputTokens: 11}
		if r.Run.Usage != wantUsage {
			t.Errorf("run usage = %+v, want %+v", r.Run.Usage, wantUsage)
		}
	}).Exec(t)
}

// ---------------------------------------------------------------------------
// Scenario 3 — permission-denied tool: a hard deny short-circuits with NO
// execution and NO human ask; the denied call is fed back as an error
// observation and the model finishes with text (FR-PERM-01 AC-1).
// ---------------------------------------------------------------------------

func TestScenario_PermissionDeniedNoExecution(t *testing.T) {
	eval.Scenario{
		Name:        "permission-denied",
		UserMessage: "delete everything",
		ModelTurns: []eval.ModelTurn{
			// Turn 1: the assistant requests a denied tool.
			eval.ToolCallTurn("call-bash-1", "bash", map[string]any{"cmd": "rm -rf /"}, llm.Usage{}),
			// Turn 2: after the deny observation, the assistant ends with text.
			eval.TextTurn("understood, I will not do that"),
		},
		Tools: []app.ToolDescriptor{
			{Name: "bash", SideEffect: domain.SideEffectMutating, EgressClass: domain.EgressClassNone},
		},
		// A deny rule for bash; deny wins unconditionally.
		Rules: []policy.Rule{
			{ID: "deny-bash", Effect: policy.EffectDeny, ToolName: "bash"},
		},
		// No ToolResults scripted: a dispatch would panic the fake runtime,
		// proving the denied tool was never executed.

		WantReason: domain.Success,
		WantEvents: []domain.EventType{
			domain.EventMessageAppended,   // user task
			domain.EventTurnStarted,       // turn 1
			domain.EventAssistantMessage,  // tool_call
			domain.EventPermissionDecided, // DENY (no ApprovalRequested, no ToolExecutionStarted)
			domain.EventToolResult,        // synthetic error observation fed back
			domain.EventMessageAppended,   // results fed back (tool role)
			domain.EventTurnStarted,       // turn 2
			domain.EventAssistantMessage,  // final text
			domain.EventTurnFinished,      // success
		},
		WantNumTurns:        2,
		WantNoToolExecution: true,
	}.WithCheck(func(t *testing.T, r eval.Result) {
		// Exactly one PermissionDecided, a deny, with no human resolution.
		decs := eval.PayloadsOf[domain.PermissionDecided](r)
		if len(decs) != 1 {
			t.Fatalf("want exactly one PermissionDecided, got %d", len(decs))
		}
		if decs[0].Decision != domain.PermissionDeny {
			t.Errorf("decision = %q, want deny", decs[0].Decision)
		}
		if decs[0].Resolved != domain.AskUnresolved {
			t.Errorf("a hard deny must carry no human resolution, got %q", decs[0].Resolved)
		}
		if decs[0].RuleID != "deny-bash" {
			t.Errorf("deny RuleID = %q, want deny-bash", decs[0].RuleID)
		}
		// No execution intent was ever committed (no side effect).
		if n := eval.CountEventType(r, domain.EventToolExecutionStarted); n != 0 {
			t.Errorf("ToolExecutionStarted count = %d, want 0 (denied tool not dispatched)", n)
		}
		// The denied call was fed back to the model as an error observation.
		results := eval.PayloadsOf[domain.ToolResult](r)
		if len(results) != 1 {
			t.Fatalf("want exactly one ToolResult (the deny observation), got %d", len(results))
		}
		if !results[0].IsError {
			t.Error("denied ToolResult.IsError = false, want true (error observation)")
		}
	}).Exec(t)
}

// ---------------------------------------------------------------------------
// Scenario 4 — max-turns cap -> ErrorMaxTurns (FR-LOOP-01 AC-2 / FR-LOOP-02).
// The model never returns text-only; the cap fires and the terminal
// TurnFinished carries the subtype and the RED error counter increments.
// ---------------------------------------------------------------------------

func TestScenario_MaxTurnsCap(t *testing.T) {
	const maxTurns = 2
	turns := make([]eval.ModelTurn, 0, maxTurns)
	results := make([]app.ToolResult, 0, maxTurns)
	for i := 0; i < maxTurns; i++ {
		// Vary the args so the doom-loop detector (disabled here anyway) is moot.
		turns = append(turns, eval.ToolCallTurn("call-read", "read",
			map[string]any{"path": idxPath(i)}, llm.Usage{InputTokens: 5, OutputTokens: 1}))
		results = append(results, app.ToolResult{Content: "ok"})
	}

	eval.Scenario{
		Name:              "max-turns-cap",
		UserMessage:       "loop forever",
		ModelTurns:        turns,
		Tools:             []app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}},
		ToolResults:       results,
		Rules:             []policy.Rule{allowRule()},
		MaxTurns:          maxTurns,
		DoomLoopThreshold: -1, // explicit-disable to isolate the max-turns cap (FIX 2: 0 now means default-on)
		CostPerTurnUSD:    0.05,

		WantReason: domain.ErrorMaxTurns,
		WantEvents: []domain.EventType{
			domain.EventMessageAppended, // user task
			// turn 1 (tool round-trip)
			domain.EventTurnStarted,
			domain.EventAssistantMessage,
			domain.EventPermissionDecided,
			domain.EventToolExecutionStarted,
			domain.EventToolResult,
			domain.EventMessageAppended,
			// turn 2 (tool round-trip)
			domain.EventTurnStarted,
			domain.EventAssistantMessage,
			domain.EventPermissionDecided,
			domain.EventToolExecutionStarted,
			domain.EventToolResult,
			domain.EventMessageAppended,
			// cap fires before turn 3: a fresh TurnStarted/TurnFinished records it.
			domain.EventTurnStarted,
			domain.EventTurnFinished,
		},
		WantNumTurns:    maxTurns,
		WantErrorMetric: "error_max_turns",
	}.WithCheck(func(t *testing.T, r eval.Result) {
		fins := eval.PayloadsOf[domain.TurnFinished](r)
		if len(fins) != 1 {
			t.Fatalf("want exactly one TurnFinished, got %d", len(fins))
		}
		if fins[0].Reason != domain.ErrorMaxTurns {
			t.Errorf("TurnFinished.Reason = %q, want error_max_turns", fins[0].Reason)
		}
		if fins[0].NumTurns != maxTurns {
			t.Errorf("TurnFinished.NumTurns = %d, want %d", fins[0].NumTurns, maxTurns)
		}
	}).Exec(t)
}

// ---------------------------------------------------------------------------
// Scenario 5 — refusal -> the distinct Refusal subtype, NOT
// error_during_execution (architecture §11.3).
// ---------------------------------------------------------------------------

func TestScenario_Refusal(t *testing.T) {
	eval.Scenario{
		Name:        "refusal",
		UserMessage: "do something disallowed",
		ModelTurns: []eval.ModelTurn{
			eval.RefusalTurn("I can't help with that."),
		},

		WantReason: domain.Refusal,
		WantEvents: []domain.EventType{
			domain.EventMessageAppended,
			domain.EventTurnStarted,
			domain.EventAssistantMessage, // stop reason refusal
			domain.EventTurnFinished,     // refusal
		},
		WantNumTurns: 1,
	}.WithCheck(func(t *testing.T, r eval.Result) {
		if r.Run.Reason == domain.ErrorDuringExecution {
			t.Error("a refusal must NOT be folded into error_during_execution")
		}
		fins := eval.PayloadsOf[domain.TurnFinished](r)
		if len(fins) != 1 {
			t.Fatalf("want exactly one TurnFinished, got %d", len(fins))
		}
		if fins[0].Reason != domain.Refusal {
			t.Errorf("TurnFinished.Reason = %q, want refusal", fins[0].Reason)
		}
		// The assistant message recorded the refusal stop reason.
		asst := eval.PayloadsOf[domain.AssistantMessage](r)
		if len(asst) != 1 {
			t.Fatalf("want exactly one AssistantMessage, got %d", len(asst))
		}
		if asst[0].StopReason != llm.StopRefusal {
			t.Errorf("AssistantMessage.StopReason = %q, want refusal", asst[0].StopReason)
		}
	}).Exec(t)
}

// ---------------------------------------------------------------------------
// Scenario 6 (optional) — structured-output retry exhaustion ->
// ErrorMaxStructuredOutputRetries (FR-LOOP-02).
// ---------------------------------------------------------------------------

func TestScenario_StructuredOutputRetryExhaustion(t *testing.T) {
	const retries = 2 // initial attempt + 2 retries = 3 model round-trips
	turns := make([]eval.ModelTurn, 0, retries+1)
	for i := 0; i < retries+1; i++ {
		turns = append(turns, eval.TextTurn("this is not valid json"))
	}

	eval.Scenario{
		Name:                       "structured-output-exhaustion",
		UserMessage:                "return JSON with an answer field",
		ModelTurns:                 turns,
		OutputSchema:               []byte(`{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}`),
		MaxStructuredOutputRetries: retries,

		WantReason:      domain.ErrorMaxStructuredOutputRetries,
		WantNumTurns:    retries + 1,
		WantErrorMetric: "error_max_structured_output_retries",
	}.WithCheck(func(t *testing.T, r eval.Result) {
		// Each failed attempt fed a corrective user message back, so there are
		// 1 (initial user) + retries (corrective) MessageAppended events, and
		// exactly retries+1 AssistantMessage / TurnStarted events.
		if got := eval.CountEventType(r, domain.EventAssistantMessage); got != retries+1 {
			t.Errorf("AssistantMessage count = %d, want %d (initial + retries)", got, retries+1)
		}
		if got := eval.CountEventType(r, domain.EventMessageAppended); got != retries+1 {
			t.Errorf("MessageAppended count = %d, want %d (initial user + %d corrective)", got, retries+1, retries)
		}
		fins := eval.PayloadsOf[domain.TurnFinished](r)
		if len(fins) != 1 {
			t.Fatalf("want exactly one TurnFinished, got %d", len(fins))
		}
		if fins[0].Reason != domain.ErrorMaxStructuredOutputRetries {
			t.Errorf("TurnFinished.Reason = %q, want error_max_structured_output_retries", fins[0].Reason)
		}
	}).Exec(t)
}

// ---------------------------------------------------------------------------
// Scenario 7 (bonus) — provider stream error with no retry budget ->
// ErrorDuringExecution + a TurnAborted accounting the (zero) partial usage
// (FR-LOOP-02 AC-2). Demonstrates the harness covers the execution-error path.
// ---------------------------------------------------------------------------

func TestScenario_StreamErrorDuringExecution(t *testing.T) {
	eval.Scenario{
		Name:        "stream-error-during-execution",
		UserMessage: "go",
		ModelTurns: []eval.ModelTurn{
			eval.StreamErrorTurn(&llm.ProviderError{Kind: llm.ErrServer}),
		},

		WantReason: domain.ErrorDuringExecution,
		WantEvents: []domain.EventType{
			domain.EventMessageAppended, // user task
			domain.EventTurnStarted,     // turn 1 started before Generate
			domain.EventTurnAborted,     // stream failed; open turn aborted
		},
		WantErrorMetric: "error_during_execution",
	}.WithCheck(func(t *testing.T, r eval.Result) {
		aborts := eval.PayloadsOf[domain.TurnAborted](r)
		if len(aborts) != 1 {
			t.Fatalf("want exactly one TurnAborted, got %d", len(aborts))
		}
		if aborts[0].Reason != domain.ErrorDuringExecution {
			t.Errorf("TurnAborted.Reason = %q, want error_during_execution", aborts[0].Reason)
		}
	}).Exec(t)
}

// ---------------------------------------------------------------------------
// Scenario 8 (bonus) — doom-loop detection AND termination: the SAME tool call
// repeated up to the configured threshold emits a doom-loop signal labeled by
// tool name (FR-OBS-04 AC-1) AND, since FIX 2 (ADR-0032), TERMINATES the run
// with domain.ErrorDoomLoop on the tripping batch BEFORE dispatching it — rather
// than burning turns until a cap. The doom-loop metric is still emitted on the
// trip; the terminal reason is now ErrorDoomLoop (no longer Success).
// ---------------------------------------------------------------------------

func TestScenario_DoomLoopDetected(t *testing.T) {
	const threshold = 3
	// Script MORE identical read calls than the threshold (the run trips on the
	// threshold-th identical batch BEFORE dispatch, so the extra scripted turns —
	// including the final text turn — are simply left unconsumed; the fake gateway
	// only panics on queue EXHAUSTION, never on leftover queued streams).
	turns := make([]eval.ModelTurn, 0, threshold+2)
	results := make([]app.ToolResult, 0, threshold+1)
	for i := 0; i < threshold+1; i++ {
		turns = append(turns, eval.ToolCallTurn("call-same", "read",
			map[string]any{"path": "/same"}, llm.Usage{}))
		results = append(results, app.ToolResult{Content: "same content"})
	}
	turns = append(turns, eval.TextTurn("done after repeating"))

	eval.Scenario{
		Name:              "doom-loop-detected",
		UserMessage:       "keep reading the same file",
		ModelTurns:        turns,
		Tools:             []app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}},
		ToolResults:       results,
		Rules:             []policy.Rule{allowRule()},
		DoomLoopThreshold: threshold,
		MaxTurns:          16,

		WantReason:       domain.ErrorDoomLoop,
		WantDoomLoopTool: "read",
	}.Exec(t)
}

// ---------------------------------------------------------------------------
// Scenario 9 — max-budget cap -> ErrorMaxBudgetUSD (FR-LOOP-02 AC-1; DOD-03).
// The budget is checked BEFORE each Generate (strictly cost > cap): after one
// tool round-trip the cumulative cost exceeds the cap, so the run terminates
// on a fresh turn boundary with error_max_budget_usd and the model is never
// called again.
// ---------------------------------------------------------------------------

func TestScenario_MaxBudgetUSDCap(t *testing.T) {
	// One $0.06 turn against a $0.05 cap. Only ONE model turn is scripted: a
	// second Stream would panic the fake gateway, proving no further Generate
	// occurred after the cap fired.
	eval.Scenario{
		Name:        "max-budget-usd-cap",
		UserMessage: "do an expensive thing",
		ModelTurns: []eval.ModelTurn{
			eval.ToolCallTurn("call-read-1", "read", map[string]any{"path": "/big"},
				llm.Usage{InputTokens: 12, OutputTokens: 3}),
		},
		Tools:          []app.ToolDescriptor{{Name: "read", SideEffect: domain.SideEffectReadOnly}},
		ToolResults:    []app.ToolResult{{Content: "big output"}},
		Rules:          []policy.Rule{allowRule()},
		MaxBudgetUSD:   0.05,
		CostPerTurnUSD: 0.06,

		WantReason: domain.ErrorMaxBudgetUSD,
		WantEvents: []domain.EventType{
			domain.EventMessageAppended, // user task
			// turn 1 (tool round-trip; costs $0.06, exceeding the $0.05 cap)
			domain.EventTurnStarted,
			domain.EventAssistantMessage,
			domain.EventPermissionDecided,
			domain.EventToolExecutionStarted,
			domain.EventToolResult,
			domain.EventMessageAppended,
			// cap fires before turn 2: a fresh TurnStarted/TurnFinished records it.
			domain.EventTurnStarted,
			domain.EventTurnFinished,
		},
		WantNumTurns:     1,
		WantCostUSD:      0.06,
		WantCostAsserted: true,
		WantErrorMetric:  "error_max_budget_usd",
	}.WithCheck(func(t *testing.T, r eval.Result) {
		// The terminal TurnFinished carries the budget subtype and the run cost.
		fins := eval.PayloadsOf[domain.TurnFinished](r)
		if len(fins) != 1 {
			t.Fatalf("want exactly one TurnFinished, got %d", len(fins))
		}
		if fins[0].Reason != domain.ErrorMaxBudgetUSD {
			t.Errorf("TurnFinished.Reason = %q, want error_max_budget_usd", fins[0].Reason)
		}
		if math.Abs(fins[0].CostUSD-0.06) > 1e-9 {
			t.Errorf("TurnFinished.CostUSD = %.6f, want 0.06", fins[0].CostUSD)
		}
		if fins[0].NumTurns != 1 {
			t.Errorf("TurnFinished.NumTurns = %d, want 1", fins[0].NumTurns)
		}
	}).Exec(t)
}

// ---------------------------------------------------------------------------
// Scenario 10 — permission-mode enforcement: plan mode holds a mutating tool
// for approval (FR-PERM-01 AC-2; DOD-03). Mode precedes allow in the
// deny→mode→allow→ask pipeline, so plan maps the mutating call to ASK (rule id
// "plan") even though a catch-all allow rule exists; the scripted human DENIES,
// so the tool is never executed and the denial is fed back as an error
// observation. Modeled on TestScenario_PermissionDeniedNoExecution.
// ---------------------------------------------------------------------------

func TestScenario_PlanModeHoldsMutatingTool(t *testing.T) {
	eval.Scenario{
		Name:        "plan-mode-holds-mutating-tool",
		UserMessage: "write main.go",
		ModelTurns: []eval.ModelTurn{
			// Turn 1: the assistant requests a mutating edit while in plan mode.
			eval.ToolCallTurn("call-write-1", "write",
				map[string]any{"path": "main.go", "content": "package main"}, llm.Usage{}),
			// Turn 2: after the denial observation, the assistant ends with text.
			eval.TextTurn("staying read-only; here is the plan instead"),
		},
		Tools: []app.ToolDescriptor{
			{Name: "write", SideEffect: domain.SideEffectMutating, EgressClass: domain.EgressClassNone},
		},
		// A catch-all allow rule that plan mode must PREEMPT (asserted via the
		// decision's RuleID below).
		Rules: []policy.Rule{allowRule()},
		Mode:  policy.ModePlan,
		// The held call is auto-DENIED by the scripted human resolution.
		Approvals: map[string]domain.AskResolution{"call-write-1": domain.AskDenied},
		// No ToolResults scripted: a dispatch would panic the fake runtime,
		// proving the held tool was never executed.

		WantReason: domain.Success,
		WantEvents: []domain.EventType{
			domain.EventMessageAppended,   // user task
			domain.EventTurnStarted,       // turn 1
			domain.EventAssistantMessage,  // tool_call
			domain.EventApprovalRequested, // durable pre-block ask record (ADR-0032, AC-3.3)
			domain.EventPermissionDecided, // ASK, resolved denied (no ToolExecutionStarted)
			domain.EventToolResult,        // synthetic error observation fed back
			domain.EventMessageAppended,   // results fed back (tool role)
			domain.EventTurnStarted,       // turn 2
			domain.EventAssistantMessage,  // final text
			domain.EventTurnFinished,      // success
		},
		WantNumTurns:        2,
		WantNoToolExecution: true,
	}.WithCheck(func(t *testing.T, r eval.Result) {
		// Exactly one PermissionDecided: an ASK raised by plan mode (not the allow
		// rule) and resolved DENIED by the human.
		decs := eval.PayloadsOf[domain.PermissionDecided](r)
		if len(decs) != 1 {
			t.Fatalf("want exactly one PermissionDecided, got %d", len(decs))
		}
		if decs[0].Decision != domain.PermissionAsk {
			t.Errorf("decision = %q, want ask", decs[0].Decision)
		}
		if decs[0].Resolved != domain.AskDenied {
			t.Errorf("ask resolution = %q, want denied", decs[0].Resolved)
		}
		if decs[0].RuleID != string(policy.ModePlan) {
			t.Errorf("RuleID = %q, want %q (plan-mode hold, not the allow rule)", decs[0].RuleID, policy.ModePlan)
		}
		// No execution intent was ever committed (no side effect).
		if n := eval.CountEventType(r, domain.EventToolExecutionStarted); n != 0 {
			t.Errorf("ToolExecutionStarted count = %d, want 0 (held tool not dispatched)", n)
		}
		// The denied call was fed back to the model as an error observation.
		results := eval.PayloadsOf[domain.ToolResult](r)
		if len(results) != 1 {
			t.Fatalf("want exactly one ToolResult (the deny observation), got %d", len(results))
		}
		if !results[0].IsError {
			t.Error("denied ToolResult.IsError = false, want true (error observation)")
		}
	}).Exec(t)
}

// ---------------------------------------------------------------------------
// Scenario 11 — compaction trigger (FR-CTX-01 AC-1; DOD-03): the model-visible
// window crosses the configured context-budget threshold, so the loop appends
// exactly one CompactionPerformed boundary BEFORE the turn's TurnStarted and
// the same turn generates from the reduced window. The REAL agentctx.Manager
// decides; only the token counter is scripted (current window = 1000 tokens
// >= threshold 80, projected post-boundary window = 10).
// ---------------------------------------------------------------------------

func TestScenario_CompactionTriggered(t *testing.T) {
	eval.Scenario{
		Name:        "compaction-triggered",
		UserMessage: "summarize this very long conversation",
		ModelTurns: []eval.ModelTurn{
			// The single post-compaction turn completes with text.
			eval.TextTurn("done, working from the compacted summary"),
		},
		MaxContextTokens:  100,             // threshold = 80 at the default 0.8 fraction
		WindowTokenCounts: []int{1000, 10}, // current (over budget), then projected

		WantReason: domain.Success,
		WantEvents: []domain.EventType{
			domain.EventMessageAppended,     // user task
			domain.EventCompactionPerformed, // boundary appended BEFORE the turn it reduces
			domain.EventTurnStarted,         // turn 1 (generates from the reduced window)
			domain.EventAssistantMessage,    // final text
			domain.EventTurnFinished,        // success
		},
		WantNumTurns: 1,
	}.WithCheck(func(t *testing.T, r eval.Result) {
		// Exactly one boundary carrying the measured before/after token counts,
		// so the reclamation is visible to cost/observability consumers.
		comps := eval.PayloadsOf[domain.CompactionPerformed](r)
		if len(comps) != 1 {
			t.Fatalf("want exactly one CompactionPerformed, got %d", len(comps))
		}
		if comps[0].BeforeTokens != 1000 {
			t.Errorf("BeforeTokens = %d, want 1000", comps[0].BeforeTokens)
		}
		if comps[0].AfterTokens != 10 {
			t.Errorf("AfterTokens = %d, want 10", comps[0].AfterTokens)
		}
		if comps[0].Reason == "" {
			t.Error("CompactionPerformed.Reason is empty, want a recorded trigger reason")
		}
	}).Exec(t)
}

// idxPath renders a distinct path argument per turn for the max-turns scenario.
func idxPath(i int) string { return fmt.Sprintf("/file-%d", i) }
