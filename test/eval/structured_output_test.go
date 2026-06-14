package eval_test

// Feature S (structured output) — end-to-end loop backstop locks, AC-13 / AC-14 /
// TASKS T-1 + T-8.
//
// These deterministic eval cases drive the REAL agent loop (fake provider + fake
// clock, no network) to prove the structured-output BACKSTOP is reachable and
// correct independent of any provider-native mapping:
//
//   - structured_output_success: a schema-conforming final response terminates
//     TERMINATION_SUBTYPE_SUCCESS (the success side of AC-13). [GREEN at authoring:
//     the loop already honors Config.OutputSchema; this LOCKS that behavior as the
//     acceptance net for the public field once it is wired through T-3..T-8.]
//   - no_schema_unchanged: a run with NO schema is byte-for-byte the free-form
//     behavior — same terminal subtype, same golden event shape (AC-14, backward
//     compatibility). [GREEN at authoring.]
//
// The retry-EXHAUSTION half of AC-13 is already pinned by
// TestScenario_StructuredOutputRetryExhaustion (scenarios_test.go scenario 6); we
// do not duplicate it. The eval harness drives agent.Config.OutputSchema directly
// (loop level) — by design it does NOT exercise adapter-native mapping (fake
// provider); native is an adapter-layer concern verified in the provider tests.

import (
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
	"github.com/xd1lab/harness-ai/test/eval"
)

// TestScenario_StructuredOutputSuccess pins the success side of AC-13: a single
// schema-conforming JSON response terminates Success on the first attempt (no
// retries), proving the configured schema does not penalize a valid response.
func TestScenario_StructuredOutputSuccess(t *testing.T) {
	eval.Scenario{
		Name:        "structured-output-success",
		UserMessage: "return JSON with an answer field",
		ModelTurns: []eval.ModelTurn{
			eval.TextTurnWithUsage(`{"answer":"42"}`, llm.Usage{InputTokens: 8, OutputTokens: 4}),
		},
		OutputSchema:               []byte(`{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}`),
		MaxStructuredOutputRetries: 2,

		WantReason: domain.Success,
		WantEvents: []domain.EventType{
			domain.EventMessageAppended, // user task
			domain.EventTurnStarted,
			domain.EventAssistantMessage, // schema-valid text
			domain.EventTurnFinished,     // success on first attempt
		},
		WantNumTurns: 1,
	}.Exec(t)
}

// TestScenario_NoSchemaUnchanged pins AC-14: a run that sets NO OutputSchema is the
// free-form path — identical terminal subtype and golden event shape to a plain
// single-turn text run (zero migration cost for existing clients).
func TestScenario_NoSchemaUnchanged(t *testing.T) {
	eval.Scenario{
		Name:        "no-schema-unchanged",
		UserMessage: "say hello",
		ModelTurns: []eval.ModelTurn{
			eval.TextTurn("hello, free-form world"),
		},
		// OutputSchema deliberately unset → free-form.

		WantReason: domain.Success,
		WantEvents: []domain.EventType{
			domain.EventMessageAppended,
			domain.EventTurnStarted,
			domain.EventAssistantMessage,
			domain.EventTurnFinished,
		},
		WantNumTurns: 1,
	}.Exec(t)
}
