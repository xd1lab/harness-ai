package domain

// TerminationReason is the normalized subtype describing how an agent run (or the
// turn that ends it) terminated. It is the "typed termination subtype" the RED
// metrics break error rate down by (architecture §10.5) and the result subtype
// returned to the client (architecture §3, Result{subtype=...}). The set matches
// architecture §5.1 plus the first-class [Refusal] of architecture §11.3.
//
// A run that ends cleanly is [Success]; everything else names a specific terminal
// condition. Crucially, a model refusal is its own subtype ([Refusal]) and is NOT
// folded into [ErrorDuringExecution] (architecture §11.3), so a refusal can drive a
// fallback-model policy and is reported distinctly.
type TerminationReason string

const (
	// Success is a normal completion: the model produced a text-only response and
	// the loop ended without hitting a cap or refusing (architecture §3,
	// subtype=success).
	Success TerminationReason = "success"

	// ErrorMaxTurns is termination because the run hit its configured maximum
	// number of turns cap (architecture §5.1 error_max_turns; budget caps §5.1
	// budget.go).
	ErrorMaxTurns TerminationReason = "error_max_turns"

	// ErrorMaxBudgetUSD is termination because the run hit its configured maximum
	// cumulative cost cap in USD (architecture §5.1 error_max_budget_usd).
	ErrorMaxBudgetUSD TerminationReason = "error_max_budget_usd"

	// ErrorDuringExecution is termination because of an execution error — a
	// downstream dependency failure surfaced after the bounded in-turn retry
	// budget, an unrecoverable tool/runtime error, or an interrupt/recovery abort.
	// It is the typed error the orchestrator surfaces rather than hanging
	// (architecture §4.4, §10.3, §5.1). A refusal is explicitly NOT this subtype
	// (architecture §11.3).
	ErrorDuringExecution TerminationReason = "error_during_execution"

	// ErrorMaxStructuredOutputRetries is termination because the run exhausted its
	// retries attempting to obtain schema-valid structured output (architecture
	// §5.1 error_max_structured_output_retries).
	ErrorMaxStructuredOutputRetries TerminationReason = "error_max_structured_output_retries"

	// Refusal is termination because the model declined to comply
	// ([llm.StopRefusal]). It is a first-class subtype, distinct from
	// [ErrorDuringExecution], so it can trigger a fallback-model policy and be
	// reported separately (architecture §11.3).
	Refusal TerminationReason = "refusal"
)

// IsError reports whether the reason denotes an unsuccessful termination (anything
// other than [Success]). A [Refusal] counts as a non-success terminal condition but
// is reported under its own subtype; callers that must distinguish it should
// compare against [Refusal] explicitly rather than relying on this predicate.
func (r TerminationReason) IsError() bool { return r != Success }
