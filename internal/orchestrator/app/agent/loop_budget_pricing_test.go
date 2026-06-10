// SPDX-License-Identifier: Apache-2.0

package agent_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// These tests pin the fail-closed budget/pricing contract (FR-LOOP-02; 2026-06
// hardening): when a budget cap is SET, a turn whose cost CANNOT be computed
// (CostFunc error, e.g. pricing.UnknownModelError) MUST terminate the run
// instead of silently pricing the turn at $0 — a $0 fallback disarms the cap
// and lets the run spend unbounded. Without a cap, cost remains best-effort
// observability and an unknown price degrades to zero as before.

// budgetPricingLoop builds a loop whose CostFunc always fails.
func budgetPricingLoop(h *harness, cfg agent.Config) *agent.Loop {
	return agent.NewLoop(agent.Deps{
		EventLog: h.eventlog, Model: h.model, Tools: h.tools, Approvals: h.gate,
		Hooks: h.hooks, Policy: h.pol, Clock: h.clk, IDs: h.ids, Sink: h.sink, Metrics: h.metrics,
		CostFunc: func(model string, _ llm.Usage) (float64, error) {
			// A non-zero value alongside the error proves callers discard the
			// value: an erroring price must never leak a partial/guessed cost.
			return 99, errors.New("pricing: unknown model \"" + model + "\" — add it to the pricing table or supply a config override")
		},
	}, cfg)
}

// TestRun_BudgetCapUnknownPriceFailsClosed asserts that with MaxBudgetUSD set,
// the first turn whose price is unknown aborts the run with
// error_during_execution — before any further Generate.
func TestRun_BudgetCapUnknownPriceFailsClosed(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.MaxBudgetUSD = 1.0

	// The model would happily keep going (tool turn then text), but the run must
	// die on the FIRST unpriceable turn.
	h.tools.SetTools(nil)
	h.model.AddStream(textStream("hello"), nil)
	h.model.AddStream(textStream("never reached"), nil)

	lp := budgetPricingLoop(h, cfg)
	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-bpf", UserMessage: userMsg("go")})
	require.NoError(t, err)
	assert.Equal(t, domain.ErrorDuringExecution, res.Reason,
		"a set budget cap with an unpriceable turn must fail closed, not run free")

	// Exactly one Generate: the failure is detected on the first turn.
	streamCalls := 0
	for _, c := range h.model.Calls() {
		if c.Method == "Stream" {
			streamCalls++
		}
	}
	assert.Equal(t, 1, streamCalls, "must not Generate again after an unpriceable turn")

	// The turn is closed durably with a TurnAborted carrying the reason; the
	// unpriceable turn must NOT be recorded as a normal AssistantMessage turn.
	aborts := payloadsOf[domain.TurnAborted](h, "sess-bpf")
	require.NotEmpty(t, aborts, "expected a TurnAborted closing the unpriceable turn")
	assert.Equal(t, domain.ErrorDuringExecution, aborts[len(aborts)-1].Reason)
	assert.Zero(t, aborts[len(aborts)-1].CostUSD, "an unknown price must be recorded as 0, never guessed")
	assert.Empty(t, payloadsOf[domain.AssistantMessage](h, "sess-bpf"),
		"the unpriceable turn must not land as a normal assistant turn")

	assert.Equal(t, 1, h.metrics.errorCount("error_during_execution"))
}

// TestRun_NoBudgetCapUnknownPriceIsBestEffort asserts that WITHOUT a budget cap
// an unknown price degrades to $0 (observability only) and the run completes.
func TestRun_NoBudgetCapUnknownPriceIsBestEffort(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	cfg.MaxBudgetUSD = 0 // non-positive disables the cap

	h.model.AddStream(textStream("done"), nil)

	lp := budgetPricingLoop(h, cfg)
	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-bpb", UserMessage: userMsg("go")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason,
		"without a cap an unknown price must not break the run")
	assert.Zero(t, res.CostUSD)

	msgs := payloadsOf[domain.AssistantMessage](h, "sess-bpb")
	require.Len(t, msgs, 1)
	assert.Zero(t, msgs[0].CostUSD)
}
