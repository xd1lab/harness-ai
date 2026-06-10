package policytest_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/orchestrator/domain"
	"github.com/boltrope/boltrope/internal/orchestrator/policy"
	"github.com/boltrope/boltrope/internal/orchestrator/policy/policytest"
)

func TestFakePolicyEngine_Interface(_ *testing.T) {
	var _ policy.PolicyEngine = (*policytest.FakePolicyEngine)(nil)
}

func TestFakePolicyEngine_DefaultAllow(t *testing.T) {
	pe := policytest.NewFakePolicyEngine()
	res, err := pe.Evaluate(context.Background(), policy.Input{
		ToolName:    "bash",
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
		Mode:        policy.ModeDefault,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Allow, res.Decision)
}

func TestFakePolicyEngine_ScriptedDeny(t *testing.T) {
	pe := policytest.NewFakePolicyEngine()
	pe.AddDeny("rule-deny-bash", "bash is always denied")
	res, err := pe.Evaluate(context.Background(), policy.Input{ToolName: "bash"})
	require.NoError(t, err)
	assert.Equal(t, policy.Deny, res.Decision)
	assert.Equal(t, "rule-deny-bash", res.RuleID)
}

func TestFakePolicyEngine_ScriptedAsk(t *testing.T) {
	pe := policytest.NewFakePolicyEngine()
	pe.AddAsk("", "external comms")
	res, err := pe.Evaluate(context.Background(), policy.Input{ToolName: "webfetch"})
	require.NoError(t, err)
	assert.Equal(t, policy.Ask, res.Decision)
}

func TestFakePolicyEngine_CallsRecorded(t *testing.T) {
	pe := policytest.NewFakePolicyEngine()
	in := policy.Input{ToolName: "edit", SessionID: "sess-1"}
	_, _ = pe.Evaluate(context.Background(), in)
	calls := pe.Calls()
	require.Len(t, calls, 1)
	assert.Equal(t, "edit", calls[0].In.ToolName)
}

func TestFakePolicyEngine_QueueExhaustedReturnsAllow(t *testing.T) {
	pe := policytest.NewFakePolicyEngine()
	pe.AddDeny("r", "reason")
	_, _ = pe.Evaluate(context.Background(), policy.Input{})
	// Queue exhausted — should default to Allow.
	res, err := pe.Evaluate(context.Background(), policy.Input{})
	require.NoError(t, err)
	assert.Equal(t, policy.Allow, res.Decision)
}
