// SPDX-License-Identifier: Apache-2.0

package main

// RED (test-first) for Gap#3 AC-15: the dev in-memory event store must
// round-trip the NEW domain.PlanUpdated event. Because the dev store holds live
// domain.Event structs (no JSON codec), no codec change is needed there — this
// test verifies a PlanUpdated appended via Append is returned by
// Load/LoadRange/LoadUpTo unchanged, and that the cost-fold helpers
// (devFoldModelCost / TenantSessionCostCount) ignore it without panic.
//
// It references domain.PlanUpdated / domain.PlanItem, which do not exist yet, so
// this file does NOT compile — the RED proof.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

func TestDevStore_PlanUpdatedRoundTrips(t *testing.T) {
	ctx := context.Background()
	st := newStore()
	const sid = "sess-plan-dev"

	_, err := st.CreateSession(ctx, sid, domain.ModeDefault)
	require.NoError(t, err)
	_, err = st.Append(ctx, sid, 0, 0, "req-ss", app.AppendInput{
		Event: domain.SessionStarted{}, Actor: domain.ActorSystem,
	})
	require.NoError(t, err)

	plan := domain.PlanUpdated{TurnID: "t-1", Items: []domain.PlanItem{
		{Content: "first", Status: "in_progress"},
		{Content: "second", Status: "pending"},
	}}
	envs, err := st.Append(ctx, sid, 1, 0, "req-plan", app.AppendInput{
		Event: plan, Actor: domain.ActorAssistant,
	})
	require.NoError(t, err)
	require.Len(t, envs, 1)
	assert.Equal(t, int64(2), envs[0].Seq)

	// Load returns the live struct unchanged.
	all, err := st.Load(ctx, sid, 0)
	require.NoError(t, err)
	got := findPlan(t, all)
	assert.Equal(t, plan, got, "Load must return the PlanUpdated unchanged")

	// LoadRange (seq > 1) and LoadUpTo (seq <= 2) both include it.
	rng, err := st.LoadRange(ctx, sid, 1, 100)
	require.NoError(t, err)
	assert.Equal(t, plan, findPlan(t, rng), "LoadRange must include the PlanUpdated")

	upTo, err := st.LoadUpTo(ctx, sid, 2)
	require.NoError(t, err)
	assert.Equal(t, plan, findPlan(t, upTo), "LoadUpTo must include the PlanUpdated")

	// The cost-fold helpers must ignore it without panicking.
	assert.NotPanics(t, func() {
		_ = devFoldModelCost(all)
	})
	_, err = st.TenantSessionCostCount(ctx)
	require.NoError(t, err)
}

func findPlan(t *testing.T, envs []domain.EventEnvelope) domain.PlanUpdated {
	t.Helper()
	for _, e := range envs {
		if p, ok := e.Event.(domain.PlanUpdated); ok {
			return p
		}
	}
	t.Fatalf("no PlanUpdated event found in %d envelopes", len(envs))
	return domain.PlanUpdated{}
}
