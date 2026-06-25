//go:build integration

package eventstore

import (
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// TestPlanUpdated_AppendLoadRoundTrip is the integration RED proof for Gap#3
// AC-12: a domain.PlanUpdated event Appends and Loads back from real Postgres
// with its Items intact (event_type "PlanUpdated" persists and decodePayload
// reconstructs it). It references domain.PlanUpdated / domain.PlanItem, which do
// not exist yet, so the package does not compile under -tags integration — the
// RED proof that the feature is absent.
func TestPlanUpdated_AppendLoadRoundTrip(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	want := domain.PlanUpdated{
		TurnID: "t-1",
		Items: []domain.PlanItem{
			{Content: "explore", Status: "completed"},
			{Content: "implement", Status: "in_progress"},
			{Content: "verify", Status: "pending"},
		},
	}
	envs, err := h.store.Append(ctx, sessionID, 0, 0, newUUID(t),
		app.AppendInput{Event: want, Actor: domain.ActorAssistant})
	if err != nil {
		t.Fatalf("append PlanUpdated: %v", err)
	}
	if len(envs) != 1 || envs[0].Type != domain.EventPlanUpdated {
		t.Fatalf("append returned %d envs, type %v; want 1 PlanUpdated", len(envs), envs[0].Type)
	}

	loaded, err := h.store.Load(ctx, sessionID, 1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var got *domain.PlanUpdated
	for _, e := range loaded {
		if p, ok := e.Event.(domain.PlanUpdated); ok {
			got = &p
		}
	}
	if got == nil {
		t.Fatalf("no PlanUpdated decoded from %d loaded events", len(loaded))
	}
	if got.TurnID != want.TurnID || len(got.Items) != len(want.Items) {
		t.Fatalf("PlanUpdated round-trip mismatch: got %+v, want %+v", *got, want)
	}
	for i := range want.Items {
		if got.Items[i] != want.Items[i] {
			t.Fatalf("PlanItem[%d] = %+v, want %+v", i, got.Items[i], want.Items[i])
		}
	}
}
