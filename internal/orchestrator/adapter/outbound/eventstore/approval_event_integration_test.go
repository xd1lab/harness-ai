//go:build integration

package eventstore

import (
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// TestApprovalRequested_AppendLoadRoundTrip is the integration RED proof for FIX 3
// AC-3.9 / AC-3.10: a domain.ApprovalRequested event (the un-collapsed general
// tool-dispatch approval request) Appends and Loads back from real Postgres with
// its Args intact — event_type "ApprovalRequested" persists and decodePayload
// reconstructs the typed event. It simulates a crash mid-ask: the ApprovalRequested
// is written with NO matching PermissionDecided, exactly the durable state a resume
// must classify as a SuspendedApproval rather than silently abort.
//
// It references domain.ApprovalRequested / domain.EventApprovalRequested, which do
// not exist yet, so the package does not compile under -tags integration — the RED
// proof that the feature is absent.
func TestApprovalRequested_AppendLoadRoundTrip(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	want := domain.ApprovalRequested{
		TurnID:   "t-1",
		CallID:   "call-7",
		ToolName: "bash",
		Reason:   "mutating shell command",
		Args:     map[string]any{"cmd": "rm -rf /tmp/x", "force": float64(1)},
	}

	envs, err := h.store.Append(ctx, sessionID, 0, 0, newUUID(t),
		app.AppendInput{Event: want, Actor: domain.ActorSystem})
	if err != nil {
		t.Fatalf("append ApprovalRequested: %v", err)
	}
	if len(envs) != 1 || envs[0].Type != domain.EventApprovalRequested {
		t.Fatalf("append returned %d envs, type %v; want 1 ApprovalRequested", len(envs), envs[0].Type)
	}

	loaded, err := h.store.Load(ctx, sessionID, 1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var got *domain.ApprovalRequested
	for _, e := range loaded {
		if p, ok := e.Event.(domain.ApprovalRequested); ok {
			got = &p
		}
	}
	if got == nil {
		t.Fatalf("no ApprovalRequested decoded from %d loaded events (decodePayload must handle the new type)", len(loaded))
	}
	if got.TurnID != want.TurnID || got.CallID != want.CallID || got.ToolName != want.ToolName || got.Reason != want.Reason {
		t.Fatalf("ApprovalRequested round-trip mismatch: got %+v, want %+v", *got, want)
	}
	// Args must survive the round trip intact (JSON numbers decode to float64).
	if got.Args["cmd"] != want.Args["cmd"] || got.Args["force"] != want.Args["force"] {
		t.Fatalf("ApprovalRequested.Args round-trip mismatch: got %+v, want %+v", got.Args, want.Args)
	}

	// There is NO matching PermissionDecided for call-7 — this is the suspended-ask
	// shape a resume must re-raise rather than silently TurnAbort.
	for _, e := range loaded {
		if pd, ok := e.Event.(domain.PermissionDecided); ok && pd.CallID == want.CallID {
			t.Fatalf("unexpected PermissionDecided for %q; the ask must be unresolved (crash mid-ask)", want.CallID)
		}
	}
}
